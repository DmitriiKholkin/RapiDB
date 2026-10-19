import { createServer } from "node:http";
import { Client, errors } from "@elastic/elasticsearch";
import { describe, expect, it } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { ElasticsearchSerializer } from "../../src/extension/dbDrivers/elasticsearchSerializer";
import { canonicalizeJsonNumericValues } from "../../src/extension/utils/jsonCanonical";

const exportRequest = {
  database: "default",
  schema: "indices",
  table: "records",
  chunkSize: 10,
};

/** Send literal JSON, never JSON.stringify a JS number in this fixture. */
async function fixture(source: string, mutationResponse?: string) {
  const requests: Array<{ method?: string; path: string; body: string }> = [];
  const closedIds: string[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const path = request.url ?? "";
    const body = Buffer.concat(chunks).toString();
    requests.push({ method: request.method, path, body });
    response.setHeader("x-elastic-product", "Elasticsearch");
    response.setHeader("content-type", "application/json");
    if (request.method === "HEAD") response.end();
    else if (path.includes("/_settings/"))
      response.end(
        '{"records":{"settings":{"index":{"max_result_window":"100"}}}}',
      );
    else if (request.method === "POST" && path.startsWith("/records/_pit"))
      response.end('{"id":"precision-pit"}');
    else if (request.method === "DELETE" && path.startsWith("/_pit")) {
      closedIds.push(JSON.parse(body).id);
      response.end('{"succeeded":true,"num_freed":1}');
    } else if (path.includes("_search"))
      response.end(
        `{"timed_out":false,"_shards":{"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"doc-1","_source":${source},"sort":[0]}]}}`,
      );
    else if (request.method === "GET" && path === "/records/_doc/doc-1")
      response.end(`{"_id":"doc-1","_source":${source},"found":true}`);
    else
      response.end(
        mutationResponse ??
          '{"result":"created","_id":"doc-1","_index":"records"}',
      );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("Missing HTTP port");
  const node = `http://127.0.0.1:${address.port}`;
  const driver = new ElasticsearchDriver({
    id: "es-response-precision",
    name: "ES response precision",
    type: "elasticsearch",
    endpoint: node,
  });
  return {
    node,
    driver,
    requests,
    closedIds,
    async close() {
      await driver.disconnect();
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

describe("Elasticsearch response numeric precision audit (real SDK + HTTP)", () => {
  it("independently reproduces rounding by the stock SDK deserializer", async () => {
    const f = await fixture('{"amount":9007199254740993}');
    const client = new Client({ node: f.node });
    try {
      const response = await client.search({ index: "records" });
      expect(JSON.stringify(response.hits.hits[0]._source)).toBe(
        '{"amount":9007199254740992}',
      );
    } finally {
      await client.close();
      await f.close();
    }
  });

  it.each([
    "9007199254740993",
    "1000000000000000128",
    "0.123456789012345678901",
    "1e400",
    "1e-400",
  ])("rejects unsafe source %s before exporting a chunk and closes its PIT", async (token) => {
    const f = await fixture(`{"amount":${token}}`);
    try {
      await f.driver.connect();
      await expect(
        f.driver.exportTableChunks(exportRequest).next(),
      ).rejects.toThrow(/numeric precision/i);
      expect(f.closedIds).toEqual(["precision-pit"]);
    } finally {
      await f.close();
    }
  });

  it.each([
    "GET /records/_doc/doc-1",
    "GET /records/_search",
    "POST /_search",
  ])("rejects unsafe numbers through query mode: %s", async (command) => {
    const f = await fixture('{"nested":[{"amount":9007199254740993}]}');
    try {
      await f.driver.connect();
      await expect(f.driver.query(command)).rejects.toMatchObject({
        name: "DeserializationError",
        message: expect.stringMatching(/numeric precision/i),
      });
      // In particular, an SDK retry must not turn a rejected response into
      // another request (which could replay a mutation for other endpoints).
      expect(f.requests.filter((r) => r.method !== "HEAD")).toHaveLength(1);
    } finally {
      await f.close();
    }
  });

  it.each([
    false,
    true,
  ])("fails closed for a table read (materialized=%s)", async (materialized) => {
    const f = await fixture('{"amount":0.123456789012345678901}');
    try {
      await f.driver.connect();
      await expect(
        f.driver.readTablePage({
          ...exportRequest,
          page: 1,
          pageSize: 10,
          filters: [],
          skipCount: false,
          sort: materialized ? { column: "_id", direction: "asc" } : null,
        }),
      ).rejects.toMatchObject({ name: "DeserializationError" });
      expect(f.closedIds).toEqual(materialized ? [] : ["precision-pit"]);
    } finally {
      await f.close();
    }
  });

  it("keeps ordinary numeric types and their values through export and a downstream write", async () => {
    const source =
      '{"amount":0.1,"count":9007199254740991,"largeExact":9007199254740992,"nested":[1.00,1e3,-0],"text":"9007199254740993"}';
    const f = await fixture(source);
    try {
      await f.driver.connect();
      const rows: Record<string, unknown>[] = [];
      for await (const chunk of f.driver.exportTableChunks(exportRequest))
        rows.push(...chunk.rows);
      expect(rows).toHaveLength(1);
      const exported = rows[0]._source as string;
      expect(canonicalizeJsonNumericValues(exported)).toBe(
        canonicalizeJsonNumericValues(source),
      );
      expect(typeof JSON.parse(exported).amount).toBe("number");
      await f.driver.insertRow({ ...exportRequest, values: rows[0] });
      const write = f.requests.find(
        (r) => r.method === "PUT" && r.path.includes("/_doc/"),
      );
      expect(write).toBeDefined();
      expect(canonicalizeJsonNumericValues(write?.body ?? "")).toBe(
        canonicalizeJsonNumericValues(source),
      );
      expect(f.closedIds).toEqual(["precision-pit"]);
    } finally {
      await f.close();
    }
  });

  it("still refuses unsafe numeric source writes before sending HTTP", async () => {
    const f = await fixture("{}");
    const source = '{"amount":9007199254740993}';
    try {
      await f.driver.connect();
      await expect(
        f.driver.insertRow({ ...exportRequest, values: { _source: source } }),
      ).rejects.toThrow(/numeric precision/i);
      await expect(
        f.driver.updateRows({
          ...exportRequest,
          updates: [
            { primaryKeys: { _id: "doc-1" }, changes: { _source: source } },
          ],
        }),
      ).rejects.toThrow(/numeric precision/i);
      await expect(
        f.driver.query(`PUT /records/_doc/doc-1\n${source}`),
      ).rejects.toThrow(/numeric precision/i);
      expect(f.requests.map((r) => r.method)).toEqual(["HEAD"]);
    } finally {
      await f.close();
    }
  });

  it("never replays a mutation when its response cannot be parsed safely", async () => {
    const f = await fixture(
      "{}",
      '{"result":"created","_id":"doc-1","_index":"records","_seq_no":9007199254740993}',
    );
    try {
      await f.driver.connect();
      await expect(
        f.driver.query('PUT /records/_doc/doc-1\n{"amount":0.1}'),
      ).rejects.toMatchObject({ name: "DeserializationError" });
      expect(f.requests.filter((r) => r.method === "PUT")).toHaveLength(1);
    } finally {
      await f.close();
    }
  });
});

describe("ElasticsearchSerializer portable numeric boundary", () => {
  const serializer = new ElasticsearchSerializer();

  it.each([
    "9007199254740993",
    "-9007199254740993",
    "9223372036854775807",
    // JSON.stringify(Number(token)) appears unchanged here, but the actual
    // IEEE-754 integer is 1000000000000000128. Checking stringify is not enough.
    "1000000000000000100",
    // Conversely, this actual integer is representable, but JSON.stringify
    // renders it as 1000000000000000100. Read/export must reject this too.
    "1000000000000000128",
    "1.0000000000000001",
    "0.123456789012345678901",
    "9007199254740993e0",
    "1e400",
    "1e-400",
  ])("rejects the original unsafe token %s", (token) => {
    expect(() =>
      serializer.deserialize(`{"nested":[{"amount":${token}}]}`),
    ).toThrow(errors.DeserializationError);
  });

  it("checks duplicate keys before a safe value can hide an unsafe token", () => {
    expect(() =>
      serializer.deserialize('{"amount":9007199254740993,"amount":1}'),
    ).toThrow(/numeric precision/i);
  });

  it("also protects response metadata and pagination tokens outside _source", () => {
    expect(() =>
      serializer.deserialize(
        '{"_seq_no":9007199254740993,"sort":[9007199254740993],"_source":{}}',
      ),
    ).toThrow(/numeric precision/i);
  });

  it("does not mistake escaped strings or keys for numeric tokens", () => {
    const value = {
      '9007199254740993"': 'escaped "9007199254740993" \\ 1e400',
      number: 0.1,
    };
    expect(serializer.deserialize(JSON.stringify(value))).toEqual(value);
  });

  it("retains SDK malformed JSON and prototype protection behavior", () => {
    expect(() => serializer.deserialize('{"amount":01}')).toThrow(
      errors.DeserializationError,
    );
    const protectedSerializer = new ElasticsearchSerializer({
      enablePrototypePoisoningProtection: true,
    });
    expect(() =>
      protectedSerializer.deserialize('{"__proto__":{"polluted":true}}'),
    ).toThrow(errors.DeserializationError);
    expect(() =>
      protectedSerializer.deserialize(
        '{"constructor":{"prototype":{"polluted":true}}}',
      ),
    ).toThrow(errors.DeserializationError);
  });
});
