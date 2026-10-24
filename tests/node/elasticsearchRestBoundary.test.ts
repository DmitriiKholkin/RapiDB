import { createServer } from "node:http";
import { describe, expect, it } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { ELASTICSEARCH_READ_BUDGET } from "../../src/shared/safetyContracts";

async function fixture() {
  const requests: Array<{ method?: string; url: URL; body: string }> = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const url = new URL(request.url ?? "/", "http://localhost");
    requests.push({
      method: request.method,
      url,
      body: Buffer.concat(chunks).toString(),
    });
    response.setHeader("x-elastic-product", "Elasticsearch");
    response.setHeader("content-type", "application/json");
    response.end(
      request.method === "HEAD"
        ? undefined
        : JSON.stringify({
            acknowledged: true,
            result: "updated",
            _index: "records",
            _id: "selected",
            hits: { hits: [], total: { value: 0, relation: "eq" } },
          }),
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing port");
  const driver = new ElasticsearchDriver({
    id: "rest-boundary",
    name: "REST boundary",
    type: "elasticsearch",
    endpoint: `http://127.0.0.1:${address.port}`,
  });
  return {
    driver,
    requests,
    async close() {
      await driver.disconnect();
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

describe("Elasticsearch REST request boundaries (real SDK + HTTP)", () => {
  it.each([
    "/records/_doc/..",
    "/records/_doc/%2e%2e",
    "/records/_doc/.%2E",
    "/records/_doc/%2e.",
    "/records/_doc/.",
    "/records/_doc\\..",
    "/records/_doc/selected#ignored",
    "https://other-cluster/records/_doc/selected",
    "//other-cluster/records/_doc/selected",
  ])("rejects paths that can silently change the selected resource: %s", async (path) => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await expect(
        f.driver.query(`DELETE /records/_doc/first\nDELETE ${path}`),
      ).rejects.toThrow(/path/i);
      expect(f.requests.filter((entry) => entry.method !== "HEAD")).toEqual([]);
    } finally {
      await f.close();
    }
  });

  it("preserves encoded document ID delimiters without treating them as URL syntax", async () => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await f.driver.query("DELETE /records/_doc/id%2Fpart%23tag%3Fvalue");
      const request = f.requests.find((entry) => entry.method === "DELETE");
      expect(request?.url.pathname).toBe(
        "/records/_doc/id%2Fpart%23tag%3Fvalue",
      );
    } finally {
      await f.close();
    }
  });

  it.each([
    "_mapping",
    "_settings",
  ])("reads index %s using the named REST resource", async (endpoint) => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await f.driver.query(`GET /records/${endpoint}?index=production`);
      const request = f.requests.find((entry) => entry.method === "GET");
      expect(request?.url.pathname).toBe(`/records/${endpoint}`);
      expect(request?.url.searchParams.get("index")).toBe("production");
    } finally {
      await f.close();
    }
  });
  it.each([
    ["DELETE /records?index=production", "/records"],
    [
      "DELETE /records/_doc/selected?index=production&id=other",
      "/records/_doc/selected",
    ],
    [
      'POST /records/_update/selected\n{"index":"production","id":"other","doc":{"active":false}}',
      "/records/_update/selected",
    ],
    [
      'PUT /records\n{"index":"production","settings":{"number_of_shards":1}}',
      "/records",
    ],
    [
      'PUT /records/_settings\n{"index":{"number_of_replicas":0}}',
      "/records/_settings",
    ],
    [
      'PUT /records/_mapping\n{"index":"production","properties":{}}',
      "/records/_mapping",
    ],
    [
      'POST /records/_search\n{"index":"production","query":{"match_all":{}}}',
      "/records/_search",
    ],
  ])("never lets query/body fields retarget %s", async (command, pathname) => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await f.driver.query(command);
      const request = f.requests.find((entry) => entry.method !== "HEAD");
      expect(request?.url.pathname).toBe(pathname);
      if (command.includes("\n")) {
        const input = JSON.parse(command.slice(command.indexOf("\n") + 1));
        expect(JSON.parse(request?.body ?? "")).toMatchObject(input);
      } else {
        expect(request?.url.searchParams.get("index")).toBe("production");
      }
    } finally {
      await f.close();
    }
  });

  it("keeps query options out of the document and preserves exact string values", async () => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await f.driver.query(
        'PUT /records/_doc/selected?routing=00042&pipeline=001&if_seq_no=9007199254740993&if_primary_term=1\n{"body":{"value":1},"index":"document-field"}',
      );
      const request = f.requests.find((entry) => entry.method === "PUT");
      expect(request?.url.pathname).toBe("/records/_doc/selected");
      expect(request?.url.searchParams.get("routing")).toBe("00042");
      expect(request?.url.searchParams.get("pipeline")).toBe("001");
      expect(request?.url.searchParams.get("if_seq_no")).toBe(
        "9007199254740993",
      );
      expect(JSON.parse(request?.body ?? "")).toEqual({
        body: { value: 1 },
        index: "document-field",
      });
    } finally {
      await f.close();
    }
  });

  it.each([
    ["POST /records/_search?size=25000", {}, 10000],
    ["POST /records/_search?size=2", {}, 2],
    ["POST /records/_search", { size: 25000 }, undefined],
    ["POST /_search?size=25000", { size: 25000 }, 10000],
  ])("retains search size checks across query/body boundaries: %s", async (path, body, querySize) => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await f.driver.query(`${path}\n${JSON.stringify(body)}`);
      const request = f.requests.find((entry) => entry.method !== "HEAD");
      expect(request?.url.searchParams.get("size")).toBe(
        querySize === undefined ? null : String(querySize),
      );
      expect(JSON.parse(request?.body ?? "").size).toBe(
        "size" in body ? ELASTICSEARCH_READ_BUDGET.hardCap : undefined,
      );
    } finally {
      await f.close();
    }
  });

  it("does not expose an unchecked alternate search body through source", async () => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await expect(
        f.driver.query(
          `GET /records/_search?source=${encodeURIComponent('{"size":1000000}')}&source_content_type=application/json`,
        ),
      ).rejects.toThrow(/source query parameter/i);
      expect(f.requests.filter((entry) => entry.method !== "HEAD")).toEqual([]);
    } finally {
      await f.close();
    }
  });

  it.each([
    "DELETE /records",
    "DELETE /records/_doc/selected",
    "GET /records/_doc/selected",
    "GET /records/_mapping",
    "GET /records/_settings",
  ])("rejects an ignored body instead of executing %s with different semantics", async (path) => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await expect(
        f.driver.query(`${path}\n{"query":{"term":{"expired":true}}}`),
      ).rejects.toThrow(/does not accept.*body/i);
      expect(f.requests.filter((entry) => entry.method !== "HEAD")).toEqual([]);
    } finally {
      await f.close();
    }
  });

  it.each([
    ["GET /records/_unsupported", /unsupported/i],
    ["POST /records/_update/selected\n[]", /JSON object body/i],
    ["PUT /other", /JSON object body/i],
    ["DELETE /other\n{}", /does not accept.*body/i],
    ["GET /records/_search?source=%7B%7D", /source query parameter/i],
  ])("validates the entire script before the first mutation: %s", async (invalid, reason) => {
    const f = await fixture();
    try {
      await f.driver.connect();
      await expect(
        f.driver.query(`DELETE /records/_doc/selected\n${invalid}`),
      ).rejects.toThrow(reason);
      expect(f.requests.filter((entry) => entry.method !== "HEAD")).toEqual([]);
    } finally {
      await f.close();
    }
  });
});
