import { createServer } from "node:http";
import { errors } from "@elastic/elasticsearch";
import { describe, expect, it } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { ElasticsearchSerializer } from "../../src/extension/dbDrivers/elasticsearchSerializer";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  isRoundTripJsonNumberToken,
  parseJsonForWrite,
} from "../../src/extension/utils/jsonWriteSafety";
import { elasticsearchKeys } from "../support/elasticsearchIdentity";

type Reply = { status?: number; body: unknown };

async function fixture(reply: (method: string, path: string) => Reply) {
  const requests: Array<{ method?: string; path: string }> = [];
  const server = createServer(async (request, response) => {
    for await (const _chunk of request);
    const path = request.url ?? "";
    requests.push({ method: request.method, path });
    response.setHeader("x-elastic-product", "Elasticsearch");
    response.setHeader("content-type", "application/json");
    if (request.method === "HEAD") {
      response.end();
      return;
    }
    const { status, body } = reply(request.method ?? "", path);
    response.statusCode = status ?? 200;
    response.end(typeof body === "string" ? body : JSON.stringify(body));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing port");
  const driver = new ElasticsearchDriver({
    id: "es-fidelity",
    name: "ES fidelity",
    type: "elasticsearch",
    endpoint: `http://127.0.0.1:${address.port}`,
  });
  await driver.connect();
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

function plainRows(result: { columns: string[]; rows: unknown[] }) {
  return (result.rows as Record<string, unknown>[]).map((row) =>
    Object.fromEntries(
      result.columns.map((column, index) => [column, row[`__col_${index}`]]),
    ),
  );
}

const sourceColumn = {
  name: "_source",
  nativeType: "object",
  category: "json",
} as ColumnTypeMeta;

describe("ES-1: large decimal floats that round-trip losslessly", () => {
  const serializer = new ElasticsearchSerializer();

  it.each([
    "1e23",
    "6.02214076e23",
    "3.4028235E38",
    "-1.7976931348623157e308",
    "1.5e300",
  ])("reads and writes %s", (token) => {
    const text = `{"nested":[{"value":${token}}]}`;
    expect(serializer.deserialize(text)).toEqual(JSON.parse(text));
    expect(parseJsonForWrite(text)).toEqual(JSON.parse(text));
    const driver = new ElasticsearchDriver({
      id: "es",
      name: "es",
      type: "elasticsearch",
    });
    expect(driver.coerceInputValue(text, sourceColumn)).toEqual(
      JSON.parse(text),
    );
    expect(driver.checkPersistedEdit(sourceColumn, text)).toBeNull();
  });

  it.each([
    "100000000000000000000000",
    "1000000000000000100.0",
    "9223372036854775807",
    "1.00000000000000000001e23",
    "1e400",
  ])("still rejects lossy token %s on reads and writes", (token) => {
    const text = `{"value":${token}}`;
    expect(() => serializer.deserialize(text)).toThrow(
      errors.DeserializationError,
    );
    expect(() => parseJsonForWrite(text)).toThrow(/numeric precision/);
  });

  it.each([
    "1000000000000000128",
    "1152921504606846976",
  ])("rejects exact-binary integer %s that JSON.stringify would rewrite", (token) => {
    const text = `{"value":${token}}`;
    expect(() => serializer.deserialize(text)).toThrow(
      errors.DeserializationError,
    );
    expect(JSON.stringify(JSON.parse(text))).not.toContain(token);
    expect(() => parseJsonForWrite(text, isRoundTripJsonNumberToken)).toThrow(
      /numeric precision/,
    );
    const driver = new ElasticsearchDriver({
      id: "es",
      name: "es",
      type: "elasticsearch",
    });
    expect(() => driver.coerceInputValue(text, sourceColumn)).toThrow(
      /numeric precision/,
    );
    expect(driver.checkPersistedEdit(sourceColumn, text)).toMatchObject({
      ok: false,
    });
  });
});

describe("ES-3: server-generated metadata numbers", () => {
  const serializer = new ElasticsearchSerializer();

  it("keeps lossy sort and aggregation values as exact strings", () => {
    type Hit = { _id?: string; _source?: unknown; sort: unknown[] };
    const parsed = serializer.deserialize<{
      hits: { hits: Hit[] };
      aggregations: {
        max_price: { value: unknown };
        sum: { value: unknown };
        top: { hits: { hits: Hit[] } };
      };
    }>(
      '{"hits":{"hits":[{"_id":"a","_source":{"sort":1},"sort":[9223372036854775807,"x",1.5]}]},"aggregations":{"max_price":{"value":9.223372036854776E18},"sum":{"value":0.1},"top":{"hits":{"hits":[{"sort":[-9223372036854775808]}]}}}}',
    );
    expect(parsed.hits.hits[0]).toEqual({
      _id: "a",
      _source: { sort: 1 },
      sort: ["9223372036854775807", "x", 1.5],
    });
    expect(parsed.aggregations.max_price.value).toBe("9.223372036854776E18");
    expect(parsed.aggregations.sum.value).toBe(0.1);
    expect(parsed.aggregations.top.hits.hits[0].sort).toEqual([
      "-9223372036854775808",
    ]);
  });

  it("keeps lossy inner_hits sort values as exact strings", () => {
    const parsed = serializer.deserialize<{
      hits: {
        hits: Array<{
          inner_hits: { c: { hits: { hits: Array<{ sort: unknown[] }> } } };
        }>;
      };
    }>(
      '{"hits":{"hits":[{"_id":"a","inner_hits":{"c":{"hits":{"hits":[{"sort":[9223372036854775807]}]}}}}]}}',
    );
    expect(parsed.hits.hits[0].inner_hits.c.hits.hits[0].sort).toEqual([
      "9223372036854775807",
    ]);
  });

  it.each([
    '{"hits":{"hits":[{"_source":{"sort":[9007199254740993]}}]}}',
    '{"hits":{"hits":[{"_source":{"aggregations":9007199254740993}}]}}',
    '{"aggregations":{"top":{"hits":{"hits":[{"_source":{"v":9007199254740993}}]}}}}',
    '{"hits":{"hits":[{"_seq_no":9007199254740993,"sort":[1]}]}}',
    '{"hits":{"hits":[{"fields":{"v":[9007199254740993]}}]}}',
    '{"sort":[{"\\u005fsource":{"v":9007199254740993}}]}',
    '{"hits":{"hits":[{"fields":{"sort":[9007199254740993]}}]}}',
    '{"hits":{"hits":[{"fields":{"aggregations":[9007199254740993]}}]}}',
    '{"sort":[9007199254740993]}',
    '{"hits":{"sort":[9007199254740993]}}',
    '{"hits":{"hits":[{"inner_hits":{"x":{"aggregations":{"v":9007199254740993}}}}]}}',
    '{"aggregations":{"top":{"hits":{"hits":[{"_source":{"sort":[9007199254740993]}}]}}}}',
  ])("stays strict for document data and other metadata: %s", (json) => {
    expect(() => serializer.deserialize(json)).toThrow(/numeric precision/i);
  });

  it("returns a search whose sort values exceed 2^53 instead of failing the query", async () => {
    const f = await fixture(() => ({
      body: '{"timed_out":false,"_shards":{"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_index":"records","_id":"a","_source":{"price":1},"sort":[9223372036854775807]}]},"aggregations":{"max":{"value":9.223372036854776E18}}}',
    }));
    try {
      const result = await f.driver.query(
        'POST /records/_search\n{"sort":[{"price":"asc"}],"aggs":{"max":{"max":{"field":"price"}}}}',
      );
      expect(result.resultSets?.map((set) => set.id)).toEqual([
        "summary",
        "hits",
      ]);
      expect(plainRows(result.resultSets?.[1] ?? result)).toEqual([
        { _id: "a", _source: '{"price":1}' },
      ]);
      expect(JSON.stringify(plainRows(result))).toContain(
        "9.223372036854776E18",
      );
    } finally {
      await f.close();
    }
  });
});

describe("ES-2: conditional _source update script", () => {
  it("compares mixed and large numbers exactly instead of a 2^53 cutoff", () => {
    const driver = new ElasticsearchDriver({
      id: "es",
      name: "es",
      type: "elasticsearch",
    });
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "default",
      "indices",
      "records",
      {
        primaryKeys: elasticsearchKeys(driver, "doc-1", "records"),
        changes: { _source: '{"v":2.5e16,"w":1e23,"edited":true}' },
        originalValues: { _source: '{"v":25000000000000000,"w":1e23}' },
      },
    );
    const script = JSON.parse(preview.slice(preview.indexOf("\n") + 1))
      .script as { source: string; params: { original: unknown } };
    expect(script.source).not.toMatch(/9007199254740991/);
    expect(script.source).toContain(
      "if (af && bf) return ((Number)a).doubleValue() == ((Number)b).doubleValue();",
    );
    expect(script.source).toContain(
      "while (d >= 9007199254740992.0 || d <= -9007199254740992.0) { d /= 2; k++; }",
    );
    expect(script.source).toContain(
      "BigDecimal exact = new BigDecimal('' + m).multiply(new BigDecimal('2').pow(k));",
    );
    expect(script.source).not.toMatch(/for \(int i = 0; i < k;/);
    expect(script.source).toContain(
      "return exact.compareTo(new BigDecimal(n.toString())) == 0;",
    );
    expect(script.source).not.toMatch(/instanceof Short|instanceof Byte/);
    expect(script.params.original).toEqual({ v: 25000000000000000, w: 1e23 });
    expect(preview).toContain('"v": 25000000000000000');
    expect(preview).toContain('"w": 1e+23');
  });
});

describe("ES-4: search hits with fields and highlight", () => {
  it("includes fields and highlight as JSON columns when hits carry them", async () => {
    const f = await fixture(() => ({
      body: {
        timed_out: false,
        _shards: { failed: 0 },
        hits: {
          hits: [
            {
              _index: "records",
              _id: "a",
              fields: { a: [1], "b.c": ["x"] },
              highlight: { title: ["<em>x</em>"] },
            },
            { _index: "records", _id: "b", fields: { a: [2] } },
          ],
        },
      },
    }));
    try {
      const result = await f.driver.query(
        'POST /records/_search\n{"_source":false,"fields":["a","b.c"],"highlight":{"fields":{"title":{}}}}',
      );
      expect(result.columns).toEqual(["_id", "_source", "fields", "highlight"]);
      expect(plainRows(result)).toEqual([
        {
          _id: "a",
          _source: "{}",
          fields: '{"a":[1],"b.c":["x"]}',
          highlight: '{"title":["<em>x</em>"]}',
        },
        {
          _id: "b",
          _source: "{}",
          fields: '{"a":[2]}',
          highlight: undefined,
        },
      ]);
    } finally {
      await f.close();
    }
  });

  it("orders columns _id, _index, _source, fields, highlight for multi-index searches", async () => {
    const f = await fixture(() => ({
      body: {
        timed_out: false,
        _shards: { failed: 0 },
        hits: {
          hits: [
            {
              _index: "records-a",
              _id: "a",
              _source: { x: 1 },
              fields: { a: [1] },
              highlight: { title: ["<em>x</em>"] },
            },
            { _index: "records-b", _id: "b", _source: { x: 2 } },
          ],
        },
      },
    }));
    try {
      const result = await f.driver.query(
        'POST /records-*/_search\n{"fields":["a"],"highlight":{"fields":{"title":{}}}}',
      );
      expect(result.columns).toEqual([
        "_id",
        "_index",
        "_source",
        "fields",
        "highlight",
      ]);
      expect(plainRows(result)).toEqual([
        {
          _id: "a",
          _index: "records-a",
          _source: '{"x":1}',
          fields: '{"a":[1]}',
          highlight: '{"title":["<em>x</em>"]}',
        },
        {
          _id: "b",
          _index: "records-b",
          _source: '{"x":2}',
          fields: undefined,
          highlight: undefined,
        },
      ]);
    } finally {
      await f.close();
    }
  });

  it("keeps the plain hit shape when hits have no fields or highlight", async () => {
    const f = await fixture(() => ({
      body: {
        timed_out: false,
        _shards: { failed: 0 },
        hits: { hits: [{ _index: "records", _id: "a", _source: { x: 1 } }] },
      },
    }));
    try {
      const result = await f.driver.query("GET /records/_search");
      expect(result.columns).toEqual(["_id", "_source"]);
    } finally {
      await f.close();
    }
  });
});

describe("ES-5: multi-statement partial completion", () => {
  const missing = {
    status: 404,
    body: {
      error: { type: "index_not_found_exception", reason: "no such index" },
      status: 404,
    },
  };

  it("reports earlier applied writes when a later statement fails", async () => {
    const f = await fixture((method) =>
      method === "PUT"
        ? { body: { result: "created", _index: "a", _id: "1" } }
        : missing,
    );
    try {
      const result = await f.driver.query(
        'PUT /a/_doc/1\n{"x":1}\nDELETE /missing\nPUT /a/_doc/2\n{"x":2}',
      );
      expect(result.error).toMatch(
        /^Elasticsearch statement 2 of 3 failed after 1 of 3 statements was completed and earlier writes were applied\. It was not applied and the remaining statements were not sent\. .*index_not_found/,
      );
      expect(result.affectedRows).toBe(1);
      const results = JSON.parse(
        String((result.rows[0] as Record<string, unknown>).__col_0),
      );
      expect(results).toHaveLength(1);
      expect(results[0].statement).toContain("PUT /a/_doc/1");
      expect(
        f.requests.filter((r) => r.method !== "HEAD").map((r) => r.method),
      ).toEqual(["PUT", "DELETE"]);
    } finally {
      await f.close();
    }
  });

  it("marks the outcome unknown for non-4xx failures", async () => {
    const f = await fixture((method) =>
      method === "PUT"
        ? { body: { result: "created", _index: "a", _id: "1" } }
        : { status: 500, body: { error: "boom", status: 500 } },
    );
    try {
      const result = await f.driver.query(
        'PUT /a/_doc/1\n{"x":1}\nDELETE /a/_doc/2',
      );
      expect(result.error).toMatch(
        /statement 2 of 2 failed after 1 of 2 statements was completed and earlier writes were applied\. Its outcome is unknown/,
      );
    } finally {
      await f.close();
    }
  });

  it("still throws when no earlier statement wrote anything", async () => {
    const f = await fixture((method) =>
      method === "GET"
        ? {
            body: {
              timed_out: false,
              _shards: { failed: 0 },
              hits: { hits: [] },
            },
          }
        : missing,
    );
    try {
      await expect(
        f.driver.query("GET /a/_search\nDELETE /missing"),
      ).rejects.toMatchObject({ name: "ResponseError" });
    } finally {
      await f.close();
    }
  });
});
