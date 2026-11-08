import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { ElasticsearchSerializer } from "../../src/extension/dbDrivers/elasticsearchSerializer";
import {
  applyFilters,
  applySort,
  flattenRootRecord,
  inferColumnsFromRows,
} from "../../src/extension/dbDrivers/nosqlUtils";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";

describe("nosqlUtils reads only own row fields", () => {
  const rows: Record<string, unknown>[] = [
    { id: 1 },
    { id: 2, constructor: "set" },
  ];

  it.each([
    "constructor",
    "toString",
    "valueOf",
    "__proto__",
  ])("treats a missing %s field as null", (column) => {
    expect(
      applyFilters(rows, [{ column, operator: "is_null" }]).map(
        (row) => row.id,
      ),
    ).toEqual(column === "constructor" ? [1] : [1, 2]);
    expect(
      applyFilters(rows, [{ column, operator: "is_not_null" }]).map(
        (row) => row.id,
      ),
    ).toEqual(column === "constructor" ? [2] : []);
  });

  it("does not match scalar filters against inherited members", () => {
    expect(
      applyFilters(
        [{ id: 1 }],
        [{ column: "toString", operator: "like", value: "native" }],
      ),
    ).toEqual([]);
  });

  it("sorts missing inherited-name fields as nulls", () => {
    expect(
      applySort([{ id: 1 }, { id: 2, valueOf: "b" }, { id: 3, valueOf: "a" }], {
        column: "valueOf",
        direction: "asc",
      }).map((row) => row.id),
    ).toEqual([1, 3, 2]);
  });

  it("infers nullability without inherited members", () => {
    const [, constructorColumn] = inferColumnsFromRows(
      [{ id: 1, constructor: "x" }, { id: 2 }],
      "id",
      { consistentCategories: true },
    );
    expect(constructorColumn).toMatchObject({
      name: "constructor",
      category: "text",
      nullable: true,
    });
  });
});

describe("Elasticsearch negative zero", () => {
  const driver = new ElasticsearchDriver({
    id: "es-g5",
    name: "ES G5",
    type: "elasticsearch",
    host: "localhost",
  });
  const sourceColumn = { name: "_source" } as ColumnTypeMeta;

  it("keeps -0 in the _source cell", () => {
    const rows = (
      driver as unknown as {
        hitsToRows: (hits: unknown[]) => Array<Record<string, unknown>>;
      }
    ).hitsToRows([{ _id: "1", _source: { a: -0, b: [-0, 0], c: { d: -0 } } }]);
    expect(rows[0]?._source).toBe('{"a":-0.0,"b":[-0.0,0],"c":{"d":-0.0}}');
  });

  it("writes -0 back after editing the _source cell", () => {
    const parsed = driver.coerceInputValue(
      '{"a":-0.0,"label":"-0.0","b":-0}',
      sourceColumn,
    ) as Record<string, unknown>;
    expect(Object.is(parsed.a, -0)).toBe(true);
    expect(Object.is(parsed.b, -0)).toBe(true);
    const serializer = new ElasticsearchSerializer();
    const json = serializer.serialize({ doc: parsed });
    expect(json).toBe('{"doc":{"a":-0.0,"label":"-0.0","b":-0.0}}');
    const roundTrip = serializer.deserialize<{
      doc: Record<string, unknown>;
    }>(json);
    expect(Object.is(roundTrip.doc.a, -0)).toBe(true);
    expect(serializer.serialize({ a: 0, b: "x" })).toBe('{"a":0,"b":"x"}');
  });

  it("keeps nested -0 in query panel results", async () => {
    const queryDriver = new ElasticsearchDriver({
      id: "es-g5-query",
      name: "ES G5 query",
      type: "elasticsearch",
      host: "localhost",
    });
    const search = vi.fn().mockResolvedValue({
      hits: { total: { value: 0, relation: "eq" }, hits: [] },
      aggregations: { min_delta: { value: -0 } },
    });
    const get = vi.fn().mockResolvedValue({
      _id: "1",
      _source: { nested: { delta: -0 }, flat: 1 },
    });
    Object.assign(queryDriver, { client: { search, get }, connected: true });

    const searchResult = await queryDriver.query(
      'POST /notes/_search\n{"size":0,"aggs":{"min_delta":{"min":{"field":"delta"}}}}',
    );
    expect(searchResult.rows[0]?.__col_1).toBe('{"min_delta":{"value":-0.0}}');

    const getResult = await queryDriver.query("GET /notes/_doc/1");
    const nestedIndex = getResult.columns.indexOf("nested");
    expect(getResult.rows[0]?.[`__col_${nestedIndex}`]).toBe('{"delta":-0.0}');
  });

  it("keeps plain JSON stringification for other nosql drivers", () => {
    expect(flattenRootRecord({ nested: { delta: -0 } })).toEqual({
      nested: '{"delta":0}',
    });
  });

  it("keeps -0 in REST mutation previews", () => {
    const preview = driver.buildMutationPreviewStatement(
      "insert",
      "",
      "",
      "notes",
      { values: { _id: "1", _source: '{"a":-0.0}' } },
    );
    expect(preview).toContain("-0.0");
  });
});
