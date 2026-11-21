import { describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { BoundedPostgresQuery } from "../../src/extension/dbDrivers/postgresBoundedQuery";
import {
  buildQueryExportColumns,
  serializeJsonExportRecord,
} from "../../src/extension/utils/exportValueFormat";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";

type WireQuery = BoundedPostgresQuery & {
  handleDataRow(message: { fields: Array<string | null> }): void;
  handleCommandComplete(message: { text: string }, connection: object): void;
  handleReadyForQuery(connection: object): void;
};

const field = (name: string, dataTypeID: number) => ({
  name,
  tableID: 0,
  columnID: 0,
  dataTypeID,
  dataTypeSize: -1,
  dataTypeModifier: -1,
  format: "text",
});

async function queryWireRow(
  fields: ReturnType<typeof field>[],
  values: Array<string | null>,
) {
  const client = {
    query: vi.fn((query: WireQuery | string) => {
      if (typeof query === "string") return Promise.resolve({ rows: [] });
      queueMicrotask(() => {
        query.handleRowDescription({ fields });
        query.handleDataRow({ fields: values });
        query.handleCommandComplete({ text: "SELECT 1" }, {});
        query.handleReadyForQuery({});
      });
      return query;
    }),
    release: vi.fn(),
  };
  const driver = new PostgresDriver({
    id: "pg-query-export",
    name: "PG",
    type: "pg",
    host: "localhost",
  });
  (driver as unknown as { pool: unknown }).pool = {
    connect: vi.fn(async () => client),
    totalCount: 0,
    idleCount: 1,
    waitingCount: 0,
  };
  const formatted = formatQueryResult(
    await driver.query("SELECT typed values", undefined, { hardCap: 10 }),
    100,
  );
  const exported = serializeJsonExportRecord(
    buildQueryExportColumns(formatted.columns, formatted.columnMeta).map(
      (column) => ({ ...column, value: formatted.rows[0][column.sourceKey] }),
    ),
  );
  return { formatted, exported };
}

describe("PostgreSQL query results through formatting and JSON export", () => {
  it("formats bytea once and preserves the typed binary export", async () => {
    const { formatted, exported } = await queryWireRow(
      [field("bytes", 17)],
      ["\\x00ffaa"],
    );
    expect(formatted.rows).toBe(formatted.resultSets?.[0].rows);
    expect(formatted.columnMeta).toBe(formatted.resultSets?.[0].columnMeta);
    expect(formatted.rows).toEqual([{ __col_0: "0x00ffaa" }]);
    expect(JSON.parse(exported)).toEqual({ bytes: "0x00ffaa" });
  });

  it("exports array result columns as JSON arrays", async () => {
    const { formatted, exported } = await queryWireRow(
      [
        field("jsonb_values", 3807),
        field("json_values", 199),
        field("text_values", 1009),
        field("int_values", 1007),
        field("numeric_values", 1231),
        field("uuid_values", 2951),
        field("bigint_values", 1016),
      ],
      [
        String.raw`{"{\"a\": 1}","{\"x\": 1, \"y\": 2}"}`,
        String.raw`{"{\"n\": 12345678901234567890}",NULL}`,
        String.raw`{a,123,"{\"a\":1}"}`,
        "{{1,2},{3,4}}",
        "{{1.50,12345678901234567890.1234567890}}",
        "{00000000-0000-4000-8000-000000000001}",
        "{{9223372036854775807,NULL}}",
      ],
    );

    expect(formatted.columnMeta).toEqual([
      { category: "array", nativeType: "jsonb[]" },
      { category: "array", nativeType: "json[]" },
      { category: "array", nativeType: "text[]" },
      { category: "array", nativeType: "integer[]" },
      { category: "array", nativeType: "numeric[]" },
      { category: "array", nativeType: "uuid[]" },
      { category: "array", nativeType: "bigint[]" },
    ]);
    expect(formatted.rows[0].__col_2).toBe(String.raw`["a","123","{\"a\":1}"]`);
    expect(JSON.parse(exported)).toEqual({
      jsonb_values: [{ a: 1 }, { x: 1, y: 2 }],
      json_values: [{ n: expect.any(Number) }, null],
      text_values: ["a", "123", '{"a":1}'],
      int_values: [
        [1, 2],
        [3, 4],
      ],
      numeric_values: [[1.5, expect.any(Number)]],
      uuid_values: ["00000000-0000-4000-8000-000000000001"],
      bigint_values: [[expect.any(Number), null]],
    });
    expect(exported).toContain(
      '"json_values":[{"n": 12345678901234567890},null]',
    );
    expect(exported).toContain(
      '"numeric_values":[[1.50,12345678901234567890.1234567890]]',
    );
    expect(exported).toContain('"bigint_values":[[9223372036854775807,null]]');
  });

  it("leaves an unregistered array OID as wire text", async () => {
    const value = '{"123","00123"}';
    const { formatted, exported } = await queryWireRow(
      [field("custom_values", 65001)],
      [value],
    );

    expect(formatted.columnMeta).toEqual([{ category: null }]);
    expect(formatted.rows).toEqual([{ __col_0: value }]);
    expect(JSON.parse(exported)).toEqual({ custom_values: value });
  });

  it("formats geometric and interval columns from their type metadata", async () => {
    const { formatted, exported } = await queryWireRow(
      [field("p", 600), field("c", 718), field("i", 1186), field("doc", 3802)],
      [
        "(1,2)",
        "<(1,2),3>",
        "1 year 2 mons 3 days 04:05:06.7",
        '{"x":1,"y":2}',
      ],
    );

    expect(formatted.columnMeta).toEqual([
      { category: "spatial" },
      { category: "spatial" },
      { category: "interval" },
      { category: "json" },
    ]);
    expect(formatted.rows[0]).toEqual({
      __col_0: "(1, 2)",
      __col_1: "<(1,2),3>",
      __col_2: "P1Y2M3DT4H5M6.7S",
      __col_3: '{"x":1,"y":2}',
    });
    expect(JSON.parse(exported).doc).toEqual({ x: 1, y: 2 });
  });

  it("preserves the PostgreSQL ISO interval supplied by the server", async () => {
    const { formatted, exported } = await queryWireRow(
      [field("duration", 1186)],
      ["P1DT-2.000001S"],
    );
    expect(formatted.rows).toEqual([{ __col_0: "P1DT-2.000001S" }]);
    expect(JSON.parse(exported)).toEqual({ duration: "P1DT-2.000001S" });
  });

  it.each([
    "@ 1 day 2 hours ago",
    "-1-2",
    "+2 03:04:05.123456",
  ])("preserves an interval the stock PostgreSQL decoder cannot parse: %s", async (interval) => {
    const { formatted } = await queryWireRow(
      [field("duration", 1186)],
      [interval],
    );
    expect(formatted.rows).toEqual([{ __col_0: interval }]);
  });
});
