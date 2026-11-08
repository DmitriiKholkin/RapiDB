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
  it("exports array result columns as JSON arrays", async () => {
    const { formatted, exported } = await queryWireRow(
      [
        field("jsonb_values", 3807),
        field("json_values", 199),
        field("text_values", 1009),
        field("int_values", 1007),
        field("numeric_values", 1231),
        field("uuid_values", 2951),
      ],
      [
        String.raw`{"{\"a\": 1}","{\"x\": 1, \"y\": 2}"}`,
        String.raw`{"{\"n\": 12345678901234567890}",NULL}`,
        String.raw`{a,123,"{\"a\":1}"}`,
        "{{1,2},{3,4}}",
        "{1.50,-2}",
        "{00000000-0000-4000-8000-000000000001}",
      ],
    );

    expect(formatted.columnMeta).toEqual([
      { category: "array", nativeType: "jsonb[]" },
      { category: "array", nativeType: "json[]" },
      { category: "array", nativeType: "text[]" },
      { category: "array", nativeType: "integer[]" },
      { category: "array", nativeType: "numeric[]" },
      { category: "array", nativeType: "uuid[]" },
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
      numeric_values: [1.5, -2],
      uuid_values: ["00000000-0000-4000-8000-000000000001"],
    });
    expect(exported).toContain(
      '"json_values":[{"n": 12345678901234567890},null]',
    );
    expect(exported).toContain('"numeric_values":[1.50,-2]');
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
});
