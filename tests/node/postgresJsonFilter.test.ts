import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const driver = new PostgresDriver({
  id: "postgres-json-filter-test",
  name: "Postgres JSON Filter Test",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
} as ConnectionConfig);

const jsonColumn: ColumnTypeMeta = {
  name: "payload",
  type: "jsonb",
  nativeType: "jsonb",
  category: "json",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

describe("postgres json filter", () => {
  it("uses text contains for valid JSON-like filter input", () => {
    const condition = driver.buildFilterCondition(
      jsonColumn,
      "like",
      '{"key":"value","num":42,"bool":true,"null_val":null}',
      1,
    );

    expect(condition).toEqual({
      sql: 'CAST("payload" AS TEXT) ILIKE $1',
      params: ['%{"key":"value","num":42,"bool":true,"null_val":null}%'],
    });
  });

  it("falls back to text search for non-JSON filter text", () => {
    const condition = driver.buildFilterCondition(
      jsonColumn,
      "like",
      '"key":"value"',
      1,
    );

    expect(condition).toEqual({
      sql: 'CAST("payload" AS TEXT) ILIKE $1',
      params: ['%"key":"value"%'],
    });
  });
});
