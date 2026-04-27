import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const driver = new MySQLDriver({
  id: "mysql-filter-normalization",
  name: "MySQL Filter Normalization",
  type: "mysql",
  host: "127.0.0.1",
  port: 3306,
  database: "test",
  username: "root",
  password: "root",
} as ConnectionConfig);

function buildColumn(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    defaultValue: undefined,
    isPrimaryKey: false,
    primaryKeyOrdinal: undefined,
    isForeignKey: false,
    isAutoIncrement: false,
    filterable: true,
    filterOperators: ["like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

describe("mysql filter normalization", () => {
  it("preserves exact decimal precision in equality filters", () => {
    const condition = driver.buildFilterCondition(
      buildColumn("amount", "decimal(18,10)", "decimal"),
      "eq",
      "9999999.1234567890",
      1,
    );

    expect(condition).toEqual({
      sql: "`amount` = ?",
      params: ["9999999.1234567890"],
    });
  });

  it("uses structural JSON matching for valid JSON filter input", () => {
    const condition = driver.buildFilterCondition(
      buildColumn("payload", "json", "json"),
      "like",
      '{"arr":[1,2,3],"key":"value","num":42,"bool":true,"nested":{"a":1}}',
      1,
    );

    expect(condition).toEqual({
      sql: "JSON_CONTAINS(`payload`, ?)",
      params: [
        '{"arr":[1,2,3],"key":"value","num":42,"bool":true,"nested":{"a":1}}',
      ],
    });
  });
});
