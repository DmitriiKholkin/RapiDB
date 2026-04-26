import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const datetimeOffsetColumn: ColumnTypeMeta = {
  name: "created_at",
  type: "datetimeoffset",
  nativeType: "datetimeoffset",
  category: "datetime",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  isAutoIncrement: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

const varcharMaxColumn: ColumnTypeMeta = {
  name: "payload",
  type: "varchar(max)",
  nativeType: "varchar(max)",
  category: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  isAutoIncrement: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

function makeDriver(): MSSQLDriver {
  return new MSSQLDriver({
    id: "mssql-datetimeoffset-filter",
    name: "MSSQL DatetimeOffset Filter",
    type: "mssql",
    host: "127.0.0.1",
    port: 1433,
    database: "master",
    username: "sa",
    password: "secret",
  } as ConnectionConfig);
}

describe("MSSQL datetimeoffset filter", () => {
  it("matches display-formatted datetimeoffset with timezone", () => {
    const driver = makeDriver();

    const condition = driver.buildFilterCondition(
      datetimeOffsetColumn,
      "like",
      "2024-06-15 11:30:00.123 +00:00",
      1,
    );

    expect(condition).toEqual({
      sql: "REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(40), [created_at], 127), 'Z', '+00:00'), ' +', '+'), ' -', '-') LIKE ?",
      params: ["%2024-06-15T11:30:00.123%+00:00%"],
    });
  });

  it("accepts copied value wrapped in quotes", () => {
    const driver = makeDriver();

    const condition = driver.buildFilterCondition(
      datetimeOffsetColumn,
      "like",
      '"2024-06-15 11:30:00.123 +00:00"',
      1,
    );

    expect(condition).toEqual({
      sql: "REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(40), [created_at], 127), 'Z', '+00:00'), ' +', '+'), ' -', '-') LIKE ?",
      params: ["%2024-06-15T11:30:00.123%+00:00%"],
    });
  });

  it("uses CHARINDEX with NVARCHAR(MAX) for long varchar(max) filters", () => {
    const driver = makeDriver();
    const longText = "x".repeat(3000);

    const condition = driver.buildFilterCondition(
      varcharMaxColumn,
      "like",
      longText,
      1,
    );

    expect(condition).toEqual({
      sql: "CHARINDEX(CAST(? AS NVARCHAR(MAX)), CAST([payload] AS NVARCHAR(MAX))) > 0",
      params: [longText],
    });
  });

  it("falls back to exact NVARCHAR(MAX) comparison for oversized text filters", () => {
    const driver = makeDriver();
    const hugeText = "x".repeat(12000);

    const condition = driver.buildFilterCondition(
      varcharMaxColumn,
      "like",
      hugeText,
      1,
    );

    expect(condition).toEqual({
      sql: "CAST([payload] AS NVARCHAR(MAX)) = CAST(? AS NVARCHAR(MAX))",
      params: [hugeText],
    });
  });
});
