import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const floatColumn: ColumnTypeMeta = {
  name: "col_real",
  type: "real",
  nativeType: "real",
  category: "float",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  isAutoIncrement: false,
  filterable: true,
  filterOperators: [
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
    "in",
    "is_null",
    "is_not_null",
  ],
  valueSemantics: "plain",
};

describe("float filter tolerance", () => {
  it("uses tolerant equality for PostgreSQL float filters", () => {
    const driver = new PostgresDriver({
      id: "float-filter-pg",
      name: "Float Filter PG",
      type: "pg",
      host: "127.0.0.1",
      port: 5432,
      database: "postgres",
      username: "postgres",
      password: "postgres",
    } as ConnectionConfig);

    const condition = driver.buildFilterCondition(
      floatColumn,
      "eq",
      "2.71828",
      1,
    );

    expect(condition?.sql).toContain(
      'ABS(("col_real")::double precision - $1::double precision)',
    );
    expect(condition?.sql).toContain(
      "GREATEST($2::double precision, ABS($3::double precision) * $4::double precision)",
    );
    expect(condition?.params).toEqual([2.71828, 1e-7, 2.71828, 1e-7]);
  });

  it("uses tolerant equality for MSSQL real filters", () => {
    const driver = new MSSQLDriver({
      id: "float-filter-mssql",
      name: "Float Filter MSSQL",
      type: "mssql",
      host: "127.0.0.1",
      port: 1433,
      database: "master",
      username: "sa",
      password: "secret",
    } as ConnectionConfig);

    const condition = driver.buildFilterCondition(
      floatColumn,
      "eq",
      "2.71828",
      1,
    );

    expect(condition?.sql).toContain("ABS(CAST([col_real] AS float) - ?)");
    expect(condition?.sql).toContain(
      "CASE WHEN ABS(?) * ? > ? THEN ABS(?) * ? ELSE ? END",
    );
    expect(condition?.params).toEqual([
      2.71828, 2.71828, 1e-7, 1e-7, 2.71828, 1e-7, 1e-7,
    ]);
  });

  it("uses tolerant equality for SQLite REAL filters", () => {
    const driver = new SQLiteDriver({
      id: "float-filter-sqlite",
      name: "Float Filter SQLite",
      type: "sqlite",
      filePath: ":memory:",
    } as ConnectionConfig);

    const condition = driver.buildFilterCondition(
      floatColumn,
      "eq",
      "2.71828",
      1,
    );

    expect(condition?.sql).toContain('ABS(CAST("col_real" AS REAL) - ?)');
    expect(condition?.sql).toContain("MAX(?, ABS(?) * ?)");
    expect(condition?.params).toEqual([2.71828, 1e-7, 2.71828, 1e-7]);
  });
});
