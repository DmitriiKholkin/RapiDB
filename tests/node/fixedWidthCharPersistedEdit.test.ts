import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

const baseConfig = {
  id: "fixed-width-char-check",
  name: "Fixed Width Char Check",
  host: "127.0.0.1",
  port: 0,
  database: "db",
  username: "user",
  password: "pass",
};

function buildColumn(
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta {
  return {
    name: "col_char",
    type: nativeType,
    nativeType,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category,
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

describe("fixed-width CHAR persisted edit verification", () => {
  it("treats PostgreSQL char/bpchar values as text categories", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);

    expect(driver.mapTypeCategory("char(10)")).toBe("text");
    expect(driver.mapTypeCategory("bpchar")).toBe("text");
  });

  it("ignores trailing storage padding for PostgreSQL CHAR", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);
    const result = driver.checkPersistedEdit(
      buildColumn("char(10)", "text"),
      "wad 23",
      {
        persistedValue: "wad 23   ",
      },
    );

    expect(result?.ok).toBe(true);
  });

  it("ignores trailing storage padding for MySQL CHAR", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
    } as ConnectionConfig);
    const result = driver.checkPersistedEdit(
      buildColumn("char(10)", "text"),
      "wad 23",
      {
        persistedValue: "wad 23   ",
      },
    );

    expect(result?.ok).toBe(true);
  });

  it("ignores trailing storage padding for MSSQL NCHAR", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);
    const result = driver.checkPersistedEdit(
      buildColumn("nchar(10)", "text"),
      "wad 23",
      {
        persistedValue: "wad 23   ",
      },
    );

    expect(result?.ok).toBe(true);
  });

  it("ignores trailing storage padding for Oracle CHAR", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const result = driver.checkPersistedEdit(
      buildColumn("CHAR(10)", "text"),
      "wad 23",
      {
        persistedValue: "wad 23   ",
      },
    );

    expect(result?.ok).toBe(true);
  });

  it("still reports meaningful mismatches for internal spaces", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);
    const result = driver.checkPersistedEdit(
      buildColumn("char(10)", "text"),
      "wad  23",
      {
        persistedValue: "wad 23   ",
      },
    );

    expect(result?.ok).toBe(false);
    expect(result?.message).toContain('"wad 23   "');
    expect(result?.message).toContain('"wad  23"');
  });
});
