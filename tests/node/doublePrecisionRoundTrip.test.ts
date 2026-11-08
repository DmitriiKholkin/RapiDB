import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

const column = (nativeType: string): ColumnTypeMeta => ({
  name: "value",
  type: nativeType,
  nativeType,
  category: "float",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
});

const postgres = new PostgresDriver({
  id: "double-round-trip-pg",
  name: "double-round-trip-pg",
  type: "pg",
});
const sqlServer = new MSSQLDriver({
  id: "double-round-trip-mssql",
  name: "double-round-trip-mssql",
  type: "mssql",
});

const cases = [
  { driver: postgres, nativeType: "double precision" },
  { driver: postgres, nativeType: "float8" },
  { driver: sqlServer, nativeType: "float" },
  { driver: sqlServer, nativeType: "float(53)" },
];

describe("double precision round-trip validation", () => {
  it.each(cases)("accepts 16-17 digit values that round-trip in $nativeType", ({
    driver,
    nativeType,
  }) => {
    const meta = column(nativeType);
    for (const value of [
      "0.30000000000000004",
      "12.345678901234567",
      "0.1",
      "1e-7",
    ]) {
      expect(driver.checkPersistedEdit(meta, value)?.ok).toBe(true);
    }
  });

  it.each(cases)("rejects values beyond double precision in $nativeType", ({
    driver,
    nativeType,
  }) => {
    const check = driver.checkPersistedEdit(
      column(nativeType),
      "0.123456789012345678",
    );
    expect(check?.ok).toBe(false);
    expect(check?.message).toContain("would round to 0.12345678901234568");
  });

  it.each(
    cases,
  )("verifies readback of 17 digit values exactly in $nativeType", ({
    driver,
    nativeType,
  }) => {
    const meta = column(nativeType);
    expect(
      driver.checkPersistedEdit(meta, "0.30000000000000004", {
        persistedValue: 0.30000000000000004,
      })?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(meta, "0.1", {
        persistedValue: "0.10000000000000001",
      })?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(meta, "0.30000000000000004", {
        persistedValue: 0.3,
      })?.ok,
    ).toBe(false);
  });

  it.each([
    { driver: postgres, nativeType: "real" },
    { driver: postgres, nativeType: "float4" },
    { driver: sqlServer, nativeType: "real" },
    { driver: sqlServer, nativeType: "float(24)" },
  ])("keeps single precision limits for $nativeType", ({
    driver,
    nativeType,
  }) => {
    const meta = column(nativeType);
    expect(driver.checkPersistedEdit(meta, "1.5")?.ok).toBe(true);
    expect(driver.checkPersistedEdit(meta, "16777217")?.ok).toBe(false);
  });

  it("binds sql_variant float values as binary numbers", () => {
    const variant: ColumnTypeMeta = {
      ...column("sql_variant"),
      category: "other",
    };
    const snapshot = (
      baseType: string,
      precision: number | null,
      collation: string | null = null,
    ) => ({
      __rapidbSqlVariant: 1,
      displayValue: "1",
      baseType,
      precision,
      scale: null,
      maxLength: baseType === "nvarchar" ? 200 : null,
      collation,
      binaryHex: "00",
    });

    expect(
      sqlServer.buildMutationValueExpr(
        variant,
        "0.30000000000000004",
        "update",
        snapshot("float", 53),
      ),
    ).toEqual({
      sql: "CAST(? AS float(53))",
      params: [0.30000000000000004],
    });
    expect(
      sqlServer.buildMutationValueExpr(
        variant,
        " 1.5 ",
        "update",
        snapshot("real", null),
      ),
    ).toEqual({ sql: "CAST(? AS real)", params: [1.5] });
    expect(
      sqlServer.buildMutationValueExpr(
        variant,
        "0.1",
        "update",
        snapshot("nvarchar", null, "Latin1_General_CI_AS"),
      )?.params,
    ).toEqual(["0.1"]);
  });

  it("only binds plain decimal sql_variant float text as numbers", () => {
    const variant: ColumnTypeMeta = {
      ...column("sql_variant"),
      category: "other",
    };
    const snapshot = {
      __rapidbSqlVariant: 1,
      displayValue: "1",
      baseType: "float",
      precision: 53,
      scale: null,
      maxLength: null,
      collation: null,
      binaryHex: "00",
    };

    for (const value of ["0x10", "0b101", "0o17", "1_000"]) {
      expect(() =>
        sqlServer.buildMutationValueExpr(variant, value, "update", snapshot),
      ).toThrow('Column "value" expects a numeric value.');
    }
    expect(
      sqlServer.buildMutationValueExpr(variant, " 1.5 ", "update", snapshot)
        ?.params,
    ).toEqual([1.5]);
    expect(
      sqlServer.buildMutationValueExpr(variant, "1e3", "update", snapshot)
        ?.params,
    ).toEqual([1000]);
  });

  it.each([
    "float",
    "real",
  ])("does not canonicalize non-decimal text as a number in %s", (nativeType) => {
    const meta = column(nativeType);
    for (const value of ["0x10", "0b101", "0o17", "1_000"]) {
      expect(sqlServer.checkPersistedEdit(meta, value)?.ok).toBe(false);
    }
    expect(
      sqlServer.checkPersistedEdit(meta, "0x10", { persistedValue: 16 })?.ok,
    ).toBe(false);
    expect(sqlServer.checkPersistedEdit(meta, " 1.5 ")?.ok).toBe(true);
    expect(sqlServer.checkPersistedEdit(meta, "1e3")?.ok).toBe(true);
    expect(
      sqlServer.checkPersistedEdit(meta, "1e3", { persistedValue: 1000 })?.ok,
    ).toBe(true);
  });
});
