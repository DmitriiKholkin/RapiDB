import mssql from "mssql";
import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

const driver = new PostgresDriver({
  id: "persisted-edit-diagnostics",
  name: "persisted-edit-diagnostics",
  type: "pg",
  host: "localhost",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
});

const column = (
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta => ({
  name: "value",
  type: nativeType,
  nativeType,
  category,
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
});

describe("C01 UUID syntax and parameter inference", () => {
  it.each([
    "01890f47-6a42-7cc3-98c4-dc0c0c07398f",
    "01890f47-6a42-6cc3-98c4-dc0c0c07398f",
    "01890f47-6a42-8cc3-98c4-dc0c0c07398f",
    "00000000-0000-0000-0000-000000000000",
    "ffffffff-ffff-ffff-ffff-ffffffffffff",
    "12345678-1234-abcd-0123-123456789abc",
  ])("accepts and verifies backend UUID syntax %s", (uuid) => {
    expect(driver.checkPersistedEdit(column("uuid", "uuid"), uuid)).toEqual({
      ok: true,
      shouldVerify: true,
    });
    expect(
      driver.checkPersistedEdit(column("uuid", "uuid"), uuid.toUpperCase(), {
        persistedValue: uuid,
      }),
    ).toEqual({ ok: true, shouldVerify: true });
    const sqlServer = new MSSQLDriver({
      id: "uuid",
      name: "uuid",
      type: "mssql",
    });
    expect(
      (
        sqlServer as unknown as { typeForValue(value: unknown): unknown }
      ).typeForValue(uuid.toUpperCase()),
    ).toBe(mssql.UniqueIdentifier);
    expect(
      sqlServer.checkPersistedEdit(column("uniqueidentifier", "uuid"), uuid)
        ?.ok,
    ).toBe(true);
  });
  it.each([
    "not-a-uuid",
    "01890f47-6a42-7cc3-98c4-dc0c0c07398g",
    "00000000-0000-0000-000000000000",
  ])("rejects malformed UUID %s", (uuid) => {
    expect(driver.checkPersistedEdit(column("uuid", "uuid"), uuid)?.ok).toBe(
      false,
    );
  });
});

describe("C02 exact numeric precision", () => {
  it.each([
    ["numeric(2,2)", "0.12", true],
    ["numeric(2,2)", "-000.12", true],
    ["numeric(2,2)", "+0.00", true],
    ["numeric(2,2)", "-0", true],
    ["numeric(2,2)", "1", false],
    ["numeric(2,2)", "0.1200", true],
    ["numeric(2,2)", "0.001", false],
    ["numeric(2,4)", "0.0012", true],
    ["numeric(2,4)", "-0.0099", true],
    ["numeric(2,4)", "0.0000", true],
    ["numeric(2,4)", "0", true],
    ["numeric(2,4)", "0.0123", false],
    ["numeric(2,4)", "1.0012", false],
    ["numeric(2,4)", "0.00123", false],
    ["numeric(4,2)", "0", true],
    ["numeric(4,2)", "1.239", false],
    ["numeric(2)", "12", true],
    ["numeric(2)", "12.1", false],
    ["numeric(2)", "123", false],
    ["numeric(30,10)", "12345678901234567890.1234567890", true],
    ["numeric(30,10)", "123456789012345678901.1234567890", false],
    ["numeric", "123456789012345678901.12345678901234567890", true],
  ])("%s checks %s: %s", (type, value, ok) => {
    const meta = column(type, "decimal");
    expect(driver.checkPersistedEdit(meta, value)?.ok).toBe(ok);
    if (ok)
      expect(
        driver.checkPersistedEdit(meta, value, { persistedValue: value })?.ok,
      ).toBe(true);
  });
  it("does not compare large exact decimals via JS Number", () => {
    expect(
      driver.checkPersistedEdit(
        column("numeric(30,10)", "decimal"),
        "12345678901234567890.1234567890",
        { persistedValue: "12345678901234567890.1234567891" },
      )?.ok,
    ).toBe(false);
  });
  it("preserves native negative-scale and nonfinite fallback", () => {
    expect(
      driver.checkPersistedEdit(column("numeric(2,-3)", "decimal"), "12001"),
    ).toBeNull();
    for (const value of [Infinity, NaN, "Infinity", "NaN"]) {
      expect(
        driver.checkPersistedEdit(column("numeric", "decimal"), value),
      ).toBeNull();
    }
  });
});

describe("persisted edit diagnostics", () => {
  it("quotes string values so whitespace differences are visible", () => {
    const column: ColumnTypeMeta = {
      name: "col_char",
      type: "TEXT",
      nativeType: "TEXT",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "text",
      filterable: true,
      filterOperators: ["eq", "like"],
      valueSemantics: "plain",
    };

    const check = driver.checkPersistedEdit(column, "wad  23", {
      persistedValue: "wad 23",
    });

    expect(check?.ok).toBe(false);
    expect(check?.message).toContain('"wad 23"');
    expect(check?.message).toContain('"wad  23"');
  });

  it("formats Date diagnostics as SQL datetime text", () => {
    const column: ColumnTypeMeta = {
      name: "col_char",
      type: "TEXT",
      nativeType: "TEXT",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "text",
      filterable: true,
      filterOperators: ["eq", "like"],
      valueSemantics: "plain",
    };

    const check = driver.checkPersistedEdit(column, "expected", {
      persistedValue: new Date(Date.UTC(2026, 4, 29, 20, 30, 57, 769)),
    });

    expect(check?.ok).toBe(false);
    expect(check?.message).toContain('"2026-05-29 20:30:57.769"');
    expect(check?.message).not.toContain("Fri May");
  });

  it("treats equivalent timestamptz offset formats as persisted matches", () => {
    const column: ColumnTypeMeta = {
      name: "created_at",
      type: "timestamp with time zone",
      nativeType: "timestamp with time zone",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
      valueSemantics: "plain",
    };

    const check = driver.checkPersistedEdit(
      column,
      "1970-01-01 00:00:00+00:00",
      {
        persistedValue: "1970-01-01 00:00:00+00",
      },
    );

    expect(check?.ok).toBe(true);
  });

  it("skips verification for likely auto-updated temporal columns", () => {
    const column: ColumnTypeMeta = {
      name: "updated_at",
      type: "timestamp with time zone",
      nativeType: "timestamp with time zone",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
      valueSemantics: "plain",
    };

    const check = driver.checkPersistedEdit(
      column,
      "2026-05-28 13:08:31.75746+00:00",
      {
        persistedValue: "2026-05-28 19:16:35.014986+00",
      },
    );

    expect(check).toEqual({
      ok: true,
      shouldVerify: false,
    });
  });
});
