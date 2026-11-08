import mssql from "mssql";
import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";

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

describe("MSSQL exact temporal persistence", () => {
  const sqlServer = new MSSQLDriver({
    id: "temporal",
    name: "temporal",
    type: "mssql",
  });
  for (const type of ["time", "datetime2", "datetimeoffset"] as const) {
    const literal = (time: string) =>
      type === "time"
        ? time
        : `2026-10-20 ${time}${type === "datetimeoffset" ? "+02:00" : ""}`;
    it.each([
      [0, "12:00:00", true],
      [0, "12:00:00.0000000", true],
      [0, "12:00:00.1234567", false],
      [3, "12:00:00.1230000", true],
      [3, "12:00:00.1230001", false],
      [7, "12:00:00.1234567", true],
      [7, "12:00:00.12345678", false],
    ] as const)(`${type}(%i) prevalidates significant fractional digits in %s (%s)`, (scale, time, ok) => {
      const meta = column(
        `${type}(${scale})`,
        type === "time" ? "time" : "datetime",
      );
      expect(sqlServer.checkPersistedEdit(meta, literal(time))).toMatchObject({
        ok,
        shouldVerify: ok,
        ...(!ok
          ? {
              message: expect.stringContaining(
                `${scale} fractional second digits`,
              ),
            }
          : {}),
      });
    });
    it(`${type} defaults to precision 7 without discarding significant digits`, () => {
      const meta = column(type, type === "time" ? "time" : "datetime");
      expect(
        sqlServer.checkPersistedEdit(meta, literal("12:00:00.1234567"))?.ok,
      ).toBe(true);
      expect(
        sqlServer.checkPersistedEdit(meta, literal("12:00:00.12345678"))?.ok,
      ).toBe(false);
    });
    it.each([
      null,
      NULL_SENTINEL,
    ])(`${type} verifies SQL NULL (%s)`, (value) => {
      const meta = column(`${type}(0)`, type === "time" ? "time" : "datetime");
      expect(sqlServer.checkPersistedEdit(meta, value)).toEqual({
        ok: true,
        shouldVerify: true,
      });
      expect(
        sqlServer.checkPersistedEdit(meta, value, { persistedValue: null })?.ok,
      ).toBe(true);
      expect(
        sqlServer.checkPersistedEdit(meta, value, {
          persistedValue: literal("12:00:00"),
        })?.ok,
      ).toBe(false);
      expect(
        sqlServer.checkPersistedEdit(meta, literal("12:00:00"), {
          persistedValue: null,
        })?.ok,
      ).toBe(false);
    });
    it(`${type} normalizes zero padding without hiding fractional changes`, () => {
      const meta = column(`${type}(7)`, type === "time" ? "time" : "datetime");
      for (const [expected, persisted] of [
        ["12:00:00", "12:00:00.0000000"],
        ["12:00:00.123", "12:00:00.1230000"],
      ]) {
        expect(
          sqlServer.checkPersistedEdit(meta, literal(expected), {
            persistedValue: literal(persisted),
          }),
        ).toEqual({ ok: true, shouldVerify: true });
      }
      expect(
        sqlServer.checkPersistedEdit(meta, literal("12:00:00.1234567"), {
          persistedValue: literal("12:00:00.1234566"),
        })?.ok,
      ).toBe(false);
    });
  }

  it.each([
    ["time(7)", "12:00", "12:00:00.0000000"],
    ["time(7)", "1:02", "01:02:00.0000000"],
    ["time(7)", "1:02:03.123", "01:02:03.1230000"],
    ["datetime2(7)", "2026-10-20T12:00", "2026-10-20 12:00:00.0000000"],
    [
      "datetimeoffset(7)",
      "2026-10-20T12:00+02:00",
      "2026-10-20 12:00:00.0000000+02:00",
    ],
    ["datetime2(3)", "2026-10-20T12:00:00.123Z", "2026-10-20 12:00:00.1230000"],
    ["datetime2(0)", "2026-10-20", "2026-10-20 00:00:00"],
    [
      "datetimeoffset(3)",
      "2026-10-20T12:00:00.123Z",
      "2026-10-20 12:00:00.1230000+00:00",
    ],
    [
      "datetimeoffset(3)",
      "2026-10-20 12:00:00.123 +02",
      "2026-10-20 12:00:00.1230000+02:00",
    ],
    ["datetimeoffset(0)", "2026-10-20", "2026-10-20 00:00:00+00:00"],
    [
      "datetimeoffset(3)",
      "2026-10-20T12:00:00.123-00:00",
      "2026-10-20 12:00:00.1230000+00:00",
    ],
  ])("verifies equivalent %s formatting", (type, expected, persistedValue) => {
    expect(
      sqlServer.checkPersistedEdit(column(type, "datetime"), expected, {
        persistedValue,
      }),
    ).toEqual({ ok: true, shouldVerify: true });
  });

  it("prevalidates Date milliseconds without rounding to the declared scale", () => {
    const date = new Date(2026, 9, 20, 12, 0, 0, 123);
    expect(
      sqlServer.checkPersistedEdit(column("datetime2(0)", "datetime"), date)
        ?.ok,
    ).toBe(false);
    expect(
      sqlServer.checkPersistedEdit(column("time(3)", "time"), date)?.ok,
    ).toBe(true);
  });

  it.each([
    "datetime",
    "smalldatetime",
  ])("leaves %s rounding to exact readback", (type) => {
    const meta = column(type, "datetime");
    const expected = "2026-10-20 12:00:00.123";
    expect(sqlServer.checkPersistedEdit(meta, expected)).toEqual({
      ok: true,
      shouldVerify: true,
    });
    expect(
      sqlServer.checkPersistedEdit(meta, expected, {
        persistedValue: "2026-10-20 12:00:00",
      })?.ok,
    ).toBe(false);
  });
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
    ).toMatchObject({ type: mssql.NVarChar, length: uuid.length });
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
  it.each([
    ["numeric(10,2)", "1.235e0", false],
    ["numeric(10,2)", "1.23e0", true],
    ["numeric(10,2)", "12345e-2", true],
    ["numeric(4,2)", "1e3", false],
    ["numeric(2)", "-1.2E1", true],
  ])("%s validates exponent input %s: %s", (type, value, ok) => {
    expect(driver.checkPersistedEdit(column(type, "decimal"), value)?.ok).toBe(
      ok,
    );
  });
  it("verifies exponent input against the persisted decimal", () => {
    const meta = column("numeric(10,2)", "decimal");
    expect(
      driver.checkPersistedEdit(meta, "1.5e1", { persistedValue: "15.00" }),
    ).toEqual({ ok: true, shouldVerify: true });
    expect(
      driver.checkPersistedEdit(meta, "1.5e1", { persistedValue: "1.50" })?.ok,
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

  it("preserves PostgreSQL's driver-specific temporal opt-out without inferring on-update metadata", () => {
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

describe("JSON persisted-edit number normalization", () => {
  const mysqlDriver = new MySQLDriver({
    id: "json-numbers",
    name: "json-numbers",
    type: "mysql",
  });
  const sqliteDriver = new SQLiteDriver({
    id: "json-numbers",
    name: "json-numbers",
    type: "sqlite",
    filePath: ":memory:",
  });

  it.each([
    ['{"a":1e2}', '{"a": 100}'],
    ['{"a":1.50,"b":[0.10]}', { b: [0.1], a: 1.5 }],
    ["-0.0", "0"],
  ])("accepts PostgreSQL jsonb %s stored as %j", (expected, persisted) => {
    expect(
      driver.checkPersistedEdit(column("jsonb", "json"), expected, {
        persistedValue: persisted,
      }),
    ).toEqual({ ok: true, shouldVerify: true });
  });

  it("accepts MySQL JSON numbers rewritten by the server", () => {
    const meta = column("json", "json");
    expect(
      mysqlDriver.checkPersistedEdit(meta, '{"a":1.50}', {
        persistedValue: { a: 1.5 },
      }),
    ).toEqual({ ok: true, shouldVerify: true });
    expect(
      mysqlDriver.checkPersistedEdit(meta, '{"a":1e2}', {
        persistedValue: '{"a": 100.0}',
      }),
    ).toEqual({ ok: true, shouldVerify: true });
    expect(
      mysqlDriver.checkPersistedEdit(meta, '{"a":1.5}', {
        persistedValue: '{"a": 1.6}',
      })?.ok,
    ).toBe(false);
  });

  it("keeps exact number text for PostgreSQL json and SQLite JSON", () => {
    expect(
      driver.checkPersistedEdit(column("json", "json"), '{"a":1e2}', {
        persistedValue: '{"a": 100}',
      })?.ok,
    ).toBe(false);
    expect(
      sqliteDriver.checkPersistedEdit(column("JSON", "json"), '{"a":1.50}', {
        persistedValue: '{"a":1.5}',
      })?.ok,
    ).toBe(false);
  });
});

describe("binary and single-precision persisted edits", () => {
  it("verifies a binary cell cleared to empty text against zero bytes", () => {
    expect(
      driver.checkPersistedEdit(column("bytea", "binary"), "", {
        persistedValue: Buffer.alloc(0),
      }),
    ).toEqual({ ok: true, shouldVerify: true });
    expect(
      driver.checkPersistedEdit(column("bytea", "binary"), "", {
        persistedValue: Buffer.from([0]),
      })?.ok,
    ).toBe(false);
  });

  it.each([
    ["16777216", true],
    ["12345678", true],
    ["1.2345678", true],
    ["0.1", true],
    ["16777217", false],
    ["3.14159265", false],
  ])("real precheck for %s: %s", (value, ok) => {
    expect(driver.checkPersistedEdit(column("real", "float"), value)?.ok).toBe(
      ok,
    );
  });

  it("verifies real values at single precision after save", () => {
    const meta = column("real", "float");
    expect(
      driver.checkPersistedEdit(meta, "1.2345678", {
        persistedValue: "1.2345679",
      })?.ok,
    ).toBe(false);
    expect(
      driver.checkPersistedEdit(meta, "1.2345678", {
        persistedValue: Math.fround(1.2345678),
      }),
    ).toEqual({ ok: true, shouldVerify: true });
    expect(
      driver.checkPersistedEdit(meta, "16777216", {
        persistedValue: "1.6777216e+07",
      }),
    ).toEqual({ ok: true, shouldVerify: true });
  });

  it("reports the single-precision value a rejected real input rounds to", () => {
    expect(
      driver.checkPersistedEdit(column("real", "float"), "16777217")?.message,
    ).toContain("would round to 16777216");
  });
});
