import * as mssql from "mssql";
import oracledb from "oracledb";
import { describe, expect, it, vi } from "vitest";
import type { BaseDBDriver } from "../../src/extension/dbDrivers/BaseDBDriver";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnMeta,
  ColumnTypeMeta,
} from "../../src/extension/dbDrivers/types";
import { col } from "./helpers";

// Minimal configs for instantiation (no connection needed for pure methods)
const pgConfig = {
  id: "t",
  name: "t",
  type: "pg" as const,
  host: "localhost",
  port: 5432,
  database: "test",
  username: "u",
  password: "p",
};
const myConfig = {
  id: "t",
  name: "t",
  type: "mysql" as const,
  host: "localhost",
  port: 3306,
  database: "test",
  username: "u",
  password: "p",
};
const msConfig = {
  id: "t",
  name: "t",
  type: "mssql" as const,
  host: "localhost",
  port: 1433,
  database: "test",
  username: "u",
  password: "p",
};
const oraConfig = {
  id: "t",
  name: "t",
  type: "oracle" as const,
  host: "localhost",
  port: 1521,
  database: "test",
  username: "u",
  password: "p",
};
const sqliteConfig = {
  id: "t",
  name: "t",
  type: "sqlite" as const,
  filePath: "/tmp/test.db",
};

const pg = new PostgresDriver(pgConfig);
const my = new MySQLDriver(myConfig);
const ms = new MSSQLDriver(msConfig);
const ora = new OracleDriver(oraConfig);
const lite = new SQLiteDriver(sqliteConfig);

describe("preview SQL materialization", () => {
  it("interpolates SQLite positional parameters with display values", () => {
    expect(
      lite.materializePreviewSql(
        'UPDATE "main"."apples" SET "name" = ? WHERE "id" = ?',
        ["O'Red", 7],
      ),
    ).toBe(`UPDATE "main"."apples" SET "name" = 'O''Red' WHERE "id" = 7`);
  });

  it("interpolates PostgreSQL numbered parameters with display values", () => {
    expect(
      pg.materializePreviewSql(
        'UPDATE "public"."users" SET "name" = $1 WHERE "id" = $2',
        ["Alice", 1],
      ),
    ).toBe(`UPDATE "public"."users" SET "name" = 'Alice' WHERE "id" = 1`);
  });

  it("interpolates Oracle numbered parameters including NULL values", () => {
    expect(
      ora.materializePreviewSql(
        'INSERT INTO "APPLES" ("NAME", "COLOR") VALUES (:1, :2)',
        ["Honeycrisp", null],
      ),
    ).toBe(
      `INSERT INTO "APPLES" ("NAME", "COLOR") VALUES ('Honeycrisp', NULL)`,
    );
  });

  it("interpolates MSSQL positional parameters with display values", () => {
    expect(
      ms.materializePreviewSql(
        "UPDATE [dbo].[apples] SET [name] = CAST(? AS nvarchar(max)) WHERE [id] = ?",
        ["Granny Smith", 3],
      ),
    ).toBe(
      `UPDATE [dbo].[apples] SET [name] = CAST('Granny Smith' AS nvarchar(max)) WHERE [id] = 3`,
    );
  });
});

function mssqlTypeName(sqlType: unknown): string {
  if (typeof sqlType === "function") {
    return sqlType.name;
  }

  if (sqlType && typeof sqlType === "object" && "type" in sqlType) {
    const nestedType = (sqlType as { type?: unknown }).type;
    return typeof nestedType === "function" ? nestedType.name : "";
  }

  return "";
}

function expectMssqlTimeBinding(
  boundValue: unknown,
  expectedTime: {
    hours: number;
    minutes: number;
    seconds: number;
    milliseconds: number;
    nanosecondDelta?: number;
  },
): void {
  expect(boundValue).toBeInstanceOf(Date);

  const date = boundValue as Date & { nanosecondDelta?: number };
  expect(date.getUTCFullYear()).toBe(1970);
  expect(date.getUTCMonth()).toBe(0);
  expect(date.getUTCDate()).toBe(1);
  expect(date.getUTCHours()).toBe(expectedTime.hours);
  expect(date.getUTCMinutes()).toBe(expectedTime.minutes);
  expect(date.getUTCSeconds()).toBe(expectedTime.seconds);
  expect(date.getUTCMilliseconds()).toBe(expectedTime.milliseconds);
  expect(date.nanosecondDelta).toBe(expectedTime.nanosecondDelta);
}

function expectMssqlDatetimeBinding(
  boundValue: unknown,
  expectedValue: {
    year: number;
    month: number;
    day: number;
    hours: number;
    minutes: number;
    seconds: number;
    milliseconds: number;
    nanosecondDelta?: number;
  },
): void {
  expect(boundValue).toBeInstanceOf(Date);

  const date = boundValue as Date & { nanosecondDelta?: number };
  expect(date.getUTCFullYear()).toBe(expectedValue.year);
  expect(date.getUTCMonth()).toBe(expectedValue.month - 1);
  expect(date.getUTCDate()).toBe(expectedValue.day);
  expect(date.getUTCHours()).toBe(expectedValue.hours);
  expect(date.getUTCMinutes()).toBe(expectedValue.minutes);
  expect(date.getUTCSeconds()).toBe(expectedValue.seconds);
  expect(date.getUTCMilliseconds()).toBe(expectedValue.milliseconds);
  expect(date.nanosecondDelta).toBe(expectedValue.nanosecondDelta);
}

function enrichTestColumn(
  driver: BaseDBDriver,
  column: ColumnMeta,
): ColumnTypeMeta {
  return (
    driver as unknown as {
      enrichColumn: (value: ColumnMeta) => ColumnTypeMeta;
    }
  ).enrichColumn(column);
}

function setSqliteDb(
  driver: SQLiteDriver,
  db: {
    isOpen: boolean;
    all: (sql: string) => unknown;
    run: (sql: string) => { changes: number };
  },
): void {
  (driver as unknown as { db: typeof db }).db = db;
}

const oraInternals = ora as unknown as {
  _fallbackDDL: (
    conn: Pick<oracledb.Connection, "execute">,
    schema: string,
    table: string,
  ) => Promise<string>;
  _fetchTypeHandler: (
    metaData: oracledb.Metadata<unknown>,
  ) => oracledb.FetchTypeResponse | undefined;
};

describe("null-only filter support", () => {
  it.each([
    ["Postgres", pg, '"payload"'],
    ["MySQL", my, "`payload`"],
    ["MSSQL", ms, "[payload]"],
    ["Oracle", ora, '"payload"'],
    ["SQLite", lite, '"payload"'],
  ] as const)("allows IS NULL for non-filterable nullable columns in %s", (_name, driver, quotedColumn) => {
    const column = col({
      name: "payload",
      type: "json",
      category: "json",
      nativeType: "json",
      filterable: false,
      nullable: true,
      filterOperators: ["is_null", "is_not_null"],
    });

    const result = driver.buildFilterCondition(column, "is_null", undefined, 1);

    expect(result).toEqual({
      sql: `${quotedColumn} IS NULL`,
      params: [],
    });
  });

  it.each([
    ["Postgres", pg, '"payload"'],
    ["MySQL", my, "`payload`"],
    ["MSSQL", ms, "[payload]"],
    ["Oracle", ora, '"payload"'],
    ["SQLite", lite, '"payload"'],
  ] as const)("allows IS NOT NULL for non-filterable nullable columns in %s", (_name, driver, quotedColumn) => {
    const column = col({
      name: "payload",
      type: "json",
      category: "json",
      nativeType: "json",
      filterable: false,
      nullable: true,
      filterOperators: ["is_null", "is_not_null"],
    });

    const result = driver.buildFilterCondition(
      column,
      "is_not_null",
      undefined,
      1,
    );

    expect(result).toEqual({
      sql: `${quotedColumn} IS NOT NULL`,
      params: [],
    });
  });
});

describe("cross-driver type capability parity", () => {
  it.each([
    ["Postgres spatial", pg, "point", "spatial", false, false],
    ["Postgres interval", pg, "interval", "interval", false, false],
    ["MySQL spatial", my, "geometry", "spatial", false, false],
    ["MSSQL spatial", ms, "geometry", "spatial", false, false],
    ["Oracle spatial", ora, "SDO_GEOMETRY", "spatial", false, false],
    [
      "Oracle interval",
      ora,
      "INTERVAL DAY TO SECOND",
      "interval",
      false,
      false,
    ],
  ] as const)("aligns %s metadata", (_name, driver, type, category, filterable, editable) => {
    const result = enrichTestColumn(driver, {
      name: "value_col",
      type,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });

    expect(result.category).toBe(category);
    expect(result.filterable).toBe(filterable);
    expect(result.editable).toBe(editable);
    expect(result.filterOperators).toEqual(["is_null", "is_not_null"]);
  });
});

describe("driver-owned persisted edit checks", () => {
  it.each([
    ["Postgres", pg, "numeric(10,2)", "123.523"],
    ["MySQL", my, "decimal(10,2)", "123.523"],
    ["MSSQL", ms, "money", "12.12345"],
    ["Oracle", ora, "NUMBER(10,2)", "123.523"],
  ] as const)("rejects out-of-scale exact numerics in %s before write", (_name, driver, nativeType, invalidValue) => {
    const result = driver.checkPersistedEdit(
      col({
        name: "amount",
        type: nativeType,
        category: "decimal",
        nativeType,
      }),
      invalidValue,
    );

    expect(result).toEqual(
      expect.objectContaining({
        ok: false,
        shouldVerify: false,
      }),
    );
    expect(result?.message).toContain("amount");
  });

  it.each([
    ["Postgres", pg, "numeric(10,2)", "123.45", "123.40"],
    ["MySQL", my, "decimal(10,2)", "123.45", "123.40"],
    ["MSSQL", ms, "money", "12.3400", "12.0000"],
    ["Oracle", ora, "NUMBER(10,2)", "123.45", "123.40"],
  ] as const)("compares persisted exact numerics in %s during verification", (_name, driver, nativeType, expectedValue, persistedValue) => {
    const result = driver.checkPersistedEdit(
      col({
        name: "amount",
        type: nativeType,
        category: "decimal",
        nativeType,
      }),
      expectedValue,
      { persistedValue },
    );

    expect(result).toEqual(
      expect.objectContaining({
        ok: false,
        shouldVerify: true,
      }),
    );
    expect(result?.message).toContain("stored");
  });

  it("verifies SQLite declared DECIMAL values without enforcing a fixed scale", () => {
    const column = col({
      name: "amount",
      type: "DECIMAL(10,2)",
      category: "decimal",
      nativeType: "DECIMAL(10,2)",
    });

    expect(lite.checkPersistedEdit(column, "123.523")).toEqual(
      expect.objectContaining({
        ok: true,
        shouldVerify: true,
      }),
    );
    expect(
      lite.checkPersistedEdit(column, "123.52", { persistedValue: "123.00" }),
    ).toEqual(
      expect.objectContaining({
        ok: false,
        shouldVerify: true,
      }),
    );
  });

  it.each([
    ["Postgres", pg, "numeric(10,2)"],
    ["MySQL", my, "decimal(10,2)"],
    ["MSSQL", ms, "money"],
    ["Oracle", ora, "NUMBER(10,2)"],
    ["SQLite", lite, "DECIMAL(10,2)"],
  ] as const)("verifies NULL exact numerics in %s instead of silently opting out", (_name, driver, nativeType) => {
    const column = col({
      name: "amount",
      type: nativeType,
      category: "decimal",
      nativeType,
    });

    expect(driver.checkPersistedEdit(column, null)).toEqual(
      expect.objectContaining({
        ok: true,
        shouldVerify: true,
      }),
    );
    expect(
      driver.checkPersistedEdit(column, null, { persistedValue: "0.00" }),
    ).toEqual(
      expect.objectContaining({
        ok: false,
        shouldVerify: true,
      }),
    );
  });

  it.each([
    ["Postgres json", pg, "jsonb", '{"a":1}', "json", true],
    ["Postgres array", pg, "integer[]", "[1,2,3]", "array", true],
    [
      "Postgres uuid",
      pg,
      "uuid",
      "550e8400-e29b-41d4-a716-446655440000",
      "uuid",
      true,
    ],
    ["Postgres bytea", pg, "bytea", "\\xdeadbeef", "binary", true],
    ["Postgres text", pg, "text", "alpha", "text", true],
    ["Postgres float", pg, "float8", "12.5", "float", true],
    ["MySQL json", my, "json", '{"a":1}', "json", true],
    ["MySQL enum", my, "enum('a','b')", "a", "enum", true],
    ["MySQL blob", my, "blob", "\\xdeadbeef", "binary", true],
    ["MySQL text", my, "varchar(32)", "alpha", "text", true],
    ["MySQL float", my, "double", "12.5", "float", true],
    [
      "MSSQL uuid",
      ms,
      "uniqueidentifier",
      "550e8400-e29b-41d4-a716-446655440000",
      "uuid",
      true,
    ],
    ["MSSQL varbinary", ms, "varbinary(16)", "\\xdeadbeef", "binary", true],
    ["MSSQL text", ms, "nvarchar(64)", "alpha", "text", true],
    [
      "MSSQL datetime",
      ms,
      "datetime2(3)",
      "2026-04-20 12:34:56.123",
      "datetime",
      true,
    ],
    ["Oracle RAW", ora, "RAW(16)", "\\xdeadbeef", "binary", true],
    ["Oracle text", ora, "VARCHAR2(32)", "alpha", "text", true],
    [
      "Oracle datetime",
      ora,
      "TIMESTAMP(3)",
      "2026-04-20 12:34:56.123",
      "datetime",
      true,
    ],
    ["Oracle float", ora, "BINARY_DOUBLE", "12.5", "float", true],
    ["SQLite json", lite, "JSON", '{"a":1}', "json", true],
    [
      "SQLite uuid",
      lite,
      "UUID",
      "550e8400-e29b-41d4-a716-446655440000",
      "uuid",
      true,
    ],
    ["SQLite blob", lite, "BLOB", "\\xdeadbeef", "binary", true],
    [
      "SQLite datetime",
      lite,
      "DATETIME",
      "2026-04-20 12:34:56",
      "datetime",
      true,
    ],
    ["SQLite float", lite, "DOUBLE", "12.5", "float", true],
  ] as const)("returns an explicit edit check for %s", (_name, driver, type, value, category, editable) => {
    const column = enrichTestColumn(driver, {
      name: "value_col",
      type,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });

    expect(column.category).toBe(category);
    expect(column.editable).toBe(editable);
    expect(driver.checkPersistedEdit(column, value)).toEqual(
      expect.objectContaining({
        ok: true,
        shouldVerify: true,
      }),
    );
  });

  it.each([
    ["Postgres", pg, "double precision", "1.12345678901234567", 15],
    ["MySQL", my, "double", "1.12345678901234567", 15],
    ["MySQL float", my, "float", "1.123456789", 7],
    ["MSSQL", ms, "float", "1.12345678901234567", 15],
    ["MSSQL real", ms, "real", "1.123456789", 7],
    ["Oracle", ora, "BINARY_DOUBLE", "1.12345678901234567", 15],
    ["Oracle BINARY_FLOAT", ora, "BINARY_FLOAT", "1.123456789", 7],
    ["SQLite", lite, "DOUBLE", "1.12345678901234567", 15],
    ["Postgres real", pg, "real", "1.123456789", 7],
  ] as const)("rejects lossy approximate numerics before write in %s", (_name, driver, nativeType, invalidValue, significantDigits) => {
    const column = enrichTestColumn(driver, {
      name: "ratio",
      type: nativeType,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });

    const result = driver.checkPersistedEdit(column, invalidValue);
    expect(result).toEqual(
      expect.objectContaining({
        ok: false,
        shouldVerify: false,
      }),
    );
    expect(result?.message).toContain(
      `${significantDigits} significant digits`,
    );
    expect(result?.message).toContain("would round to");
  });

  it.each([
    ["Postgres", pg, "double precision", "1.2300000000000000000"],
    ["MySQL", my, "double", "1.2300000000000000000"],
    ["MySQL float", my, "float", "1.230000000"],
    ["MSSQL", ms, "float", "1.2300000000000000000"],
    ["MSSQL real", ms, "real", "1.230000000"],
    ["Oracle", ora, "BINARY_DOUBLE", "1.2300000000000000000"],
    ["Oracle BINARY_FLOAT", ora, "BINARY_FLOAT", "1.230000000"],
    ["SQLite", lite, "DOUBLE", "1.2300000000000000000"],
  ] as const)("allows approximate numerics with redundant trailing zeros in %s", (_name, driver, nativeType, value) => {
    const column = enrichTestColumn(driver, {
      name: "ratio",
      type: nativeType,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });

    expect(driver.checkPersistedEdit(column, value)).toEqual(
      expect.objectContaining({
        ok: true,
        shouldVerify: true,
      }),
    );
  });

  it.each([
    ["Postgres", pg, "jsonb", "{bad json", "valid JSON"],
    ["MySQL", my, "json", "{bad json", "valid JSON"],
    ["SQLite", lite, "JSON", "{bad json", "valid JSON"],
    ["Postgres", pg, "uuid", "not-a-uuid", "valid UUID"],
    ["MSSQL", ms, "uniqueidentifier", "not-a-uuid", "valid UUID"],
    ["SQLite", lite, "UUID", "not-a-uuid", "valid UUID"],
    ["Postgres", pg, "bytea", "not-hex", "hex value"],
    ["MySQL", my, "blob", "not-hex", "hex value"],
    ["MSSQL", ms, "varbinary(16)", "not-hex", "hex value"],
    ["Oracle", ora, "RAW(16)", "not-hex", "hex value"],
    ["SQLite", lite, "BLOB", "not-hex", "hex value"],
    ["Postgres", pg, "integer[]", "not-an-array", "JSON array"],
  ] as const)("rejects malformed %s edit input before write", (_name, driver, type, invalidValue, messageFragment) => {
    const column = enrichTestColumn(driver, {
      name: "payload",
      type,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });

    const result = driver.checkPersistedEdit(column, invalidValue);
    expect(result).toEqual(
      expect.objectContaining({
        ok: false,
        shouldVerify: false,
      }),
    );
    expect(result?.message).toContain(messageFragment);
  });

  it.each([
    ["Postgres", pg, "jsonb"],
    ["MySQL", my, "json"],
    ["SQLite", lite, "JSON"],
  ] as const)("canonicalizes JSON key order during verification in %s", (_name, driver, nativeType) => {
    const result = driver.checkPersistedEdit(
      col({
        name: "payload",
        type: nativeType,
        category: "json",
        nativeType,
      }),
      '{"a":1,"b":2}',
      { persistedValue: '{"b":2,"a":1}' },
    );

    expect(result).toEqual(
      expect.objectContaining({
        ok: true,
        shouldVerify: true,
      }),
    );
  });

  it("keeps PostgreSQL money columns read-only because locale-aware formatting is not safely verifiable", () => {
    const column = enrichTestColumn(pg, {
      name: "price",
      type: "money",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });

    expect(column.category).toBe("decimal");
    expect(column.editable).toBe(false);
  });
});

// ────────────────────────────────────────────
// PostgreSQL Driver
// ────────────────────────────────────────────

describe("PostgresDriver", () => {
  describe("mapTypeCategory", () => {
    it.each([
      ["boolean", "boolean"],
      ["bool", "boolean"],
      ["smallint", "integer"],
      ["integer", "integer"],
      ["bigint", "integer"],
      ["serial", "integer"],
      ["smallserial", "integer"],
      ["bigserial", "integer"],
      ["oid", "integer"],
      ["xid", "integer"],
      ["cid", "integer"],
      ["numeric", "decimal"],
      ["numeric(10,2)", "decimal"],
      ["decimal", "decimal"],
      ["money", "decimal"],
      ["real", "float"],
      ["double precision", "float"],
      ["float4", "float"],
      ["float8", "float"],
      ["date", "date"],
      ["time", "time"],
      ["timetz", "time"],
      ["time with time zone", "time"],
      ["time without time zone", "time"],
      ["timestamp", "datetime"],
      ["timestamp with time zone", "datetime"],
      ["timestamp without time zone", "datetime"],
      ["bytea", "binary"],
      ["json", "json"],
      ["jsonb", "json"],
      ["uuid", "uuid"],
      ["point", "spatial"],
      ["line", "spatial"],
      ["lseg", "spatial"],
      ["box", "spatial"],
      ["path", "spatial"],
      ["polygon", "spatial"],
      ["circle", "spatial"],
      ["interval", "interval"],
      ["integer[]", "array"],
      ["_int4", "array"],
      ["text", "text"],
      ["varchar(50)", "text"],
      ["character varying(50)", "text"],
      ["character(10)", "text"],
      ["name", "text"],
      ["xml", "text"],
      ["inet", "text"],
      ["cidr", "text"],
      ["macaddr", "text"],
      ["macaddr8", "text"],
      ["tsvector", "text"],
      ["tsquery", "text"],
      ["bit", "other"],
      ["varbit", "other"],
    ] as const)("maps %s → %s", (input, expected) => {
      expect(pg.mapTypeCategory(input)).toBe(expected);
    });
  });

  describe("isBooleanType", () => {
    it("true for boolean/bool", () => {
      expect(pg.isBooleanType("boolean")).toBe(true);
      expect(pg.isBooleanType("bool")).toBe(true);
      expect(pg.isBooleanType("BOOLEAN")).toBe(true);
    });
    it("false for others", () => {
      expect(pg.isBooleanType("int")).toBe(false);
      expect(pg.isBooleanType("bit")).toBe(false);
    });
  });

  describe("isDatetimeWithTime", () => {
    it.each([
      "timestamp",
      "timestamp with time zone",
      "timetz",
      "time with time zone",
      "time",
    ])("true for %s", (t) => expect(pg.isDatetimeWithTime(t)).toBe(true));
    it("false for date", () =>
      expect(pg.isDatetimeWithTime("date")).toBe(false));
  });

  describe("quoteIdentifier (uses base)", () => {
    it("double-quotes", () => {
      expect(pg.quoteIdentifier("users")).toBe('"users"');
    });
  });

  describe("buildPagination ($N params)", () => {
    it("uses positional $N placeholders", () => {
      const r = pg.buildPagination(20, 50, 3);
      expect(r.sql).toBe("LIMIT $3 OFFSET $4");
      expect(r.params).toEqual([50, 20]);
    });
  });

  describe("buildInsertValueExpr ($N)", () => {
    it("returns $N", () => {
      expect(pg.buildInsertValueExpr(col({ name: "a", type: "text" }), 5)).toBe(
        "$5",
      );
    });
  });

  describe("buildSetExpr ($N)", () => {
    it("returns quoted = $N", () => {
      expect(pg.buildSetExpr(col({ name: "age", type: "int" }), 2)).toBe(
        '"age" = $2',
      );
    });
  });

  describe("coerceInputValue", () => {
    it("returns native true/false for booleans", () => {
      const c = col({
        name: "b",
        type: "boolean",
        category: "boolean",
        isBoolean: true,
      });
      expect(pg.coerceInputValue("true", c)).toBe(true);
      expect(pg.coerceInputValue("false", c)).toBe(false);
    });

    it("parses JSON arrays for array columns", () => {
      const c = col({
        name: "tags",
        type: "text[]",
        category: "array",
        nativeType: "text[]",
      });
      expect(pg.coerceInputValue("[1,2,3]", c)).toEqual([1, 2, 3]);
    });

    it("passes non-JSON arrays through", () => {
      const c = col({
        name: "tags",
        type: "text[]",
        category: "array",
        nativeType: "text[]",
      });
      expect(pg.coerceInputValue("{1,2,3}", c)).toBe("{1,2,3}");
    });

    it("converts interval JSON to ISO format", () => {
      const c = col({
        name: "dur",
        type: "interval",
        category: "interval",
        nativeType: "interval",
      });
      expect(pg.coerceInputValue('{"years":1,"months":2,"days":3}', c)).toBe(
        "P1Y2M3D",
      );
    });

    it("returns P0D for empty interval JSON", () => {
      const c = col({
        name: "dur",
        type: "interval",
        category: "interval",
        nativeType: "interval",
      });
      expect(pg.coerceInputValue("{}", c)).toBe("P0D");
    });

    it("converts hex to Buffer for binary", () => {
      const c = col({ name: "d", type: "bytea", category: "binary" });
      const r = pg.coerceInputValue("\\xab", c);
      expect(Buffer.isBuffer(r)).toBe(true);
    });

    it("converts ISO datetime to date-only for date columns", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });
      expect(pg.coerceInputValue("2024-06-15T10:30:00Z", c)).toBe("2024-06-15");
    });

    it("normalizes cidr values for insert and update operations", () => {
      const c = col({
        name: "network",
        type: "cidr",
        category: "text",
        nativeType: "cidr",
      });

      expect(pg.coerceInputValue("192.168.10.42/24", c)).toBe(
        "192.168.10.0/24",
      );
    });

    it("converts circle JSON payloads to PostgreSQL circle literals", () => {
      const c = col({
        name: "coverage",
        type: "circle",
        category: "spatial",
        nativeType: "circle",
      });

      expect(pg.coerceInputValue('{"x":10.5,"y":-3.25,"radius":7}', c)).toBe(
        "<(10.5,-3.25),7>",
      );
    });
  });

  describe("formatOutputValue", () => {
    it("formats point object as (x, y)", () => {
      const c = col({ name: "p", type: "point", category: "spatial" });
      expect(pg.formatOutputValue({ x: 1, y: 2 }, c)).toBe("(1, 2)");
    });

    it("JSON.stringify for other objects", () => {
      const c = col({ name: "j", type: "json", category: "json" });
      expect(pg.formatOutputValue({ a: 1, b: [2] }, c)).toBe('{"a":1,"b":[2]}');
    });

    it("converts bigint to string", () => {
      const c = col({ name: "n", type: "bigint", category: "integer" });
      expect(pg.formatOutputValue(BigInt(42), c)).toBe("42");
    });

    it("normalizes timestamp display strings for schema and grid rendering", () => {
      const c = col({
        name: "created_at",
        type: "timestamp with time zone",
        category: "datetime",
        nativeType: "timestamp with time zone",
      });

      expect(pg.formatOutputValue("2024-06-15 10:30:45.123456+00:00", c)).toBe(
        "2024-06-15 10:30:45.123+00:00",
      );
    });
  });

  describe("buildFilterCondition", () => {
    it("uses $N for boolean eq", () => {
      const c = col({
        name: "active",
        type: "boolean",
        category: "boolean",
        isBoolean: true,
      });
      const r = pg.buildFilterCondition(c, "eq", "true", 1);
      expect(r?.sql).toBe('"active" = $1');
      expect(r?.params).toEqual([true]);
    });

    it("uses ILIKE for text", () => {
      const c = col({ name: "name", type: "text" });
      const r = pg.buildFilterCondition(c, "like", "foo", 1);
      expect(r?.sql).toContain("ILIKE");
      expect(r?.sql).toContain("$1");
    });

    it("uses ::date cast for date eq", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });
      const r = pg.buildFilterCondition(c, "eq", "2024-06-15", 1);
      expect(r?.sql).toContain("::date");
    });

    it("uses bigint params for bigint equality filters", () => {
      const c = col({
        name: "id",
        type: "bigint",
        category: "integer",
        nativeType: "bigint",
      });

      expect(
        pg.buildFilterCondition(c, "eq", "9223372036854775807", 1),
      ).toEqual({
        sql: '"id" = $1',
        params: [BigInt("9223372036854775807")],
      });
    });

    it("uses textual ILIKE matching for time equality filters", () => {
      const c = col({
        name: "starts_at",
        type: "time with time zone",
        category: "time",
        nativeType: "time with time zone",
      });

      expect(pg.buildFilterCondition(c, "eq", "10:30", 3)).toEqual({
        sql: 'CAST("starts_at" AS TEXT) ILIKE $3',
        params: ["%10:30%"],
      });
    });

    it("uses typed timestamp bounds for datetime range filters", () => {
      const c = col({
        name: "created_at",
        type: "timestamp with time zone",
        category: "datetime",
        nativeType: "timestamp with time zone",
      });

      expect(
        pg.buildFilterCondition(
          c,
          "between",
          ["2024-06-01T00:00:00Z", "2024-06-30T23:59:59Z"],
          2,
        ),
      ).toEqual({
        sql: '"created_at" BETWEEN $2::timestamp AND $3::timestamp',
        params: ["2024-06-01T00:00:00Z", "2024-06-30T23:59:59Z"],
      });
    });
  });

  describe("enrichColumn", () => {
    it.each([
      "point",
      "line",
      "polygon",
      "circle",
    ])("marks geometric %s columns as read-only", (type) => {
      const result = enrichTestColumn(pg, {
        name: "geom_col",
        type,
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(false);
    });

    it("marks interval columns as read-only", () => {
      const result = enrichTestColumn(pg, {
        name: "duration_col",
        type: "interval",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(false);
    });

    it.each([
      "bit(1)",
      "bit varying(8)",
      "varbit(8)",
    ])("keeps PostgreSQL %s columns writable", (type) => {
      const result = enrichTestColumn(pg, {
        name: "bits_col",
        type,
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });

      expect(result.category).toBe("other");
      expect(result.editable).toBe(true);
    });
  });

  describe("describeColumns", () => {
    it("keeps schema metadata aligned with filter and edit capabilities", async () => {
      const describeTableSpy = vi.spyOn(pg, "describeTable").mockResolvedValue([
        {
          name: "tags",
          type: "text[]",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
        },
        {
          name: "duration",
          type: "interval",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
        },
        {
          name: "location",
          type: "point",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
        },
        {
          name: "payload",
          type: "jsonb",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
        },
      ]);

      const result = await pg.describeColumns("appdb", "public", "events");

      expect(result).toEqual([
        expect.objectContaining({
          name: "tags",
          category: "array",
          filterable: false,
          editable: true,
        }),
        expect.objectContaining({
          name: "duration",
          category: "interval",
          filterable: false,
          editable: false,
        }),
        expect.objectContaining({
          name: "location",
          category: "spatial",
          filterable: false,
          editable: false,
        }),
        expect.objectContaining({
          name: "payload",
          category: "json",
          filterable: true,
          editable: true,
        }),
      ]);

      describeTableSpy.mockRestore();
    });
  });

  describe("describeTable", () => {
    it("preserves composite PK ordinals and auto-increment metadata", async () => {
      const query = vi.fn().mockResolvedValue({
        rows: [
          {
            column_name: "tenant_id",
            data_type: "integer",
            is_nullable: false,
            column_default: null,
            identity_kind: null,
            is_pk: true,
            pk_ordinal: 2,
            is_fk: true,
          },
          {
            column_name: "user_id",
            data_type: "integer",
            is_nullable: false,
            column_default:
              "nextval('public.user_roles_user_id_seq'::regclass)",
            identity_kind: null,
            is_pk: true,
            pk_ordinal: 1,
            is_fk: false,
          },
          {
            column_name: "payload",
            data_type: "jsonb",
            is_nullable: true,
            column_default: null,
            identity_kind: null,
            is_pk: false,
            pk_ordinal: null,
            is_fk: false,
          },
        ],
      });

      (pg as unknown as { pool: { query: typeof query } }).pool = { query };

      const result = await pg.describeTable("appdb", "public", "user_roles");
      const [sql, params] = query.mock.calls[0] ?? [];

      expect(sql).toContain("WITH ORDINALITY");
      expect(sql).toContain("pk.pk_ordinal");
      expect(params).toEqual(["public", "user_roles"]);
      expect(result).toEqual([
        {
          name: "tenant_id",
          type: "integer",
          nullable: false,
          defaultValue: undefined,
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          isForeignKey: true,
          isAutoIncrement: false,
        },
        {
          name: "user_id",
          type: "integer",
          nullable: false,
          defaultValue: undefined,
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          isForeignKey: false,
          isAutoIncrement: true,
        },
        {
          name: "payload",
          type: "jsonb",
          nullable: true,
          defaultValue: undefined,
          isPrimaryKey: false,
          primaryKeyOrdinal: undefined,
          isForeignKey: false,
          isAutoIncrement: false,
        },
      ]);
    });

    it("treats GENERATED AS IDENTITY columns as auto-increment", async () => {
      const query = vi.fn().mockResolvedValue({
        rows: [
          {
            column_name: "id",
            data_type: "bigint",
            is_nullable: false,
            column_default: null,
            identity_kind: "d",
            is_pk: true,
            pk_ordinal: 1,
            is_fk: false,
          },
        ],
      });

      (pg as unknown as { pool: { query: typeof query } }).pool = { query };

      await expect(
        pg.describeTable("appdb", "public", "events"),
      ).resolves.toEqual([
        {
          name: "id",
          type: "bigint",
          nullable: false,
          defaultValue: undefined,
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          isForeignKey: false,
          isAutoIncrement: true,
        },
      ]);
    });
  });

  describe("getIndexes", () => {
    it("preserves actual index key order from catalog ordinality", async () => {
      const query = vi.fn().mockResolvedValue({
        rows: [
          {
            name: "user_roles_lookup_idx",
            unique: false,
            primary: false,
            column: "user_id",
          },
          {
            name: "user_roles_lookup_idx",
            unique: false,
            primary: false,
            column: "tenant_id",
          },
          {
            name: "user_roles_pkey",
            unique: true,
            primary: true,
            column: "tenant_id",
          },
          {
            name: "user_roles_pkey",
            unique: true,
            primary: true,
            column: "user_id",
          },
        ],
      });

      (pg as unknown as { pool: { query: typeof query } }).pool = { query };

      const result = await pg.getIndexes("appdb", "public", "user_roles");
      const [sql, params] = query.mock.calls[0] ?? [];

      expect(sql).toContain("WITH ORDINALITY");
      expect(sql).toContain("idx.key_ordinal");
      expect(params).toEqual(["public", "user_roles"]);
      expect(result).toEqual([
        {
          name: "user_roles_lookup_idx",
          columns: ["user_id", "tenant_id"],
          unique: false,
          primary: false,
        },
        {
          name: "user_roles_pkey",
          columns: ["tenant_id", "user_id"],
          unique: true,
          primary: true,
        },
      ]);
    });

    it("keeps expression index entries visible in schema metadata", async () => {
      const query = vi.fn().mockResolvedValue({
        rows: [
          {
            name: "users_email_lower_idx",
            unique: true,
            primary: false,
            column: "lower(email)",
          },
        ],
      });

      (pg as unknown as { pool: { query: typeof query } }).pool = { query };

      const result = await pg.getIndexes("appdb", "public", "users");
      const [sql] = query.mock.calls[0] ?? [];

      expect(sql).toContain("pg_get_indexdef");
      expect(result).toEqual([
        {
          name: "users_email_lower_idx",
          columns: ["lower(email)"],
          unique: true,
          primary: false,
        },
      ]);
    });
  });

  describe("getForeignKeys", () => {
    it("pairs local and referenced columns by ordinality", async () => {
      const query = vi.fn().mockResolvedValue({
        rows: [
          {
            constraint_name: "user_roles_account_fk",
            column_name: "tenant_id",
            ref_schema: "public",
            ref_table: "accounts",
            ref_column: "tenant_id",
          },
          {
            constraint_name: "user_roles_account_fk",
            column_name: "account_id",
            ref_schema: "public",
            ref_table: "accounts",
            ref_column: "id",
          },
        ],
      });

      (pg as unknown as { pool: { query: typeof query } }).pool = { query };

      const result = await pg.getForeignKeys("appdb", "public", "user_roles");
      const [sql, params] = query.mock.calls[0] ?? [];

      expect(sql).toContain("unnest(con.conkey, con.confkey) WITH ORDINALITY");
      expect(params).toEqual(["public", "user_roles"]);
      expect(result).toEqual([
        {
          constraintName: "user_roles_account_fk",
          column: "tenant_id",
          referencedSchema: "public",
          referencedTable: "accounts",
          referencedColumn: "tenant_id",
        },
        {
          constraintName: "user_roles_account_fk",
          column: "account_id",
          referencedSchema: "public",
          referencedTable: "accounts",
          referencedColumn: "id",
        },
      ]);
    });
  });

  describe("getCreateTableDDL", () => {
    it("emits a table-level PRIMARY KEY clause for composite keys", async () => {
      const query = vi
        .fn()
        .mockResolvedValueOnce({ rows: [{ table_type: "BASE TABLE" }] })
        .mockResolvedValueOnce({
          rows: [
            {
              column_name: "tenant_id",
              data_type: "integer",
              is_nullable: false,
              column_default: null,
              identity_kind: null,
            },
            {
              column_name: "user_id",
              data_type: "integer",
              is_nullable: false,
              column_default: null,
              identity_kind: null,
            },
            {
              column_name: "display_name",
              data_type: '"DisplayNameDomain"',
              is_nullable: false,
              column_default: "'guest'::text",
              identity_kind: null,
            },
          ],
        })
        .mockResolvedValueOnce({
          rows: [
            { column_name: "tenant_id", key_ordinal: 1 },
            { column_name: "user_id", key_ordinal: 2 },
          ],
        });

      (pg as unknown as { pool: { query: typeof query } }).pool = { query };

      const ddl = await pg.getCreateTableDDL("appdb", "public", "user_roles");

      expect(ddl).toBe(
        `CREATE TABLE "public"."user_roles" (\n  "tenant_id" integer,\n  "user_id" integer,\n  "display_name" "DisplayNameDomain" NOT NULL DEFAULT 'guest'::text,\n  PRIMARY KEY ("tenant_id", "user_id")\n);`,
      );
      expect(ddl).not.toContain('"tenant_id" integer PRIMARY KEY');
      expect(ddl).not.toContain('"user_id" integer PRIMARY KEY');
      expect(query).toHaveBeenCalledTimes(3);
    });

    it("preserves PostgreSQL identity clauses in generated DDL", async () => {
      const query = vi
        .fn()
        .mockResolvedValueOnce({ rows: [{ table_type: "BASE TABLE" }] })
        .mockResolvedValueOnce({
          rows: [
            {
              column_name: "id",
              data_type: "bigint",
              is_nullable: false,
              column_default: null,
              identity_kind: "a",
            },
            {
              column_name: "title",
              data_type: "text",
              is_nullable: false,
              column_default: null,
              identity_kind: null,
            },
          ],
        })
        .mockResolvedValueOnce({
          rows: [{ column_name: "id", key_ordinal: 1 }],
        });

      (pg as unknown as { pool: { query: typeof query } }).pool = { query };

      await expect(
        pg.getCreateTableDDL("appdb", "public", "articles"),
      ).resolves.toBe(
        `CREATE TABLE "public"."articles" (\n  "id" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n  "title" text NOT NULL\n);`,
      );
    });
  });
});

// ────────────────────────────────────────────
// MySQL Driver
// ────────────────────────────────────────────

describe("MySQLDriver", () => {
  describe("mapTypeCategory", () => {
    it.each([
      ["tinyint(1)", "boolean"],
      ["bit(1)", "boolean"],
      ["bool", "boolean"],
      ["boolean", "boolean"],
      ["tinyint(4)", "integer"],
      ["bit(8)", "integer"],
      ["year", "integer"],
      ["int", "integer"],
      ["int unsigned", "integer"],
      ["bigint unsigned zerofill", "integer"],
      ["integer", "integer"],
      ["mediumint", "integer"],
      ["bigint", "integer"],
      ["decimal(10,2)", "decimal"],
      ["numeric(5,3)", "decimal"],
      ["float", "float"],
      ["double unsigned", "float"],
      ["double", "float"],
      ["real", "float"],
      ["json", "json"],
      ["point", "spatial"],
      ["geometry", "spatial"],
      ["enum('a','b')", "enum"],
      ["set('x','y')", "enum"],
      ["text", "text"],
      ["tinytext", "text"],
      ["mediumtext", "text"],
      ["longtext", "text"],
      ["varchar(100)", "text"],
      ["char(10)", "text"],
      ["binary(4)", "binary"],
      ["varbinary(16)", "binary"],
      ["tinyblob", "binary"],
      ["blob", "binary"],
      ["mediumblob", "binary"],
      ["longblob", "binary"],
      ["date", "date"],
      ["datetime", "datetime"],
      ["datetime(6)", "datetime"],
      ["timestamp", "datetime"],
      ["timestamp(6)", "datetime"],
      ["time", "time"],
    ] as const)("maps %s → %s", (input, expected) => {
      expect(my.mapTypeCategory(input)).toBe(expected);
    });
  });

  describe("isBooleanType", () => {
    it("true for tinyint(1), bit(1), boolean", () => {
      expect(my.isBooleanType("tinyint(1)")).toBe(true);
      expect(my.isBooleanType("tinyint(1) unsigned")).toBe(true);
      expect(my.isBooleanType("bit(1)")).toBe(true);
      expect(my.isBooleanType("boolean")).toBe(true);
    });
    it("false for tinyint(4), bit(8)", () => {
      expect(my.isBooleanType("tinyint(4)")).toBe(false);
      expect(my.isBooleanType("bit(8)")).toBe(false);
    });
  });

  describe("isDatetimeWithTime", () => {
    it("true for datetime, timestamp", () => {
      expect(my.isDatetimeWithTime("datetime")).toBe(true);
      expect(my.isDatetimeWithTime("datetime(6)")).toBe(true);
      expect(my.isDatetimeWithTime("timestamp")).toBe(true);
      expect(my.isDatetimeWithTime("timestamp(6)")).toBe(true);
    });
    it("false for date, time", () => {
      expect(my.isDatetimeWithTime("date")).toBe(false);
      expect(my.isDatetimeWithTime("time")).toBe(false);
    });
  });

  describe("quoteIdentifier (backticks)", () => {
    it("wraps in backticks", () => {
      expect(my.quoteIdentifier("users")).toBe("`users`");
    });
    it("escapes embedded backticks", () => {
      expect(my.quoteIdentifier("my`col")).toBe("`my``col`");
    });
  });

  describe("qualifiedTableName", () => {
    it("uses database.table, ignores schema", () => {
      expect(my.qualifiedTableName("mydb", "ignored", "users")).toBe(
        "`mydb`.`users`",
      );
    });
    it("returns only table if database empty", () => {
      expect(my.qualifiedTableName("", "", "users")).toBe("`users`");
    });
  });

  describe("buildInsertValueExpr", () => {
    it("returns ? for inserts", () => {
      const c = col({
        name: "a",
        type: "int",
        category: "integer",
        nativeType: "int",
      });
      expect(my.buildInsertValueExpr(c, 1)).toBe("?");
    });
  });

  describe("coerceInputValue", () => {
    it("returns 1/0 for boolean true/false", () => {
      const c = col({
        name: "b",
        type: "tinyint(1)",
        category: "boolean",
        isBoolean: true,
      });
      expect(my.coerceInputValue("true", c)).toBe(1);
      expect(my.coerceInputValue("false", c)).toBe(0);
    });

    it("converts ISO datetime to MySQL format", () => {
      const c = col({
        name: "dt",
        type: "datetime",
        category: "datetime",
        nativeType: "datetime",
      });
      const result = my.coerceInputValue("2024-06-15T10:30:45Z", c);
      expect(result).toBe("2024-06-15 10:30:45");
    });

    it("preserves ms in datetime conversion", () => {
      const c = col({
        name: "dt",
        type: "datetime",
        category: "datetime",
        nativeType: "datetime",
      });
      const result = my.coerceInputValue("2024-06-15T10:30:45.123Z", c);
      expect(result).toBe("2024-06-15 10:30:45.123");
    });

    it("preserves up to 6 fractional digits for datetime conversion", () => {
      const c = col({
        name: "dt",
        type: "datetime(6)",
        category: "datetime",
        nativeType: "datetime(6)",
      });
      const result = my.coerceInputValue("2024-06-15T10:30:45.123456Z", c);
      expect(result).toBe("2024-06-15 10:30:45.123456");
    });

    it("normalizes pre-epoch timezone-aware datetimes without corrupting microseconds", () => {
      const c = col({
        name: "dt",
        type: "datetime(6)",
        category: "datetime",
        nativeType: "datetime(6)",
      });
      const result = my.coerceInputValue("1969-12-31T23:59:59.999999Z", c);
      expect(result).toBe("1969-12-31 23:59:59.999999");
    });

    it("normalizes ISO datetime input to date-only text for date columns", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });
      expect(my.coerceInputValue("2024-06-15T10:30:45Z", c)).toBe("2024-06-15");
    });

    it("rejects impossible DATE inputs instead of normalizing them", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });

      expect(() => my.coerceInputValue("2024-02-30", c)).toThrow(
        "expects a valid DATE value",
      );
    });

    it("rejects impossible DATETIME inputs instead of normalizing them", () => {
      const c = col({
        name: "dt",
        type: "datetime(6)",
        category: "datetime",
        nativeType: "datetime(6)",
      });

      expect(() =>
        my.coerceInputValue("2024-02-30 25:30:45.123456", c),
      ).toThrow("expects a valid DATETIME value");
    });

    it.each([
      ["text", "text"],
      ["json", "json"],
      ["kind", "enum('a','b')"],
    ] as const)("does not rewrite ISO-looking strings for %s columns", (name, nativeType) => {
      const category = nativeType.startsWith("enum")
        ? "enum"
        : (name as "text" | "json");
      const c = col({
        name,
        type: nativeType,
        category,
        nativeType,
      });
      expect(my.coerceInputValue("2024-06-15T10:30:45Z", c)).toBe(
        "2024-06-15T10:30:45Z",
      );
    });

    it("parses bit values", () => {
      const c = col({
        name: "flags",
        type: "bit(8)",
        category: "integer",
        nativeType: "bit(8)",
        isBoolean: false,
      });
      expect(my.coerceInputValue("5", c)).toBe(5);
    });
  });

  describe("formatOutputValue", () => {
    it("converts Buffer to hex", () => {
      const c = col({ name: "d", type: "binary(4)", category: "binary" });
      expect(my.formatOutputValue(Buffer.from([0xde, 0xad]), c)).toBe(
        "\\xdead",
      );
    });

    it("converts bigint to string", () => {
      const c = col({ name: "n", type: "bigint", category: "integer" });
      expect(my.formatOutputValue(BigInt(999), c)).toBe("999");
    });

    it("JSON.stringify for objects", () => {
      const c = col({ name: "j", type: "json", category: "json" });
      expect(my.formatOutputValue({ x: 1 }, c)).toBe('{"x":1}');
    });

    it("preserves 6-digit datetime strings for display", () => {
      const c = col({
        name: "dt",
        type: "datetime(6)",
        category: "datetime",
        nativeType: "datetime(6)",
      });
      expect(my.formatOutputValue("2024-06-15 10:30:45.123456", c)).toBe(
        "2024-06-15 10:30:45.123456",
      );
    });
  });

  describe("buildFilterCondition", () => {
    it("uses integer 1/0 for boolean", () => {
      const c = col({
        name: "active",
        type: "tinyint(1)",
        category: "boolean",
        isBoolean: true,
      });
      const r = my.buildFilterCondition(c, "eq", "true", 1);
      expect(r?.params).toEqual([1]);
    });

    it("does not expose spatial filtering when metadata marks the column non-filterable", () => {
      const c = col({
        name: "p",
        type: "point",
        category: "spatial",
        nativeType: "point",
        filterable: false,
      });
      const r = my.buildFilterCondition(c, "like", "POINT", 1);
      expect(r).toBeNull();
    });

    it("uses HEX LIKE for binary", () => {
      const c = col({
        name: "d",
        type: "binary(4)",
        category: "binary",
        nativeType: "binary(4)",
      });
      const r = my.buildFilterCondition(c, "like", "0xDEAD", 1);
      expect(r?.sql).toContain("HEX");
    });

    it("uses CAST AS CHAR LIKE for text", () => {
      const c = col({ name: "name", type: "text", nativeType: "text" });
      const r = my.buildFilterCondition(c, "like", "foo", 1);
      expect(r?.sql).toContain("CAST");
      expect(r?.sql).toContain("CHAR");
      expect(r?.sql).toContain("LIKE");
    });

    it("keeps FLOAT filters on the textual fallback path", () => {
      const c = col({
        name: "score",
        type: "float",
        category: "float",
        nativeType: "float",
      });
      const r = my.buildFilterCondition(c, "eq", "0.1", 1);
      expect(r?.sql).toContain("ABS");
      expect(r?.sql).toContain("GREATEST");
      expect(r?.params).toEqual([0.1, 0.000001, 0.1, 0.000001]);
    });

    it("keeps DOUBLE filters on the textual fallback path", () => {
      const c = col({
        name: "score",
        type: "double",
        category: "float",
        nativeType: "double",
      });
      const r = my.buildFilterCondition(c, "eq", "0.1", 1);
      expect(r?.sql).toContain("ABS");
      expect(r?.sql).toContain("GREATEST");
      expect(r?.params).toEqual([0.1, 0.000001, 0.1, 0.000001]);
    });

    it("normalizes equivalent FLOAT inputs before textual matching", () => {
      const c = col({
        name: "score",
        type: "float",
        category: "float",
        nativeType: "float",
      });
      const r = my.buildFilterCondition(c, "eq", "1.250", 1);
      expect(r?.params).toEqual([1.25, 0.000001, 1.25, 0.000001]);
    });

    it("uses typed numeric comparisons for YEAR range filters", () => {
      const c = col({
        name: "release_year",
        type: "year",
        category: "integer",
        nativeType: "year",
      });
      const r = my.buildFilterCondition(c, "gt", "2024", 1);
      expect(r).toEqual({
        sql: "`release_year` > ?",
        params: [2024],
      });
    });

    it("limits approximate float tolerance handling to eq/neq", () => {
      const c = col({
        name: "score",
        type: "float",
        category: "float",
        nativeType: "float",
      });
      const r = my.buildFilterCondition(c, "gt", "0.1", 1);
      expect(r).toEqual({
        sql: "`score` > ?",
        params: [0.1],
      });
    });

    it("uses typed comparison for date equality filters", () => {
      const c = col({
        name: "created_on",
        type: "date",
        category: "date",
        nativeType: "date",
      });
      const r = my.buildFilterCondition(c, "eq", "2026-04-15", 1);
      expect(r?.sql).toBe("`created_on` = CAST(? AS DATE)");
      expect(r?.params).toEqual(["2026-04-15"]);
    });
  });

  describe("describeTable", () => {
    it("uses constraint metadata instead of COLUMN_KEY=MUL and preserves PK ordinals", async () => {
      const query = vi.fn().mockResolvedValue([
        [
          {
            COLUMN_NAME: "lookup_id",
            COLUMN_TYPE: "int unsigned",
            IS_NULLABLE: "NO",
            COLUMN_DEFAULT: null,
            COLUMN_KEY: "MUL",
            EXTRA: "",
            IS_PRIMARY_KEY: 0,
            PRIMARY_KEY_ORDINAL: null,
            IS_FOREIGN_KEY: 0,
          },
          {
            COLUMN_NAME: "tenant_id",
            COLUMN_TYPE: "int",
            IS_NULLABLE: "NO",
            COLUMN_DEFAULT: null,
            EXTRA: "auto_increment",
            IS_PRIMARY_KEY: 1,
            PRIMARY_KEY_ORDINAL: 2,
            IS_FOREIGN_KEY: 1,
          },
        ],
      ]);

      (my as unknown as { pool: { query: typeof query } }).pool = { query };

      const result = await my.describeTable("appdb", "", "orders");

      expect(query).toHaveBeenCalledWith(
        expect.stringContaining("TABLE_CONSTRAINTS"),
        ["appdb", "orders", "appdb", "orders", "appdb", "orders"],
      );
      expect(result).toEqual([
        expect.objectContaining({
          name: "lookup_id",
          isPrimaryKey: false,
          primaryKeyOrdinal: undefined,
          isForeignKey: false,
        }),
        expect.objectContaining({
          name: "tenant_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          isForeignKey: true,
          isAutoIncrement: true,
        }),
      ]);
    });
  });

  describe("query parsing", () => {
    it("decodes BIT values wider than 48 bits to bigint without truncation", () => {
      const result = (
        my as unknown as {
          _parseQueryResult: (
            rawRows: unknown,
            fields: unknown[],
            executionTimeMs: number,
          ) => {
            rows: Array<Record<string, unknown>>;
          };
        }
      )._parseQueryResult(
        [[Buffer.from([0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef])]],
        [{ name: "flags", type: 16, length: 64 }],
        1,
      );

      expect(result.rows[0]?.__col_0).toBe(BigInt("0x0123456789abcdef"));
    });
  });
});

// ────────────────────────────────────────────
// MSSQL Driver
// ────────────────────────────────────────────

describe("MSSQLDriver", () => {
  describe("mapTypeCategory", () => {
    it.each([
      ["bit", "boolean"],
      ["tinyint", "integer"],
      ["smallint", "integer"],
      ["int", "integer"],
      ["bigint", "integer"],
      ["decimal(10,2)", "decimal"],
      ["numeric(5,3)", "decimal"],
      ["money", "decimal"],
      ["smallmoney", "decimal"],
      ["real", "float"],
      ["float", "float"],
      ["date", "date"],
      ["time(7)", "time"],
      ["datetime", "datetime"],
      ["datetime2(7)", "datetime"],
      ["datetimeoffset(7)", "datetime"],
      ["smalldatetime", "datetime"],
      ["char(10)", "text"],
      ["nchar(10)", "text"],
      ["varchar(50)", "text"],
      ["nvarchar(50)", "text"],
      ["varchar(max)", "text"],
      ["text", "text"],
      ["ntext", "text"],
      ["xml", "text"],
      ["binary(16)", "binary"],
      ["varbinary(max)", "binary"],
      ["image", "binary"],
      ["timestamp", "binary"],
      ["rowversion", "binary"],
      ["uniqueidentifier", "uuid"],
      ["geography", "spatial"],
      ["geometry", "spatial"],
      ["hierarchyid", "other"],
      ["sql_variant", "other"],
    ] as const)("maps %s → %s", (input, expected) => {
      expect(ms.mapTypeCategory(input)).toBe(expected);
    });
  });

  describe("isBooleanType", () => {
    it("true only for bit", () => {
      expect(ms.isBooleanType("bit")).toBe(true);
    });
    it("false for others", () => {
      expect(ms.isBooleanType("tinyint")).toBe(false);
      expect(ms.isBooleanType("boolean")).toBe(false);
    });
  });

  describe("quoteIdentifier (brackets)", () => {
    it("wraps in square brackets", () => {
      expect(ms.quoteIdentifier("users")).toBe("[users]");
    });
    it("escapes embedded ]", () => {
      expect(ms.quoteIdentifier("col]name")).toBe("[col]]name]");
    });
  });

  describe("qualifiedTableName", () => {
    it("uses [schema].[table]", () => {
      expect(ms.qualifiedTableName("mydb", "dbo", "users")).toBe(
        "[dbo].[users]",
      );
    });
  });

  describe("buildPagination (OFFSET/FETCH)", () => {
    it("uses OFFSET/FETCH syntax", () => {
      const r = ms.buildPagination(10, 25, 1);
      expect(r.sql).toBe("OFFSET ? ROWS FETCH NEXT ? ROWS ONLY");
      expect(r.params).toEqual([10, 25]);
    });
  });

  describe("mutation placeholders", () => {
    it("casts decimal inserts to the declared MSSQL type", () => {
      const c = col({
        name: "amount",
        type: "decimal(10,2)",
        category: "decimal",
        nativeType: "decimal(10,2)",
      });

      expect(ms.buildInsertValueExpr(c, 1)).toBe("CAST(? AS decimal(10,2))");
      expect(ms.buildSetExpr(c, 1)).toBe("[amount] = CAST(? AS decimal(10,2))");
    });
  });

  describe("buildOrderByDefault", () => {
    it("falls back to (SELECT NULL) when no columns", () => {
      expect(ms.buildOrderByDefault([])).toBe("ORDER BY (SELECT NULL)");
    });
    it("uses first PK column", () => {
      const cols = [
        col({ name: "id", type: "int", isPrimaryKey: true }),
        col({ name: "name", type: "text" }),
      ];
      expect(ms.buildOrderByDefault(cols)).toContain("[id]");
    });

    it("uses all PK columns in ordinal order for composite keys", () => {
      const cols = [
        col({
          name: "tenant_id",
          type: "int",
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
        }),
        col({
          name: "user_id",
          type: "int",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
        }),
      ];

      expect(ms.buildOrderByDefault(cols)).toBe(
        "ORDER BY [user_id], [tenant_id]",
      );
    });
  });

  describe("coerceInputValue", () => {
    it("returns true/false for boolean", () => {
      const c = col({
        name: "b",
        type: "bit",
        category: "boolean",
        isBoolean: true,
      });
      expect(ms.coerceInputValue("true", c)).toBe(true);
      expect(ms.coerceInputValue("false", c)).toBe(false);
    });

    it("converts hex to Buffer for binary", () => {
      const c = col({
        name: "d",
        type: "varbinary(16)",
        category: "binary",
        nativeType: "varbinary(16)",
      });
      const r = ms.coerceInputValue("0xdead", c);
      expect(Buffer.isBuffer(r)).toBe(true);
    });

    it("keeps time strings for time columns", () => {
      const c = col({
        name: "t",
        type: "time(7)",
        category: "time",
        nativeType: "time(7)",
      });
      const r = ms.coerceInputValue("10:30:00", c);
      expect(r).toBe("10:30:00");
    });

    it("coerces bigint strings using column metadata", () => {
      const c = col({
        name: "id",
        type: "bigint",
        category: "integer",
        nativeType: "bigint",
      });

      expect(ms.coerceInputValue("9223372036854775807", c)).toBe(
        BigInt("9223372036854775807"),
      );
    });

    it("normalizes date inputs for date columns", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });

      expect(ms.coerceInputValue("2026-04-15 00:00:00 +00:00", c)).toBe(
        "2026-04-15",
      );
    });

    it("preserves leading and trailing whitespace for plain text values", () => {
      const c = col({
        name: "display_name",
        type: "nvarchar(100)",
        category: "text",
        nativeType: "nvarchar(100)",
      });

      expect(ms.coerceInputValue("  Alice  ", c)).toBe("  Alice  ");
    });
  });

  describe("describeTable", () => {
    it("preserves composite primary key ordinals from metadata", async () => {
      const request = {
        query: vi.fn().mockResolvedValue({
          recordset: [
            {
              COLUMN_NAME: "tenant_id",
              DATA_TYPE: "int",
              max_length: 4,
              precision: 10,
              scale: 0,
              IS_NULLABLE: 0,
              is_identity: 0,
              COLUMN_DEFAULT: null,
              IS_PK: 1,
              PK_ORDINAL: 2,
              IS_FK: 0,
            },
            {
              COLUMN_NAME: "user_id",
              DATA_TYPE: "int",
              max_length: 4,
              precision: 10,
              scale: 0,
              IS_NULLABLE: 0,
              is_identity: 0,
              COLUMN_DEFAULT: null,
              IS_PK: 1,
              PK_ORDINAL: 1,
              IS_FK: 1,
            },
          ],
        }),
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      const result = await ms.describeTable("appdb", "dbo", "user_roles");

      expect(result).toEqual([
        expect.objectContaining({
          name: "tenant_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          isForeignKey: false,
        }),
        expect.objectContaining({
          name: "user_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          isForeignKey: true,
        }),
      ]);
    });
  });

  describe("formatOutputValue", () => {
    it("formats Date as YYYY-MM-DD for date columns", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });
      expect(ms.formatOutputValue(new Date("2024-06-15T00:00:00Z"), c)).toBe(
        "2024-06-15",
      );
    });

    it("formats Date as time string for time columns", () => {
      const c = col({
        name: "t",
        type: "time(7)",
        category: "time",
        nativeType: "time(7)",
      });
      const d = new Date("1970-01-01T10:30:45.123Z");
      const result = ms.formatOutputValue(d, c);
      expect(result).toContain("10:30:45");
    });

    it("formats datetimeoffset with +00:00", () => {
      const c = col({
        name: "dto",
        type: "datetimeoffset(7)",
        category: "datetime",
        nativeType: "datetimeoffset(7)",
      });
      const d = new Date("2024-06-15T10:30:00Z");
      const result = ms.formatOutputValue(d, c) as string;
      expect(result).toContain("+00:00");
      expect(result).toContain("2024-06-15");
    });

    it("converts bigint to string", () => {
      const c = col({ name: "n", type: "bigint", category: "integer" });
      expect(ms.formatOutputValue(BigInt(42), c)).toBe("42");
    });
  });

  describe("query execution", () => {
    it("creates a dedicated ConnectionPool per driver instance", async () => {
      const fakeConnectedPool = {
        connected: true,
        close: vi.fn(),
      } as unknown as mssql.ConnectionPool;
      const connectInstances: mssql.ConnectionPool[] = [];
      const connectSpy = vi
        .spyOn(mssql.ConnectionPool.prototype, "connect")
        .mockImplementation(async function (this: mssql.ConnectionPool) {
          connectInstances.push(this);
          return fakeConnectedPool;
        });
      const onSpy = vi
        .spyOn(mssql.ConnectionPool.prototype, "on")
        .mockImplementation(function (this: mssql.ConnectionPool) {
          return this;
        });

      try {
        const firstDriver = new MSSQLDriver(msConfig);
        const secondDriver = new MSSQLDriver({ ...msConfig, id: "t-2" });

        await firstDriver.connect();
        await secondDriver.connect();

        expect(connectSpy).toHaveBeenCalledTimes(2);
        expect(connectInstances).toHaveLength(2);
        expect(connectInstances[0]).not.toBe(connectInstances[1]);
        expect(onSpy).toHaveBeenCalledWith("error", expect.any(Function));
        expect(firstDriver.isConnected()).toBe(true);
        expect(secondDriver.isConnected()).toBe(true);
      } finally {
        connectSpy.mockRestore();
        onSpy.mockRestore();
      }
    });

    it("uses arrayRowMode and preserves duplicate column names", async () => {
      const query = vi.fn().mockResolvedValue({
        recordset: [[1, "Alice", 2]],
        rowsAffected: [1],
        columns: [
          [
            {
              index: 0,
              name: "id",
              type: mssql.Int,
              nullable: false,
              identity: false,
              readOnly: false,
            },
            {
              index: 1,
              name: "name",
              type: mssql.NVarChar,
              nullable: false,
              identity: false,
              readOnly: false,
            },
            {
              index: 2,
              name: "id",
              type: mssql.Int,
              nullable: false,
              identity: false,
              readOnly: false,
            },
          ],
        ],
      });
      const request = {
        arrayRowMode: false,
        input: vi.fn(),
        query,
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      const result = await (
        ms as unknown as {
          _executeBatch: (
            sql: string,
            params?: unknown[],
          ) => Promise<{
            columns: string[];
            rows: Array<Record<string, unknown>>;
          }>;
        }
      )._executeBatch("SELECT 1", []);

      expect(request.arrayRowMode).toBe(true);
      expect(result.columns).toEqual(["id", "name", "id"]);
      expect(result.rows).toEqual([
        { __col_0: 1, __col_1: "Alice", __col_2: 2 },
      ]);
    });

    it("binds temporal strings using explicit MSSQL parameter types", async () => {
      const input = vi.fn();
      const query = vi.fn().mockResolvedValue({
        recordset: [],
        rowsAffected: [0],
        columns: [[]],
      });
      const request = {
        arrayRowMode: false,
        input,
        query,
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      await (
        ms as unknown as {
          _executeBatch: (sql: string, params?: unknown[]) => Promise<unknown>;
        }
      )._executeBatch("SELECT ?, ?, ?", [
        "2026-04-15",
        "10:30:00.1234",
        "2026-04-15T10:30:00+02:00",
      ]);

      expect(input).toHaveBeenNthCalledWith(
        1,
        "p1",
        expect.anything(),
        "2026-04-15",
      );
      expect(input).toHaveBeenNthCalledWith(
        3,
        "p3",
        expect.anything(),
        "2026-04-15T10:30:00+02:00",
      );

      const timeBinding = input.mock.calls[1]?.[2];
      expectMssqlTimeBinding(timeBinding, {
        hours: 10,
        minutes: 30,
        seconds: 0,
        milliseconds: 123,
        nanosecondDelta: 0.0004,
      });
      expect(
        input.mock.calls.map(([, sqlType]) => mssqlTypeName(sqlType)),
      ).toEqual(["Date", "Time", "DateTimeOffset"]);
      expect((input.mock.calls[1]?.[1] as { scale?: number }).scale).toBe(4);
    });

    it("binds naive MSSQL datetime strings as UTC dates", async () => {
      const input = vi.fn();
      const query = vi.fn().mockResolvedValue({
        recordset: [],
        rowsAffected: [0],
        columns: [[]],
      });
      const request = {
        arrayRowMode: false,
        input,
        query,
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      await (
        ms as unknown as {
          _executeBatch: (sql: string, params?: unknown[]) => Promise<unknown>;
        }
      )._executeBatch("SELECT ?, ?", [
        "2026-04-15 12:34:56.1234",
        "2026-04-15T12:34:56+02:00",
      ]);

      expect(
        input.mock.calls.map(([, sqlType]) => mssqlTypeName(sqlType)),
      ).toEqual(["DateTime2", "DateTimeOffset"]);
      expect(
        input.mock.calls.map(
          ([, sqlType]) => (sqlType as { scale?: number }).scale,
        ),
      ).toEqual([4, 7]);

      expectMssqlDatetimeBinding(input.mock.calls[0]?.[2], {
        year: 2026,
        month: 4,
        day: 15,
        hours: 12,
        minutes: 34,
        seconds: 56,
        milliseconds: 123,
        nanosecondDelta: 0.0004,
      });
      expect(input.mock.calls[1]?.[2]).toBe("2026-04-15T12:34:56+02:00");
    });

    it("rejects invalid naive MSSQL datetime strings instead of rolling them forward", async () => {
      const request = {
        arrayRowMode: false,
        input: vi.fn(),
        query: vi.fn(),
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      await expect(
        (
          ms as unknown as {
            _executeBatch: (
              sql: string,
              params?: unknown[],
            ) => Promise<unknown>;
          }
        )._executeBatch("SELECT ?", ["2025-02-29 00:00:00"]),
      ).rejects.toThrow("Invalid datetime.");

      expect(request.input).not.toHaveBeenCalled();
      expect(request.query).not.toHaveBeenCalled();
    });

    it("accepts valid early-year MSSQL datetime2 literals", async () => {
      const input = vi.fn();
      const query = vi.fn().mockResolvedValue({
        recordset: [],
        rowsAffected: [0],
        columns: [[]],
      });
      const request = {
        arrayRowMode: false,
        input,
        query,
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      await (
        ms as unknown as {
          _executeBatch: (sql: string, params?: unknown[]) => Promise<unknown>;
        }
      )._executeBatch("SELECT ?", ["0001-01-01 00:00:00"]);

      expect(mssqlTypeName(input.mock.calls[0]?.[1])).toBe("DateTime2");
      expectMssqlDatetimeBinding(input.mock.calls[0]?.[2], {
        year: 1,
        month: 1,
        day: 1,
        hours: 0,
        minutes: 0,
        seconds: 0,
        milliseconds: 0,
      });
    });

    it("binds MSSQL time values as UTC dates across supported scales", async () => {
      const input = vi.fn();
      const query = vi.fn().mockResolvedValue({
        recordset: [],
        rowsAffected: [0],
        columns: [[]],
      });
      const request = {
        arrayRowMode: false,
        input,
        query,
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      await (
        ms as unknown as {
          _executeBatch: (sql: string, params?: unknown[]) => Promise<unknown>;
        }
      )._executeBatch("SELECT ?, ?, ?", [
        "10:30:00",
        "10:30:00.123",
        "10:30:00.1234567",
      ]);

      expect(
        input.mock.calls.map(([, sqlType]) => mssqlTypeName(sqlType)),
      ).toEqual(["Time", "Time", "Time"]);
      expect(
        input.mock.calls.map(
          ([, sqlType]) => (sqlType as { scale?: number }).scale,
        ),
      ).toEqual([7, 3, 7]);

      expectMssqlTimeBinding(input.mock.calls[0]?.[2], {
        hours: 10,
        minutes: 30,
        seconds: 0,
        milliseconds: 0,
      });
      expectMssqlTimeBinding(input.mock.calls[1]?.[2], {
        hours: 10,
        minutes: 30,
        seconds: 0,
        milliseconds: 123,
      });
      expectMssqlTimeBinding(input.mock.calls[2]?.[2], {
        hours: 10,
        minutes: 30,
        seconds: 0,
        milliseconds: 123,
        nanosecondDelta: 0.0004567,
      });
    });
  });

  describe("getCreateTableDDL", () => {
    it("builds composite primary keys in ordinal order with escaped identifiers", async () => {
      const request = {
        query: vi.fn().mockResolvedValue({
          recordset: [
            {
              COLUMN_NAME: "tenant_id",
              DATA_TYPE: "int",
              max_length: 4,
              precision: 10,
              scale: 0,
              IS_NULLABLE: 0,
              is_identity: 0,
              COLUMN_DEFAULT: null,
              IS_PK: 1,
              PK_ORDINAL: 2,
            },
            {
              COLUMN_NAME: "user_id",
              DATA_TYPE: "int",
              max_length: 4,
              precision: 10,
              scale: 0,
              IS_NULLABLE: 0,
              is_identity: 0,
              COLUMN_DEFAULT: null,
              IS_PK: 1,
              PK_ORDINAL: 1,
            },
          ],
        }),
      };

      (ms as unknown as { pool: { request: () => typeof request } }).pool = {
        request: () => request,
      };

      const ddl = await ms.getCreateTableDDL(
        "appdb",
        "dbo]sales",
        "user]roles",
      );

      expect(ddl).toBe(
        "CREATE TABLE [dbo]]sales].[user]]roles] (\n" +
          "  [tenant_id] int NOT NULL,\n" +
          "  [user_id] int NOT NULL,\n" +
          "  PRIMARY KEY ([user_id], [tenant_id])\n" +
          ");",
      );
    });
  });

  describe("enrichColumn", () => {
    it("marks timestamp/rowversion as read-only and non-filterable", () => {
      const result = enrichTestColumn(ms, {
        name: "rv",
        type: "timestamp",
        nullable: false,
        isPrimaryKey: false,
        isForeignKey: false,
      });

      expect(result.category).toBe("binary");
      expect(result.filterable).toBe(false);
      expect(result.editable).toBe(false);
    });
  });

  describe("buildFilterCondition", () => {
    it("uses <> for boolean neq (not !=)", () => {
      const c = col({
        name: "active",
        type: "bit",
        category: "boolean",
        isBoolean: true,
      });
      const r = ms.buildFilterCondition(c, "neq", "true", 1);
      expect(r?.sql).toContain("<>");
    });

    it("uses CONVERT for binary", () => {
      const c = col({
        name: "d",
        type: "varbinary(16)",
        category: "binary",
        nativeType: "varbinary(16)",
      });
      const r = ms.buildFilterCondition(c, "like", "DEAD", 1);
      expect(r?.sql).toContain("CONVERT");
    });

    it("uses CONVERT for datetime", () => {
      const c = col({
        name: "dt",
        type: "datetime2(7)",
        category: "datetime",
        nativeType: "datetime2(7)",
      });
      const r = ms.buildFilterCondition(c, "like", "2024", 1);
      expect(r?.sql).toContain("CONVERT");
    });

    it("uses CONVERT for time filters", () => {
      const c = col({
        name: "t",
        type: "time(7)",
        category: "time",
        nativeType: "time(7)",
      });
      const r = ms.buildFilterCondition(c, "like", "10:30", 1);
      expect(r?.sql).toContain("CONVERT(VARCHAR(16)");
    });

    it("uses CONVERT(date, col) for typed date equality filters", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });
      const r = ms.buildFilterCondition(c, "eq", "2026-04-15", 1);
      expect(r?.sql).toContain("CONVERT(date");
      expect(r?.params).toEqual(["2026-04-15"]);
    });
  });

  describe("enrichColumn", () => {
    it.each([
      "geography",
      "geometry",
      "hierarchyid",
      "sql_variant",
      "image",
      "text",
      "ntext",
      "xml",
    ])("marks %s as read-only", (type) => {
      const result = enrichTestColumn(ms, {
        name: "unsupported_col",
        type,
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(false);
    });
  });
});

// ────────────────────────────────────────────
// Oracle Driver
// ────────────────────────────────────────────

describe("OracleDriver", () => {
  describe("mapTypeCategory", () => {
    it.each([
      ["NUMBER", "decimal"],
      ["NUMBER(10)", "integer"],
      ["NUMBER(10,0)", "integer"],
      ["NUMBER(10,2)", "decimal"],
      ["INTEGER", "integer"],
      ["SMALLINT", "integer"],
      ["PLS_INTEGER", "integer"],
      ["BINARY_INTEGER", "integer"],
      ["FLOAT", "float"],
      ["FLOAT(24)", "float"],
      ["BINARY_FLOAT", "float"],
      ["BINARY_DOUBLE", "float"],
      ["DATE", "datetime"],
      ["TIMESTAMP(6)", "datetime"],
      ["TIMESTAMP(6) WITH TIME ZONE", "datetime"],
      ["TIMESTAMP(6) WITH LOCAL TIME ZONE", "datetime"],
      ["INTERVAL YEAR TO MONTH", "interval"],
      ["INTERVAL DAY TO SECOND", "interval"],
      ["VARCHAR2(100)", "text"],
      ["NVARCHAR2(100)", "text"],
      ["CHAR(10)", "text"],
      ["NCHAR(10)", "text"],
      ["CLOB", "text"],
      ["NCLOB", "text"],
      ["LONG", "text"],
      ["XMLTYPE", "text"],
      ["ROWID", "text"],
      ["UROWID", "text"],
      ["BLOB", "binary"],
      ["RAW(16)", "binary"],
      ["LONG RAW", "binary"],
      ["SDO_GEOMETRY", "spatial"],
    ] as const)("maps %s → %s", (input, expected) => {
      expect(ora.mapTypeCategory(input)).toBe(expected);
    });
  });

  describe("isBooleanType", () => {
    it("always returns false (Oracle has no boolean)", () => {
      expect(ora.isBooleanType("NUMBER")).toBe(false);
      expect(ora.isBooleanType("VARCHAR2")).toBe(false);
      expect(ora.isBooleanType("BOOLEAN")).toBe(false);
    });
  });

  describe("isDatetimeWithTime", () => {
    it("true for DATE and TIMESTAMP*", () => {
      expect(ora.isDatetimeWithTime("DATE")).toBe(true);
      expect(ora.isDatetimeWithTime("TIMESTAMP(6)")).toBe(true);
    });
    it("false for VARCHAR2", () => {
      expect(ora.isDatetimeWithTime("VARCHAR2(100)")).toBe(false);
    });
  });

  describe("buildPagination (:N bind syntax)", () => {
    it("uses :N placeholders", () => {
      const r = ora.buildPagination(10, 25, 3);
      expect(r.sql).toBe("OFFSET :3 ROWS FETCH NEXT :4 ROWS ONLY");
      expect(r.params).toEqual([10, 25]);
    });
  });

  describe("query bind handling", () => {
    it("passes through native Oracle bind placeholders", async () => {
      const execute = vi.fn(async () => ({
        metaData: [],
        rows: [],
        rowsAffected: 0,
      }));
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      const sql = 'SELECT * FROM "T" OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY';
      const result = await ora.query(sql, [10, 25]);

      expect(execute).toHaveBeenCalledTimes(1);
      expect(execute).toHaveBeenCalledWith(
        sql,
        [10, 25],
        expect.objectContaining({
          autoCommit: true,
          fetchArraySize: 100,
        }),
      );
      expect(result.rows).toEqual([]);
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("rewrites legacy qmark placeholders to Oracle binds", async () => {
      const execute = vi.fn(async () => ({
        metaData: [],
        rows: [],
        rowsAffected: 0,
      }));
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      await ora.query("SELECT ? FROM dual", [1]);

      expect(execute).toHaveBeenCalledTimes(1);
      expect(execute).toHaveBeenCalledWith(
        "SELECT :1 FROM dual",
        [1],
        expect.objectContaining({
          autoCommit: true,
          fetchArraySize: 100,
        }),
      );
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("formats Oracle DATE query results before returning rows", async () => {
      const execute = vi.fn(async () => ({
        metaData: [{ name: "D", dbType: oracledb.DB_TYPE_DATE }],
        rows: [[new Date(2024, 5, 15, 10, 30, 0)]],
        rowsAffected: 0,
      }));
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      const result = await ora.query("SELECT CURRENT_DATE AS D FROM dual");

      expect(result.rows).toEqual([{ __col_0: "2024-06-15 10:30:00" }]);
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("preserves unsafe Oracle NUMBER values as strings", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_NUMBER,
        scale: 0,
      } as unknown as oracledb.Metadata<unknown>);

      expect(response?.converter?.("9007199254740993")).toBe(
        "9007199254740993",
      );
    });

    it("converts safe integer Oracle NUMBER values to numbers", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_NUMBER,
        scale: 0,
      } as unknown as oracledb.Metadata<unknown>);

      expect(response?.converter?.("42")).toBe(42);
    });

    it("converts lower-bound safe Oracle NUMBER values to numbers", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_NUMBER,
        scale: 0,
      } as unknown as oracledb.Metadata<unknown>);

      expect(response?.converter?.("-9007199254740991")).toBe(
        -9007199254740991,
      );
    });

    it("preserves decimal Oracle NUMBER values as strings", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_NUMBER,
        scale: 2,
      } as unknown as oracledb.Metadata<unknown>);

      expect(response?.converter?.("38.73")).toBe("38.73");
    });
  });

  describe("describeTable", () => {
    it("returns ordered Oracle PK ordinals and identity metadata", async () => {
      const execute = vi.fn(async (sql: string) => {
        if (sql.includes("FROM all_tab_columns")) {
          return {
            rows: [
              {
                COLUMN_NAME: "ACCOUNT_ID",
                DATA_TYPE: "NUMBER",
                DATA_PRECISION: 10,
                DATA_SCALE: 0,
                DATA_LENGTH: 22,
                NULLABLE: "N",
                DATA_DEFAULT: null,
                COLUMN_ID: 1,
              },
              {
                COLUMN_NAME: "TENANT_ID",
                DATA_TYPE: "NUMBER",
                DATA_PRECISION: 10,
                DATA_SCALE: 0,
                DATA_LENGTH: 22,
                NULLABLE: "N",
                DATA_DEFAULT: null,
                COLUMN_ID: 2,
              },
              {
                COLUMN_NAME: "NAME",
                DATA_TYPE: "VARCHAR2",
                DATA_PRECISION: null,
                DATA_SCALE: null,
                DATA_LENGTH: 50,
                NULLABLE: "Y",
                DATA_DEFAULT: "'demo' ",
                COLUMN_ID: 3,
              },
            ],
          };
        }
        if (sql.includes("cons.constraint_type = 'P'")) {
          return {
            rows: [
              { COLUMN_NAME: "TENANT_ID", POSITION: 1 },
              { COLUMN_NAME: "ACCOUNT_ID", POSITION: 2 },
            ],
          };
        }
        if (sql.includes("cons.constraint_type = 'R'")) {
          return { rows: [{ COLUMN_NAME: "TENANT_ID" }] };
        }
        if (sql.includes("FROM all_tab_identity_cols")) {
          return {
            rows: [
              {
                COLUMN_NAME: "ACCOUNT_ID",
                GENERATION_TYPE: "BY DEFAULT",
              },
            ],
          };
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      });
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      const result = await ora.describeTable("test", "APP", "ACCOUNTS");

      expect(result).toEqual([
        {
          name: "ACCOUNT_ID",
          type: "NUMBER(10,0)",
          nullable: false,
          defaultValue: undefined,
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          isForeignKey: false,
          isAutoIncrement: true,
        },
        {
          name: "TENANT_ID",
          type: "NUMBER(10,0)",
          nullable: false,
          defaultValue: undefined,
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          isForeignKey: true,
          isAutoIncrement: false,
        },
        {
          name: "NAME",
          type: "VARCHAR2(50)",
          nullable: true,
          defaultValue: "'demo'",
          isPrimaryKey: false,
          primaryKeyOrdinal: undefined,
          isForeignKey: false,
          isAutoIncrement: false,
        },
      ]);
      expect(close).toHaveBeenCalledTimes(1);
    });
  });

  describe("schema metadata", () => {
    it("groups Oracle index columns in database order", async () => {
      const execute = vi.fn(async () => ({
        rows: [
          {
            INDEX_NAME: "ACCOUNTS_PK",
            COLUMN_NAME: "TENANT_ID",
            UNIQUENESS: "UNIQUE",
            INDEX_TYPE: "PRIMARY",
          },
          {
            INDEX_NAME: "ACCOUNTS_PK",
            COLUMN_NAME: "ACCOUNT_ID",
            UNIQUENESS: "UNIQUE",
            INDEX_TYPE: "PRIMARY",
          },
          {
            INDEX_NAME: "ACCOUNTS_NAME_IDX",
            COLUMN_NAME: "NAME",
            UNIQUENESS: "NONUNIQUE",
            INDEX_TYPE: "NORMAL",
          },
        ],
      }));
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      await expect(ora.getIndexes("test", "APP", "ACCOUNTS")).resolves.toEqual([
        {
          name: "ACCOUNTS_PK",
          columns: ["TENANT_ID", "ACCOUNT_ID"],
          unique: true,
          primary: true,
        },
        {
          name: "ACCOUNTS_NAME_IDX",
          columns: ["NAME"],
          unique: false,
          primary: false,
        },
      ]);
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("maps Oracle foreign keys with stable column pairings", async () => {
      const execute = vi.fn(async () => ({
        rows: [
          {
            CONSTRAINT_NAME: "ACCOUNTS_TENANT_FK",
            COLUMN_NAME: "TENANT_ID",
            R_OWNER: "APP",
            R_TABLE_NAME: "TENANTS",
            R_COLUMN_NAME: "ID",
          },
          {
            CONSTRAINT_NAME: "ACCOUNTS_PARENT_FK",
            COLUMN_NAME: "PARENT_ID",
            R_OWNER: "APP",
            R_TABLE_NAME: "ACCOUNTS",
            R_COLUMN_NAME: "ACCOUNT_ID",
          },
        ],
      }));
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      await expect(
        ora.getForeignKeys("test", "APP", "ACCOUNTS"),
      ).resolves.toEqual([
        {
          constraintName: "ACCOUNTS_TENANT_FK",
          column: "TENANT_ID",
          referencedSchema: "APP",
          referencedTable: "TENANTS",
          referencedColumn: "ID",
        },
        {
          constraintName: "ACCOUNTS_PARENT_FK",
          column: "PARENT_ID",
          referencedSchema: "APP",
          referencedTable: "ACCOUNTS",
          referencedColumn: "ACCOUNT_ID",
        },
      ]);
      expect(close).toHaveBeenCalledTimes(1);
    });
  });

  describe("getCreateTableDDL", () => {
    it("prefers DBMS_METADATA for Oracle tables", async () => {
      const execute = vi.fn(async (sql: string) => {
        if (sql.includes("SELECT object_type FROM all_objects")) {
          return { rows: [{ OBJECT_TYPE: "TABLE" }] };
        }
        if (sql.includes("SET_TRANSFORM_PARAM")) {
          return { rowsAffected: 0 };
        }
        if (sql.includes("GET_DDL('TABLE'")) {
          return {
            rows: [
              {
                DDL: 'CREATE TABLE "APP"."EVENTS" (\n  "ID" NUMBER\n);',
              },
            ],
          };
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      });
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      const ddl = await ora.getCreateTableDDL("test", "APP", "EVENTS");

      expect(ddl).toBe('CREATE TABLE "APP"."EVENTS" (\n  "ID" NUMBER\n);');
      expect(execute).toHaveBeenCalledWith(
        expect.stringContaining("SET_TRANSFORM_PARAM"),
      );
      expect(execute).toHaveBeenCalledWith(
        expect.stringContaining("GET_DDL('TABLE'"),
        ["APP", "EVENTS"],
        expect.objectContaining({ outFormat: oracledb.OUT_FORMAT_OBJECT }),
      );
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("falls back to view source when all_objects lookup is unavailable", async () => {
      const execute = vi.fn(async (sql: string) => {
        if (sql.includes("SELECT object_type FROM all_objects")) {
          throw new Error("all_objects unavailable");
        }
        if (sql.includes("SELECT view_name FROM all_views")) {
          return { rows: [{ VIEW_NAME: "EVENT_V" }] };
        }
        if (sql.includes("SET_TRANSFORM_PARAM")) {
          return { rowsAffected: 0 };
        }
        if (sql.includes("SELECT text FROM all_views")) {
          return { rows: [{ TEXT: 'SELECT "ID" FROM "APP"."EVENTS"' }] };
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      });
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      const ddl = await ora.getCreateTableDDL("test", "APP", "EVENT_V");

      expect(ddl).toBe(
        'CREATE OR REPLACE VIEW "APP"."EVENT_V" AS\nSELECT "ID" FROM "APP"."EVENTS"',
      );
      expect(execute).toHaveBeenCalledWith(
        expect.stringContaining("SELECT view_name FROM all_views"),
        ["APP", "EVENT_V"],
        expect.objectContaining({ outFormat: oracledb.OUT_FORMAT_OBJECT }),
      );
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("falls back to manual table DDL when object type metadata is unavailable", async () => {
      const execute = vi.fn(async (sql: string) => {
        if (sql.includes("SELECT object_type FROM all_objects")) {
          throw new Error("all_objects unavailable");
        }
        if (sql.includes("SELECT view_name FROM all_views")) {
          throw new Error("all_views unavailable");
        }
        if (sql.includes("SELECT table_name FROM all_tables")) {
          throw new Error("all_tables unavailable");
        }
        if (sql.includes("SELECT text FROM all_views")) {
          return { rows: [] };
        }
        if (sql.includes("FROM all_tab_columns")) {
          return {
            rows: [
              {
                COLUMN_NAME: "ID",
                DATA_TYPE: "NUMBER",
                DATA_PRECISION: 10,
                DATA_SCALE: 0,
                DATA_LENGTH: 22,
                NULLABLE: "N",
                DATA_DEFAULT: null,
              },
            ],
          };
        }
        if (sql.includes("FROM all_tab_identity_cols")) {
          return { rows: [] };
        }
        if (sql.includes("constraint_type = 'P'")) {
          return { rows: [] };
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      });
      const close = vi.fn(async () => {});
      const pool = {
        getConnection: vi.fn(async () => ({ execute, close })),
      };
      (ora as unknown as { pool: typeof pool }).pool = pool;

      const ddl = await ora.getCreateTableDDL("test", "APP", "EVENTS");

      expect(ddl).toBe(
        'CREATE TABLE "APP"."EVENTS" (\n  "ID" NUMBER(10,0) NOT NULL\n);',
      );
      expect(execute).toHaveBeenCalledWith(
        expect.stringContaining("SELECT text FROM all_views"),
        ["APP", "EVENTS"],
        expect.objectContaining({ outFormat: oracledb.OUT_FORMAT_OBJECT }),
      );
      expect(execute).toHaveBeenCalledWith(
        expect.stringContaining("FROM all_tab_columns"),
        ["APP", "EVENTS"],
        expect.objectContaining({ outFormat: oracledb.OUT_FORMAT_OBJECT }),
      );
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("manual Oracle DDL fallback keeps interval types valid", async () => {
      const conn = {
        execute: vi
          .fn()
          .mockResolvedValueOnce({
            rows: [
              {
                COLUMN_NAME: "IYM_COL",
                DATA_TYPE: "INTERVAL YEAR(2) TO MONTH",
                DATA_PRECISION: 2,
                DATA_SCALE: 0,
                DATA_LENGTH: 5,
                NULLABLE: "Y",
                DATA_DEFAULT: null,
              },
              {
                COLUMN_NAME: "IDS_COL",
                DATA_TYPE: "INTERVAL DAY(2) TO SECOND(6)",
                DATA_PRECISION: 2,
                DATA_SCALE: 6,
                DATA_LENGTH: 11,
                NULLABLE: "Y",
                DATA_DEFAULT: null,
              },
              {
                COLUMN_NAME: "AMOUNT",
                DATA_TYPE: "NUMBER",
                DATA_PRECISION: 10,
                DATA_SCALE: 2,
                DATA_LENGTH: 22,
                NULLABLE: "N",
                DATA_DEFAULT: "0 ",
              },
            ],
          })
          .mockResolvedValueOnce({ rows: [] })
          .mockResolvedValueOnce({ rows: [] }),
      };

      const ddl = await oraInternals._fallbackDDL(conn, "APP", "EVENTS");

      expect(ddl).toContain('"IYM_COL" INTERVAL YEAR(2) TO MONTH');
      expect(ddl).toContain('"IDS_COL" INTERVAL DAY(2) TO SECOND(6)');
      expect(ddl).toContain('"AMOUNT" NUMBER(10,2) NOT NULL DEFAULT 0');
      expect(ddl).not.toContain("TO MONTH (2, 0)");
      expect(ddl).not.toContain("SECOND(6) (2, 6)");
    });
  });

  describe("buildInsertValueExpr (:N)", () => {
    it("returns :N", () => {
      expect(
        ora.buildInsertValueExpr(col({ name: "a", type: "VARCHAR2(100)" }), 5),
      ).toBe(":5");
    });
  });

  describe("buildSetExpr (:N)", () => {
    it("returns quoted = :N", () => {
      expect(ora.buildSetExpr(col({ name: "AGE", type: "NUMBER" }), 2)).toBe(
        '"AGE" = :2',
      );
    });
  });

  describe("enrichColumn", () => {
    it.each([
      "SDO_GEOMETRY",
      "BLOB",
      "CLOB",
      "XMLTYPE",
      "OBJECT",
      "INTERVAL DAY TO SECOND",
    ])("marks %s as read-only", (type) => {
      const result = enrichTestColumn(ora, {
        name: "unsupported_col",
        type,
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(false);
    });

    it("classifies Oracle intervals as interval columns", () => {
      const result = enrichTestColumn(ora, {
        name: "ival",
        type: "INTERVAL DAY TO SECOND",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });

      expect(result.category).toBe("interval");
      expect(result.filterOperators).toEqual(["is_null", "is_not_null"]);
    });

    it("keeps RAW columns editable", () => {
      const result = enrichTestColumn(ora, {
        name: "raw_col",
        type: "RAW(16)",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(true);
    });
  });

  describe("coerceInputValue", () => {
    it.each([
      "BINARY_FLOAT",
      "BINARY_DOUBLE",
      "FLOAT",
    ])("coerces %s string input to number", (nativeType) => {
      const c = col({
        name: "n",
        type: nativeType,
        category: "float",
        nativeType,
      });
      expect(ora.coerceInputValue("1.25", c)).toBe(1.25);
    });

    it("preserves precision-sensitive Oracle NUMBER text for inserts and updates", () => {
      const integerColumn = col({
        name: "account_id",
        type: "NUMBER(20,0)",
        category: "integer",
        nativeType: "NUMBER(20,0)",
      });
      const decimalColumn = col({
        name: "amount",
        type: "NUMBER(38,9)",
        category: "decimal",
        nativeType: "NUMBER(38,9)",
      });

      expect(ora.coerceInputValue("9007199254740993", integerColumn)).toBe(
        "9007199254740993",
      );
      expect(
        ora.coerceInputValue("12345678901234567890.123456789", decimalColumn),
      ).toBe("12345678901234567890.123456789");
    });

    it("converts ISO datetime to Date object for datetime columns", () => {
      const c = col({
        name: "d",
        type: "DATE",
        category: "datetime",
        nativeType: "DATE",
      });
      const r = ora.coerceInputValue("2024-06-15T10:30:00Z", c);
      expect(r).toBeInstanceOf(Date);
    });

    it("accepts SQL datetime display strings for datetime columns", () => {
      const c = col({
        name: "d",
        type: "DATE",
        category: "datetime",
        nativeType: "DATE",
      });
      const r = ora.coerceInputValue("2024-06-15 10:30:00", c);
      expect(r).toBeInstanceOf(Date);
      expect((r as Date).getFullYear()).toBe(2024);
      expect((r as Date).getMonth()).toBe(5);
      expect((r as Date).getDate()).toBe(15);
      expect((r as Date).getHours()).toBe(10);
      expect((r as Date).getMinutes()).toBe(30);
      expect((r as Date).getSeconds()).toBe(0);
    });

    it("treats offset-bearing input for plain Oracle DATE columns as wall-clock text", () => {
      const c = col({
        name: "d",
        type: "DATE",
        category: "datetime",
        nativeType: "DATE",
      });
      const r = ora.coerceInputValue("2024-06-15T10:30:00+02:00", c);
      expect(r).toBeInstanceOf(Date);
      expect((r as Date).getFullYear()).toBe(2024);
      expect((r as Date).getMonth()).toBe(5);
      expect((r as Date).getDate()).toBe(15);
      expect((r as Date).getHours()).toBe(10);
      expect((r as Date).getMinutes()).toBe(30);
      expect((r as Date).getSeconds()).toBe(0);
    });

    it("treats timezone-aware ISO input without an explicit offset as UTC", () => {
      const c = col({
        name: "d",
        type: "TIMESTAMP WITH TIME ZONE",
        category: "datetime",
        nativeType: "TIMESTAMP WITH TIME ZONE",
      });
      const r = ora.coerceInputValue("2024-06-15T10:30:00", c);
      expect(r).toBeInstanceOf(Date);
      expect((r as Date).getUTCFullYear()).toBe(2024);
      expect((r as Date).getUTCMonth()).toBe(5);
      expect((r as Date).getUTCDate()).toBe(15);
      expect((r as Date).getUTCHours()).toBe(10);
      expect((r as Date).getUTCMinutes()).toBe(30);
      expect((r as Date).getUTCSeconds()).toBe(0);
    });

    it("does not coerce impossible Oracle datetime input into a different real timestamp", () => {
      const c = col({
        name: "d",
        type: "DATE",
        category: "datetime",
        nativeType: "DATE",
      });
      const r = ora.coerceInputValue("2026-02-31T10:00:00+02:00", c);
      expect(r).toBe("2026-02-31T10:00:00+02:00");
    });

    it("passes plain strings through for text", () => {
      const c = col({
        name: "s",
        type: "VARCHAR2(100)",
        category: "text",
        nativeType: "VARCHAR2(100)",
      });
      expect(ora.coerceInputValue("hello", c)).toBe("hello");
    });
  });

  describe("formatOutputValue", () => {
    it("formats Oracle DATE values with their local wall-clock components", () => {
      const c = col({ name: "d", type: "DATE", category: "datetime" });
      const result = ora.formatOutputValue(
        new Date(2024, 5, 15, 10, 30, 0),
        c,
      ) as string;
      expect(result).toBe("2024-06-15 10:30:00");
    });

    it("trims trailing zeroes in Oracle wall-clock fractional seconds", () => {
      const c = col({ name: "d", type: "DATE", category: "datetime" });
      const result = ora.formatOutputValue(
        new Date(2024, 5, 15, 10, 30, 0, 120),
        c,
      ) as string;
      expect(result).toBe("2024-06-15 10:30:00.12");
    });

    it("formats timezone-aware Oracle values as canonical display text", () => {
      const c = col({
        name: "d",
        type: "TIMESTAMP WITH TIME ZONE",
        category: "datetime",
        nativeType: "TIMESTAMP WITH TIME ZONE",
      });
      const result = ora.formatOutputValue(
        new Date("2024-06-15T10:30:00Z"),
        c,
      ) as string;
      expect(result).toContain("2024-06-15");
      expect(result).toContain("10:30:00");
      expect(result).not.toContain("+00");
    });

    it("trims trailing zeroes for timezone-aware Oracle fractional seconds", () => {
      const c = col({
        name: "d",
        type: "TIMESTAMP WITH TIME ZONE",
        category: "datetime",
        nativeType: "TIMESTAMP WITH TIME ZONE",
      });
      const result = ora.formatOutputValue(
        new Date("2024-06-15T10:30:00.120Z"),
        c,
      ) as string;
      expect(result).toBe("2024-06-15 10:30:00.12");
    });

    it("converts bigint to string", () => {
      const c = col({ name: "n", type: "NUMBER", category: "integer" });
      expect(ora.formatOutputValue(BigInt(42), c)).toBe("42");
    });

    it("normalizes Oracle BINARY_FLOAT artifacts in the driver", () => {
      const c = col({
        name: "value",
        type: "BINARY_FLOAT",
        category: "float",
        nativeType: "BINARY_FLOAT",
      });
      expect(ora.formatOutputValue(1.2000000476837158, c)).toBe("1.2");
    });

    it("normalizes Oracle FLOAT precision artifacts in the driver", () => {
      const c = col({
        name: "value",
        type: "FLOAT(24)",
        category: "float",
        nativeType: "FLOAT(24)",
      });
      expect(ora.formatOutputValue(1.2300000190734863, c)).toBe("1.23");
    });

    it("falls back to BaseDBDriver formatting for non-interval objects", () => {
      const c = col({
        name: "payload",
        type: "VARCHAR2(100)",
        category: "text",
        nativeType: "VARCHAR2(100)",
      });
      expect(ora.formatOutputValue({ years: 1, months: 14 }, c)).toBe(
        '{"years":1,"months":14}',
      );
    });

    it("defensively normalizes IntervalYM values", () => {
      const c = col({
        name: "ival",
        type: "INTERVAL YEAR TO MONTH",
        category: "interval",
        nativeType: "INTERVAL YEAR TO MONTH",
      });
      expect(ora.formatOutputValue({ years: 1, months: 14 }, c)).toBe("2-02");
    });

    it("defensively normalizes IntervalDS values", () => {
      const c = col({
        name: "ival",
        type: "INTERVAL DAY TO SECOND",
        category: "interval",
        nativeType: "INTERVAL DAY TO SECOND",
      });
      expect(
        ora.formatOutputValue(
          {
            days: 3,
            hours: 4,
            minutes: 5,
            seconds: 6,
            fseconds: 120000000,
          },
          c,
        ),
      ).toBe("3 04:05:06.12");
    });
  });

  describe("fetchTypeHandler", () => {
    it("returns string-backed Oracle NUMBER conversion", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_NUMBER,
        scale: 0,
      } as unknown as oracledb.Metadata<unknown>);

      expect(response?.type).toBe(oracledb.STRING);
    });

    it("normalizes IntervalYM values at fetch time", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_INTERVAL_YM,
      } as unknown as oracledb.Metadata<unknown>);

      expect(response?.converter?.({ years: 1, months: 14 })).toBe("2-02");
    });

    it("normalizes IntervalDS values at fetch time", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_INTERVAL_DS,
      } as unknown as oracledb.Metadata<unknown>);

      expect(
        response?.converter?.({
          days: -1,
          hours: -2,
          minutes: -3,
          seconds: -4,
          fseconds: -120000000,
        }),
      ).toBe("-1 02:03:04.12");
    });
  });

  describe("buildFilterCondition", () => {
    it("uses :N params and preserves Oracle NUMBER binds as strings", () => {
      const c = col({ name: "age", type: "NUMBER", category: "integer" });
      const r = ora.buildFilterCondition(c, "eq", "42", 1);
      expect(r?.sql).toContain(":1");
      expect(r?.params).toEqual(["42"]);
    });

    it("keeps precision-sensitive Oracle NUMBER filters as string binds", () => {
      const c = col({
        name: "amount",
        type: "NUMBER(38,9)",
        category: "decimal",
        nativeType: "NUMBER(38,9)",
      });
      const r = ora.buildFilterCondition(
        c,
        "eq",
        "12345678901234567890.123456789",
        1,
      );

      expect(r).toEqual({
        sql: '"amount" = :1',
        params: ["12345678901234567890.123456789"],
      });
    });

    it("uses UPPER for text LIKE", () => {
      const c = col({
        name: "name",
        type: "VARCHAR2(100)",
        category: "text",
        nativeType: "VARCHAR2(100)",
      });
      const r = ora.buildFilterCondition(c, "like", "foo", 1);
      expect(r?.sql).toContain("UPPER");
    });

    it("uses TO_CHAR for datetime LIKE", () => {
      const c = col({
        name: "dt",
        type: "TIMESTAMP(6)",
        category: "datetime",
        nativeType: "TIMESTAMP(6)",
      });
      const r = ora.buildFilterCondition(c, "like", "2024", 1);
      expect(r?.sql).toContain("TO_CHAR");
      expect(r?.params).toEqual(["%2024%"]);
    });

    it("preserves wall-clock text for offset-bearing non-timezone datetime filters", () => {
      const c = col({
        name: "dt",
        type: "TIMESTAMP(6)",
        category: "datetime",
        nativeType: "TIMESTAMP(6)",
      });
      const r = ora.buildFilterCondition(
        c,
        "like",
        "2024-06-15T10:30:00+02:00",
        1,
      );
      expect(r?.params).toEqual(["%2024-06-15 10:30:00%"]);
    });

    it("normalizes timezone-aware datetime filters to the UTC display basis", () => {
      const c = col({
        name: "dt",
        type: "TIMESTAMP WITH TIME ZONE",
        category: "datetime",
        nativeType: "TIMESTAMP WITH TIME ZONE",
      });
      const r = ora.buildFilterCondition(
        c,
        "like",
        "2024-06-15T10:30:00+02:00",
        1,
      );
      expect(r?.sql).toContain("SYS_EXTRACT_UTC");
      expect(r?.params).toEqual(["%2024-06-15 08:30:00%"]);
    });

    it("does not expose Oracle interval text filtering", () => {
      const c = col({
        name: "duration",
        type: "INTERVAL DAY TO SECOND",
        category: "interval",
        nativeType: "INTERVAL DAY TO SECOND",
        filterable: false,
      });
      const r = ora.buildFilterCondition(c, "like", "04:05:06", 1);

      expect(r).toBeNull();
    });
  });
});

// ────────────────────────────────────────────
// SQLite Driver
// ────────────────────────────────────────────

describe("SQLiteDriver", () => {
  describe("mapTypeCategory", () => {
    it.each([
      // Explicit well-known types
      ["", "text"],
      ["TEXT", "text"],
      ["JSON", "json"],
      ["UUID", "uuid"],
      ["VARCHAR(50)", "text"],
      ["NVARCHAR(100)", "text"],
      ["CLOB", "text"],
      ["CHARACTER VARYING(50)", "text"],
      ["BOOLEAN", "boolean"],
      ["BOOL", "boolean"],
      ["INTEGER", "integer"],
      ["INT", "integer"],
      ["BIGINT", "integer"],
      ["SMALLINT", "integer"],
      ["TINYINT", "integer"],
      ["MEDIUMINT", "integer"],
      ["INT2", "integer"],
      ["INT8", "integer"],
      ["REAL", "float"],
      ["FLOAT", "float"],
      ["DOUBLE", "float"],
      ["DOUBLE PRECISION", "float"],
      ["BLOB", "binary"],
      ["DATE", "date"],
      ["TIME", "time"],
      ["DATETIME(3)", "datetime"],
      ["DATETIME", "datetime"],
      ["TIMESTAMP(6)", "datetime"],
      ["TIMESTAMP", "datetime"],
      ["NUMERIC", "decimal"],
      ["DECIMAL", "decimal"],
      ["DECIMAL(10,2)", "decimal"],
    ] as const)("maps %s → %s", (input, expected) => {
      expect(lite.mapTypeCategory(input)).toBe(expected);
    });
  });

  describe("isBooleanType", () => {
    it("true for BOOLEAN/BOOL", () => {
      expect(lite.isBooleanType("BOOLEAN")).toBe(true);
      expect(lite.isBooleanType("BOOL")).toBe(true);
      expect(lite.isBooleanType("boolean")).toBe(true);
    });
    it("false for INTEGER", () => {
      expect(lite.isBooleanType("INTEGER")).toBe(false);
    });
  });

  describe("isDatetimeWithTime", () => {
    it("true for DATETIME, TIMESTAMP", () => {
      expect(lite.isDatetimeWithTime("DATETIME")).toBe(true);
      expect(lite.isDatetimeWithTime("DATETIME(3)")).toBe(true);
      expect(lite.isDatetimeWithTime("TIMESTAMP")).toBe(true);
      expect(lite.isDatetimeWithTime("TIMESTAMP(6)")).toBe(true);
    });
    it("false for DATE", () => {
      expect(lite.isDatetimeWithTime("DATE")).toBe(false);
    });
  });

  describe("coerceInputValue", () => {
    it("returns 1/0 for boolean true/false", () => {
      const c = col({
        name: "b",
        type: "BOOLEAN",
        category: "boolean",
        isBoolean: true,
      });
      expect(lite.coerceInputValue("true", c)).toBe(1);
      expect(lite.coerceInputValue("false", c)).toBe(0);
    });

    it("passes text through", () => {
      const c = col({ name: "s", type: "TEXT", category: "text" });
      expect(lite.coerceInputValue("hello", c)).toBe("hello");
    });

    it("restores shared binary hex parsing", () => {
      const c = col({ name: "blob_col", type: "BLOB", category: "binary" });
      const result = lite.coerceInputValue("0xdeadbeef", c);
      expect(Buffer.isBuffer(result)).toBe(true);
      expect((result as Buffer).toString("hex")).toBe("deadbeef");
    });
  });

  describe("formatOutputValue", () => {
    it("normalizes SQLite booleans to true/false", () => {
      const c = col({
        name: "is_active",
        type: "BOOLEAN",
        category: "boolean",
        isBoolean: true,
      });

      expect(lite.formatOutputValue(1, c)).toBe(true);
      expect(lite.formatOutputValue(0, c)).toBe(false);
      expect(lite.formatOutputValue("1", c)).toBe(true);
      expect(lite.formatOutputValue("0", c)).toBe(false);
    });

    it("converts bigint to string", () => {
      const c = col({ name: "n", type: "INTEGER", category: "integer" });
      expect(lite.formatOutputValue(BigInt(99), c)).toBe("99");
    });

    it("passes numbers through", () => {
      const c = col({ name: "n", type: "INTEGER", category: "integer" });
      expect(lite.formatOutputValue(42, c)).toBe(42);
    });

    it("passes null through", () => {
      const c = col({ name: "n", type: "TEXT", category: "text" });
      expect(lite.formatOutputValue(null, c)).toBeNull();
    });

    it("restores shared binary hex formatting", () => {
      const c = col({ name: "blob_col", type: "BLOB", category: "binary" });
      expect(lite.formatOutputValue(Buffer.from([0xde, 0xad]), c)).toBe(
        "\\xdead",
      );
    });
  });

  describe("buildFilterCondition", () => {
    it("uses integer 1/0 for boolean", () => {
      const c = col({
        name: "active",
        type: "BOOLEAN",
        category: "boolean",
        isBoolean: true,
      });
      const r = lite.buildFilterCondition(c, "eq", "true", 1);
      expect(r?.params).toEqual([1]);
    });

    it("uses col LIKE ? for text (no CAST)", () => {
      const c = col({
        name: "name",
        type: "TEXT",
        category: "text",
        nativeType: "TEXT",
      });
      const r = lite.buildFilterCondition(c, "like", "foo", 1);
      expect(r?.sql).toBe('"name" LIKE ?');
      expect(r?.params).toEqual(["%foo%"]);
    });

    it("numeric eq", () => {
      const c = col({ name: "age", type: "INTEGER", category: "integer" });
      const r = lite.buildFilterCondition(c, "eq", "25", 1);
      expect(r?.sql).toBe('"age" = ?');
      expect(r?.params).toEqual([25]);
    });

    it("uses typed comparison for date equality filters", () => {
      const c = col({
        name: "created_on",
        type: "DATE",
        category: "date",
        nativeType: "DATE",
      });
      const r = lite.buildFilterCondition(c, "eq", "2026-04-15", 1);
      expect(r?.sql).toBe('DATE("created_on") = DATE(?)');
      expect(r?.params).toEqual(["2026-04-15"]);
    });
  });

  describe("buildPagination (uses base default)", () => {
    it("uses LIMIT ? OFFSET ?", () => {
      const r = lite.buildPagination(5, 10, 1);
      expect(r.sql).toBe("LIMIT ? OFFSET ?");
      expect(r.params).toEqual([10, 5]);
    });
  });

  describe("splitSQLiteScript (comment handling)", () => {
    // Access the private export indirectly by importing the module.
    // The function is not exported so we test it through the query path
    // via a stub, OR we import the tested symbol directly if it were exported.
    // Since it is a module-level private function we verify observable
    // behaviour: comments must NOT appear as statements and must not
    // corrupt the surrounding SQL.

    it("strips line comments and still executes the real statement", () => {
      // We call the driver's internal script splitter indirectly by
      // verifying that a comment-only script produces no statements.
      // Construct a mock db.all / db.run that records calls.
      const calls: string[] = [];
      const mockDb = {
        isOpen: true,
        all: (sql: string) => {
          calls.push(sql);
          return [];
        },
        run: (sql: string) => {
          calls.push(sql);
          return { changes: 0 };
        },
      };
      setSqliteDb(lite, mockDb);

      // A script with only a comment must result in no actual query calls.
      lite.query("-- just a comment\n");
      expect(calls).toHaveLength(0);
    });

    it("does not inject comment text into the statement that follows it", () => {
      const executed: string[] = [];
      const mockDb = {
        isOpen: true,
        all: (sql: string) => {
          executed.push(sql);
          return [];
        },
        run: (sql: string) => {
          executed.push(sql);
          return { changes: 0 };
        },
      };
      setSqliteDb(lite, mockDb);

      lite.query("-- ignore me\nSELECT 1");
      expect(executed).toHaveLength(1);
      expect(executed[0]).toBe("SELECT 1");
    });
  });
});
