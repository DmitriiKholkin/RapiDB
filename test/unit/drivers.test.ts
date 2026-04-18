import oracledb from "oracledb";
import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
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
  });

  describe("enrichColumn", () => {
    it.each([
      "point",
      "line",
      "polygon",
      "circle",
    ])("marks geometric %s columns as read-only", (type) => {
      const result = (pg as any).enrichColumn({
        name: "geom_col",
        type,
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(false);
    });

    it("keeps interval columns editable", () => {
      const result = (pg as any).enrichColumn({
        name: "duration_col",
        type: "interval",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(true);
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
      ["integer", "integer"],
      ["mediumint", "integer"],
      ["bigint", "integer"],
      ["decimal(10,2)", "decimal"],
      ["numeric(5,3)", "decimal"],
      ["float", "float"],
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
      ["timestamp", "datetime"],
      ["time", "time"],
    ] as const)("maps %s → %s", (input, expected) => {
      expect(my.mapTypeCategory(input)).toBe(expected);
    });
  });

  describe("isBooleanType", () => {
    it("true for tinyint(1), bit(1), boolean", () => {
      expect(my.isBooleanType("tinyint(1)")).toBe(true);
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
      expect(my.isDatetimeWithTime("timestamp")).toBe(true);
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
    it("returns ST_GeomFromText(?) for spatial", () => {
      const c = col({
        name: "p",
        type: "point",
        category: "spatial",
        nativeType: "point",
      });
      expect(my.buildInsertValueExpr(c, 1)).toBe("ST_GeomFromText(?)");
    });
    it("returns ? for non-spatial", () => {
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

    it("normalizes ISO datetime input to date-only text for date columns", () => {
      const c = col({
        name: "d",
        type: "date",
        category: "date",
        nativeType: "date",
      });
      expect(my.coerceInputValue("2024-06-15T10:30:45Z", c)).toBe("2024-06-15");
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

    it("uses ST_AsText LIKE for spatial", () => {
      const c = col({
        name: "p",
        type: "point",
        category: "spatial",
        nativeType: "point",
      });
      const r = my.buildFilterCondition(c, "like", "POINT", 1);
      expect(r?.sql).toContain("ST_AsText");
      expect(r?.sql).toContain("LIKE");
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

    it("converts time string to Date for time columns", () => {
      const c = col({
        name: "t",
        type: "time(7)",
        category: "time",
        nativeType: "time(7)",
      });
      const r = ms.coerceInputValue("10:30:00", c);
      expect(r).toBeInstanceOf(Date);
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

  describe("enrichColumn", () => {
    it("marks timestamp/rowversion as read-only and non-filterable", () => {
      const result = (ms as any).enrichColumn({
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
      const result = (ms as any).enrichColumn({
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
      ["NUMBER", "integer"],
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
      ["INTERVAL YEAR TO MONTH", "text"],
      ["INTERVAL DAY TO SECOND", "text"],
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
    ])("marks %s as read-only", (type) => {
      const result = (ora as any).enrichColumn({
        name: "unsupported_col",
        type,
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.editable).toBe(false);
    });

    it("keeps RAW columns editable", () => {
      const result = (ora as any).enrichColumn({
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
        category: "text",
        nativeType: "INTERVAL YEAR TO MONTH",
      });
      expect(ora.formatOutputValue({ years: 1, months: 14 }, c)).toBe("2-02");
    });

    it("defensively normalizes IntervalDS values", () => {
      const c = col({
        name: "ival",
        type: "INTERVAL DAY TO SECOND",
        category: "text",
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
    it("normalizes IntervalYM values at fetch time", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_INTERVAL_YM,
      } as oracledb.Metadata<unknown>);

      expect(response?.converter?.({ years: 1, months: 14 })).toBe("2-02");
    });

    it("normalizes IntervalDS values at fetch time", () => {
      const response = oraInternals._fetchTypeHandler({
        dbType: oracledb.DB_TYPE_INTERVAL_DS,
      } as oracledb.Metadata<unknown>);

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
    it("uses :N params", () => {
      const c = col({ name: "age", type: "NUMBER", category: "integer" });
      const r = ora.buildFilterCondition(c, "eq", "42", 1);
      expect(r?.sql).toContain(":1");
      expect(r?.params).toEqual([42]);
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
      ["DATETIME", "datetime"],
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
      expect(lite.isDatetimeWithTime("TIMESTAMP")).toBe(true);
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
      (lite as any).db = mockDb;

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
      (lite as any).db = mockDb;

      lite.query("-- ignore me\nSELECT 1");
      expect(executed).toHaveLength(1);
      expect(executed[0]).toBe("SELECT 1");
    });
  });
});
