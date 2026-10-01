import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnTypeMeta,
  FilterOperator,
} from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const postgresDriver = new PostgresDriver({
  id: "numeric-filter-normalization-pg",
  name: "Numeric Filter Normalization PG",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
} as ConnectionConfig);

const mssqlDriver = new MSSQLDriver({
  id: "numeric-filter-normalization-mssql",
  name: "Numeric Filter Normalization MSSQL",
  type: "mssql",
  host: "127.0.0.1",
  port: 1433,
  database: "master",
  username: "sa",
  password: "secret",
} as ConnectionConfig);

const moneyColumn: ColumnTypeMeta = {
  name: "col_money",
  type: "money",
  nativeType: "money",
  category: "decimal",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
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

const sqliteDriver = new SQLiteDriver({
  id: "numeric-filter-normalization-sqlite",
  name: "Numeric Filter Normalization SQLite",
  type: "sqlite",
  filePath: ":memory:",
});
const sqliteDecimalColumn: ColumnTypeMeta = {
  ...moneyColumn,
  name: "amount",
  type: "decimal(20,0)",
  nativeType: "DECIMAL(20,0)",
};

describe("SQLite decimal filter bindings", () => {
  it.each([
    "9007199254740993",
    "-9007199254740993",
    "9223372036854775807",
    "-9223372036854775808",
    "42",
    "+42",
    ".125",
    "99999999999.12345678",
    "1.25e+2",
    "1E-3",
  ])("binds %s as numeric text without JS rounding", (value) => {
    for (const [operator, sqlOp] of [
      ["eq", "="],
      ["neq", "!="],
      ["gt", ">"],
      ["gte", ">="],
      ["lt", "<"],
      ["lte", "<="],
    ] as const) {
      expect(
        sqliteDriver.buildFilterCondition(
          sqliteDecimalColumn,
          operator,
          value,
          1,
        ),
      ).toEqual({ sql: `"amount" ${sqlOp} ?`, params: [value] });
    }
  });

  it("preserves exact strings in IN and BETWEEN and normalizes formatted values", () => {
    expect(
      sqliteDriver.buildFilterCondition(
        sqliteDecimalColumn,
        "in",
        "9007199254740993, -9007199254740993",
        1,
      ),
    ).toEqual({
      sql: '"amount" IN (?, ?)',
      params: ["9007199254740993", "-9007199254740993"],
    });
    expect(
      sqliteDriver.buildFilterCondition(
        sqliteDecimalColumn,
        "between",
        ["9007199254740993", "9007199254740994"],
        1,
      ),
    ).toEqual({
      sql: '"amount" BETWEEN ? AND ?',
      params: ["9007199254740993", "9007199254740994"],
    });
    expect(
      sqliteDriver.buildFilterCondition(
        sqliteDecimalColumn,
        "eq",
        "$1,234.56",
        1,
      ),
    ).toEqual({ sql: '"amount" = ?', params: ["1234.56"] });
  });

  it.each([
    "NaN",
    "Infinity",
    "-Infinity",
    "1e999",
    "0x10",
    "1.",
    "abc123",
    "1 OR 1=1",
    "1); DROP TABLE items; --",
    "",
  ])("rejects invalid decimal %j in normalization and direct builders", (value) => {
    const inputs: [FilterOperator, string | [string, string]][] = [
      ["eq", value],
      ["neq", value],
      ["gt", value],
      ["in", `1, ${value || "NaN"}`],
      ["between", ["1", value]],
    ];
    for (const [operator, input] of inputs) {
      expect(() =>
        sqliteDriver.normalizeFilterValue(sqliteDecimalColumn, operator, input),
      ).toThrow(/RapiDB Filter/);
      expect(() =>
        sqliteDriver.buildFilterCondition(
          sqliteDecimalColumn,
          operator,
          input,
          1,
        ),
      ).toThrow(/RapiDB Filter/);
    }
  });
});

describe("numeric filter normalization", () => {
  it("preserves a precise bigint in PostgreSQL filter parameters", () => {
    const column = {
      ...moneyColumn,
      category: "integer" as const,
      nativeType: "bigint",
      type: "bigint",
    };
    expect(
      postgresDriver.buildFilterCondition(
        column,
        "eq",
        "9223372036854775807",
        1,
      ),
    ).toEqual({
      sql: '"col_money" = $1',
      params: [9223372036854775807n],
    });
  });

  it.each([
    "abc123",
    "10xyz",
    "mis500",
    "Infinity",
    "1e999",
  ])("rejects invalid numeric filter input %s", (value) => {
    for (const driver of [postgresDriver, mssqlDriver]) {
      expect(() =>
        driver.normalizeFilterValue(moneyColumn, "eq", value),
      ).toThrow();
      expect(() =>
        driver.normalizeFilterValue(moneyColumn, "in", `1, ${value}`),
      ).toThrow();
      expect(() =>
        driver.normalizeFilterValue(moneyColumn, "between", ["1", value]),
      ).toThrow();
    }
  });

  it("normalizes formatted money value for numeric equality filters", () => {
    const normalized = postgresDriver.normalizeFilterValue(
      moneyColumn,
      "eq",
      "$1,234.56",
    );

    expect(normalized).toBe("1234.56");
  });

  it("normalizes values with any currency symbols on both sides", () => {
    const euroPrefix = postgresDriver.normalizeFilterValue(
      moneyColumn,
      "eq",
      "€1,234.56",
    );
    const rubleSuffix = postgresDriver.normalizeFilterValue(
      moneyColumn,
      "eq",
      "1,234.56 ₽",
    );

    expect(euroPrefix).toBe("1234.56");
    expect(rubleSuffix).toBe("1234.56");
  });

  it("normalizes values wrapped with ISO currency codes and apostrophe grouping", () => {
    const normalized = postgresDriver.normalizeFilterValue(
      moneyColumn,
      "eq",
      "CHF 1'234.56",
    );

    expect(normalized).toBe("1234.56");
  });

  it("normalizes formatted money values for numeric IN filters", () => {
    const normalized = postgresDriver.normalizeFilterValue(
      moneyColumn,
      "in",
      "€1,234.56, 2,345.67 ₽, CHF 3'456.78",
    );

    expect(normalized).toBe("1234.56, 2345.67, 3456.78");
  });

  it("keeps rejecting invalid grouped numeric input", () => {
    expect(() =>
      postgresDriver.normalizeFilterValue(moneyColumn, "in", "$1,234.56, nope"),
    ).toThrow(
      "[RapiDB Filter] Column col_money expects comma-separated numbers.",
    );
  });

  it("preserves precision for large decimal values in PostgreSQL filter SQL", () => {
    const condition = postgresDriver.buildFilterCondition(
      moneyColumn,
      "eq",
      "99999999999.12345678",
      1,
    );

    expect(condition).toEqual({
      sql: '"col_money" = $1',
      params: ["99999999999.12345678"],
    });
  });

  it("preserves precision for large decimal values in MSSQL filter SQL", () => {
    const condition = mssqlDriver.buildFilterCondition(
      moneyColumn,
      "eq",
      "9999999999.123455",
      1,
    );

    expect(condition).toEqual({
      sql: "[col_money] = CAST(? AS money)",
      params: ["9999999999.123455"],
    });
  });

  it("preserves precision for numeric(28,10) values in MSSQL filter SQL", () => {
    const numericColumn: ColumnTypeMeta = {
      name: "col_numeric",
      type: "numeric(28,10)",
      nativeType: "numeric(28,10)",
      category: "decimal",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
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

    const condition = mssqlDriver.buildFilterCondition(
      numericColumn,
      "eq",
      "9999999999.123455",
      1,
    );

    expect(condition).toEqual({
      sql: "[col_numeric] = CAST(? AS numeric(28,10))",
      params: ["9999999999.123455"],
    });
  });
});
