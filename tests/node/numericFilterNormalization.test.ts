import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
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

const moneyColumn: ColumnTypeMeta = {
  name: "col_money",
  type: "money",
  nativeType: "money",
  category: "decimal",
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

describe("numeric filter normalization", () => {
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
});
