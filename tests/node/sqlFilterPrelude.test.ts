import { describe, expect, it } from "vitest";
import { createSqlFilterPreamble } from "../../src/extension/dbDrivers/sqlFilterPrelude";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";

function mockColumn(overrides: Partial<ColumnTypeMeta> = {}): ColumnTypeMeta {
  return {
    name: "status",
    type: "text",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    nativeType: "text",
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
    ...overrides,
  };
}

describe("createSqlFilterPreamble", () => {
  it("returns resolved null-check condition for is_null", () => {
    const result = createSqlFilterPreamble({
      column: mockColumn({ name: "display_name" }),
      operator: "is_null",
      value: undefined,
      quoteIdentifier: (identifier) => `"${identifier}"`,
    });

    expect(result).toEqual({
      kind: "resolved",
      condition: {
        sql: '"display_name" IS NULL',
        params: [],
      },
    });
  });

  it("returns resolved null-check condition for is_not_null", () => {
    const result = createSqlFilterPreamble({
      column: mockColumn({ name: "display_name" }),
      operator: "is_not_null",
      value: undefined,
      quoteIdentifier: (identifier) => `"${identifier}"`,
    });

    expect(result).toEqual({
      kind: "resolved",
      condition: {
        sql: '"display_name" IS NOT NULL',
        params: [],
      },
    });
  });

  it("returns null when column is not filterable", () => {
    const result = createSqlFilterPreamble({
      column: mockColumn({ filterable: false }),
      operator: "eq",
      value: "active",
      quoteIdentifier: (identifier) => `"${identifier}"`,
    });

    expect(result).toBeNull();
  });

  it("returns null when value is undefined for non-null operators", () => {
    const result = createSqlFilterPreamble({
      column: mockColumn(),
      operator: "eq",
      value: undefined,
      quoteIdentifier: (identifier) => `"${identifier}"`,
    });

    expect(result).toBeNull();
  });

  it("returns ready preamble and trims scalar string values", () => {
    const result = createSqlFilterPreamble({
      column: mockColumn({ name: "name" }),
      operator: "like",
      value: "  Alice  ",
      quoteIdentifier: (identifier) => `[${identifier}]`,
    });

    expect(result).toEqual({
      kind: "ready",
      columnSql: "[name]",
      value: "Alice",
    });
  });

  it("returns ready preamble and preserves tuple values", () => {
    const result = createSqlFilterPreamble({
      column: mockColumn({ name: "created_at" }),
      operator: "between",
      value: ["2026-01-01", "2026-01-31"],
      quoteIdentifier: (identifier) => `"${identifier}"`,
    });

    expect(result).toEqual({
      kind: "ready",
      columnSql: '"created_at"',
      value: ["2026-01-01", "2026-01-31"],
    });
  });
});
