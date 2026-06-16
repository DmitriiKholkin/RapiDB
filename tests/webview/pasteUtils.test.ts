import { describe, expect, it } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import {
  formatNormalizedPasteValue,
  validatePasteData,
  validatePasteValue,
} from "../../src/webview/utils/pasteUtils";

function makeColumn(
  overrides: Partial<ColumnTypeMeta> & {
    name: string;
    category: ColumnTypeMeta["category"];
  },
): ColumnTypeMeta {
  return {
    name: overrides.name,
    type: overrides.type ?? "numeric",
    nativeType: overrides.nativeType ?? "numeric",
    nullable: overrides.nullable ?? true,
    isPrimaryKey: overrides.isPrimaryKey ?? false,
    isForeignKey: overrides.isForeignKey ?? false,
    category: overrides.category,
    filterable: overrides.filterable ?? true,
    filterOperators: overrides.filterOperators ?? [
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
    valueSemantics: overrides.valueSemantics ?? "plain",
  };
}

const MONEY_COLUMN_PG = makeColumn({
  name: "col_money",
  type: "money",
  nativeType: "money",
  category: "decimal",
});

const MONEY_COLUMN_MSSQL = makeColumn({
  name: "col_money",
  type: "money",
  nativeType: "money",
  category: "decimal",
});

const MONEY_COLUMN_SQLSERVER_SMALLMONEY = makeColumn({
  name: "col_smallmoney",
  type: "smallmoney",
  nativeType: "smallmoney",
  category: "decimal",
});

const NUMERIC_COLUMN = makeColumn({
  name: "col_numeric",
  type: "numeric(10,2)",
  nativeType: "numeric(10,2)",
  category: "decimal",
});

const DECIMAL_COLUMN = makeColumn({
  name: "col_decimal",
  type: "decimal(10,2)",
  nativeType: "decimal(10,2)",
  category: "decimal",
});

const INTEGER_COLUMN = makeColumn({
  name: "col_int",
  type: "integer",
  nativeType: "integer",
  category: "integer",
});

const FLOAT_COLUMN = makeColumn({
  name: "col_float",
  type: "float8",
  nativeType: "float8",
  category: "float",
});

describe("validatePasteValue — money / currency formats", () => {
  it("accepts plain PostgreSQL money literal with dollar prefix", () => {
    const result = validatePasteValue("$99.99", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("99.99");
  });

  it("accepts negative PostgreSQL money literal with leading minus and dollar sign", () => {
    const result = validatePasteValue("-$1.00", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("-1.00");
  });

  it("accepts grouped thousands money literal", () => {
    const result = validatePasteValue("$1,234.56", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1234.56");
  });

  it("accepts money literal wrapped in accounting parentheses", () => {
    const result = validatePasteValue("($99.99)", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("-99.99");
  });

  it("accepts euro-prefixed values", () => {
    const result = validatePasteValue("€50.00", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("50.00");
  });

  it("accepts ruble-suffixed values", () => {
    const result = validatePasteValue("1,234.56 ₽", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1234.56");
  });

  it("accepts ISO currency code prefix", () => {
    const result = validatePasteValue("CHF 1'234.56", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1234.56");
  });

  it("accepts Swiss-style apostrophe grouping", () => {
    const result = validatePasteValue("1'000.50", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1000.50");
  });

  it("accepts plain numbers without currency symbols", () => {
    const result = validatePasteValue("99.99", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("99.99");
  });

  it("accepts negative money literal with thousands separator", () => {
    const result = validatePasteValue("-$1,234.56", MONEY_COLUMN_PG);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("-1234.56");
  });

  it("rejects invalid numeric input", () => {
    const result = validatePasteValue("abc", MONEY_COLUMN_PG);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("Invalid number value");
  });

  it("rejects mixed-format strings with no recognizable number", () => {
    const result = validatePasteValue("1.234,56", MONEY_COLUMN_PG);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("Invalid number value");
  });

  it("rejects non-numeric garbage", () => {
    const result = validatePasteValue("$", MONEY_COLUMN_PG);
    expect(result.valid).toBe(false);
  });
});

describe("validatePasteValue — money formats across drivers", () => {
  it("normalizes money literals for MSSQL money column", () => {
    const result = validatePasteValue("$1,234.56", MONEY_COLUMN_MSSQL);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1234.56");
  });

  it("normalizes money literals for MSSQL smallmoney column", () => {
    const result = validatePasteValue(
      "-$99.99",
      MONEY_COLUMN_SQLSERVER_SMALLMONEY,
    );
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("-99.99");
  });

  it("normalizes money-formatted input for plain numeric(10,2) column", () => {
    const result = validatePasteValue("$99.99", NUMERIC_COLUMN);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("99.99");
  });

  it("normalizes money-formatted input for plain decimal(10,2) column", () => {
    const result = validatePasteValue("€1,234.50", DECIMAL_COLUMN);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1234.50");
  });

  it("normalizes money-formatted input for integer column", () => {
    const result = validatePasteValue("$1,000", INTEGER_COLUMN);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1000");
  });

  it("normalizes money-formatted input for float column", () => {
    const result = validatePasteValue("$1,234.56", FLOAT_COLUMN);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("1234.56");
  });
});

describe("validatePasteValue — NULL handling", () => {
  it("returns empty string for empty input on nullable column", () => {
    const result = validatePasteValue("", NUMERIC_COLUMN);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("");
  });

  it("returns null for NULL sentinel on nullable column", () => {
    const result = validatePasteValue("NULL", NUMERIC_COLUMN);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe(null);
  });

  it("accepts empty string on non-nullable column (empty string is not NULL)", () => {
    const column = makeColumn({
      name: "col_not_null",
      category: "decimal",
      nullable: false,
    });
    const result = validatePasteValue("", column);
    expect(result.valid).toBe(true);
    expect(result.coercedValue).toBe("");
  });
});

describe("validatePasteValue — other categories", () => {
  it("accepts boolean-like values for boolean columns", () => {
    const column = makeColumn({ name: "col_bool", category: "boolean" });
    expect(validatePasteValue("true", column).valid).toBe(true);
    expect(validatePasteValue("yes", column).valid).toBe(true);
    expect(validatePasteValue("invalid", column).valid).toBe(false);
  });

  it("accepts ISO dates for date columns", () => {
    const column = makeColumn({ name: "col_date", category: "date" });
    expect(validatePasteValue("2024-01-15", column).valid).toBe(true);
    expect(validatePasteValue("not-a-date", column).valid).toBe(false);
  });

  it("accepts UUID format for uuid columns", () => {
    const column = makeColumn({ name: "col_uuid", category: "uuid" });
    expect(
      validatePasteValue("123e4567-e89b-12d3-a456-426614174000", column).valid,
    ).toBe(true);
    expect(validatePasteValue("not-a-uuid", column).valid).toBe(false);
  });
});

describe("validatePasteData — bulk paste with money literals", () => {
  it("normalizes money-formatted rows when all values are valid", () => {
    const result = validatePasteData(
      {
        rows: [
          ["$99.99", "-$1.00"],
          ["$1,234.56", "€50.00"],
        ],
      },
      0,
      0,
      [MONEY_COLUMN_PG, NUMERIC_COLUMN],
      10,
    );

    expect(result.errors).toEqual([]);
    expect(result.rows).toHaveLength(2);
    expect(result.rows[0]?.[0]?.normalized).toBe("99.99");
    expect(result.rows[0]?.[1]?.normalized).toBe("-1.00");
    expect(result.rows[1]?.[0]?.normalized).toBe("1234.56");
    expect(result.rows[1]?.[1]?.normalized).toBe("50.00");
  });

  it("returns errors when any cell fails validation", () => {
    const result = validatePasteData(
      {
        rows: [
          ["$99.99", "abc"],
          ["$1,234.56", "42.00"],
        ],
      },
      0,
      0,
      [MONEY_COLUMN_PG, NUMERIC_COLUMN],
      10,
    );

    const numericErrors = result.errors.filter(
      (err) => err.columnName === "col_numeric",
    );
    expect(numericErrors).toHaveLength(1);
    expect(numericErrors[0]?.value).toBe("abc");
  });

  it("flags paste that would exceed table bounds", () => {
    const result = validatePasteData(
      { rows: [["$99.99"], ["$1,234.56"]] },
      9,
      0,
      [MONEY_COLUMN_PG],
      10,
    );

    expect(result.errors.length).toBeGreaterThan(0);
    const outOfBoundsError = result.errors.find((err) =>
      err.message.includes("does not exist"),
    );
    expect(outOfBoundsError?.message).toBeDefined();
  });

  it("flags paste into primary key column", () => {
    const pkColumn = makeColumn({
      name: "id",
      type: "integer",
      nativeType: "integer",
      category: "integer",
      isPrimaryKey: true,
    });
    const result = validatePasteData({ rows: [["42"]] }, 0, 0, [pkColumn], 10);

    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]?.message).toContain(
      "Cannot paste into primary key",
    );
  });
});

describe("formatNormalizedPasteValue", () => {
  it("returns empty string for empty input (empty string is not NULL)", () => {
    const result = formatNormalizedPasteValue("", null);
    expect(result).toBe("");
  });

  it("returns NULL_SENTINEL for NULL keyword input", () => {
    const sentinel = formatNormalizedPasteValue("NULL", null);
    expect(sentinel).toBe("\x00__NULL__\x00");
  });

  it("returns the normalized string when present", () => {
    const result = formatNormalizedPasteValue("$99.99", "99.99");
    expect(result).toBe("99.99");
  });

  it("returns the original value when normalized is null", () => {
    const result = formatNormalizedPasteValue("hello", null);
    expect(result).toBe("hello");
  });

  it("stringifies number normalized value", () => {
    const result = formatNormalizedPasteValue("$99.99", 99.99);
    expect(result).toBe("99.99");
  });

  it("stringifies bigint normalized value", () => {
    const result = formatNormalizedPasteValue(
      "$99",
      BigInt("1000000000000000"),
    );
    expect(result).toBe("1000000000000000");
  });
});
