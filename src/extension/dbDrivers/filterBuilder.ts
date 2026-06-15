import { normalizeNumericToken } from "../../shared/numericNormalization";
import { normalizeDateFilterValue } from "../utils/dateUtils";
import {
  createSqlFilterPreamble,
  type SqlFilterPreambleResult,
} from "./sqlFilterPrelude";
import type {
  ColumnTypeMeta,
  FilterOperator,
  TypeCategory,
  ValueSemantics,
} from "./types";

// Re-export date utilities for backward compatibility.
export { normalizeDateFilterValue } from "../utils/dateUtils";

const SQL_FILTER_ERROR_RE =
  /^\[RapiDB Filter\]|invalid input syntax|invalid cidr|malformed array|not a valid (binary|hex|uuid)|syntax error in input|invalid value for type|invalid number|operator does not exist|conversion failed|arithmetic overflow|ORA-0(1841|1843|1858|1861|6502)|ORA-01722|incorrect (date|datetime|time)|Incorrect integer value|Truncated incorrect|data truncat/i;

/**
 * Result of building a filter condition.
 */
export interface FilterConditionResult {
  sql: string;
  params: unknown[];
}

/**
 * Builds SQL filter conditions for database queries.
 * Encapsulates all filter-related logic including value normalization
 * and SQL generation for different column types and operators.
 */
export class FilterBuilder {
  /**
   * Checks if a value looks like a filter input error based on known patterns.
   */
  static isSqlFilterError(value: unknown): boolean {
    return typeof value === "string" && SQL_FILTER_ERROR_RE.test(value);
  }

  /**
   * Normalizes a filter value based on column type and operator.
   */
  normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ): string | [string, string] | undefined {
    if (operator === "is_null" || operator === "is_not_null") {
      return undefined;
    }
    if (value === undefined) {
      return undefined;
    }
    if (operator === "between") {
      if (!Array.isArray(value)) {
        return value;
      }
      return [
        this.normalizeScalarFilterValue(column, value[0], operator),
        this.normalizeScalarFilterValue(column, value[1], operator),
      ];
    }
    if (typeof value !== "string") {
      return value;
    }
    return this.normalizeScalarFilterValue(column, value, operator);
  }

  /**
   * Normalizes a scalar filter value based on column type.
   */
  protected normalizeScalarFilterValue(
    column: ColumnTypeMeta,
    rawValue: string,
    operator: FilterOperator,
  ): string {
    const value = rawValue.trim();
    if (value === "") {
      throw invalidFilterInputError(column.name, "a filter value");
    }
    if (this.hasBooleanSemantics(column)) {
      const normalized = normalizeBooleanFilterValue(value);
      if (!normalized) {
        throw invalidFilterInputError(column.name, "true or false");
      }
      return normalized;
    }
    if (this.isNumericCategory(column.category)) {
      if (operator === "in") {
        const rawValues = splitNumericFilterInList(value);
        if (rawValues.length === 0) {
          throw invalidFilterInputError(column.name, "comma-separated numbers");
        }
        const values = rawValues.map((part) =>
          normalizeNumericFilterToken(part),
        );
        if (values.some((part) => part === null)) {
          throw invalidFilterInputError(column.name, "comma-separated numbers");
        }
        return values.join(", ");
      }
      const normalizedNumericValue = normalizeNumericFilterToken(value);
      if (!normalizedNumericValue) {
        throw invalidFilterInputError(column.name, "a number");
      }
      return normalizedNumericValue;
    }
    if (column.category === "date") {
      if (operator === "like") {
        return value;
      }
      if (operator === "in") {
        const values = value
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        if (values.length === 0) {
          throw invalidFilterInputError(column.name, "comma-separated dates");
        }
        const normalizedValues = values.map((part) => {
          const normalizedDate = normalizeDateFilterValue(part);
          if (!normalizedDate) {
            throw invalidFilterInputError(column.name, "a valid date");
          }
          return normalizedDate;
        });
        return normalizedValues.join(", ");
      }
      const normalizedDate = normalizeDateFilterValue(value);
      if (!normalizedDate) {
        throw invalidFilterInputError(column.name, "a valid date");
      }
      return normalizedDate;
    }
    return value;
  }

  /**
   * Builds a filter condition for a column.
   */
  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    _paramIndex: number,
    quoteIdentifier: (identifier: string) => string,
  ): FilterConditionResult | null {
    const preamble = createSqlFilterPreamble({
      column,
      operator,
      value,
      quoteIdentifier,
    });
    if (!preamble) return null;
    if (preamble.kind === "resolved") return preamble.condition;
    const col = preamble.columnSql;
    const val = preamble.value;
    if (column.category === "array") {
      if (operator !== "like" && operator !== "ilike") {
        return null;
      }
      const arrayValue = typeof val === "string" ? val : val[0];
      return this.buildArrayLikeFilter(col, arrayValue);
    }
    if (
      column.category === "binary" &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const sqlOp = operator === "neq" ? "!=" : "=";
      return {
        sql: `${col} ${sqlOp} ?`,
        params: [this.coerceInputValue(val, column)],
      };
    }
    if (
      this.hasBooleanSemantics(column) &&
      (operator === "eq" || operator === "neq")
    ) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        return this.buildBooleanFilter(col, operator, strVal === "true");
      }
    }
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      return this.buildNumericFilter(col, column, operator, val);
    }
    if (operator === "between" && Array.isArray(val)) {
      return this.buildBetweenFilter(col, column, val);
    }
    return this.buildTextFilter(
      col,
      column,
      operator,
      typeof val === "string" ? val : val[0],
    );
  }

  /**
   * Checks if a column has boolean semantics.
   */
  hasBooleanSemantics(column: ColumnTypeMeta): boolean {
    return (
      column.valueSemantics === ("boolean" as ValueSemantics) ||
      column.category === "boolean"
    );
  }

  /**
   * Coerces an input value based on column type.
   */
  coerceInputValue(value: string, _column: ColumnTypeMeta): unknown {
    if (value.startsWith("0x") || value.startsWith("0X")) {
      const hex = value.slice(2);
      if (/^[0-9a-fA-F]*$/.test(hex) && hex.length % 2 === 0) {
        return Buffer.from(hex, "hex");
      }
    }
    return value;
  }

  /**
   * Checks if a type category is numeric.
   */
  isNumericCategory(cat: TypeCategory): boolean {
    return cat === "integer" || cat === "float" || cat === "decimal";
  }

  /**
   * Builds a boolean filter condition.
   */
  protected buildBooleanFilter(
    col: string,
    operator: FilterOperator,
    isTrue: boolean,
  ): FilterConditionResult {
    const op = operator === "neq" ? "!=" : "=";
    return { sql: `${col} ${op} ?`, params: [isTrue ? 1 : 0] };
  }

  /**
   * Builds a numeric filter condition.
   */
  protected buildNumericFilter(
    col: string,
    _column: ColumnTypeMeta,
    operator: FilterOperator,
    val: string,
  ): FilterConditionResult {
    const num = Number(val);
    const sqlOp = this.sqlOperator(operator);
    return { sql: `${col} ${sqlOp} ?`, params: [num] };
  }

  /**
   * Builds a BETWEEN filter condition.
   */
  protected buildBetweenFilter(
    col: string,
    _column: ColumnTypeMeta,
    val: [string, string],
  ): FilterConditionResult {
    return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
  }

  /**
   * Builds a text filter condition.
   */
  protected buildTextFilter(
    col: string,
    _column: ColumnTypeMeta,
    operator: FilterOperator,
    val: string,
  ): FilterConditionResult {
    const sqlOp = this.sqlOperator(operator);
    if (operator === "like" || operator === "ilike") {
      return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "eq") {
      return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "neq") {
      return { sql: `CAST(${col} AS CHAR) NOT LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "in") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }
    return { sql: `${col} ${sqlOp} ?`, params: [val] };
  }

  /**
   * Builds an array LIKE filter condition.
   */
  protected buildArrayLikeFilter(
    col: string,
    val: string,
  ): FilterConditionResult {
    return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [`%${val}%`] };
  }

  /**
   * Returns the SQL operator for a filter operator.
   */
  sqlOperator(op: FilterOperator): string {
    switch (op) {
      case "eq":
        return "=";
      case "neq":
        return "!=";
      case "gt":
        return ">";
      case "gte":
        return ">=";
      case "lt":
        return "<";
      case "lte":
        return "<=";
      default:
        return "=";
    }
  }
}

// Helper functions (moved from BaseDBDriver)

function normalizeBooleanFilterValue(value: string): "true" | "false" | null {
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return "true";
  if (normalized === "false" || normalized === "0") return "false";
  return null;
}

function invalidFilterInputError(columnName: string, expected: string): Error {
  return new Error(`[RapiDB Filter] Column ${columnName} expects ${expected}.`);
}

function normalizeNumericFilterToken(rawValue: string): string | null {
  return normalizeNumericToken(rawValue);
}

function splitNumericFilterInList(rawValue: string): string[] {
  const input = rawValue.trim();
  if (input === "") return [];
  const parts: string[] = [];
  let start = 0;
  for (let i = 0; i < input.length; i += 1) {
    if (input[i] !== ",") continue;
    const prevChar = i > 0 ? input[i - 1] : "";
    const nextSlice = input.slice(i + 1);
    const isThousandsSeparator =
      /\d/.test(prevChar) && /^\d{3}(?=[^\d]|$)/.test(nextSlice);
    if (isThousandsSeparator) {
      continue;
    }
    parts.push(input.slice(start, i).trim());
    start = i + 1;
  }
  parts.push(input.slice(start).trim());
  return parts.filter(Boolean);
}
