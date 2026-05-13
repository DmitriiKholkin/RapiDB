import {
  type FilterExpression,
  type FilterOperator,
  inferValueCategory,
  type ScalarFilterOperator,
} from "../../shared/tableTypes";
import type { ColumnTypeMeta, DriverSortConfig } from "./types";
import { resolveFilterOperators } from "./types";

function stringifyNested(value: unknown): unknown {
  if (value === null || value === undefined) {
    return value;
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return value;
}

export function flattenRootRecord(
  record: Record<string, unknown>,
): Record<string, unknown> {
  const flattened: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(record)) {
    flattened[key] = stringifyNested(value);
  }
  return flattened;
}

export function inferColumnsFromRows(
  rows: readonly Record<string, unknown>[],
  primaryKeyName = "id",
  options?: {
    primaryKeyNames?: readonly string[];
    nullableMode?: "sample" | "schemaLess";
  },
): ColumnTypeMeta[] {
  const primaryKeyNames =
    options?.primaryKeyNames && options.primaryKeyNames.length > 0
      ? options.primaryKeyNames
      : [primaryKeyName];
  const primaryKeyNameSet = new Set(primaryKeyNames);

  const keys = new Set<string>();
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      keys.add(key);
    }
  }

  return [...keys]
    .sort((left, right) => left.localeCompare(right))
    .map((name) => {
      const isPrimaryKey = primaryKeyNameSet.has(name);
      const sample = rows.find((row) => row[name] !== undefined)?.[name];
      const category =
        inferValueCategory(sample) ??
        (typeof sample === "string" && sample.trim().length > 0
          ? "text"
          : "other");
      const nullable = isPrimaryKey
        ? false
        : options?.nullableMode === "schemaLess"
          ? true
          : rows.some((row) => row[name] == null);
      const filterable = category !== "binary" && category !== "spatial";
      return {
        name,
        type: category,
        nativeType: category,
        category,
        nullable,
        defaultValue: undefined,
        isPrimaryKey,
        primaryKeyOrdinal: isPrimaryKey
          ? primaryKeyNames.indexOf(name) + 1
          : undefined,
        isForeignKey: false,
        filterable,
        filterOperators: resolveFilterOperators(category, {
          filterable,
          nullable,
        }),
        valueSemantics: "plain",
      } satisfies ColumnTypeMeta;
    });
}

function compareValues(left: unknown, right: unknown): number {
  if (left == null && right == null) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }

  const leftNumeric = toComparableNumber(left);
  const rightNumeric = toComparableNumber(right);
  if (leftNumeric !== null && rightNumeric !== null) {
    return leftNumeric - rightNumeric;
  }

  const leftDate = new Date(String(left));
  const rightDate = new Date(String(right));
  if (!Number.isNaN(leftDate.getTime()) && !Number.isNaN(rightDate.getTime())) {
    return leftDate.getTime() - rightDate.getTime();
  }

  return String(left).localeCompare(String(right));
}

function toComparableNumber(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  if (!/^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/.test(trimmed)) {
    return null;
  }

  const numeric = Number(trimmed);
  return Number.isFinite(numeric) ? numeric : null;
}

function splitInValues(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function evaluateScalarOperator(
  operator: ScalarFilterOperator,
  rawValue: unknown,
  inputValue: string,
): boolean {
  if (rawValue === null || rawValue === undefined) {
    return false;
  }

  switch (operator) {
    case "eq":
      return String(rawValue) === inputValue;
    case "neq":
      return String(rawValue) !== inputValue;
    case "gt":
      return compareValues(rawValue, inputValue) > 0;
    case "gte":
      return compareValues(rawValue, inputValue) >= 0;
    case "lt":
      return compareValues(rawValue, inputValue) < 0;
    case "lte":
      return compareValues(rawValue, inputValue) <= 0;
    case "like":
    case "ilike": {
      const haystack = String(rawValue ?? "");
      const needle = inputValue.replace(/%/g, "");
      return operator === "ilike"
        ? haystack.toLowerCase().includes(needle.toLowerCase())
        : haystack.includes(needle);
    }
    case "in": {
      const candidates = splitInValues(inputValue);
      return candidates.includes(String(rawValue));
    }
  }
}

function evaluateFilter(
  filter: FilterExpression,
  row: Record<string, unknown>,
): boolean {
  const rawValue = row[filter.column];
  if (filter.operator === "is_null") {
    return rawValue === null || rawValue === undefined;
  }
  if (filter.operator === "is_not_null") {
    return rawValue !== null && rawValue !== undefined;
  }
  if (filter.operator === "between") {
    const [start, end] = filter.value;
    return (
      compareValues(rawValue, start) >= 0 && compareValues(rawValue, end) <= 0
    );
  }
  if (!("value" in filter)) {
    return false;
  }
  return evaluateScalarOperator(filter.operator, rawValue, filter.value);
}

export function applyFilters(
  rows: readonly Record<string, unknown>[],
  filters: readonly FilterExpression[],
): Record<string, unknown>[] {
  if (filters.length === 0) {
    return [...rows];
  }
  return rows.filter((row) =>
    filters.every((filter) => evaluateFilter(filter, row)),
  );
}

export function applySort(
  rows: readonly Record<string, unknown>[],
  sort: DriverSortConfig | null,
): Record<string, unknown>[] {
  if (!sort) {
    return [...rows];
  }
  const sorted = [...rows];
  sorted.sort((left, right) => {
    const cmp = compareValues(left[sort.column], right[sort.column]);
    return sort.direction === "desc" ? -cmp : cmp;
  });
  return sorted;
}

export function pageRows(
  rows: readonly Record<string, unknown>[],
  page: number,
  pageSize: number,
): Record<string, unknown>[] {
  const offset = Math.max(0, (page - 1) * pageSize);
  return rows.slice(offset, offset + pageSize);
}

export function unsupported(operation: string): never {
  throw new Error(`${operation} is not supported by this driver.`);
}

export function stringifyCommandPayload(
  command: string,
  payload: unknown,
): string {
  return `${command} ${JSON.stringify(payload)}`;
}

export function hasOperator(
  operator: FilterOperator,
  supported: readonly FilterOperator[],
): boolean {
  return supported.includes(operator);
}
