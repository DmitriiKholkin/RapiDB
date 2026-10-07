import { compareNumericTokens } from "../../shared/numericNormalization";
import {
  type FilterExpression,
  type FilterOperator,
  inferValueCategory,
  type ScalarFilterOperator,
  type TypeCategory,
} from "../../shared/tableTypes";
import type {
  ColumnTypeMeta,
  DriverSortConfig,
  ForeignKeyMeta,
  TableConstraintMeta,
  TriggerMeta,
} from "./types";
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
    consistentCategories?: boolean;
  },
): ColumnTypeMeta[] {
  const primaryKeyNames =
    options?.primaryKeyNames !== undefined
      ? options.primaryKeyNames
      : [primaryKeyName];
  const primaryKeyNameSet = new Set(primaryKeyNames);

  const keys = new Set<string>();
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      keys.add(key);
    }
  }

  const discoveredNames = [...keys];
  const orderedPrimaryKeys = primaryKeyNames.filter((name) => keys.has(name));
  const orderedNonPrimaryColumns = discoveredNames.filter(
    (name) => !primaryKeyNameSet.has(name),
  );
  const orderedColumnNames = [
    ...orderedPrimaryKeys,
    ...orderedNonPrimaryColumns,
  ];

  return orderedColumnNames.map((name) => {
    const isPrimaryKey = primaryKeyNameSet.has(name);
    const samples = options?.consistentCategories
      ? rows.filter((row) => row[name] != null).map((row) => row[name])
      : [rows.find((row) => row[name] !== undefined)?.[name]];
    const categories = new Set(
      samples.map(
        (sample) =>
          inferValueCategory(sample) ??
          (options?.consistentCategories &&
          typeof sample === "string" &&
          /^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/.test(sample.trim())
            ? "decimal"
            : typeof sample === "string" && sample.trim()
              ? "text"
              : "other"),
      ),
    );
    const category =
      categories.size === 1
        ? [...categories][0]
        : categories.size > 0 && [...categories].every(isNumericCategory)
          ? "decimal"
          : "other";
    const nullable = isPrimaryKey
      ? false
      : options?.nullableMode === "schemaLess"
        ? true
        : rows.some((row) => row[name] == null);
    const filterable = category !== "spatial";
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

type ComparisonColumn = Pick<ColumnTypeMeta, "name" | "category">;
type ValueCategory = (
  row: Record<string, unknown>,
  column: string,
) => TypeCategory | undefined;

function isNumericCategory(category: TypeCategory | undefined): boolean {
  return (
    category === "integer" || category === "float" || category === "decimal"
  );
}

function isTemporalCategory(category: TypeCategory | undefined): boolean {
  return category === "date" || category === "datetime" || category === "time";
}

function compatibleCategory(
  category: TypeCategory | undefined,
  actual: TypeCategory | undefined,
): boolean {
  return (
    category === actual ||
    (isNumericCategory(category) && isNumericCategory(actual))
  );
}

function compareValues(
  left: unknown,
  right: unknown,
  category?: TypeCategory,
): number | null {
  if (left == null && right == null) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (isNumericCategory(category)) {
    if (
      ![left, right].every(
        (value) =>
          typeof value === "string" ||
          typeof value === "bigint" ||
          (typeof value === "number" && Number.isFinite(value)),
      )
    )
      return null;
    return compareNumericTokens(String(left), String(right));
  }
  if (isTemporalCategory(category)) {
    const timestamp = (value: unknown) => {
      if (value instanceof Date) return value.getTime();
      return Date.parse(
        category === "time" ? `1970-01-01T${value}Z` : String(value),
      );
    };
    const a = timestamp(left);
    const b = timestamp(right);
    if (!Number.isNaN(a) && !Number.isNaN(b)) return a - b;
    return null;
  }

  return String(left).localeCompare(String(right));
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
  category?: TypeCategory,
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
    case "gte":
    case "lt":
    case "lte": {
      const cmp = compareValues(rawValue, inputValue, category);
      if (cmp === null) return false;
      return operator === "gt"
        ? cmp > 0
        : operator === "gte"
          ? cmp >= 0
          : operator === "lt"
            ? cmp < 0
            : cmp <= 0;
    }
    case "like":
    case "ilike": {
      const haystack = String(rawValue ?? "");
      const needle = inputValue;
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
  category?: TypeCategory,
  valueCategory?: ValueCategory,
): boolean {
  const rawValue = row[filter.column];
  if (filter.operator === "is_null") {
    return rawValue === null || rawValue === undefined;
  }
  if (filter.operator === "is_not_null") {
    return rawValue !== null && rawValue !== undefined;
  }
  if (rawValue == null) return false;
  const isRange = ["gt", "gte", "lt", "lte", "between"].includes(
    filter.operator,
  );
  if (
    isRange &&
    valueCategory &&
    (isNumericCategory(category) || isTemporalCategory(category)) &&
    !compatibleCategory(category, valueCategory(row, filter.column))
  )
    return false;
  if (filter.operator === "between") {
    const [start, end] = filter.value;
    const lower = compareValues(rawValue, start, category);
    const upper = compareValues(rawValue, end, category);
    return lower !== null && upper !== null && lower >= 0 && upper <= 0;
  }
  if (!("value" in filter)) {
    return false;
  }
  return evaluateScalarOperator(
    filter.operator,
    rawValue,
    filter.value,
    category,
  );
}

export function applyFilters(
  rows: readonly Record<string, unknown>[],
  filters: readonly FilterExpression[],
  columns: readonly ComparisonColumn[] = [],
  valueCategory?: ValueCategory,
): Record<string, unknown>[] {
  if (filters.length === 0) {
    return [...rows];
  }
  const categories = new Map(
    columns.map((column) => [column.name, column.category]),
  );
  return rows.filter((row) =>
    filters.every((filter) =>
      evaluateFilter(filter, row, categories.get(filter.column), valueCategory),
    ),
  );
}

export function applySort(
  rows: readonly Record<string, unknown>[],
  sort: DriverSortConfig | null,
  columns: readonly ComparisonColumn[] = [],
  valueCategory?: ValueCategory,
): Record<string, unknown>[] {
  if (!sort) {
    return [...rows];
  }
  const sorted = [...rows];
  const declaredCategory = columns.find(
    (column) => column.name === sort.column,
  )?.category;
  // A heterogeneous column must use one ordering for the entire sort. Switching
  // between numeric and text comparison per pair would break transitivity.
  const category = rows.some(
    (row) =>
      row[sort.column] != null &&
      ((valueCategory &&
        !compatibleCategory(
          declaredCategory,
          valueCategory(row, sort.column),
        )) ||
        compareValues(row[sort.column], row[sort.column], declaredCategory) ===
          null),
  )
    ? undefined
    : declaredCategory;
  sorted.sort((left, right) => {
    const cmp =
      compareValues(left[sort.column], right[sort.column], category) ??
      String(left[sort.column]).localeCompare(String(right[sort.column]));
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

export function createNoSqlUnsupportedMetadataHandlers(driverName: string): {
  getForeignKeys: () => Promise<ForeignKeyMeta[]>;
  getConstraints: () => Promise<TableConstraintMeta[]>;
  getTriggers: () => Promise<TriggerMeta[] | null>;
  getConstraintDDL: () => Promise<string>;
  getTriggerDDL: () => Promise<string>;
  getObjectDefinition: () => Promise<string | null>;
  getRoutineDefinition: () => Promise<string>;
} {
  return {
    getForeignKeys: async () => [],
    getConstraints: async () => [],
    getTriggers: async () => null,
    getConstraintDDL: async () => unsupported(`${driverName} constraints DDL`),
    getTriggerDDL: async () => unsupported(`${driverName} trigger DDL`),
    getObjectDefinition: async () => null,
    getRoutineDefinition: async () =>
      unsupported(`${driverName} routine definition`),
  };
}
