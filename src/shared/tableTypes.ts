export const NULL_SENTINEL = "\x00__NULL__\x00";

export type TypeCategory =
  | "text"
  | "integer"
  | "float"
  | "decimal"
  | "boolean"
  | "date"
  | "time"
  | "datetime"
  | "binary"
  | "json"
  | "uuid"
  | "spatial"
  | "interval"
  | "array"
  | "enum"
  | "lob"
  | "other";

export type ValueSemantics = "plain" | "boolean" | "bit";

export type ColumnDefaultKind = "literal" | "expression";

export type GeneratedKind = "virtual" | "stored";

export interface QueryColumnMeta {
  category: TypeCategory | null;
}

const UUID_VALUE_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const INTEGER_VALUE_RE = /^[+-]?\d+$/;
const DECIMAL_VALUE_RE = /^[+-]?(?:\d+\.\d*|\d*\.\d+|\d+(?:[eE][+-]?\d+))$/;
const DATE_VALUE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_VALUE_RE = /^\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/;
const DATETIME_VALUE_RE =
  /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?: ?(?:Z|[+-]\d{2}:\d{2}))?$/i;
const HEX_BINARY_VALUE_RE = /^\\x[0-9a-f]+$/i;
const SPATIAL_VALUE_RE =
  /^(?:srid=\d+;)?\s*(?:point|linestring|polygon|multipoint|multilinestring|multipolygon|geometrycollection|circularstring|compoundcurve|curvepolygon|multicurve|multisurface|polyhedralsurface|tin|triangle)\s*\(/i;

export function inferValueCategory(value: unknown): TypeCategory | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "boolean") {
    return "boolean";
  }

  if (typeof value === "bigint") {
    return "integer";
  }

  if (typeof value === "number") {
    return Number.isInteger(value) ? "integer" : "float";
  }

  if (value instanceof Date) {
    return "datetime";
  }

  if (Array.isArray(value)) {
    return "array";
  }

  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    return "binary";
  }

  if (typeof value === "object") {
    return "json";
  }

  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  if (HEX_BINARY_VALUE_RE.test(trimmed)) {
    return "binary";
  }

  if (UUID_VALUE_RE.test(trimmed)) {
    return "uuid";
  }

  if (SPATIAL_VALUE_RE.test(trimmed)) {
    return "spatial";
  }

  if (DATETIME_VALUE_RE.test(trimmed)) {
    return "datetime";
  }

  if (DATE_VALUE_RE.test(trimmed)) {
    return "date";
  }

  if (TIME_VALUE_RE.test(trimmed)) {
    return "time";
  }

  if (INTEGER_VALUE_RE.test(trimmed)) {
    return "integer";
  }

  if (DECIMAL_VALUE_RE.test(trimmed)) {
    return "decimal";
  }

  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    try {
      const parsed = JSON.parse(trimmed) as unknown;
      return Array.isArray(parsed) ? "array" : "json";
    } catch {
      return null;
    }
  }

  return null;
}

export function inferQueryColumnCategory(
  values: readonly unknown[],
): TypeCategory | null {
  for (const value of values) {
    const category = inferValueCategory(value);
    if (category) {
      return category;
    }
  }
  return null;
}

export type FilterOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "between"
  | "like"
  | "ilike"
  | "in"
  | "is_null"
  | "is_not_null";

export type ScalarFilterOperator = Exclude<
  FilterOperator,
  "between" | "is_null" | "is_not_null"
>;

const SCALAR_FILTER_OPERATORS = new Set<ScalarFilterOperator>([
  "eq",
  "neq",
  "gt",
  "gte",
  "lt",
  "lte",
  "like",
  "ilike",
  "in",
]);

export type FilterExpression =
  | {
      column: string;
      operator: ScalarFilterOperator;
      value: string;
    }
  | {
      column: string;
      operator: "between";
      value: [string, string];
    }
  | {
      column: string;
      operator: "is_null" | "is_not_null";
    };

export type FilterDraft =
  | {
      operator: ScalarFilterOperator;
      value: string;
    }
  | {
      operator: "between";
      value: [string, string];
    }
  | {
      operator: "is_null" | "is_not_null";
    };

export type FilterDraftMap = Partial<Record<string, FilterDraft>>;

type FilterDraftColumn = Pick<
  ColumnTypeMeta,
  "name" | "filterable" | "filterOperators"
>;

export interface ColumnMeta {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue?: string;
  defaultKind?: ColumnDefaultKind;
  onUpdateExpression?: string;
  isComputed?: boolean;
  computedExpression?: string;
  generatedKind?: GeneratedKind;
  isPersisted?: boolean;
  isPrimaryKey: boolean;
  primaryKeyOrdinal?: number;
  isForeignKey: boolean;
  isAutoIncrement?: boolean;
}

export interface ColumnTypeMeta extends ColumnMeta {
  category: TypeCategory;
  nativeType: string;
  filterable: boolean;
  filterOperators: FilterOperator[];
  valueSemantics: ValueSemantics;
}

export interface IndexMeta {
  name: string;
  columns: string[];
  unique: boolean;
  primary: boolean;
}

export interface ForeignKeyMeta {
  column: string;
  referencedSchema: string;
  referencedTable: string;
  referencedColumn: string;
  constraintName: string;
}

export function isNumericCategory(category: TypeCategory): boolean {
  return (
    category === "integer" || category === "float" || category === "decimal"
  );
}

export function defaultFilterOperator(
  column: Pick<ColumnTypeMeta, "category">,
): "eq" | "like" {
  if (column.category === "boolean") {
    return "eq";
  }

  if (
    isNumericCategory(column.category) ||
    column.category === "date" ||
    column.category === "time" ||
    column.category === "datetime"
  ) {
    return "eq";
  }
  return "like";
}

export function valueFilterOperator(
  column: Pick<ColumnTypeMeta, "category" | "filterable" | "filterOperators">,
): "eq" | "like" | null {
  if (!column.filterable) return null;

  const operator = defaultFilterOperator(column);
  return column.filterOperators.includes(operator) ? operator : null;
}

function hasFilterOperator(
  column: Pick<ColumnTypeMeta, "filterOperators">,
  operator: FilterOperator,
): boolean {
  return column.filterOperators.includes(operator);
}

function normalizeFilterValue(rawValue: string): string | null {
  const value = rawValue.trim();
  return value === "" ? null : value;
}

export function buildFilterExpressionFromDraft(
  column: FilterDraftColumn,
  draft: FilterDraft | null | undefined,
): FilterExpression | null {
  if (!draft) return null;

  switch (draft.operator) {
    case "is_null":
    case "is_not_null":
      return hasFilterOperator(column, draft.operator)
        ? { column: column.name, operator: draft.operator }
        : null;
    case "between": {
      if (!column.filterable || !hasFilterOperator(column, "between")) {
        return null;
      }

      const start = normalizeFilterValue(draft.value[0]);
      const end = normalizeFilterValue(draft.value[1]);
      if (!start || !end) return null;

      return {
        column: column.name,
        operator: "between",
        value: [start, end],
      };
    }
    default: {
      if (!column.filterable || !hasFilterOperator(column, draft.operator)) {
        return null;
      }

      const value = normalizeFilterValue(draft.value);
      if (!value) return null;

      return {
        column: column.name,
        operator: draft.operator,
        value,
      };
    }
  }
}

export function serializeFilterDrafts(
  columns: readonly FilterDraftColumn[],
  drafts: FilterDraftMap | null | undefined,
): FilterExpression[] {
  if (!drafts) return [];

  return columns.flatMap<FilterExpression>((column) => {
    const filter = buildFilterExpressionFromDraft(column, drafts[column.name]);
    return filter ? [filter] : [];
  });
}

export function buildFilterExpression(
  column: Pick<
    ColumnTypeMeta,
    "name" | "category" | "filterable" | "filterOperators"
  >,
  rawValue: string,
): FilterExpression | null {
  const value = normalizeFilterValue(rawValue);
  if (!value) return null;

  if (value === NULL_SENTINEL) {
    return buildFilterExpressionFromDraft(column, { operator: "is_null" });
  }

  const operator = valueFilterOperator(column);
  if (!operator) return null;

  return buildFilterExpressionFromDraft(column, { operator, value });
}

export function coerceFilterExpressions(
  rawFilters: unknown,
): FilterExpression[] {
  if (!Array.isArray(rawFilters)) return [];

  return rawFilters.flatMap<FilterExpression>(
    (rawFilter): FilterExpression[] => {
      if (!rawFilter || typeof rawFilter !== "object") {
        return [];
      }

      const filter = rawFilter as Record<string, unknown>;
      const columnName =
        typeof filter.column === "string" ? filter.column : null;
      if (!columnName) return [];

      const operator = filter.operator;

      if (operator === "is_null" || operator === "is_not_null") {
        return [{ column: columnName, operator }];
      }

      if (operator === "between") {
        const value = filter.value;
        if (
          Array.isArray(value) &&
          value.length === 2 &&
          typeof value[0] === "string" &&
          typeof value[1] === "string"
        ) {
          const start = normalizeFilterValue(value[0]);
          const end = normalizeFilterValue(value[1]);
          if (!start || !end) {
            return [];
          }

          return [
            {
              column: columnName,
              operator,
              value: [start, end],
            },
          ];
        }
        return [];
      }

      if (
        typeof operator === "string" &&
        SCALAR_FILTER_OPERATORS.has(operator as ScalarFilterOperator) &&
        typeof filter.value === "string"
      ) {
        const value = normalizeFilterValue(filter.value);
        if (!value) {
          return [];
        }

        return [
          {
            column: columnName,
            operator: operator as ScalarFilterOperator,
            value,
          },
        ];
      }

      return [];
    },
  );
}
