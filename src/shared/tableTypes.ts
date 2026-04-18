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

export interface ColumnMeta {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue?: string;
  isPrimaryKey: boolean;
  primaryKeyOrdinal?: number;
  isForeignKey: boolean;
  isAutoIncrement?: boolean;
}

export interface ColumnTypeMeta extends ColumnMeta {
  category: TypeCategory;
  nativeType: string;
  filterable: boolean;
  editable: boolean;
  filterOperators: FilterOperator[];
  isBoolean: boolean;
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
  column: Pick<ColumnTypeMeta, "category" | "isBoolean">,
): "eq" | "like" {
  if (
    column.isBoolean ||
    isNumericCategory(column.category) ||
    column.category === "date"
  ) {
    return "eq";
  }
  return "like";
}

export function valueFilterOperator(
  column: Pick<
    ColumnTypeMeta,
    "category" | "filterable" | "filterOperators" | "isBoolean"
  >,
): "eq" | "like" | null {
  if (!column.filterable) return null;

  const operator = defaultFilterOperator(column);
  return column.filterOperators.includes(operator) ? operator : null;
}

export function buildFilterExpression(
  column: Pick<
    ColumnTypeMeta,
    "name" | "category" | "filterable" | "filterOperators" | "isBoolean"
  >,
  rawValue: string,
): FilterExpression | null {
  const value = rawValue.trim();
  if (value === "") return null;

  if (value === NULL_SENTINEL) {
    return column.filterOperators.includes("is_null")
      ? { column: column.name, operator: "is_null" }
      : null;
  }

  const operator = valueFilterOperator(column);
  if (!operator) return null;

  return {
    column: column.name,
    operator,
    value,
  };
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
          return [
            {
              column: columnName,
              operator,
              value: [value[0], value[1]],
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
        return [
          {
            column: columnName,
            operator: operator as ScalarFilterOperator,
            value: filter.value,
          },
        ];
      }

      return [];
    },
  );
}
