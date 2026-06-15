/**
 * Display formatters for column metadata. Pure functions, no I/O.
 */
import type {
  ColumnMeta,
  ColumnTypeMeta,
  PrimaryKeyRole,
  TypeCategory,
} from "./types";

/** Categories where the default filter operator is `eq` (exact match). */
const EQ_FILTER_CATEGORIES: ReadonlySet<TypeCategory> = new Set([
  "integer",
  "float",
  "decimal",
  "date",
  "time",
  "datetime",
  "boolean",
  "binary",
]);

/** True for integer/float/decimal categories. */
export function isNumericCategory(category: TypeCategory): boolean {
  return (
    category === "integer" || category === "float" || category === "decimal"
  );
}

type GeneratedColumnShape = Pick<
  ColumnMeta,
  | "isComputed"
  | "generatedKind"
  | "computedExpression"
  | "isPersisted"
  | "onUpdateExpression"
>;

function formatGeneratedColumnDetail(
  column: GeneratedColumnShape,
): string | undefined {
  if (!column.isComputed && !column.generatedKind) {
    return undefined;
  }

  const parts = [
    column.generatedKind ? `generated ${column.generatedKind}` : "generated",
    column.computedExpression,
    column.isPersisted ? "persisted" : undefined,
    column.onUpdateExpression
      ? `on update: ${column.onUpdateExpression}`
      : undefined,
  ].filter((value): value is string => Boolean(value));

  return parts.join(", ");
}

/**
 * Strips matching outer parens around a string, e.g. `((x))` -> `x`.
 * Used to normalize default-value expressions like `((nextval('seq')))`.
 */
function stripOuterDetailParens(value: string): string {
  let current = value.trim();
  while (current.startsWith("(") && current.endsWith(")")) {
    let depth = 0;
    let wrapsWholeValue = true;
    for (let index = 0; index < current.length; index++) {
      const char = current[index];
      if (char === "(") {
        depth++;
        continue;
      }
      if (char === ")") {
        depth--;
        if (depth === 0 && index < current.length - 1) {
          wrapsWholeValue = false;
          break;
        }
      }
    }
    if (!wrapsWholeValue || depth !== 0) {
      break;
    }
    current = current.slice(1, -1).trim();
  }
  return current;
}

const AUTO_INCREMENT_DEFAULT_RE = /^(?:nextval\s*\(|next value for\b)/i;

/**
 * True when the column is auto-increment, either via `identityGeneration`
 * or via a `nextval(...)` / `next value for ...` default expression.
 */
export function isColumnAutoIncrement(
  column: Pick<ColumnMeta, "defaultValue" | "identityGeneration">,
): boolean {
  if (column.identityGeneration !== undefined) {
    return true;
  }
  const defaultValue = column.defaultValue?.trim();
  if (!defaultValue) {
    return false;
  }
  const normalizedDefault = stripOuterDetailParens(defaultValue);
  return AUTO_INCREMENT_DEFAULT_RE.test(normalizedDefault);
}

const PK_ROLE_LABELS: Partial<Record<PrimaryKeyRole, string>> = {
  partition: "Partition key",
  sort: "Sort key",
};

/** Returns the human-readable role label or `undefined`. */
export function formatPrimaryKeyRoleLabel(
  role?: PrimaryKeyRole,
): string | undefined {
  return role ? PK_ROLE_LABELS[role] : undefined;
}

/** Returns the short badge for a PK role; defaults to `"PK"`. */
export function formatPrimaryKeyBadgeLabel(role?: PrimaryKeyRole): "PK" | "SK" {
  return role === "sort" ? "SK" : "PK";
}

type ColumnDetailShape = Pick<
  ColumnMeta,
  | "type"
  | "nullable"
  | "defaultValue"
  | "identityGeneration"
  | "isComputed"
  | "generatedKind"
  | "computedExpression"
  | "isPersisted"
  | "onUpdateExpression"
>;

/** Returns the `type?` description, e.g. `"int?"` or `"text, default: 'foo'"`. */
export function formatColumnDetailDescription(
  column: ColumnDetailShape,
): string {
  const generated = formatGeneratedColumnDetail(column);
  const autoIncrement = isColumnAutoIncrement(column);
  const extras = [
    autoIncrement
      ? "auto increment"
      : column.defaultValue
        ? `default: ${column.defaultValue}`
        : undefined,
    generated,
  ].filter((value): value is string => Boolean(value));

  return `${column.type}${column.nullable ? "?" : ""}${extras.length > 0 ? `, ${extras.join(", ")}` : ""}`;
}

type ColumnTooltipShape = ColumnDetailShape &
  Pick<ColumnMeta, "name" | "isPrimaryKey" | "primaryKeyRole" | "isForeignKey">;

/** Multi-line tooltip text for a column. */
export function formatColumnDetailTooltip(column: ColumnTooltipShape): string {
  return [
    `${column.name} ${formatColumnDetailDescription(column)}`,
    column.isPrimaryKey
      ? (formatPrimaryKeyRoleLabel(column.primaryKeyRole) ?? "Primary key")
      : undefined,
    column.isForeignKey ? "Foreign key" : undefined,
  ]
    .filter((value): value is string => Boolean(value))
    .join("\n");
}

/**
 * Default filter operator for a column based on its `category`.
 * Always returns either `"eq"` or `"like"`, regardless of the
 * column's `filterable` flag.
 */
export function defaultFilterOperator(
  column: Pick<ColumnTypeMeta, "category">,
): "eq" | "like" {
  return EQ_FILTER_CATEGORIES.has(column.category) ? "eq" : "like";
}

/**
 * Returns the appropriate default filter operator for a column, or
 * `null` if the column is non-filterable or doesn't allow its
 * category's default.
 */
export function valueFilterOperator(
  column: Pick<ColumnTypeMeta, "category" | "filterable" | "filterOperators">,
): "eq" | "like" | null {
  if (!column.filterable) {
    return null;
  }
  const operator = defaultFilterOperator(column);
  return column.filterOperators.includes(operator) ? operator : null;
}
