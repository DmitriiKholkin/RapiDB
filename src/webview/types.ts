export {
  buildFilterExpression,
  type ColumnTypeMeta as ColumnMeta,
  defaultFilterOperator,
  type FilterExpression,
  type FilterOperator,
  type ForeignKeyMeta,
  type IndexMeta,
  isNumericCategory,
  NULL_SENTINEL,
  type TypeCategory,
  valueFilterOperator,
} from "../shared/tableTypes";

import type { TypeCategory } from "../shared/tableTypes";

// ─── Table data types ───

export type Row = Record<string, unknown>;

export type PendingEdits = Map<number, Map<string, unknown>>;

// ─── Formatting helpers ───

/** Returns a human-friendly placeholder based on column category. */
export function placeholderForCategory(
  cat: TypeCategory,
  isBoolean: boolean,
): string {
  if (isBoolean) return "true / false";
  switch (cat) {
    case "integer":
    case "float":
    case "decimal":
      return "number";
    case "date":
      return "YYYY-MM-DD";
    case "time":
      return "HH:MM:SS";
    case "datetime":
      return "YYYY-MM-DD HH:MM:SS";
    case "uuid":
      return "UUID";
    case "json":
      return '{"key": "value"}';
    case "binary":
      return "\\xHEX";
    case "spatial":
      return "POINT(x y)";
    case "interval":
      return "interval";
    case "array":
      return "[1, 2, 3]";
    case "enum":
      return "value";
    default:
      return "filter";
  }
}

/** Category display label for badges. */
export function categoryLabel(cat: TypeCategory): string {
  switch (cat) {
    case "integer":
    case "float":
    case "decimal":
      return "NUM";
    case "boolean":
      return "BOOL";
    case "date":
    case "time":
    case "datetime":
      return "DATE";
    case "binary":
      return "BIN";
    case "json":
      return "JSON";
    case "uuid":
      return "UUID";
    case "spatial":
      return "GEO";
    case "interval":
      return "INTV";
    case "array":
      return "ARR";
    case "enum":
      return "ENUM";
    case "lob":
      return "LOB";
    case "text":
      return "TEXT";
    default:
      return "—";
  }
}

/** Returns a color for the given category. */
export function categoryColor(cat: TypeCategory): string {
  switch (cat) {
    case "integer":
    case "float":
    case "decimal":
      return "var(--vscode-charts-blue, #4fc3f7)";
    case "boolean":
      return "var(--vscode-charts-orange, #e0a040)";
    case "date":
    case "time":
    case "datetime":
      return "var(--vscode-charts-purple, #b48ead)";
    case "binary":
      return "var(--vscode-charts-red, #e06050)";
    case "json":
      return "var(--vscode-charts-green, #4ec94e)";
    case "uuid":
      return "var(--vscode-charts-yellow, #cca700)";
    case "spatial":
      return "#00bcd4";
    case "text":
      return "var(--vscode-foreground)";
    default:
      return "var(--vscode-disabledForeground)";
  }
}
