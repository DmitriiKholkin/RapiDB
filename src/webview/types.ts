export {
  buildFilterExpression,
  type ColumnTypeMeta as ColumnMeta,
  defaultFilterOperator,
  type FilterExpression,
  type FilterOperator,
  type ForeignKeyMeta,
  type IndexMeta,
  isNumericCategory,
  inferQueryColumnCategory,
  inferValueCategory,
  NULL_SENTINEL,
  type QueryColumnMeta,
  type TypeCategory,
  valueFilterOperator,
} from "../shared/tableTypes";

import type { TypeCategory } from "../shared/tableTypes";

// ─── Table data types ───

export type Row = Record<string, unknown>;

export type PendingEdits = Map<number, Map<string, unknown>>;

export interface BadgePresentation {
  foreground: string;
  badgeBackground: string;
  badgeBorder: "none";
}

export interface CategoryPresentation extends BadgePresentation {
  label: string;
}

export type StructuralBadgeKind =
  | "pk"
  | "fk"
  | "ai"
  | "primary"
  | "unique"
  | "index";

export interface StructuralBadgePresentation extends BadgePresentation {
  label: string;
}

type CategoryPresentationKey =
  | "numeric"
  | "boolean"
  | "date"
  | "binary"
  | "json"
  | "uuid"
  | "spatial"
  | "interval"
  | "array"
  | "enum"
  | "lob"
  | "text"
  | "other";

const CATEGORY_PRESENTATIONS: Record<
  CategoryPresentationKey,
  CategoryPresentation
> = {
  numeric: makeCategoryPresentation(
    "NUM",
    "var(--vscode-textLink-foreground, #2f6f9f)",
    "rgba(47, 111, 159, 0.16)",
  ),
  boolean: makeCategoryPresentation(
    "BOOL",
    "var(--vscode-editorWarning-foreground, #8f5b00)",
    "rgba(143, 91, 0, 0.16)",
  ),
  date: makeCategoryPresentation(
    "DATE",
    "var(--vscode-terminal-ansiMagenta, #7c4ea3)",
    "rgba(124, 78, 163, 0.16)",
  ),
  binary: makeCategoryPresentation(
    "BIN",
    "var(--vscode-errorForeground, #a03d30)",
    "rgba(160, 61, 48, 0.16)",
  ),
  json: makeCategoryPresentation(
    "JSON",
    "var(--vscode-terminal-ansiGreen, #2f7d44)",
    "rgba(47, 125, 68, 0.16)",
  ),
  uuid: makeCategoryPresentation(
    "UUID",
    "var(--vscode-terminal-ansiYellow, #7b6200)",
    "rgba(123, 98, 0, 0.18)",
  ),
  spatial: makeCategoryPresentation(
    "GEO",
    "var(--vscode-terminal-ansiCyan, #0d7284)",
    "rgba(13, 114, 132, 0.16)",
  ),
  interval: makeCategoryPresentation(
    "INTV",
    "var(--vscode-editorWarning-foreground, #8f5b00)",
    "rgba(143, 91, 0, 0.16)",
  ),
  array: makeCategoryPresentation(
    "ARR",
    "var(--vscode-terminal-ansiBlue, #356fa8)",
    "rgba(53, 111, 168, 0.16)",
  ),
  enum: makeCategoryPresentation(
    "ENUM",
    "var(--vscode-terminal-ansiMagenta, #8b3f7b)",
    "rgba(139, 63, 123, 0.16)",
  ),
  lob: makeCategoryPresentation(
    "LOB",
    "var(--vscode-errorForeground, #a03d30)",
    "rgba(160, 61, 48, 0.16)",
  ),
  text: makeCategoryPresentation(
    "TEXT",
    "var(--vscode-foreground)",
    "rgba(128,128,128,0.16)",
  ),
  other: makeCategoryPresentation(
    "—",
    "var(--vscode-descriptionForeground, #6b7280)",
    "rgba(128,128,128,0.12)",
  ),
};

const STRUCTURAL_BADGE_PRESENTATIONS: Record<
  StructuralBadgeKind,
  StructuralBadgePresentation
> = {
  pk: makeStructuralBadgePresentation(
    "PK",
    "var(--vscode-editorWarning-foreground, #8f5b00)",
    "rgba(143, 91, 0, 0.16)",
  ),
  fk: makeStructuralBadgePresentation(
    "FK",
    "var(--vscode-textLink-foreground, #2f6f9f)",
    "rgba(47, 111, 159, 0.16)",
  ),
  ai: makeStructuralBadgePresentation(
    "AI",
    "var(--vscode-terminal-ansiMagenta, #7c4ea3)",
    "rgba(124, 78, 163, 0.16)",
  ),
  primary: makeStructuralBadgePresentation(
    "PRIMARY",
    "var(--vscode-editorWarning-foreground, #8f5b00)",
    "rgba(143, 91, 0, 0.16)",
  ),
  unique: makeStructuralBadgePresentation(
    "UNIQUE",
    "var(--vscode-textLink-foreground, #2f6f9f)",
    "rgba(47, 111, 159, 0.16)",
  ),
  index: makeStructuralBadgePresentation(
    "INDEX",
    "var(--vscode-descriptionForeground, #6b7280)",
    "rgba(107, 114, 128, 0.12)",
  ),
};

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
  return getCategoryPresentation(cat).label;
}

/** Returns a color for the given category. */
export function categoryColor(cat: TypeCategory): string {
  return getCategoryPresentation(cat).foreground;
}

export function getCategoryPresentation(
  cat: TypeCategory,
): CategoryPresentation {
  return CATEGORY_PRESENTATIONS[getCategoryPresentationKey(cat)];
}

export function getStructuralBadgePresentation(
  kind: StructuralBadgeKind,
): StructuralBadgePresentation {
  return STRUCTURAL_BADGE_PRESENTATIONS[kind];
}

function getCategoryPresentationKey(
  cat: TypeCategory,
): CategoryPresentationKey {
  switch (cat) {
    case "integer":
    case "float":
    case "decimal":
      return "numeric";
    case "boolean":
      return "boolean";
    case "date":
    case "time":
    case "datetime":
      return "date";
    case "binary":
      return "binary";
    case "json":
      return "json";
    case "uuid":
      return "uuid";
    case "spatial":
      return "spatial";
    case "interval":
      return "interval";
    case "array":
      return "array";
    case "enum":
      return "enum";
    case "lob":
      return "lob";
    case "text":
      return "text";
    default:
      return "other";
  }
}

function makeCategoryPresentation(
  label: string,
  foreground: string,
  badgeBackground: string,
): CategoryPresentation {
  return {
    label,
    foreground,
    badgeBackground,
    badgeBorder: "none",
  };
}

function makeStructuralBadgePresentation(
  label: string,
  foreground: string,
  badgeBackground: string,
): StructuralBadgePresentation {
  return {
    label,
    foreground,
    badgeBackground,
    badgeBorder: "none",
  };
}
