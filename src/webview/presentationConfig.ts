import type { TypeCategory } from "../shared/tableTypes";

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

type CategoryKey =
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

interface BadgeSpec {
  label: string;
  foreground: string;
  background: string;
}

interface LabeledBadgePresentation extends BadgePresentation {
  label: string;
}

function makeBadge({
  label,
  foreground,
  background,
}: BadgeSpec): LabeledBadgePresentation {
  return {
    label,
    foreground,
    badgeBackground: background,
    badgeBorder: "none",
  };
}

const CATEGORY_PRESENTATIONS: Record<CategoryKey, CategoryPresentation> = {
  numeric: makeBadge({
    label: "NUM",
    foreground: "var(--vscode-textLink-foreground, #2f6f9f)",
    background: "rgba(47, 111, 159, 0.16)",
  }),
  boolean: makeBadge({
    label: "BOOL",
    foreground: "var(--vscode-editorWarning-foreground, #8f5b00)",
    background: "rgba(143, 91, 0, 0.16)",
  }),
  date: makeBadge({
    label: "DATE",
    foreground: "var(--vscode-terminal-ansiMagenta, #7c4ea3)",
    background: "rgba(124, 78, 163, 0.16)",
  }),
  binary: makeBadge({
    label: "BIN",
    foreground: "var(--vscode-errorForeground, #a03d30)",
    background: "rgba(160, 61, 48, 0.16)",
  }),
  json: makeBadge({
    label: "JSON",
    foreground: "var(--vscode-terminal-ansiGreen, #2f7d44)",
    background: "rgba(47, 125, 68, 0.16)",
  }),
  uuid: makeBadge({
    label: "UUID",
    foreground: "var(--vscode-terminal-ansiYellow, #7b6200)",
    background: "rgba(123, 98, 0, 0.18)",
  }),
  spatial: makeBadge({
    label: "GEO",
    foreground: "var(--vscode-terminal-ansiCyan, #0d7284)",
    background: "rgba(13, 114, 132, 0.16)",
  }),
  interval: makeBadge({
    label: "INTV",
    foreground: "var(--vscode-editorWarning-foreground, #8f5b00)",
    background: "rgba(143, 91, 0, 0.16)",
  }),
  array: makeBadge({
    label: "ARR",
    foreground: "var(--vscode-terminal-ansiBlue, #356fa8)",
    background: "rgba(53, 111, 168, 0.16)",
  }),
  enum: makeBadge({
    label: "ENUM",
    foreground: "var(--vscode-terminal-ansiMagenta, #8b3f7b)",
    background: "rgba(139, 63, 123, 0.16)",
  }),
  lob: makeBadge({
    label: "LOB",
    foreground: "var(--vscode-errorForeground, #a03d30)",
    background: "rgba(160, 61, 48, 0.16)",
  }),
  text: makeBadge({
    label: "TEXT",
    foreground: "var(--vscode-foreground)",
    background: "rgba(128,128,128,0.16)",
  }),
  other: makeBadge({
    label: "—",
    foreground: "var(--vscode-descriptionForeground, #6b7280)",
    background: "rgba(128,128,128,0.12)",
  }),
};

const STRUCTURAL_BADGE_PRESENTATIONS: Record<
  StructuralBadgeKind,
  StructuralBadgePresentation
> = {
  pk: makeBadge({
    label: "PK",
    foreground: "var(--vscode-editorWarning-foreground, #8f5b00)",
    background: "rgba(143, 91, 0, 0.16)",
  }),
  fk: makeBadge({
    label: "FK",
    foreground: "var(--vscode-textLink-foreground, #2f6f9f)",
    background: "rgba(47, 111, 159, 0.16)",
  }),
  ai: makeBadge({
    label: "AI",
    foreground: "var(--vscode-terminal-ansiMagenta, #7c4ea3)",
    background: "rgba(124, 78, 163, 0.16)",
  }),
  primary: makeBadge({
    label: "PRIMARY",
    foreground: "var(--vscode-editorWarning-foreground, #8f5b00)",
    background: "rgba(143, 91, 0, 0.16)",
  }),
  unique: makeBadge({
    label: "UNIQUE",
    foreground: "var(--vscode-textLink-foreground, #2f6f9f)",
    background: "rgba(47, 111, 159, 0.16)",
  }),
  index: makeBadge({
    label: "INDEX",
    foreground: "var(--vscode-descriptionForeground, #6b7280)",
    background: "rgba(107, 114, 128, 0.12)",
  }),
};

export function placeholderForCategory(cat: TypeCategory): string {
  switch (cat) {
    case "boolean":
      return "true / false";
    case "date":
      return "YYYY-MM-DD";
    case "time":
      return "HH:MM:SS";
    case "datetime":
      return "YYYY-MM-DD HH:MM:SS";
    case "binary":
      return "0xHEX";
    default:
      return "";
  }
}

export function categoryLabel(cat: TypeCategory): string {
  return getCategoryPresentation(cat).label;
}

export function categoryColor(cat: TypeCategory): string {
  return getCategoryPresentation(cat).foreground;
}

export function getCategoryPresentation(
  cat: TypeCategory,
): CategoryPresentation {
  return CATEGORY_PRESENTATIONS[getCategoryKey(cat)];
}

export function getStructuralBadgePresentation(
  kind: StructuralBadgeKind,
): StructuralBadgePresentation {
  return STRUCTURAL_BADGE_PRESENTATIONS[kind];
}

function getCategoryKey(cat: TypeCategory): CategoryKey {
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
