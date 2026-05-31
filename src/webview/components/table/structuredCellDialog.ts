import type { ColumnTypeMeta as ColumnMeta } from "../../../shared/tableTypes";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

export type StructuredCellLanguage = "json" | "xml" | "plaintext";

export interface StructuredCellDialogValue {
  kind: "json" | "array" | "xml";
  language: StructuredCellLanguage;
  formattedText: string;
}

export interface StructuredCellDialogState {
  rowKind: "persisted" | "draft";
  rowIdx: number | null;
  column: ColumnMeta;
  title: string;
  description: string;
  language: StructuredCellLanguage;
  initialText: string;
  draftText: string;
  originalValue: unknown;
  nullable: boolean;
  readOnly: boolean;
  initialIsNull: boolean;
  isNull: boolean;
}

function resolveStructuredCellKind(
  column: Pick<ColumnMeta, "category" | "nativeType">,
): StructuredCellDialogValue["kind"] | null {
  const normalizedNativeType = column.nativeType.trim().toLowerCase();

  if (column.category === "array" || normalizedNativeType.endsWith("[]")) {
    return "array";
  }

  if (column.category === "json" || normalizedNativeType.includes("json")) {
    return "json";
  }

  if (normalizedNativeType.includes("xml")) {
    return "xml";
  }

  return null;
}

function formatJsonText(text: string): string | null {
  try {
    return JSON.stringify(JSON.parse(text) as unknown, null, 2);
  } catch {
    return null;
  }
}

function formatStructuredJsonValue(value: unknown): string | null {
  if (Array.isArray(value) || (value !== null && typeof value === "object")) {
    return JSON.stringify(value, null, 2);
  }

  return null;
}

function isXmlLikeText(text: string, nativeType: string): boolean {
  const trimmedNativeType = nativeType.trim().toLowerCase();
  const trimmed = text.trim();

  if (!trimmed.startsWith("<") || !trimmed.endsWith(">")) {
    return false;
  }

  if (trimmedNativeType.includes("xml") || trimmed.startsWith("<?xml")) {
    return true;
  }

  return (
    /^<([A-Za-z_][\w:.-]*)(?:\s[^>]*)?>[\s\S]*<\/\1>$/.test(trimmed) ||
    /^<([A-Za-z_][\w:.-]*)(?:\s[^>]*)?\/>$/.test(trimmed)
  );
}

function shouldIncreaseXmlIndent(token: string): boolean {
  if (
    token.startsWith("</") ||
    token.startsWith("<?") ||
    token.startsWith("<!") ||
    token.endsWith("/>") ||
    token.includes("</")
  ) {
    return false;
  }

  return /^<[^/][^>]*>$/.test(token);
}

function formatXmlText(text: string): string {
  const normalized = text.trim().replace(/>\s*</g, ">\n<");
  const tokens = normalized
    .split("\n")
    .map((token) => token.trim())
    .filter((token) => token.length > 0);

  let indentLevel = 0;
  const lines: string[] = [];

  for (const token of tokens) {
    if (token.startsWith("</")) {
      indentLevel = Math.max(0, indentLevel - 1);
    }

    lines.push(`${"  ".repeat(indentLevel)}${token}`);

    if (shouldIncreaseXmlIndent(token)) {
      indentLevel += 1;
    }
  }

  return lines.join("\n");
}

export function serializeStructuredCellDialogDraft(
  draftText: string,
  column: Pick<ColumnMeta, "category" | "nativeType">,
): string {
  const structuredKind = resolveStructuredCellKind(column);
  const trimmed = draftText.trim();

  if (!structuredKind) {
    return trimmed;
  }

  if (structuredKind === "json" || structuredKind === "array") {
    if (!trimmed) {
      return "";
    }

    try {
      return JSON.stringify(JSON.parse(trimmed) as unknown);
    } catch {
      return trimmed;
    }
  }

  if (!trimmed) {
    return "";
  }

  return trimmed.replace(/>\s+</g, "><");
}

export function getStructuredCellDialogValue(
  value: unknown,
  column: Pick<ColumnMeta, "category" | "nativeType">,
): StructuredCellDialogValue | null {
  const structuredKind = resolveStructuredCellKind(column);
  if (!structuredKind) {
    return null;
  }

  if (value === null || value === undefined) {
    return {
      kind: structuredKind,
      language: structuredKind === "xml" ? "xml" : "json",
      formattedText: "",
    };
  }

  if (structuredKind !== "xml") {
    const formattedStructuredValue = formatStructuredJsonValue(value);
    if (formattedStructuredValue !== null) {
      return {
        kind: structuredKind,
        language: "json",
        formattedText: formattedStructuredValue,
      };
    }
  }

  const rawText = formatScalarValueForDisplay(value);
  const trimmed = rawText.trim();

  if (!trimmed) {
    return null;
  }

  if (structuredKind === "json") {
    const formattedJson = formatJsonText(trimmed);
    return {
      kind: "json",
      language: "json",
      formattedText: formattedJson ?? rawText,
    };
  }

  if (structuredKind === "array") {
    const formattedJson = formatJsonText(trimmed);
    return {
      kind: "array",
      language: formattedJson ? "json" : "plaintext",
      formattedText: formattedJson ?? rawText,
    };
  }

  return {
    kind: "xml",
    language: "xml",
    formattedText: isXmlLikeText(trimmed, column.nativeType)
      ? formatXmlText(trimmed)
      : rawText,
  };
}
