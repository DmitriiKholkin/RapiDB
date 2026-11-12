import { getDynamoDbNativeCellValidationError } from "../../../shared/dynamodbCellInput";
import {
  hasPostgresArrayBounds,
  parsePostgresArrayLiteral,
} from "../../../shared/postgresArrayParser";
import { redisBinarySnapshotDisplay } from "../../../shared/redisValueSnapshot";
import type { ColumnTypeMeta as ColumnMeta } from "../../../shared/tableTypes";
import { formatJsonPreservingTokens } from "../../utils/jsonFormatting";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

export type StructuredCellLanguage = "json" | "xml" | "plaintext";
type StructuredCellKind = "json" | "array" | "xml";

export interface StructuredCellDialogValue {
  kind: StructuredCellKind | "text";
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
): StructuredCellKind | null {
  if (column.category === "binary") return null;
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
  const formatted = formatJsonPreservingTokens(text);
  return "result" in formatted ? formatted.result : null;
}

function formatStructuredJsonValue(value: unknown): string | null {
  if (Array.isArray(value) || (value !== null && typeof value === "object")) {
    return JSON.stringify(value, null, 2);
  }

  return null;
}

export function serializeStructuredCellDialogDraft(
  draftText: string,
  column: Pick<ColumnMeta, "category" | "nativeType">,
): string {
  if (!resolveStructuredCellKind(column)) {
    return draftText;
  }

  return draftText.trim();
}

export function getStructuredCellDraftValidationError(
  draftText: string,
  column: Pick<ColumnMeta, "category" | "nativeType">,
  isNull = false,
): string | null {
  if (isNull) {
    return null;
  }

  const structuredKind = resolveStructuredCellKind(column);
  if (structuredKind !== "json" && structuredKind !== "array") {
    return null;
  }

  const normalizedNativeType = column.nativeType.trim().toLowerCase();
  const requiresObjectRoot = normalizedNativeType === "map";
  const nativeError = getDynamoDbNativeCellValidationError(
    draftText,
    normalizedNativeType,
  );
  if (nativeError !== undefined) return nativeError;
  const supportsPostgresLiteral =
    structuredKind === "array" && normalizedNativeType.endsWith("[]");
  const trimmed = draftText.trim();
  if (
    supportsPostgresLiteral &&
    (trimmed.startsWith("{") || hasPostgresArrayBounds(trimmed))
  ) {
    try {
      parsePostgresArrayLiteral(trimmed);
      return null;
    } catch {
      return "Enter a valid PostgreSQL array literal before applying this value.";
    }
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(draftText) as unknown;
  } catch {
    return requiresObjectRoot
      ? "Enter a valid JSON object before applying this DynamoDB Map value."
      : "Enter valid JSON before applying this value.";
  }

  if (
    requiresObjectRoot &&
    (parsed === null || Array.isArray(parsed) || typeof parsed !== "object")
  ) {
    return "Enter a JSON object before applying this DynamoDB Map value.";
  }

  return null;
}

export function getStructuredCellDialogValue(
  value: unknown,
  column: Pick<ColumnMeta, "category" | "nativeType">,
): StructuredCellDialogValue | null {
  const binaryDisplay = redisBinarySnapshotDisplay(value, column.nativeType);
  if (binaryDisplay !== undefined)
    return {
      kind: "text",
      language: "plaintext",
      formattedText: binaryDisplay,
    };
  const structuredKind = resolveStructuredCellKind(column);
  if (!structuredKind) {
    if (
      value === null ||
      value === undefined ||
      (column.category !== "text" && column.category !== "lob")
    ) {
      return null;
    }
    const rawText = formatScalarValueForDisplay(value);
    return {
      kind: "text",
      language: "plaintext",
      formattedText: rawText,
    };
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
    formattedText: rawText,
  };
}
