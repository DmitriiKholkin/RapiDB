import type { QueryColumnMeta, TypeCategory } from "../dbDrivers/types";
import {
  isRawNumberToken,
  parseJsonPreservingRawNumbers,
} from "./jsonCanonical";

export interface ExportColumnDescriptor {
  key: string;
  sourceKey: string;
  category?: TypeCategory | null;
  nativeType?: string;
}

export type JsonExportScalar =
  | null
  | boolean
  | number
  | string
  | RawJsonLiteral;

export type JsonExportValue =
  | JsonExportScalar
  | JsonExportValue[]
  | { [key: string]: JsonExportValue };

const RAW_EXPORT_LITERAL = Symbol("raw export JSON literal");
export type RawJsonLiteral = {
  readonly [RAW_EXPORT_LITERAL]: true;
  readonly literal: string;
};

const JSON_NUMBER_LITERAL_RE = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;

const pad2 = (n: number): string => String(n).padStart(2, "0");

export function queryColumnKey(index: number): string {
  return `__col_${index}`;
}

export function formatExportCellValue(value: unknown): string {
  if (value == null) {
    return "";
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return "";
    }

    return (
      `${value.getUTCFullYear()}-${pad2(value.getUTCMonth() + 1)}-${pad2(value.getUTCDate())} ` +
      `${pad2(value.getUTCHours())}:${pad2(value.getUTCMinutes())}:${pad2(value.getUTCSeconds())}`
    );
  }

  if (Array.isArray(value) || (value !== null && typeof value === "object")) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

export function buildQueryExportColumns(
  columns: readonly string[],
  columnMeta?: readonly QueryColumnMeta[],
): ExportColumnDescriptor[] {
  const reservedNames = new Set(columns);
  const usedNames = new Set<string>();
  const nextSuffixes = new Map<string, number>();

  return columns.map((columnName, index) => {
    let key = columnName;
    if (usedNames.has(key)) {
      let suffix = nextSuffixes.get(columnName) ?? 2;
      do {
        key = `${columnName}_${suffix++}`;
      } while (reservedNames.has(key) || usedNames.has(key));
      nextSuffixes.set(columnName, suffix);
    }
    usedNames.add(key);

    return {
      key,
      sourceKey: queryColumnKey(index),
      category: columnMeta?.[index]?.category ?? null,
    } satisfies ExportColumnDescriptor;
  });
}

export function formatTableCsvExportValue(
  value: unknown,
  category: TypeCategory | null,
  nativeType?: string,
): string {
  const displayValue = getDriverDisplaySnapshotValue(value, nativeType);
  if (displayValue !== undefined) return displayValue;
  if (typeof value === "string") {
    const parsed = tryParseStructuredExportValue(value, category);
    if (parsed !== undefined) {
      return stringifyJsonExportValue(normalizeNestedJsonExportValue(parsed));
    }
  }

  return formatExportCellValue(value);
}

export function serializeJsonExportRecord(
  entries: ReadonlyArray<
    ExportColumnDescriptor & {
      value: unknown;
      omit?: boolean;
    }
  >,
): string {
  return `{${entries
    .filter((entry) => !entry.omit)
    .map(
      (entry) =>
        `${JSON.stringify(entry.key)}:${stringifyJsonExportValue(
          normalizeJsonExportValue(
            entry.value,
            entry.category ?? null,
            entry.nativeType,
          ),
        )}`,
    )
    .join(",")}}`;
}

export function normalizeJsonExportValue(
  value: unknown,
  category: TypeCategory | null,
  nativeType?: string,
): JsonExportValue {
  const displayValue = getDriverDisplaySnapshotValue(value, nativeType);
  if (displayValue !== undefined) return displayValue;
  if (value == null) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : formatExportCellValue(value);
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "bigint") {
    return rawJsonLiteral(value.toString());
  }

  if (typeof value === "string") {
    const parsed = tryParseStructuredExportValue(value, category);
    if (parsed !== undefined) {
      return normalizeNestedJsonExportValue(parsed);
    }

    if (category === "boolean") {
      const lowered = value.trim().toLowerCase();
      if (lowered === "true") return true;
      if (lowered === "false") return false;
    }

    if (isNumericExportCategory(category)) {
      const numericLiteral = toJsonNumericLiteral(value);
      if (numericLiteral) {
        return rawJsonLiteral(numericLiteral);
      }
    }

    return value;
  }

  return normalizeNestedJsonExportValue(value);
}

function normalizeNestedJsonExportValue(value: unknown): JsonExportValue {
  if (value == null) {
    return null;
  }

  if (isRawNumberToken(value)) return rawJsonLiteral(value.raw);

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : formatExportCellValue(value);
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "bigint") {
    return rawJsonLiteral(value.toString());
  }

  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((entry) => normalizeNestedJsonExportValue(entry));
  }

  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        normalizeNestedJsonExportValue(entry),
      ]),
    );
  }

  return String(value);
}

function getDriverDisplaySnapshotValue(
  value: unknown,
  nativeType?: string,
): string | undefined {
  const redisDisplay = redisBinarySnapshotDisplay(value, nativeType);
  if (redisDisplay !== undefined) return redisDisplay;
  if (!value || typeof value !== "object") {
    return undefined;
  }

  const snapshot = value as {
    __rapidbSqlVariant?: unknown;
    __rapidbSpatial?: unknown;
    displayValue?: unknown;
  };
  if (
    ((nativeType?.toLowerCase() === "sql_variant" &&
      snapshot.__rapidbSqlVariant === 1) ||
      (["geometry", "geography"].includes(nativeType?.toLowerCase() ?? "") &&
        snapshot.__rapidbSpatial === 1)) &&
    typeof snapshot.displayValue === "string"
  ) {
    return snapshot.displayValue;
  }

  return undefined;
}

function stringifyJsonExportValue(value: JsonExportValue): string {
  if (isRawJsonLiteral(value)) {
    return value.literal;
  }

  if (value === null) {
    return "null";
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "null";
  }

  if (typeof value === "string") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((entry) => stringifyJsonExportValue(entry)).join(",")}]`;
  }

  return `{${Object.entries(value)
    .map(
      ([key, entry]) =>
        `${JSON.stringify(key)}:${stringifyJsonExportValue(entry)}`,
    )
    .join(",")}}`;
}

function tryParseStructuredExportValue(
  value: string,
  category: TypeCategory | null,
): unknown | undefined {
  if (category !== "json" && category !== "array") {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  try {
    JSON.parse(trimmed);
    return parseJsonPreservingRawNumbers(trimmed);
  } catch {
    return undefined;
  }
}

function isNumericExportCategory(category: TypeCategory | null): boolean {
  return (
    category === "integer" || category === "float" || category === "decimal"
  );
}

function toJsonNumericLiteral(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const normalized = trimmed.startsWith("+") ? trimmed.slice(1) : trimmed;
  return JSON_NUMBER_LITERAL_RE.test(normalized) ? normalized : null;
}

function rawJsonLiteral(literal: string): RawJsonLiteral {
  return {
    [RAW_EXPORT_LITERAL]: true,
    literal,
  };
}

function isRawJsonLiteral(value: JsonExportValue): value is RawJsonLiteral {
  return (
    typeof value === "object" && value !== null && RAW_EXPORT_LITERAL in value
  );
}

import { redisBinarySnapshotDisplay } from "../../shared/redisValueSnapshot";
