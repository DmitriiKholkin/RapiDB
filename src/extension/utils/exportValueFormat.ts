/**
 * Pure value-formatting helpers for the export pipeline.
 *
 * Extracted from `exportService.ts` to:
 * 1. keep the I/O orchestration file focused on streaming and UI flow,
 * 2. make the value → string / value → JSON-value transformations
 *    individually unit-testable in isolation,
 * 3. keep the file under the 200-line "component" guideline from the
 *    project code-quality standards.
 *
 * All functions here are pure (no I/O, no global state).
 */

import type { QueryColumnMeta, TypeCategory } from "../dbDrivers/types";

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

export type RawJsonLiteral = {
  readonly __rapidbRawJsonLiteral: unique symbol;
  readonly literal: string;
};

const JSON_NUMBER_LITERAL_RE = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;

const pad2 = (n: number): string => String(n).padStart(2, "0");

/**
 * Generate the synthetic column key used in query-result export rows
 * (e.g. `__col_0`). Centralised so format conversions stay in sync.
 */
export function queryColumnKey(index: number): string {
  return `__col_${index}`;
}

/**
 * Format any value as a plain string suitable for a CSV cell.
 *
 * - `null` / `undefined` → empty string
 * - `bigint` → decimal string
 * - `Date` → canonical `YYYY-MM-DD HH:MM:SS` UTC
 * - objects / arrays → JSON string (with fallback to `String(value)`)
 */
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

/**
 * Build the column descriptor list for a query-result export, deduplicating
 * column names that collide (e.g. two `id` columns become `id`, `id_2`).
 */
export function buildQueryExportColumns(
  columns: readonly string[],
  columnMeta?: readonly QueryColumnMeta[],
): ExportColumnDescriptor[] {
  const seenColumnNames = new Map<string, number>();

  return columns.map((columnName, index) => {
    const seenCount = seenColumnNames.get(columnName) ?? 0;
    seenColumnNames.set(columnName, seenCount + 1);

    return {
      key: seenCount === 0 ? columnName : `${columnName}_${seenCount + 1}`,
      sourceKey: queryColumnKey(index),
      category: columnMeta?.[index]?.category ?? null,
    } satisfies ExportColumnDescriptor;
  });
}

/**
 * Format a single cell for the table-data CSV export. Mirrors
 * {@link formatExportCellValue} but additionally tries to parse
 * string-encoded JSON values for `json` / `array` columns so the
 * CSV cell contains a properly-typed representation.
 */
export function formatTableCsvExportValue(
  value: unknown,
  category: TypeCategory | null,
): string {
  if (typeof value === "string") {
    const parsed = tryParseStructuredExportValue(value, category);
    if (parsed !== undefined) {
      return formatExportCellValue(parsed);
    }
  }

  return formatExportCellValue(value);
}

/**
 * Serialize one row of a JSON export, including the column-name → JSON
 * value mapping. Keeps the row's shape stable for both query and
 * table-data exports.
 */
export function serializeJsonExportRecord(
  entries: ReadonlyArray<
    ExportColumnDescriptor & {
      value: unknown;
    }
  >,
): string {
  return `{${entries
    .map(
      (entry) =>
        `${JSON.stringify(entry.key)}:${stringifyJsonExportValue(
          normalizeJsonExportValue(entry.value, entry.category ?? null),
        )}`,
    )
    .join(",")}}`;
}

/**
 * Normalize a query/table value into a JSON-friendly shape, honouring
 * per-column category hints (e.g. parse JSON strings, coerce booleans
 * and numerics, wrap `bigint` as a raw literal).
 */
export function normalizeJsonExportValue(
  value: unknown,
  category: TypeCategory | null,
): JsonExportValue {
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
    return JSON.parse(trimmed) as unknown;
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
    __rapidbRawJsonLiteral: Symbol(
      "rapidb-raw-json",
    ) as RawJsonLiteral["__rapidbRawJsonLiteral"],
    literal,
  };
}

function isRawJsonLiteral(value: JsonExportValue): value is RawJsonLiteral {
  return typeof value === "object" && value !== null && "literal" in value;
}
