/**
 * Transform a raw `QueryResult` (driver output) into a
 * `FormattedQueryResult` (webview-ready) by:
 *  - applying a row-limit (and reporting whether truncation happened);
 *  - resolving column metadata (one entry per column, defaulting to
 *    `category: null`);
 *  - normalizing each cell to a value the table renderer can display
 *    (numbers, bigints, buffers, dates, points, intervals, etc.).
 */

import { type QueryColumnMeta } from "../../shared/tableTypes";
import {
  formatDatetimeForDisplay,
  hexFromBuffer,
  isHexLike,
  normalizeNumericDisplayValue,
  parseHexToBuffer,
} from "../dbDrivers/BaseDBDriver";
import { colKey, type QueryResult } from "../dbDrivers/types";
import { serializeArrayPreservingRawTokens } from "./arraySerialization";

export interface FormattedQueryResult extends QueryResult {
  columnMeta: QueryColumnMeta[];
  rows: Record<string, unknown>[];
  truncated: boolean;
  truncatedAt: number;
}

export function formatQueryResult(
  result: QueryResult,
  rowLimit: number,
): FormattedQueryResult {
  const truncated = result.rows.length > rowLimit;
  const sampledRows = truncated ? result.rows.slice(0, rowLimit) : result.rows;
  const columnMeta = resolveQueryColumnMeta(result.columns, result.columnMeta);

  return {
    ...result,
    columnMeta,
    rows: normalizeQueryRows(sampledRows, result.columns, columnMeta),
    truncated,
    truncatedAt: rowLimit,
  };
}

// ── Cell value type guards ──────────────────────────────────────────────────

/** `value` has exactly `{ x, y }` — a 2D point shape. */
export function isPointLike(
  value: object,
): value is { x: unknown; y: unknown } {
  return "x" in value && "y" in value && Object.keys(value).length === 2;
}

/** `value` has exactly `{ x, y, radius }` — a circle shape. */
export function isCircleLike(value: object): value is {
  x: unknown;
  y: unknown;
  radius: unknown;
} {
  return (
    "x" in value &&
    "y" in value &&
    "radius" in value &&
    Object.keys(value).length === 3
  );
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function trimNumericFraction(value: number): string {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value
    .toString()
    .replace(/(\.\d*?[1-9])0+$/, "$1")
    .replace(/\.0+$/, "");
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

// ── Interval formatting (ISO 8601 duration) ────────────────────────────────

const INTERVAL_FIELDS = [
  "years",
  "months",
  "days",
  "hours",
  "minutes",
  "seconds",
  "milliseconds",
  "microseconds",
] as const;

const INTERVAL_KEY_SET: ReadonlySet<string> = new Set(INTERVAL_FIELDS);

type IntervalFieldValues = Record<(typeof INTERVAL_FIELDS)[number], number>;

function readIntervalFields(
  record: Record<string, unknown>,
): IntervalFieldValues {
  return {
    years: isFiniteNumber(record.years) ? record.years : 0,
    months: isFiniteNumber(record.months) ? record.months : 0,
    days: isFiniteNumber(record.days) ? record.days : 0,
    hours: isFiniteNumber(record.hours) ? record.hours : 0,
    minutes: isFiniteNumber(record.minutes) ? record.minutes : 0,
    seconds: isFiniteNumber(record.seconds) ? record.seconds : 0,
    milliseconds: isFiniteNumber(record.milliseconds) ? record.milliseconds : 0,
    microseconds: isFiniteNumber(record.microseconds) ? record.microseconds : 0,
  };
}

function isRecognizedIntervalRecord(record: Record<string, unknown>): boolean {
  const keys = Object.keys(record);
  if (keys.length === 0) return false;
  return keys.every(
    (key) => INTERVAL_KEY_SET.has(key) && isFiniteNumber(record[key]),
  );
}

function appendIntervalSegment(
  iso: string,
  value: number,
  suffix: string,
): string {
  return value !== 0 ? `${iso}${trimNumericFraction(value)}${suffix}` : iso;
}

function buildIntervalIso(fields: IntervalFieldValues): string {
  const normalizedSeconds =
    fields.seconds +
    fields.milliseconds / 1000 +
    fields.microseconds / 1_000_000;

  let iso = "P";
  iso = appendIntervalSegment(iso, fields.years, "Y");
  iso = appendIntervalSegment(iso, fields.months, "M");
  iso = appendIntervalSegment(iso, fields.days, "D");

  if (fields.hours !== 0 || fields.minutes !== 0 || normalizedSeconds !== 0) {
    iso += "T";
    iso = appendIntervalSegment(iso, fields.hours, "H");
    iso = appendIntervalSegment(iso, fields.minutes, "M");
    if (normalizedSeconds !== 0) {
      iso += `${trimNumericFraction(normalizedSeconds)}S`;
    }
  }

  return iso === "P" ? "P0D" : iso;
}

/**
 * Try to format an object as an ISO 8601 duration (`PnYnMnDTnHnMnS`).
 * Returns `null` if the object has unknown keys or non-numeric values.
 */
function formatIntervalLikeObject(value: object): string | null {
  const record = value as Record<string, unknown>;
  if (!isRecognizedIntervalRecord(record)) {
    return null;
  }
  return buildIntervalIso(readIntervalFields(record));
}

// ── Row normalization ──────────────────────────────────────────────────────

function normalizeQueryRows(
  rows: readonly Record<string, unknown>[],
  columns: readonly string[],
  columnMeta: readonly QueryColumnMeta[],
): Record<string, unknown>[] {
  return rows.map((row) => {
    const normalized: Record<string, unknown> = {};

    for (const [index, key] of columns.entries()) {
      const normalizedKey = colKey(index);
      const value = Object.hasOwn(row, normalizedKey)
        ? row[normalizedKey]
        : row[key];
      const category = columnMeta[index]?.category ?? null;

      if (typeof value === "number") {
        normalized[normalizedKey] = normalizeNumericDisplayValue(value);
        continue;
      }

      if (typeof value === "bigint") {
        normalized[normalizedKey] = value.toString();
        continue;
      }

      if (Buffer.isBuffer(value)) {
        normalized[normalizedKey] = hexFromBuffer(value);
        continue;
      }

      if (
        category === "binary" &&
        typeof value === "string" &&
        isHexLike(value)
      ) {
        normalized[normalizedKey] = hexFromBuffer(parseHexToBuffer(value));
        continue;
      }

      if (
        value !== null &&
        typeof value === "object" &&
        !(value instanceof Date) &&
        !Buffer.isBuffer(value)
      ) {
        if (isCircleLike(value)) {
          normalized[normalizedKey] =
            `<(${String(value.x)},${String(value.y)}),${String(value.radius)}>`;
          continue;
        }

        if (isPointLike(value)) {
          normalized[normalizedKey] =
            `(${String(value.x)}, ${String(value.y)})`;
          continue;
        }

        const intervalLike = formatIntervalLikeObject(value);
        if (intervalLike !== null) {
          normalized[normalizedKey] = intervalLike;
          continue;
        }

        if (Array.isArray(value)) {
          normalized[normalizedKey] = serializeArrayPreservingRawTokens(value);
          continue;
        }

        normalized[normalizedKey] = safeStringify(value);
        continue;
      }

      const formattedDatetime = formatDatetimeForDisplay(value);
      normalized[normalizedKey] =
        formattedDatetime !== null ? formattedDatetime : value;
    }

    return normalized;
  });
}

function resolveQueryColumnMeta(
  columns: string[],
  rawMeta: QueryColumnMeta[] | undefined,
): QueryColumnMeta[] {
  return columns.map((_, index) => ({
    category: rawMeta?.[index]?.category ?? null,
  }));
}
