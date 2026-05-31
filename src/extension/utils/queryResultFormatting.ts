import { type QueryColumnMeta } from "../../shared/tableTypes";
import {
  formatDatetimeForDisplay,
  hexFromBuffer,
  isHexLike,
  normalizeNumericDisplayValue,
  parseHexToBuffer,
} from "../dbDrivers/BaseDBDriver";
import { colKey, type QueryResult } from "../dbDrivers/types";

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

function isPointLike(value: object): value is { x: unknown; y: unknown } {
  return "x" in value && "y" in value && Object.keys(value).length === 2;
}

function isCircleLike(value: object): value is {
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

function hasNumericValue(value: unknown): value is number {
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

function formatIntervalLikeObject(value: object): string | null {
  const record = value as Record<string, unknown>;
  const years = hasNumericValue(record.years) ? record.years : 0;
  const months = hasNumericValue(record.months) ? record.months : 0;
  const days = hasNumericValue(record.days) ? record.days : 0;
  const hours = hasNumericValue(record.hours) ? record.hours : 0;
  const minutes = hasNumericValue(record.minutes) ? record.minutes : 0;
  const seconds = hasNumericValue(record.seconds) ? record.seconds : 0;
  const milliseconds = hasNumericValue(record.milliseconds)
    ? record.milliseconds
    : 0;
  const microseconds = hasNumericValue(record.microseconds)
    ? record.microseconds
    : 0;

  const knownKeys = new Set([
    "years",
    "months",
    "days",
    "hours",
    "minutes",
    "seconds",
    "milliseconds",
    "microseconds",
  ]);
  const keys = Object.keys(record);
  if (
    keys.length === 0 ||
    keys.some((key) => !knownKeys.has(key)) ||
    keys.some((key) => !hasNumericValue(record[key]))
  ) {
    return null;
  }

  const normalizedSeconds =
    seconds + milliseconds / 1000 + microseconds / 1_000_000;

  let iso = "P";
  if (years !== 0) iso += `${trimNumericFraction(years)}Y`;
  if (months !== 0) iso += `${trimNumericFraction(months)}M`;
  if (days !== 0) iso += `${trimNumericFraction(days)}D`;

  if (hours !== 0 || minutes !== 0 || normalizedSeconds !== 0) {
    iso += "T";
    if (hours !== 0) iso += `${trimNumericFraction(hours)}H`;
    if (minutes !== 0) iso += `${trimNumericFraction(minutes)}M`;
    if (normalizedSeconds !== 0) {
      iso += `${trimNumericFraction(normalizedSeconds)}S`;
    }
  }

  return iso === "P" ? "P0D" : iso;
}

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
