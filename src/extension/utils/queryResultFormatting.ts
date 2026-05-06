import { type QueryColumnMeta } from "../../shared/tableTypes";
import {
  formatDatetimeForDisplay,
  hexFromBuffer,
  isHexLike,
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
        normalized[normalizedKey] = JSON.stringify(value);
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
