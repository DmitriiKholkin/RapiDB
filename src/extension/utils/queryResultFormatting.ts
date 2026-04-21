import {
  inferQueryColumnCategory,
  type QueryColumnMeta,
} from "../../shared/tableTypes";
import { formatDatetimeForDisplay } from "../dbDrivers/BaseDBDriver";
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

  return {
    ...result,
    columnMeta: resolveQueryColumnMeta(
      result.columns,
      result.columnMeta,
      result.rows,
    ),
    rows: normalizeQueryRows(sampledRows),
    truncated,
    truncatedAt: rowLimit,
  };
}

function normalizeQueryRows(
  rows: readonly Record<string, unknown>[],
): Record<string, unknown>[] {
  return rows.map((row) => {
    const normalized: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(row)) {
      if (typeof value === "bigint") {
        normalized[key] = value.toString();
        continue;
      }

      if (Buffer.isBuffer(value)) {
        normalized[key] =
          value.length === 0 ? "" : `\\x${value.toString("hex")}`;
        continue;
      }

      if (
        value !== null &&
        typeof value === "object" &&
        !(value instanceof Date) &&
        !Buffer.isBuffer(value)
      ) {
        normalized[key] = JSON.stringify(value);
        continue;
      }

      const formattedDatetime = formatDatetimeForDisplay(value);
      normalized[key] = formattedDatetime !== null ? formattedDatetime : value;
    }

    return normalized;
  });
}

function resolveQueryColumnMeta(
  columns: string[],
  rawMeta: QueryColumnMeta[] | undefined,
  rows: Record<string, unknown>[],
): QueryColumnMeta[] {
  const samples = rows.slice(0, 50);
  return columns.map((_, index) => ({
    category:
      rawMeta?.[index]?.category ??
      inferQueryColumnCategory(samples.map((row) => row[colKey(index)])),
  }));
}
