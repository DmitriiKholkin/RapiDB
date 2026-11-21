import { jsonStringByteSize } from "../utils/queryResultSize";
import type { BoundedPgResultField } from "./postgresBoundedQuery";

export const POSTGRES_QUERY_RESULT_LIMITS = {
  cellBytes: 256 * 1024,
  totalBytes: 8 * 1024 * 1024,
} as const;

type ResultSizeLimits = { cellBytes: number; totalBytes: number };
const ROW_OVERHEAD = 32;
const CELL_OVERHEAD = 48;

// Include representation overhead as well as display encoding. This is an
// accounting budget, not a measurement or a guarantee about the V8 heap.
function decodedCellSize(value: unknown, limit: number): number {
  let remaining = limit;
  const charge = (bytes: number) => {
    remaining -= bytes;
    return remaining >= 0;
  };
  function visit(entry: unknown, depth: number): boolean {
    if (depth > 32 || !charge(CELL_OVERHEAD)) return false;
    if (typeof entry === "string") {
      if (entry.length * 2 > remaining) return false;
      return charge(
        Math.max(
          entry.length * 2,
          jsonStringByteSize(entry, remaining, depth > 0),
        ),
      );
    }
    // Top-level bytea is displayed as hex. A nested Buffer would be serialized
    // as JSON decimal bytes, up to 4 bytes per input byte plus its wrapper.
    if (Buffer.isBuffer(entry))
      return charge(depth === 0 ? 2 * entry.length : 4 * entry.length + 32);
    if (typeof entry === "bigint")
      return entry >= -(2n ** 63n) && entry < 2n ** 64n;
    if (entry === null || typeof entry !== "object" || entry instanceof Date)
      return true;
    if (Array.isArray(entry)) {
      for (const item of entry) if (!visit(item, depth + 1)) return false;
    } else {
      // pg's interval and geometric parsers produce small scalar records.
      for (const key in entry) {
        if (
          Object.hasOwn(entry, key) &&
          (!charge(
            Math.max(key.length * 2, jsonStringByteSize(key, remaining, true)),
          ) ||
            !visit((entry as Record<string, unknown>)[key], depth + 1))
        )
          return false;
      }
    }
    return true;
  }
  return visit(value, 0) ? limit - remaining : Infinity;
}

export class PostgresQueryResultBudget {
  private remaining: number;
  constructor(
    private readonly limits: ResultSizeLimits = POSTGRES_QUERY_RESULT_LIMITS,
  ) {
    this.remaining = limits.totalBytes;
  }

  retainFields(fields: readonly BoundedPgResultField[]): boolean {
    let remaining = this.remaining - 256;
    for (const { name } of fields) {
      remaining -= 128;
      const limit = Math.min(this.limits.cellBytes, remaining);
      const bytes = Math.max(name.length * 2, jsonStringByteSize(name, limit));
      if (bytes > limit) return false;
      remaining -= bytes;
    }
    if (remaining < 0) return false;
    this.remaining = remaining;
    return true;
  }

  // pg-protocol has already materialized the wire row (normally strings).
  // Check it before pg runs the column parsers (bytea, arrays, etc.).
  wireRowSize(fields: readonly unknown[]): number | undefined {
    let remaining = this.remaining - ROW_OVERHEAD;
    for (const field of fields) {
      remaining -= CELL_OVERHEAD;
      const limit = Math.min(remaining, this.limits.cellBytes - CELL_OVERHEAD);
      const length =
        typeof field === "string"
          ? field.length * 2
          : Buffer.isBuffer(field)
            ? field.length
            : 0;
      if (length > limit) return;
      const bytes =
        typeof field === "string"
          ? Math.max(length, Buffer.byteLength(field, "utf8"))
          : length;
      if (bytes > limit) return;
      remaining -= bytes;
    }
    if (remaining < 0) return;
    return this.remaining - remaining;
  }

  retainRow(row: readonly unknown[], wireBytes: number): boolean {
    let remaining = this.remaining - ROW_OVERHEAD;
    for (const cell of row) {
      remaining -= decodedCellSize(
        cell,
        Math.min(this.limits.cellBytes, remaining),
      );
      if (remaining < 0) return false;
    }
    // A parser may keep pieces of the original wire string alive.
    this.remaining = Math.min(remaining, this.remaining - wireBytes);
    return true;
  }
}
