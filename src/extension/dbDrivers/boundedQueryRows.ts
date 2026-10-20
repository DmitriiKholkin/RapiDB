import { QUERY_LIMIT_POLICY } from "../../shared/safetyContracts";

export function queryCollectionLimit(
  hardCap: number | undefined,
): number | undefined {
  if (hardCap === undefined) return undefined;
  if (!Number.isSafeInteger(hardCap) || hardCap < 1) {
    throw new Error("[RapiDB] Query hardCap must be a positive integer.");
  }
  return Math.min(hardCap, QUERY_LIMIT_POLICY.hardCap + 1);
}

export class BoundedQueryRows<T> {
  rows: T[] = [];
  rowCount = 0;
  constructor(readonly limit: number) {}
  reset(): void {
    this.rows = [];
    this.rowCount = 0;
  }
  add(row: T): void {
    this.rowCount++;
    if (this.rows.length < this.limit) this.rows.push(row);
  }
  get truncated(): boolean {
    return this.rowCount > this.rows.length;
  }
}
