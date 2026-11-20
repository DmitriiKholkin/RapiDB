import type { ConnectionConfig } from "../connectionManager";
import type { TransactionVerificationFailure } from "./transactionVerification";

export const SQLITE_WORKER_METHODS = [
  "listDatabases",
  "listSchemas",
  "listObjects",
  "getSQLiteAttachmentTarget",
  "describeTable",
  "describeColumns",
  "describeSQLiteAttachmentColumns",
  "getIndexes",
  "getForeignKeys",
  "getConstraints",
  "getTriggers",
  "getConstraintDDL",
  "getIndexDDL",
  "getTriggerDDL",
  "getCreateTableDDL",
  "getObjectDefinition",
  "getRoutineDefinition",
  "query",
  "runTransaction",
] as const;

export type SQLiteWorkerMethod = (typeof SQLITE_WORKER_METHODS)[number];

// The worker and host share the OS monotonic clock, unlike performance.now(),
// whose origin is process-local.
export function sqliteConnectionNow(): number {
  return Number(process.hrtime.bigint()) / 1e6;
}

export interface SQLiteWorkerRequest {
  id: number;
  method: SQLiteWorkerMethod | "connect" | "disconnect";
  args: unknown[];
  deadline: number; // Monotonic milliseconds for connect, wall time otherwise.
  config?: ConnectionConfig;
  runtimeTargets?: string[];
  managedRuntimePackage?: string;
}

export function sqliteWorkerRequestExpired(
  request: SQLiteWorkerRequest,
): boolean {
  return (
    (request.method === "connect" ? sqliteConnectionNow() : Date.now()) >=
    request.deadline
  );
}

export interface SQLiteWorkerResponse {
  id: number;
  value?: unknown;
  error?: {
    message: string;
    name: string;
    verificationFailure?: TransactionVerificationFailure;
    rollbackConfirmed?: boolean;
    executionState?: "not-executed" | "unknown";
    managedNativeFailure?: true;
  };
}
