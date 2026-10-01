import type { ConnectionConfig } from "../connectionManager";

export const SQLITE_WORKER_METHODS = [
  "listDatabases",
  "listSchemas",
  "listObjects",
  "describeTable",
  "describeColumns",
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
export interface SQLiteWorkerRequest {
  id: number;
  method: SQLiteWorkerMethod | "connect" | "disconnect";
  args: unknown[];
  deadline: number;
  config?: ConnectionConfig;
  runtimeTargets?: string[];
}
export interface SQLiteWorkerResponse {
  id: number;
  value?: unknown;
  error?: { message: string; name: string };
}
