import { isPostgresQueryDatabase } from "./postgresQueryDatabase";

export interface QueryDatabaseFields {
  postgresDatabase?: string;
  mssqlDatabase?: string;
}

// Preserve the existing PostgreSQL string argument and persisted field.
export type QueryDatabaseScope = string | Readonly<{ mssqlDatabase: string }>;

export function isMssqlQueryDatabase(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 128 &&
    !value.includes("\0")
  );
}

export function readQueryDatabaseScope({
  postgresDatabase,
  mssqlDatabase,
}: {
  postgresDatabase?: unknown;
  mssqlDatabase?: unknown;
}): QueryDatabaseScope | null | undefined {
  if (mssqlDatabase !== undefined) {
    return postgresDatabase === undefined && isMssqlQueryDatabase(mssqlDatabase)
      ? { mssqlDatabase }
      : null;
  }
  if (postgresDatabase !== undefined) {
    return isPostgresQueryDatabase(postgresDatabase) ? postgresDatabase : null;
  }
  return undefined;
}

export function queryDatabaseFields(
  scope?: QueryDatabaseScope,
): QueryDatabaseFields {
  return typeof scope === "string"
    ? { postgresDatabase: scope }
    : scope === undefined
      ? {}
      : { mssqlDatabase: scope.mssqlDatabase };
}

export function queryDatabaseTarget(
  scope?: QueryDatabaseScope,
): { type: "pg" | "mssql"; database: string; label: string } | undefined {
  if (scope === undefined) return undefined;
  return typeof scope === "string"
    ? { type: "pg", database: scope, label: "PostgreSQL" }
    : { type: "mssql", database: scope.mssqlDatabase, label: "SQL Server" };
}
