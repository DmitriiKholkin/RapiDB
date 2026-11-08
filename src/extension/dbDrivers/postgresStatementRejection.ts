import { DatabaseError } from "pg";

export class PostgresStatementRejectedAndRolledBackError extends Error {
  readonly executionState = "not-executed" as const;

  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = "PostgresStatementRejectedAndRolledBackError";
  }
}

export function isPostgresStatementRejectedAndRolledBack(
  error: unknown,
): error is PostgresStatementRejectedAndRolledBackError {
  return error instanceof PostgresStatementRejectedAndRolledBackError;
}

export function isPostgresStatementRejection(error: unknown): boolean {
  if (!(error instanceof DatabaseError)) return false;
  const code = error.code;
  if (typeof code !== "string" || !/^[0-9A-Z]{5}$/i.test(code)) return false;

  const sqlState = code.toUpperCase();
  return (
    !sqlState.startsWith("08") &&
    sqlState !== "40003" &&
    sqlState !== "57P01" &&
    sqlState !== "57P02"
  );
}
