import { DatabaseError } from "pg";

/** True when a mutation is known not to have executed or committed. */
export function isMutationNotExecutedError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const state = error as { code?: unknown; executionState?: unknown };
  return (
    state.code === "NOT_EXECUTED" || state.executionState === "not-executed"
  );
}

/**
 * A PostgreSQL ErrorResponse with a SQLSTATE confirms that the statement was
 * rejected by the server. Connection failures and the explicit
 * statement/transaction completion-unknown states stay conservative.
 */
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

/** A local preflight rejected a mutation before it could be sent to a driver. */
export class MutationNotExecutedError extends Error {
  readonly executionState = "not-executed" as const;

  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "MutationNotExecutedError";
  }
}
