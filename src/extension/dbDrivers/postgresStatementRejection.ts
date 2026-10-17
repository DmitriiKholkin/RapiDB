import { DatabaseError } from "pg";

/**
 * A PostgreSQL ErrorResponse with a SQLSTATE confirms that the statement was
 * rejected by the server. Connection failures and the explicit
 * statement/transaction completion-unknown states stay conservative.
 * Keep this dependency out of shared mutation errors used by the SQLite worker.
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
