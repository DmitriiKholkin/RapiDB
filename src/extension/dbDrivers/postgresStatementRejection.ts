import { DatabaseError } from "pg";

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
