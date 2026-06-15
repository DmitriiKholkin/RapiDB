/**
 * SQL string utility functions.
 * Extracted from BaseDBDriver for single-responsibility adherence.
 */

/** Ensure a SQL statement ends with a semicolon. */
export function ensureSqlTerminator(sql: string): string {
  const trimmed = sql.trimEnd();
  return trimmed.endsWith(";") ? trimmed : `${trimmed};`;
}
