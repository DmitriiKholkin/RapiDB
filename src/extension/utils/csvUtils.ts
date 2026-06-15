/**
 * CSV cell formatting.
 *
 * Quotes a value when it contains characters that would otherwise be
 * ambiguous in a CSV stream: `,`, `"`, `\n`, or `\r`. Embedded quotes
 * are escaped by doubling. `null`/`undefined` and empty strings are
 * returned as an empty cell.
 */

const NEEDS_QUOTING = /[",\r\n]/;

export function csvCell(value: unknown): string {
  if (value == null) {
    return "";
  }
  const s = String(value);
  if (s === "") {
    return "";
  }
  return NEEDS_QUOTING.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
