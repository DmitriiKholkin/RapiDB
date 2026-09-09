/**
 * CSV cell formatting.
 *
 * Quotes a value when it contains characters that would otherwise be
 * ambiguous in a CSV stream: `,`, `"`, `\n`, `\r`, or `\t`. Embedded quotes
 * are escaped by doubling. `null`/`undefined` and empty strings are
 * returned as an empty cell.
 */

const NEEDS_QUOTING = /[",\r\n\t]/;

export function csvCell(value: unknown): string {
  if (value == null) {
    return "";
  }
  const raw = String(value);
  const s =
    typeof value === "string" && /^[=+\-@\t\r]/.test(raw) ? `'${raw}` : raw;
  if (s === "") {
    return "";
  }
  return NEEDS_QUOTING.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
