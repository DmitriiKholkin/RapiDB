/**
 * Escapes a single cell value for CSV output.
 * Returns empty string for null/undefined.
 * Wraps in double-quotes if the value contains commas, quotes, or newlines.
 */
export function csvCell(value: unknown): string {
  if (value == null) {
    return "";
  }
  const s = String(value);
  if (s === "") {
    return "";
  }
  return s.includes(",") ||
    s.includes('"') ||
    s.includes("\n") ||
    s.includes("\r")
    ? `"${s.replace(/"/g, '""')}"`
    : s;
}
