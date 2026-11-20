// Database identifiers from the catalog are literal, including whitespace.
export function isPostgresQueryDatabase(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && !value.includes("\0");
}
