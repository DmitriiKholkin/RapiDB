/**
 * LIKE pattern for the UI's literal Contains operator. Pair with ESCAPE '!'.
 * A non-backslash escape keeps the SQL independent of dialect string modes;
 * backslashes in the bound value are ordinary characters, not LIKE escapes.
 */
export function literalContainsPattern(value: string): string {
  return `%${value.replace(/[!%_]/g, "!$&")}%`;
}
