export function literalContainsPattern(value: string): string {
  return `%${value.replace(/[!%_]/g, "!$&")}%`;
}
