export function escapeSqlPreviewStringLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

export function formatSqlPreviewStringLiteral(
  value: string,
  prefix = "",
): string {
  return `${prefix}'${escapeSqlPreviewStringLiteral(value)}'`;
}

export function formatHexSqlPreviewLiteral(
  value: Buffer,
  options?: {
    prefix?: string;
    suffix?: string;
  },
): string {
  return `${options?.prefix ?? ""}${value.toString("hex")}${options?.suffix ?? ""}`;
}
