import type { ColumnTypeMeta } from "./tableTypes";

export type MssqlSqlVariantPrimaryKeyValue = { $rapidbMssqlVariantKey: string };
export type MssqlSqlVariantRowIdentity = Record<
  string,
  MssqlSqlVariantPrimaryKeyValue | null
>;

export function isMssqlSqlVariantPrimaryKeyColumn(
  column: Pick<ColumnTypeMeta, "isPrimaryKey" | "baseType" | "nativeType">,
): boolean {
  return (
    column.isPrimaryKey &&
    (column.baseType ?? column.nativeType).trim().toLowerCase() ===
      "sql_variant"
  );
}

export function isMssqlSqlVariantPrimaryKeyValue(
  value: unknown,
): value is MssqlSqlVariantPrimaryKeyValue {
  if (!value || typeof value !== "object") return false;
  const key = (value as Partial<MssqlSqlVariantPrimaryKeyValue>)
    .$rapidbMssqlVariantKey;
  return (
    Object.hasOwn(value, "$rapidbMssqlVariantKey") &&
    typeof key === "string" &&
    /^v1:[a-z]+:\d{0,3}:\d{0,3}:\d{0,4}:[A-Za-z0-9_]{0,128}:(?:[0-9A-F]{2}){0,8000}$/.test(
      key,
    )
  );
}

export function mssqlSqlVariantKeyChangeMessage(columnName: string): string {
  return `The Table Viewer cannot safely change SQL Server sql_variant primary key "${columnName}". Use the Query Editor with an explicitly typed key.`;
}
