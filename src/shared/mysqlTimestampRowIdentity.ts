/** MySQL's stored TIMESTAMP instant, not its session-local display text. */
export interface MySQLTimestampPrimaryKeyValue {
  $rapidbMySqlTimestampKey: string;
}

export type MySQLTimestampRowIdentity = Record<
  string,
  MySQLTimestampPrimaryKeyValue | null
>;

export function isMySQLTimestampPrimaryKeyValue(
  value: unknown,
): value is MySQLTimestampPrimaryKeyValue {
  if (!value || typeof value !== "object") return false;
  const key = (value as Partial<MySQLTimestampPrimaryKeyValue>)
    .$rapidbMySqlTimestampKey;
  return (
    Object.keys(value).length === 1 &&
    Object.hasOwn(value, "$rapidbMySqlTimestampKey") &&
    typeof key === "string" &&
    /^(?:0|[1-9][0-9]{0,9})\.[0-9]{6}$/.test(key)
  );
}
