/** SQLite keys travel separately from display cells. The tag tells the SQL
 * builder how to bind the value without applying edit-input coercion. */
export interface SQLitePrimaryKeyValue {
  $rapidbSqliteKey: string;
}

export type SQLiteRowIdentity = Record<string, SQLitePrimaryKeyValue | null>;

export function isSQLitePrimaryKeyValue(
  value: unknown,
): value is SQLitePrimaryKeyValue {
  if (value === null || typeof value !== "object") return false;
  if (
    !Object.hasOwn(value, "$rapidbSqliteKey") ||
    Object.keys(value).length !== 1
  )
    return false;
  const token = (value as SQLitePrimaryKeyValue).$rapidbSqliteKey;
  if (typeof token !== "string") return false;
  const body = token.slice(2);
  switch (token.slice(0, 2)) {
    case "i:":
      return /^(?:0|-?[1-9]\d{0,18})$/.test(body);
    case "r:":
      return body !== "NaN" && String(Number(body)) === body;
    case "t:":
    case "b:":
      return body.length % 2 === 0 && !/[^0-9A-F]/.test(body);
    default:
      return false;
  }
}
