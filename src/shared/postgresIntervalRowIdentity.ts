export type PostgresIntervalPrimaryKeyKind = "scalar" | "array";

/** PostgreSQL's binary interval or interval-array identity, in hex. */
export interface PostgresIntervalPrimaryKeyValue {
  $rapidbPgIntervalKey: string;
}

export type PostgresIntervalRowIdentity = Record<
  string,
  PostgresIntervalPrimaryKeyValue | null
>;

export function isPostgresIntervalPrimaryKeyValue(
  value: unknown,
  kind?: PostgresIntervalPrimaryKeyKind,
): value is PostgresIntervalPrimaryKeyValue {
  if (
    value === null ||
    typeof value !== "object" ||
    Object.keys(value).length !== 1 ||
    !Object.hasOwn(value, "$rapidbPgIntervalKey")
  ) {
    return false;
  }
  const hex = (value as PostgresIntervalPrimaryKeyValue).$rapidbPgIntervalKey;
  if (typeof hex !== "string" || /[^0-9a-f]/.test(hex)) return false;
  const scalar = hex.length === 32;
  // Binary arrays have a 12-byte header, plus dimensions and elements.
  const array = hex.length === 24 || (hex.length >= 48 && hex.length % 2 === 0);
  return kind === "scalar"
    ? scalar
    : kind === "array"
      ? array
      : scalar || array;
}
