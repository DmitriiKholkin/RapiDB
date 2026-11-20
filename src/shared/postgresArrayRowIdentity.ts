/** Server-native array text, separate from the JSON shown in the grid. */
export interface PostgresArrayPrimaryKeyValue {
  $rapidbPgArrayKey: string;
}

export type PostgresArrayRowIdentity = Record<
  string,
  PostgresArrayPrimaryKeyValue | null
>;

export function isPostgresArrayPrimaryKeyValue(
  value: unknown,
): value is PostgresArrayPrimaryKeyValue {
  return (
    value !== null &&
    typeof value === "object" &&
    Object.keys(value).length === 1 &&
    Object.hasOwn(value, "$rapidbPgArrayKey") &&
    typeof (value as PostgresArrayPrimaryKeyValue).$rapidbPgArrayKey ===
      "string"
  );
}
