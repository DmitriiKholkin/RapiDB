/** Oracle's Gregorian, era-qualified key; not the text shown in the grid. */
export interface OracleTemporalPrimaryKeyValue {
  $rapidbOracleTemporalKey: string;
}

export type OracleTemporalRowIdentity = Record<
  string,
  OracleTemporalPrimaryKeyValue | null
>;

export function isOracleSignedTemporalText(value: unknown): value is string {
  // SYYYY emits a leading space for AD and a minus for BC; there is no year 0.
  return (
    typeof value === "string" &&
    (value.length === 20 || value.length === 30) &&
    /^[ -](?!0000)[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{9})?$/.test(
      value,
    )
  );
}

export function isOracleTemporalPrimaryKeyValue(
  value: unknown,
): value is OracleTemporalPrimaryKeyValue {
  return (
    !!value &&
    typeof value === "object" &&
    Object.keys(value).length === 1 &&
    Object.hasOwn(value, "$rapidbOracleTemporalKey") &&
    isOracleSignedTemporalText(
      (value as Partial<OracleTemporalPrimaryKeyValue>)
        .$rapidbOracleTemporalKey,
    )
  );
}
