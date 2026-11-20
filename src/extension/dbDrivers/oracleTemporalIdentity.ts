export function oracleTemporalIdentityKind(nativeType: string) {
  const type = nativeType
    .toUpperCase()
    .replace(/\(\s*\d+\s*\)/g, "")
    .replace(/\s+/g, " ")
    .trim();
  switch (type) {
    case "DATE":
      return "date";
    case "TIMESTAMP":
      return "timestamp";
    case "TIMESTAMP WITH LOCAL TIME ZONE":
      return "localTimeZone";
    case "TIMESTAMP WITH TIME ZONE":
      return "timeZone";
    default:
      return null;
  }
}

type OracleTemporalIdentityKind = NonNullable<
  ReturnType<typeof oracleTemporalIdentityKind>
>;

function oracleTemporalIdentityFormat(kind: OracleTemporalIdentityKind) {
  // The signed year and calendar must agree across pooled sessions.
  const fraction = kind === "date" ? "" : ".FF9";
  return `'SYYYY-MM-DD HH24:MI:SS${fraction}', 'NLS_DATE_LANGUAGE=American NLS_CALENDAR=Gregorian'`;
}

function oracleLocalTimestampFromUtc(timestamp: string): string {
  return `CAST(FROM_TZ(${timestamp}, '+00:00') AS TIMESTAMP(9) WITH LOCAL TIME ZONE)`;
}

export function oracleTemporalIdentityProjection(
  kind: OracleTemporalIdentityKind,
  name: string,
): string {
  const expression =
    kind === "localTimeZone" || kind === "timeZone"
      ? `SYS_EXTRACT_UTC(CAST(${name} AS TIMESTAMP(9) WITH TIME ZONE))`
      : name;
  const text = `TO_CHAR(${expression}, ${oracleTemporalIdentityFormat(kind)})`;
  // If a zone conversion cannot round-trip to the stored key, do not
  // publish an identity that might reconstruct to another row.
  return kind === "localTimeZone"
    ? `CASE WHEN ${name} = ${oracleLocalTimestampFromUtc(expression)} THEN ${text} END`
    : text;
}

export function oracleTemporalIdentityComparison(
  kind: OracleTemporalIdentityKind,
  name: string,
  operator: "=" | ">" | "<",
  paramIndex: number,
): string {
  const format = oracleTemporalIdentityFormat(kind);
  if (kind === "date")
    return `${name} ${operator} TO_DATE(:${paramIndex}, ${format})`;
  const timestamp = `TO_TIMESTAMP(:${paramIndex}, ${format})`;
  if (kind === "localTimeZone") {
    return `${name} ${operator} ${oracleLocalTimestampFromUtc(timestamp)}`;
  }
  return kind === "timeZone"
    ? `SYS_EXTRACT_UTC(CAST(${name} AS TIMESTAMP(9) WITH TIME ZONE)) ${operator} ${timestamp}`
    : `${name} ${operator} ${timestamp}`;
}
