/** Table-only byte identity; a literal Redis string is never decoded as a marker. */
export interface RedisBinaryValueSnapshot {
  __rapidbRedisBinary: 1;
  displayValue: string;
  binaryHex: string;
}

export function isRedisBinaryValueSnapshot(
  value: unknown,
): value is RedisBinaryValueSnapshot {
  if (!value || typeof value !== "object") return false;
  const snapshot = value as Partial<RedisBinaryValueSnapshot>;
  return (
    snapshot.__rapidbRedisBinary === 1 &&
    typeof snapshot.binaryHex === "string" &&
    /^(?:[0-9a-f]{2})*$/i.test(snapshot.binaryHex) &&
    snapshot.displayValue === `RedisBinary(0x${snapshot.binaryHex})`
  );
}

export function redisBinarySnapshotDisplay(
  value: unknown,
  nativeType?: string,
): string | undefined {
  const type = nativeType?.toLowerCase() ?? "";
  const stringType =
    type === "string" ||
    (type.startsWith("mixed(") &&
      type
        .slice(6, -1)
        .split(",")
        .some((entry) => entry.trim() === "string"));
  return stringType && isRedisBinaryValueSnapshot(value)
    ? value.displayValue
    : undefined;
}
