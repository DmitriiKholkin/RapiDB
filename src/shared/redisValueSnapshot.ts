export interface RedisBinaryValueSnapshot {
  __rapidbRedisBinary: 1;
  displayValue: string;
  binaryHex: string;
}

export interface RedisBinaryCollectionValueSnapshot {
  __rapidbRedisBinaryCollection: 1;
  displayValue: string;
}

export interface RedisOversizedValueSnapshot {
  __rapidbRedisOversized: 1;
  displayValue: string;
}

const REDIS_VALUE_TYPES = new Set([
  "string",
  "hash",
  "list",
  "set",
  "zset",
  "stream",
]);

const REDIS_OVERSIZED_DISPLAY =
  /^RedisValue(?:TooLarge|NotLoaded)\((?:string|hash|list|set|zset|stream), \d+ (?:bytes|entries)\)$/;

export function redisOversizedValueSnapshot(
  redisType: string,
  size: number,
  unit: "bytes" | "entries" = redisType === "string" ? "bytes" : "entries",
  budgetExhausted = false,
): RedisOversizedValueSnapshot {
  return {
    __rapidbRedisOversized: 1,
    displayValue: `RedisValue${budgetExhausted ? "NotLoaded" : "TooLarge"}(${redisType}, ${size} ${unit})`,
  };
}

export function isRedisOversizedValueSnapshot(
  value: unknown,
): value is RedisOversizedValueSnapshot {
  if (!value || typeof value !== "object") return false;
  const snapshot = value as Partial<RedisOversizedValueSnapshot>;
  return (
    snapshot.__rapidbRedisOversized === 1 &&
    typeof snapshot.displayValue === "string" &&
    REDIS_OVERSIZED_DISPLAY.test(snapshot.displayValue)
  );
}

export function isRedisNotLoadedValueSnapshot(value: unknown): boolean {
  return (
    isRedisOversizedValueSnapshot(value) &&
    value.displayValue.startsWith("RedisValueNotLoaded(")
  );
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

export function isRedisBinaryCollectionValueSnapshot(
  value: unknown,
): value is RedisBinaryCollectionValueSnapshot {
  if (!value || typeof value !== "object") return false;
  const snapshot = value as Partial<RedisBinaryCollectionValueSnapshot>;
  return (
    snapshot.__rapidbRedisBinaryCollection === 1 &&
    typeof snapshot.displayValue === "string"
  );
}

export function redisBinarySnapshotDisplay(
  value: unknown,
  nativeType?: string,
): string | undefined {
  const type = nativeType?.toLowerCase() ?? "";
  const redisTypes = type.startsWith("mixed(")
    ? type
        .slice(6, -1)
        .split(",")
        .map((entry) => entry.trim())
    : [type];
  if (isRedisOversizedValueSnapshot(value)) {
    return redisTypes.some((entry) => REDIS_VALUE_TYPES.has(entry))
      ? value.displayValue
      : undefined;
  }
  if (isRedisBinaryCollectionValueSnapshot(value)) {
    return redisTypes.some(
      (entry) => entry !== "string" && REDIS_VALUE_TYPES.has(entry),
    )
      ? value.displayValue
      : undefined;
  }
  return redisTypes.includes("string") && isRedisBinaryValueSnapshot(value)
    ? value.displayValue
    : undefined;
}
