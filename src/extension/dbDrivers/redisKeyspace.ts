/** Internal table identities cannot collide with a Redis prefix (prefixes end
 * before the first colon). The old `default` identity remains an alias for
 * all keys so previously opened views continue to work. */
export const REDIS_ALL_KEYS_TABLE = "__rapidb:all-keys";
export const REDIS_UNPREFIXED_KEYS_TABLE = "__rapidb:unprefixed-keys";
export const REDIS_EMPTY_PREFIX_TABLE = "__rapidb:empty-prefix";

export function redisKeyspacePattern(table: string): string {
  if (table === REDIS_ALL_KEYS_TABLE || table === "default") return "*";
  if (table === REDIS_UNPREFIXED_KEYS_TABLE) return "*";
  if (table === REDIS_EMPTY_PREFIX_TABLE) return ":*";
  if (table === "default:") return "default:*";
  return `${table}:*`;
}

export function isRedisUnprefixedKeysTable(table: string): boolean {
  return table === REDIS_UNPREFIXED_KEYS_TABLE;
}

export function redisKeyspaceDisplayName(table: string): string {
  if (table === REDIS_ALL_KEYS_TABLE || table === "default") return "All keys";
  if (table === REDIS_UNPREFIXED_KEYS_TABLE) return "Unprefixed keys";
  if (table === REDIS_EMPTY_PREFIX_TABLE) return ":*";
  if (table === "default:") return "default:*";
  return `${table}:*`;
}

export function redisKeyspaceFileName(table: string): string {
  if (table === REDIS_ALL_KEYS_TABLE || table === "default") return "all-keys";
  if (table === REDIS_UNPREFIXED_KEYS_TABLE) return "unprefixed-keys";
  if (table === REDIS_EMPTY_PREFIX_TABLE) return "empty-prefix";
  if (table === "default:") return "default";
  return table;
}
