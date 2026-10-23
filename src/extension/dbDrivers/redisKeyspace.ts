export const REDIS_ALL_KEYS_TABLE = "__rapidb:all-keys";
export const REDIS_UNPREFIXED_KEYS_TABLE = "__rapidb:unprefixed-keys";
export const REDIS_EMPTY_PREFIX_TABLE = "__rapidb:empty-prefix";

export function redisKeyspacePattern(table: string): string {
  if (table === REDIS_ALL_KEYS_TABLE || table === "default") return "*";
  if (table === REDIS_UNPREFIXED_KEYS_TABLE) return "*";
  if (table === REDIS_EMPTY_PREFIX_TABLE) return ":*";
  if (table === "default:") return "default:*";
  // Explorer groups come from literal key prefixes, not user-supplied globs.
  // Unescaped metacharacters can include another prefix or hide this one.
  return `${table.replace(/[\\*?[\]]/g, "\\$&")}:*`;
}

export function isRedisUnprefixedKeysTable(table: string): boolean {
  return table === REDIS_UNPREFIXED_KEYS_TABLE;
}

export function redisKeyspaceDisplayName(table: string): string {
  if (table === REDIS_ALL_KEYS_TABLE || table === "default") return "All keys";
  if (table === REDIS_UNPREFIXED_KEYS_TABLE) return "Unprefixed keys";
  if (table === REDIS_EMPTY_PREFIX_TABLE) return ":*";
  if (table === "default:") return "default:*";
  return redisKeyspacePattern(table);
}

export function redisKeyspaceFileName(table: string): string {
  if (table === REDIS_ALL_KEYS_TABLE || table === "default") return "all-keys";
  if (table === REDIS_UNPREFIXED_KEYS_TABLE) return "unprefixed-keys";
  if (table === REDIS_EMPTY_PREFIX_TABLE) return "empty-prefix";
  if (table === "default:") return "default";
  return table;
}
