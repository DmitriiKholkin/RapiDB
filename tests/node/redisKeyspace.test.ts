import { describe, expect, it } from "vitest";
import {
  isRedisUnprefixedKeysTable,
  REDIS_ALL_KEYS_TABLE,
  REDIS_EMPTY_PREFIX_TABLE,
  REDIS_UNPREFIXED_KEYS_TABLE,
  redisKeyspaceDisplayName,
  redisKeyspacePattern,
} from "../../src/extension/dbDrivers/redisKeyspace";

describe("Redis keyspace identities", () => {
  it("keeps virtual scopes distinct from real key prefixes", () => {
    expect(redisKeyspaceDisplayName(REDIS_ALL_KEYS_TABLE)).toBe("All keys");
    expect(redisKeyspaceDisplayName(REDIS_UNPREFIXED_KEYS_TABLE)).toBe(
      "Unprefixed keys",
    );
    expect(redisKeyspaceDisplayName("default:")).toBe("default:*");
    expect(redisKeyspaceDisplayName("users")).toBe("users:*");

    expect(redisKeyspacePattern(REDIS_ALL_KEYS_TABLE)).toBe("*");
    expect(redisKeyspacePattern(REDIS_UNPREFIXED_KEYS_TABLE)).toBe("*");
    expect(redisKeyspacePattern("default:")).toBe("default:*");
    expect(redisKeyspacePattern("users")).toBe("users:*");
    expect(isRedisUnprefixedKeysTable(REDIS_UNPREFIXED_KEYS_TABLE)).toBe(true);
    expect(isRedisUnprefixedKeysTable(REDIS_ALL_KEYS_TABLE)).toBe(false);
  });

  it("retains the legacy all-keys alias and supports keys starting with a colon", () => {
    expect(redisKeyspacePattern("default")).toBe("*");
    expect(redisKeyspaceDisplayName("default")).toBe("All keys");
    expect(redisKeyspacePattern(REDIS_EMPTY_PREFIX_TABLE)).toBe(":*");
    expect(redisKeyspaceDisplayName(REDIS_EMPTY_PREFIX_TABLE)).toBe(":*");
  });
});
