import { vi } from "vitest";

export function withRedisReplyMapping<T extends object>(client: T) {
  const commands = client as {
    scan?: (...args: unknown[]) => Promise<{
      cursor: string | Buffer;
      keys: Array<string | Buffer>;
    }>;
    get?: (key: string) => Promise<unknown>;
    zRangeWithScores?: (...args: unknown[]) => Promise<unknown>;
  };
  return Object.assign(client, {
    withTypeMapping: vi.fn(() => ({
      scan: async (...args: unknown[]) => {
        if (!commands.scan)
          throw new Error("Redis fixture has no SCAN command.");
        const response = await commands.scan(...args);
        return {
          cursor: Buffer.from(response.cursor),
          keys: response.keys.map((key) => Buffer.from(key)),
        };
      },
      get: async (key: string) => {
        const value = await commands.get?.(key);
        return value == null
          ? null
          : Buffer.isBuffer(value)
            ? value
            : Buffer.from(String(value));
      },
      zRangeWithScores: (...args: unknown[]) =>
        commands.zRangeWithScores?.(...args),
    })),
  });
}
