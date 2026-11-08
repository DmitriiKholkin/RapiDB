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
  for (const command of ["strLen", "hLen", "lLen", "sCard", "zCard", "xLen"]) {
    if (!(command in client))
      Object.assign(client, { [command]: vi.fn(async () => 0) });
  }
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
      getRange: async (key: string, start: number, end: number) => {
        const value = await commands.get?.(key);
        return value == null
          ? null
          : (Buffer.isBuffer(value)
              ? value
              : Buffer.from(String(value))
            ).subarray(start, end + 1);
      },
      zRangeWithScores: (...args: unknown[]) =>
        commands.zRangeWithScores?.(...args),
      ...Object.fromEntries(
        ["hGetAll", "lRange", "sMembers", "xRange", "sendCommand"]
          .filter((command) => command in client)
          .map((command) => [
            command,
            (...args: unknown[]) =>
              (client as Record<string, (...args: unknown[]) => unknown>)[
                command
              ](...args),
          ]),
      ),
    })),
  });
}
