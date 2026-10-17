import { vi } from "vitest";

/** Model the Buffer/String reply mapping used by the actual node-redis client. */
export function withRedisReplyMapping<T extends object>(client: T) {
  const commands = client as {
    get?: (key: string) => Promise<unknown>;
    zRangeWithScores?: (...args: unknown[]) => Promise<unknown>;
  };
  return Object.assign(client, {
    withTypeMapping: vi.fn(() => ({
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
