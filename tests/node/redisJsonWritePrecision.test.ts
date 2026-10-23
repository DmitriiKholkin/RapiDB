import { randomUUID } from "node:crypto";
import { createClient } from "redis";
import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";

function harness(kind: string) {
  const driver = new RedisDriver({
    id: "precision",
    name: "Precision",
    type: "redis",
  });
  const client = {
    type: vi.fn().mockResolvedValue(kind),
    sendCommand: vi.fn().mockResolvedValue(1),
  };
  Object.assign(driver, { getDatabaseClient: async () => client });
  return { driver, client };
}

describe("Redis structured edit numeric precision", () => {
  it.each([
    ["hash", '{"account":9007199254740993}'],
    ["list", "[9007199254740993]"],
    ["set", "[0.10000000000000001]"],
    ["zset", '[{"value":9007199254740993,"score":1}]'],
  ])("rejects inaccurate %s edits before submitting a replacement", async (kind, value) => {
    const { driver, client } = harness(kind);
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: "audit",
        updates: [{ primaryKeys: { key: "audit:source" }, changes: { value } }],
      }),
    ).rejects.toThrow(/numeric precision/);
    expect(client.sendCommand).not.toHaveBeenCalled();
    await expect(
      driver.buildMutationPreviewStatements("update", "db0", "", "audit", {
        primaryKeys: { key: "audit:source" },
        changes: { value },
      }),
    ).rejects.toThrow(/numeric precision/);
  });

  it.each([
    [
      "hash",
      '{"account":"9007199254740993"}',
      '{"account":"9007199254740993"}',
    ],
    ["list", '["9007199254740993",42]', '["9007199254740993","42"]'],
    ["set", '["0.10000000000000001"]', '["0.10000000000000001"]'],
    [
      "zset",
      '[{"value":"9007199254740993","score":0.1}]',
      '[{"score":0.1,"value":"9007199254740993"}]',
    ],
  ])("preserves exact strings and ordinary numeric input in %s", async (kind, value, expected) => {
    const { driver, client } = harness(kind);
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: "audit",
        updates: [{ primaryKeys: { key: "audit:source" }, changes: { value } }],
      }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.sendCommand).toHaveBeenCalledOnce();
    expect(client.sendCommand.mock.calls[0][0][9]).toBe(expected);
  });

  it("allows repairing a displayed zset score without revalidating the original as a new write", async () => {
    const { driver, client } = harness("zset");
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: "audit",
        updates: [
          {
            primaryKeys: { key: "audit:source" },
            changes: { value: '[{"value":"member","score":1}]' },
            originalValues: {
              value: '[{"value":"member","score":1000000000000000100}]',
            },
          },
        ],
      }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.sendCommand.mock.calls[0][0][7]).toBe(
      '[{"score":1000000000000000100,"value":"member"}]',
    );
  });
});

it.skipIf(!process.env.RAPIDB_REDIS_QUERY_AUDIT_URL)(
  "keeps a real Redis hash intact on inaccurate input and accepts exact quoted input",
  async () => {
    const url = process.env.RAPIDB_REDIS_QUERY_AUDIT_URL;
    const key = `rapidb-precision:${randomUUID()}`;
    const client = createClient({ url, socket: { reconnectStrategy: false } });
    const driver = new RedisDriver({
      id: key,
      name: key,
      type: "redis",
      connectionUri: url,
    });
    try {
      await client.connect();
      await driver.connect();
      await client.hSet(key, { account: "keep" });
      const update = (value: string) =>
        driver.updateRows({
          database: "db0",
          schema: "",
          table: "audit",
          updates: [
            {
              primaryKeys: { key },
              changes: { value },
              originalValues: { value: '{"account":"keep"}' },
            },
          ],
        });
      await expect(update('{"account":9007199254740993}')).rejects.toThrow(
        /numeric precision/,
      );
      expect(await client.hGetAll(key)).toEqual({ account: "keep" });
      await expect(
        update('{"account":"9007199254740993"}'),
      ).resolves.toMatchObject({ affectedRows: 1 });
      expect(await client.hGetAll(key)).toEqual({
        account: "9007199254740993",
      });
    } finally {
      await driver.disconnect();
      if (client.isReady) await client.del(key);
      if (client.isOpen) client.destroy();
    }
  },
);
