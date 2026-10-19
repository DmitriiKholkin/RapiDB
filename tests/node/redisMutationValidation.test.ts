import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";

function fixture() {
  const driver = new RedisDriver({
    id: "redis-edit-validation",
    name: "Redis edit validation",
    type: "redis",
    host: "localhost",
  });
  const client = {
    type: vi.fn().mockResolvedValue("string"),
    sendCommand: vi.fn().mockResolvedValue(1),
  };
  Object.assign(driver, {
    client,
    connected: true,
    getDatabaseClient: async () => client,
  });
  return { driver, client };
}

describe("Redis mutation TTL validation", () => {
  it.each([
    Number.MAX_SAFE_INTEGER,
    Number.MAX_SAFE_INTEGER + 1,
    1e20,
    Number.POSITIVE_INFINITY,
    Number.NaN,
    9007199254741,
    "9007199254741",
    "9223372036854776",
  ])("rejects unsafe TTL %s in update execution and previews before writes", async (ttl) => {
    const { driver, client } = fixture();
    const edit = {
      primaryKeys: { key: "audit:source" },
      changes: { key: "audit:target", ttl },
    };
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: "audit",
        updates: [edit],
      }),
    ).rejects.toThrow("TTL");
    expect(client.sendCommand).not.toHaveBeenCalled();
    expect(() =>
      driver.buildMutationPreviewStatement("update", "db0", "", "audit", edit),
    ).toThrow("TTL");
    expect(() =>
      driver.buildMutationPreviewStatement("insert", "db0", "", "audit", {
        values: { key: "audit:new", ttl },
      }),
    ).toThrow("TTL");
  });

  it.each([
    9007199254740,
    "9007199254740",
  ])("accepts the largest supported TTL %s without rounding", async (ttl) => {
    const { driver, client } = fixture();
    await driver.updateRows({
      database: "db0",
      schema: "",
      table: "audit",
      updates: [{ primaryKeys: { key: "audit:source" }, changes: { ttl } }],
    });
    expect(client.sendCommand.mock.calls[0]?.[0].at(-2)).toBe("9007199254740");
    expect(
      driver.buildMutationPreviewStatement("update", "db0", "", "audit", {
        primaryKeys: { key: "audit:source" },
        changes: { ttl },
      }),
    ).toBe('EXPIRE "audit:source" "9007199254740"');
  });
});
