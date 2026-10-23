import { randomUUID } from "node:crypto";
import { createClient } from "redis";
import { describe, expect, it } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";

const url = process.env.RAPIDB_REDIS_QUERY_AUDIT_URL;

describe.skipIf(!url)("Redis query input safety on a real server", () => {
  it("does not delete a replacement-character key through an invalid surrogate", async () => {
    const prefix = `rapidb-input-${randomUUID()}`;
    const key = `${prefix}:�`;
    const client = createClient({ url, socket: { reconnectStrategy: false } });
    const driver = new RedisDriver({
      id: prefix,
      name: prefix,
      type: "redis",
      connectionUri: url,
    });
    try {
      await client.connect();
      await driver.connect();
      await client.set(key, "keep");
      await expect(driver.query(`DEL "${prefix}:\\uD800"`)).rejects.toThrow(
        /Unicode/,
      );
      expect(await client.get(key)).toBe("keep");
      await expect(
        driver.query(`DEL ${JSON.stringify(key)}; SET target "\\uZZZZ"`),
      ).rejects.toThrow(/Unicode/);
      expect(await client.get(key)).toBe("keep");
      await driver.query(`DEL ${JSON.stringify(key)}`);
      expect(await client.get(key)).toBeNull();
    } finally {
      await driver.disconnect();
      if (client.isReady) await client.del(key);
      if (client.isOpen) client.destroy();
    }
  });
});
