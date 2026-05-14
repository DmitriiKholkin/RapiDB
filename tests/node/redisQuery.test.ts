import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";

function createDriver() {
  const driver = new RedisDriver({
    id: "redis-test",
    name: "Redis Test",
    type: "redis",
    host: "localhost",
  });

  const sendCommand = vi.fn().mockResolvedValue("OK");

  (
    driver as unknown as { client: { sendCommand: typeof sendCommand } | null }
  ).client = {
    sendCommand,
  };
  (driver as unknown as { connected: boolean }).connected = true;

  return { driver, sendCommand };
}

describe("RedisDriver — query()", () => {
  it("preserves quoted arguments and escaped spaces when tokenizing commands", async () => {
    const { driver, sendCommand } = createDriver();

    const result = await driver.query('SET key "hello world"');

    expect(sendCommand).toHaveBeenCalledWith(["SET", "key", "hello world"]);
    expect(result.rowCount).toBe(1);
    expect(result.columns).toEqual(["result"]);
    expect(result.rows[0]?.__col_0).toBe("OK");
  });

  it("treats escaped whitespace as part of the same argument", async () => {
    const { driver, sendCommand } = createDriver();

    await driver.query("SET key hello\\ world");

    expect(sendCommand).toHaveBeenCalledWith(["SET", "key", "hello world"]);
  });

  it("rejects unterminated quoted arguments", async () => {
    const { driver } = createDriver();

    await expect(driver.query('SET key "hello world')).rejects.toThrow(
      /unterminated quoted argument/i,
    );
  });
});
