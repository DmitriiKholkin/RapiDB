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

describe("RedisDriver — disconnect()", () => {
  it("closes the active client and clears connection state", async () => {
    const driver = new RedisDriver({
      id: "redis-disconnect-test",
      name: "Redis Disconnect Test",
      type: "redis",
      host: "localhost",
    });
    const close = vi.fn().mockResolvedValue(undefined);
    const driverState = driver as unknown as {
      client: { close: typeof close; isOpen: boolean } | null;
      connected: boolean;
    };

    driverState.client = { close, isOpen: true };
    driverState.connected = true;

    await driver.disconnect();

    expect(close).toHaveBeenCalledTimes(1);
    expect(driverState.client).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });

  it("skips close when the client socket is already closed", async () => {
    const driver = new RedisDriver({
      id: "redis-disconnect-closed-client",
      name: "Redis Disconnect Closed Client",
      type: "redis",
      host: "localhost",
    });
    const close = vi.fn().mockResolvedValue(undefined);
    const driverState = driver as unknown as {
      client: { close: typeof close; isOpen: boolean } | null;
      connected: boolean;
    };

    driverState.client = { close, isOpen: false };
    driverState.connected = true;

    await driver.disconnect();

    expect(close).not.toHaveBeenCalled();
    expect(driverState.client).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });

  it("remains idempotent when no client is present", async () => {
    const driver = new RedisDriver({
      id: "redis-disconnect-empty",
      name: "Redis Disconnect Empty",
      type: "redis",
      host: "localhost",
    });

    await expect(driver.disconnect()).resolves.toBeUndefined();
    expect(driver.isConnected()).toBe(false);
  });
});

describe("RedisDriver — metadata and pages", () => {
  it("lists logical databases and groups keys into table-like prefixes", async () => {
    const driver = new RedisDriver({
      id: "redis-metadata-test",
      name: "Redis Metadata Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      info: vi
        .fn()
        .mockResolvedValue(
          "# Keyspace\ndb0:keys=2,expires=0,avg_ttl=0\ndb4:keys=7,expires=0,avg_ttl=0\n",
        ),
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:1", "users:2", "orders:1", "orphan"],
      }),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = client;
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(driver.listDatabases()).resolves.toEqual([
      { name: "db0", schemas: [] },
      { name: "db4", schemas: [] },
    ]);
    await expect(driver.listObjects()).resolves.toEqual([
      { schema: "default", name: "default", type: "table" },
      { schema: "default", name: "orders", type: "table" },
      { schema: "default", name: "users", type: "table" },
    ]);
    expect(client.scan).toHaveBeenCalledWith("0", {
      MATCH: "*",
      COUNT: 500,
    });
  });

  it("reads mixed Redis value types into stable flattened page rows and inferred columns", async () => {
    const driver = new RedisDriver({
      id: "redis-page-test",
      name: "Redis Page Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:5", "users:3", "users:4", "users:2", "users:1"],
      }),
      type: vi.fn(async (key: string) => {
        if (key === "users:1") {
          return "string";
        }
        if (key === "users:2") {
          return "hash";
        }
        if (key === "users:3") {
          return "list";
        }
        if (key === "users:4") {
          return "set";
        }
        if (key === "users:5") {
          return "zset";
        }
        return "none";
      }),
      get: vi.fn(async (key: string) => (key === "users:1" ? "Alice" : null)),
      hGetAll: vi.fn(async (key: string) =>
        key === "users:2" ? { first: "Bob", last: "Builder" } : {},
      ),
      lRange: vi.fn(async (key: string) =>
        key === "users:3" ? ["draft", "published"] : [],
      ),
      sMembers: vi.fn(async (key: string) =>
        key === "users:4" ? ["alpha", "beta"] : [],
      ),
      zRangeWithScores: vi.fn(async (key: string) =>
        key === "users:5"
          ? [
              { value: "gold", score: 10 },
              { value: "silver", score: 5 },
            ]
          : [],
      ),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = client;
    (driver as unknown as { connected: boolean }).connected = true;

    const described = await driver.describeColumns("db0", "default", "users");
    const page = await driver.readTablePage({
      database: "db0",
      schema: "default",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "key", direction: "asc" },
      skipCount: false,
    });

    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "key",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          category: "text",
        }),
        expect.objectContaining({
          name: "type",
          category: "text",
        }),
        expect.objectContaining({
          name: "value",
          category: "text",
        }),
      ]),
    );
    expect(page.totalCount).toBe(5);
    expect(page.rows).toEqual([
      { key: "users:1", type: "string", value: "Alice" },
      {
        key: "users:2",
        type: "hash",
        value: '{"first":"Bob","last":"Builder"}',
      },
      {
        key: "users:3",
        type: "list",
        value: '["draft","published"]',
      },
      {
        key: "users:4",
        type: "set",
        value: '["alpha","beta"]',
      },
      {
        key: "users:5",
        type: "zset",
        value: '[{"value":"gold","score":10},{"value":"silver","score":5}]',
      },
    ]);
  });
});
