import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { REDIS_READ_BUDGET } from "../../src/shared/safetyContracts";

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

  it("runs multiple preview-style Redis commands sequentially", async () => {
    const driver = new RedisDriver({
      id: "redis-query-preview-test",
      name: "Redis Query Preview Test",
      type: "redis",
      host: "localhost",
    });
    const sendCommand = vi
      .fn()
      .mockResolvedValueOnce(1)
      .mockResolvedValueOnce(2);

    (
      driver as unknown as {
        client: { sendCommand: typeof sendCommand } | null;
      }
    ).client = {
      sendCommand,
    };
    (driver as unknown as { connected: boolean }).connected = true;

    const result = await driver.query(
      'DEL "list:activity:recent"\n\nRPUSH "list:activity:recent" "{\\"id\\":\\"high-1\\",\\"type\\":\\"payment\\",\\"amount\\":999.99}" "{\\"id\\":\\"high-2\\",\\"type\\":\\"alert\\",\\"message\\":\\"System critical\\"}"',
    );

    expect(sendCommand).toHaveBeenNthCalledWith(1, [
      "DEL",
      "list:activity:recent",
    ]);
    expect(sendCommand).toHaveBeenNthCalledWith(2, [
      "RPUSH",
      "list:activity:recent",
      '{"id":"high-1","type":"payment","amount":999.99}',
      '{"id":"high-2","type":"alert","message":"System critical"}',
    ]);
    expect(result.rowCount).toBe(1);
    expect(result.columns).toEqual(["results"]);
    expect(result.rows[0]?.__col_0).toBe("[1,2]");
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
  it("uses bounded scan COUNT for small finite insert-type discovery limits", async () => {
    const driver = new RedisDriver({
      id: "redis-insert-infer-scan-limit",
      name: "Redis Insert Infer Scan Limit",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:1", "users:2"],
      }),
      type: vi.fn().mockResolvedValue("list"),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = client;
    (driver as unknown as { connected: boolean }).connected = true;

    await driver.buildMutationPreviewStatements(
      "insert",
      "db0",
      "db0",
      "users",
      {
        values: {
          key: "users:3",
          value: '["a","b"]',
        },
      },
    );

    expect(client.scan).toHaveBeenCalledWith("0", {
      MATCH: "users:*",
      COUNT: 25,
    });
  });

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
      { schema: "", name: "default", type: "table" },
      { schema: "", name: "orders", type: "table" },
      { schema: "", name: "users", type: "table" },
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

    const described = await driver.describeColumns("db0", "db0", "users");
    const page = await driver.readTablePage({
      database: "db0",
      schema: "db0",
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
          type: "string",
          nativeType: "string",
        }),
        expect.objectContaining({
          name: "value",
          category: "text",
          type: "mixed(string, hash, list, set, zset)",
          nativeType: "mixed(string, hash, list, set, zset)",
        }),
      ]),
    );
    expect(page.totalCount).toBe(5);
    expect(page.rows).toEqual([
      { key: "users:1", value: "Alice" },
      {
        key: "users:2",
        value: '{"first":"Bob","last":"Builder"}',
      },
      {
        key: "users:3",
        value: '["draft","published"]',
      },
      {
        key: "users:4",
        value: '["alpha","beta"]',
      },
      {
        key: "users:5",
        value: '[{"value":"gold","score":10},{"value":"silver","score":5}]',
      },
    ]);
  });

  it("uses the native Redis stream type for the value column", async () => {
    const driver = new RedisDriver({
      id: "redis-stream-test",
      name: "Redis Stream Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["events:1"],
      }),
      type: vi.fn().mockResolvedValue("stream"),
      xRange: vi.fn().mockResolvedValue([
        {
          id: "1716115200000-0",
          message: { event: "created", userId: "42" },
        },
      ]),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = client;
    (driver as unknown as { connected: boolean }).connected = true;

    const described = await driver.describeColumns("db0", "db0", "events");
    const page = await driver.readTablePage({
      database: "db0",
      schema: "db0",
      table: "events",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "key", direction: "asc" },
      skipCount: false,
    });

    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "value",
          type: "stream",
          nativeType: "stream",
        }),
      ]),
    );
    expect(page.rows).toEqual([
      {
        key: "events:1",
        value:
          '[{"id":"1716115200000-0","message":{"event":"created","userId":"42"}}]',
      },
    ]);
    expect(client.xRange).toHaveBeenCalledWith("events:1", "-", "+");
  });

  it("preserves Redis list type when updating a list-backed key", async () => {
    const driver = new RedisDriver({
      id: "redis-list-update-test",
      name: "Redis List Update Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      exists: vi.fn().mockResolvedValue(1),
      type: vi.fn().mockResolvedValue("list"),
      del: vi.fn().mockResolvedValue(1),
      rPush: vi.fn().mockResolvedValue(2),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = client;
    (driver as unknown as { connected: boolean }).connected = true;

    const listJson =
      '["{\\"id\\":\\"high-1\\",\\"type\\":\\"payment\\",\\"amount\\":999.99}","{\\"id\\":\\"high-2\\",\\"type\\":\\"alert\\",\\"message\\":\\"System critical\\"}"]';

    await expect(
      driver.buildMutationPreviewStatements("update", "db0", "db0", "list", {
        primaryKeys: { key: "list:activity:recent" },
        changes: { value: listJson },
      }),
    ).resolves.toEqual([
      'DEL "list:activity:recent"',
      'RPUSH "list:activity:recent" "{\\"id\\":\\"high-1\\",\\"type\\":\\"payment\\",\\"amount\\":999.99}" "{\\"id\\":\\"high-2\\",\\"type\\":\\"alert\\",\\"message\\":\\"System critical\\"}"',
    ]);

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "list",
        updates: [
          {
            primaryKeys: { key: "list:activity:recent" },
            changes: { value: listJson },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(client.del).toHaveBeenCalledWith("list:activity:recent");
    expect(client.rPush).toHaveBeenCalledWith("list:activity:recent", [
      '{"id":"high-1","type":"payment","amount":999.99}',
      '{"id":"high-2","type":"alert","message":"System critical"}',
    ]);
  });

  it("caps key scans when discovering Redis objects", async () => {
    const driver = new RedisDriver({
      id: "redis-object-cap-test",
      name: "Redis Object Cap Test",
      type: "redis",
      host: "localhost",
    });

    const keyBatch = Array.from(
      { length: 250 },
      (_value, index) => `ns:${index}`,
    );
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "1",
        keys: keyBatch,
      }),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = client;
    (driver as unknown as { connected: boolean }).connected = true;

    await driver.listObjects();

    const expectedCalls = Math.ceil(
      REDIS_READ_BUDGET.maxScanKeys / keyBatch.length,
    );
    expect(client.scan).toHaveBeenCalledTimes(expectedCalls);
  });
});
