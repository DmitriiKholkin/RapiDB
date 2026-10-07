import net from "node:net";
import { createClient } from "redis";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { REDIS_ALL_KEYS_TABLE } from "../../src/extension/dbDrivers/redisKeyspace";
import { loadDatabaseScope } from "../../src/extension/schema/schemaLoaders";

vi.mock("redis", async (importOriginal) => {
  const actual = await importOriginal<typeof import("redis")>();
  return { ...actual, createClient: vi.fn() };
});

function withClient<T>(driver: T, client: unknown): T {
  Object.assign(driver as object, { client, connected: true });
  return driver;
}

describe("driver schema discovery error boundaries", () => {
  it("propagates MongoDB collection and sampling failures instead of reporting an empty schema", async () => {
    const denied = new Error("not authorized on app");
    const collections = vi.fn().mockRejectedValue(denied);
    const documents = vi.fn().mockRejectedValue(denied);
    const driver = withClient(
      new MongoDBDriver({
        id: "mongo",
        name: "Mongo",
        type: "mongodb",
        uri: "mongodb://localhost/app",
      }),
      {
        db: () => ({
          listCollections: () => ({ toArray: collections }),
          collection: () => ({
            find: () => ({ limit: () => ({ toArray: documents }) }),
          }),
        }),
      },
    );
    await expect(driver.listObjects("app")).rejects.toBe(denied);
    await expect(driver.describeTable("app", "app", "users")).rejects.toBe(
      denied,
    );
    collections.mockResolvedValue([]);
    await expect(driver.listObjects("app")).resolves.toEqual([]);
    collections.mockResolvedValue([{ name: "users", type: "collection" }]);
    await expect(driver.listObjects("app")).resolves.toEqual([
      { name: "users", schema: "app", type: "table" },
    ]);
  });

  it("propagates Elasticsearch catalog rejection and distinguishes empty recovery", async () => {
    const denied = new Error("security_exception: forbidden");
    const resolveIndex = vi.fn().mockRejectedValue(denied);
    const driver = withClient(
      new ElasticsearchDriver({
        id: "es",
        name: "Search",
        type: "elasticsearch",
        host: "localhost",
      }),
      { indices: { resolveIndex } },
    );
    await expect(driver.listObjects()).rejects.toBe(denied);
    resolveIndex.mockResolvedValue({
      indices: [],
      data_streams: [],
      aliases: [],
    });
    await expect(driver.listObjects()).resolves.toEqual([]);
  });

  it("propagates DynamoDB ListTables rejection and distinguishes empty recovery", async () => {
    const denied = new Error("AccessDeniedException: ListTables");
    const send = vi.fn().mockRejectedValue(denied);
    const driver = withClient(
      new DynamoDBDriver({
        id: "ddb",
        name: "Dynamo",
        type: "dynamodb",
        awsRegion: "us-east-1",
      }),
      { send },
    );
    await expect(driver.listObjects("us-east-1")).rejects.toBe(denied);
    send.mockResolvedValue({ TableNames: [] });
    await expect(driver.listObjects("us-east-1")).resolves.toEqual([]);
  });

  it("preserves Redis's no-schema fallback while surfacing SCAN permission errors", async () => {
    const denied = new Error("NOPERM: SCAN");
    const scan = vi.fn().mockRejectedValue(denied);
    const makeClient = (scanMock: typeof scan) => {
      const client = {
        isOpen: false,
        isReady: false,
        on: vi.fn(),
        connect: vi.fn<() => Promise<void>>(),
        destroy: vi.fn<() => void>(),
        scan: scanMock,
      };
      client.connect.mockImplementation(async () => {
        client.isOpen = true;
        client.isReady = true;
      });
      client.destroy.mockImplementation(() => {
        client.isOpen = false;
        client.isReady = false;
      });
      return client;
    };
    const base = makeClient(vi.fn());
    const scoped = makeClient(scan);
    // Exercise production scoping through the SDK boundary, with distinct
    // editor and native clients even when both select DB 0.
    vi.mocked(createClient)
      .mockReturnValueOnce(base as unknown as ReturnType<typeof createClient>)
      .mockReturnValueOnce(
        scoped as unknown as ReturnType<typeof createClient>,
      );
    const tcp = vi.spyOn(net, "createConnection").mockImplementation(() => {
      throw new Error("Redis discovery fixture attempted real TCP");
    });
    const driver = new RedisDriver({
      id: "redis",
      name: "Redis",
      type: "redis",
      host: "localhost",
    });
    try {
      await driver.connect();
      await expect(driver.listObjects()).rejects.toBe(denied);
      const failed = await loadDatabaseScope(driver, "db0", "baseline");
      expect(failed.loadedSchemas).toEqual([]);
      expect(failed.failedSchemas).toEqual([{ name: "db0", error: denied }]);
      scan.mockResolvedValue({ cursor: "0", keys: [] });
      const recovered = await loadDatabaseScope(driver, "db0", "baseline");
      expect(recovered.failedSchemas).toEqual([]);
      expect(recovered.loadedSchemas[0]).toMatchObject({
        name: "db0",
        objects: [{ name: REDIS_ALL_KEYS_TABLE, type: "table" }],
      });
      vi.spyOn(driver, "listSchemas").mockRejectedValue(denied);
      await expect(loadDatabaseScope(driver, "db0", "baseline")).rejects.toBe(
        denied,
      );
      expect(base.scan).not.toHaveBeenCalled();
      expect(scoped.connect).toHaveBeenCalledTimes(1);
      expect(createClient).toHaveBeenCalledTimes(2);
      for (const [options] of vi.mocked(createClient).mock.calls) {
        expect(options).toMatchObject({ database: 0 });
      }
    } finally {
      await driver.disconnect();
      expect(driver.isConnected()).toBe(false);
      for (const client of [base, scoped]) {
        expect(client.destroy).toHaveBeenCalledTimes(1);
        expect(client.isOpen).toBe(false);
        expect(client.isReady).toBe(false);
      }
      expect(
        (driver as unknown as { clients: Set<unknown> }).clients.size,
      ).toBe(0);
      expect(tcp).not.toHaveBeenCalled();
    }
  });
});
