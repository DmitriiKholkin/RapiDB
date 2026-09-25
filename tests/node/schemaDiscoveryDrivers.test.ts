import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { loadDatabaseScope } from "../../src/extension/schema/schemaLoaders";

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
    const driver = withClient(
      new RedisDriver({
        id: "redis",
        name: "Redis",
        type: "redis",
        host: "localhost",
      }),
      { scan },
    );
    await expect(driver.listObjects()).rejects.toBe(denied);
    const failed = await loadDatabaseScope(driver, "db0", "baseline");
    expect(failed.loadedSchemas).toEqual([]);
    expect(failed.failedSchemas).toEqual([{ name: "db0", error: denied }]);
    scan.mockResolvedValue({ cursor: "0", keys: [] });
    const recovered = await loadDatabaseScope(driver, "db0", "baseline");
    expect(recovered.failedSchemas).toEqual([]);
    expect(recovered.loadedSchemas[0]).toMatchObject({
      name: "db0",
      objects: [{ name: "default", type: "table" }],
    });
    vi.spyOn(driver, "listSchemas").mockRejectedValue(denied);
    await expect(loadDatabaseScope(driver, "db0", "baseline")).rejects.toBe(
      denied,
    );
  });
});
