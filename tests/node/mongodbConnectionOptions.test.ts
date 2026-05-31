import { beforeEach, describe, expect, it, vi } from "vitest";

const mongoClientMocks = vi.hoisted(() => ({
  connect: vi.fn().mockResolvedValue(undefined),
  close: vi.fn().mockResolvedValue(undefined),
  db: vi.fn(),
  constructorCalls: [] as Array<{
    uri: string;
    options: Record<string, unknown>;
  }>,
}));

vi.mock("mongodb", async (importOriginal) => {
  const actual = await importOriginal<typeof import("mongodb")>();

  class MockMongoClient {
    constructor(uri: string, options: Record<string, unknown> = {}) {
      mongoClientMocks.constructorCalls.push({ uri, options });
    }

    connect = mongoClientMocks.connect;
    close = mongoClientMocks.close;
    db = mongoClientMocks.db;
  }

  return {
    ...actual,
    MongoClient: MockMongoClient,
  };
});

import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

describe("MongoDBDriver — connect()", () => {
  beforeEach(() => {
    mongoClientMocks.constructorCalls.length = 0;
    mongoClientMocks.connect.mockClear().mockResolvedValue(undefined);
    mongoClientMocks.close.mockClear().mockResolvedValue(undefined);
    mongoClientMocks.db.mockClear();
  });

  it("builds a URI with the selected database and authSource", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-connect-auth-database",
      name: "Mongo Connect Auth Database",
      type: "mongodb",
      host: "mongo.internal",
      port: 27018,
      username: "app user",
      password: "p@ss word",
      database: "appdb",
      authSource: "admin-auth",
    });

    await driver.connect();

    expect(mongoClientMocks.connect).toHaveBeenCalledTimes(1);
    expect(driver.isConnected()).toBe(true);
    expect(mongoClientMocks.constructorCalls).toEqual([
      expect.objectContaining({
        uri: "mongodb://app%20user:p%40ss%20word@mongo.internal:27018/appdb?authSource=admin-auth",
        options: expect.objectContaining({
          authSource: "admin-auth",
        }),
      }),
    ]);
  });

  it("defaults the database name to admin while preserving authSource in the URI and options", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-connect-default-database",
      name: "Mongo Connect Default Database",
      type: "mongodb",
      host: "localhost",
      authSource: "admin",
    });

    await driver.connect();

    expect(mongoClientMocks.constructorCalls).toEqual([
      expect.objectContaining({
        uri: "mongodb://localhost:27017/admin?authSource=admin",
        options: expect.objectContaining({
          authSource: "admin",
        }),
      }),
    ]);
  });

  it("uses an explicit connectionUri unchanged while still forwarding authSource options", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-connect-explicit-uri",
      name: "Mongo Connect Explicit URI",
      type: "mongodb",
      connectionUri:
        "mongodb://cluster.example:27017/customdb?retryWrites=true",
      database: "ignored-db",
      authSource: "admin",
    });

    await driver.connect();

    expect(mongoClientMocks.constructorCalls).toEqual([
      expect.objectContaining({
        uri: "mongodb://cluster.example:27017/customdb?retryWrites=true",
        options: expect.objectContaining({
          authSource: "admin",
        }),
      }),
    ]);
  });

  it("uses legacy uri when connectionUri is absent and still applies authSource", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-connect-legacy-uri",
      name: "Mongo Connect Legacy URI",
      type: "mongodb",
      uri: "mongodb://legacy.example:27017/legacydb",
      authSource: "admin-auth",
    });

    await driver.connect();

    expect(mongoClientMocks.constructorCalls).toEqual([
      expect.objectContaining({
        uri: "mongodb://legacy.example:27017/legacydb",
        options: expect.objectContaining({
          authSource: "admin-auth",
        }),
      }),
    ]);
  });
});
