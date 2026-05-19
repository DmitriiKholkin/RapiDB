import { marshall } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const config: ConnectionConfig = {
  id: "conn-ddb",
  name: "Dynamo",
  type: "dynamodb",
  awsRegion: "us-east-1",
};

function createDriver() {
  const driver = new DynamoDBDriver(config);
  const driverState = driver as unknown as {
    client: { send: ReturnType<typeof vi.fn> } | null;
    connected: boolean;
  };

  driverState.connected = true;
  driverState.client = {
    send: vi.fn(async (command: { input?: Record<string, unknown> }) => {
      if (command.constructor.name === "DescribeTableCommand") {
        return {
          Table: {
            KeySchema: [
              { AttributeName: "tenant_id", KeyType: "HASH" },
              { AttributeName: "user_id", KeyType: "RANGE" },
            ],
            AttributeDefinitions: [
              { AttributeName: "tenant_id", AttributeType: "S" },
              { AttributeName: "user_id", AttributeType: "S" },
            ],
          },
        };
      }

      if (command.constructor.name === "ScanCommand") {
        return command.input?.Select === "COUNT"
          ? { Count: 1 }
          : {
              Items: [
                marshall({
                  tenant_id: "tenant-1",
                  user_id: "user-1",
                  email: "person@example.com",
                  age: 31,
                  active: true,
                  profile: { tier: "pro", visits: 3 },
                  tags: new Set(["alpha", "beta"]),
                  scores: new Set([1, 2.5]),
                  history: [1, "two", true],
                  payload: Uint8Array.from([0xde, 0xad, 0xbe, 0xef]),
                  deleted_at: null,
                }),
              ],
            };
      }

      return {
        TableNames: ["users", "orders"],
      };
    }),
  };

  return driver;
}

describe("DynamoDBDriver metadata", () => {
  it("uses a single logical database and collapses the schema name to match it", async () => {
    const driver = createDriver();

    await expect(driver.listDatabases()).resolves.toEqual([
      {
        name: "us-east-1",
        schemas: [],
      },
    ]);
    await expect(driver.listSchemas("us-east-1")).resolves.toEqual([
      { name: "us-east-1" },
    ]);
    await expect(driver.listObjects("us-east-1")).resolves.toEqual([
      { schema: "us-east-1", name: "orders", type: "table" },
      { schema: "us-east-1", name: "users", type: "table" },
    ]);
  });

  it("distinguishes partition and sort keys in described and paged columns", async () => {
    const driver = createDriver();

    const described = await driver.describeColumns(
      "us-east-1",
      "us-east-1",
      "users",
    );

    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "tenant_id",
          type: "string",
          nativeType: "string",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          primaryKeyRole: "partition",
        }),
        expect.objectContaining({
          name: "user_id",
          type: "string",
          nativeType: "string",
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          primaryKeyRole: "sort",
        }),
        expect.objectContaining({
          name: "email",
          type: "string",
          nativeType: "string",
          category: "text",
        }),
        expect.objectContaining({
          name: "age",
          type: "number",
          nativeType: "number",
          category: "integer",
        }),
        expect.objectContaining({
          name: "active",
          type: "boolean",
          nativeType: "boolean",
          category: "boolean",
        }),
        expect.objectContaining({
          name: "profile",
          type: "map",
          nativeType: "map",
          category: "json",
        }),
        expect.objectContaining({
          name: "history",
          type: "list",
          nativeType: "list",
          category: "array",
        }),
        expect.objectContaining({
          name: "tags",
          type: "string set",
          nativeType: "string set",
          category: "array",
        }),
        expect.objectContaining({
          name: "scores",
          type: "number set",
          nativeType: "number set",
          category: "array",
        }),
        expect.objectContaining({
          name: "payload",
          type: "binary",
          nativeType: "binary",
          category: "binary",
        }),
        expect.objectContaining({
          name: "deleted_at",
          type: "null",
          nativeType: "null",
          category: "other",
        }),
      ]),
    );
    expect(described.slice(0, 2).map((column) => column.name)).toEqual([
      "tenant_id",
      "user_id",
    ]);

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 25,
      filters: [],
      sort: null,
      skipCount: false,
    });

    expect(page.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "tenant_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          primaryKeyRole: "partition",
        }),
        expect.objectContaining({
          name: "user_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          primaryKeyRole: "sort",
        }),
      ]),
    );
    expect(page.columns.slice(0, 2).map((column) => column.name)).toEqual([
      "tenant_id",
      "user_id",
    ]);
    expect(page.rows).toEqual([
      expect.objectContaining({
        tenant_id: "tenant-1",
        user_id: "user-1",
        email: "person@example.com",
        age: 31,
        active: true,
        profile: '{"tier":"pro","visits":3}',
        history: '[1,"two",true]',
        tags: "<<'alpha', 'beta'>>",
        scores: "<<1, 2.5>>",
        payload: "0xdeadbeef",
        deleted_at: null,
      }),
    ]);
  });

  it("destroys the client and clears driver state on disconnect", async () => {
    const driver = new DynamoDBDriver(config);
    const destroy = vi.fn();
    const driverState = driver as unknown as {
      client: { destroy: typeof destroy } | null;
      connected: boolean;
    };

    driverState.client = { destroy };
    driverState.connected = true;

    await driver.disconnect();

    expect(destroy).toHaveBeenCalledTimes(1);
    expect(driverState.client).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });
});
