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
    readRows: ReturnType<typeof vi.fn>;
  };

  driverState.connected = true;
  driverState.readRows = vi.fn(async () => [
    {
      tenant_id: "tenant-1",
      user_id: "user-1",
      email: "person@example.com",
    },
  ]);
  driverState.client = {
    send: vi.fn(async (command: { input?: Record<string, unknown> }) => {
      if (command.input?.TableName) {
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
  });
});
