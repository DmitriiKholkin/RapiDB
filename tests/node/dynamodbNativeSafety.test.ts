import { randomUUID } from "node:crypto";
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} from "@aws-sdk/client-dynamodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { inferDynamoDbNativeOperationName } from "../../src/shared/dynamodbNative";

function fixture() {
  const driver = new DynamoDBDriver({
    id: "native-safety",
    name: "Native safety",
    type: "dynamodb",
    awsRegion: "us-east-1",
  });
  const send = vi.fn(async (_command: { input: unknown }) => ({}));
  Object.assign(driver, { client: { send }, connected: true });
  return { driver, send };
}

describe("DynamoDB native request safety", () => {
  it.each([
    { Expected: { version: { Value: { N: "1" } } } },
    { ReturnValues: "ALL_OLD" },
    {},
  ])("never dispatches a legacy update as a delete (%j)", async (options) => {
    const { driver, send } = fixture();
    const input = {
      TableName: "users",
      Key: { id: { S: "selected" } },
      AttributeUpdates: { name: { Action: "PUT", Value: { S: "new" } } },
      ...options,
    };
    expect(inferDynamoDbNativeOperationName(input)).toBe("UpdateItem");
    await driver.query(JSON.stringify(input));
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0]?.[0].constructor.name).toBe("UpdateItemCommand");
    expect(send.mock.calls[0]?.[0].input).toEqual(input);
    expect(
      driver.getCapabilities().readOnlyQueryGuard?.(JSON.stringify(input))
        .allowed,
    ).toBe(false);
  });

  it("preserves base64 binary keys even when they look like hex", async () => {
    const { driver, send } = fixture();
    await driver.query(
      JSON.stringify({
        TableName: "users",
        Key: { id: { B: "0xAB" } },
        ReturnValues: "ALL_OLD",
      }),
    );
    expect(send.mock.calls[0]?.[0].input).toMatchObject({
      Key: { id: { B: Buffer.from("0xAB", "base64") } },
    });
  });

  it.each([
    "!",
    "Zg",
    "Zh==",
    "0xdeadbeef",
    123,
    null,
  ])("rejects malformed native binary values before any script write (%j)", async (value) => {
    const { driver, send } = fixture();
    const valid = { TableName: "users", Item: { id: { S: "first" } } };
    const invalid = {
      TableName: "users",
      Item: { id: { S: "next" }, data: { B: value } },
    };
    await expect(
      driver.query(`${JSON.stringify(valid)}\n${JSON.stringify(invalid)}`),
    ).rejects.toThrow(/base64/i);
    expect(send).not.toHaveBeenCalled();
  });

  it("decodes binary sets and nested binary values without changing attribute names", async () => {
    const { driver, send } = fixture();
    await driver.query(
      JSON.stringify({
        TableName: "users",
        Item: {
          id: { S: "selected" },
          B: { M: { BS: { BS: ["", "0xAB"] }, L: { L: [{ B: "Zg==" }] } } },
        },
      }),
    );
    expect(send.mock.calls[0]?.[0].input).toMatchObject({
      Item: {
        B: {
          M: {
            BS: { BS: [Buffer.alloc(0), Buffer.from("0xAB", "base64")] },
            L: { L: [{ B: Buffer.from("f") }] },
          },
        },
      },
    });
  });

  it.each([
    { B: "Zg==", S: "different" },
    { B: "!", N: "1" },
    { BS: ["Zg==", 123] },
    { BS: "Zg==" },
    { N: 9007199254740992 },
    { M: [] },
    { L: {} },
    { SS: [1] },
  ])("rejects ambiguous or coercible AttributeValues before any write (%j)", async (value) => {
    const { driver, send } = fixture();
    const valid = { TableName: "users", Item: { id: { S: "first" } } };
    const invalid = {
      TableName: "users",
      Item: { id: { S: "next" }, data: value },
    };
    await expect(
      driver.query(`${JSON.stringify(valid)}\n${JSON.stringify(invalid)}`),
    ).rejects.toThrow(/AttributeValue|base64/i);
    expect(send).not.toHaveBeenCalled();
  });

  it.each([
    "B",
    "BS",
    "M",
    "L",
    "S",
    "N",
    "Item",
    "Key",
  ])("preserves an attribute named %s", async (name) => {
    const { driver, send } = fixture();
    await driver.query(
      JSON.stringify({ TableName: "users", Item: { [name]: { B: "0xAB" } } }),
    );
    expect(send.mock.calls[0]?.[0].input).toEqual({
      TableName: "users",
      Item: { [name]: { B: Buffer.from("0xAB", "base64") } },
    });
  });

  it.each([
    { RequestItems: { Item: { Keys: [{ id: { B: "0xAB" } }] } } },
    {
      RequestItems: {
        Item: [
          { PutRequest: { Item: { id: { B: "0xAB" } } } },
          { DeleteRequest: { Key: { id: { B: "0xAB" } } } },
        ],
      },
    },
    {
      TransactItems: [
        { Get: { TableName: "users", Key: { id: { B: "0xAB" } } } },
      ],
    },
    {
      TransactItems: [
        { Put: { TableName: "users", Item: { id: { B: "0xAB" } } } },
      ],
    },
    {
      TableName: "users",
      Key: { id: { S: "one" } },
      AttributeUpdates: { data: { Value: { B: "0xAB" }, Action: "PUT" } },
      Expected: { data: { Value: { B: "0xAB" } } },
    },
    {
      TableName: "users",
      FilterExpression: "#data = :data",
      ExpressionAttributeValues: { ":data": { B: "0xAB" } },
      ExclusiveStartKey: { id: { B: "0xAB" } },
    },
    {
      TableName: "users",
      ScanFilter: {
        data: { ComparisonOperator: "EQ", AttributeValueList: [{ B: "0xAB" }] },
      },
    },
  ])("preserves binary semantics in nested request structures (%j)", async (input) => {
    const { driver, send } = fixture();
    await driver.query(JSON.stringify(input));
    let binaries = 0;
    const jsonValue = (value: unknown): unknown => {
      if (Buffer.isBuffer(value)) {
        binaries++;
        return value.toString("base64");
      }
      if (Array.isArray(value)) return value.map(jsonValue);
      if (value && typeof value === "object")
        return Object.fromEntries(
          Object.entries(value).map(([key, entry]) => [key, jsonValue(entry)]),
        );
      return value;
    };
    expect(jsonValue(send.mock.calls[0]?.[0].input)).toEqual(input);
    expect(binaries).toBeGreaterThan(0);
  });
});

const liveIt = process.env.RAPIDB_LIVE_NOSQL === "1" ? it : it.skip;

liveIt(
  "updates and deletes only the intended binary key against DynamoDB Local",
  async () => {
    const config = {
      id: "native-safety-live",
      name: "Native safety live",
      type: "dynamodb" as const,
      endpoint: "http://127.0.0.1:8000",
      awsRegion: "us-east-1",
      awsAccessKeyId: "rapidb",
      awsSecretAccessKey: "rapidb-secret",
    };
    const admin = new DynamoDBClient({
      endpoint: config.endpoint,
      region: config.awsRegion,
      credentials: {
        accessKeyId: config.awsAccessKeyId,
        secretAccessKey: config.awsSecretAccessKey,
      },
      maxAttempts: 1,
    });
    const driver = new DynamoDBDriver(config);
    const table = `native_safety_${randomUUID().replaceAll("-", "")}`;
    const intended = Buffer.from("0xAB", "base64");
    const wrong = Buffer.from("ab", "hex");
    const read = async (key: Buffer) =>
      (
        await admin.send(
          new GetItemCommand({
            TableName: table,
            Key: { id: { B: key } },
            ConsistentRead: true,
          }),
        )
      ).Item;
    try {
      await admin.send(
        new CreateTableCommand({
          TableName: table,
          KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
          AttributeDefinitions: [{ AttributeName: "id", AttributeType: "B" }],
          BillingMode: "PAY_PER_REQUEST",
        }),
      );
      for (const key of [intended, wrong]) {
        await admin.send(
          new PutItemCommand({
            TableName: table,
            Item: { id: { B: key }, name: { S: "old" } },
          }),
        );
      }
      await driver.connect();
      await driver.query(
        JSON.stringify({
          TableName: table,
          Key: { id: { B: "0xAB" } },
          AttributeUpdates: { name: { Action: "PUT", Value: { S: "new" } } },
          Expected: { name: { Value: { S: "old" } } },
          ReturnValues: "ALL_OLD",
        }),
      );
      expect((await read(intended))?.name).toEqual({ S: "new" });
      expect((await read(wrong))?.name).toEqual({ S: "old" });
      await driver.query(
        JSON.stringify({
          TableName: table,
          Key: { id: { B: "0xAB" } },
          ReturnValues: "ALL_OLD",
        }),
      );
      expect(await read(intended)).toBeUndefined();
      expect((await read(wrong))?.name).toEqual({ S: "old" });

      const insert = (binary: string) =>
        JSON.stringify({ TableName: table, Item: { id: { B: binary } } });
      await expect(
        driver.query(`${insert("Yw==")}\n${insert("!")}`),
      ).rejects.toThrow(/base64/i);
      expect(await read(Buffer.from("c"))).toBeUndefined();
    } finally {
      await driver.disconnect();
      await admin
        .send(new DeleteTableCommand({ TableName: table }))
        .catch(() => undefined);
      admin.destroy();
    }
  },
  30_000,
);
