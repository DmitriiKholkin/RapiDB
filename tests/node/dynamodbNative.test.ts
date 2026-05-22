import { marshall } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import type {
  ColumnTypeMeta,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import {
  parseDynamoDbNativeQueryInput,
  parseDynamoDbNativeQueryInputs,
} from "../../src/shared/dynamodbNative";

const config: ConnectionConfig = {
  id: "conn-ddb",
  name: "Dynamo",
  type: "dynamodb",
  awsRegion: "us-east-1",
};

function createColumns(): ColumnTypeMeta[] {
  return [
    {
      name: "tenant_id",
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      primaryKeyRole: "partition",
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "like", "in"],
      valueSemantics: "plain",
    },
    {
      name: "user_id",
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 2,
      primaryKeyRole: "sort",
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "like", "in", "gte", "lte"],
      valueSemantics: "plain",
    },
    {
      name: "email",
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "like", "in", "is_null", "is_not_null"],
      valueSemantics: "plain",
    },
    {
      name: "age",
      type: "number",
      nativeType: "number",
      category: "float",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: [
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
        "in",
        "is_null",
        "is_not_null",
      ],
      valueSemantics: "plain",
    },
  ];
}

function createMapColumn(): ColumnTypeMeta {
  return {
    name: "address",
    type: "map",
    nativeType: "map",
    category: "json",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

type CreateDriverOptions = {
  columns?: ColumnTypeMeta[];
  describeTable?: {
    KeySchema: Array<{
      AttributeName: string;
      KeyType: "HASH" | "RANGE";
    }>;
    AttributeDefinitions: Array<{
      AttributeName: string;
      AttributeType: "S" | "N" | "B";
    }>;
    GlobalSecondaryIndexes?: Array<{
      IndexName: string;
      KeySchema: Array<{
        AttributeName: string;
        KeyType: "HASH" | "RANGE";
      }>;
      Projection: { ProjectionType: "ALL" };
    }>;
  };
};

function createDriver(options: CreateDriverOptions = {}) {
  const columns = options.columns ?? createColumns();
  const describeTable = options.describeTable ?? {
    KeySchema: [
      { AttributeName: "tenant_id", KeyType: "HASH" },
      { AttributeName: "user_id", KeyType: "RANGE" },
    ],
    AttributeDefinitions: [
      { AttributeName: "tenant_id", AttributeType: "S" },
      { AttributeName: "user_id", AttributeType: "S" },
      { AttributeName: "email", AttributeType: "S" },
    ],
    GlobalSecondaryIndexes: [
      {
        IndexName: "email-index",
        KeySchema: [
          { AttributeName: "email", KeyType: "HASH" },
          { AttributeName: "user_id", KeyType: "RANGE" },
        ],
        Projection: { ProjectionType: "ALL" },
      },
    ],
  };

  const queuedResponses: unknown[] = [];
  const driver = new DynamoDBDriver(config);
  const clientSend = vi.fn(
    async (command: {
      constructor: { name: string };
      input?: Record<string, unknown>;
    }) => {
      if (command.constructor.name === "ListTablesCommand") {
        return { TableNames: ["users"] };
      }
      if (command.constructor.name === "DescribeTableCommand") {
        return { Table: describeTable };
      }
      return queuedResponses.shift() ?? {};
    },
  );

  const driverState = driver as unknown as {
    client: { send: typeof clientSend } | null;
    connected: boolean;
    describeColumns: ReturnType<typeof vi.fn>;
  };
  driverState.connected = true;
  driverState.client = { send: clientSend };
  driverState.describeColumns = vi.fn(async () => columns);

  return {
    driver,
    clientSend,
    queueResponses: (...responses: unknown[]) => {
      queuedResponses.push(...responses);
    },
  };
}

function toJsonSafeValue(value: unknown): unknown {
  if (Buffer.isBuffer(value)) {
    return value.toString("base64");
  }
  if (Array.isArray(value)) {
    return value.map((entry) => toJsonSafeValue(entry));
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        toJsonSafeValue(entry),
      ]),
    );
  }
  return value;
}

function commandInputs(
  clientSend: ReturnType<typeof vi.fn>,
  commandName: string,
): Array<Record<string, unknown>> {
  return clientSend.mock.calls
    .map(([command]) => command)
    .filter((command) => command.constructor.name === commandName)
    .map((command) => command.input as Record<string, unknown>);
}

describe("parseDynamoDbNativeQueryInput", () => {
  it("parses a single raw AWS request body", () => {
    expect(
      parseDynamoDbNativeQueryInput(
        JSON.stringify({ TableName: "users", ConsistentRead: true }),
      ),
    ).toEqual({ TableName: "users", ConsistentRead: true });
  });

  it("parses multiple consecutive raw AWS request bodies", () => {
    expect(
      parseDynamoDbNativeQueryInputs(
        [
          JSON.stringify({ TableName: "users", Limit: 1 }, null, 2),
          JSON.stringify({ TableName: "users", Limit: 2 }, null, 2),
        ].join("\n\n"),
      ),
    ).toEqual([
      { TableName: "users", Limit: 1 },
      { TableName: "users", Limit: 2 },
    ]);
  });

  it("rejects empty, malformed, and legacy envelope payloads", () => {
    expect(() => parseDynamoDbNativeQueryInput("   ")).toThrow(
      /cannot be empty/i,
    );
    expect(() => parseDynamoDbNativeQueryInput("{")).toThrow(
      /must be valid JSON/i,
    );
    expect(() =>
      parseDynamoDbNativeQueryInput(
        JSON.stringify({ operation: "Scan", input: { TableName: "users" } }),
      ),
    ).toThrow(/remove the legacy operation wrapper/i);
  });
});

describe("DynamoDBDriver native API", () => {
  it("executes native JSON queries from the editor", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses({
      Items: [
        marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        }),
      ],
    });

    const result = await driver.query(JSON.stringify({ TableName: "users" }), [
      "Scan",
    ]);

    const scanInputs = commandInputs(clientSend, "ScanCommand");
    expect(scanInputs).toHaveLength(1);
    expect(scanInputs[0]).toMatchObject({ TableName: "users" });
    expect(result.rowCount).toBe(1);
    expect(result.columns).toEqual(
      expect.arrayContaining(["email", "tenant_id", "user_id"]),
    );
  });

  it("infers UpdateItem when the editor action is still the generic default", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses({ Attributes: marshall({ tenant_id: "tenant-1" }) });

    const result = await driver.query(
      JSON.stringify({
        TableName: "users",
        Key: marshall({ tenant_id: "tenant-1", user_id: "user-1" }),
        UpdateExpression: "SET #email = :email",
        ExpressionAttributeNames: { "#email": "email" },
        ExpressionAttributeValues: {
          ":email": marshall({ value: "updated@example.com" }).value,
        },
        ReturnValues: "ALL_NEW",
      }),
      ["Query"],
    );

    expect(commandInputs(clientSend, "UpdateItemCommand")).toHaveLength(1);
    expect(result.rowCount).toBe(1);
    expect(result.affectedRows).toBe(1);
  });

  it("executes multiple consecutive request bodies with the selected action", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-1",
            email: "one@example.com",
          }),
        ],
      },
      {
        Items: [
          marshall({
            tenant_id: "tenant-2",
            user_id: "user-2",
            email: "two@example.com",
          }),
        ],
      },
    );

    const result = await driver.query(
      [
        JSON.stringify({ TableName: "users", Limit: 1 }, null, 2),
        JSON.stringify({ TableName: "users", Limit: 2 }, null, 2),
      ].join("\n\n"),
      ["Scan"],
    );

    const scanInputs = commandInputs(clientSend, "ScanCommand");
    expect(scanInputs).toHaveLength(2);
    expect(scanInputs[0]).toMatchObject({ TableName: "users", Limit: 1 });
    expect(scanInputs[1]).toMatchObject({ TableName: "users", Limit: 2 });
    expect(result.rowCount).toBe(2);
    expect(result.rows.flatMap((row) => Object.values(row))).toEqual(
      expect.arrayContaining([
        "tenant-1",
        "user-1",
        "one@example.com",
        "tenant-2",
        "user-2",
        "two@example.com",
      ]),
    );
  });

  it("dispatches batch and transaction native commands from selected actions", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Responses: {
          users: [marshall({ tenant_id: "tenant-1", user_id: "user-1" })],
          audit: [marshall({ id: "event-1" })],
        },
      },
      {
        Responses: [
          {
            Item: marshall({ tenant_id: "tenant-2", user_id: "user-2" }),
          },
        ],
      },
      {
        UnprocessedItems: {
          users: [
            {
              DeleteRequest: {
                Key: marshall({ tenant_id: "tenant-4", user_id: "user-4" }),
              },
            },
          ],
        },
      },
      {},
    );

    const batchGetResult = await driver.query(
      JSON.stringify({
        RequestItems: {
          users: {
            Keys: [marshall({ tenant_id: "tenant-1", user_id: "user-1" })],
          },
          audit: {
            Keys: [marshall({ id: "event-1" })],
          },
        },
      }),
      ["batchGetItemCommand"],
    );

    const transactGetResult = await driver.query(
      JSON.stringify({
        TransactItems: [
          {
            Get: {
              TableName: "users",
              Key: marshall({ tenant_id: "tenant-2", user_id: "user-2" }),
            },
          },
        ],
      }),
      ["transactgetitems"],
    );

    const batchWriteResult = await driver.query(
      JSON.stringify({
        RequestItems: {
          users: [
            {
              PutRequest: {
                Item: marshall({ tenant_id: "tenant-3", user_id: "user-3" }),
              },
            },
            {
              DeleteRequest: {
                Key: marshall({ tenant_id: "tenant-4", user_id: "user-4" }),
              },
            },
          ],
        },
      }),
      ["BatchWriteItemCommand"],
    );

    const transactWriteResult = await driver.query(
      JSON.stringify({
        TransactItems: [
          {
            Put: {
              TableName: "users",
              Item: marshall({ tenant_id: "tenant-5", user_id: "user-5" }),
            },
          },
          {
            Delete: {
              TableName: "users",
              Key: marshall({ tenant_id: "tenant-6", user_id: "user-6" }),
            },
          },
        ],
      }),
      ["TransactWriteItems"],
    );

    expect(commandInputs(clientSend, "BatchGetItemCommand")).toHaveLength(1);
    expect(commandInputs(clientSend, "TransactGetItemsCommand")).toHaveLength(
      1,
    );
    expect(commandInputs(clientSend, "BatchWriteItemCommand")).toHaveLength(1);
    expect(commandInputs(clientSend, "TransactWriteItemsCommand")).toHaveLength(
      1,
    );
    expect(batchGetResult.rowCount).toBe(2);
    expect(transactGetResult.rowCount).toBe(1);
    expect(batchWriteResult.affectedRows).toBe(1);
    expect(transactWriteResult.affectedRows).toBe(2);
    expect(
      [...batchGetResult.rows, ...transactGetResult.rows].flatMap((row) =>
        Object.values(row),
      ),
    ).toEqual(
      expect.arrayContaining([
        "users",
        "audit",
        "event-1",
        "tenant-1",
        "user-1",
        "tenant-2",
        "user-2",
      ]),
    );
  });

  it("rejects saved legacy PartiQL queries", async () => {
    const { driver } = createDriver();

    await expect(
      driver.query('SELECT * FROM "users" WHERE "tenant_id" = \'tenant-1\''),
    ).rejects.toThrow(/PartiQL is no longer supported/i);
  });

  it("treats a bare table request body as Scan", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses({ Items: [] });

    await expect(
      driver.query(JSON.stringify({ TableName: "users" })),
    ).resolves.toMatchObject({ rowCount: 0 });
    expect(commandInputs(clientSend, "ScanCommand")).toHaveLength(1);
  });

  it("builds native JSON previews for insert, update, and delete mutations", () => {
    const { driver } = createDriver();

    const insertPreview = JSON.parse(
      driver.buildMutationPreviewStatement(
        "insert",
        "us-east-1",
        "us-east-1",
        "users",
        {
          values: {
            tenant_id: "tenant-1",
            user_id: "user-1",
            email: "person@example.com",
          },
        },
      ),
    ) as Record<string, unknown>;
    expect(insertPreview).toMatchObject({ TableName: "users" });
    expect(insertPreview).not.toHaveProperty("operation");

    const updatePreview = JSON.parse(
      driver.buildMutationPreviewStatement(
        "update",
        "us-east-1",
        "us-east-1",
        "users",
        {
          primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
          changes: { email: "next@example.com" },
        },
      ),
    ) as Record<string, unknown>;
    expect(updatePreview).toMatchObject({
      TableName: "users",
      UpdateExpression: "SET #u0 = :u0",
    });

    const deletePreviews = driver
      .buildMutationPreviewStatement(
        "delete",
        "us-east-1",
        "us-east-1",
        "users",
        {
          primaryKeyValuesList: [
            { tenant_id: "tenant-1", user_id: "user-1" },
            { tenant_id: "tenant-2", user_id: "user-2" },
          ],
        },
      )
      .split("\n\n")
      .map((entry) => JSON.parse(entry) as Record<string, unknown>);
    expect(deletePreviews).toHaveLength(2);
    expect(deletePreviews[0]).toMatchObject({ TableName: "users" });
    expect(deletePreviews[1]).toMatchObject({ TableName: "users" });
    expect(deletePreviews[0]).not.toHaveProperty("operation");
    expect(deletePreviews[1]).not.toHaveProperty("operation");
  });

  it("coerces edited DynamoDB values into native types before building previews", async () => {
    const { driver } = createDriver();
    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-ddb" }),
        getDriver: () => driver,
      } as never,
      {
        getColumns: async () => [
          ...createColumns(),
          {
            name: "active",
            type: "boolean",
            nativeType: "boolean",
            category: "boolean",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["eq", "neq", "is_null", "is_not_null"],
            valueSemantics: "boolean",
          },
          {
            name: "profile",
            type: "map",
            nativeType: "map",
            category: "json",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["like", "is_null", "is_not_null"],
            valueSemantics: "plain",
          },
          {
            name: "tags",
            type: "string set",
            nativeType: "string set",
            category: "array",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["like", "is_null", "is_not_null"],
            valueSemantics: "plain",
          },
          {
            name: "history",
            type: "list",
            nativeType: "list",
            category: "array",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["like", "is_null", "is_not_null"],
            valueSemantics: "plain",
          },
          {
            name: "payload",
            type: "binary",
            nativeType: "binary",
            category: "binary",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: false,
            filterOperators: ["is_null", "is_not_null"],
            valueSemantics: "plain",
          },
        ],
      },
    );

    const plan = await mutationService.prepareInsertRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      {
        tenant_id: "tenant-1",
        user_id: "user-1",
        age: "31",
        active: "true",
        profile: '{"tier":"pro","visits":3}',
        tags: '["alpha","beta"]',
        history: '[1,"two",true]',
        payload: "0xdeadbeef",
      },
    );

    expect(plan.mode).toBe("driver");
    expect(plan.values).toEqual({
      tenant_id: "tenant-1",
      user_id: "user-1",
      age: 31,
      active: true,
      profile: { tier: "pro", visits: 3 },
      tags: new Set(["alpha", "beta"]),
      history: [1, "two", true],
      payload: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
    });

    const preview = JSON.parse(plan.previewStatements[0] ?? "{}") as {
      ConditionExpression?: string;
      ExpressionAttributeNames?: Record<string, unknown>;
      Item?: Record<string, unknown>;
    };
    expect(preview.ConditionExpression).toBe(
      "attribute_not_exists(#k0) AND attribute_not_exists(#k1)",
    );
    expect(preview.ExpressionAttributeNames).toEqual({
      "#k0": "tenant_id",
      "#k1": "user_id",
    });
    expect(preview.Item).toEqual(
      toJsonSafeValue(
        marshall(
          {
            tenant_id: "tenant-1",
            user_id: "user-1",
            age: 31,
            active: true,
            profile: { tier: "pro", visits: 3 },
            tags: new Set(["alpha", "beta"]),
            history: [1, "two", true],
            payload: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
          },
          { removeUndefinedValues: true },
        ),
      ),
    );
  });

  it("executes native item operations for insert, update, and delete", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {},
      { Attributes: marshall({ tenant_id: "tenant-1" }) },
      { Attributes: marshall({ tenant_id: "tenant-1" }) },
      {},
    );

    await expect(
      driver.insertRow({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        values: {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        updates: [
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
            changes: { email: "next@example.com" },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.deleteRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        primaryKeyValuesList: [
          { tenant_id: "tenant-1", user_id: "user-1" },
          { tenant_id: "tenant-2", user_id: "user-2" },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    const putInputs = commandInputs(clientSend, "PutItemCommand");
    expect(putInputs[0]).toMatchObject({
      TableName: "users",
      ConditionExpression:
        "attribute_not_exists(#k0) AND attribute_not_exists(#k1)",
      ExpressionAttributeNames: {
        "#k0": "tenant_id",
        "#k1": "user_id",
      },
      Item: marshall(
        {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        },
        { removeUndefinedValues: true },
      ),
    });

    const updateInputs = commandInputs(clientSend, "UpdateItemCommand");
    expect(updateInputs[0]).toMatchObject({
      TableName: "users",
      Key: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
      UpdateExpression: "SET #u0 = :u0",
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ReturnValues: "ALL_NEW",
      ExpressionAttributeNames: {
        "#u0": "email",
        "#k0": "tenant_id",
        "#k1": "user_id",
      },
      ExpressionAttributeValues: {
        ":u0": marshall(
          { value: "next@example.com" },
          { removeUndefinedValues: true },
        ).value,
      },
    });

    const deleteInputs = commandInputs(clientSend, "DeleteItemCommand");
    expect(deleteInputs).toHaveLength(2);
    expect(deleteInputs[0]).toMatchObject({
      TableName: "users",
      Key: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ReturnValues: "ALL_OLD",
      ExpressionAttributeNames: {
        "#k0": "tenant_id",
        "#k1": "user_id",
      },
    });
  });

  it("aliases reserved key names in mutation conditions", async () => {
    const { driver, clientSend, queueResponses } = createDriver({
      columns: [
        {
          name: "namespace",
          type: "string",
          nativeType: "string",
          category: "text",
          nullable: false,
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          primaryKeyRole: "partition",
          isForeignKey: false,
          filterable: true,
          filterOperators: ["eq", "neq", "like", "in"],
          valueSemantics: "plain",
        },
        {
          name: "key",
          type: "string",
          nativeType: "string",
          category: "text",
          nullable: false,
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          primaryKeyRole: "sort",
          isForeignKey: false,
          filterable: true,
          filterOperators: ["eq", "neq", "like", "in"],
          valueSemantics: "plain",
        },
        {
          name: "value",
          type: "string",
          nativeType: "string",
          category: "text",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          filterable: true,
          filterOperators: [
            "eq",
            "neq",
            "like",
            "in",
            "is_null",
            "is_not_null",
          ],
          valueSemantics: "plain",
        },
      ],
      describeTable: {
        KeySchema: [
          { AttributeName: "namespace", KeyType: "HASH" },
          { AttributeName: "key", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "namespace", AttributeType: "S" },
          { AttributeName: "key", AttributeType: "S" },
          { AttributeName: "value", AttributeType: "S" },
        ],
      },
    });

    queueResponses(
      {},
      {
        Attributes: marshall({
          namespace: "limits",
          key: "max_login_attempts",
        }),
      },
      {
        Attributes: marshall({
          namespace: "limits",
          key: "max_login_attempts",
        }),
      },
      {},
    );

    await expect(
      driver.insertRow({
        database: "us-east-1",
        schema: "us-east-1",
        table: "ConfigStore",
        values: {
          namespace: "limits",
          key: "max_login_attempts",
          value: "111",
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "ConfigStore",
        updates: [
          {
            primaryKeys: {
              namespace: "limits",
              key: "max_login_attempts",
            },
            changes: { value: "111" },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.deleteRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "ConfigStore",
        primaryKeyValuesList: [
          { namespace: "limits", key: "max_login_attempts" },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    const putInputs = commandInputs(clientSend, "PutItemCommand");
    expect(putInputs[0]).toMatchObject({
      TableName: "ConfigStore",
      ConditionExpression:
        "attribute_not_exists(#k0) AND attribute_not_exists(#k1)",
      ExpressionAttributeNames: {
        "#k0": "namespace",
        "#k1": "key",
      },
    });

    const updateInputs = commandInputs(clientSend, "UpdateItemCommand");
    expect(updateInputs[0]).toMatchObject({
      TableName: "ConfigStore",
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ExpressionAttributeNames: {
        "#u0": "value",
        "#k0": "namespace",
        "#k1": "key",
      },
    });

    const deleteInputs = commandInputs(clientSend, "DeleteItemCommand");
    expect(deleteInputs[0]).toMatchObject({
      TableName: "ConfigStore",
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ExpressionAttributeNames: {
        "#k0": "namespace",
        "#k1": "key",
      },
    });
  });

  it("uses GetItem planning when the full primary key is provided", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Item: marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        }),
      },
      {
        Item: marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        }),
      },
    );

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 25,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant-1" },
        { column: "user_id", operator: "eq", value: "user-1" },
      ],
      sort: null,
      skipCount: false,
    });

    const getItemInputs = commandInputs(clientSend, "GetItemCommand");
    expect(getItemInputs).toHaveLength(2);
    expect(getItemInputs[0]).toMatchObject({
      TableName: "users",
      Key: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
    });
    expect(page.totalCount).toBe(1);
    expect(page.rows).toEqual([
      expect.objectContaining({
        tenant_id: "tenant-1",
        user_id: "user-1",
      }),
    ]);
  });

  it("uses Query planning for partition-key filters and reuses cursor state across pages", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-1",
            email: "one@example.com",
          }),
        ],
        LastEvaluatedKey: marshall(
          { tenant_id: "tenant-1", user_id: "user-1" },
          { removeUndefinedValues: true },
        ),
      },
      { Count: 2 },
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-2",
            email: "two@example.com",
          }),
        ],
      },
    );

    const request = {
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      pageSize: 1,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant-1" },
        { column: "email", operator: "like", value: "example" },
      ] satisfies FilterExpression[],
      sort: { column: "user_id", direction: "asc" } as const,
      skipCount: false,
    };

    const page1 = await driver.readTablePage({ ...request, page: 1 });
    const page2 = await driver.readTablePage({ ...request, page: 2 });

    const queryInputs = commandInputs(clientSend, "QueryCommand");
    expect(queryInputs).toHaveLength(3);
    expect(queryInputs[0]).toMatchObject({
      TableName: "users",
      Limit: 1,
      ScanIndexForward: true,
      KeyConditionExpression: "#n0 = :v0",
      FilterExpression: "contains(#n1, :v1)",
      ExpressionAttributeNames: {
        "#n0": "tenant_id",
        "#n1": "email",
      },
    });
    expect(queryInputs[1]).toMatchObject({
      TableName: "users",
      Select: "COUNT",
    });
    expect(queryInputs[2]).toMatchObject({
      TableName: "users",
      Limit: 1,
      ExclusiveStartKey: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
    });
    expect(page1.totalCount).toBe(2);
    expect(page2.totalCount).toBe(2);
    expect(page1.rows[0]).toEqual(
      expect.objectContaining({ user_id: "user-1" }),
    );
    expect(page2.rows[0]).toEqual(
      expect.objectContaining({ user_id: "user-2" }),
    );
  });

  it("avoids full materialization for client-side filters when skipCount is enabled", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    const driverState = driver as unknown as {
      materializeReadPlanRows: ReturnType<typeof vi.fn>;
    };
    driverState.materializeReadPlanRows = vi.fn(async () => {
      throw new Error("materialization should not be used");
    });

    queueResponses({
      Items: [
        marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "alice@example.com",
        }),
        marshall({
          tenant_id: "tenant-1",
          user_id: "user-2",
          email: "bob@sample.org",
        }),
      ],
      LastEvaluatedKey: marshall(
        { tenant_id: "tenant-1", user_id: "user-2" },
        { removeUndefinedValues: true },
      ),
    });

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 1,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant-1" },
        { column: "email", operator: "ilike", value: "%example.com%" },
      ],
      sort: { column: "user_id", direction: "asc" },
      skipCount: true,
    });

    expect(driverState.materializeReadPlanRows).not.toHaveBeenCalled();
    const queryInputs = commandInputs(clientSend, "QueryCommand");
    expect(queryInputs).toHaveLength(1);
    expect(queryInputs[0]).toMatchObject({
      TableName: "users",
      Limit: 200,
      ScanIndexForward: true,
      KeyConditionExpression: "#n0 = :v0",
      ExpressionAttributeNames: {
        "#n0": "tenant_id",
      },
    });
    expect(page.totalCount).toBe(0);
    expect(page.rows).toEqual([
      expect.objectContaining({
        tenant_id: "tenant-1",
        user_id: "user-1",
        email: "alice@example.com",
      }),
    ]);
  });

  it("falls back to Scan planning for non-key JSON filters", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    const driverState = driver as unknown as {
      describeColumns: ReturnType<typeof vi.fn>;
    };
    driverState.describeColumns = vi.fn(async () => [
      ...createColumns(),
      createMapColumn(),
    ]);
    queueResponses(
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-1",
            address: {
              country: "RU",
              lon: 37.6173,
              city: "Moscow",
              lat: 55.7558,
            },
          }),
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-2",
            address: {
              country: "DE",
              lon: 13.405,
              city: "Berlin",
              lat: 52.52,
            },
          }),
        ],
      },
      { Count: 2 },
    );

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 25,
      filters: [
        {
          column: "address",
          operator: "like",
          value: '{"country":"RU","lon":37.6173,"city":"Moscow","lat":55.7558}',
        },
      ],
      sort: null,
      skipCount: false,
    });

    const scanInputs = commandInputs(clientSend, "ScanCommand");
    expect(scanInputs).toHaveLength(1);
    expect(scanInputs[0]).toMatchObject({
      TableName: "users",
    });
    expect(page.rows).toEqual([
      expect.objectContaining({
        address: '{"country":"RU","lon":37.6173,"city":"Moscow","lat":55.7558}',
      }),
    ]);
  });
});
