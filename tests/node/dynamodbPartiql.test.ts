import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import type {
  ColumnTypeMeta,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

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
      filterOperators: ["eq", "neq", "like", "in"],
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

function createDriver() {
  const driver = new DynamoDBDriver(config);
  const driverState = driver as unknown as {
    client: { send: ReturnType<typeof vi.fn> } | null;
    documentClient: { send: ReturnType<typeof vi.fn> } | null;
    connected: boolean;
    describeColumns: ReturnType<typeof vi.fn>;
  };

  driverState.connected = true;
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

      return { TableNames: ["users"] };
    }),
  };
  driverState.documentClient = {
    send: vi.fn(async () => ({ Items: [] })),
  };
  driverState.describeColumns = vi.fn(async () => createColumns());

  return {
    driver,
    clientSend: driverState.client.send,
    documentSend: driverState.documentClient.send,
  };
}

describe("DynamoDBDriver PartiQL", () => {
  it("executes raw PartiQL queries from the editor", async () => {
    const { driver, documentSend } = createDriver();
    documentSend.mockResolvedValueOnce({
      Items: [
        {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        },
      ],
    });

    const result = await driver.query(
      'SELECT * FROM "users" WHERE "tenant_id" = \'tenant-1\'',
    );

    expect(documentSend).toHaveBeenCalledTimes(1);
    expect(documentSend.mock.calls[0]?.[0].input).toMatchObject({
      Statement: 'SELECT * FROM "users" WHERE "tenant_id" = \'tenant-1\'',
    });
    expect(result.rowCount).toBe(1);
    expect(result.columns).toEqual(
      expect.arrayContaining(["email", "tenant_id", "user_id"]),
    );
    expect(result.rows[0]).toEqual(
      expect.objectContaining({
        __col_0: expect.any(String),
      }),
    );
  });

  it("builds PartiQL previews for insert, update, and delete mutations", () => {
    const { driver } = createDriver();

    const insertPreview = driver.buildMutationPreviewStatement(
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
    );
    expect(insertPreview).toBe(
      "INSERT INTO \"users\" VALUE {'tenant_id': 'tenant-1', 'user_id': 'user-1', 'email': 'person@example.com'}",
    );

    const updatePreview = driver.buildMutationPreviewStatement(
      "update",
      "us-east-1",
      "us-east-1",
      "users",
      {
        primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
        changes: { email: "next@example.com" },
      },
    );
    expect(updatePreview).toBe(
      'UPDATE "users" SET "email" = \'next@example.com\' WHERE "tenant_id" = \'tenant-1\' AND "user_id" = \'user-1\' RETURNING ALL NEW *',
    );

    const deletePreview = driver.buildMutationPreviewStatement(
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
    );
    expect(deletePreview).toBe(
      'DELETE FROM "users" WHERE "tenant_id" = \'tenant-1\' AND "user_id" = \'user-1\' RETURNING ALL OLD *;\nDELETE FROM "users" WHERE "tenant_id" = \'tenant-2\' AND "user_id" = \'user-2\' RETURNING ALL OLD *',
    );
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
    expect(plan.previewStatements).toEqual([
      "INSERT INTO \"users\" VALUE {'tenant_id': 'tenant-1', 'user_id': 'user-1', 'age': 31, 'active': true, 'profile': {'tier': 'pro', 'visits': 3}, 'tags': <<'alpha', 'beta'>>, 'history': [1, 'two', true], 'payload': '3q2+7w=='}",
    ]);
  });

  it("executes PartiQL statements for insert, update, and delete operations", async () => {
    const { driver, documentSend } = createDriver();
    documentSend
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ Items: [{ tenant_id: "tenant-1" }] })
      .mockResolvedValueOnce({ Items: [{ tenant_id: "tenant-1" }] })
      .mockResolvedValueOnce({ Items: [] });

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

    expect(documentSend.mock.calls[0]?.[0].input).toMatchObject({
      Statement:
        "INSERT INTO \"users\" VALUE {'tenant_id': ?, 'user_id': ?, 'email': ?}",
      Parameters: ["tenant-1", "user-1", "person@example.com"],
    });
    expect(documentSend.mock.calls[1]?.[0].input).toMatchObject({
      Statement:
        'UPDATE "users" SET "email" = ? WHERE "tenant_id" = ? AND "user_id" = ? RETURNING ALL NEW *',
      Parameters: ["next@example.com", "tenant-1", "user-1"],
    });
    expect(documentSend.mock.calls[2]?.[0].input).toMatchObject({
      Statement:
        'DELETE FROM "users" WHERE "tenant_id" = ? AND "user_id" = ? RETURNING ALL OLD *',
      Parameters: ["tenant-1", "user-1"],
    });
    expect(documentSend.mock.calls[3]?.[0].input).toMatchObject({
      Statement:
        'DELETE FROM "users" WHERE "tenant_id" = ? AND "user_id" = ? RETURNING ALL OLD *',
      Parameters: ["tenant-2", "user-2"],
    });
  });

  it("uses PartiQL WHERE clauses for table-viewer filters", async () => {
    const { driver, documentSend } = createDriver();
    const filters: FilterExpression[] = [
      { column: "email", operator: "like", value: "example" },
      { column: "age", operator: "gte", value: "30" },
    ];
    documentSend.mockResolvedValueOnce({
      Items: [
        {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
          age: 31,
        },
      ],
    });

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 25,
      filters,
      sort: { column: "user_id", direction: "asc" },
      skipCount: false,
    });

    expect(documentSend).toHaveBeenCalledTimes(1);
    expect(documentSend.mock.calls[0]?.[0].input).toMatchObject({
      Statement:
        'SELECT * FROM "users" WHERE contains("email", ?) AND "age" >= ? ORDER BY "user_id" ASC',
      Parameters: ["example", 30],
      Limit: 200,
    });
    expect(page.rows).toEqual([
      {
        tenant_id: "tenant-1",
        user_id: "user-1",
        email: "person@example.com",
        age: 31,
      },
    ]);
    expect(page.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "tenant_id",
          primaryKeyRole: "partition",
        }),
        expect.objectContaining({
          name: "user_id",
          primaryKeyRole: "sort",
        }),
      ]),
    );
  });
});
