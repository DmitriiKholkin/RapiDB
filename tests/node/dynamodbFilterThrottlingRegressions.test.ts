import { marshall, NumberValueImpl } from "@aws-sdk/util-dynamodb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DeleteMutationError } from "../../src/extension/dbDrivers/deleteOutcomes";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { isMutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import type {
  ColumnTypeMeta,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";

type Command = {
  constructor: { name: string };
  input: Record<string, unknown>;
};

function column(
  name: string,
  category: ColumnTypeMeta["category"],
  nativeType: string,
  isPrimaryKey = false,
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: !isPrimaryKey,
    isPrimaryKey,
    ...(isPrimaryKey
      ? { primaryKeyOrdinal: 1, primaryKeyRole: "partition" as const }
      : {}),
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "neq", "in", "gt", "lt", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

function createDriver(
  options: {
    keyName?: string;
    keyType?: "S" | "N" | "B";
    items?: Record<string, unknown>[];
    send?: (command: Command) => unknown;
  } = {},
) {
  const keyName = options.keyName ?? "id";
  const columns = [
    column(
      keyName,
      options.keyType === "N" ? "float" : "text",
      options.keyType === "N" ? "number" : "string",
      true,
    ),
    column("name", "text", "string"),
    column("score", "float", "number"),
    column("active", "boolean", "boolean"),
  ];
  const driver = new DynamoDBDriver({
    id: "ddb-g5",
    name: "Dynamo G5",
    type: "dynamodb",
    awsRegion: "us-east-1",
  });
  const send = vi.fn(async (command: Command) => {
    if (command.constructor.name === "DescribeTableCommand") {
      return {
        Table: {
          KeySchema: [{ AttributeName: keyName, KeyType: "HASH" }],
          AttributeDefinitions: [
            { AttributeName: keyName, AttributeType: options.keyType ?? "S" },
          ],
        },
      };
    }
    const custom = options.send?.(command);
    if (custom !== undefined) return custom;
    if (command.constructor.name === "GetItemCommand") {
      const item = options.items?.[0];
      return item ? { Item: marshall(item) } : {};
    }
    if (
      command.constructor.name === "ScanCommand" ||
      command.constructor.name === "QueryCommand"
    ) {
      return { Items: (options.items ?? []).map((item) => marshall(item)) };
    }
    return {};
  });
  Object.assign(driver, {
    client: { send },
    connected: true,
    describeColumns: vi.fn(async () => columns),
  });
  return { driver, send };
}

function inputs(send: ReturnType<typeof vi.fn>, name: string) {
  return send.mock.calls
    .map(([command]) => command as Command)
    .filter((command) => command.constructor.name === name)
    .map((command) => command.input);
}

const pageRequest = {
  database: "us-east-1",
  schema: "us-east-1",
  table: "items",
  page: 1,
  pageSize: 25,
  sort: null,
  skipCount: true,
};

function namedError(name: string) {
  return Object.assign(new Error(`${name} raised`), { name });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("DynamoDB read retries", () => {
  type RetryRead = <T>(
    read: () => Promise<T>,
    signal?: AbortSignal,
    deadline?: number,
  ) => Promise<T>;
  const retryReadOf = (driver: DynamoDBDriver): RetryRead =>
    (driver as unknown as { retryRead: RetryRead }).retryRead.bind(driver);

  it("keeps retrying throttled reads with exponential backoff", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const { driver } = createDriver();
    let calls = 0;
    const read = vi.fn(async () => {
      calls += 1;
      if (calls <= 5) throw namedError("ThrottlingException");
      return "ok";
    });
    await expect(retryReadOf(driver)(read)).resolves.toBe("ok");
    expect(read).toHaveBeenCalledTimes(6);
  });

  it("gives up within the retry budget", async () => {
    const { driver } = createDriver();
    const read = vi.fn(async () => {
      throw namedError("ProvisionedThroughputExceededException");
    });
    const startedAt = Date.now();
    await expect(retryReadOf(driver)(read)).rejects.toThrow(
      "ProvisionedThroughputExceededException raised",
    );
    expect(Date.now() - startedAt).toBeLessThan(2100);
    expect(read.mock.calls.length).toBeGreaterThan(3);
  });

  it("does not sleep past the operation deadline", async () => {
    const { driver } = createDriver();
    const read = vi.fn(async () => {
      throw namedError("ThrottlingException");
    });
    const startedAt = Date.now();
    await expect(
      retryReadOf(driver)(read, undefined, Date.now() + 80),
    ).rejects.toThrow("ThrottlingException raised");
    expect(Date.now() - startedAt).toBeLessThan(150);
  });

  it("stops waiting when the signal aborts", async () => {
    const { driver } = createDriver();
    const controller = new AbortController();
    const read = vi.fn(async () => {
      setTimeout(() => controller.abort(new Error("cancelled")), 5);
      throw namedError("ThrottlingException");
    });
    await expect(
      retryReadOf(driver)(read, controller.signal),
    ).rejects.toThrow();
    expect(read).toHaveBeenCalledTimes(1);
  });
});

describe("DynamoDB throttled writes are reported as not applied", () => {
  const throttled = (name: string) => (command: Command) =>
    [
      "UpdateItemCommand",
      "TransactWriteItemsCommand",
      "PutItemCommand",
      "DeleteItemCommand",
    ].includes(command.constructor.name)
      ? Promise.reject(namedError(name))
      : undefined;

  it.each([
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
  ])("UpdateItem %s", async (name) => {
    const { driver } = createDriver({ send: throttled(name) });
    const error = await driver
      .updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        updates: [{ primaryKeys: { id: "a" }, changes: { name: "x" } }],
      })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(DriverUpdateRowsError);
    expect((error as DriverUpdateRowsError).result.updateRowOutcomes).toEqual([
      { rowIndex: 0, status: "not_applied" },
    ]);
  });

  it("TransactWriteItems throttling", async () => {
    const { driver } = createDriver({ send: throttled("ThrottlingException") });
    const error = await driver
      .updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        updates: [
          { primaryKeys: { id: "a" }, changes: { name: "x" } },
          { primaryKeys: { id: "b" }, changes: { name: "y" } },
        ],
      })
      .catch((caught: unknown) => caught);
    expect(
      (error as DriverUpdateRowsError).result.updateRowOutcomes?.map(
        ({ status }) => status,
      ),
    ).toEqual(["not_applied", "not_applied"]);
  });

  it("keeps unknown outcomes for non-throttling transport failures", async () => {
    const { driver } = createDriver({
      send: (command) =>
        command.constructor.name === "UpdateItemCommand"
          ? Promise.reject(namedError("TimeoutError"))
          : undefined,
    });
    const error = await driver
      .updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        updates: [{ primaryKeys: { id: "a" }, changes: { name: "x" } }],
      })
      .catch((caught: unknown) => caught);
    expect((error as DriverUpdateRowsError).result.updateRowOutcomes).toEqual([
      { rowIndex: 0, status: "unknown" },
    ]);
  });

  it("PutItem throttling is not executed", async () => {
    const { driver } = createDriver({ send: throttled("ThrottlingException") });
    const error = await driver
      .insertRow({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        values: { id: "a", name: "x" },
      })
      .catch((caught: unknown) => caught);
    expect(isMutationNotExecutedError(error)).toBe(true);
  });

  it("DeleteItem throttling marks the row as failed rather than unknown", async () => {
    const { driver } = createDriver({
      send: throttled("RequestLimitExceeded"),
    });
    const error = await driver
      .deleteRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        primaryKeyValuesList: [{ id: "a" }, { id: "b" }],
      })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(DeleteMutationError);
    expect(
      (error as DeleteMutationError).deleteResult.rowOutcomes?.map(
        ({ status }) => status,
      ),
    ).toEqual(["failed", "skipped"]);
  });
});

describe("DynamoDB filter values", () => {
  async function scanValues(filters: FilterExpression[]) {
    const { driver, send } = createDriver();
    await driver.readTablePage({ ...pageRequest, filters });
    const scan = inputs(send, "ScanCommand")[0];
    return scan?.ExpressionAttributeValues as Record<string, unknown>;
  }

  it("does not trim string filter values", async () => {
    expect(
      Object.values(
        await scanValues([{ column: "name", operator: "eq", value: " ABC " }]),
      ),
    ).toEqual([{ S: " ABC " }]);
  });

  it("still trims around numeric filter values", async () => {
    expect(
      Object.values(
        await scanValues([{ column: "score", operator: "eq", value: " 5 " }]),
      ),
    ).toEqual([{ N: "5" }]);
  });

  it("splits IN lists with quote awareness", async () => {
    expect(
      Object.values(
        await scanValues([
          {
            column: "name",
            operator: "in",
            value: String.raw`"a,b", 'c,\'d', e`,
          },
        ]),
      ),
    ).toEqual([{ S: "a,b" }, { S: "c,'d" }, { S: "e" }]);
  });
});

describe("DynamoDB client-side equality follows native types", () => {
  const items = [
    { id: "t", name: "a", score: 1, active: true },
    { id: "f", name: "b", score: 2, active: false },
    { id: "s", name: "c", score: "1", active: "1" },
  ];

  it.each([
    ["1", ["t"]],
    ["TRUE", ["t"]],
    ["true", ["t"]],
    ["0", ["f"]],
  ])("matches boolean filter %s on materialized sorted pages", async (value, ids) => {
    const { driver } = createDriver({ items });
    const page = await driver.readTablePage({
      ...pageRequest,
      sort: { column: "name", direction: "asc" },
      filters: [{ column: "active", operator: "eq", value }],
    });
    expect(page.rows.map((row) => row.id)).toEqual(ids);
  });

  it("does not match S('1') with a numeric filter", async () => {
    const { driver } = createDriver({ items });
    const page = await driver.readTablePage({
      ...pageRequest,
      sort: { column: "name", direction: "asc" },
      filters: [{ column: "score", operator: "eq", value: "1" }],
    });
    expect(page.rows.map((row) => row.id)).toEqual(["t"]);
  });

  it.each([
    [{ id: "k", name: "a", score: "1" }, []],
    [{ id: "k", name: "a", score: 1 }, ["k"]],
  ])("applies typed equality after a GetItem plan for %j", async (item, ids) => {
    const { driver, send } = createDriver({ items: [item] });
    const page = await driver.readTablePage({
      ...pageRequest,
      sort: { column: "name", direction: "asc" },
      filters: [
        { column: "id", operator: "eq", value: "k" },
        { column: "score", operator: "eq", value: "1" },
      ],
    });
    expect(inputs(send, "GetItemCommand")).toHaveLength(1);
    expect(page.rows.map((row) => row.id)).toEqual(ids);
  });

  it("keeps neq true across mismatched native types", async () => {
    const { driver } = createDriver({ items });
    const page = await driver.readTablePage({
      ...pageRequest,
      sort: { column: "name", direction: "asc" },
      filters: [{ column: "score", operator: "neq", value: "1" }],
    });
    expect(page.rows.map((row) => row.id)).toEqual(["f", "s"]);
  });
});

describe("DynamoDB native mutation affected rows", () => {
  it.each([
    ["DeleteItem", { ReturnValues: "NONE" }, {}, undefined],
    [
      "DeleteItem",
      { ConditionExpression: "attribute_exists(id)" },
      {},
      undefined,
    ],
    ["DeleteItem", { ReturnValues: "ALL_OLD" }, {}, 0],
    [
      "DeleteItem",
      { ReturnValues: "ALL_OLD" },
      { Attributes: marshall({ id: "a" }) },
      1,
    ],
    ["PutItem", {}, {}, 1],
  ] as const)("%s %j reports %s", async (operation, extra, response, expected) => {
    const { driver, send } = createDriver({
      send: (command) =>
        command.constructor.name === `${operation}Command`
          ? response
          : undefined,
    });
    const body =
      operation === "PutItem"
        ? { TableName: "items", Item: marshall({ id: "a" }) }
        : { TableName: "items", Key: marshall({ id: "a" }), ...extra };
    const result = await driver.query(JSON.stringify(body));
    expect(inputs(send, `${operation}Command`)).toHaveLength(1);
    expect(result.affectedRows).toBe(expected);
  });
});

describe("DynamoDB key change detection", () => {
  it.each([
    ["N", NumberValueImpl.from("1"), NumberValueImpl.from("1.0")],
    ["N", NumberValueImpl.from("10"), 10],
    ["B", Buffer.from([1, 2]), Buffer.from([1, 2])],
    ["S", "k", "k"],
  ] as const)("accepts an unchanged %s key value", async (keyType, original, edited) => {
    const { driver, send } = createDriver({
      keyType,
      send: (command) =>
        command.constructor.name === "UpdateItemCommand"
          ? { Attributes: marshall({ id: "k" }) }
          : undefined,
    });
    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        updates: [
          { primaryKeys: { id: original }, changes: { id: edited, name: "x" } },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });
    expect(inputs(send, "UpdateItemCommand")).toHaveLength(1);
  });

  it("still rejects a real key change", async () => {
    const { driver } = createDriver({ keyType: "N" });
    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        updates: [
          {
            primaryKeys: { id: NumberValueImpl.from("1") },
            changes: { id: NumberValueImpl.from("2"), name: "x" },
          },
        ],
      }),
    ).rejects.toThrow("does not support updating key attribute 'id'");
  });
});

describe("DynamoDB keys named __proto__", () => {
  it("keeps the attribute in DeleteItem and UpdateItem keys", async () => {
    const { driver, send } = createDriver({
      keyName: "__proto__",
      send: (command) =>
        command.constructor.name === "DeleteItemCommand" ||
        command.constructor.name === "UpdateItemCommand"
          ? { Attributes: JSON.parse('{"__proto__":{"S":"v"}}') }
          : undefined,
    });
    const primaryKeys = JSON.parse('{"__proto__":"v"}') as Record<
      string,
      unknown
    >;
    await driver.deleteRows({
      database: "us-east-1",
      schema: "us-east-1",
      table: "items",
      primaryKeyValuesList: [primaryKeys],
    });
    await driver.updateRows({
      database: "us-east-1",
      schema: "us-east-1",
      table: "items",
      updates: [{ primaryKeys, changes: { name: "x" } }],
    });
    for (const name of ["DeleteItemCommand", "UpdateItemCommand"]) {
      const key = inputs(send, name)[0]?.Key as Record<string, unknown>;
      expect(Object.hasOwn(key, "__proto__")).toBe(true);
      expect(Object.getOwnPropertyDescriptor(key, "__proto__")?.value).toEqual({
        S: "v",
      });
    }
  });

  it("does not read inherited members as key values", async () => {
    const { driver, send } = createDriver({ keyName: "constructor" });
    await expect(
      driver.deleteRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        primaryKeyValuesList: [{}],
      }),
    ).rejects.toThrow("full primary key");
    expect(inputs(send, "DeleteItemCommand")).toHaveLength(0);
  });

  it("does not show inherited members in native query cells", async () => {
    const { driver } = createDriver({
      send: (command) =>
        command.constructor.name === "ScanCommand"
          ? {
              Items: [
                { id: { S: "a" }, constructor: { S: "own" } },
                { id: { S: "b" } },
              ],
            }
          : undefined,
    });
    const result = await driver.query(JSON.stringify({ TableName: "items" }));
    const index = result.columns.indexOf("constructor");
    expect(index).toBeGreaterThanOrEqual(0);
    expect(result.rows.map((row) => row[`__col_${index}`])).toEqual([
      "own",
      null,
    ]);
    expect(result.missingCells).toContainEqual({
      rowIndex: 1,
      columnIndex: index,
    });
  });

  it("keeps the attribute in a GetItem plan key", async () => {
    const { driver, send } = createDriver({ keyName: "__proto__" });
    await driver.readTablePage({
      ...pageRequest,
      filters: [{ column: "__proto__", operator: "eq", value: "v" }],
    });
    const key = inputs(send, "GetItemCommand")[0]?.Key as Record<
      string,
      unknown
    >;
    expect(Object.getOwnPropertyDescriptor(key, "__proto__")?.value).toEqual({
      S: "v",
    });
  });
});
