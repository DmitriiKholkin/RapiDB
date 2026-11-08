import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import type {
  DriverSortConfig,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";

type Command = {
  constructor: { name: string };
  input: Record<string, unknown>;
};
type Item = Record<string, AttributeValue>;

function createDriver(options: {
  keys: Array<{ name: string; type: "S" | "N" | "B"; role: "HASH" | "RANGE" }>;
  items: Item[];
}) {
  const keyOf = (item: Item) =>
    JSON.stringify(
      options.keys.map(({ name }) => {
        const value = item[name];
        return value?.B ? Buffer.from(value.B).toString("hex") : value;
      }),
    );
  const send = vi.fn(async (command: Command) => {
    const name = command.constructor.name;
    if (name === "DescribeTableCommand") {
      return {
        Table: {
          KeySchema: options.keys.map((key) => ({
            AttributeName: key.name,
            KeyType: key.role,
          })),
          AttributeDefinitions: options.keys.map((key) => ({
            AttributeName: key.name,
            AttributeType: key.type,
          })),
        },
      };
    }
    if (name === "GetItemCommand") {
      const wanted = keyOf(command.input.Key as Item);
      return { Item: options.items.find((item) => keyOf(item) === wanted) };
    }
    if (name === "ScanCommand" || name === "QueryCommand") {
      return command.input.Select === "COUNT"
        ? { Count: options.items.length }
        : { Items: options.items };
    }
    if (name === "TransactWriteItemsCommand" || name === "UpdateItemCommand") {
      return { Attributes: {} };
    }
    throw new Error(`Unexpected command ${name}`);
  });
  const driver = new DynamoDBDriver({
    id: "ddb-parity",
    name: "Dynamo parity",
    type: "dynamodb",
    awsRegion: "us-east-1",
  });
  Object.assign(driver, { connected: true, client: { send } });
  const read = async (
    filters: FilterExpression[],
    sort: DriverSortConfig | null = null,
  ) =>
    (
      await driver.readTablePage({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        page: 1,
        pageSize: 50,
        filters,
        sort,
        skipCount: true,
      })
    ).rows;
  return { driver, send, read };
}

describe("DynamoDB client-side range filters on binary attributes", () => {
  const items: Item[] = [
    { pk: { S: "p" }, sk: { B: Buffer.from([0x0a]) } },
    { pk: { S: "p" }, sk: { B: Buffer.from([0xff]) } },
    { pk: { S: "p" }, sk: { B: Buffer.from([0x01, 0x00]) } },
  ];
  const harness = () =>
    createDriver({
      keys: [
        { name: "pk", type: "S", role: "HASH" },
        { name: "sk", type: "B", role: "RANGE" },
      ],
      items,
    });

  it.each([
    "0x0a",
    "0x0A",
    "Cg==",
  ])("compares raw bytes for sk gte %s", async (bound) => {
    const { read } = harness();
    const rows = await read([
      { column: "pk", operator: "eq", value: "p" },
      { column: "sk", operator: "gte", value: bound },
    ]);
    expect(rows.map((row) => row.sk)).toEqual(["0x0a", "0xff"]);
  });

  it("orders binary values as unsigned bytes for lt and between", async () => {
    const { read } = harness();
    const lt = await read([
      { column: "pk", operator: "eq", value: "p" },
      { column: "sk", operator: "lt", value: "0x0A" },
    ]);
    expect(lt.map((row) => row.sk)).toEqual(["0x0100"]);
    const between = await read([
      { column: "pk", operator: "eq", value: "p" },
      { column: "sk", operator: "between", value: ["AQA=", "0x0A"] },
    ]);
    expect(between.map((row) => row.sk)).toEqual(["0x0a", "0x0100"]);
  });

  it("does not match a binary range bound that cannot be parsed", async () => {
    const { read } = harness();
    const rows = await read([
      { column: "pk", operator: "eq", value: "p" },
      { column: "sk", operator: "gt", value: "0xzz" },
    ]);
    expect(rows).toEqual([]);
  });
});

describe("DynamoDB client-side filters follow native type semantics", () => {
  const harness = () =>
    createDriver({
      keys: [{ name: "pk", type: "S", role: "HASH" }],
      items: [
        { pk: { S: "a" }, code: { S: "1" }, other: { S: "x" } },
        { pk: { S: "b" }, code: { N: "5" }, other: { S: "y" } },
        { pk: { S: "c" }, code: { NULL: true }, other: { S: "z" } },
        { pk: { S: "d" }, code: { S: "4" }, other: { S: "w" } },
      ],
    });
  const sortByOther = { column: "other", direction: "asc" } as const;

  it("excludes a number item from a string range filter on the GetItem path", async () => {
    const { read, send } = harness();
    const rows = await read(
      [
        { column: "pk", operator: "eq", value: "b" },
        { column: "code", operator: "gt", value: "3" },
      ],
      sortByOther,
    );
    expect(
      send.mock.calls.some(
        ([command]) => command.constructor.name === "GetItemCommand",
      ),
    ).toBe(true);
    expect(rows).toEqual([]);
  });

  it("still matches a string item against a string range filter", async () => {
    const { read } = harness();
    const rows = await read(
      [
        { column: "pk", operator: "eq", value: "d" },
        { column: "code", operator: "gt", value: "3" },
      ],
      sortByOther,
    );
    expect(rows.map((row) => row.pk)).toEqual(["d"]);
  });

  it("matches a NULL-typed attribute with neq like the native <> operator", async () => {
    const { read } = harness();
    const neq = await read(
      [
        { column: "pk", operator: "eq", value: "c" },
        { column: "code", operator: "neq", value: "1" },
      ],
      sortByOther,
    );
    expect(neq.map((row) => row.pk)).toEqual(["c"]);
    const eq = await read(
      [
        { column: "pk", operator: "eq", value: "c" },
        { column: "code", operator: "eq", value: "1" },
      ],
      sortByOther,
    );
    expect(eq).toEqual([]);
  });
});

describe("DynamoDB multi-row updates respect the transaction size limit", () => {
  const harness = () =>
    createDriver({
      keys: [{ name: "id", type: "S", role: "HASH" }],
      items: [],
    });
  const updates = (count: number) =>
    Array.from({ length: count }, (_, index) => ({
      primaryKeys: { id: `row-${index}` },
      changes: { name: `value-${index}` },
    }));

  it("refuses more than 100 row edits before sending anything", async () => {
    const { driver, send } = harness();
    const error = await driver
      .updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "items",
        updates: updates(101),
      })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(DriverUpdateRowsError);
    expect((error as Error).message).toMatch(/100/);
    expect(
      (error as DriverUpdateRowsError).result.updateRowOutcomes?.every(
        ({ status }) => status === "not_applied",
      ),
    ).toBe(true);
    expect(
      send.mock.calls.some(([command]) =>
        ["TransactWriteItemsCommand", "UpdateItemCommand"].includes(
          command.constructor.name,
        ),
      ),
    ).toBe(false);
  });

  it("still applies exactly 100 row edits in one transaction", async () => {
    const { driver, send } = harness();
    const result = await driver.updateRows({
      database: "us-east-1",
      schema: "us-east-1",
      table: "items",
      updates: updates(100),
    });
    expect(result.affectedRows).toBe(100);
    expect(
      send.mock.calls.filter(
        ([command]) => command.constructor.name === "TransactWriteItemsCommand",
      ),
    ).toHaveLength(1);
  });
});
