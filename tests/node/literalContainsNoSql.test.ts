import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import type {
  ColumnTypeMeta,
  DriverTablePageRequest,
} from "../../src/extension/dbDrivers/types";
import { resolveFilterOperators } from "../../src/extension/dbDrivers/types";

const needles = ["%", "_", "\\", "!", String.raw`!%_\!`, "ordinary", ".*[x]"];
const samples = [
  "plain",
  "100% done",
  "a_b",
  String.raw`path\file`,
  "bang!",
  String.raw`left!%_\!right`,
  "leftXYright",
  "aXb",
  "ordinary string",
  ".*[x]",
];
const columns: ColumnTypeMeta[] = [
  {
    name: "value",
    type: "text",
    nativeType: "text",
    category: "text",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: resolveFilterOperators("text", {
      filterable: true,
      nullable: true,
    }),
    valueSemantics: "plain",
  },
];
const rows = samples.map((value, index) => ({ _id: String(index), value }));

function request(
  column: string,
  value: string,
  operator: "like" | "ilike" = "like",
): DriverTablePageRequest {
  return {
    database: "db",
    schema: "db",
    table: "items",
    page: 1,
    pageSize: 100,
    filters: [{ column, operator, value }],
    sort: null,
    skipCount: true,
  };
}

describe("H1 NoSQL active Contains routes", () => {
  it.each([
    "server",
    "fallback",
  ] as const)("MongoDB %s preserves SQL wildcard characters and regex metacharacters as literal text", async (mode) => {
    const driver = new MongoDBDriver({
      id: "h1-mongo",
      name: "H1",
      type: "mongodb",
    });
    const find = vi.fn(
      (criteria: { value: { $regex: string; $options?: string } }) => {
        if (mode === "fallback") throw new Error("server unavailable");
        const regex = new RegExp(
          criteria.value.$regex,
          criteria.value.$options,
        );
        const cursor = {
          sort: () => cursor,
          skip: () => cursor,
          limit: () => cursor,
          toArray: async () => rows.filter((row) => regex.test(row.value)),
        };
        return cursor;
      },
    );
    Object.assign(driver, {
      describeSchemaColumns: vi.fn().mockResolvedValue(columns),
      requireDb: () => ({ collection: () => ({ find }) }),
      readRows: vi.fn().mockResolvedValue(rows),
    });
    for (const needle of needles) {
      const page = await driver.readTablePage(request("value", needle));
      expect(
        page.rows.map((row) => row.value),
        needle,
      ).toEqual(samples.filter((value) => value.includes(needle)));
    }
    expect(find).toHaveBeenCalledTimes(needles.length);
    if (mode === "server") {
      expect(find.mock.calls[0]?.[0].value.$regex).toBe("%");
      expect(find.mock.calls[1]?.[0].value.$regex).toBe("_");
    }
  });

  it("Redis active client filtering does not strip % from key Contains", async () => {
    const driver = new RedisDriver({
      id: "h1-redis",
      name: "H1",
      type: "redis",
    });
    Object.assign(driver, {
      getDatabaseClient: vi.fn().mockResolvedValue({}),
      readRows: vi.fn().mockResolvedValue(
        samples.map((key) => ({
          type: "string",
          row: { key, value: "value", ttl: null },
        })),
      ),
    });
    for (const needle of needles) {
      const page = await driver.readTablePage(request("key", needle));
      expect(
        page.rows.map((row) => row.key),
        needle,
      ).toEqual(samples.filter((value) => value.includes(needle)));
    }
  });

  it("Elasticsearch active bounded-search filtering does not strip % from _id Contains", async () => {
    const driver = new ElasticsearchDriver({
      id: "h1-es",
      name: "H1",
      type: "elasticsearch",
    });
    const search = vi.fn().mockResolvedValue({
      hits: {
        total: { value: samples.length, relation: "eq" },
        hits: samples.map((_id) => ({ _id, _source: { value: "value" } })),
      },
    });
    Object.assign(driver, { connected: true, client: { search } });
    for (const needle of needles) {
      const page = await driver.readTablePage(request("_id", needle));
      expect(
        page.rows.map((row) => row._id),
        needle,
      ).toEqual(samples.filter((value) => value.includes(needle)));
    }
    expect(search).toHaveBeenCalledTimes(needles.length);
  });

  it.each([
    "like",
    "ilike",
  ] as const)("DynamoDB active %s preserves literal input in native expressions or client filtering", async (operator) => {
    const driver = new DynamoDBDriver({
      id: "h1-ddb",
      name: "H1",
      type: "dynamodb",
      awsRegion: "us-east-1",
    });
    const send = vi.fn(
      async (command: {
        constructor: { name: string };
        input: Record<string, unknown>;
      }) => {
        if (command.constructor.name === "DescribeTableCommand")
          return {
            Table: {
              KeySchema: [{ AttributeName: "_id", KeyType: "HASH" }],
              AttributeDefinitions: [
                { AttributeName: "_id", AttributeType: "S" },
              ],
            },
          };
        const inputs = command.input.ExpressionAttributeValues as
          | Record<string, { S: string }>
          | undefined;
        const needle = inputs ? Object.values(inputs)[0]?.S : undefined;
        return {
          Items: rows
            .filter((row) => needle === undefined || row.value.includes(needle))
            .map((row) => marshall(row)),
        };
      },
    );
    Object.assign(driver, { connected: true, client: { send } });
    vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
    for (const needle of needles) {
      const page = await driver.readTablePage(
        request("value", needle, operator),
      );
      expect(
        page.rows.map((row) => row.value),
        needle,
      ).toEqual(samples.filter((value) => value.includes(needle)));
      const scans = send.mock.calls.filter(
        ([command]) => command.constructor.name === "ScanCommand",
      );
      const input = scans.at(-1)?.[0].input;
      if (operator === "like") {
        expect(input?.FilterExpression).toMatch(/^contains\(/);
        expect(
          Object.values(unmarshall(input?.ExpressionAttributeValues as never)),
        ).toEqual([needle]);
      } else expect(input?.FilterExpression).toBeUndefined();
      const column = columns[0];
      if (!column) throw new Error("Missing Contains column");
      expect(
        driver.buildFilterCondition(column, operator, needle, 1)?.params,
      ).toEqual([needle]);
    }
  });
});
