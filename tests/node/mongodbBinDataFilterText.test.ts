import { Binary } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type {
  ColumnTypeMeta,
  DriverTablePageRequest,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";

const binaryColumn: ColumnTypeMeta = {
  name: "payload",
  type: "binData",
  nativeType: "binData",
  category: "binary",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};

function setup(documents: Record<string, unknown>[]) {
  const driver = new MongoDBDriver({
    id: "mongo-bindata-filter",
    name: "Mongo BinData filter",
    type: "mongodb",
  });
  const find = vi.fn((_criteria: unknown, _options?: unknown) => {
    let limit = Number.POSITIVE_INFINITY;
    const cursor = {
      sort: vi.fn(() => cursor),
      skip: vi.fn(() => cursor),
      limit: vi.fn((value: number) => {
        limit = value;
        return cursor;
      }),
      toArray: vi.fn(async () => documents.slice(0, limit)),
    };
    return cursor;
  });
  Object.assign(driver, {
    connected: true,
    client: {
      db: () => ({
        collection: () => ({
          find,
          countDocuments: vi.fn(async () => documents.length),
        }),
        listCollections: () => ({
          toArray: async () => [
            { name: "items", type: "collection", options: {} },
          ],
        }),
      }),
    },
  });
  return { driver, find };
}

function pageRequest(filters: FilterExpression[]): DriverTablePageRequest {
  return {
    database: "db",
    schema: "",
    table: "items",
    page: 1,
    pageSize: 25,
    filters,
    sort: null,
    skipCount: true,
  };
}

const cafe = new Binary(Buffer.from("cafe", "hex"), 0);
const documents = [
  { _id: "a", payload: cafe },
  { _id: "b", payload: new Binary(Buffer.from("beef", "hex"), 0) },
  { _id: "c", payload: "BinData(x" },
];

describe("MongoDB binData filters with malformed literal text", () => {
  const driver = new MongoDBDriver({
    id: "mongo-bindata-filter",
    name: "Mongo BinData filter",
    type: "mongodb",
  });

  it.each([
    ["like", "BinData(x", "BinData(x"],
    ["ilike", "BinData(0, 'abc')", "BinData(0, 'abc')"],
    ["eq", "BinData(", "BinData("],
    ["eq", "0xzz", "0xzz"],
    ["eq", "BinData(0, 'yv4=')", "0xcafe"],
    ["eq", "0xCAFE", "0xcafe"],
  ] as const)("normalizes %s %s", (operator, value, expected) => {
    expect(driver.normalizeFilterValue(binaryColumn, operator, value)).toBe(
      expected,
    );
  });

  it("searches malformed BinData text as text on the server", async () => {
    const s = setup(documents);
    await s.driver.readTablePage(
      pageRequest([
        { column: "payload", operator: "like", value: "BinData(x" },
      ]),
    );
    expect(s.find.mock.calls.at(-1)?.[0]).toEqual({
      payload: { $regex: "BinData\\(x" },
    });
    await s.driver.readTablePage(
      pageRequest([{ column: "payload", operator: "eq", value: "BinData(x" }]),
    );
    expect(s.find.mock.calls.at(-1)?.[0]).toEqual({ payload: "BinData(x" });
  });

  it("still filters well-formed BinData by binary value", async () => {
    const s = setup(documents);
    await s.driver.readTablePage(
      pageRequest([
        { column: "payload", operator: "eq", value: "BinData(0, 'yv4=')" },
      ]),
    );
    const criteria = s.find.mock.calls.at(-1)?.[0] as { payload: Binary };
    expect(criteria.payload).toBeInstanceOf(Binary);
    expect(Buffer.from(criteria.payload.buffer).toString("hex")).toBe("cafe");
  });

  it.each([
    ["BinData(x", ["c"]],
    ["BinData(0, 'yv4=')", ["a"]],
  ])("matches eq %s on the client", async (value, ids) => {
    const s = setup(documents);
    Object.assign(s.driver, {
      buildMongoFilterCriteria: () => {
        throw new Error("untranslatable filter");
      },
    });
    const page = await s.driver.readTablePage(
      pageRequest([{ column: "payload", operator: "eq", value }]),
    );
    expect(page.rows.map((row) => row._id)).toEqual(ids);
  });
});
