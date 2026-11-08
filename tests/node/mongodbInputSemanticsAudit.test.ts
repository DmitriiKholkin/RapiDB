import { Binary, Int32, ObjectId } from "mongodb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type {
  ColumnTypeMeta,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
  extra: Partial<ColumnTypeMeta> = {},
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
    ...extra,
  };
}

const binaryColumn = column("payload", "binData", "binary");

function harness() {
  const cursor = {
    sort: vi.fn().mockReturnThis(),
    skip: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    toArray: vi.fn(async () => [{ _id: "existing" }]),
  };
  const find = vi.fn(() => cursor);
  const insertOne = vi.fn().mockResolvedValue({
    acknowledged: true,
    insertedId: "inserted",
  });
  const updateOne = vi
    .fn()
    .mockResolvedValue({ acknowledged: true, matchedCount: 1 });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 2 });
  const config = {
    id: "mongo-audit",
    name: "Mongo audit",
    type: "mongodb",
    database: "test",
  } as const;
  const driver = new MongoDBDriver(config);
  Object.assign(driver, {
    connected: true,
    client: {
      db: () => ({
        collection: () => ({
          find,
          insertOne,
          updateOne,
          deleteOne,
          deleteMany,
        }),
      }),
    },
  });
  return { driver, config, find, insertOne, updateOne, deleteOne, deleteMany };
}

describe("MONGO-1: ISODate follows mongosh UTC defaults", () => {
  const originalTz = process.env.TZ;
  beforeEach(() => {
    process.env.TZ = "Europe/Belgrade";
  });
  afterEach(() => {
    process.env.TZ = originalTz;
  });

  async function deletedId(literal: string): Promise<unknown> {
    const { driver, deleteOne } = harness();
    await driver.query(`db.items.deleteOne({_id: ${literal}})`);
    return deleteOne.mock.calls[0][0]._id;
  }

  it("runs in a non-UTC zone", () => {
    expect(new Date(2026, 0, 1).getTimezoneOffset()).toBe(-60);
  });

  it.each([
    ['ISODate("2026-01-01T00:00:00")', Date.UTC(2026, 0, 1)],
    ['ISODate("2026-01-01T00:00")', Date.UTC(2026, 0, 1)],
    ['ISODate("2026-01-01")', Date.UTC(2026, 0, 1)],
    ['ISODate("2026-01-01 00:00:00")', Date.UTC(2026, 0, 1)],
    ['ISODate("2026-01-01T00:00:00.123")', Date.UTC(2026, 0, 1, 0, 0, 0, 123)],
    [
      'ISODate("2026-01-01T00:00:00.123456")',
      Date.UTC(2026, 0, 1, 0, 0, 0, 123),
    ],
    ['ISODate("2026-01-01T00:00:00Z")', Date.UTC(2026, 0, 1)],
    ['ISODate("2026-01-01T01:00:00+01:00")', Date.UTC(2026, 0, 1)],
    ['ISODate("2026-01-01T01:30:00+0130")', Date.UTC(2026, 0, 1)],
    ['ISODate("2026-01-01T01:00:00+01")', Date.UTC(2026, 0, 1)],
    ['ISODate("2025-12-31T19:00:00-05:00")', Date.UTC(2026, 0, 1)],
    ['ISODate("2024-02-29T12:00:00")', Date.UTC(2024, 1, 29, 12)],
    ['new ISODate("2026-01-01T00:00:00")', Date.UTC(2026, 0, 1)],
    ['new Date(ISODate("2026-01-01T00:00:00"))', Date.UTC(2026, 0, 1)],
  ])("parses %s as UTC when no zone is given", async (literal, expected) => {
    const value = await deletedId(literal);
    expect(value).toBeInstanceOf(Date);
    expect((value as Date).getTime()).toBe(expected);
  });

  it("keeps JavaScript Date local-time semantics for new Date(string)", async () => {
    const value = await deletedId('new Date("2026-01-01T00:00:00")');
    expect((value as Date).getTime()).toBe(
      new Date("2026-01-01T00:00:00").getTime(),
    );
    expect((value as Date).getTime()).toBe(Date.UTC(2025, 11, 31, 23));
  });

  it("keeps current-time support for ISODate()", async () => {
    const before = Date.now();
    const value = (await deletedId("ISODate()")) as Date;
    expect(value.getTime()).toBeGreaterThanOrEqual(before);
    expect(value.getTime()).toBeLessThanOrEqual(Date.now());
  });

  it.each([
    'ISODate("not a date")',
    'ISODate("")',
    'ISODate("2026-02-30")',
    'ISODate("2025-02-29T00:00:00")',
    'ISODate("2026-13-01")',
    'ISODate("2026-01-01T24:00:00")',
    'ISODate("2026-01-01T23:60:00")',
    'ISODate("2026-01-01T00:00:00+25:00")',
    'ISODate("2026-01-01T00:00:00 UTC")',
    'ISODate("Jan 1 2026")',
    "ISODate(2026, 0, 1)",
    'ISODate("2026-01-01", "x")',
    "ISODate(undefined)",
  ])("rejects %s before sending any operation", async (literal) => {
    const { driver, insertOne, deleteOne } = harness();
    await expect(
      driver.query(
        `db.items.insertOne({_id: "first"}); db.items.deleteOne({_id: ${literal}})`,
      ),
    ).rejects.toThrow(/ISODate/);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteOne).not.toHaveBeenCalled();
  });
});

describe("MONGO-2: table inserts follow edit quoting and strictness", () => {
  const idColumn = column("_id", "objectId", "text", {
    nullable: false,
    isPrimaryKey: true,
  });
  const columns = [
    idColumn,
    column("count", "int", "integer"),
    column("ratio", "double", "float"),
    column("flag", "bool", "boolean"),
    column("ref", "objectId", "text"),
    column("label", "string", "text"),
  ];

  function setup() {
    const s = harness();
    const manager = {
      getDriver: () => s.driver,
      getConnection: () => s.config,
    };
    const service = new TableMutationService(manager as never, {
      getColumns: async () => columns,
    });
    return { ...s, service };
  }

  it.each([
    [{ count: '"123"' }, { count: "123" }],
    [
      { ref: '"507f1f77bcf86cd799439011"' },
      { ref: "507f1f77bcf86cd799439011" },
    ],
    [{ flag: '"true"' }, { flag: "true" }],
    [{ _id: '"custom-id"' }, { _id: "custom-id" }],
  ])("stores quoted %j as a string, like edits", async (values, expected) => {
    const { service, insertOne } = setup();
    const plan = await service.prepareInsertRow(
      "mongo-audit",
      "test",
      "",
      "items",
      values,
    );
    expect(plan.values).toEqual(expected);
    await service.insertRow("mongo-audit", "test", "", "items", values);
    expect(insertOne.mock.calls[0][0]).toEqual(expected);
  });

  it("still converts unquoted typed values", async () => {
    const { service } = setup();
    const plan = await service.prepareInsertRow(
      "mongo-audit",
      "test",
      "",
      "items",
      {
        count: "123",
        flag: "true",
        ref: "507f1f77bcf86cd799439011",
        label: "plain",
      },
    );
    expect(plan.values?.count).toBeInstanceOf(Int32);
    expect(Number(plan.values?.count)).toBe(123);
    expect(plan.values?.flag).toBe(true);
    expect(plan.values?.ref).toBeInstanceOf(ObjectId);
    expect(plan.values?.label).toBe("plain");
  });

  it.each([
    [{ count: "abc" }, "count"],
    [{ ratio: "abc" }, "ratio"],
    [{ flag: "yes" }, "flag"],
    [{ ref: "not-an-object-id" }, "ref"],
    [{ _id: "custom-id" }, "_id"],
  ])("rejects unquoted invalid %j instead of storing a string", async (values, name) => {
    const { service, insertOne, driver } = setup();
    const edit = columns.find((entry) => entry.name === name);
    expect(() =>
      driver.coerceTableEditValue(Object.values(values)[0], edit as never),
    ).toThrow(/Quote the value/);
    for (const run of [
      () =>
        service.prepareInsertRow("mongo-audit", "test", "", "items", values),
      () => service.insertRow("mongo-audit", "test", "", "items", values),
    ]) {
      await expect(run()).rejects.toMatchObject({
        status: "prevalidation_failed",
        columns: [name],
        message: expect.stringMatching(/Quote the value/),
      });
    }
    expect(insertOne).not.toHaveBeenCalled();
  });
});

describe("MONGO-3: partial multi-statement failures report applied statements", () => {
  it("returns earlier results with an error naming the failed statement", async () => {
    const { driver, insertOne, deleteMany } = harness();
    deleteMany.mockRejectedValueOnce(new Error("not authorized on test"));
    const result = await driver.query(
      "db.a.insertOne({x: 1}); db.b.deleteMany({}); db.c.insertOne({y: 2})",
    );
    expect(insertOne).toHaveBeenCalledTimes(1);
    expect(result.error).toBe(
      "MongoDB statement 2 of 3 failed: not authorized on test. Earlier statements completed; the outcome of statement 2 is unknown; later statements were not run.",
    );
    expect(result.rowCount).toBe(1);
  });

  it("still throws when nothing was written before the failure", async () => {
    const { driver, deleteMany, find } = harness();
    deleteMany.mockRejectedValueOnce(new Error("boom"));
    await expect(
      driver.query("db.a.find({}); db.b.deleteMany({})"),
    ).rejects.toThrow("boom");
    expect(find).toHaveBeenCalledTimes(1);
  });

  it("still throws when the first statement fails", async () => {
    const { driver, insertOne } = harness();
    insertOne.mockRejectedValueOnce(new Error("first failed"));
    await expect(
      driver.query("db.a.insertOne({x: 1}); db.b.insertOne({x: 2})"),
    ).rejects.toThrow("first failed");
    expect(insertOne).toHaveBeenCalledTimes(1);
  });

  it("reports cancellation after an applied write", async () => {
    const { driver, insertOne, deleteMany } = harness();
    const controller = new AbortController();
    insertOne.mockImplementationOnce(async () => {
      controller.abort();
      return { acknowledged: true, insertedId: "inserted" };
    });
    const result = await driver.query(
      "db.a.insertOne({x: 1}); db.b.deleteMany({})",
      undefined,
      { signal: controller.signal },
    );
    expect(deleteMany).not.toHaveBeenCalled();
    expect(result.error).toBe(
      "MongoDB script was cancelled after statement 1 of 2. Statements 1 to 1 completed; later statements were not run.",
    );
  });
});

describe("MONGO-4: binary input requires explicit forms and round-trips display", () => {
  const driver = new MongoDBDriver({
    id: "mongo-binary-audit",
    name: "Mongo binary audit",
    type: "mongodb",
  });

  it.each([
    "test",
    "name",
    "cafe",
    "1234",
    "hello",
    "q80=",
    "dGVzdA==",
  ])("keeps unprefixed %j as text and requires quotes to edit it", (value) => {
    expect(driver.coerceInputValue(value, binaryColumn)).toBe(value);
    expect(() => driver.coerceTableEditValue(value, binaryColumn)).toThrow(
      /Quote the value/,
    );
    expect(driver.coerceTableEditValue(`"${value}"`, binaryColumn)).toBe(value);
    expect(driver.coerceOriginalValue(value, binaryColumn)).toBe(value);
    expect(driver.formatOutputValue(value, binaryColumn)).toBe(value);
  });

  it.each([
    ["0xcafe", "cafe", 0],
    ["\\xCAFE", "cafe", 0],
    ['BinData(0, "dGVzdA==")', "74657374", 0],
    ['BinData(5, "yv4=")', "cafe", 5],
  ] as const)("decodes explicit %j", (value, hex, subtype) => {
    const coerced = driver.coerceInputValue(value, binaryColumn) as Binary;
    expect(coerced).toBeInstanceOf(Binary);
    expect(coerced.sub_type).toBe(subtype);
    expect(Buffer.from(coerced.buffer).toString("hex")).toBe(hex);
  });

  it.each([
    [Buffer.from("test"), 0],
    [Buffer.from([0xca, 0xfe]), 0],
    [Buffer.alloc(0), 0],
    [Buffer.from("00112233445566778899aabbccddeeff", "hex"), 4],
  ] as const)("re-enters the grid display of %j unchanged", (bytes, subtype) => {
    const column = { ...binaryColumn, bsonSubtype: subtype };
    const displayed = driver.formatOutputValue(
      new Binary(bytes, subtype),
      column,
    );
    expect(displayed).toBe(`0x${bytes.toString("hex")}`);
    const roundtrip = driver.coerceTableEditValue(displayed, column) as Binary;
    expect(roundtrip).toBeInstanceOf(Binary);
    expect(roundtrip.sub_type).toBe(subtype);
    expect(Buffer.from(roundtrip.buffer)).toEqual(bytes);
    const original = driver.coerceOriginalValue(displayed, column) as Binary;
    expect(Buffer.from(original.buffer)).toEqual(bytes);
  });
});

describe("MONGO-5: IS NULL filters use whole-field type semantics", () => {
  const driver = new MongoDBDriver({
    id: "mongo-null-audit",
    name: "Mongo null audit",
    type: "mongodb",
  });
  const tags = column("tags", "array", "array");
  const build = (filters: FilterExpression[]) =>
    (
      driver as unknown as {
        buildMongoFilterCriteria: (
          filters: FilterExpression[],
          columns: ColumnTypeMeta[],
        ) => Record<string, unknown>;
      }
    ).buildMongoFilterCriteria(filters, [tags]);
  const nullTypes = ["null", "missing", "undefined"];

  it("matches null, undefined and missing fields but not arrays containing null", () => {
    expect(build([{ column: "tags", operator: "is_null" }])).toEqual({
      $expr: { $in: [{ $type: "$tags" }, nullTypes] },
    });
  });

  it("negates the same whole-field check for IS NOT NULL", () => {
    expect(build([{ column: "tags", operator: "is_not_null" }])).toEqual({
      $expr: { $not: [{ $in: [{ $type: "$tags" }, nullTypes] }] },
    });
  });

  it("combines with other filters", () => {
    const criteria = build([
      { column: "tags", operator: "is_not_null" },
      { column: "tags", operator: "eq", value: "x" },
    ]);
    expect(criteria.$and).toHaveLength(2);
    expect(JSON.stringify(criteria)).not.toContain('"$ne":null');
  });
});

describe("MONGO-6: unsafe-token checks ignore ordinary object keys", () => {
  it.each([
    'db.c.find({"module": "billing"})',
    'db.c.find({process: "etl"})',
    "db.c.find({ require: 1, import: 2, eval: 3, Function: 4, globalThis: 5 })",
    "db.c.find({for: 1, try: 2, while: 3, catch: 4})",
    "db.c.find({'process': 1, \"for\": 2, 'a\\u0062c': 3})",
    "db.c.find({ /* note */ module /* key */ : 1 })",
    "db.c.find({\n  process: 1,\n  nested: { import: true },\n})",
    "db.c.find({ módulo: 1, processé: 2 })",
    'db.c.find({ "x.constructor": 1, "__proto": 2 })',
  ])("allows %s", async (query) => {
    const { driver, find } = harness();
    await driver.query(query);
    expect(find).toHaveBeenCalledTimes(1);
  });

  it("allows key names in $set", async () => {
    const { driver, updateOne } = harness();
    await driver.query(
      'db.c.updateOne({_id: 1}, {$set: {"import": true, module: "m", process: "p"}})',
    );
    expect(updateOne.mock.calls[0][1]).toEqual({
      $set: { import: true, module: "m", process: "p" },
    });
  });

  it.each([
    "db.c.find({constructor: 1})",
    'db.c.find({"constructor": 1})',
    "db.c.find({'prototype': 1})",
    "db.c.find({__proto__: {}})",
    'db.c.find({"__proto__": {}})',
    'db.c.find({"\\u005f_proto__": {}})',
    "db.c.find({'\\x5f_proto__': {}})",
    "db.c.find({'proto\\x74ype': 1})",
    'db.c.find({"\\q": 1})',
    "db.c.find({a: 1, __defineGetter__: 2})",
    "db.c.find({a: process})",
    "db.c.find({a: this.constructor})",
    "db.c.find({a: globalThis})",
    "db.c.find({[process]: 1})",
    'db.c.find({a: {}["constructor"]})',
    "db.c.find({a: eval})",
    "db.c.find({a: require})",
    'db.c.find({a: Function("return 1")()})',
    "const {constructor: C} = {}; db.c.find({a: C})",
    "db.c.find({a: import.meta})",
    "for (;;) {}",
    "db.c.find({a: (() => 1)()})",
    "db.c.find({module() {}})",
    "db.c.find({ get process() { return 1 } })",
    "db.c.find({a: 1}).constructor",
    "db.c.find({a: x.__proto__})",
  ])("still blocks %s", async (query) => {
    const { driver, find } = harness();
    await expect(driver.query(query)).rejects.toThrow(
      /Unsafe mongosh query blocked/,
    );
    expect(find).not.toHaveBeenCalled();
  });
});
