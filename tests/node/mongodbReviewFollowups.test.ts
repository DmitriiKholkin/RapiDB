import { Binary } from "mongodb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
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
const idColumn = column("_id", "string", "text", {
  nullable: false,
  isPrimaryKey: true,
});
const primaryKeys = {
  _id: { $rapidbMongoId: { type: "string", value: "row-1" } },
};

function harness() {
  const cursor = {
    sort: vi.fn().mockReturnThis(),
    skip: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    toArray: vi.fn(async () => [{ _id: "existing" }]),
  };
  const find = vi.fn((_filter?: unknown) => cursor);
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
    id: "mongo-followup",
    name: "Mongo follow-up",
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
  const columns = [idColumn, binaryColumn];
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  };
  const service = new TableMutationService(manager as never, {
    getColumns: async () => columns,
  });
  return {
    driver,
    manager,
    service,
    columns,
    find,
    insertOne,
    updateOne,
    deleteOne,
    deleteMany,
  };
}

describe("MONGO-1 follow-up: ISODate forms", () => {
  const originalTz = process.env.TZ;
  beforeEach(() => {
    process.env.TZ = "Europe/Belgrade";
  });
  afterEach(() => {
    process.env.TZ = originalTz;
  });

  it("parses the compact ISO-8601 form", async () => {
    const { driver, deleteOne } = harness();
    await driver.query(
      'db.items.deleteOne({_id: ISODate("20260101T000000Z")})',
    );
    const value = deleteOne.mock.calls[0][0]._id as Date;
    expect(value).toBeInstanceOf(Date);
    expect(value.getTime()).toBe(Date.UTC(2026, 0, 1));
  });

  it("parses ISODate nested inside $gte as UTC", async () => {
    const { driver, deleteMany } = harness();
    await driver.query(
      'db.items.deleteMany({ts: {$gte: ISODate("2026-01-01T00:00:00")}})',
    );
    const value = deleteMany.mock.calls[0][0].ts.$gte as Date;
    expect(value).toBeInstanceOf(Date);
    expect(value.getTime()).toBe(Date.UTC(2026, 0, 1));
  });
});

describe("MONGO-3 follow-up: cancellation during a statement after a write", () => {
  it("reports the cancelled statement's outcome as unknown", async () => {
    const { driver, insertOne, deleteMany } = harness();
    const controller = new AbortController();
    deleteMany.mockImplementationOnce(async () => {
      controller.abort();
      throw new Error("operation was interrupted");
    });
    const result = await driver.query(
      "db.a.insertOne({x: 1}); db.b.deleteMany({}); db.c.insertOne({y: 2})",
      undefined,
      { signal: controller.signal },
    );
    expect(insertOne).toHaveBeenCalledTimes(1);
    expect(deleteMany).toHaveBeenCalledTimes(1);
    expect(result.error).toBe(
      "MongoDB statement 2 of 3 was cancelled. Earlier statements completed; the outcome of statement 2 is unknown; later statements were not run.",
    );
    expect(result.rowCount).toBe(1);
  });
});

describe("MONGO-4 follow-up: grid BinData input", () => {
  const driver = new MongoDBDriver({
    id: "mongo-bindata-followup",
    name: "Mongo BinData follow-up",
    type: "mongodb",
  });

  it.each([
    [
      "BinData(4, 'ESIzRFVmd4iZqrvM3e7/AA==')",
      4,
      "112233445566778899aabbccddeeff00",
    ],
    [
      'BinData(4, "ESIzRFVmd4iZqrvM3e7/AA==")',
      4,
      "112233445566778899aabbccddeeff00",
    ],
    ["new BinData(0, 'yv4=')", 0, "cafe"],
    ["BinData(255,'')", 255, ""],
  ] as const)("decodes %s", (value, subtype, hex) => {
    for (const coerce of [
      () => driver.coerceInputValue(value, binaryColumn),
      () => driver.coerceTableEditValue(value, binaryColumn),
      () => driver.coerceOriginalValue(value, binaryColumn),
    ]) {
      const coerced = coerce() as Binary;
      expect(coerced).toBeInstanceOf(Binary);
      expect(coerced.sub_type).toBe(subtype);
      expect(Buffer.from(coerced.buffer).toString("hex")).toBe(hex);
    }
    expect(driver.checkPersistedEdit(binaryColumn, value)).toBeNull();
  });

  const invalidBinData = [
    'BinData(256, "AA==")',
    'BinData(0, "AB==")',
    'BinData(0, "abc")',
    'BinData(0, "AA")',
    'BinData(-1, "AA==")',
    'BinData(1.5, "AA==")',
    "BinData(0, 'AA==\")",
    "BinData(0, AA==)",
    'BinData(0, "AA==", 1)',
    "BinData(",
  ];

  it.each(invalidBinData)("rejects malformed %s in edits", (value) => {
    expect(() => driver.coerceInputValue(value, binaryColumn)).toThrow(
      /BinData requires/,
    );
    expect(() => driver.coerceTableEditValue(value, binaryColumn)).toThrow(
      /BinData requires/,
    );
    expect(driver.checkPersistedEdit(binaryColumn, value)).toMatchObject({
      ok: false,
      shouldVerify: false,
      message: expect.stringMatching(/BinData requires/),
    });
    expect(driver.coerceOriginalValue(value, binaryColumn)).toBe(value);
    expect(driver.formatOutputValue(value, binaryColumn)).toBe(value);
  });

  it.each(invalidBinData)("rejects %s before any $set", async (value) => {
    const s = harness();
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-followup",
      "test",
      "",
      "items",
      [{ primaryKeys, changes: { payload: value } }],
      s.columns,
    );
    expect(result).toMatchObject({
      success: false,
      rowOutcomes: [
        {
          status: "prevalidation_failed",
          columns: ["payload"],
          message: expect.stringMatching(/BinData requires/),
        },
      ],
    });
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it("stores quoted BinData text as a string", () => {
    expect(
      driver.coerceTableEditValue("\"BinData(256, 'AA==')\"", binaryColumn),
    ).toBe("BinData(256, 'AA==')");
  });

  it.each([
    ["\"BinData(0, 'abc')\"", "BinData(0, 'abc')"],
    ["\"BinData(256,'AA==')\"", "BinData(256,'AA==')"],
    ['"BinData("', "BinData("],
    ['"0xzz"', "0xzz"],
    ["'0x1'", "0x1"],
  ])("applies quoted %s as a string", async (value, stored) => {
    expect(driver.checkPersistedEdit(binaryColumn, value)).toBeNull();
    const s = harness();
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-followup",
      "test",
      "",
      "items",
      [{ primaryKeys, changes: { payload: value } }],
      s.columns,
    );
    expect(result.success).toBe(true);
    expect(s.updateOne.mock.calls[0][1]).toEqual({
      $set: { payload: stored },
    });
  });

  it("matches the mongosh BinData validation", async () => {
    const { driver: shell, insertOne } = harness();
    for (const literal of ['BinData(256, "AA==")', 'BinData(0, "AB==")']) {
      await expect(
        shell.query(`db.items.insertOne({payload: ${literal}})`),
      ).rejects.toThrow(/BinData requires/);
    }
    expect(insertOne).not.toHaveBeenCalled();
    await shell.query("db.items.insertOne({payload: BinData(4, 'AA==')})");
    expect(insertOne.mock.calls[0][0].payload.sub_type).toBe(4);
  });

  it("rejects unquoted text in an existing Binary cell", async () => {
    const s = harness();
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-followup",
      "test",
      "",
      "items",
      [
        {
          primaryKeys,
          changes: { payload: "deadbeef" },
          originalTypes: {
            payload: {
              category: "binary",
              nativeType: "binData",
              bsonSubtype: 0,
            },
          },
        },
      ],
      s.columns,
    );
    expect(result).toMatchObject({
      success: false,
      rowOutcomes: [
        {
          status: "prevalidation_failed",
          message: expect.stringMatching(/Quote the value/),
        },
      ],
    });
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it("keeps accepting text in rows whose field is a string", async () => {
    const s = harness();
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-followup",
      "test",
      "",
      "items",
      [
        {
          primaryKeys,
          changes: { payload: "deadbeef" },
          originalTypes: {
            payload: { category: "text", nativeType: "string" },
          },
        },
      ],
      s.columns,
    );
    expect(result.success).toBe(true);
    expect(s.updateOne.mock.calls[0][1]).toEqual({
      $set: { payload: "deadbeef" },
    });
  });

  it("rejects unquoted text on insert into a Binary column", async () => {
    const s = harness();
    await expect(
      s.service.prepareInsertRow("mongo-followup", "test", "", "items", {
        payload: "deadbeef",
      }),
    ).rejects.toMatchObject({
      status: "prevalidation_failed",
      columns: ["payload"],
      message: expect.stringMatching(/Quote the value/),
    });
    expect(s.insertOne).not.toHaveBeenCalled();
  });
});

describe("MONGO-6 follow-up: JavaScript string keys", () => {
  it.each([
    'db.c.find({"\\x41": 1})',
    'db.c.find({"it\\\'s": 1})',
    'db.c.find({"\\u{41}": 1})',
    'db.c.find({"a\\\nb": 1})',
    "db.c.find({'\\u{41}': 1, \"\\u0041\": 2})",
  ])("allows JavaScript escapes in double-quoted key %s", async (query) => {
    const { driver, find } = harness();
    await driver.query(query);
    expect(find).toHaveBeenCalledTimes(1);
  });

  it("decodes double-quoted keys like JavaScript", async () => {
    const { driver, find } = harness();
    await driver.query('db.c.find({"\\x41": 1, "it\\\'s": 2, "a\\\nb": 3})');
    expect(find.mock.calls[0][0]).toEqual({ A: 1, "it's": 2, ab: 3 });
  });

  it.each([
    "switch (0) { case 0: break; case 1, process: break; } db.c.find({})",
    "{eval: 1}; db.c.find({})",
    "var p; ({process: p} = this); db.c.find({a: p === undefined})",
  ])("rejects non-operation JS programs even with key-shaped names: %s", async (query) => {
    const { driver, find, insertOne } = harness();
    await expect(
      driver.query(`db.c.insertOne({_id: 'earlier'}); ${query}`),
    ).rejects.toThrow(/Unsupported mongosh JavaScript/);
    expect(find).not.toHaveBeenCalled();
    expect(insertOne).not.toHaveBeenCalled();
  });

  it.each([
    "db.c.find({a: c ? process : x})",
    'db.c.find({"\\x5f_proto__": {}})',
    'db.c.find({"\\u{5f}_proto__": {}})',
    "db.c.find({'\\u{5F}\\u{5F}proto__': {}})",
    'db.c.find({"constr\\u{75}ctor": 1})',
    'db.c.find({"constr\\x75ctor": 1})',
    'db.c.find({"proto\\\ntype": 1})',
    'db.c.find({"\\q": 1})',
    'db.c.find({"\\u{110000}": 1})',
    'db.c.find({"\\01": 1})',
  ])("still blocks %s", async (query) => {
    const { driver, find } = harness();
    await expect(driver.query(query)).rejects.toThrow(
      /Unsafe mongosh query blocked/,
    );
    expect(find).not.toHaveBeenCalled();
  });
});
