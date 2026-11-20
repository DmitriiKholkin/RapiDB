import {
  BSON,
  BSONRegExp,
  Code,
  DBRef,
  type Document,
  Double,
  Int32,
  Long,
  MongoOperationTimeoutError,
  ObjectId,
  Timestamp,
} from "mongodb";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import {
  colKey,
  type DriverTablePageRequest,
} from "../../src/extension/dbDrivers/types";
import { startMongoWireFixture } from "../support/mongodbWireFixture";

const textDocument = { _id: "text", value: "no numbers" };
const bsonDocument = {
  _id: "bson",
  smallLong: Long.fromNumber(7),
  largeLong: Long.fromString("9007199254740993"),
  int: new Int32(7),
  double: new Double(7),
  regex: new BSONRegExp("a b", "ix"),
  reference: new DBRef("other", new ObjectId("507f1f77bcf86cd799439011")),
  "literal.field": "client-side sort",
};
const extendedJson = BSON.EJSON.serialize(bsonDocument, { relaxed: false });
const regexDocument = {
  _id: "regex",
  pattern: new BSONRegExp("(?>red|blue)", "imsux"),
  variants: [{ pattern: new BSONRegExp("a.b", "su") }],
  scoped: new Code("return sample", {
    sample: [new BSONRegExp("(?>red|blue)", "ix")],
    score: new Double(1.5),
  }),
};

const tablePage = {
  database: "db",
  schema: "db",
  table: "items",
  page: 1,
  pageSize: 10,
  filters: [],
  sort: null,
  skipCount: false,
} satisfies DriverTablePageRequest;

const countReplies = {
  int32Count: new Int32(1),
  safeLimit: Long.fromString("9007199254740991"),
  unsafeCount: Long.fromString("9007199254740992"),
  stringCount: "1",
  negativeCount: new Int32(-1),
  fractionalCount: new Double(1.5),
  timestampCount: new Timestamp({ t: 0, i: 1 }),
};

function respond(command: Document): Document {
  if (command.ping || command.drop) return { ok: 1, ...bsonDocument };
  const batch = (collection: string, firstBatch: Document[]) => ({
    ok: 1,
    cursor: { id: Long.ZERO, ns: `${command.$db}.${collection}`, firstBatch },
  });
  if (command.listCollections) {
    return batch("$cmd.listCollections", [
      { name: "items", type: "collection" },
    ]);
  }
  if (command.find || command.aggregate) {
    const collection = command.find ?? command.aggregate;
    const lastStage = command.pipeline?.at(-1);
    if (lastStage?.$project?.t0) {
      // A DBRef decodes as a DBRef on the client, but $type reports "object".
      return batch(collection, [{ _id: bsonDocument._id, t0: "object" }]);
    }
    if (collection === "empty") return batch(collection, []);
    if (lastStage?.$group) {
      return batch(collection, [
        {
          n: countReplies[collection as keyof typeof countReplies] ?? Long.ONE,
        },
      ]);
    }
    if (collection === "regexes") return batch(collection, [regexDocument]);
    return batch(collection, [
      collection === "text" ? textDocument : bsonDocument,
    ]);
  }
  return { ok: 0, errmsg: "Unexpected command in MongoDB test fixture" };
}

function createDriver(port: number, options: string) {
  return new MongoDBDriver({
    id: "mongo-bson-options",
    name: "MongoDB BSON options",
    type: "mongodb",
    mongoConnectionMode: "uri",
    connectionUri: `mongodb://127.0.0.1:${port}/db?directConnection=true${options}`,
  });
}

describe("MongoDB URI options with native BSON reads", () => {
  let server: Awaited<ReturnType<typeof startMongoWireFixture>>;
  let driver: MongoDBDriver;
  let stalledReads = 0;
  let countReads = 0;

  beforeAll(async () => {
    server = await startMongoWireFixture((command) => {
      const isCount = Boolean(command.pipeline?.at(-1)?.$group);
      if (isCount) countReads++;
      if (
        command.find === "stalled" ||
        (isCount && command.aggregate === "stalledCount")
      ) {
        stalledReads++;
        return undefined;
      }
      return respond(command);
    });
    driver = createDriver(server.port, "&useBigInt64=true");
    await driver.connect();
  });

  afterAll(async () => {
    try {
      await driver?.disconnect();
    } finally {
      await server?.close();
    }
  });

  it.each([
    "find({})",
    "findOne({})",
    "aggregate([])",
  ])("decodes %s results even when there are no numeric fields", async (helper) => {
    const result = await driver.query(`db.text.${helper}`);
    expect(result.rows).toEqual([
      { [colKey(0)]: "text", [colKey(1)]: "no numbers" },
    ]);
  });

  it.each([
    "db.items.find({})",
    "db.items.aggregate([])",
    "db.runCommand({ ping: 1 })",
    "db.adminCommand({ ping: 1 })",
    "db.items.drop()",
  ])("preserves raw BSON for JSON export from %s", async (query) => {
    const result = await driver.query(query);
    const entries = result.columns.map((name, index): [string, unknown] => [
      name,
      result.exportRows?.[0]?.[colKey(index)],
    ]);
    expect(Object.fromEntries(entries)).toMatchObject(bsonDocument);
    const serialize = result.serializeJsonExportRow;
    expect.assert(serialize);
    expect(JSON.parse(serialize(entries))).toMatchObject(extendedJson);
  });

  it("renders server PCRE and BSON options consistently in command and find replies", async () => {
    const command = await driver.query('db.runCommand({ find: "regexes" })');
    const find = await driver.query("db.regexes.find({})");
    const value = (result: typeof find, field: string) =>
      result.rows[0][colKey(result.columns.indexOf(field))];
    const { firstBatch } = JSON.parse(String(value(command, "cursor")));
    const variants = [{ pattern: "/a.b/su" }];
    const scoped = JSON.stringify({
      code: "return sample",
      scope: { sample: ["/(?>red|blue)/ix"], score: 1.5 },
    });
    expect(firstBatch).toEqual([
      {
        _id: "regex",
        pattern: "/(?>red|blue)/imsux",
        variants,
        scoped,
      },
    ]);
    expect(value(find, "pattern")).toBe(firstBatch[0].pattern);
    expect(value(find, "variants")).toBe(JSON.stringify(variants));
    expect(value(find, "scoped")).toBe(scoped);
  });

  it.each([
    null,
    { column: "literal.field", direction: "asc" } as const,
  ])("preserves table schema, DBRef detection, and JSON export with sort %j", async (sort) => {
    const page = await driver.readTablePage({
      ...tablePage,
      sort,
      skipCount: true,
    });
    expect(page.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "smallLong", nativeType: "long" }),
        expect.objectContaining({ name: "int", nativeType: "int" }),
        expect.objectContaining({ name: "double", nativeType: "double" }),
        expect.objectContaining({ name: "reference", nativeType: "dbRef" }),
      ]),
    );
    expect(page.mongoValueTypes?.[0].reference.nativeType).toBe("dbRef");
    expect(page.rows[0].largeLong).toBe("9007199254740993");
    expect(driver.formatTableExportChunk(page).rows).toEqual([extendedJson]);
  });

  it("leaves useBigInt64 enabled on the SDK's non-BSON count path", async () => {
    const result = await driver.query("db.items.countDocuments({})");
    expect(result.rows).toEqual([{ [colKey(0)]: 1n }]);
  });

  it.each([
    ["promoted number", "", "int32Count", 1],
    ["BSON Int32", "&promoteValues=false", "int32Count", new Int32(1)],
    ["BSON Long", "&promoteValues=false", "items", Long.ONE],
    ["native bigint", "&useBigInt64=true", "items", 1n],
  ])("normalizes %s only for table pagination", async (_name, options, table, rawCount) => {
    const countDriver = createDriver(server.port, options);
    try {
      await countDriver.connect();
      expect(
        (await countDriver.query(`db.${table}.countDocuments({})`)).rows,
      ).toStrictEqual([{ [colKey(0)]: rawCount }]);
      const page = await countDriver.readTablePage({ ...tablePage, table });
      expect(page.totalCount).toBe(1);
      expect(page.rows).toEqual([expect.objectContaining({ _id: "bson" })]);
    } finally {
      await countDriver.disconnect();
    }
  });

  it("accepts the largest safe count and the SDK's empty-collection zero", async () => {
    expect(
      (await driver.readTablePage({ ...tablePage, table: "safeLimit" }))
        .totalCount,
    ).toBe(Number.MAX_SAFE_INTEGER);
    expect(
      await driver.readTablePage({ ...tablePage, table: "empty" }),
    ).toMatchObject({
      rows: [],
      totalCount: 0,
    });
  });

  it.each([
    "unsafeCount",
    "stringCount",
    "negativeCount",
    "fractionalCount",
    "timestampCount",
  ])("rejects an invalid pagination count (%s) instead of reporting an exact total", async (table) => {
    await expect(driver.readTablePage({ ...tablePage, table })).rejects.toThrow(
      /MongoDB.*count.*non-negative safe integer/i,
    );
  });

  it("does not send a count when skipCount is set", async () => {
    const before = countReads;
    expect(
      await driver.readTablePage({
        ...tablePage,
        table: "unsafeCount",
        skipCount: true,
      }),
    ).toMatchObject({
      totalCount: 0,
      rows: [expect.objectContaining({ _id: "bson" })],
    });
    expect(countReads).toBe(before);
  });

  it("passes cancellation through to the table count", async () => {
    const controller = new AbortController();
    const before = stalledReads;
    const result = driver.readTablePage({
      ...tablePage,
      table: "stalledCount",
      signal: controller.signal,
      deadline: Date.now() + 2_000,
    });
    try {
      await vi.waitFor(() => expect(stalledReads).toBe(before + 1));
    } finally {
      controller.abort();
      await expect(result).rejects.toBe(controller.signal.reason);
    }
  });

  it("passes cancellation through to a pending SDK read", async () => {
    const controller = new AbortController();
    const before = stalledReads;
    const result = driver.query("db.stalled.find({})", undefined, {
      signal: controller.signal,
      deadline: Date.now() + 2_000,
    });
    try {
      await vi.waitFor(() => expect(stalledReads).toBe(before + 1));
    } finally {
      controller.abort();
      await expect(result).rejects.toBe(controller.signal.reason);
    }
  });

  it("passes the deadline through to a pending SDK read", async () => {
    await expect(
      driver.query("db.stalled.find({})", undefined, {
        deadline: Date.now() + 200,
      }),
    ).rejects.toBeInstanceOf(MongoOperationTimeoutError);
  });
});
