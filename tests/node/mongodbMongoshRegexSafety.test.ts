import { randomUUID } from "node:crypto";
import { BSON, BSONRegExp, MongoClient } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function harness() {
  const deleteMany = vi.fn(async (_filter: Record<string, unknown>) => ({
    deletedCount: 1,
  }));
  const insertOne = vi.fn(async (_document: Record<string, unknown>) => ({
    acknowledged: true,
    insertedId: 1,
  }));
  const command = vi.fn(async (_input: Record<string, unknown>) => ({ ok: 1 }));
  const driver = new MongoDBDriver({
    id: "regex-safety",
    name: "Regex safety",
    type: "mongodb",
  });
  Object.assign(driver, {
    client: {
      db: () => ({ collection: () => ({ deleteMany, insertOne }), command }),
    },
    connected: true,
  });
  return { driver, deleteMany, insertOne, command };
}

describe("MongoDB mongosh regex wire semantics", () => {
  it.each([
    "",
    "i",
    "m",
    "s",
    "u",
    "imsu",
  ])("preserves supported regex flags in BSON: %s", async (flags) => {
    const { driver, deleteMany } = harness();
    await driver.query(`db.items.deleteMany({name: /^a.b$/${flags}})`);
    const filter = deleteMany.mock.calls[0][0];
    const decoded = BSON.deserialize(BSON.serialize(filter), {
      bsonRegExp: true,
    });
    expect(decoded.name).toEqual(new BSONRegExp("^a.b$", flags));
  });

  it.each([
    "g",
    "y",
    "d",
    "v",
    "gi",
    "sy",
  ])("rejects unsupported regex flag %s before any script operation", async (flags) => {
    const { driver, deleteMany, insertOne } = harness();
    await expect(
      driver.query(
        `db.items.insertOne({keep:true}); db.items.deleteMany({name: new RegExp("^a.b$", "${flags}")})`,
      ),
    ).rejects.toThrow(/regex.*flag/i);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteMany).not.toHaveBeenCalled();
  });

  it("preserves nested constructor regex filters", async () => {
    const { driver, deleteMany } = harness();
    await driver.query(
      'db.items.deleteMany({$or:[{name:RegExp("^a.b$", "s")}]})',
    );
    const decoded = BSON.deserialize(
      BSON.serialize(deleteMany.mock.calls[0][0]),
      { bsonRegExp: true },
    );
    expect(decoded.$or[0].name).toEqual(new BSONRegExp("^a.b$", "s"));
  });

  it("preserves regexes in native commands and inserted documents", async () => {
    const { driver, command, insertOne } = harness();
    await driver.query(
      'db.runCommand({delete:"items", deletes:[{q:{name:/^a.b$/s},limit:0}]}); db.items.insertOne({pattern:/^a.b$/s})',
    );
    const decodedCommand = BSON.deserialize(
      BSON.serialize(command.mock.calls[0][0]),
      { bsonRegExp: true },
    );
    const decodedDocument = BSON.deserialize(
      BSON.serialize(insertOne.mock.calls[0][0]),
      { bsonRegExp: true },
    );
    expect(decodedCommand.deletes[0].q.name).toEqual(
      new BSONRegExp("^a.b$", "s"),
    );
    expect(decodedDocument.pattern).toEqual(new BSONRegExp("^a.b$", "s"));
  });

  it.each([
    ["RegExp(/^a.b$/s)", "^a.b$", "s"],
    ['new RegExp(/^a.b$/s, "i")', "^a.b$", "i"],
    ["RegExp()", "(?:)", ""],
    ["RegExp(undefined)", "(?:)", ""],
  ])("retains constructor semantics: %s", async (expression, pattern, flags) => {
    const { driver, deleteMany } = harness();
    await driver.query(`db.items.deleteMany({name:${expression}})`);
    expect(deleteMany.mock.calls[0][0].name).toEqual(
      new BSONRegExp(pattern, flags),
    );
  });

  it.each([
    ['RegExp("a\\u0000b")', /null bytes/],
    ['RegExp("a", "i", "ignored")', /optional flags only/],
  ])("prevalidates unrepresentable regexes: %s", async (expression, reason) => {
    const { driver, insertOne, deleteMany } = harness();
    await expect(
      driver.query(
        `db.items.insertOne({keep:true}); db.items.deleteMany({name:${expression}})`,
      ),
    ).rejects.toThrow(reason);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteMany).not.toHaveBeenCalled();
  });
});

const mongoUri =
  process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL ??
  (process.env.RAPIDB_LIVE_NOSQL === "1"
    ? "mongodb://mongo_admin:mongo_pass123@127.0.0.1:27017/admin"
    : undefined);

it.skipIf(!mongoUri)(
  "never broadens a real MongoDB delete through the BSON g-to-s conversion",
  async () => {
    const uri = mongoUri ?? "";
    const database = `rapidb_regex_${randomUUID().replaceAll("-", "")}`;
    const client = new MongoClient(uri);
    const driver = new MongoDBDriver({
      id: database,
      name: database,
      type: "mongodb",
      connectionUri: uri,
      database,
    });
    try {
      await client.connect();
      await driver.connect();
      const items = client.db(database).collection("items");
      await items.insertMany([
        { name: "a b" },
        { name: "a\nb" },
        { name: "keep" },
      ]);
      await expect(
        driver.query("db.items.deleteMany({name:/^a.b$/g})"),
      ).rejects.toThrow(/regex.*flag/i);
      expect(await items.countDocuments({})).toBe(3);
      await driver.query("db.items.deleteMany({name:/^a.b$/})");
      expect(await items.countDocuments({ name: "a\nb" })).toBe(1);
      await driver.query('db.items.deleteMany({name:RegExp("^a.b$", "s")})');
      expect(await items.distinct("name")).toEqual(["keep"]);
    } finally {
      await driver.disconnect();
      await client.db(database).dropDatabase();
      await client.close();
    }
  },
);
