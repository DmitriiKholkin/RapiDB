import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function harness() {
  const toArray = vi.fn(async () => []);
  const cursor = { toArray, sort: vi.fn(), skip: vi.fn(), limit: vi.fn() };
  cursor.sort.mockReturnValue(cursor);
  cursor.skip.mockReturnValue(cursor);
  cursor.limit.mockReturnValue(cursor);
  const collection = {
    aggregate: vi.fn(() => cursor),
    find: vi.fn(() => cursor),
    deleteMany: vi.fn(async () => ({ deletedCount: 1 })),
  };
  const createCollection = vi.fn(async () => ({}));
  const driver = new MongoDBDriver({
    id: "cursor-safety",
    name: "audit",
    type: "mongodb",
  });
  Object.assign(driver, {
    requireDb: () => ({ collection: () => collection, createCollection }),
  });
  return { driver, collection, cursor, createCollection };
}

describe("MongoDB cursor modifiers cannot silently change execution semantics", () => {
  it.each([
    "explain()",
    'explain("executionStats")',
    "close()",
    'collation({locale:"en",strength:2})',
    "sort({amount:-1})",
    "unknownModifier()",
  ])("never executes a write pipeline with the unsupported modifier %s", async (modifier) => {
    const h = harness();
    await expect(
      h.driver.query(`db.items.aggregate([{$out:"backup"}]).${modifier}`),
    ).rejects.toThrow(/modifier.*not supported/i);
    expect(h.collection.aggregate).not.toHaveBeenCalled();
    expect(h.cursor.toArray).not.toHaveBeenCalled();
  });

  it("rejects unsupported cursor modifiers before a preceding delete", async () => {
    const h = harness();
    await expect(
      h.driver.query("db.items.deleteMany({}); db.items.find({}).explain()"),
    ).rejects.toThrow(/modifier.*not supported/i);
    expect(h.collection.deleteMany).not.toHaveBeenCalled();
    expect(h.collection.find).not.toHaveBeenCalled();
  });

  it.each([
    "toArray(1)",
    "pretty(1)",
  ])("does not ignore arguments to %s", async (modifier) => {
    const h = harness();
    await expect(
      h.driver.query(`db.items.find({}).${modifier}`),
    ).rejects.toThrow(/does not accept arguments/i);
    expect(h.collection.find).not.toHaveBeenCalled();
  });

  it("retains supported cursor reads", async () => {
    const h = harness();
    await h.driver.query(
      "db.items.find({}).sort({amount:1}).skip(2).limit(3).toArray()",
    );
    expect(h.cursor.sort).toHaveBeenCalledWith({ amount: 1 });
    expect(h.cursor.skip).toHaveBeenCalledWith(2);
    expect(h.cursor.limit).toHaveBeenCalledWith(3);
    expect(h.cursor.toArray).toHaveBeenCalledOnce();
  });

  it.each([
    "skip(-1)",
    "skip(1.5)",
    "skip()",
    "skip(1, 2)",
  ])("rejects invalid %s before sending operations", async (modifier) => {
    const h = harness();
    await expect(
      h.driver.query(`db.items.find({}).${modifier}`),
    ).rejects.toThrow(/skip/);
    expect(h.collection.find).not.toHaveBeenCalled();
  });

  it("uses the last skip() value rather than silently keeping an earlier offset", async () => {
    const h = harness();
    await h.driver.query("db.items.find({}).skip(1).skip(5)");
    expect(h.cursor.skip).toHaveBeenCalledWith(5);
  });
});

describe("MongoDB view definitions must not silently lose their filter", () => {
  it.each([
    'db.createView("public_items", "private_items", {$match:{public:true}})',
    'db.createView("public_items", "private_items", null)',
    'db.createView("public_items", "private_items")',
    'db.createView("public_items", "private_items", "invalid")',
  ])("rejects invalid view pipelines before any request: %s", async (query) => {
    const h = harness();
    await expect(h.driver.query(query)).rejects.toThrow(/pipeline array/i);
    expect(h.createCollection).not.toHaveBeenCalled();
  });

  it("validates a later view definition before an earlier destructive operation", async () => {
    const h = harness();
    await expect(
      h.driver.query(
        'db.items.deleteMany({}); db.createView("v", "items", {})',
      ),
    ).rejects.toThrow(/pipeline array/i);
    expect(h.collection.deleteMany).not.toHaveBeenCalled();
    expect(h.createCollection).not.toHaveBeenCalled();
  });

  it("retains explicit empty and filtered view pipelines", async () => {
    const h = harness();
    await h.driver.query('db.createView("all_items", "items", [])');
    await h.driver.query(
      'db.createView("public_items", "items", [{$match:{public:true}}])',
    );
    expect(h.createCollection).toHaveBeenNthCalledWith(1, "all_items", {
      viewOn: "items",
      pipeline: [],
    });
    expect(h.createCollection).toHaveBeenNthCalledWith(2, "public_items", {
      viewOn: "items",
      pipeline: [{ $match: { public: true } }],
    });
  });

  it.each([
    'db.createView("v", "items", [], {pipeline:[]})',
    'db.createView("v", "items", [], {viewOn:"private_items"})',
    'db.createView("v", "items", [], [])',
    'db.createView("", "items", [])',
    'db.createView("v", 42, [])',
    'db.createView("v", "items", [], {}, 1)',
  ])("rejects ambiguous view definitions: %s", async (query) => {
    const h = harness();
    await expect(h.driver.query(query)).rejects.toThrow(/createView/);
    expect(h.createCollection).not.toHaveBeenCalled();
  });
});

it.skipIf(!process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL)(
  "does not create an unfiltered view on a real MongoDB server",
  async () => {
    const uri = process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL ?? "";
    const database = `rapidb_view_audit_${randomUUID().replaceAll("-", "")}`;
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
      const db = client.db(database);
      await db
        .collection("items")
        .insertMany([{ public: true }, { secret: true }]);
      await expect(
        driver.query(
          'db.createView("public_items", "items", {$match:{public:true}})',
        ),
      ).rejects.toThrow(/pipeline array/);
      expect(
        await db.listCollections({ name: "public_items" }).toArray(),
      ).toEqual([]);
      await driver.query(
        'db.createView("public_items", "items", [{$match:{public:true}}])',
      );
      expect(await db.collection("public_items").countDocuments({})).toBe(1);
      expect(
        await db.collection("public_items").countDocuments({ secret: true }),
      ).toBe(0);
    } finally {
      await driver.disconnect();
      await client.db(database).dropDatabase();
      await client.close();
    }
  },
);
