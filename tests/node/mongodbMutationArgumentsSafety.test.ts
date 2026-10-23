import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function setup() {
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 2 });
  const updateOne = vi.fn().mockResolvedValue({ modifiedCount: 1 });
  const updateMany = vi.fn().mockResolvedValue({ modifiedCount: 2 });
  const insertMany = vi.fn().mockResolvedValue({ insertedCount: 2 });
  const collection = {
    insertOne,
    insertMany,
    deleteOne,
    deleteMany,
    updateOne,
    updateMany,
  };
  const driver = new MongoDBDriver({
    id: "mongo-arguments",
    name: "Mongo arguments",
    type: "mongodb",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => collection }) },
  });
  return { driver, collection };
}

describe("MongoDB mutation argument safety", () => {
  it.each([
    'deleteOne({name: "ALICE"}, {collation: {locale: "simple"}})',
    'deleteMany({name: "ALICE"}, {collation: {locale: "simple"}})',
    'updateOne({_id: "one"}, {$set: {active: true}}, {upsert: true})',
    'updateMany({}, {$set: {active: true}}, {collation: {locale: "en"}})',
    'insertOne({_id: "one"}, {bypassDocumentValidation: true})',
    'insertMany([{_id: "one"}], {ordered: false})',
  ])("rejects unsupported options before any statement: %s", async (expression) => {
    const { driver, collection } = setup();
    await expect(
      driver.query(
        `db.items.insertOne({_id: "earlier"}); db.items.${expression}`,
      ),
    ).rejects.toThrow(/options.*not supported|unsupported.*arguments/i);
    for (const method of Object.values(collection)) {
      expect(method).not.toHaveBeenCalled();
    }
  });
});

const liveUri = process.env.RAPIDB_MONGO_ARGUMENTS_AUDIT_URI;
describe.skipIf(!liveUri)("MongoDB mutation options on a real server", () => {
  it("does not broaden a case-sensitive delete under a case-insensitive collection default", async () => {
    const database = `rapidb_arguments_${randomUUID().replaceAll("-", "")}`;
    const client = new MongoClient(liveUri as string);
    const driver = new MongoDBDriver({
      id: database,
      name: database,
      type: "mongodb",
      connectionUri: liveUri,
      database,
    });
    try {
      await client.connect();
      const collection = await client.db(database).createCollection("items", {
        collation: { locale: "en", strength: 2 },
      });
      await collection.insertMany([{ name: "Alice" }, { name: "ALICE" }]);
      expect(await collection.countDocuments({ name: "ALICE" })).toBe(2);
      expect(
        await collection.countDocuments(
          { name: "ALICE" },
          {
            collation: { locale: "simple" },
          },
        ),
      ).toBe(1);
      await driver.connect();
      await expect(
        driver.query(
          'db.items.deleteMany({name: "ALICE"}, {collation: {locale: "simple"}})',
        ),
      ).rejects.toThrow(/options are not supported/);
      expect(await collection.countDocuments({})).toBe(2);
      // The explicit command path preserves the requested server semantics.
      await driver.query(
        'db.runCommand({delete: "items", deletes: [{q: {name: "ALICE"}, limit: 0, collation: {locale: "simple"}}]})',
      );
      expect(await collection.find({}).toArray()).toEqual([
        expect.objectContaining({ name: "Alice" }),
      ]);
    } finally {
      await driver.disconnect();
      await client.db(database).dropDatabase();
      await client.close();
    }
  });
});
