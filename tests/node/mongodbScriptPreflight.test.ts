import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function harness() {
  const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const find = vi.fn();
  const aggregate = vi.fn();
  const driver = new MongoDBDriver({
    id: "preflight",
    name: "Preflight",
    type: "mongodb",
  });
  Object.assign(driver, {
    connected: true,
    client: {
      db: () => ({ collection: () => ({ deleteMany, find, aggregate }) }),
    },
  });
  return { driver, deleteMany, find, aggregate };
}

describe("MongoDB validates the whole script before destructive statements", () => {
  it.each([
    ["db.items.find({}).limit(-1)", /limit/],
    ["db.items.find({}).limit(1.5)", /limit/],
    ["db.items.find({}).limit()", /limit/],
    ["db.items.aggregate([]).limit(0)", /positive/],
    ["db.items.find({}, 42)", /projection/],
    ["db.items.find({}).sort(42)", /sort/],
    ["db.items.aggregate({})", /pipeline/],
    ["db.items.drop(true)", /arguments/],
    ["db.getCollectionNames(42)", /arguments/],
    ["db.items.unsupportedOperation()", /Unsupported/],
  ])("rejects a later local validation error: %s", async (query, reason) => {
    const { driver, deleteMany, find, aggregate } = harness();
    await expect(
      driver.query(`db.items.deleteMany({}); ${query}`),
    ).rejects.toThrow(reason);
    expect(deleteMany).not.toHaveBeenCalled();
    expect(find).not.toHaveBeenCalled();
    expect(aggregate).not.toHaveBeenCalled();
  });

  it("keeps valid scripts sequential and does not claim server errors roll back earlier writes", async () => {
    const { driver, deleteMany, find } = harness();
    const cursor = {
      skip: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      toArray: vi.fn(async () => [{ _id: "remaining" }]),
    };
    find.mockImplementation(() => {
      expect(deleteMany).toHaveBeenCalledOnce();
      return cursor;
    });
    const result = await driver.query(
      'db.items.deleteMany({_id:"remove"}); db.items.find({}).limit(1)',
    );
    expect(result.affectedRows).toBe(1);
    expect(cursor.limit).toHaveBeenCalledWith(1);
    find.mockImplementation(() => {
      throw new Error("Server unavailable");
    });
    await expect(
      driver.query('db.items.deleteMany({_id:"remove"}); db.items.find({})'),
    ).rejects.toThrow("Server unavailable");
    expect(deleteMany).toHaveBeenCalledTimes(2);
  });
});

it.skipIf(!process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL)(
  "preserves real MongoDB documents when a later cursor limit is invalid",
  async () => {
    const uri = process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL;
    const database = `rapidb_preflight_${randomUUID()}`;
    const client = new MongoClient(uri ?? "");
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
      await client.db(database).collection("items").insertOne({ keep: true });
      await expect(
        driver.query("db.items.deleteMany({}); db.items.find({}).limit(-1)"),
      ).rejects.toThrow(/limit/);
      expect(
        await client
          .db(database)
          .collection("items")
          .countDocuments({ keep: true }),
      ).toBe(1);
    } finally {
      await driver.disconnect();
      await client.db(database).dropDatabase();
      await client.close();
    }
  },
);
