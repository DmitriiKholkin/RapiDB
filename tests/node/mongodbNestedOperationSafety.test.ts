import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function setup() {
  const cursor = {
    sort: vi.fn().mockReturnThis(),
    skip: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    toArray: vi.fn().mockResolvedValue([{ _id: "one" }]),
  };
  const collection = {
    find: vi.fn(() => cursor),
    findOne: vi.fn().mockResolvedValue({ _id: "one" }),
    insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
    insertMany: vi.fn().mockResolvedValue({ acknowledged: true }),
    deleteOne: vi.fn().mockResolvedValue({ deletedCount: 1 }),
    deleteMany: vi.fn().mockResolvedValue({ deletedCount: 10 }),
    updateOne: vi.fn().mockResolvedValue({ modifiedCount: 1 }),
    updateMany: vi.fn().mockResolvedValue({ modifiedCount: 10 }),
    aggregate: vi.fn(() => cursor),
  };
  const driver = new MongoDBDriver({
    id: "mongo-nested-operation-safety",
    name: "Mongo nested operation safety",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => collection }) },
  });
  return { driver, collection, cursor };
}

describe("MongoDB mongosh operation results cannot be used as data", () => {
  it.each([
    "db.users.deleteMany(db.users.findOne({name: 'x'}))",
    "db.users.deleteOne(db.users.find({name: 'x'}).limit(1))",
    "db.users.updateMany(db.users.findOne({name: 'x'}), {$set: {active: false}})",
    "db.users.updateMany({}, {$set: {active: db.users.findOne({name: 'x'})}})",
    "db.users.insertOne(db.users.findOne({name: 'x'}))",
    "db.users.insertOne({copy: db.users.findOne({name: 'x'})})",
    "db.users.insertMany([db.users.findOne({name: 'x'})])",
    "db.users.insertOne({nested: [{deep: db.users.find({}).sort({_id: 1})}]})",
    "db.users.deleteMany({$or: [db.users.findOne({name: 'x'})]})",
    "db.users.insertOne({value: db.users})",
    "db.users.deleteMany(db.getSiblingDB('other'))",
    "db.users.insertOne({cmd: db.runCommand({ping: 1})})",
    "const doc = db.users.findOne({name: 'x'}); db.users.deleteMany(doc)",
    "db.users.deleteMany({...db.users.findOne({name: 'x'})})",
    "db.users.deleteMany(Object.assign({}, db.users.findOne({name: 'x'})))",
    "db.users.insertOne({keys: Object.keys(db.users)})",
  ])("rejects %s before sending any request", async (query) => {
    const { driver, collection, cursor } = setup();
    await expect(
      driver.query(`db.users.insertOne({_id: "earlier"}); ${query}`),
    ).rejects.toThrow(/operation results.*cannot be used as values/i);
    for (const method of [
      ...Object.values(collection),
      ...Object.values(cursor),
    ]) {
      expect(method).not.toHaveBeenCalled();
    }
  });

  it("keeps supported standalone operations and cursor chains working", async () => {
    const { driver, collection, cursor } = setup();
    await driver.query("db.users.find({active: true}).sort({_id: 1}).limit(5)");
    expect(collection.find).toHaveBeenCalledWith(
      { active: true },
      expect.anything(),
    );
    expect(cursor.sort).toHaveBeenCalledWith({ _id: 1 });
    expect(cursor.limit).toHaveBeenCalledWith(5);
    expect(cursor.toArray).toHaveBeenCalled();

    await driver.query(
      "db.users.insertOne({_id: 'two', tags: ['a']}); db.users.deleteMany({name: 'x'})",
    );
    expect(collection.insertOne.mock.calls[0]?.[0]).toEqual({
      _id: "two",
      tags: ["a"],
    });
    expect(collection.deleteMany.mock.calls[0]?.[0]).toEqual({ name: "x" });
  });
});
