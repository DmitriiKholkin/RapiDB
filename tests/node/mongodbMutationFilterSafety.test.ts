import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function setup() {
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 10 });
  const updateOne = vi.fn().mockResolvedValue({ modifiedCount: 1 });
  const updateMany = vi.fn().mockResolvedValue({ modifiedCount: 10 });
  const collection = {
    insertOne,
    deleteOne,
    deleteMany,
    updateOne,
    updateMany,
  };
  const driver = new MongoDBDriver({
    id: "mongo-filter-safety",
    name: "Mongo filter safety",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => collection }) },
  });
  return { driver, collection };
}

describe("MongoDB mutations require an explicit document filter", () => {
  it.each([
    "deleteOne",
    "deleteMany",
    "updateOne",
    "updateMany",
  ])("rejects invalid %s filters before sending any statement", async (operation) => {
    for (const filter of [
      "",
      "undefined",
      "null",
      "false",
      "42",
      '""',
      '"id"',
      "[]",
      '[{_id: "one"}]',
      "/one/",
      'ObjectId("507f1f77bcf86cd799439011")',
      'ISODate("2026-01-01T00:00:00Z")',
      "NumberInt(1)",
    ]) {
      const { driver, collection } = setup();
      const args = operation.startsWith("update")
        ? `${filter || "undefined"}, {$set: {active: false}}`
        : filter;
      await expect(
        driver.query(
          `db.items.insertOne({_id: "earlier"}); db.items.${operation}(${args})`,
        ),
      ).rejects.toThrow(/filter.*document/i);
      for (const method of Object.values(collection)) {
        expect(method).not.toHaveBeenCalled();
      }
    }
  });

  it.each([
    ["deleteOne", "{}"],
    ["deleteMany", "{}"],
    ["updateOne", "{}, {$set: {active: false}}"],
    ["updateMany", "{}, {$set: {active: false}}"],
    ["insertOne", '{_id: "one"}'],
    ["insertMany", '[{_id: "one"}, {_id: "two"}]'],
    ["drop", ""],
  ])("rejects cursor modifiers on %s rather than ignoring a requested write limit", async (operation, args) => {
    for (const modifier of [
      "limit(1)",
      "skip(1)",
      "sort({_id: 1})",
      "typo()",
    ]) {
      const { driver, collection } = setup();
      await expect(
        driver.query(
          `db.items.insertOne({_id: "earlier"}); db.items.${operation}(${args}).${modifier}`,
        ),
      ).rejects.toThrow(/cursor|modifier/i);
      for (const method of Object.values(collection)) {
        expect(method).not.toHaveBeenCalled();
      }
    }
  });

  it.each([
    "deleteOne",
    "deleteMany",
    "updateOne",
    "updateMany",
  ])("preserves explicit empty and selective filters for %s", async (operation) => {
    for (const filter of [{}, { _id: "one" }, { active: { $eq: false } }]) {
      const { driver, collection } = setup();
      const args = operation.startsWith("update")
        ? `${JSON.stringify(filter)}, {$set: {active: false}}`
        : JSON.stringify(filter);
      await driver.query(`db.items.${operation}(${args})`);
      expect(
        collection[operation as keyof typeof collection],
      ).toHaveBeenCalledWith(
        filter,
        ...(operation.startsWith("update")
          ? [{ $set: { active: false } }]
          : []),
      );
    }
  });
});
