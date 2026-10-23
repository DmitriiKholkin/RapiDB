import { Timestamp } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function setup() {
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const driver = new MongoDBDriver({
    id: "mongo-timestamps",
    name: "Mongo timestamps",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ insertOne, deleteOne }) }) },
  });
  return { driver, insertOne, deleteOne };
}

describe("mongosh BSON timestamps preserve document identity", () => {
  it.each([
    "Timestamp(1.5, 0)",
    "Timestamp(1, 0.5)",
    "Timestamp({t: 1.5, i: 0})",
    "Timestamp({t: 1, i: 0.5})",
    "Timestamp(null, 0)",
    "Timestamp(true, 0)",
    'Timestamp("1", 0)',
    "Timestamp([], 0)",
    "Timestamp({})",
    "Timestamp({t: 1})",
    "Timestamp({i: 1})",
    "Timestamp({t: 1, i: null})",
    "Timestamp(1, null)",
    "Timestamp(1, 0, 2)",
    "Timestamp({t: 1, i: 0}, 2)",
    "Timestamp(-1, 0)",
    "Timestamp(0, -1)",
    "Timestamp(4294967296, 0)",
    "Timestamp(0, 4294967296)",
    "Timestamp()",
  ])("rejects %s before submitting any writes", async (literal) => {
    const { driver, insertOne, deleteOne } = setup();
    await expect(
      driver.query(
        `db.items.insertOne({_id: "earlier"}); db.items.deleteOne({_id: ${literal}})`,
      ),
    ).rejects.toThrow(/Timestamp.*integer|Timestamp.*argument/i);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteOne).not.toHaveBeenCalled();
  });

  it.each([
    ["Timestamp(0, 0)", 0, 0],
    ["Timestamp(4294967295, 4294967295)", 4294967295, 4294967295],
    ["Timestamp({t: 1720094400, i: 42})", 1720094400, 42],
    ["new Timestamp(1720094400, 1)", 1720094400, 1],
    ["Timestamp(42)", 42, 0],
  ])("preserves %s exactly", async (literal, seconds, increment) => {
    const { driver, deleteOne } = setup();
    await driver.query(`db.items.deleteOne({_id: ${literal}})`);
    const timestamp = deleteOne.mock.calls[0][0]._id;
    expect(timestamp).toBeInstanceOf(Timestamp);
    expect(timestamp.getHighBitsUnsigned()).toBe(seconds);
    expect(timestamp.getLowBitsUnsigned()).toBe(increment);
  });
});
