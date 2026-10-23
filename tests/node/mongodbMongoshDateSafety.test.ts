import { randomUUID } from "node:crypto";
import { BSON, MongoClient } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function setup() {
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const driver = new MongoDBDriver({
    id: "mongo-dates",
    name: "Mongo dates",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ insertOne, deleteOne }) }) },
  });
  return { driver, insertOne, deleteOne };
}

describe("mongosh dates preserve persisted values and document identity", () => {
  it.each([
    'ISODate("not a date")',
    'new Date("")',
    "new Date(undefined)",
    "new Date(8640000000000001)",
    "new Date(-8640000000000001)",
    "new Date(2026, 0, 1, 0, 0, 0, 0, 42)",
    "new Date(1.5)",
    "new Date(2026, 0.5, 1)",
    "new Date(true)",
    "new Date(null)",
    "new Date([])",
    "new Date({})",
  ])("rejects %s before sending any operation in the script", async (literal) => {
    const { driver, insertOne, deleteOne } = setup();
    await expect(
      driver.query(
        `db.items.insertOne({_id: "earlier"}); db.items.deleteOne({_id: ${literal}})`,
      ),
    ).rejects.toThrow(/Date.*(?:argument|valid|integer|range)/i);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteOne).not.toHaveBeenCalled();
  });

  it.each([
    ["new Date(2026, 0)", () => new Date(2026, 0)],
    ["new Date(2026, 0, 2)", () => new Date(2026, 0, 2)],
    [
      "new Date(2026, 0, 2, 3, 4, 5, 678)",
      () => new Date(2026, 0, 2, 3, 4, 5, 678),
    ],
    [
      'ISODate("2026-10-23T12:34:56.789Z")',
      () => new Date("2026-10-23T12:34:56.789Z"),
    ],
    ["new Date(0)", () => new Date(0)],
    ["new Date(-1)", () => new Date(-1)],
    ["new Date(8640000000000000)", () => new Date(8640000000000000)],
    ["new Date(-8640000000000000)", () => new Date(-8640000000000000)],
    [
      'new Date(ISODate("2026-10-23T12:34:56.789Z"))',
      () => new Date("2026-10-23T12:34:56.789Z"),
    ],
  ])("preserves %s through BSON serialization", async (literal, expected) => {
    const { driver, deleteOne } = setup();
    await driver.query(`db.items.deleteOne({_id: ${literal}})`);
    const filter = deleteOne.mock.calls[0][0];
    const persisted = BSON.deserialize(BSON.serialize(filter));
    expect(persisted._id).toEqual(expected());
  });

  it.each([
    "new Date()",
    "ISODate()",
  ])("retains current-time support: %s", async (literal) => {
    const { driver, deleteOne } = setup();
    const before = Date.now();
    await driver.query(`db.items.deleteOne({_id: ${literal}})`);
    expect(deleteOne.mock.calls[0][0]._id.getTime()).toBeGreaterThanOrEqual(
      before,
    );
    expect(deleteOne.mock.calls[0][0]._id.getTime()).toBeLessThanOrEqual(
      Date.now(),
    );
  });
});

it.skipIf(!process.env.RAPIDB_MONGO_DATE_AUDIT_URL)(
  "persists calendar dates on live MongoDB and never sends a script containing an invalid date",
  async () => {
    const uri = process.env.RAPIDB_MONGO_DATE_AUDIT_URL;
    if (!uri) throw new Error("MongoDB audit URL is required");
    const database = "rapidb_date_audit";
    const name = `dates_${randomUUID().replaceAll("-", "")}`;
    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
    const driver = new MongoDBDriver({
      id: name,
      name,
      type: "mongodb",
      connectionUri: uri,
      database,
    });
    try {
      await client.connect();
      await driver.connect();
      const collection = client.db(database).collection<{
        _id: string | Date;
        value?: Date | string;
      }>(name);
      await driver.query(
        `db.${name}.insertOne({_id: "valid", value: new Date(2026, 0, 2, 3, 4, 5, 678)})`,
      );
      expect((await collection.findOne({ _id: "valid" }))?.value).toEqual(
        new Date(2026, 0, 2, 3, 4, 5, 678),
      );
      await collection.insertOne({ _id: new Date(0), value: "keep" });
      await expect(
        driver.query(
          `db.${name}.insertOne({_id: "must-not-exist"}); db.${name}.deleteOne({_id: ISODate("invalid")})`,
        ),
      ).rejects.toThrow(/Date.*valid/i);
      expect(await collection.countDocuments()).toBe(2);
      expect(await collection.findOne({ _id: "must-not-exist" })).toBeNull();
      expect((await collection.findOne({ _id: new Date(0) }))?.value).toBe(
        "keep",
      );
    } finally {
      await driver.disconnect();
      try {
        await client.db(database).collection(name).drop();
      } finally {
        await client.close();
      }
    }
  },
);
