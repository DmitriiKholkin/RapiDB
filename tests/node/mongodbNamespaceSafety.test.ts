import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

describe("mongosh explicit namespaces never fall back or coerce names", () => {
  it.each([
    "getSiblingDB",
    "getCollection",
  ])("rejects invalid %s names before sending any statements", async (method) => {
    for (const argument of [
      "",
      "undefined",
      "null",
      '""',
      "false",
      "0",
      "42",
      "[]",
      '["items"]',
      "{}",
    ]) {
      const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
      const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 10 });
      const db = vi.fn(() => ({
        collection: () => ({ insertOne, deleteMany }),
      }));
      const driver = new MongoDBDriver({
        id: "mongo-namespace",
        name: "Mongo namespace",
        type: "mongodb",
        database: "production",
      });
      Object.assign(driver, { connected: true, client: { db } });
      const target =
        method === "getSiblingDB"
          ? `db.getSiblingDB(${argument}).items`
          : `db.getCollection(${argument})`;
      await expect(
        driver.query(
          `db.items.insertOne({_id: "earlier"}); ${target}.deleteMany({})`,
        ),
      ).rejects.toThrow(/name.*non-empty string/i);
      expect(db).not.toHaveBeenCalled();
      expect(insertOne).not.toHaveBeenCalled();
      expect(deleteMany).not.toHaveBeenCalled();
    }
  });

  it("keeps the exact explicit database and collection names", async () => {
    const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 1 });
    const collection = vi.fn(() => ({ deleteMany }));
    const db = vi.fn(() => ({ collection }));
    const driver = new MongoDBDriver({
      id: "mongo-namespace",
      name: "Mongo namespace",
      type: "mongodb",
      database: "production",
    });
    Object.assign(driver, { connected: true, client: { db } });
    await driver.query(
      'db.getSiblingDB("staging").getCollection("items-данные").deleteMany({})',
    );
    expect(db).toHaveBeenCalledWith("staging");
    expect(collection).toHaveBeenCalledWith("items-данные");
    expect(deleteMany).toHaveBeenCalledWith({});
  });
});
