import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function harness() {
  const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const driver = new MongoDBDriver({
    id: "text-integrity",
    name: "Text integrity",
    type: "mongodb",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ deleteMany, insertOne }) }) },
  });
  return { driver, deleteMany, insertOne };
}

describe("mongosh query text integrity", () => {
  it("preserves comment-like lines inside a destructive filter", async () => {
    const { driver, deleteMany } = harness();
    await driver.query(
      "db.items.deleteMany({text: `first\n// stored text\nlast`})",
    );
    expect(deleteMany).toHaveBeenCalledWith({
      text: "first\n// stored text\nlast",
    });
  });

  it("preserves multiline inserted values while accepting actual comments", async () => {
    const { driver, insertOne } = harness();
    await driver.query(
      "// actual comment\ndb.items.insertOne({text: `first\n  // stored text\nlast`});\n// trailing comment",
    );
    expect(insertOne).toHaveBeenCalledWith({
      text: "first\n  // stored text\nlast",
    });
  });

  it("keeps comment-only input a no-op", async () => {
    const { driver, deleteMany, insertOne } = harness();
    const result = await driver.query("// first\n\n  // second");
    expect(result.rowCount).toBe(0);
    expect(deleteMany).not.toHaveBeenCalled();
    expect(insertOne).not.toHaveBeenCalled();
  });
});
