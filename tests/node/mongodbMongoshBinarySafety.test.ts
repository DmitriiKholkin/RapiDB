import { Binary, BSON } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function fixture() {
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const driver = new MongoDBDriver({
    id: "binary-literals",
    name: "Binary literals",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ insertOne, deleteOne }) }) },
  });
  return { driver, insertOne, deleteOne };
}

describe("mongosh binary literal validation", () => {
  it.each([
    'BinData(0, "YQ!!")',
    'BinData(0, "YQ=")',
    'BinData(0, "YR==")',
    'BinData(-1, "YQ==")',
    'BinData(256, "YQ==")',
    'BinData(1.5, "YQ==")',
    'BinData(null, "YQ==")',
    'BinData("0", "YQ==")',
    "BinData(0, null)",
    'BinData(0, ["YQ=="])',
    'BinData(0, "YQ==", "ignored")',
    "BinData()",
  ])("rejects %s before an earlier operation can write", async (literal) => {
    const { driver, insertOne, deleteOne } = fixture();
    await expect(
      driver.query(
        `db.items.insertOne({_id:"earlier"}); db.items.deleteOne({_id:${literal}})`,
      ),
    ).rejects.toThrow(/BinData|base64|subtype/i);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteOne).not.toHaveBeenCalled();
  });

  it.each([
    [0, ""],
    [0, "YQ=="],
    [0, "YWJj"],
    [2, "AAECAw=="],
    [4, "AAAAAAAAAAAAAAAAAAAAAA=="],
    [128, "/w=="],
    [255, "AA=="],
  ])("preserves subtype %s and bytes %s through BSON", async (subtype, base64) => {
    const { driver, insertOne } = fixture();
    await driver.query(
      `db.items.insertOne({value:BinData(${subtype}, "${base64}")})`,
    );
    const document = insertOne.mock.calls[0][0];
    expect(document.value).toBeInstanceOf(Binary);
    const persisted = BSON.deserialize(BSON.serialize(document), {
      promoteBuffers: false,
    });
    expect(persisted.value.sub_type).toBe(subtype);
    expect(Buffer.from(persisted.value.buffer).toString("base64")).toBe(base64);
  });
});
