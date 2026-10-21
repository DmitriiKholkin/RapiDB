import { BSON, Decimal128 } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function fixture() {
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const driver = new MongoDBDriver({
    id: "decimal-literals",
    name: "Decimal literals",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ insertOne, deleteOne }) }) },
  });
  return { driver, insertOne, deleteOne };
}

describe("mongosh Decimal128 literals", () => {
  it.each([
    "NumberDecimal(9007199254740993)",
    "NumberDecimal(0.123456789012345678901)",
    "NumberDecimal(1)",
    "NumberDecimal(42n)",
    'NumberDecimal(["42"])',
    'NumberDecimal({value:"42"})',
    'NumberDecimal("1", "ignored")',
    "NumberDecimal()",
    "NumberDecimal(null)",
    'NumberDecimal("invalid")',
    'NumberDecimal("1.2345678901234567890123456789012345")',
  ])("rejects %s before any earlier write in the script", async (literal) => {
    const { driver, insertOne, deleteOne } = fixture();
    await expect(
      driver.query(
        `db.items.insertOne({_id:"earlier"}); db.items.deleteOne({_id:${literal}})`,
      ),
    ).rejects.toThrow(/NumberDecimal|Decimal128|decimal|inexact/i);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteOne).not.toHaveBeenCalled();
  });

  it.each([
    "9007199254740993",
    "0.123456789012345678901",
    "1.234567890123456789012345678901234",
    "1E+6144",
    "1E-6176",
    "-0.00",
  ])("preserves the exact BSON value %s", async (text) => {
    const { driver, insertOne } = fixture();
    await driver.query(`db.items.insertOne({amount:NumberDecimal("${text}")})`);
    const document = insertOne.mock.calls[0][0];
    expect(document.amount).toBeInstanceOf(Decimal128);
    const persisted = BSON.deserialize(BSON.serialize(document));
    expect(persisted.amount.toString()).toBe(
      Decimal128.fromString(text).toString(),
    );
  });
});
