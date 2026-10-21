import { Int32, Long } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function setup() {
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  const deleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const driver = new MongoDBDriver({
    id: "mongo-literals",
    name: "Mongo literals",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ insertOne, deleteOne }) }) },
  });
  return { driver, insertOne, deleteOne };
}

describe("mongosh BSON integer literals preserve the requested value", () => {
  it.each([
    'NumberInt("2147483648")',
    'NumberInt("-2147483649")',
    'NumberInt("1.5")',
    'NumberInt("12junk")',
    "NumberInt(1.5)",
    'NumberLong("9223372036854775808")',
    'NumberLong("-9223372036854775809")',
    'NumberLong("18446744073709551616")',
    'NumberLong("1.5")',
    'NumberLong("12junk")',
    "NumberLong(9007199254740993)",
    "NumberLong(null)",
    "NumberInt(true)",
    'NumberInt(["42"])',
    'NumberLong("1", "ignored")',
  ])("rejects %s before any operation in the script is submitted", async (literal) => {
    const { driver, insertOne, deleteOne } = setup();
    await expect(
      driver.query(
        `db.items.insertOne({_id:"earlier"}); db.items.deleteOne({_id:${literal}})`,
      ),
    ).rejects.toThrow(/integer|range|safe|argument/i);
    expect(insertOne).not.toHaveBeenCalled();
    expect(deleteOne).not.toHaveBeenCalled();
  });

  it.each([
    ['NumberInt("-2147483648")', "-2147483648", Int32],
    ['NumberInt("2147483647")', "2147483647", Int32],
    ['NumberInt(" +00042 ")', "42", Int32],
    ["NumberInt(0)", "0", Int32],
    ["NumberInt(-42n)", "-42", Int32],
    ['NumberLong("-9223372036854775808")', "-9223372036854775808", Long],
    ['NumberLong("9223372036854775807")', "9223372036854775807", Long],
    ["NumberLong(9223372036854775807n)", "9223372036854775807", Long],
    ["NumberLong(9007199254740991)", "9007199254740991", Long],
    ['NumberLong(" +00042 ")', "42", Long],
  ])("preserves %s exactly", async (literal, expected, bsonType) => {
    const { driver, insertOne } = setup();
    await driver.query(`db.items.insertOne({amount:${literal}})`);
    const value = insertOne.mock.calls[0][0].amount;
    expect(value).toBeInstanceOf(bsonType);
    expect(value.toString()).toBe(expected);
  });
});
