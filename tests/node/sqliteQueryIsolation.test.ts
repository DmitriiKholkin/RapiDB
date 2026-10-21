import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import { createProjectTempDir } from "../runtime/tempDirectories";

const drivers: SQLiteDriver[] = [];

afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function open(filePath?: string) {
  const driver = new SQLiteDriver({
    id: "isolation",
    name: "SQLite isolation",
    type: "sqlite",
    filePath: filePath ?? ":memory:",
  });
  drivers.push(driver);
  await driver.connect();
  return driver;
}

async function values(driver: SQLiteDriver) {
  const result = await driver.query("SELECT value FROM items ORDER BY value");
  return result.rows.map((row) => row.__col_0);
}

describe("SQLite query transaction isolation through the real worker", () => {
  it.each([
    "BEGIN",
    "SAVEPOINT user_work",
  ])("rejects unfinished %s rather than reporting uncommitted writes as successful", async (begin) => {
    const filePath = join(
      await createProjectTempDir("sqlite-query-isolation"),
      "data.sqlite",
    );
    const driver = await open(filePath);
    await driver.query("CREATE TABLE items(value INTEGER)");
    await expect(
      driver.query(`${begin}; INSERT INTO items VALUES(1)`),
    ).rejects.toThrow(/unfinished SQLite transaction.*rolled back/i);
    expect(await values(driver)).toEqual([]);

    // A later editor write must commit independently and survive disconnect.
    await driver.query("INSERT INTO items VALUES(2)");
    await driver.disconnect();
    expect(await values(await open(filePath))).toEqual([2]);
  });

  it("cleans a failed explicit transaction before a table mutation borrows the session", async () => {
    const driver = await open();
    await driver.query("CREATE TABLE items(value INTEGER)");
    await expect(
      driver.query(
        "BEGIN; INSERT INTO items VALUES(1); SELECT * FROM missing_table",
      ),
    ).rejects.toThrow(/missing_table/);
    expect(await values(driver)).toEqual([]);
    await driver.runTransaction([
      { sql: "INSERT INTO items VALUES(?)", params: [2] },
    ]);
    expect(await values(driver)).toEqual([2]);
  });

  it("does not claim to undo earlier commits when rolling back an unfinished transaction", async () => {
    const driver = await open();
    await driver.query("CREATE TABLE items(value INTEGER)");
    await expect(
      driver.query(
        "BEGIN; INSERT INTO items VALUES(1); COMMIT; BEGIN; INSERT INTO items VALUES(2)",
      ),
    ).rejects.toThrow(/earlier commits.*not undone/i);
    expect(await values(driver)).toEqual([1]);
  });

  it("cleans the shared session before dispatching a queued editor write", async () => {
    const driver = await open();
    await driver.query("CREATE TABLE items(value INTEGER)");
    const unfinished = driver.query("BEGIN; INSERT INTO items VALUES(1)");
    const nextEditor = driver.query("INSERT INTO items VALUES(2)");
    await expect(unfinished).rejects.toThrow(/unfinished SQLite transaction/i);
    await nextEditor;
    expect(await values(driver)).toEqual([2]);
  });

  it("preserves earlier commits on script errors without declaring the whole script not executed", async () => {
    const driver = await open();
    await driver.query("CREATE TABLE items(value INTEGER)");
    const error = await driver
      .query(
        "BEGIN; INSERT INTO items VALUES(1); COMMIT; BEGIN; INSERT INTO items VALUES(2); SELECT * FROM missing_table",
      )
      .catch((error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toHaveProperty("executionState", "not-executed");
    expect(await values(driver)).toEqual([1]);
  });

  it("keeps balanced transactions, nested savepoints and session objects working", async () => {
    const driver = await open();
    await driver.query("CREATE TEMP TABLE items(value INTEGER)");
    await driver.query(
      "BEGIN; INSERT INTO items VALUES(1); SAVEPOINT inner_work; INSERT INTO items VALUES(2); ROLLBACK TO inner_work; RELEASE inner_work; COMMIT; SELECT value FROM items",
      undefined,
      { hardCap: 10 },
    );
    expect(await values(driver)).toEqual([1]);
    await driver.query("BEGIN; INSERT INTO items VALUES(3); ROLLBACK");
    expect(await values(driver)).toEqual([1]);
    await driver.query(
      "SAVEPOINT outer_work; INSERT INTO items VALUES(4); RELEASE outer_work",
    );
    expect(await values(driver)).toEqual([1, 4]);
  });
});
