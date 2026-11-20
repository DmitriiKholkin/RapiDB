import { join } from "node:path";
import { expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { createProjectTempDir } from "../../runtime/tempDirectories";

it("fences a SQLite attachment reused with the same filename after detach or worker restart", async () => {
  const directory = await createProjectTempDir("sqlite-attachment");
  const file = join(directory, "attached.sqlite");
  const driver = new SQLiteDriver({
    id: "sqlite-attachment-continuity",
    name: "SQLite attachment continuity",
    type: "sqlite",
    filePath: ":memory:",
  });
  try {
    const attach = () => driver.query("ATTACH ? AS aux", [file]);
    const write = { sql: "UPDATE aux.items SET amount = amount + 1" };
    await driver.connect();
    await attach();
    await driver.query(
      "CREATE TABLE aux.items(id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO aux.items VALUES (1, 10)",
    );
    const first = await driver.getSQLiteAttachmentTarget("AuX");

    await driver.query("DETACH [aux]");
    await attach();
    const second = await driver.getSQLiteAttachmentTarget("aux");
    await expect(
      driver.runTransaction([write], undefined, {
        sqliteAttachmentTarget: first,
      }),
    ).rejects.toMatchObject({ executionState: "not-executed" });
    await driver.runTransaction([write], undefined, {
      sqliteAttachmentTarget: second,
    });

    await driver.disconnect();
    await driver.connect();
    await attach();
    await expect(
      driver.runTransaction([write], undefined, {
        sqliteAttachmentTarget: second,
      }),
    ).rejects.toMatchObject({ executionState: "not-executed" });
    expect((await driver.query("SELECT amount FROM aux.items")).rows).toEqual([
      { __col_0: 11 },
    ]);
  } finally {
    await driver.disconnect();
  }
});
