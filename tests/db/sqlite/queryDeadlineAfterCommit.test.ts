import { afterEach, describe, expect, it } from "vitest";
import { MutationNotExecutedError } from "../../../src/extension/dbDrivers/mutationExecutionState";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";

const SLOW_COUNT =
  "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3000000) SELECT count(*) FROM c";

describe("SQLite query deadline after execution", () => {
  let driver: SQLiteCoreDriver | undefined;

  afterEach(async () => {
    await driver?.disconnect();
    driver = undefined;
  });

  const connect = async () => {
    driver = new SQLiteCoreDriver({
      id: "deadline-after-commit",
      name: "Deadline after commit",
      type: "sqlite",
      filePath: ":memory:",
    });
    await driver.connect();
    await driver.query("CREATE TABLE t (id INTEGER)");
    return driver;
  };

  it("does not report a deadline failure for an autocommitted script that already completed", async () => {
    const db = await connect();

    const result = await db.query(
      `PRAGMA user_version = 7; INSERT INTO t VALUES (1); ${SLOW_COUNT}`,
      [],
      { deadline: Date.now() + 20 },
    );

    expect(result.rows[0]?.__col_0).toBe(3000000);
    const version = await db.query("PRAGMA user_version");
    expect(version.rows[0]?.__col_0).toBe(7);
    const rows = await db.query("SELECT count(*) FROM t");
    expect(rows.rows[0]?.__col_0).toBe(1);
  });

  it("still rolls back a savepoint-wrapped script that passes its deadline", async () => {
    const db = await connect();

    await expect(
      db.query(`INSERT INTO t VALUES (1); ${SLOW_COUNT}`, [], {
        deadline: Date.now() + 20,
      }),
    ).rejects.toBeInstanceOf(MutationNotExecutedError);

    const rows = await db.query("SELECT count(*) FROM t");
    expect(rows.rows[0]?.__col_0).toBe(0);
  });
});
