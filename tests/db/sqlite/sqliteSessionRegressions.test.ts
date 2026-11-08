import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { createDriverTimeoutSettingsSnapshot } from "../../../src/extension/dbDrivers/timeout";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { createProjectTempDir } from "../../runtime/tempDirectories";

const drivers: SQLiteDriver[] = [];
const CPU_SUM =
  "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<30000000) SELECT sum(x) FROM n";

async function createDriver(filePath?: string) {
  const directory = await createProjectTempDir("g8-sqlite-fixes");
  const config: ConnectionConfig = {
    id: "g8-sqlite",
    name: "G8 SQLite",
    type: "sqlite",
    filePath: filePath ?? join(directory, "db.sqlite"),
  };
  const driver = new SQLiteDriver(config, () =>
    createDriverTimeoutSettingsSnapshot({
      connectionTimeoutSeconds: 5,
      dbOperationTimeoutSeconds: 60,
    }),
  );
  drivers.push(driver);
  await driver.connect();
  return driver;
}

function workerPid(driver: SQLiteDriver): number | undefined {
  return (driver as unknown as { worker: { child?: { pid?: number } } }).worker
    .child?.pid;
}

afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

describe("SQLite G8 regressions", () => {
  it("keeps the :memory: session when an active page read is aborted", async () => {
    const driver = await createDriver(":memory:");
    await driver.query(
      "CREATE TEMP TABLE kept(value INTEGER); INSERT INTO kept VALUES (42)",
    );
    const pid = workerPid(driver);
    const controller = new AbortController();
    const read = driver.query(CPU_SUM.replace("30000000", "3000000"), [], {
      signal: controller.signal,
      hardCap: 1,
    });
    await new Promise((resolve) => setTimeout(resolve, 30));
    controller.abort();
    await expect(read).rejects.toMatchObject({ name: "AbortError" });
    expect(driver.isConnected()).toBe(true);
    expect(driver.getAutomaticReconnectBlockReason()).toBeUndefined();
    const result = await driver.query("SELECT value FROM kept");
    expect(result.rows[0]?.__col_0).toBe(42);
    expect(workerPid(driver)).toBe(pid);
  });

  it.each([
    ["a read followed by DELETE", `${CPU_SUM}; DELETE FROM t`],
    [
      "INSERT ... RETURNING",
      `INSERT INTO t(v) SELECT * FROM (${CPU_SUM}) RETURNING v`,
    ],
  ])("stops the worker instead of detaching an aborted %s", async (_name, sql) => {
    const driver = await createDriver();
    await driver.query("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1)");
    const pid = workerPid(driver);
    const controller = new AbortController();
    const pending = driver.query(sql, [], { signal: controller.signal });
    await new Promise((resolve) => setTimeout(resolve, 30));
    controller.abort();
    await expect(pending).rejects.toMatchObject({ code: "OUTCOME_UNKNOWN" });
    expect(driver.isConnected()).toBe(false);
    expect(driver.getAutomaticReconnectBlockReason()).toBeDefined();
    expect(() => process.kill(pid as number, 0)).toThrow();
  });

  it("reports affected rows only for DML statements", async () => {
    const driver = await createDriver();
    await driver.query("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
    const insert = await driver.query("INSERT INTO t(v) VALUES (1), (2), (3)");
    expect(insert.affectedRows).toBe(3);
    const create = await driver.query("CREATE TABLE u(x)");
    expect(create.affectedRows).toBe(0);
    expect(create.rowCount).toBe(0);
    const pragma = await driver.query("PRAGMA user_version = 3");
    expect(pragma.affectedRows ?? 0).toBe(0);
    expect(
      (await driver.query("UPDATE t SET v = v + 1", [], { hardCap: 10 }))
        .affectedRows,
    ).toBe(3);
    const index = await driver.query("CREATE INDEX t_v ON t(v)", [], {
      hardCap: 10,
    });
    expect(index.affectedRows ?? 0).toBe(0);
    expect(index.rowCount).toBe(0);
    expect(
      (
        await driver.query(
          "WITH doomed AS (SELECT id FROM t WHERE v > 3) DELETE FROM t WHERE id IN (SELECT id FROM doomed)",
          [],
          { hardCap: 10 },
        )
      ).affectedRows,
    ).toBe(1);
    const script = await driver.query(
      "INSERT INTO t(v) VALUES (9); CREATE TABLE w(x); DROP TABLE w",
    );
    expect(script.affectedRows).toBe(1);
    expect(script.rowCount).toBe(1);
  });

  it("matches foreign key and AUTOINCREMENT columns case-insensitively and with quoted or non-ASCII names", async () => {
    const driver = await createDriver();
    await driver.query(`CREATE TABLE parent(id INTEGER PRIMARY KEY);
      CREATE TABLE child(
        "my id" INTEGER PRIMARY KEY AUTOINCREMENT,
        ParentId INTEGER,
        note TEXT DEFAULT 'x INTEGER PRIMARY KEY AUTOINCREMENT',
        FOREIGN KEY (parentid) REFERENCES parent(id)
      );
      CREATE TABLE ünïcode(идентификатор INTEGER PRIMARY KEY AUTOINCREMENT, v)`);
    const columns = await driver.describeColumns("main", "main", "child");
    const byName = Object.fromEntries(
      columns.map((column) => [column.name, column]),
    );
    expect(byName["my id"]?.identityGeneration).toBe("auto_increment");
    expect(byName.ParentId?.isForeignKey).toBe(true);
    expect(byName.note?.identityGeneration).toBeUndefined();
    expect(byName.note?.isForeignKey).toBe(false);
    await driver.query(`CREATE TABLE commented(
      -- primary key, it's generated (see "docs")
      id INTEGER PRIMARY KEY AUTOINCREMENT, /* note: a, b */
      /* 'quoted', still a comment */ v TEXT
    )`);
    const commented = await driver.describeColumns("main", "main", "commented");
    expect(commented.map((column) => column.name)).toEqual(["id", "v"]);
    expect(commented[0]?.identityGeneration).toBe("auto_increment");
    expect(commented[1]?.identityGeneration).toBeUndefined();
    await driver.query(`CREATE TABLE folded(
      "Äx" INTEGER,
      "äx" INTEGER,
      FOREIGN KEY ("Äx") REFERENCES parent(id)
    )`);
    const folded = await driver.describeColumns("main", "main", "folded");
    expect(
      Object.fromEntries(
        folded.map((column) => [column.name, column.isForeignKey]),
      ),
    ).toEqual({ Äx: true, äx: false });
    const unicode = await driver.describeColumns("main", "main", "ünïcode");
    expect(
      unicode.find((column) => column.name === "идентификатор")
        ?.identityGeneration,
    ).toBe("auto_increment");
  });
});
