import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { openSQLiteDatabase } from "../../../src/extension/dbDrivers/sqliteRuntime";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
} from "../../../src/extension/dbDrivers/timeout";
import {
  configureSQLiteInstaller,
  resetSQLiteInstallerForTests,
} from "../../../src/extension/utils/sqliteInstaller";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { createProjectTempDir } from "../../runtime/tempDirectories";

const CPU_SUM =
  "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n";
const drivers: SQLiteDriver[] = [];
const settings = () =>
  createDriverTimeoutSettingsSnapshot({
    connectionTimeoutSeconds: 1,
    dbOperationTimeoutSeconds: 1,
  });

async function createDriver(
  filePath?: string,
  readOnly = false,
  sqliteWalMode?: "off",
) {
  const directory = await createProjectTempDir("sqlite-worker-execution");
  const config: ConnectionConfig = {
    id: "worker",
    name: "Worker",
    type: "sqlite",
    filePath: filePath ?? join(directory, "db.sqlite"),
    readOnly,
    sqliteWalMode,
  };
  const driver = createTimeoutAwareDriver(
    new SQLiteDriver(config, settings),
    settings,
  );
  drivers.push(driver);
  return driver;
}
async function value(driver: SQLiteDriver, sql: string) {
  return (await driver.query(sql)).rows[0]?.__col_0;
}
function workerPid(driver: SQLiteDriver): number | undefined {
  return (driver as unknown as { worker: { child?: { pid?: number } } }).worker
    .child?.pid;
}
function expectExited(pid: number | undefined) {
  if (pid === undefined) throw new Error("Expected a worker PID");
  expect(() => process.kill(pid, 0)).toThrow();
}
async function waitForSpawn(driver: SQLiteDriver) {
  for (let attempt = 0; attempt < 100 && !workerPid(driver); attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  return workerPid(driver);
}
function cancelQuery(driver: SQLiteDriver, requestToken: number) {
  return driver.cancelCurrentOperation({
    reason: "manual",
    operationName: "query",
    requestToken,
  });
}

afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
  resetSQLiteInstallerForTests();
});

describe("SQLite off-host execution", () => {
  it.each([
    "timeout",
    "abort",
  ] as const)("rolls back DML when verification read-back is interrupted by %s", async (mode) => {
    const driver = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE verification (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO verification VALUES (1, 0)",
    );
    const columns = await driver.describeColumns("", "main", "verification");
    const amount = columns.find((column) => column.name === "amount");
    if (!amount) throw new Error("Expected amount column");
    const controller = new AbortController();
    const pending = driver.runTransaction(
      [
        {
          sql: "UPDATE verification SET amount = 9 WHERE id = 1",
          expectedAffectedRows: 1,
        },
      ],
      { signal: controller.signal, deadline: Date.now() + 10000 },
      {
        verifications: [
          {
            rowIndex: 0,
            sql: CPU_SUM,
            params: [],
            values: [{ column: amount, expectedValue: 9 }],
          },
        ],
      },
    );
    const rejected = expect(pending).rejects.toThrow(
      mode === "timeout" ? /timed out.*rolled back/ : /cancelled.*rolled back/,
    );
    if (mode === "abort") {
      await new Promise((resolve) => setTimeout(resolve, 100));
      controller.abort();
    }
    await rejected;
    expect(driver.isConnected()).toBe(false);
    await driver.connect();
    expect(await value(driver, "SELECT amount FROM verification")).toBe(0);
  });

  it("keeps host timers responsive during a real CPU SELECT and enforces its deadline", async () => {
    const driver = await createDriver();
    await driver.connect();
    const pid = workerPid(driver);
    let ticks = 0;
    const pulse = setInterval(() => ticks++, 20);
    const start = Date.now();
    try {
      await expect(
        driver.query(CPU_SUM, [], { hardCap: 1, requestToken: 11 }),
      ).rejects.toThrow(/timed out.*connection is closed/);
    } finally {
      clearInterval(pulse);
    }
    expect(ticks).toBeGreaterThan(15);
    expect(Date.now() - start).toBeLessThan(3000);
    expect(driver.isConnected()).toBe(false);
    expectExited(pid);
  });

  it("rolls back a long mutation and all earlier transaction operations before reopening file data", async () => {
    const driver = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE data (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO data VALUES(1, 7)",
    );
    await expect(
      driver.runTransaction([
        { sql: "UPDATE data SET value=9 WHERE id=1", expectedAffectedRows: 1 },
        {
          sql: `UPDATE data SET value=(${CPU_SUM}) WHERE id=1`,
          expectedAffectedRows: 1,
        },
      ]),
    ).rejects.toThrow(/timed out.*rolled back/);
    await expect(
      driver.query("UPDATE data SET value=99"),
    ).rejects.toMatchObject({
      code: "NOT_EXECUTED",
      executionState: "not-executed",
      message: expect.stringMatching(/connection is closed/),
    });
    await driver.connect();
    expect(await value(driver, "SELECT value FROM data")).toBe(7);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(await value(driver, "SELECT value FROM data")).toBe(7);
    // Query editor mutations, including RETURNING, have the same isolation.
    await expect(
      driver.query(`UPDATE data SET value=(${CPU_SUM}) RETURNING value`, [], {
        hardCap: 1,
      }),
    ).rejects.toThrow(/timed out/);
    await driver.connect();
    expect(await value(driver, "SELECT value FROM data")).toBe(7);
  });

  it("targets queued/active query tokens without cancelling unrelated or newer work", async () => {
    const driver = await createDriver();
    await driver.connect();
    const pid = workerPid(driver);
    const active = expect(
      driver.query(CPU_SUM, [], { requestToken: 21 }),
    ).rejects.toThrow(/cancelled.*session state are lost/);
    const queued = expect(
      driver.query("SELECT 2", [], { requestToken: 22 }),
    ).rejects.toThrow(/cancelled/);
    await cancelQuery(driver, 22);
    await queued;
    expect(driver.isConnected()).toBe(true);
    await cancelQuery(driver, 123);
    expect(driver.isConnected()).toBe(true);
    await cancelQuery(driver, 21);
    await active;
    expectExited(pid);
    await driver.connect();
    const newer = driver.query("SELECT 3", [], { requestToken: 23 });
    await cancelQuery(driver, 21);
    expect((await newer).rows[0].__col_0).toBe(3);
  });

  it("disconnects during native execution, rejects the queue, and reopens persisted data", async () => {
    const driver = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE kept (value TEXT); INSERT INTO kept VALUES ('persisted')",
    );
    const pid = workerPid(driver);
    const active = expect(driver.query(CPU_SUM)).rejects.toThrow(
      /disconnected/,
    );
    const queued = expect(
      driver.query("INSERT INTO kept VALUES ('stale')"),
    ).rejects.toThrow(/disconnected/);
    await new Promise((resolve) => setTimeout(resolve, 100));
    await driver.disconnect();
    await Promise.all([active, queued]);
    expectExited(pid);
    await driver.connect();
    expect(await value(driver, "SELECT count(*) FROM kept")).toBe(1);
  });

  it("serializes concurrent metadata, DDL and transactions on the same ATTACH/session/memory handle", async () => {
    const driver = await createDriver(":memory:");
    await driver.connect();
    await driver.query("ATTACH ':memory:' AS attached");
    const operations = [
      driver.query(
        "CREATE TABLE attached.data (value INTEGER); INSERT INTO attached.data VALUES(1)",
      ),
      driver.runTransaction([
        {
          sql: "UPDATE attached.data SET value=value+1",
          expectedAffectedRows: 1,
        },
      ]),
      driver.describeColumns("attached", "attached", "data"),
      driver.getCreateTableDDL("attached", "attached", "data"),
      driver.query(
        "CREATE TEMP TABLE temporary (value BLOB); INSERT INTO temporary VALUES(x'0102')",
      ),
      driver.query("SELECT value FROM attached.data"),
    ];
    const results = await Promise.all(operations);
    expect(results[2]).toEqual([
      expect.objectContaining({ name: "value", category: "integer" }),
    ]);
    expect(results[3]).toContain("CREATE TABLE data");
    const selected = results[5] as Awaited<ReturnType<SQLiteDriver["query"]>>;
    expect(selected.rows[0].__col_0).toBe(2);
    expect((await driver.listDatabases()).map((entry) => entry.name)).toContain(
      "attached",
    );
    expect(
      Buffer.isBuffer(await value(driver, "SELECT value FROM temporary")),
    ).toBe(true);
    await driver.query("BEGIN; UPDATE attached.data SET value=8");
    expect(await value(driver, "SELECT value FROM attached.data")).toBe(8);
    await driver.query("ROLLBACK");
    expect(await value(driver, "SELECT value FROM attached.data")).toBe(2);
  });

  it("reports lost memory and never silently recreates it for mutations or reconnect", async () => {
    const driver = await createDriver(":memory:");
    await driver.connect();
    await driver.query("CREATE TABLE data (value INTEGER)");
    const query = expect(
      driver.query(CPU_SUM, [], { requestToken: 31 }),
    ).rejects.toThrow(/in-memory databases are lost/);
    await cancelQuery(driver, 31);
    await query;
    await expect(
      driver.runTransaction([{ sql: "INSERT INTO data VALUES(1)" }]),
    ).rejects.toMatchObject({
      code: "NOT_EXECUTED",
      executionState: "not-executed",
      message: expect.stringMatching(/connection is closed/),
    });
    await expect(driver.connect()).rejects.toThrow(
      /Create a new connection explicitly/,
    );
  });

  it("honors mutation AbortSignal and expires queued transactions without executing them", async () => {
    const driver = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE data(value INTEGER); INSERT INTO data VALUES(1)",
    );
    const controller = new AbortController();
    const transaction = expect(
      driver.runTransaction([{ sql: `UPDATE data SET value=(${CPU_SUM})` }], {
        signal: controller.signal,
        deadline: Date.now() + 10000,
      }),
    ).rejects.toThrow(/cancelled/);
    await expect(
      driver.runTransaction([{ sql: "UPDATE data SET value=2" }], {
        signal: new AbortController().signal,
        deadline: Date.now() + 50,
      }),
    ).rejects.toThrow(/queue.*not executed/);
    expect(driver.isConnected()).toBe(true);
    controller.abort();
    await transaction;
    await driver.connect();
    expect(await value(driver, "SELECT value FROM data")).toBe(1);
    await expect(
      driver.runTransaction([{ sql: "UPDATE data SET value=2" }], {
        signal: new AbortController().signal,
        deadline: Date.now() - 1,
      }),
    ).rejects.toThrow(/queue.*not executed/);
    expect(driver.isConnected()).toBe(true);
    expect(await value(driver, "SELECT value FROM data")).toBe(1);
  });

  it.each([
    "describeColumns",
    "getCreateTableDDL",
  ] as const)("keeps host timers responsive and interrupts native lock waits in %s", async (method) => {
    const directory = await createProjectTempDir("sqlite-metadata-lock");
    const filePath = join(directory, "db.sqlite");
    const driver = await createDriver(filePath, false, "off");
    await driver.connect();
    await driver.query("CREATE TABLE data(value INTEGER)");
    await driver.query("PRAGMA busy_timeout=5000");
    const blocker = await openSQLiteDatabase({
      filePath,
      sqliteWalMode: "off",
    });
    let ticks = 0;
    const pulse = setInterval(() => ticks++, 20);
    try {
      blocker.exec("BEGIN EXCLUSIVE");
      await expect(driver[method]("main", "main", "data")).rejects.toThrow(
        /timed out.*connection is closed/,
      );
      expect(ticks).toBeGreaterThan(15);
    } finally {
      clearInterval(pulse);
      blocker.exec("ROLLBACK");
      blocker.close();
    }
  });

  it("terminates a worker that never completes startup and can reopen after recovery", async () => {
    const workerPath = process.env.RAPIDB_SQLITE_WORKER_PATH;
    if (!workerPath) throw new Error("Missing test worker bundle");
    const directory = await createProjectTempDir("sqlite-startup-timeout");
    const stalledWorker = join(directory, "stalled.js");
    await writeFile(stalledWorker, "setInterval(() => {}, 1000);");
    vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", stalledWorker);
    const driver = await createDriver();
    const opening = expect(driver.connect()).rejects.toThrow(
      /connection timed out.*connection is closed/,
    );
    const pid = await waitForSpawn(driver);
    await opening;
    expectExited(pid);
    expect(driver.isConnected()).toBe(false);
    vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", workerPath);
    await driver.connect();
    expect(await value(driver, "SELECT 1")).toBe(1);
  });

  it("cleans up worker startup/open errors and can reconnect after correcting them", async () => {
    const driver = await createDriver();
    const workerPath = process.env.RAPIDB_SQLITE_WORKER_PATH;
    if (!workerPath) throw new Error("Missing test worker bundle");
    vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", join(workerPath, "missing.js"));
    const rejected = expect(driver.connect()).rejects.toThrow(/process exited/);
    const pid = await waitForSpawn(driver);
    await rejected;
    expectExited(pid);
    expect(driver.isConnected()).toBe(false);
    vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", workerPath);
    await driver.connect();
    expect(await value(driver, "SELECT 1")).toBe(1);

    const directory = await createProjectTempDir("sqlite-open-error");
    const bad = await createDriver(join(directory, "missing.sqlite"), true);
    await expect(bad.connect()).rejects.toThrow(/failed to open.*read-only/);
    expect(bad.isConnected()).toBe(false);
    expect(workerPid(bad)).toBeUndefined();
  });

  it("does not spawn a late worker after disconnect during trust-aware runtime preparation", async () => {
    let allow!: (value: boolean) => void;
    configureSQLiteInstaller({
      storageRoot: await createProjectTempDir("sqlite-installer"),
      allowInstall: () =>
        new Promise<boolean>((resolve) => {
          allow = resolve;
        }),
    });
    const driver = await createDriver();
    const opening = expect(driver.connect()).rejects.toThrow(
      /disconnected|cancelled/,
    );
    for (let i = 0; i < 100 && !allow; i++)
      await new Promise((resolve) => setTimeout(resolve, 5));
    expect(allow).toBeTypeOf("function");
    await driver.disconnect();
    allow(false);
    await opening;
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(workerPid(driver)).toBeUndefined();
    expect(driver.isConnected()).toBe(false);
  });
});
