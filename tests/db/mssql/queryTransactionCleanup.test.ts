import { randomUUID } from "node:crypto";
import mssql from "mssql";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../../src/extension/dbDrivers/mssql";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";

// Explicit opt-in; never uses/reset the shared integration fixtures. Point at
// a disposable local server only. Creates/drops just its own uniquely named table.
const port = Number(process.env.RAPIDB_MSSQL_ISOLATION_PORT);
const password = process.env.RAPIDB_MSSQL_ISOLATION_PASSWORD;
const suite = port && password ? describe : describe.skip;
const config: ConnectionConfig = {
  id: "mssql-query-isolation",
  name: "MSSQL query isolation",
  type: "mssql",
  host: "127.0.0.1",
  port,
  username: "sa",
  password,
  database: "master",
};
const pause = () => new Promise<void>((resolve) => setTimeout(resolve, 10));
async function until(predicate: () => boolean) {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    if (Date.now() > deadline)
      throw new Error("Timed out waiting for MSSQL test synchronization");
    await pause();
  }
}

suite("MSSQL physical query-session cleanup (isolated live server)", () => {
  let observer: mssql.ConnectionPool;
  const table = `dbo.rapidb_query_cleanup_${randomUUID().replaceAll("-", "")}`;
  beforeAll(async () => {
    for (let attempt = 0; ; attempt++) {
      const pool = new mssql.ConnectionPool({
        server: "127.0.0.1",
        port,
        user: "sa",
        password,
        database: "master",
        connectionTimeout: 1000,
        requestTimeout: 5000,
        options: { encrypt: false },
        pool: { min: 0, max: 1 },
      });
      try {
        observer = await pool.connect();
        break;
      } catch (error) {
        await pool.close().catch(() => {});
        if (attempt >= 40) throw error;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
    await observer
      .request()
      .batch(
        `CREATE TABLE ${table} (id int PRIMARY KEY, v int NOT NULL); INSERT ${table} VALUES (1,0),(2,0);`,
      );
  });
  afterAll(async () => {
    if (!observer) return;
    try {
      await observer.request().batch(`DROP TABLE IF EXISTS ${table}`);
    } finally {
      await observer.close();
    }
  });

  async function withDriver(
    body: (driver: MSSQLDriver, pool: mssql.ConnectionPool) => Promise<void>,
    requestTimeout = 5000,
    poolMax = 1,
  ) {
    await observer.request().query(`UPDATE ${table} SET v=0`);
    const driver = new MSSQLDriver(config);
    const runtime = driver as unknown as {
      poolConfig(): mssql.config;
      pool: mssql.ConnectionPool;
    };
    const original = runtime.poolConfig.bind(driver);
    runtime.poolConfig = () => ({
      ...original(),
      requestTimeout,
      pool: { min: 0, max: poolMax },
    });
    await driver.connect();
    try {
      await body(driver, runtime.pool);
    } finally {
      await driver.disconnect();
    }
  }
  async function values() {
    return (
      await observer
        .request()
        .query(`SET LOCK_TIMEOUT 1000; SELECT v FROM ${table} ORDER BY id`)
    ).recordset.map((row: { v: number }) => row.v);
  }

  it.each([
    undefined,
    1,
  ])("rolls back failed explicit SQL before the next durable transaction (cap=%s)", async (hardCap) => {
    await withDriver(async (driver) => {
      await expect(
        driver.query(`BEGIN TRAN; UPDATE ${table} SET v=? WHERE id=1`, [41], {
          hardCap,
        }),
      ).rejects.toMatchObject({ number: 266 });
      expect(await values()).toEqual([0, 0]);
      await driver.runTransaction([
        {
          sql: `UPDATE ${table} SET v=? WHERE id=2`,
          params: [99],
          expectedAffectedRows: 1,
        },
      ]);
      expect(await values()).toEqual([0, 99]);
      expect((await driver.query("SELECT @@TRANCOUNT AS n")).rows).toEqual([
        { __col_0: 0 },
      ]);
    });
  });

  it("fences a transaction already queued behind the failing request", async () => {
    await withDriver(async (driver, pool) => {
      const failure = driver.query(
        `SET XACT_ABORT OFF; BEGIN TRAN; UPDATE ${table} SET v=? WHERE id=1; WAITFOR DELAY '00:00:00.300'; RAISERROR('forced query failure',16,1);`,
        [41],
      );
      const rejected = expect(failure).rejects.toThrow();
      await until(() => pool.borrowed === 1);
      const following = driver.runTransaction([
        {
          sql: `UPDATE ${table} SET v=? WHERE id=2`,
          params: [99],
          expectedAffectedRows: 1,
        },
      ]);
      await until(() => pool.pending > 0);
      await rejected;
      await following;
      expect(await values()).toEqual([0, 99]);
    });
  });

  it.each([
    undefined,
    1,
  ])("preserves GO/repeats/#temp/explicit COMMIT and drains beyond display cap (cap=%s)", async (hardCap) => {
    await withDriver(async (driver) => {
      const result = await driver.query(
        `CREATE TABLE #script_values (v int);\nGO\nINSERT #script_values VALUES (7);\nGO 3\nBEGIN TRAN;\nGO\nSELECT v AS duplicate, v AS duplicate FROM #script_values; UPDATE ${table} SET v=55 WHERE id=1;\nGO\nCOMMIT;\nGO\nSELECT COUNT(*) AS total FROM #script_values;`,
        undefined,
        { hardCap },
      );
      expect(result.columns).toEqual(["total"]);
      expect(result.rows).toEqual([{ __col_0: 3 }]);
      expect(await values()).toEqual([55, 0]);
      const empty = await driver.query(
        "SELECT 1 AS [empty] WHERE 1=0",
        undefined,
        { hardCap },
      );
      expect(empty.columns).toEqual(["empty"]);
      expect(empty.rows).toEqual([]);
    });
  });

  it("rejects parameterless unbalanced success and preserves typed hostile-looking parameters", async () => {
    await withDriver(async (driver) => {
      await expect(
        driver.query(`BEGIN TRAN; UPDATE ${table} SET v=66 WHERE id=1;`),
      ).rejects.toThrow("open transaction");
      expect(await values()).toEqual([0, 0]);
      const text = "x'; DROP TABLE imaginary; --";
      expect(
        (
          await driver.query(
            "SELECT ? AS text_value, ? AS numeric_value",
            [text, 123],
            { hardCap: 1 },
          )
        ).rows,
      ).toEqual([{ __col_0: text, __col_1: 123 }]);
    });
  });

  it.each([
    "cancel",
    "signal",
    "timeout",
    "deadline",
  ])("cleans an interrupted transaction without poisoning the next request (%s)", async (kind) => {
    await withDriver(
      async (driver, pool) => {
        const controller = new AbortController();
        const failure = driver.query(
          `SET XACT_ABORT OFF; BEGIN TRAN; UPDATE ${table} SET v=77 WHERE id=1; WAITFOR DELAY '00:00:08';`,
          undefined,
          {
            hardCap: 1,
            requestToken: 17,
            signal: controller.signal,
            deadline: kind === "deadline" ? Date.now() + 250 : undefined,
          },
        );
        const rejected = expect(failure).rejects.toThrow();
        await until(() => pool.borrowed === 1);
        if (kind === "cancel" || kind === "signal") {
          await new Promise((resolve) => setTimeout(resolve, 100));
          if (kind === "cancel")
            await driver.cancelCurrentOperation({
              operationName: "query",
              reason: "manual",
              requestToken: 17,
            });
          else controller.abort();
        }
        await rejected;
        expect(await values()).toEqual([0, 0]);
        await driver.runTransaction([
          {
            sql: `UPDATE ${table} SET v=99 WHERE id=2`,
            expectedAffectedRows: 1,
          },
        ]);
        expect(await values()).toEqual([0, 99]);
        expect(pool.connected).toBe(true);
      },
      kind === "timeout" ? 250 : 5000,
    );
  });

  it("targeted cancellation leaves another active physical session running", async () => {
    await withDriver(
      async (driver, pool) => {
        const first = driver.query(
          `SET XACT_ABORT OFF; BEGIN TRAN; UPDATE ${table} SET v=77 WHERE id=1; WAITFOR DELAY '00:00:08';`,
          undefined,
          { requestToken: 71, hardCap: 1 },
        );
        const rejected = expect(first).rejects.toThrow();
        const independent = driver.query(
          "WAITFOR DELAY '00:00:00.400'; SELECT 123 AS value",
          undefined,
          { requestToken: 72, hardCap: 1 },
        );
        await until(() => pool.borrowed === 2);
        await driver.cancelCurrentOperation({
          operationName: "query",
          requestToken: 71,
          reason: "manual",
        });
        await rejected;
        expect((await independent).rows).toEqual([{ __col_0: 123 }]);
        expect(await values()).toEqual([0, 0]);
        expect(pool.connected).toBe(true);
        expect(pool.borrowed).toBe(0);
      },
      5000,
      2,
    );
  });
});
