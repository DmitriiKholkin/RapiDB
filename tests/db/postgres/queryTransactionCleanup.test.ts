import { Client, type Pool } from "pg";
import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { PgReadyForQueryProxy } from "../../support/pgReadyForQueryProxy";

async function connectionConfig() {
  const config = await resolveConnectionSeed("postgres");
  const override = process.env.RAPIDB_DATA_SAFETY_POSTGRES_URI;
  if (override) {
    const url = new URL(override);
    Object.assign(config, {
      host: url.hostname,
      port: Number(url.port),
      username: decodeURIComponent(url.username),
      password: decodeURIComponent(url.password),
      database: url.pathname.slice(1),
    });
  }
  return config;
}

describe("PostgreSQL pooled query transaction cleanup (live)", () => {
  it.each([
    undefined,
    10,
  ])("never commits a previous incomplete query from runTransaction (hardCap: %s)", async (hardCap) => {
    const config = await connectionConfig();
    const driver = new PostgresDriver(config);
    const observer = new Client({
      host: config.host,
      port: config.port,
      user: config.username,
      password: config.password,
      database: config.database,
    });
    const table = `rapidb_tx_cleanup_${Date.now()}_${process.pid}_${hardCap ?? "unbounded"}`;
    try {
      await driver.connect();
      await observer.connect();
      await driver.query(
        `CREATE TABLE ${table} (id integer PRIMARY KEY, value text)`,
      );
      await driver.query(
        `INSERT INTO ${table} VALUES (1, 'original'), (2, 'original')`,
      );
      await expect(
        driver.query(
          `BEGIN; UPDATE ${table} SET value='pending' WHERE id=1`,
          [],
          { hardCap },
        ),
      ).rejects.toThrow(/unfinished transaction.*rolled back/);
      expect(
        (await observer.query(`SELECT value FROM ${table} WHERE id=1`)).rows[0]
          .value,
      ).toBe("original");
      // pg-pool uses its newest idle client first: ensure cleanup retains it,
      // then exercise precisely the old cross-operation COMMIT reproduction.
      const pool = (
        driver as unknown as {
          pool: {
            connect: () => Promise<{
              getTransactionStatus: () => string;
              release: () => void;
            }>;
          };
        }
      ).pool;
      const client = await pool.connect();
      try {
        expect(client.getTransactionStatus()).toBe("I");
      } finally {
        client.release();
      }
      await driver.runTransaction([
        { sql: `UPDATE ${table} SET value='other' WHERE id=2` },
      ]);
      expect(
        (
          await observer.query(`SELECT value FROM ${table} ORDER BY id`)
        ).rows.map((row) => row.value),
      ).toEqual(["original", "other"]);
      await driver.query(
        `BEGIN; UPDATE ${table} SET value='committed' WHERE id=1; COMMIT`,
        [],
        { hardCap },
      );
      expect(
        (await observer.query(`SELECT value FROM ${table} WHERE id=1`)).rows[0]
          .value,
      ).toBe("committed");
      await expect(
        driver.query("BEGIN; SELECT 1/0", [], { hardCap }),
      ).rejects.toThrow(/division by zero/);
      expect(
        (await driver.query("SELECT 42", [], { hardCap })).rows[0].__col_0,
      ).toBe(42);
    } finally {
      await observer
        .query(`DROP TABLE IF EXISTS ${table}`)
        .catch(() => undefined);
      await observer.end();
      await driver.disconnect();
    }
  });

  it.each([
    "E",
    "T",
  ] as const)("discards a client with stale idle status when ReadyForQuery %s is withheld", async (status) => {
    const config = await connectionConfig();
    const proxy = new PgReadyForQueryProxy(
      config.host ?? "127.0.0.1",
      config.port ?? 5432,
    );
    const port = await proxy.listen();
    const driver = new PostgresDriver(
      { ...config, host: "127.0.0.1", port },
      () => ({
        connectionTimeoutMs: 5000,
        connectionTimeoutSeconds: 5,
        dbOperationTimeoutMs: 500,
        dbOperationTimeoutSeconds: 1,
      }),
    );
    const observer = new Client({
      host: config.host,
      port: config.port,
      user: config.username,
      password: config.password,
      database: config.database,
    });
    const table = `rapidb_tx_delayed_${Date.now()}_${process.pid}_${status.toLowerCase()}`;
    try {
      await observer.connect();
      await observer.query(
        `CREATE TABLE ${table} (id integer PRIMARY KEY, value text)`,
      );
      await observer.query(
        `INSERT INTO ${table} VALUES (1, 'original'), (2, 'original')`,
      );
      await driver.connect();
      const pid = (await driver.query("SELECT pg_backend_pid()")).rows[0]
        .__col_0;
      const pool = (driver as unknown as { pool: Pool }).pool;
      const oldClient = await pool.connect();
      oldClient.release();
      proxy.pauseNextTransaction(status);
      const pending = driver.query(
        `BEGIN; UPDATE ${table} SET value='pending' WHERE id=1${status === "E" ? "; SELECT 1/0" : ""}`,
      );
      // Attach before waiting for the proxy: ErrorResponse can reject first.
      const rejected = expect(pending).rejects.toThrow(
        status === "E" ? /division by zero/ : /read timeout/i,
      );
      await expect.poll(() => proxy.held, { timeout: 3000 }).toBe(true);
      await rejected;
      expect(
        (
          oldClient as unknown as { getTransactionStatus: () => string }
        ).getTransactionStatus(),
      ).toBe("I");
      expect(pool.idleCount).toBe(0);
      // An unrelated transaction must use a fresh socket even while the old
      // transaction's ReadyForQuery reply remains withheld.
      await driver.runTransaction([
        { sql: `UPDATE ${table} SET value='other' WHERE id=2` },
      ]);
      proxy.releaseHeld();
      expect(
        (await driver.query("SELECT pg_backend_pid()")).rows[0].__col_0,
      ).not.toBe(pid);
      expect(
        (
          await observer.query(`SELECT value FROM ${table} ORDER BY id`)
        ).rows.map((row) => row.value),
      ).toEqual(["original", "other"]);
      await observer.query("SET lock_timeout = '2s'");
      await observer.query(`UPDATE ${table} SET value='unlocked' WHERE id=1`);
    } finally {
      proxy.releaseHeld();
      await driver.disconnect();
      await proxy.close();
      await observer
        .query(`DROP TABLE IF EXISTS ${table}`)
        .catch(() => undefined);
      await observer.end();
    }
  });
});
