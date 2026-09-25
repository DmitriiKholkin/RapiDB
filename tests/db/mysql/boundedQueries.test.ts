import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../../src/extension/dbDrivers/mysql";
import type { IDBDriver } from "../../../src/extension/dbDrivers/types";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import {
  connectLiveDriverViaManager,
  disposeManagedLiveDriverSession,
  type ManagedLiveDriverSession,
  withTrustOnFirstUseSsh,
} from "../../support/liveSshManagerHarness";

vi.mock("vscode", () => ({
  EventEmitter: class {
    private listeners = new Set<(value: unknown) => void>();
    event = (listener: (value: unknown) => void) => {
      this.listeners.add(listener);
      return { dispose: () => this.listeners.delete(listener) };
    };
    fire(value: unknown) {
      for (const listener of this.listeners) listener(value);
    }
    dispose() {
      this.listeners.clear();
    }
  },
}));

for (const transport of ["direct", "ssh"] as const) {
  describe(`MySQL native bounded collection (${transport})`, () => {
    let driver: IDBDriver;
    let session: ManagedLiveDriverSession | undefined;
    const suffix = `${process.pid}_${transport}_${Date.now()}`;
    const table = `rapidb_cap_${suffix}`;
    const procedure = `rapidb_cap_proc_${suffix}`;
    const brokenProcedure = `rapidb_cap_err_${suffix}`;
    const size = 12050;

    beforeAll(async () => {
      const config = await resolveConnectionSeed("mysql");
      if (transport === "ssh") {
        session = await connectLiveDriverViaManager(
          withTrustOnFirstUseSsh({
            ...config,
            id: `mysql-cap-${suffix}`,
            host: "mysql",
          }),
        );
        driver = session.driver;
      } else {
        driver = new MySQLDriver(config);
        await driver.connect();
      }
      await driver.query(`CREATE TABLE ${table} (id INT PRIMARY KEY, hits INT NOT NULL DEFAULT 0);
        SET SESSION cte_max_recursion_depth = 15000;
        INSERT INTO ${table} (id) WITH RECURSIVE seq AS
          (SELECT 1 AS n UNION ALL SELECT n+1 FROM seq WHERE n < ${size}) SELECT n FROM seq`);
      await driver.query(`CREATE PROCEDURE ${procedure}(IN delta INT)
        BEGIN
          SELECT id AS same, id + delta AS same, REPEAT('x',1024) AS payload FROM ${table} ORDER BY id;
          UPDATE ${table} SET hits = hits + 1;
          SELECT id AS same, hits AS same FROM ${table} ORDER BY id DESC;
          UPDATE ${table} SET hits = hits + 1;
        END`);
      await driver.query(`CREATE PROCEDURE ${brokenProcedure}()
        BEGIN
          SELECT id FROM ${table};
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'late bounded failure';
        END`);
    });

    afterAll(async () => {
      try {
        if (driver) {
          await driver.query(`DROP PROCEDURE IF EXISTS ${procedure};
            DROP PROCEDURE IF EXISTS ${brokenProcedure}; DROP TABLE IF EXISTS ${table}`);
        }
      } finally {
        if (session) await disposeManagedLiveDriverSession(session);
        else await driver?.disconnect();
      }
    });

    it("drains large positional CALL results and completes every mutation after exhausting the display budget", async () => {
      await driver.query(`UPDATE ${table} SET hits = 0`);
      const result = await driver.query(`CALL ${procedure}(?)`, [10], {
        hardCap: 3,
      });
      expect(driver.getCapabilities?.().boundedQueryResults).toBe(true);
      expect(result).toMatchObject({
        columns: ["same", "same"],
        rowCount: size,
        truncated: true,
        rows: [
          { __col_0: size, __col_1: 1 },
          { __col_0: size - 1, __col_1: 1 },
          { __col_0: size - 2, __col_1: 1 },
        ],
      });
      const completed = await driver.query(
        `SELECT COUNT(*) FROM ${table} WHERE hits = 2`,
      );
      expect(Number(completed.rows[0].__col_0)).toBe(size);
    });

    it("preserves last-result script selection, drains later mutations, and counts mutation-only scripts", async () => {
      const result = await driver.query(
        `SELECT id FROM ${table} ORDER BY id;
        UPDATE ${table} SET hits = 10; SELECT id AS same, hits AS same FROM ${table} ORDER BY id DESC;
        UPDATE ${table} SET hits = 11`,
        undefined,
        { hardCap: 2 },
      );
      expect(result).toMatchObject({
        columns: ["same", "same"],
        rowCount: size,
        truncated: true,
      });
      expect(result.rows).toEqual([
        { __col_0: size, __col_1: 10 },
        { __col_0: size - 1, __col_1: 10 },
      ]);
      const completed = await driver.query(
        `SELECT COUNT(*) FROM ${table} WHERE hits = 11`,
      );
      expect(Number(completed.rows[0].__col_0)).toBe(size);
      const mutations = await driver.query(
        `UPDATE ${table} SET hits=12; UPDATE ${table} SET hits=13`,
        undefined,
        { hardCap: 1 },
      );
      expect(mutations).toMatchObject({
        rows: [],
        rowCount: size * 2,
        affectedRows: size * 2,
        truncated: false,
      });
    });

    it("handles SHOW, EXPLAIN and locking SQL using native result metadata and counts", async () => {
      for (const sql of [
        "SHOW VARIABLES",
        `EXPLAIN SELECT * FROM ${table}`,
        `SELECT id FROM ${table} ORDER BY id FOR UPDATE`,
      ]) {
        const full = await driver.query(sql);
        const bounded = await driver.query(sql, undefined, { hardCap: 2 });
        expect(bounded.columns).toEqual(full.columns);
        expect(bounded.rows).toEqual(full.rows.slice(0, 2));
        expect(bounded.rowCount).toBe(full.rowCount);
        expect(bounded.truncated).toBe(full.rowCount > 2);
      }
    });

    it("keeps empty duplicate columns, exact-at-cap metadata, and parameter value semantics", async () => {
      const empty = await driver.query(
        `SELECT id AS same, hits AS same FROM ${table} WHERE 0`,
        undefined,
        { hardCap: 2 },
      );
      expect(empty).toMatchObject({
        columns: ["same", "same"],
        rows: [],
        rowCount: 0,
        truncated: false,
      });
      const sql =
        "SELECT ? AS same, CAST(? AS DECIMAL(24,4)) AS same, ? AS nullable";
      const params = ["O'Reilly; \\ quoted", "9007199254740993.1250", null];
      const full = await driver.query(sql, params);
      const bounded = await driver.query(sql, params, { hardCap: 1 });
      expect(bounded.columns).toEqual(full.columns);
      expect(bounded.rows).toEqual(full.rows);
      expect(bounded).toMatchObject({ rowCount: 1, truncated: false });
    });

    it("propagates a server error after discarding rows and recovers on the next request", async () => {
      await expect(
        driver.query(`CALL ${brokenProcedure}()`, undefined, { hardCap: 2 }),
      ).rejects.toThrow(/late bounded failure/);
      expect(
        (await driver.query("SELECT 123", undefined, { hardCap: 1 })).rows,
      ).toEqual([{ __col_0: "123" }]);
    });

    it("times out a draining script and recovers without running its later mutation", async () => {
      const timeout = vi
        .spyOn(
          driver as unknown as { getDbOperationTimeoutMs(): number },
          "getDbOperationTimeoutMs",
        )
        .mockReturnValue(100);
      try {
        await expect(
          driver.query(
            `SELECT id FROM ${table}; SELECT SLEEP(5); UPDATE ${table} SET hits=999`,
            undefined,
            { hardCap: 2, requestToken: 501 },
          ),
        ).rejects.toThrow(/timeout|timed out/i);
      } finally {
        timeout.mockRestore();
      }
      const recovered = await driver.query(
        `SELECT COUNT(*) FROM ${table} WHERE hits=999`,
        undefined,
        { hardCap: 1 },
      );
      expect(recovered.rows).toEqual([{ __col_0: "0" }]);
    });

    it("cancels targeted requests repeatedly without leaking pool slots or stopping unrelated work", async () => {
      for (let i = 0; i < 6; i++) {
        const token = 600 + i;
        const pending = driver.query(
          `SELECT id FROM ${table}; SELECT SLEEP(5); UPDATE ${table} SET hits=998`,
          undefined,
          { hardCap: 2, requestToken: token },
        );
        const outcome = pending.catch((error: unknown) => error);
        await new Promise((resolve) => setTimeout(resolve, 100));
        const unrelated = driver.query("SELECT 123", undefined, {
          hardCap: 1,
          requestToken: token + 100,
        });
        await driver.cancelCurrentOperation?.({
          operationName: "query",
          requestToken: token,
          reason: "manual",
        });
        expect(await outcome).toBeInstanceOf(Error);
        expect(String(await outcome)).toMatch(/cancel/i);
        expect((await unrelated).rows).toEqual([{ __col_0: "123" }]);
      }
      const recovered = await driver.query(
        `SELECT COUNT(*) FROM ${table} WHERE hits=998`,
        undefined,
        { hardCap: 1 },
      );
      expect(recovered.rows).toEqual([{ __col_0: "0" }]);
    });
  });
}
