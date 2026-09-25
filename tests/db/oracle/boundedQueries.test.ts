import oracledb from "oracledb";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../../src/extension/dbDrivers/oracle";
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
    event = () => ({ dispose() {} });
    fire() {}
    dispose() {}
  },
}));

for (const transport of ["direct", "ssh"] as const) {
  describe(`Oracle bounded results (${transport})`, () => {
    let driver: IDBDriver;
    let session: ManagedLiveDriverSession | undefined;
    const table = `CAP_${process.pid}_${transport}`.toUpperCase();
    beforeAll(async () => {
      const config = await resolveConnectionSeed("oracle");
      if (transport === "ssh") {
        session = await connectLiveDriverViaManager(
          withTrustOnFirstUseSsh({ ...config, host: "oracle" }),
        );
        driver = session.driver;
      } else {
        driver = new OracleDriver(config);
        await driver.connect();
      }
      await driver.query(
        `CREATE TABLE ${table} AS SELECT level id, 0 hits FROM dual CONNECT BY level <= 12050`,
      );
    });
    afterAll(async () => {
      try {
        await driver?.query(`DROP TABLE ${table} PURGE`);
      } finally {
        if (session) await disposeManagedLiveDriverSession(session);
        else await driver?.disconnect();
      }
    });
    it("preserves empty and duplicate columns and exact counts", async () => {
      const result = await driver.query(
        `SELECT id same, id+1 same FROM ${table} ORDER BY id`,
        undefined,
        { hardCap: 3 },
      );
      expect(result).toMatchObject({
        columns: ["SAME", "SAME"],
        rowCount: 12050,
        truncated: true,
      });
      expect(result.rows).toHaveLength(3);
      expect(result.rows[0]).toEqual({ __col_0: "1", __col_1: 2 });
      expect(
        await driver.query(
          "SELECT 1 same, 2 same FROM dual WHERE 1=0",
          undefined,
          { hardCap: 3 },
        ),
      ).toMatchObject({ columns: ["SAME", "SAME"], rows: [], rowCount: 0 });
    });
    it("drains implicit results and completes trailing mutation and q/nq blocks", async () => {
      const result = await driver.query(
        `DECLARE c SYS_REFCURSOR; s NVARCHAR2(100) := nq'[it's; / ?]';
        BEGIN
          FOR i IN 1..20 LOOP
            OPEN c FOR SELECT id same, hits same FROM ${table};
            DBMS_SQL.RETURN_RESULT(c);
          END LOOP;
        END;
        /
        UPDATE ${table} SET hits=hits+1;`,
        undefined,
        { hardCap: 2 },
      );
      expect(result).toMatchObject({ rowCount: 12050, truncated: true });
      expect(result.rows).toHaveLength(2);
      expect(
        (await driver.query(`SELECT COUNT(*) n FROM ${table} WHERE hits=1`))
          .rows[0].__col_0,
      ).toBe(12050);
    });
    it("supports REF CURSOR descriptors and RETURNING binds without limiting DML", async () => {
      const cursor = await driver.query(
        `BEGIN OPEN :1 FOR SELECT id FROM ${table}; END;`,
        [{ dir: oracledb.BIND_OUT, type: oracledb.CURSOR }],
        { hardCap: 2 },
      );
      expect(cursor).toMatchObject({ rowCount: 12050, truncated: true });
      const result = await driver.query(
        `UPDATE ${table} SET hits=hits+1 RETURNING id INTO :1`,
        [{ dir: oracledb.BIND_OUT, type: oracledb.NUMBER }],
        { hardCap: 2 },
      );
      expect(result).toMatchObject({ affectedRows: 12050, rowCount: 12050 });
      await expect(
        driver.query(
          `UPDATE ${table} SET hits=hits+1 RETURNING id`,
          undefined,
          { hardCap: 2 },
        ),
      ).rejects.toThrow();
    });
    it("rolls back late script errors after oversized results and recovers", async () => {
      await expect(
        driver.query(
          `UPDATE ${table} SET hits=99; SELECT * FROM ${table}; BEGIN raise_application_error(-20001, 'late failure'); END;`,
          undefined,
          { hardCap: 1 },
        ),
      ).rejects.toThrow("late failure");
      expect(
        (await driver.query(`SELECT COUNT(*) FROM ${table} WHERE hits=99`))
          .rows[0].__col_0,
      ).toBe(0);
    });
    it("recovers after a native call timeout", async () => {
      const timeout = vi
        .spyOn(
          driver as unknown as { getDbOperationTimeoutMs(): number },
          "getDbOperationTimeoutMs",
        )
        .mockReturnValue(1000);
      try {
        await expect(
          driver.query("BEGIN DBMS_SESSION.SLEEP(3); END;", undefined, {
            hardCap: 1,
          }),
        ).rejects.toThrow();
      } finally {
        timeout.mockRestore();
      }
      expect((await driver.query("SELECT 1 FROM dual")).rowCount).toBe(1);
    });
    it("cancels an active native block and recovers the pool", async () => {
      const pending = driver.query(
        "BEGIN DBMS_SESSION.SLEEP(2); END;",
        undefined,
        { requestToken: 77, hardCap: 1 },
      );
      const rejected = expect(pending).rejects.toThrow();
      await new Promise((resolve) => setTimeout(resolve, 300));
      await driver.cancelCurrentOperation?.({
        operationName: "query",
        requestToken: 77,
        reason: "timeout",
      });
      await rejected;
      expect((await driver.query("SELECT 1 FROM dual")).rowCount).toBe(1);
    });
  });
}
