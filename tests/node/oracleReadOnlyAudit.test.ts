import { randomUUID } from "node:crypto";
import oracledb from "oracledb";
import { describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { TableReadService } from "../../src/extension/table/tableReadService";
import {
  decideReadOnlyQueryExecution,
  sqlReadOnlyQueryGuard,
} from "../../src/extension/utils/readOnlyGuards";

const reason =
  "[RapiDB] Read-only Oracle query editor execution is blocked: autonomous transactions can commit writes even inside SET TRANSACTION READ ONLY. Use database-enforced read-only permissions with a regular connection to run queries.";

describe("Oracle read-only autonomous transaction boundary", () => {
  it.each([
    "SELECT audit_touch() FROM dual",
    'SELECT "APP"."AUDIT_TOUCH"() FROM dual',
    "SELECT * FROM audit_view",
    "SELECT * FROM seemingly_plain_table",
    "SELECT 1 FROM dual",
    "WITH src AS (SELECT 1 FROM dual) SELECT * FROM src",
    "SELECT seq.NEXTVAL FROM dual",
    "BEGIN audit_touch; END;",
  ])("fails closed instead of classifying arbitrary Oracle SQL: %s", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "plsql")).toEqual({
      allowed: false,
      reason,
    });
  });

  it.each([
    undefined,
    { readOnlyQueryGuard: () => ({ allowed: true as const }) },
  ])("denies editor execution from connection settings even without trustworthy capabilities: %j", (capabilities) => {
    const manager = {
      getConnection: () => ({ type: "oracle", readOnly: true }),
      getDriverCapabilities: () => capabilities,
    };
    expect(
      decideReadOnlyQueryExecution(
        manager as never,
        "oracle",
        "SELECT 1 FROM dual",
      ),
    ).toEqual({ allowed: false, reason });
    expect(
      decideReadOnlyQueryExecution(
        { getConnection: () => ({ type: "oracle", readOnly: false }) } as never,
        "oracle",
        "SELECT audit_touch() FROM dual",
      ),
    ).toEqual({ allowed: true });
  });

  it("publishes the fail-closed contract in Oracle driver capabilities", () => {
    const driver = new OracleDriver({
      id: "oracle",
      name: "Oracle",
      type: "oracle",
    });
    expect(
      driver.getCapabilities().readOnlyQueryGuard?.("SELECT 1 FROM dual"),
    ).toEqual({
      allowed: false,
      reason,
    });
  });

  it.each([
    { params: undefined },
    { params: [1] },
  ])("rejects direct readOnly query before acquiring a session (binds: %j)", async ({
    params,
  }) => {
    const driver = new OracleDriver({
      id: "oracle",
      name: "Oracle",
      type: "oracle",
    });
    const getConnection = vi.fn();
    (driver as unknown as { pool: unknown }).pool = { getConnection };
    await expect(
      driver.query("SELECT audit_touch() FROM dual", params, {
        readOnly: true,
      }),
    ).rejects.toThrow(reason);
    expect(getConnection).not.toHaveBeenCalled();
  });
});

// Opt-in only: point this at a disposable Oracle schema, never a production DB.
const livePort = process.env.RAPIDB_ORACLE_READONLY_AUDIT_PORT;
describe.skipIf(!livePort)(
  "live Oracle read-only autonomous transaction audit",
  () => {
    it("proves rollback is insufficient and checks the driver boundary without breaking table reads", async () => {
      const config = {
        id: "oracle-readonly-audit",
        name: "Oracle audit",
        type: "oracle" as const,
        host: "127.0.0.1",
        port: Number(livePort),
        serviceName: "FREEPDB1",
        username: "rapidb_audit",
        password: "RapidbAudit7f29",
        readOnly: true,
      };
      const setup = await oracledb.getConnection({
        user: config.username,
        password: config.password,
        connectString: `${config.host}:${config.port}/${config.serviceName}`,
      });
      const driver = new OracleDriver(config);
      const suffix = randomUUID().replaceAll("-", "").slice(0, 8);
      const table = `AUDIT_COUNTER_${suffix.toUpperCase()}`;
      const routine = `AUDIT_TOUCH_${suffix.toUpperCase()}`;
      let tableCreated = false;
      let routineCreated = false;
      try {
        await setup.execute(`CREATE TABLE ${table} (n NUMBER(10,0) NOT NULL)`);
        tableCreated = true;
        await setup.execute(`INSERT INTO ${table} VALUES (0)`);
        await setup.commit();
        await setup.execute(`CREATE FUNCTION ${routine} RETURN NUMBER IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        BEGIN
          UPDATE ${table} SET n = n + 1;
          COMMIT;
          RETURN 1;
        END;`);
        routineCreated = true;
        const counter = async () => {
          const result = await setup.execute<[number]>(
            `SELECT n FROM ${table}`,
          );
          return result.rows?.[0]?.[0];
        };
        expect(await counter()).toBe(0);
        await setup.execute("SET TRANSACTION READ ONLY");
        await setup.execute(`SELECT ${routine}() FROM dual`);
        await setup.rollback();
        expect(await counter()).toBe(1);

        await driver.connect();
        const before = await counter();
        let error: unknown;
        try {
          await driver.query(`SELECT ${routine}() FROM dual`, undefined, {
            readOnly: true,
          });
        } catch (caught) {
          error = caught;
        }
        const after = await counter();
        console.info("Oracle autonomous transaction evidence", {
          rawRollbackCounter: 1,
          before,
          after,
        });
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toBe(reason);
        expect(after).toBe(before);

        const reads = new TableReadService({
          getConnection: () => config,
          getDriver: () => driver,
        } as never);
        expect(
          await reads.getPage(
            config.id,
            "FREEPDB1",
            "RAPIDB_AUDIT",
            table,
            1,
            25,
            [],
          ),
        ).toMatchObject({
          totalCount: 1,
          rows: [{ N: 1 }],
        });
        expect(await driver.listSchemas("FREEPDB1")).toContainEqual({
          name: "RAPIDB_AUDIT",
        });
      } finally {
        await driver.disconnect();
        if (routineCreated) await setup.execute(`DROP FUNCTION ${routine}`);
        if (tableCreated) await setup.execute(`DROP TABLE ${table} PURGE`);
        await setup.close();
      }
    }, 60_000);
  },
);
