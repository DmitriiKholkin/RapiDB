import { randomUUID } from "node:crypto";
import type { Pool } from "mysql2/promise";
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../../src/extension/connectionManager";
import { MySQLDriver } from "../../../src/extension/dbDrivers/mysql";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableDataService } from "../../../src/extension/tableDataService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

const timezoneSql = "SET SESSION time_zone = 'America/New_York'";
const early = "1730611800.123456";
const late = "1730615400.123456";
const wallClock = "2024-11-03 01:30:00.123456";

async function withTable(
  definition: string,
  test: (fixture: {
    driver: MySQLDriver;
    manager: ConnectionManager;
    service: TableDataService;
    scope: [string, string, string, string];
    table: string;
  }) => Promise<void>,
) {
  const config = await resolveConnectionSeed("mysql");
  const driver = new MySQLDriver(config);
  const table = `timestamp_key_${randomUUID().replaceAll("-", "")}`;
  await driver.connect();
  try {
    const pool = (driver as unknown as { pool: Pool }).pool;
    // Only the isolated test driver's sessions change zone; never SET GLOBAL.
    pool.on("connection", (connection) => connection.query(timezoneSql));
    const connection = await pool.getConnection();
    try {
      await connection.query(timezoneSql);
    } finally {
      connection.release();
    }
    await driver.query(`CREATE TABLE ${table} (${definition}) ENGINE=InnoDB`);
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    await test({
      driver,
      manager,
      service: new TableDataService(manager),
      scope: [config.id, config.database ?? "", "", table],
      table,
    });
  } finally {
    try {
      await driver.query(`DROP TABLE IF EXISTS ${table}`);
    } finally {
      await driver.disconnect();
    }
  }
}

describe("MySQL TIMESTAMP primary-key identity (live)", () => {
  it("reads distinct stored instants; prepares exact updates and deletes; rejects stale wall-clock keys", async () => {
    await withTable(
      "id TIMESTAMP(6) PRIMARY KEY, amount INT",
      async ({ driver, manager, service, scope, table }) => {
        await driver.query(
          `INSERT INTO ${table} VALUES
            ('2024-11-03 05:30:00.123456+00:00', 1),
            ('2024-11-03 06:30:00.123456+00:00', 1),
            ('2024-11-03 06:30:00.123457+00:00', 1)`,
        );
        const stored = async () =>
          (
            await driver.query(
              `SELECT UNIX_TIMESTAMP(id), amount FROM ${table} ORDER BY id`,
            )
          ).rows.map(({ __col_0, __col_1 }) => [__col_0, __col_1]);
        const page = await service.getPage(...scope, 1, 10, []);
        expect(page.rows).toEqual([
          { id: wallClock, amount: 1 },
          { id: wallClock, amount: 1 },
          { id: "2024-11-03 01:30:00.123457", amount: 1 },
        ]);
        const keys = page.mysqlTimestampRowIdentities;
        expect(keys).toEqual([
          { id: { $rapidbMySqlTimestampKey: early } },
          { id: { $rapidbMySqlTimestampKey: late } },
          { id: { $rapidbMySqlTimestampKey: "1730615400.123457" } },
        ]);
        if (!keys) throw new Error("Missing projected MySQL keys");

        expect(() =>
          prepareApplyChangesPlan(
            manager,
            ...scope,
            [{ primaryKeys: { id: wallClock }, changes: { amount: 2 } }],
            page.columns,
          ),
        ).toThrow(/Refresh the table.*exact stored identity/);
        await expect(
          service.prepareDeleteRowsPlan(...scope, [{ id: wallClock }]),
        ).rejects.toThrow(/Refresh the table.*exact stored identity/);

        const apply = async (
          changes: Record<string, unknown>,
          originalValues: Record<string, unknown>,
        ) => {
          const preparation = prepareApplyChangesPlan(
            manager,
            ...scope,
            [{ primaryKeys: keys[1], changes, originalValues }],
            page.columns,
          );
          if (!preparation.executable) throw new Error("No prepared UPDATE");
          expect(preparation.plan.previewStatements[0]).toContain(
            `UNIX_TIMESTAMP(\`id\`) = CAST('${late}' AS DECIMAL(20,6))`,
          );
          return executePreparedApplyPlan(manager, preparation.plan);
        };
        expect(await apply({ amount: 2 }, page.rows[1])).toMatchObject({
          success: true,
        });
        expect(await stored()).toEqual([
          [early, 1],
          [late, 2],
          ["1730615400.123457", 1],
        ]);
        expect(await apply({ amount: 3 }, page.rows[1])).toMatchObject({
          success: false,
        });
        const occupied = await apply(
          { id: wallClock, amount: 4 },
          { ...page.rows[1], amount: 2 },
        );
        expect(occupied).toMatchObject({
          success: false,
          changesPossible: false,
          error: expect.stringContaining("already existing primary key"),
        });
        // Verification uses the exact newly written instant, rather than the
        // late row with the same wall clock one microsecond from the old key.
        expect(
          await apply(
            { id: "2024-11-03 01:30:00.123457", amount: 4 },
            { ...page.rows[1], amount: 2 },
          ),
        ).toMatchObject({ success: true });
        expect(await stored()).toEqual([
          [early, 1],
          ["1730611800.123457", 4],
          ["1730615400.123457", 1],
        ]);

        const deletion = await service.prepareDeleteRowsPlan(...scope, [
          keys[2],
        ]);
        if (!deletion) throw new Error("No prepared DELETE");
        await service.executePreparedDeletePlan(deletion);
        expect(await stored()).toEqual([
          [early, 1],
          ["1730611800.123457", 4],
        ]);

        const current = await service.getPage(...scope, 1, 10, []);
        const exported = [];
        for await (const chunk of service.exportAll(...scope, 1)) {
          exported.push(...chunk.rows);
        }
        expect(exported).toEqual(current.rows);
        expect(
          (await driver.query(`SELECT id FROM ${table} ORDER BY id`)).rows[0],
        ).toEqual({ __col_0: wallClock });
      },
    );
  });

  it("verifies an insert against the instant written even when the other side of the overlap exists", async () => {
    await withTable(
      "id TIMESTAMP(6) PRIMARY KEY, amount INT",
      async ({ driver, service, scope, table }) => {
        await driver.query(
          `INSERT INTO ${table} VALUES ('2024-11-03 06:30:00.123456+00:00', 2)`,
        );
        await service.insertRow(...scope, { id: wallClock, amount: 1 });
        await expect(
          service.insertRow(...scope, { id: wallClock, amount: 9 }),
        ).rejects.toThrow(/already existing primary key/);
        await expect(
          service.insertRow(...scope, {
            id: "2024-11-03 06:45:00.123456+00:00",
            amount: 3,
          }),
        ).rejects.toThrow(/repeated local hour/);
        expect(
          (
            await driver.query(
              `SELECT UNIX_TIMESTAMP(id), amount FROM ${table} ORDER BY id`,
            )
          ).rows,
        ).toEqual([
          { __col_0: early, __col_1: 1 },
          { __col_0: late, __col_1: 2 },
        ]);
      },
    );
  });

  it("captures an exact TIMESTAMP component alongside a generated auto-increment key", async () => {
    await withTable(
      "n INT AUTO_INCREMENT, id TIMESTAMP(6) DEFAULT '2024-11-03 06:30:00.123456+00:00', amount INT, PRIMARY KEY (n, id)",
      async ({ service, scope }) => {
        await service.insertRow(...scope, { amount: 5 });
        const page = await service.getPage(...scope, 1, 10, []);
        expect(page.rows).toEqual([{ n: 1, id: wallClock, amount: 5 }]);
        expect(page.mysqlTimestampRowIdentities).toEqual([
          { id: { $rapidbMySqlTimestampKey: late } },
        ]);
      },
    );
  });

  it("verifies ordinary lower-precision local writes using the column's rounding", async () => {
    await withTable(
      "id TIMESTAMP(3) PRIMARY KEY, amount INT",
      async ({ service, scope }) => {
        await service.insertRow(...scope, {
          id: "2024-11-03 01:59:59.999999",
          amount: 5,
        });
        expect(await service.getPage(...scope, 1, 10, [])).toMatchObject({
          rows: [{ id: "2024-11-03 02:00:00", amount: 5 }],
          mysqlTimestampRowIdentities: [
            { id: { $rapidbMySqlTimestampKey: "1730617200.000000" } },
          ],
        });
      },
    );
  });
});
