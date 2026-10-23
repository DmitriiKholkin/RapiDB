import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../../src/extension/connectionManager";
import { MySQLDriver } from "../../../src/extension/dbDrivers/mysql";
import { applyChangesTransactional } from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

async function withTable(
  test: (
    driver: MySQLDriver,
    table: string,
    config: Awaited<ReturnType<typeof resolveConnectionSeed>>,
  ) => Promise<void>,
) {
  const config = await resolveConnectionSeed("mysql");
  const driver = new MySQLDriver(config);
  const table = `tinyint_identity_${randomUUID().replaceAll("-", "")}`;
  await driver.connect();
  try {
    await driver.query(
      `CREATE TABLE ${table} (id TINYINT(1) PRIMARY KEY, payload VARCHAR(30))`,
    );
    await driver.query(
      `INSERT INTO ${table} VALUES (0,'zero'),(1,'one'),(2,'two'),(-1,'negative')`,
    );
    await test(driver, table, config);
  } finally {
    await driver.query(`DROP TABLE IF EXISTS ${table}`);
    await driver.disconnect();
  }
}

describe("MySQL TINYINT(1) is an integer storage type, not a boolean constraint", () => {
  it("inserts and filters negative keys through the Viewer services", async () => {
    await withTable(async (driver, table, config) => {
      const manager = {
        getConnection: () => config,
        getDriver: () => driver,
      } as unknown as ConnectionManager;
      const reader = new TableReadService(manager);
      const service = new TableMutationService(manager, reader);
      await service.insertRow(config.id, config.database ?? "", "", table, {
        id: "-2",
        payload: "minus-two",
      });
      const page = await reader.getPage(
        config.id,
        config.database ?? "",
        "",
        table,
        1,
        25,
        [{ column: "id", operator: "eq", value: "-2" }],
      );
      expect(page.rows).toEqual([{ id: -2, payload: "minus-two" }]);
      const actual = await driver.query(
        `SELECT CAST(id AS SIGNED) FROM ${table} WHERE payload = 'minus-two'`,
      );
      expect(Number(actual.rows[0].__col_0)).toBe(-2);
    });
  });
  it.each([
    undefined,
    10,
  ])("preserves query values with hardCap=%s", async (hardCap) => {
    await withTable(async (driver, table) => {
      const result = await driver.query(
        `SELECT id FROM ${table} ORDER BY id`,
        undefined,
        { hardCap },
      );
      expect(result.rows.map((row) => Number(row.__col_0))).toEqual([
        -1, 0, 1, 2,
      ]);
    });
  });

  it.each([
    "update",
    "delete",
  ] as const)("%s addresses the selected key 2, not key 0 or 1", async (operation) => {
    await withTable(async (driver, table, config) => {
      const manager = {
        getConnection: () => config,
        getDriver: () => driver,
      } as unknown as ConnectionManager;
      const reader = new TableReadService(manager);
      const page = await reader.getPage(
        config.id,
        config.database ?? "",
        "",
        table,
        1,
        25,
        [],
      );
      const selected = page.rows.find((row) => row.payload === "two");
      expect(selected).toBeDefined();
      const primaryKeys = { id: selected?.id };
      if (operation === "delete") {
        await new TableMutationService(manager, reader).deleteRows(
          config.id,
          config.database ?? "",
          "",
          table,
          [primaryKeys],
        );
      } else {
        const result = await applyChangesTransactional(
          manager,
          config.id,
          config.database ?? "",
          "",
          table,
          [
            {
              primaryKeys,
              changes: { payload: "edited" },
              originalValues: { payload: "two" },
            },
          ],
          page.columns,
        );
        expect(result.success).toBe(true);
      }
      // CAST prevents the driver's TINYINT display heuristic from hiding the
      // actual persisted identities in this independent verification query.
      const actual = await driver.query(
        `SELECT CAST(id AS SIGNED), payload FROM ${table} ORDER BY id`,
      );
      expect(
        actual.rows.map((row) => ({ ...row, __col_0: Number(row.__col_0) })),
      ).toEqual([
        { __col_0: -1, __col_1: "negative" },
        { __col_0: 0, __col_1: "zero" },
        { __col_0: 1, __col_1: "one" },
        ...(operation === "update" ? [{ __col_0: 2, __col_1: "edited" }] : []),
      ]);
    });
  });

  it("preserves every key in a multi-chunk table export", async () => {
    await withTable(async (driver, table, config) => {
      const manager = {
        getConnection: () => config,
        getDriver: () => driver,
      } as unknown as ConnectionManager;
      const reader = new TableReadService(manager);
      const keys: unknown[] = [];
      for await (const chunk of reader.exportAll(
        config.id,
        config.database ?? "",
        "",
        table,
        2,
      )) {
        keys.push(...chunk.rows.map((row) => row.id));
      }
      expect(keys).toEqual([-1, 0, 1, 2]);
    });
  });
});
