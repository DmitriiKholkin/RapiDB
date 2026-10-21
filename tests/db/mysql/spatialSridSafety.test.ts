import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../../src/extension/connectionManager";
import { MySQLDriver } from "../../../src/extension/dbDrivers/mysql";
import { applyChangesTransactional } from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

async function withTable(
  test: (context: {
    driver: MySQLDriver;
    manager: ConnectionManager;
    reader: TableReadService;
    service: TableMutationService;
    table: string;
    database: string;
    connectionId: string;
  }) => Promise<void>,
) {
  const config = await resolveConnectionSeed("mysql");
  const driver = new MySQLDriver(config);
  const table = `spatial_safety_${randomUUID().replaceAll("-", "")}`;
  await driver.connect();
  try {
    await driver.query(
      `CREATE TABLE ${table} (id INT PRIMARY KEY, shape GEOMETRY)`,
    );
    await driver.query(
      `INSERT INTO ${table} VALUES (1, ST_GeomFromText('POINT(1 2)', 4326)), (2, NULL), (3, ST_GeomFromText('POINT(1 2)', 0))`,
    );
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const reader = new TableReadService(manager);
    await test({
      driver,
      manager,
      reader,
      service: new TableMutationService(manager, reader),
      table,
      database: config.database ?? "",
      connectionId: config.id,
    });
  } finally {
    await driver.query(`DROP TABLE IF EXISTS ${table}`);
    await driver.disconnect();
  }
}

describe("MySQL spatial SRID data safety", () => {
  it("retains nonzero SRIDs in table reads and multi-chunk exports", async () => {
    await withTable(async ({ reader, connectionId, database, table }) => {
      const page = await reader.getPage(
        connectionId,
        database,
        "",
        table,
        1,
        25,
        [],
      );
      expect(page.rows.map((row) => row.shape)).toEqual([
        "SRID=4326;POINT(1 2)",
        null,
        "POINT(1 2)",
      ]);
      const exported: unknown[] = [];
      for await (const chunk of reader.exportAll(
        connectionId,
        database,
        "",
        table,
        2,
      )) {
        exported.push(...chunk.rows.map((row) => row.shape));
      }
      expect(exported).toEqual(page.rows.map((row) => row.shape));
    });
  });

  it("preserves the existing SRID when updating with plain WKT", async () => {
    await withTable(
      async ({ service, driver, connectionId, database, table }) => {
        await service.updateRow(
          connectionId,
          database,
          "",
          table,
          { id: 1 },
          { shape: "POINT(3 4)" },
        );
        expect(
          (
            await driver.query(
              `SELECT ST_SRID(shape), ST_AsText(shape) FROM ${table} WHERE id=1`,
            )
          ).rows,
        ).toEqual([{ __col_0: "4326", __col_1: "POINT(3 4)" }]);
      },
    );
  });

  it("supports explicit SRIDs in insert and update values", async () => {
    await withTable(
      async ({ service, driver, connectionId, database, table }) => {
        await service.insertRow(connectionId, database, "", table, {
          id: 4,
          shape: "SRID=4326;POINT(5 6)",
        });
        expect(
          (await driver.query(`SELECT ST_SRID(shape) FROM ${table} WHERE id=4`))
            .rows,
        ).toEqual([{ __col_0: "4326" }]);
        await service.updateRow(
          connectionId,
          database,
          "",
          table,
          { id: 4 },
          { shape: "SRID=0;POINT(7 8)" },
        );
        expect(
          (
            await driver.query(
              `SELECT ST_SRID(shape), ST_AsText(shape) FROM ${table} WHERE id=4`,
            )
          ).rows,
        ).toEqual([{ __col_0: "0", __col_1: "POINT(7 8)" }]);
      },
    );
  });

  it("applies an edit using the SRID-bearing original value from the viewer", async () => {
    await withTable(
      async ({ manager, reader, driver, connectionId, database, table }) => {
        const page = await reader.getPage(
          connectionId,
          database,
          "",
          table,
          1,
          25,
          [],
        );
        const result = await applyChangesTransactional(
          manager,
          connectionId,
          database,
          "",
          table,
          [
            {
              primaryKeys: { id: 1 },
              changes: { shape: "POINT(3 4)" },
              originalValues: { shape: page.rows[0].shape },
            },
          ],
          page.columns,
        );
        expect(result.success).toBe(true);
        expect(
          (await driver.query(`SELECT ST_SRID(shape) FROM ${table} WHERE id=1`))
            .rows,
        ).toEqual([{ __col_0: "4326" }]);
      },
    );
  });

  it("does not overwrite a concurrent SRID-only change with identical WKB", async () => {
    await withTable(
      async ({ manager, reader, driver, connectionId, database, table }) => {
        const columns = await reader.getColumns(
          connectionId,
          database,
          "",
          table,
        );
        await driver.query(
          `UPDATE ${table} SET shape=ST_GeomFromText('POINT(1 2)', 4326) WHERE id=3`,
        );
        const result = await applyChangesTransactional(
          manager,
          connectionId,
          database,
          "",
          table,
          [
            {
              primaryKeys: { id: 3 },
              changes: { shape: "POINT(3 4)" },
              originalValues: { shape: "POINT(1 2)" },
            },
          ],
          columns,
        );
        expect(result.success).toBe(false);
        expect(
          (
            await driver.query(
              `SELECT ST_SRID(shape), ST_AsText(shape) FROM ${table} WHERE id=3`,
            )
          ).rows,
        ).toEqual([{ __col_0: "4326", __col_1: "POINT(1 2)" }]);
      },
    );
  });

  it("preserves NULL and defaults a new plain-WKT value to SRID zero", async () => {
    await withTable(
      async ({ service, driver, connectionId, database, table }) => {
        await service.updateRow(
          connectionId,
          database,
          "",
          table,
          { id: 1 },
          { shape: null },
        );
        await service.updateRow(
          connectionId,
          database,
          "",
          table,
          { id: 2 },
          { shape: "POINT(3 4)" },
        );
        expect(
          (
            await driver.query(
              `SELECT ST_SRID(shape) FROM ${table} WHERE id IN (1,2) ORDER BY id`,
            )
          ).rows,
        ).toEqual([{ __col_0: null }, { __col_0: "0" }]);
      },
    );
  });
});
