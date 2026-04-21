import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../../src/extension/connectionManager";
import type { IDBDriver } from "../../../src/extension/dbDrivers/types";
import {
  applyChangesTransactional,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import type { DbEngineId } from "../../contracts/testingContracts";
import {
  createLiveDriverHarness,
  disposeLiveDriverHarness,
  fixtureTableName,
  rowsFromQuery,
} from "../../support/liveDbHarness";

function caseInsensitiveValue(
  row: Record<string, unknown> | undefined,
  key: string,
): unknown {
  if (!row) {
    return undefined;
  }

  const match = Object.keys(row).find(
    (candidate) => candidate.toLowerCase() === key.toLowerCase(),
  );
  return match ? row[match] : undefined;
}

function findColumnName(
  columns: Array<{ name: string }>,
  logicalName: string,
): string {
  const column = columns.find(
    (candidate) => candidate.name.toLowerCase() === logicalName.toLowerCase(),
  );

  if (!column) {
    throw new Error(`Expected column ${logicalName}.`);
  }

  return column.name;
}

function toPhysicalRecord(
  columns: Array<{ name: string }>,
  values: Record<string, unknown>,
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(values).map(([logicalName, value]) => [
      findColumnName(columns, logicalName),
      value,
    ]),
  );
}

function createManagerLike(
  connectionId: string,
  driver: IDBDriver | undefined,
  connection = { id: connectionId },
): ConnectionManager {
  return {
    getConnection: (id: string) =>
      id === connectionId ? connection : undefined,
    getDriver: (id: string) => (id === connectionId ? driver : undefined),
  } as unknown as ConnectionManager;
}

export function registerTableServiceIntegrationTests(
  engineId: DbEngineId,
): void {
  describe(`${engineId} table services`, () => {
    let harness: Awaited<ReturnType<typeof createLiveDriverHarness>>;
    let readService: TableReadService;
    let mutationService: TableMutationService;
    let connectionManager: ConnectionManager;
    const connectionId = `live-${engineId}`;

    beforeAll(async () => {
      harness = await createLiveDriverHarness(engineId);
      connectionManager = createManagerLike(connectionId, harness.driver);
      readService = new TableReadService(connectionManager);
      mutationService = new TableMutationService(
        connectionManager,
        readService,
      );
    });

    afterAll(async () => {
      await disposeLiveDriverHarness(harness);
    });

    it("reads filtered and sorted pages and falls back cleanly when count queries fail", async () => {
      const columns = await readService.getColumns(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "paginationRows"),
      );
      const pageGroupColumn = columns.find(
        (column) => column.name.toLowerCase() === "page_group",
      );
      const titleColumn = columns.find(
        (column) => column.name.toLowerCase() === "title",
      );

      if (!pageGroupColumn || !titleColumn) {
        throw new Error("Expected pagination fixture columns.");
      }

      const page = await readService.getPage(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "paginationRows"),
        1,
        5,
        [{ column: pageGroupColumn.name, operator: "eq", value: "2" }],
        { column: titleColumn.name, direction: "desc" },
      );

      expect(page.totalCount).toBe(12);
      expect(page.rows).toHaveLength(5);
      expect(
        String(caseInsensitiveValue(page.rows[0], titleColumn.name)),
      ).toContain("24");

      const countFailingDriver = Object.create(harness.driver) as IDBDriver;
      countFailingDriver.query = async (sql, params) => {
        if (/count\(\*\)/i.test(sql)) {
          throw new Error("Count failed");
        }

        return harness.driver.query(sql, params);
      };

      const fallbackService = new TableReadService(
        createManagerLike(connectionId, countFailingDriver),
      );
      const fallbackPage = await fallbackService.getPage(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "paginationRows"),
        1,
        5,
        [],
      );

      expect(fallbackPage.totalCount).toBe(0);
      expect(fallbackPage.rows).toHaveLength(5);
    });

    it("exports rows in deterministic chunks", async () => {
      const chunkSizes: number[] = [];
      let totalRows = 0;

      for await (const chunk of readService.exportAll(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "exportRows"),
        50,
      )) {
        chunkSizes.push(chunk.rows.length);
        totalRows += chunk.rows.length;
      }

      expect(chunkSizes).toEqual([50, 50, 28]);
      expect(totalRows).toBe(128);
    });

    it("inserts, updates, and deletes rows with preview SQL", async () => {
      const probeId = 960_000 + Math.floor(Math.random() * 10_000);
      const tableName = fixtureTableName(engineId, "transactionProbe");
      const transactionProbeColumns = await readService.getColumns(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
      );
      const transactionProbeIdColumn = findColumnName(
        transactionProbeColumns,
        "id",
      );
      const transactionProbeAccountNameColumn = findColumnName(
        transactionProbeColumns,
        "account_name",
      );

      const plan = await mutationService.prepareInsertRow(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
        toPhysicalRecord(transactionProbeColumns, {
          id: probeId,
          account_name: "Mutation Probe",
          balance: "55.50",
          updated_at: "2026-04-21T12:00:00.000Z",
        }),
      );

      expect(plan.previewStatements).toHaveLength(1);
      expect(plan.previewStatements[0]).toContain("INSERT INTO");

      await mutationService.executePreparedInsertPlan(plan);
      await mutationService.updateRow(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
        { [transactionProbeIdColumn]: probeId },
        { [transactionProbeAccountNameColumn]: "Mutation Probe Updated" },
      );

      let rowResult = await harness.driver.query(
        `SELECT * FROM ${harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          tableName,
        )} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn)} = ${probeId}`,
      );
      const row = rowsFromQuery(rowResult)[0];
      expect(String(caseInsensitiveValue(row, "account_name"))).toContain(
        "Updated",
      );

      await mutationService.deleteRows(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
        [{ [transactionProbeIdColumn]: probeId }],
      );

      rowResult = await harness.driver.query(
        `SELECT * FROM ${harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          tableName,
        )} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn)} = ${probeId}`,
      );
      expect(rowsFromQuery(rowResult)).toHaveLength(0);

      const compositeLinkColumns = await readService.getColumns(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "compositeLinks"),
      );
      const compositeTenantIdColumn = findColumnName(
        compositeLinkColumns,
        "tenant_id",
      );
      const compositeExternalIdColumn = findColumnName(
        compositeLinkColumns,
        "external_id",
      );
      const compositeDescriptionColumn = findColumnName(
        compositeLinkColumns,
        "description",
      );
      const compositeCreatedAtColumn = findColumnName(
        compositeLinkColumns,
        "created_at",
      );

      await harness.driver.query(
        `INSERT INTO ${harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          fixtureTableName(engineId, "compositeLinks"),
        )} (${harness.driver.quoteIdentifier(compositeTenantIdColumn)}, ${harness.driver.quoteIdentifier(compositeExternalIdColumn)}, ${harness.driver.quoteIdentifier(compositeDescriptionColumn)}, ${harness.driver.quoteIdentifier(compositeCreatedAtColumn)}) VALUES (99, 999, 'Composite Probe', ${engineId === "oracle" ? "TO_TIMESTAMP('2026-04-21 12:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3')" : engineId === "mssql" ? "CAST('2026-04-21 12:00:00.000' AS DATETIME2(3))" : engineId === "postgres" ? "TIMESTAMPTZ '2026-04-21T12:00:00.000Z'" : "'2026-04-21 12:00:00.000'"})`,
      );
      await mutationService.deleteRows(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "compositeLinks"),
        [
          {
            [compositeTenantIdColumn]: 99,
            [compositeExternalIdColumn]: 999,
          },
        ],
      );

      const compositeResult = await harness.driver.query(
        `SELECT * FROM ${harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          fixtureTableName(engineId, "compositeLinks"),
        )} WHERE ${harness.driver.quoteIdentifier(compositeTenantIdColumn)} = 99 AND ${harness.driver.quoteIdentifier(compositeExternalIdColumn)} = 999`,
      );
      expect(rowsFromQuery(compositeResult)).toHaveLength(0);
    });

    it("prepares previews, rolls back failed transactions, reports skipped rows, and surfaces verification warnings", async () => {
      const tableName = fixtureTableName(engineId, "transactionProbe");
      const columns = await readService.getColumns(
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
      );
      const idColumn = columns.find((column) => column.isPrimaryKey);
      const accountNameColumn = columns.find(
        (column) => column.name.toLowerCase() === "account_name",
      );
      const balanceColumn = columns.find(
        (column) => column.name.toLowerCase() === "balance",
      );
      const updatedAtColumn = columns.find(
        (column) => column.name.toLowerCase() === "updated_at",
      );

      if (
        !idColumn ||
        !accountNameColumn ||
        !balanceColumn ||
        !updatedAtColumn
      ) {
        throw new Error("Expected transaction probe columns.");
      }

      const previewPlan = prepareApplyChangesPlan(
        connectionManager,
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
        [
          {
            primaryKeys: { [idColumn.name]: 1 },
            changes: { [accountNameColumn.name]: "Preview Only" },
          },
        ],
        columns,
      );

      expect(previewPlan.executable).toBe(true);
      if (!previewPlan.executable) {
        throw new Error("Expected executable preview plan.");
      }
      expect(previewPlan.plan.previewStatements[0]).toContain("UPDATE");

      const skippedPlan = prepareApplyChangesPlan(
        connectionManager,
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
        [
          {
            primaryKeys: { [idColumn.name]: 1 },
            changes: {},
          },
        ],
        columns,
      );

      expect(skippedPlan.executable).toBe(false);
      if (skippedPlan.executable) {
        throw new Error("Expected skipped-only plan.");
      }
      expect(skippedPlan.result.rowOutcomes?.[0]?.status).toBe("skipped");

      const rollbackResult = await applyChangesTransactional(
        connectionManager,
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
        [
          {
            primaryKeys: { [idColumn.name]: 1 },
            changes: { [accountNameColumn.name]: "Should Roll Back" },
          },
          {
            primaryKeys: { [idColumn.name]: 999999 },
            changes: { [accountNameColumn.name]: "Missing" },
          },
        ],
        columns,
      );

      expect(rollbackResult.success).toBe(false);
      const rollbackCheck = await harness.driver.query(
        `SELECT ${harness.driver.quoteIdentifier(accountNameColumn.name)} AS account_name FROM ${harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          tableName,
        )} WHERE ${harness.driver.quoteIdentifier(idColumn.name)} = 1`,
      );
      expect(
        String(
          caseInsensitiveValue(rowsFromQuery(rollbackCheck)[0], "account_name"),
        ),
      ).not.toContain("Should Roll Back");

      const warningId = 970_000 + Math.floor(Math.random() * 10_000);
      await harness.driver.query(
        `INSERT INTO ${harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          tableName,
        )} (${harness.driver.quoteIdentifier(idColumn.name)}, ${harness.driver.quoteIdentifier(accountNameColumn.name)}, ${harness.driver.quoteIdentifier(balanceColumn.name)}, ${harness.driver.quoteIdentifier(updatedAtColumn.name)}) VALUES (${warningId}, 'Verification Probe', 10.00, ${engineId === "oracle" ? "TO_TIMESTAMP('2026-04-21 13:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3')" : engineId === "mssql" ? "CAST('2026-04-21 13:00:00.000' AS DATETIME2(3))" : engineId === "postgres" ? "TIMESTAMPTZ '2026-04-21T13:00:00.000Z'" : "'2026-04-21 13:00:00.000'"})`,
      );

      const warningDriver = Object.create(harness.driver) as IDBDriver;
      warningDriver.checkPersistedEdit = (column, expectedValue, options) => {
        if (column.name.toLowerCase() === balanceColumn.name.toLowerCase()) {
          if (options?.persistedValue !== undefined) {
            return {
              ok: false,
              shouldVerify: true,
              message: "Forced verification mismatch",
            };
          }

          return { ok: true, shouldVerify: true };
        }

        return harness.driver.checkPersistedEdit(
          column,
          expectedValue,
          options,
        );
      };

      const warningResult = await applyChangesTransactional(
        createManagerLike(connectionId, warningDriver),
        connectionId,
        harness.databaseName,
        harness.schemaName,
        tableName,
        [
          {
            primaryKeys: { [idColumn.name]: warningId },
            changes: { [balanceColumn.name]: "20.00" },
          },
        ],
        columns,
      );

      expect(warningResult.success).toBe(true);
      expect(warningResult.warning).toContain("could not be confirmed");
      expect(warningResult.rowOutcomes?.[0]?.status).toBe(
        "verification_failed",
      );

      await harness.driver.query(
        `DELETE FROM ${harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          tableName,
        )} WHERE ${harness.driver.quoteIdentifier(idColumn.name)} = ${warningId}`,
      );
    });

    it("throws a disconnected-state error when no live driver is available", async () => {
      const disconnectedService = new TableReadService(
        createManagerLike(connectionId, undefined),
      );

      await expect(
        disconnectedService.getPage(
          connectionId,
          harness.databaseName,
          harness.schemaName,
          fixtureTableName(engineId, "fixtureRows"),
          1,
          5,
          [],
        ),
      ).rejects.toThrow(/Not connected/);
    });
  });
}
