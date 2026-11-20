import "./harness";
import { join } from "node:path";
import { waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { DeleteExecutionError } from "../../src/extension/dbDrivers/deleteOutcomes";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import { TableDataService } from "../../src/extension/tableDataService";
import type { SqlDeleteObservation } from "../../src/shared/sqlDeleteObservation";
import type { SQLiteRowIdentity } from "../../src/shared/sqliteRowIdentity";
import { createProjectTempDir } from "../runtime/tempDirectories";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";

async function withSQLiteViewer(
  schema: string,
  setup: (driver: IDBDriver, table: string) => Promise<void>,
  exercise: (args: {
    driver: IDBDriver;
    table: string;
    viewer: BridgeSession;
    identity: SQLiteRowIdentity;
    observation: SqlDeleteObservation;
    refreshSchema: () => void;
    skipPreview: () => void;
  }) => Promise<void>,
) {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("sqlite");
  const connection = scenario.buildConnection(":memory:");
  scenario.buildConnection = () => connection;
  scenario.tableFixture = {
    ...scenario.tableFixture,
    database: schema,
    // Also exercise the workflow's database-only location for main.
    schema: schema === "main" ? undefined : schema,
  };
  const context = await bootstrapWorkflowContext({
    scenario,
    state: workflowState as unknown as Parameters<
      typeof bootstrapWorkflowContext
    >[0]["state"],
  });
  let session: BridgeSession | undefined;
  try {
    await context.connect();
    context.store.setSkipTableMutationPreview(false);
    const driver = context.connectionManager.getDriver(connection.id);
    if (!driver) throw new Error("Missing SQLite driver");
    const table = scenario.tableFixture.table;
    await setup(driver, table);
    session = (await context.openTableViewer()).session;
    const viewer = session;
    const { identity, observation } = await waitFor(() => {
      const page = viewer
        .hostMessages()
        .find(({ type }) => type === "tableData")?.payload as
        | {
            sqliteRowIdentities: SQLiteRowIdentity[];
            sqlDeleteObservations: Array<SqlDeleteObservation | null>;
          }
        | undefined;
      expect(page?.sqliteRowIdentities).toHaveLength(1);
      const observation = page?.sqlDeleteObservations?.[0];
      if (!page || !observation) throw new Error("Waiting for SQLite page");
      return { identity: page.sqliteRowIdentities[0], observation };
    });
    await exercise({
      driver,
      table,
      viewer,
      identity,
      observation,
      refreshSchema: () =>
        context.connectionManager.refreshSchemaCache(connection.id),
      skipPreview: () => context.store.setSkipTableMutationPreview(true),
    });
  } finally {
    session?.unmount();
    TablePanel.disposeAll();
    await context.dispose();
  }
}

async function seedTable(driver: IDBDriver, table: string, label: string) {
  await driver.query(
    `CREATE TABLE ${table} (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER);
     INSERT INTO ${table} VALUES (1, '${label}', 10)`,
  );
}

function findOperation(
  viewer: BridgeSession,
  type: string,
  operationId: string,
) {
  return viewer
    .hostMessages()
    .find(
      (message) =>
        message.type === type &&
        (message.payload as { operationId: string }).operationId ===
          operationId,
    )?.payload;
}

function getPreview(viewer: BridgeSession, operationId: string) {
  return waitFor(() => {
    const preview = findOperation(viewer, "tableMutationPreview", operationId);
    if (!preview) throw new Error(`Missing preview for ${operationId}`);
    return preview;
  });
}

function expectTargetConflict(
  viewer: BridgeSession,
  type: string,
  operationId: string,
) {
  return waitFor(() => {
    expect(findOperation(viewer, type, operationId)).toMatchObject({
      operationId,
      success: false,
      changesPossible: false,
      error: expect.stringMatching(/SQLite.*target.*changed/i),
    });
  });
}

it("sqlite rejects a prepared delete after replacing an attachment with the same schema and key", async () => {
  const directory = await createProjectTempDir("sqlite-attachment");
  const original = join(directory, "original.sqlite");
  const replacement = join(directory, "replacement.sqlite");
  await withSQLiteViewer(
    "aux",
    async (driver, table) => {
      await driver.query("ATTACH DATABASE ? AS aux", [original]);
      await driver.query("ATTACH DATABASE ? AS replacement", [replacement]);
      await seedTable(driver, `aux.${table}`, "original");
      await seedTable(driver, `replacement.${table}`, "replacement");
      await driver.query("DETACH replacement");
    },
    async ({ driver, table, viewer, identity, observation }) => {
      const operationId = "delete-from-original";
      await viewer.sendFromWebview({
        type: "deleteRows",
        payload: {
          operationId,
          primaryKeysList: [identity],
          sqlDeleteObservations: [observation],
        },
      });
      const preview = await getPreview(viewer, operationId);
      await driver.query("DETACH DATABASE aux");
      await driver.query("ATTACH DATABASE ? AS aux", [replacement]);
      await viewer.sendFromWebview({
        type: "confirmMutationPreview",
        payload: preview,
      });
      await expectTargetConflict(viewer, "deleteResult", operationId);
      expect(
        await driver.query(`SELECT label FROM aux.${table}`),
      ).toMatchObject({
        rows: [{ __col_0: "replacement" }],
      });
      await driver.query("ATTACH DATABASE ? AS original", [original]);
      expect(
        await driver.query(`SELECT label FROM original.${table}`),
      ).toMatchObject({
        rows: [{ __col_0: "original" }],
      });
    },
  );
});

it("sqlite cannot verify an unresolved delete against a replacement attachment", async () => {
  await withSQLiteViewer(
    "aux",
    async (driver, table) => {
      await driver.query("ATTACH ':memory:' AS aux");
      await seedTable(driver, `aux.${table}`, "original");
    },
    async ({ driver, table, viewer, identity, observation }) => {
      const operationId = "uncertain-delete";
      const execute = vi
        .spyOn(TableDataService.prototype, "executePreparedDeletePlan")
        .mockRejectedValueOnce(
          new DeleteExecutionError({
            success: false,
            error: "Delete acknowledgement lost",
            affectedRows: 0,
            rowOutcomes: [],
            changesPossible: true,
            outcomeUnknown: true,
          }),
        );
      try {
        await viewer.sendFromWebview({
          type: "deleteRows",
          payload: {
            operationId,
            primaryKeysList: [identity],
            sqlDeleteObservations: [observation],
          },
        });
        await viewer.sendFromWebview({
          type: "confirmMutationPreview",
          payload: await getPreview(viewer, operationId),
        });
        await waitFor(() =>
          expect(
            findOperation(viewer, "deleteResult", operationId),
          ).toMatchObject({ outcomeUnknown: true }),
        );
        await driver.query("DETACH aux; ATTACH ':memory:' AS aux");
        await seedTable(driver, `aux.${table}`, "replacement");
        const fetchId = 129;
        await viewer.sendFromWebview({
          type: "fetchPage",
          payload: { fetchId, page: 1 },
        });
        await waitFor(() =>
          expect(viewer.hostMessages()).toContainEqual({
            type: "tableError",
            payload: {
              fetchId,
              error: expect.stringMatching(/SQLite.*target.*changed/i),
              isFilterError: false,
            },
          }),
        );
        expect(viewer.hostMessages()).not.toContainEqual({
          type: "tableData",
          payload: expect.objectContaining({ fetchId }),
        });
        expect(
          await driver.query(`SELECT label FROM aux.${table}`),
        ).toMatchObject({ rows: [{ __col_0: "replacement" }] });
      } finally {
        execute.mockRestore();
      }
    },
  );
});

it.each([
  ":memory:",
  "",
])("sqlite rejects stale Apply and Insert for a re-created anonymous attachment (%j)", async (file) => {
  await withSQLiteViewer(
    "aux",
    async (driver, table) => {
      await driver.query("ATTACH ? AS aux", [file]);
      await seedTable(driver, `aux.${table}`, "original");
    },
    async ({ driver, table, viewer, identity, refreshSchema, skipPreview }) => {
      await viewer.sendFromWebview({
        type: "insertRow",
        payload: {
          operationId: "insert",
          values: { id: 2, label: "new", amount: 20 },
        },
      });
      const preview = await getPreview(viewer, "insert");
      // The alias, slot, and empty PRAGMA database_list filename are the same
      // at both ends of this script. Original values also match the update.
      await driver.query(
        `/* swap */ DETACH DATABASE "AuX"; ATTACH '${file}' AS aux;
         CREATE TABLE aux.${table} (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER);
         INSERT INTO aux.${table} VALUES (1, 'replacement', 10)`,
        undefined,
        { hardCap: 10 },
      );
      await viewer.sendFromWebview({
        type: "confirmMutationPreview",
        payload: preview,
      });
      await expectTargetConflict(viewer, "insertResult", "insert");

      skipPreview();
      await viewer.sendFromWebview({
        type: "applyChanges",
        payload: {
          operationId: "apply",
          updates: [
            {
              primaryKeys: identity,
              originalValues: { amount: 10 },
              changes: { amount: 11 },
            },
          ],
          insertValues: [{ id: 2, label: "new", amount: 20 }],
        },
      });
      await expectTargetConflict(viewer, "applyResult", "apply");
      expect(
        await driver.query(`SELECT id, amount FROM aux.${table}`),
      ).toMatchObject({
        rows: [{ __col_0: 1, __col_1: 10 }],
      });

      // A metadata refresh cannot rebind the original viewer or show replacement rows.
      const pages = () =>
        viewer.hostMessages().filter(({ type }) => type === "tableData");
      const pageCount = pages().length;
      refreshSchema();
      await viewer.sendFromWebview({
        type: "fetchPage",
        payload: { page: 1, pageSize: 25, fetchId: 81 },
      });
      await waitFor(() =>
        expect(viewer.hostMessages()).toContainEqual({
          type: "tableError",
          payload: expect.objectContaining({
            fetchId: 81,
            error: expect.stringMatching(/close and reopen the Table Viewer/i),
          }),
        }),
      );
      expect(pages()).toHaveLength(pageCount);
    },
  );
});

it("sqlite does not transfer a temp preview after SQLite resets the temp database", async () => {
  await withSQLiteViewer(
    "temp",
    async (driver, table) => {
      await driver.query("PRAGMA temp_store = MEMORY", undefined, {
        hardCap: 10,
      });
      await seedTable(driver, `temp.${table}`, "original");
    },
    async ({ driver, table, viewer, identity, observation }) => {
      await viewer.sendFromWebview({
        type: "deleteRows",
        payload: {
          operationId: "temp-delete",
          primaryKeysList: [identity],
          sqlDeleteObservations: [observation],
        },
      });
      const preview = await getPreview(viewer, "temp-delete");
      await driver.query("PRAGMA temp_store = FILE", undefined, {
        hardCap: 10,
      });
      await seedTable(driver, `temp.${table}`, "replacement");
      await viewer.sendFromWebview({
        type: "confirmMutationPreview",
        payload: preview,
      });
      await expectTargetConflict(viewer, "deleteResult", "temp-delete");
      expect(
        await driver.query(`SELECT label FROM temp.${table}`),
      ).toMatchObject({
        rows: [{ __col_0: "replacement" }],
      });
    },
  );
});

it.each([
  "main",
  "aux",
])("sqlite keeps an unchanged %s target usable through unrelated queries and attachment changes", async (schema) => {
  await withSQLiteViewer(
    schema,
    async (driver, table) => {
      await driver.query("ATTACH ':memory:' AS unrelated");
      if (schema === "aux") await driver.query("ATTACH ':memory:' AS aux");
      await seedTable(driver, `${schema}.${table}`, "original");
      // An unqualified main target must not resolve to a temp shadow table.
      await seedTable(driver, `temp.${table}`, "shadow");
    },
    async ({ driver, table, viewer, identity, refreshSchema }) => {
      await viewer.sendFromWebview({
        type: "applyChanges",
        payload: {
          operationId: "apply",
          updates: [
            {
              primaryKeys: identity,
              originalValues: { amount: 10 },
              changes: { amount: 11 },
            },
          ],
        },
      });
      const preview = await getPreview(viewer, "apply");
      await driver.query(
        "DETACH unrelated; ATTACH '' AS unrelated; SELECT 1; PRAGMA database_list",
      );
      refreshSchema();
      await viewer.sendFromWebview({
        type: "confirmMutationPreview",
        payload: preview,
      });
      await waitFor(() =>
        expect(findOperation(viewer, "applyResult", "apply")).toMatchObject({
          success: true,
        }),
      );
      expect(
        await driver.query(`SELECT amount FROM ${schema}.${table}`),
      ).toMatchObject({
        rows: [{ __col_0: 11 }],
      });
      expect(
        await driver.query(`SELECT amount FROM temp.${table}`),
      ).toMatchObject({
        rows: [{ __col_0: 10 }],
      });
    },
  );
});
