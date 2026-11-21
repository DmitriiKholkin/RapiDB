import mssql from "mssql";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { mssqlCompletionTokenHarness } from "../helpers/mssqlCompletionTokenHarness";
import {
  MockMssqlRequest,
  mockMssqlSessionPool,
} from "../helpers/mssqlQuerySessionHarness";

const config = {
  id: "query-error",
  name: "Query error",
  type: "mssql" as const,
  database: "profile_default",
};
const sourceDatabase = "db";
const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "int",
    nativeType: "int",
    category: "integer",
    identityGeneration: "by_default",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  },
];

function harness(error?: Error, diagnosticRows = true) {
  const setup = new MockMssqlRequest();
  const request = new MockMssqlRequest();
  const pool = mockMssqlSessionPool([setup, request]);
  const [connection] = pool.connections;
  let currentDatabase = config.database;
  setup.execute.mockImplementation(async (sql, physical) => {
    expect(physical).toBe(connection);
    expect(currentDatabase).toBe(config.database);
    expect(sql).toBe(
      `USE [${sourceDatabase}]; SELECT DB_NAME() AS databaseName;`,
    );
    currentDatabase = sourceDatabase;
    const recordset = [{ databaseName: currentDatabase }];
    return {
      recordsets: [recordset],
      recordset,
      rowsAffected: [1],
      output: {},
    };
  });
  connection.reset.mockImplementation((callback) => {
    currentDatabase = config.database;
    callback();
  });
  // Install the real parser factory before the query session hooks it. The
  // diagnostic rowset count is not evidence of a successful INSERT: supply
  // a distinct, classified parent count to keep the positive-count/error test.
  const wire = mssqlCompletionTokenHarness(connection, request);
  request.execute.mockImplementation(async (sql, physical) => {
    expect(physical).toBe(connection);
    expect(currentDatabase).toBe(sourceDatabase);
    expect(sql).toContain("[db].[dbo].[items]");
    request.emit("recordset", [{ name: "diagnostic", type: mssql.Int }]);
    if (diagnosticRows) {
      request.emit("row", [1]);
    }
    await wire.complete(0xc1, diagnosticRows ? 1 : 0);
    if (diagnosticRows) await wire.complete(0xc3, 1);
    if (error) {
      request.emit("error", error);
      throw error;
    }
    return {
      recordsets: [],
      recordset: [],
      rowsAffected: diagnosticRows ? [1, 1] : [0],
      output: {},
    };
  });
  const driver = new MSSQLDriver(config);
  Object.assign(driver, { pool });
  const manager = {
    getConnection: () => ({ ...config, readOnly: false }),
    getDriver: () => driver,
    getQueryEditorPresentation: () => undefined,
  } as unknown as ConnectionManager;
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  const controller = new TableMutationPreviewController({
    connectionId: config.id,
    tableName: "items",
    connectionManager: manager,
    tableDataService: service,
    notifyWarning: vi.fn(),
  });
  return { setup, request, driver, service, controller };
}

afterEach(() => vi.restoreAllMocks());

describe("returned SQL errors cannot confirm a table mutation", () => {
  it.each([
    "insert",
    "apply",
  ] as const)("keeps a default-only %s failed after a trigger result set and late error", async (mode) => {
    const { setup, request, driver, service, controller } = harness(
      new Error("trigger rejected INSERT"),
    );
    const query = vi.spyOn(driver, "query");
    const plan = await service.prepareInsertRow(
      config.id,
      sourceDatabase,
      "dbo",
      "items",
      {},
    );
    expect(plan.verification).toBeUndefined();
    expect(plan.operation.sql).toContain("DEFAULT VALUES");
    const preview =
      mode === "insert"
        ? controller.createInsertPreview("rejected-insert", plan)
        : controller.createApplyChangesPreview("rejected-insert", {
            apply: null,
            applyResultWhenEmpty: null,
            inserts: [plan],
          });
    const result = await controller.confirm(
      preview.previewToken,
      "rejected-insert",
    );
    expect(await query.mock.results[0].value).toMatchObject({
      error: "trigger rejected INSERT",
      affectedRows: 1,
      rows: [{ __col_0: 1 }],
    });
    expect(result).toMatchObject({
      type: mode === "insert" ? "insertResult" : "applyResult",
      payload: {
        success: false,
        error: expect.stringContaining("trigger rejected INSERT"),
        changesPossible: true,
        outcomeUnknown: true,
      },
    });
    expect(result?.payload).not.toHaveProperty("insertApplied", true);
    expect(setup.batch).toHaveBeenCalledOnce();
    expect(request.execute).toHaveBeenCalledOnce();
  });

  it("still confirms a default-only insert when the server reports no error", async () => {
    const { setup, request, service, controller } = harness();
    const plan = await service.prepareInsertRow(
      config.id,
      sourceDatabase,
      "dbo",
      "items",
      {},
    );
    const preview = controller.createInsertPreview("accepted-insert", plan);
    expect(await controller.confirm(preview.previewToken)).toMatchObject({
      type: "insertResult",
      payload: { success: true },
    });
    expect(setup.batch).toHaveBeenCalledOnce();
    expect(request.execute).toHaveBeenCalledOnce();
  });

  it("does not treat a failed empty verification SELECT as proof of deletion", async () => {
    const { setup, request, driver, service } = harness(
      new Error("verification SELECT failed"),
      false,
    );
    vi.spyOn(driver, "getMutationAtomicityRisk").mockResolvedValue(null);
    const transaction = vi
      .spyOn(driver, "runTransaction")
      .mockResolvedValue(undefined);
    const plan = await service.prepareDeleteRowsPlan(
      config.id,
      sourceDatabase,
      "dbo",
      "items",
      [{ id: 1 }],
    );
    if (!plan) throw new Error("Expected delete plan");
    await expect(service.executePreparedDeletePlan(plan)).rejects.toThrow(
      "verification SELECT failed",
    );
    expect(transaction).toHaveBeenCalledTimes(1);
    expect(setup.batch).toHaveBeenCalledOnce();
    expect(request.execute).toHaveBeenCalledOnce();
  });
});
