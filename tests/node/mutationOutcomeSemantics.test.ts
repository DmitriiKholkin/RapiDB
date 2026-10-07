import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { TransactionVerificationError } from "../../src/extension/dbDrivers/transactionVerification";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import type { PreparedInsertPlan } from "../../src/extension/table/tableDataContracts";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const config = { id: "insert-outcome", name: "insert-outcome" };

async function confirmSingleDriverInsert(error: Error) {
  const driver = new MongoDBDriver({ ...config, type: "mongodb" });
  const insertedRows: Record<string, unknown>[] = [];
  const insertRow = vi
    .spyOn(driver, "insertRow")
    .mockImplementation(async () => {
      if ((error as Error & { code?: string }).code !== "NOT_EXECUTED") {
        insertedRows.push({ name: "new row" });
      }
      throw error;
    });
  const connectionManager = {
    getConnection: () => ({ ...config, type: "mongodb", readOnly: false }),
    getDriver: () => driver,
    getQueryEditorPresentation: () => undefined,
  };
  const tableDataService = new TableMutationService(
    connectionManager as never,
    { getColumns: async () => [] },
  );
  const controller = new TableMutationPreviewController({
    connectionId: config.id,
    tableName: "items",
    connectionManager: connectionManager as never,
    tableDataService,
    notifyWarning: vi.fn(),
  });
  const insert: PreparedInsertPlan = {
    connectionId: config.id,
    database: "db",
    schema: "",
    table: "items",
    mode: "driver",
    values: { name: "new row" },
    operation: { sql: "INSERT INTO items", params: [] },
    previewStatements: ["INSERT INTO items"],
    verificationCriteria: null,
  };
  const preview = controller.createApplyChangesPreview("insert-one", {
    apply: null,
    applyResultWhenEmpty: null,
    inserts: [insert],
  });

  return {
    insertRow,
    insertedRows,
    result: await controller.confirm(preview.previewToken, "insert-one"),
  };
}

async function confirmSingleVerifiedInsert(options: {
  atomicityRisk: string | null;
  transactionError?: Error;
}) {
  const driver = {
    getMutationAtomicityRisk: vi.fn().mockResolvedValue(options.atomicityRisk),
    runTransaction: vi.fn(async () => {
      if (options.transactionError) throw options.transactionError;
    }),
  };
  const connectionManager = {
    getConnection: () => ({ ...config, type: "postgres", readOnly: false }),
    getDriver: () => driver,
    getQueryEditorPresentation: () => undefined,
  };
  const tableDataService = new TableMutationService(
    connectionManager as never,
    { getColumns: async () => [] },
  );
  const controller = new TableMutationPreviewController({
    connectionId: config.id,
    tableName: "items",
    connectionManager: connectionManager as never,
    tableDataService,
    notifyWarning: vi.fn(),
  });
  const insert: PreparedInsertPlan = {
    connectionId: config.id,
    database: "db",
    schema: "public",
    table: "items",
    mode: "sql",
    values: { name: "new row" },
    operation: { sql: "INSERT INTO items", params: [] },
    previewStatements: ["INSERT INTO items"],
    verificationCriteria: { name: "new row" },
    verification: {
      rowIndex: 0,
      mutation: "insert",
      sql: "SELECT name FROM items",
      params: [],
      values: [],
    },
  };
  const preview = controller.createApplyChangesPreview("insert-verified", {
    apply: null,
    applyResultWhenEmpty: null,
    inserts: [insert],
  });

  return {
    driver,
    result: await controller.confirm(preview.previewToken, "insert-verified"),
  };
}

describe("mutation outcome semantics", () => {
  it.each([
    "driver",
    "sql",
  ] as const)("does not reconcile a %s insert rejected before any execution", async (mode) => {
    for (const reason of [
      "readonly",
      "disconnected",
      "blocked",
      "missing",
    ] as const) {
      let rejected = false;
      const driver = {
        insertRow: vi.fn(),
        query: vi.fn(),
        runTransaction: vi.fn(),
      };
      const connectionManager = {
        getConnection: () =>
          rejected && reason === "missing"
            ? undefined
            : {
                ...config,
                type: "mongodb",
                readOnly: rejected && reason === "readonly",
              },
        getDriver: () =>
          rejected && reason === "disconnected" ? undefined : driver,
        getAutomaticReconnectBlockReason: () =>
          rejected && reason === "blocked" ? "Reconnect is blocked" : undefined,
        getQueryEditorPresentation: () => undefined,
      };
      const service = new TableMutationService(connectionManager as never, {
        getColumns: async () => [],
      });
      const controller = new TableMutationPreviewController({
        connectionId: config.id,
        tableName: "items",
        connectionManager: connectionManager as never,
        tableDataService: service,
        notifyWarning: vi.fn(),
      });
      const insert: PreparedInsertPlan = {
        connectionId: config.id,
        database: "db",
        schema: "",
        table: "items",
        mode,
        values: { name: "new row" },
        operation: { sql: "INSERT INTO items", params: [] },
        previewStatements: ["INSERT INTO items"],
        verificationCriteria: null,
      };
      const preview = controller.createApplyChangesPreview("preflight", {
        apply: null,
        applyResultWhenEmpty: null,
        inserts: [insert],
      });
      rejected = true;
      expect(
        await controller.confirm(preview.previewToken, "preflight"),
      ).toMatchObject({
        type: "applyResult",
        payload: {
          success: false,
          changesPossible: false,
          outcomeUnknown: false,
        },
      });
      expect(driver.insertRow).not.toHaveBeenCalled();
      expect(driver.query).not.toHaveBeenCalled();
      expect(driver.runTransaction).not.toHaveBeenCalled();
    }
  });

  it("reconciles a single driver insert after its write acknowledgement is lost", async () => {
    const { result, insertRow, insertedRows } = await confirmSingleDriverInsert(
      new Error("connection lost after write"),
    );

    expect(insertRow).toHaveBeenCalledOnce();
    expect(insertedRows).toEqual([{ name: "new row" }]);
    expect(result).toMatchObject({
      type: "applyResult",
      payload: {
        success: false,
        changesPossible: true,
        outcomeUnknown: true,
      },
    });
    expect(result?.payload).not.toHaveProperty("insertApplied");
  });

  it("does not reconcile a single driver insert proven not executed", async () => {
    const notExecuted = Object.assign(new Error("insert remained queued"), {
      code: "NOT_EXECUTED",
    });
    const { result, insertRow, insertedRows } =
      await confirmSingleDriverInsert(notExecuted);

    expect(insertRow).toHaveBeenCalledOnce();
    expect(insertedRows).toEqual([]);
    expect(result).toMatchObject({
      type: "applyResult",
      payload: {
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
      },
    });
    expect(result?.payload).not.toHaveProperty("insertApplied");
  });

  it("does not reconcile a single verified insert rejected by unsafe-target preflight", async () => {
    const { driver, result } = await confirmSingleVerifiedInsert({
      atomicityRisk: "Table items does not support transactional rollback.",
    });

    expect(driver.getMutationAtomicityRisk).toHaveBeenCalledOnce();
    expect(driver.runTransaction).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      type: "applyResult",
      payload: {
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
      },
    });
    expect(result?.payload).not.toHaveProperty("insertApplied");
  });

  it("does not reconcile a single verified insert rolled back after verification failure", async () => {
    const verificationError = new TransactionVerificationError({
      rowIndex: 0,
      mutation: "insert",
      columns: ["name"],
      message: "Persisted value did not match.",
    });
    const { driver, result } = await confirmSingleVerifiedInsert({
      atomicityRisk: null,
      transactionError: verificationError,
    });

    expect(driver.getMutationAtomicityRisk).toHaveBeenCalledOnce();
    expect(driver.runTransaction).toHaveBeenCalledOnce();
    expect(result).toMatchObject({
      type: "applyResult",
      payload: {
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
      },
    });
    expect(result?.payload).not.toHaveProperty("insertApplied");
  });
});
