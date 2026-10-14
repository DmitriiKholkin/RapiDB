import { DatabaseError } from "pg";
import { describe, expect, it, vi } from "vitest";
import { TransactionVerificationError } from "../../src/extension/dbDrivers/transactionVerification";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import type { PreparedInsertPlan } from "../../src/extension/table/tableDataContracts";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const config = { id: "insert-outcome", name: "insert-outcome" };

async function confirmSingleDriverInsert(
  error: Error,
  type: "mongodb" | "pg" = "mongodb",
) {
  const insertedRows: Record<string, unknown>[] = [];
  const insertRow = vi.fn(async () => {
    const code = (error as Error & { code?: string }).code;
    if (
      code !== "NOT_EXECUTED" &&
      !(type === "pg" && error instanceof DatabaseError && code === "23502")
    ) {
      insertedRows.push({ name: "new row" });
    }
    throw error;
  });
  const driver = { insertRow };
  const connectionManager = {
    getConnection: () => ({ ...config, type, readOnly: false }),
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
    controller,
    insert,
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
  it("does not replay an unknown INSERT with a fresh preview token or after clear", async () => {
    const { controller, insert, insertRow, result } =
      await confirmSingleDriverInsert(new Error("Acknowledgement lost"));
    expect(result?.payload).toMatchObject({ outcomeUnknown: true });
    controller.clear();
    const retry = controller.createApplyChangesPreview("insert-one", {
      apply: null,
      applyResultWhenEmpty: null,
      inserts: [insert],
    });
    expect(
      await controller.confirm(retry.previewToken, "insert-one"),
    ).toBeNull();
    const standalone = controller.createInsertPreview("insert-one", insert);
    expect(
      await controller.confirm(standalone.previewToken, "insert-one"),
    ).toBeNull();
    expect(insertRow).toHaveBeenCalledOnce();
  });

  it("reports PostgreSQL SQLSTATE rejections as definite insert failures", async () => {
    const error = new DatabaseError(
      'null value in column "id" violates not-null constraint',
      0,
      "error",
    );
    error.code = "23502";
    const { result, insertedRows } = await confirmSingleDriverInsert(
      error,
      "pg",
    );

    expect(result?.payload).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
    });
    expect(result?.payload.error).toContain("violates not-null constraint");
    expect(result?.payload.error).not.toContain("outcome is unknown");
    expect(insertedRows).toEqual([]);
  });

  it("reports standalone PostgreSQL SQLSTATE rejections as definite insert failures", async () => {
    const error = new DatabaseError(
      'null value in column "id" violates not-null constraint',
      0,
      "error",
    );
    error.code = "23502";
    const controller = new TableMutationPreviewController({
      connectionId: config.id,
      tableName: "items",
      connectionManager: {
        getConnection: () => ({ ...config, type: "pg", readOnly: false }),
        getQueryEditorPresentation: () => undefined,
      } as never,
      tableDataService: {
        executePreparedInsertPlan: vi.fn().mockRejectedValue(error),
        executePreparedDeletePlan: vi.fn(),
      },
      notifyWarning: vi.fn(),
    });
    const insert: PreparedInsertPlan = {
      connectionId: config.id,
      database: "db",
      schema: "public",
      table: "items",
      mode: "sql",
      values: { id: null },
      operation: { sql: "INSERT INTO items (id) VALUES (NULL)", params: [] },
      previewStatements: ["INSERT INTO items (id) VALUES (NULL)"],
      verificationCriteria: null,
    };
    const preview = controller.createInsertPreview("standalone-pg", insert);
    const result = await controller.confirm(
      preview.previewToken,
      "standalone-pg",
    );

    expect(result?.payload).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
    });
    expect(result?.payload.error).not.toContain("outcome is unknown");
  });

  it("keeps PostgreSQL connection failures outcome-unknown", async () => {
    const error = new DatabaseError("connection lost", 0, "error");
    error.code = "08006";
    const { result } = await confirmSingleDriverInsert(error, "pg");

    expect(result?.payload).toMatchObject({
      success: false,
      changesPossible: true,
      outcomeUnknown: true,
    });
  });

  it("keeps PostgreSQL socket EPIPE errors outcome-unknown", async () => {
    const error = Object.assign(new Error("socket write failed"), {
      code: "EPIPE",
    });
    const { result, insertedRows } = await confirmSingleDriverInsert(
      error,
      "pg",
    );

    expect(result?.payload).toMatchObject({
      success: false,
      changesPossible: true,
      outcomeUnknown: true,
    });
    expect(insertedRows).toEqual([{ name: "new row" }]);
  });

  it("consumes standalone insert operation IDs before execution and reports uncertainty", async () => {
    let rejectInsert!: (error: Error) => void;
    const executePreparedInsertPlan = vi.fn(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectInsert = reject;
        }),
    );
    const controller = new TableMutationPreviewController({
      connectionId: config.id,
      tableName: "items",
      connectionManager: {
        getQueryEditorPresentation: () => undefined,
      } as never,
      tableDataService: {
        executePreparedInsertPlan,
        executePreparedDeletePlan: vi.fn(),
      },
      notifyWarning: vi.fn(),
    });
    const insert: PreparedInsertPlan = {
      connectionId: config.id,
      database: "db",
      schema: "",
      table: "items",
      mode: "driver",
      values: {},
      operation: { sql: "INSERT", params: [] },
      previewStatements: ["INSERT"],
      verificationCriteria: null,
    };
    const first = controller.createInsertPreview("standalone", insert);
    const second = controller.createInsertPreview("standalone", insert);
    const running = controller.confirm(first.previewToken, "standalone");
    expect(
      await controller.confirm(second.previewToken, "standalone"),
    ).toBeNull();
    rejectInsert(new Error("Lost acknowledgement"));
    expect(await running).toMatchObject({
      type: "insertResult",
      payload: {
        success: false,
        changesPossible: true,
        outcomeUnknown: true,
        error: expect.stringContaining("do not repeat"),
      },
    });
    expect(executePreparedInsertPlan).toHaveBeenCalledOnce();
  });

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
