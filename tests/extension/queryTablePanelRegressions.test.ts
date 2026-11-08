import { beforeEach, describe, expect, it, vi } from "vitest";

describe("query and table panel regressions", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.doMock("vscode", () => ({
      window: { showWarningMessage: vi.fn() },
      env: { clipboard: { readText: vi.fn(async () => "") } },
    }));
    vi.doMock("../../src/extension/utils/queryResultFormatting", () => ({
      formatQueryResult: (result: {
        columns: string[];
        rows: Record<string, unknown>[];
      }) => ({
        columns: result.columns,
        columnMeta: [],
        rows: result.rows,
        rowCount: result.rows.length,
        executionTimeMs: 0,
      }),
    }));
  });

  it("keeps delivering the running query result when a superseding query is rejected", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    let resolveFirstQuery: ((value: unknown) => void) | undefined;
    const query = vi.fn(
      () =>
        new Promise((resolve) => {
          resolveFirstQuery = resolve;
        }),
    );
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Writable",
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };
    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "active"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    const first = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow", operationId: "op-slow" },
    });
    await vi.waitFor(() => expect(query).toHaveBeenCalledOnce());
    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select fresh", operationId: "op-fresh" },
    });
    resolveFirstQuery?.({ columns: ["q"], rows: [{ q: "slow" }] });
    await first;

    expect(query).toHaveBeenCalledOnce();
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        operationId: "op-fresh",
        error: expect.stringContaining("previous query is still running"),
      }),
    });
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        operationId: "op-slow",
        rows: [{ q: "slow" }],
      }),
    });
    expect(view.setLastQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ q: "slow" }] }),
    );
    warnSpy.mockRestore();
  });

  it("restores the accepted query, not a rejected one, after two superseding queries fail", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    let resolveFirstQuery: ((value: unknown) => void) | undefined;
    const query = vi.fn(
      () =>
        new Promise((resolve) => {
          resolveFirstQuery = resolve;
        }),
    );
    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "active"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      {
        getConnection: vi.fn(() => ({
          id: "active",
          name: "Writable",
          type: "pg",
          readOnly: false,
        })),
        getDriverCapabilities: vi.fn(() => ({})),
        isConnected: vi.fn(() => true),
        connectTo: vi.fn(async () => undefined),
        addToHistory: vi.fn(async () => undefined),
        getDriver: vi.fn(() => ({ query })),
        getQueryRowLimit: vi.fn(() => 100),
        getSchemaAsync: vi.fn(async () => []),
      } as never,
      view,
    );

    const first = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select a", operationId: "op-a" },
    });
    await vi.waitFor(() => expect(query).toHaveBeenCalledOnce());
    const second = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select b", operationId: "op-b" },
    });
    const third = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select c", operationId: "op-c" },
    });
    await Promise.all([second, third]);
    resolveFirstQuery?.({ columns: ["q"], rows: [{ q: "a" }] });
    await first;

    expect(query).toHaveBeenCalledOnce();
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        operationId: "op-c",
        error: expect.stringContaining("previous query is still running"),
      }),
    });
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        operationId: "op-a",
        rows: [{ q: "a" }],
      }),
    });
    expect(view.setLastQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ q: "a" }] }),
    );
    warnSpy.mockRestore();
  });

  it("reports the unique deleted row count for duplicated selections", async () => {
    const { TableMutationPreviewController } = await import(
      "../../src/extension/panels/tableMutationPreviewController"
    );
    const controller = new TableMutationPreviewController({
      connectionId: "conn",
      tableName: "items",
      connectionManager: {
        getQueryEditorPresentation: () => undefined,
        getConnection: () => ({ id: "conn", type: "pg" }),
      } as never,
      tableDataService: {
        executePreparedInsertPlan: vi.fn(async () => undefined),
        executePreparedDeletePlan: vi.fn(async () => undefined),
      },
      notifyWarning: vi.fn(),
    });
    const preview = controller.createDeleteRowsPreview("op-delete", {
      connectionId: "conn",
      database: "db",
      schema: "public",
      table: "items",
      executionMode: "transaction",
      rowIdentities: [{ id: 1 }, { id: 2 }, { id: 1 }],
      verificationCriteriaList: [{ id: 1 }, { id: 2 }],
      operations: [],
      previewStatements: ["DELETE"],
    });

    const result = await controller.confirm(preview.previewToken, "op-delete");

    expect(result).toMatchObject({
      type: "deleteResult",
      payload: {
        success: true,
        affectedRows: 2,
        rowOutcomes: [
          { rowIndex: 0, status: "deleted" },
          { rowIndex: 1, status: "deleted" },
          { rowIndex: 2, status: "deleted" },
        ],
      },
    });
  });

  it("bounds the executed preview operation ids", async () => {
    const { TableMutationPreviewController } = await import(
      "../../src/extension/panels/tableMutationPreviewController"
    );
    const controller = new TableMutationPreviewController({
      connectionId: "conn",
      tableName: "items",
      connectionManager: {
        getQueryEditorPresentation: () => undefined,
        getConnection: () => ({ id: "conn", type: "pg" }),
      } as never,
      tableDataService: {
        executePreparedInsertPlan: vi.fn(async () => undefined),
        executePreparedDeletePlan: vi.fn(async () => undefined),
      },
      notifyWarning: vi.fn(),
    });
    for (let index = 0; index < 1100; index++) {
      const preview = controller.createInsertPreview(`op-${index}`, {
        previewStatements: ["INSERT"],
      } as never);
      await controller.confirm(preview.previewToken, `op-${index}`);
    }

    const executed = (
      controller as unknown as { executedOperationIds: Set<string> }
    ).executedOperationIds;
    expect(executed.size).toBe(1000);
    expect(executed.has("op-0")).toBe(false);
    expect(executed.has("op-1099")).toBe(true);
  });
});
