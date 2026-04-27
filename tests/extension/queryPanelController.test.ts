import { beforeEach, describe, expect, it, vi } from "vitest";

describe("QueryPanelController", () => {
  let showWarningMessage: ReturnType<typeof vi.fn>;
  let exportQueryResultsAsCsv: ReturnType<typeof vi.fn>;
  let exportQueryResultsAsJson: ReturnType<typeof vi.fn>;
  let formatQueryResult: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.resetModules();

    showWarningMessage = vi.fn();
    exportQueryResultsAsCsv = vi.fn(async () => undefined);
    exportQueryResultsAsJson = vi.fn(async () => undefined);
    formatQueryResult = vi.fn(
      (result: {
        columns: string[];
        rows: Record<string, unknown>[];
        columnMeta?: unknown[];
        rowCount?: number;
        executionTimeMs?: number;
      }) => ({
        columns: result.columns,
        columnMeta: result.columnMeta ?? [],
        rows: result.rows,
        rowCount: result.rowCount ?? result.rows.length,
        executionTimeMs: result.executionTimeMs ?? 0,
      }),
    );

    vi.doMock("vscode", () => ({
      window: {
        showWarningMessage,
      },
      env: {
        clipboard: {
          readText: vi.fn(async () => ""),
        },
      },
    }));

    vi.doMock("../../src/extension/utils/exportService", () => ({
      exportQueryResultsAsCsv,
      exportQueryResultsAsJson,
    }));

    vi.doMock("../../src/extension/utils/queryResultFormatting", () => ({
      formatQueryResult,
    }));
  });

  it("keeps explicit, active, then initial connection precedence", async () => {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 5,
    }));
    const addToHistory = vi.fn(async () => undefined);
    const addBookmark = vi.fn(async () => undefined);
    const getSchemaAsync = vi.fn(async () => []);
    const connectTo = vi.fn(async () => undefined);
    const isConnected = vi.fn(
      (connectionId: string) => connectionId !== "initial",
    );
    let activeConnectionId = "active";

    const connectionManager = {
      isConnected,
      connectTo,
      addToHistory,
      addBookmark,
      getDriver: vi.fn((connectionId: string) =>
        connectionId === "override" || connectionId === "active"
          ? { query }
          : undefined,
      ),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync,
    };

    const view = {
      getActiveConnectionId: vi.fn(() => activeConnectionId),
      getInitialConnectionId: vi.fn(() => "initial"),
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

    await controller.handleMessage({
      type: "executeQuery",
      payload: { sql: "select 1", connectionId: "override" },
    });

    expect(isConnected).toHaveBeenNthCalledWith(1, "override");
    expect(addToHistory).toHaveBeenNthCalledWith(1, "override", "select 1");
    expect(query).toHaveBeenNthCalledWith(1, "select 1");

    await controller.handleMessage({
      type: "addBookmark",
      payload: { sql: "select 2" },
    });

    expect(addBookmark).toHaveBeenCalledWith("active", "select 2");

    activeConnectionId = "";

    await controller.handleMessage({
      type: "getSchema",
      payload: {},
    });

    expect(isConnected).toHaveBeenLastCalledWith("initial");
    expect(connectTo).not.toHaveBeenCalledWith("initial");
    expect(getSchemaAsync).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "initial", schema: [] },
    });
  });

  it("warns and skips both exports when cached results are empty", async () => {
    const connectionManager = {
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => ({ columns: [], rows: [] })),
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

    await controller.handleMessage({ type: "exportResultsCSV" });
    await controller.handleMessage({ type: "exportResultsJSON" });

    expect(showWarningMessage).toHaveBeenCalledTimes(2);
    expect(showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] No query results to export.",
    );
    expect(exportQueryResultsAsCsv).not.toHaveBeenCalled();
    expect(exportQueryResultsAsJson).not.toHaveBeenCalled();
  });
});
