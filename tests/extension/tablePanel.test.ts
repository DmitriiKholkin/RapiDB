import { beforeEach, describe, expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";

type MockColumn = { name: string; isPrimaryKey: boolean };

const getColumnsMock = vi.hoisted(() =>
  vi.fn(async (): Promise<MockColumn[]> => []),
);
const getPageMock = vi.hoisted(() =>
  vi.fn(async () => ({ rows: [], totalCount: 0, columns: [] })),
);
const prepareDeleteRowsPlanMock = vi.hoisted(() =>
  vi.fn<
    (
      connectionId: string,
      database: string,
      schema: string,
      table: string,
      primaryKeysList: Array<Record<string, unknown>>,
    ) => Promise<unknown | null>
  >(async () => null),
);
const createDeleteRowsPreviewMock = vi.hoisted(() => vi.fn());
const createWebviewShellMock = vi.hoisted(() => vi.fn(() => "<html></html>"));

const vscodeMock = vi.hoisted(() => {
  const configurationListeners = new Set<
    (event: { affectsConfiguration: (section: string) => boolean }) => void
  >();
  const createWebviewPanel = vi.fn(() => {
    const disposeListeners = new Set<() => void>();
    const messageListeners = new Set<
      (message: unknown) => void | Promise<void>
    >();
    const panel = {
      title: "",
      webview: {
        html: "",
        postMessage: vi.fn(),
        onDidReceiveMessage(
          listener: (message: unknown) => void | Promise<void>,
        ) {
          messageListeners.add(listener);
          return {
            dispose: () => {
              messageListeners.delete(listener);
            },
          };
        },
        async dispatchMessage(message: unknown) {
          for (const listener of messageListeners) {
            await listener(message);
          }
        },
      },
      onDidDispose(listener: () => void) {
        disposeListeners.add(listener);
        return {
          dispose: () => {
            disposeListeners.delete(listener);
          },
        };
      },
      dispose() {
        for (const listener of disposeListeners) {
          listener();
        }
      },
      reveal: vi.fn(),
    };
    return panel;
  });

  return {
    createWebviewPanel,
    dispatchConfigurationChange(section: string) {
      const event = {
        affectsConfiguration: (candidate: string) => candidate === section,
      };
      for (const listener of configurationListeners) {
        listener(event);
      }
    },
    module: {
      ViewColumn: { One: 1 },
      window: {
        createWebviewPanel,
        showWarningMessage: vi.fn(),
        showErrorMessage: vi.fn(),
      },
      workspace: {
        onDidChangeConfiguration: vi.fn((listener) => {
          configurationListeners.add(listener);
          return {
            dispose: () => {
              configurationListeners.delete(listener);
            },
          };
        }),
      },
    },
  };
});

vi.mock("vscode", () => vscodeMock.module);

vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: createWebviewShellMock,
}));

vi.mock("../../src/extension/tableDataService", () => ({
  TableDataService: class {
    getColumns = getColumnsMock;
    getPage = getPageMock;
    prepareDeleteRowsPlan = prepareDeleteRowsPlanMock;
    clearForConnection = vi.fn();
  },
  prepareApplyChangesPlan: vi.fn(),
}));

vi.mock("../../src/extension/panels/tableMutationPreviewController", () => ({
  TableMutationPreviewController: class {
    clear = vi.fn();
    confirm = vi.fn(async () => null);
    cancel = vi.fn();
    createApplyChangesPreview = vi.fn();
    createInsertPreview = vi.fn();
    createDeleteRowsPreview = createDeleteRowsPreviewMock;
  },
}));

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

describe("TablePanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    TablePanel.disposeAll();
    getColumnsMock.mockReset();
    getColumnsMock.mockResolvedValue([]);
    getPageMock.mockClear();
    prepareDeleteRowsPlanMock.mockReset();
    prepareDeleteRowsPlanMock.mockResolvedValue(null);
    createDeleteRowsPreviewMock.mockReset();
    createDeleteRowsPreviewMock.mockReturnValue({
      previewToken: "preview-token",
      kind: "deleteRows",
      title: "Apply changes to users",
      sql: "DELETE FROM users WHERE id = 1;",
      statementCount: 1,
    });
    createWebviewShellMock.mockClear();
  });

  it("normalizes fetchPage pagination before querying data service", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      1,
      expect.objectContaining({
        enableScripts: true,
        retainContextWhenHidden: true,
      }),
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: {
        fetchId: 1,
        page: "0",
        pageSize: "25000",
        filters: [],
        sort: null,
      },
    });

    expect(getPageMock).toHaveBeenNthCalledWith(
      1,
      "conn-1",
      "db1",
      "public",
      "users",
      1,
      10000,
      [],
      null,
    );

    await panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: {
        fetchId: 2,
        page: "abc",
        pageSize: "2.9",
        filters: [],
        sort: null,
      },
    });

    expect(getPageMock).toHaveBeenNthCalledWith(
      2,
      "conn-1",
      "db1",
      "public",
      "users",
      1,
      2,
      [],
      null,
    );
  });

  it("wires readonly state into the table webview initial state and ready payload", async () => {
    const columns = [{ name: "id", isPrimaryKey: true }];
    getColumnsMock.mockResolvedValueOnce(columns);

    const connectionManager = {
      getConnection: vi.fn(() => ({
        name: "Readonly",
        type: "pg",
        readOnly: true,
      })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 50),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    expect(createWebviewShellMock).toHaveBeenCalledWith(
      expect.objectContaining({
        initialState: expect.objectContaining({
          view: "table",
          connectionId: "conn-1",
          database: "db1",
          schema: "public",
          table: "users",
          isView: false,
          connectionReadOnly: true,
          defaultPageSize: 50,
        }),
      }),
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({ type: "ready" });

    expect(getColumnsMock).toHaveBeenCalledWith(
      "conn-1",
      "db1",
      "public",
      "users",
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "tableInit",
      payload: {
        columns,
        primaryKeyColumns: ["id"],
        isView: false,
        connectionReadOnly: true,
      },
    });
  });

  it("does not force table re-init for an open panel after connection settings change", async () => {
    const columns = [{ name: "id", isPrimaryKey: true }];
    getColumnsMock.mockResolvedValue(columns);

    let readOnly = false;
    const connectionManager = {
      getConnection: vi.fn(() => ({
        name: "Main",
        type: "pg",
        readOnly,
      })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({ type: "ready" });

    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "tableInit",
      payload: expect.objectContaining({ connectionReadOnly: false }),
    });
    expect(getColumnsMock).toHaveBeenCalledTimes(1);

    readOnly = true;
    vscodeMock.dispatchConfigurationChange("rapidb.connections");

    expect(getColumnsMock).toHaveBeenCalledTimes(1);
    expect(panel.webview.postMessage).toHaveBeenCalledTimes(1);
    expect(panel.title).toContain("[Main]");
  });

  it("reuses existing panel on reveal without re-creating or re-initializing data", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        name: "Main",
        type: "pg",
        readOnly: false,
      })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({ type: "ready" });
    expect(getColumnsMock).toHaveBeenCalledTimes(1);

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(1);
    expect(panel.reveal).toHaveBeenCalledTimes(1);
    expect(getColumnsMock).toHaveBeenCalledTimes(1);
  });

  it("deduplicates concurrent fetchPage calls with identical parameters", async () => {
    type FetchResult = { rows: []; totalCount: number; columns: [] };
    let resolveFetch: ((value: FetchResult) => void) | undefined;
    getPageMock.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveFetch = resolve;
        }),
    );

    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    const firstFetchPromise = panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: {
        fetchId: 1,
        page: 1,
        pageSize: 25,
        filters: [],
        sort: null,
      },
    });
    const secondFetchPromise = panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: {
        fetchId: 2,
        page: 1,
        pageSize: 25,
        filters: [],
        sort: null,
      },
    });

    expect(getPageMock).toHaveBeenCalledTimes(1);

    if (!resolveFetch) {
      throw new Error("Expected in-flight fetch resolver");
    }
    resolveFetch({ rows: [], totalCount: 0, columns: [] });

    await Promise.all([firstFetchPromise, secondFetchPromise]);

    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "tableData",
      payload: { fetchId: 1, rows: [], totalCount: 0 },
    });
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "tableData",
      payload: { fetchId: 2, rows: [], totalCount: 0 },
    });
  });

  it("routes deleteRows through mutation preview when prepared plan exists", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    prepareDeleteRowsPlanMock.mockResolvedValueOnce({
      connectionId: "conn-1",
      database: "db1",
      schema: "public",
      table: "users",
      executionMode: "sequential",
      operations: [{ sql: "DELETE FROM users WHERE id = ?", params: [1] }],
      previewStatements: ["DELETE FROM users WHERE id = 1"],
      verificationCriteriaList: [{ id: 1 }],
    });

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 1 }] },
    });

    expect(prepareDeleteRowsPlanMock).toHaveBeenCalledWith(
      "conn-1",
      "db1",
      "public",
      "users",
      [{ id: 1 }],
    );
    expect(createDeleteRowsPreviewMock).toHaveBeenCalledOnce();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "tableMutationPreview",
      payload: expect.objectContaining({ kind: "deleteRows" }),
    });
  });

  it("returns immediate success when prepared delete plan is empty", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    prepareDeleteRowsPlanMock.mockResolvedValueOnce(null);

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { primaryKeysList: [] },
    });

    expect(createDeleteRowsPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "deleteResult",
      payload: { success: true },
    });
  });

  it("returns delete failure when preparing delete preview plan throws", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    prepareDeleteRowsPlanMock.mockRejectedValueOnce(new Error("Plan failed"));

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 1 }] },
    });

    expect(createDeleteRowsPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "deleteResult",
      payload: { success: false, error: "Plan failed" },
    });
  });

  it("uses connection-specific object labels in the tab title", () => {
    const redisManager = {
      getConnection: vi.fn(() => ({ name: "Cache", type: "redis" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      redisManager as never,
      "conn-redis",
      "db0",
      "db0",
      "activity",
    );

    expect(vscodeMock.createWebviewPanel).toHaveBeenLastCalledWith(
      "rapidb.tablePanel",
      "activity (keyspace) [Cache]",
      expect.anything(),
      expect.anything(),
    );

    TablePanel.disposeAll();

    const mongoManager = {
      getConnection: vi.fn(() => ({ name: "Docs", type: "mongodb" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      mongoManager as never,
      "conn-mongo",
      "app_db",
      "app_db",
      "users",
      false,
      "table",
    );

    expect(vscodeMock.createWebviewPanel).toHaveBeenLastCalledWith(
      "rapidb.tablePanel",
      "users (collection) [Docs]",
      expect.anything(),
      expect.anything(),
    );
  });

  it("uses driver capabilities to classify filter errors", async () => {
    getPageMock.mockRejectedValueOnce(
      new Error("invalid input syntax for type uuid"),
    );

    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      getDriverCapabilities: vi.fn(() => ({
        isTableFilterError: (message: string) =>
          /invalid input syntax/i.test(message),
      })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
    };

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected table panel instance");
    }

    await panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: {
        fetchId: 3,
        page: 1,
        pageSize: 25,
        filters: [{ column: "id", operator: "eq", value: "bad-uuid" }],
        sort: null,
      },
    });

    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "tableError",
      payload: {
        fetchId: 3,
        error: "invalid input syntax for type uuid",
        isFilterError: true,
      },
    });
  });
});
