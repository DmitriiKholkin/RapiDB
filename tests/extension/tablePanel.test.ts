import { beforeEach, describe, expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";

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

const vscodeMock = vi.hoisted(() => {
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
    module: {
      ViewColumn: { One: 1 },
      window: {
        createWebviewPanel,
        showWarningMessage: vi.fn(),
        showErrorMessage: vi.fn(),
      },
      workspace: {
        onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
      },
    },
  };
});

vi.mock("vscode", () => vscodeMock.module);

vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

vi.mock("../../src/extension/tableDataService", () => ({
  TableDataService: class {
    getColumns = vi.fn(async () => []);
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
});
