import { beforeEach, describe, expect, it, vi } from "vitest";

const getGraphMock = vi.hoisted(() => vi.fn());
const tableCreateOrShowMock = vi.hoisted(() => vi.fn());
const schemaCreateOrShowMock = vi.hoisted(() => vi.fn());

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

vi.mock("../../src/extension/services/erdGraphService", () => ({
  ErdGraphService: class {
    getGraph = getGraphMock;
    dispose = vi.fn();
  },
}));

vi.mock("../../src/extension/panels/tablePanel", () => ({
  TablePanel: {
    createOrShow: tableCreateOrShowMock,
  },
}));

vi.mock("../../src/extension/panels/schemaPanel", () => ({
  SchemaPanel: {
    createOrShow: schemaCreateOrShowMock,
  },
}));

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

describe("ErdPanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getGraphMock.mockResolvedValue({
      graph: {
        nodes: [],
        edges: [],
        scope: {},
      },
      fromCache: false,
    });
  });

  it("loads graph on ready", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");

    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      {
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      },
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected ERD panel instance");
    }

    await panel.webview.dispatchMessage({ type: "ready" });

    expect(getGraphMock).toHaveBeenCalledWith(
      {
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      },
      false,
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "erdGraph",
      }),
    );

    ErdPanel.disposeAll();
  });

  it("reloads the graph when requested", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");

    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      {
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      },
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected ERD panel instance");
    }

    await panel.webview.dispatchMessage({ type: "reload" });

    expect(getGraphMock).toHaveBeenCalledWith(
      {
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      },
      true,
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "erdLoading",
      payload: {
        forceReload: true,
      },
    });

    ErdPanel.disposeAll();
  });

  it("posts erdError when graph loading fails", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    getGraphMock.mockRejectedValueOnce(new Error("Metadata fetch failed"));

    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      {
        connectionId: "conn-1",
      },
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected ERD panel instance");
    }

    await panel.webview.dispatchMessage({ type: "ready" });

    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "erdError",
      payload: {
        error: "Metadata fetch failed",
      },
    });

    ErdPanel.disposeAll();
  });

  it("delegates schema and data actions", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");

    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      {
        connectionId: "conn-1",
      },
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected ERD panel instance");
    }

    await panel.webview.dispatchMessage({
      type: "openSchema",
      payload: {
        table: "users",
        database: "app_db",
        schema: "public",
      },
    });

    await panel.webview.dispatchMessage({
      type: "openTableData",
      payload: {
        table: "users",
        database: "app_db",
        schema: "public",
        isView: false,
      },
    });

    expect(schemaCreateOrShowMock).toHaveBeenCalledWith(
      expect.anything(),
      connectionManager,
      "conn-1",
      "app_db",
      "public",
      "users",
    );

    expect(tableCreateOrShowMock).toHaveBeenCalledWith(
      expect.anything(),
      connectionManager,
      "conn-1",
      "app_db",
      "public",
      "users",
      false,
    );

    ErdPanel.disposeAll();
  });

  it("uses database.schema title format for schema-level ERD", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");

    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      {
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      },
    );

    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledWith(
      "rapidb.erdPanel",
      "app_db.public (ERD) [Primary]",
      expect.anything(),
      expect.anything(),
    );

    ErdPanel.disposeAll();
  });

  it("uses database title format for database-level ERD", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");

    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      {
        connectionId: "conn-1",
        database: "app_db",
      },
    );

    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledWith(
      "rapidb.erdPanel",
      "app_db (ERD) [Primary]",
      expect.anything(),
      expect.anything(),
    );

    ErdPanel.disposeAll();
  });
});
