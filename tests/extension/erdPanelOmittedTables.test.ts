import { beforeEach, describe, expect, it, vi } from "vitest";

const getGraphMock = vi.hoisted(() => vi.fn());
const showWarningMessageMock = vi.hoisted(() => vi.fn());

const vscodeMock = vi.hoisted(() => {
  const createWebviewPanel = vi.fn(() => {
    const disposeListeners = new Set<() => void>();
    const messageListeners = new Set<
      (message: unknown) => void | Promise<void>
    >();
    return {
      title: "",
      webview: {
        html: "",
        postMessage: vi.fn(),
        onDidReceiveMessage(
          listener: (message: unknown) => void | Promise<void>,
        ) {
          messageListeners.add(listener);
          return { dispose: () => messageListeners.delete(listener) };
        },
        async dispatchMessage(message: unknown) {
          for (const listener of messageListeners) {
            await listener(message);
          }
        },
      },
      onDidDispose(listener: () => void) {
        disposeListeners.add(listener);
        return { dispose: () => disposeListeners.delete(listener) };
      },
      dispose() {
        for (const listener of disposeListeners) {
          listener();
        }
      },
      reveal: vi.fn(),
    };
  });

  return {
    createWebviewPanel,
    module: {
      ViewColumn: { One: 1 },
      window: {
        createWebviewPanel,
        showErrorMessage: vi.fn(),
        showWarningMessage: showWarningMessageMock,
      },
      workspace: {
        onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
      },
    },
  };
});

vi.mock("vscode", () => vscodeMock.module);

vi.mock("../../src/extension/panels/webviewShell", () => ({
  FULLSCREEN_WEBVIEW_SHELL_LAYOUT: {},
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

vi.mock("../../src/extension/services/erdGraphService", () => ({
  ErdGraphService: class {
    getGraph = getGraphMock;
    dispose = vi.fn();
  },
}));

vi.mock("../../src/extension/panels/tablePanel", () => ({
  TablePanel: { createOrShow: vi.fn() },
}));

const omittedGraph = {
  nodes: [],
  edges: [],
  scope: {},
  omittedTables: [
    {
      database: "app",
      schema: "main",
      table: "vec_items",
      error: "no such module: vec0",
    },
  ],
};

describe("ErdPanel with omitted tables", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("leaves unreadable tables to the in-diagram banner instead of a popup", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    getGraphMock
      .mockResolvedValueOnce({
        graph: omittedGraph,
        fromCache: false,
        partial: true,
      })
      .mockResolvedValueOnce({
        graph: omittedGraph,
        fromCache: true,
        partial: true,
      })
      .mockResolvedValueOnce({
        graph: { nodes: [], edges: [], scope: {} },
        fromCache: false,
        partial: true,
      });

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      {
        onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
        getConnection: vi.fn(() => ({ name: "Primary" })),
      } as never,
      { connectionId: "conn-1" },
    );
    const panel = vscodeMock.createWebviewPanel.mock.results[0]?.value;

    try {
      await panel.webview.dispatchMessage({ type: "ready" });
      await panel.webview.dispatchMessage({ type: "ready" });
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "erdGraph",
        payload: expect.objectContaining({
          graph: omittedGraph,
          partial: true,
        }),
      });
      expect(showWarningMessageMock).not.toHaveBeenCalled();

      await panel.webview.dispatchMessage({ type: "reload" });
      expect(showWarningMessageMock).toHaveBeenCalledTimes(1);
    } finally {
      ErdPanel.disposeAll();
    }
  });

  it("still warns about incomplete metadata when the graph also has unreadable tables", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    const incompleteGraph = { ...omittedGraph, metadataIncomplete: true };
    getGraphMock
      .mockResolvedValueOnce({
        graph: incompleteGraph,
        fromCache: false,
        partial: true,
      })
      .mockResolvedValueOnce({
        graph: omittedGraph,
        fromCache: true,
        partial: true,
      })
      .mockResolvedValueOnce({
        graph: incompleteGraph,
        fromCache: false,
        partial: true,
      });

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      {
        onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
        getConnection: vi.fn(() => ({ name: "Primary" })),
      } as never,
      { connectionId: "conn-1" },
    );
    const panel = vscodeMock.createWebviewPanel.mock.results[0]?.value;

    try {
      await panel.webview.dispatchMessage({ type: "ready" });
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "erdGraph",
        payload: expect.objectContaining({ graph: incompleteGraph }),
      });
      expect(showWarningMessageMock).toHaveBeenCalledTimes(1);
      expect(showWarningMessageMock).toHaveBeenCalledWith(
        expect.stringContaining("may be incomplete"),
      );

      await panel.webview.dispatchMessage({ type: "ready" });
      expect(showWarningMessageMock).toHaveBeenCalledTimes(1);

      await panel.webview.dispatchMessage({ type: "reload" });
      expect(showWarningMessageMock).toHaveBeenCalledTimes(2);
    } finally {
      ErdPanel.disposeAll();
    }
  });
});
