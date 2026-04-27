import { beforeEach, describe, expect, it, vi } from "vitest";

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

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

describe("QueryPanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("refreshes schema in an open editor when the connection becomes available", async () => {
    let connectListener: (() => void) | undefined;
    let disconnectListener: (() => void) | undefined;
    let connected = false;

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn((listener: () => void) => {
        connectListener = listener;
        return { dispose: vi.fn() };
      }),
      onDidDisconnect: vi.fn((listener: () => void) => {
        disconnectListener = listener;
        return { dispose: vi.fn() };
      }),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      isConnected: vi.fn((id: string) => connected && id === "conn-1"),
      getSchemaAsync: vi.fn(async (id: string) => [
        {
          database: "app_db",
          schema: "public",
          object: id === "conn-1" ? "users" : "other",
          columns: [],
        },
      ]),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      getConnectedCount: vi.fn(() => (connected ? 1 : 0)),
    };

    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "select 1",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    expect(connectListener).toBeDefined();
    expect(disconnectListener).toBeDefined();

    connected = true;
    connectListener?.();

    await Promise.resolve();
    await Promise.resolve();

    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "connections",
        payload: expect.arrayContaining([
          expect.objectContaining({ id: "conn-1", name: "Primary" }),
        ]),
      }),
    );

    expect(connectionManager.getSchemaAsync).toHaveBeenCalledWith("conn-1");

    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "schema",
        payload: expect.objectContaining({
          connectionId: "conn-1",
          schema: expect.arrayContaining([
            expect.objectContaining({ object: "users" }),
          ]),
        }),
      }),
    );

    panel.webview.postMessage.mockClear();
    connected = false;
    disconnectListener?.();

    await Promise.resolve();
    await Promise.resolve();

    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "conn-1", schema: [] },
    });
  });
});
