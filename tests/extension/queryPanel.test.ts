import { beforeEach, describe, expect, it, vi } from "vitest";

const vscodeMock = vi.hoisted(() => {
  const serializers = new Map<string, unknown>();
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
  const registerWebviewPanelSerializer = vi.fn(
    (viewType: string, serializer: unknown) => {
      serializers.set(viewType, serializer);
      return { dispose: vi.fn() };
    },
  );

  return {
    createWebviewPanel,
    registerWebviewPanelSerializer,
    serializers,
    module: {
      ViewColumn: { One: 1 },
      window: {
        createWebviewPanel,
        registerWebviewPanelSerializer,
      },
      workspace: {
        onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
      },
    },
  };
});

vi.mock("vscode", () => vscodeMock.module);
vi.mock("../../src/extension/panels/webviewShell", () => ({
  APP_WEBVIEW_SHELL_LAYOUT: {
    htmlStyles: "height: 100%; overflow: hidden;",
    bodyStyles: "height: 100%; overflow: hidden;",
    rootStyles: "height: 100vh;",
  },
  FULLSCREEN_WEBVIEW_SHELL_LAYOUT: {
    htmlStyles: "height: 100%; overflow: hidden;",
    bodyStyles: "height: 100%; overflow: hidden;",
    rootStyles: "height: 100vh; overflow: hidden;",
  },
  WEBVIEW_SCROLLBAR_STYLES: "",
  MONACO_SCROLLBAR_STYLES: "",
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

function createEventSource<T>() {
  const listeners = new Set<(value: T) => void>();

  return {
    event: vi.fn((listener: (value: T) => void) => {
      listeners.add(listener);
      return {
        dispose: () => {
          listeners.delete(listener);
        },
      };
    }),
    fire(value: T) {
      for (const listener of listeners) {
        listener(value);
      }
    },
  };
}

describe("QueryPanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vscodeMock.serializers.clear();
  });

  it("preserves driver-owned formatOnOpen defaults when no explicit override is provided", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => ({
        formatOnOpen: true,
        editorLanguage: "sql",
        sqlDialect: "postgresql",
      })),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      getConnectedCount: vi.fn(() => 1),
    };

    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "select 1",
      true,
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

    const initialState = (createWebviewShell as ReturnType<typeof vi.fn>).mock
      .calls[0]?.[0]?.initialState;

    expect(initialState).toMatchObject({
      connectionType: "pg",
      editorPresentation: {
        formatOnOpen: true,
        editorLanguage: "sql",
        sqlDialect: "postgresql",
      },
    });
    expect(initialState.formatOnOpen).toBeUndefined();
  });

  it.each([
    "qp_42",
    "qp_701acb27-3ee6-4ff9-8424-477d65022fb2",
  ])("restores serialized query panel %s using retained webview state", async (panelId) => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => []),
    };
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );
    QueryPanel.disposeAll();
    QueryPanel.registerSerializer(
      { extensionUri: {} } as never,
      connectionManager as never,
    );
    const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const panel = vscodeMock.createWebviewPanel();

    await serializer.deserializeWebviewPanel(panel, {
      initialState: {
        view: "query",
        panelId,
        connectionId: "conn-1",
        queryText: "select 1",
        initialSql: "select 1",
      },
      queryDraft: {
        panelId,
        text: "select 2",
        activeConnectionId: "conn-1",
      },
    });

    expect(panel.webview.html).toBe("<html></html>");
    expect(connectionManager.getConnection).toHaveBeenCalledWith("conn-1");
    panel.dispose();
    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
    );
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);
    QueryPanel.disposeAll();
  });

  it.each([
    "new",
    "restored",
  ])("keeps both panels registered when a legacy panel restores lazily and %s closes first", async (closedFirst) => {
    // Simulate a restarted host: no serializer has reserved legacy qp_1 yet.
    vi.resetModules();
    const context = { extensionUri: {} } as never;
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-lazy",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );
    QueryPanel.registerSerializer(context, connectionManager as never);
    const newInstance = QueryPanel.createOrShow(
      context,
      connectionManager as never,
      "conn-lazy",
    );
    const newPanel = createdPanel();
    const newPanelId = (
      createWebviewShell as ReturnType<typeof vi.fn>
    ).mock.calls.at(-1)?.[0]?.initialState?.panelId;
    expect(newPanelId).toMatch(/^qp_[0-9a-f]{8}-[0-9a-f-]{27}$/);
    const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const restoredPanel = vscodeMock.createWebviewPanel();
    await serializer.deserializeWebviewPanel(restoredPanel, {
      initialState: {
        panelId: "qp_1",
        connectionId: "conn-lazy",
        queryText: "select 1",
      },
      queryDraft: {
        panelId: "qp_1",
        text: "select 2",
        activeConnectionId: "conn-lazy",
      },
    });
    expect(createWebviewShell).toHaveBeenLastCalledWith(
      expect.objectContaining({
        initialState: expect.objectContaining({
          panelId: "qp_1",
          queryText: "select 1",
        }),
      }),
    );
    expect(
      QueryPanel.createOrShow(context, connectionManager as never, "conn-lazy"),
    ).toBe(newInstance);
    expect(newPanel.reveal).toHaveBeenCalledOnce();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);

    const firstPanel = closedFirst === "new" ? newPanel : restoredPanel;
    const remainingPanel = closedFirst === "new" ? restoredPanel : newPanel;
    const remainingDispose = vi.spyOn(remainingPanel, "dispose");
    firstPanel.dispose();
    const remainingInstance = QueryPanel.createOrShow(
      context,
      connectionManager as never,
      "conn-lazy",
    );
    expect(remainingPanel.reveal).toHaveBeenCalledWith(1);
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);
    if (closedFirst === "new") expect(remainingInstance).not.toBe(newInstance);
    else expect(remainingInstance).toBe(newInstance);
    QueryPanel.disposeAll();
    expect(remainingDispose).toHaveBeenCalledOnce();
    expect(
      QueryPanel.createOrShow(context, connectionManager as never, "conn-lazy"),
    ).not.toBe(remainingInstance);
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(3);
    QueryPanel.disposeAll();
  });

  it("passes editor language overrides through the webview initial state", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Mongo",
        type: "mongodb",
      })),
      getQueryEditorPresentation: vi.fn(() => ({
        formatOnOpen: false,
        editorLanguage: "javascript",
      })),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Mongo", type: "mongodb" },
      ]),
      getConnectedCount: vi.fn(() => 1),
    };

    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      'db.users.createCollection("users")',
      true,
      false,
      false,
      "javascript",
    );

    expect(createWebviewShell).toHaveBeenCalledWith(
      expect.objectContaining({
        initialState: expect.objectContaining({
          connectionType: "mongodb",
          editorLanguage: "javascript",
          formatOnOpen: false,
          editorPresentation: {
            formatOnOpen: false,
            editorLanguage: "javascript",
          },
        }),
      }),
    );
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
      getQueryEditorPresentation: vi.fn(() => undefined),
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

  it("pushes incremental shared-cache schema updates and refresh reloads into the webview", async () => {
    const schemaLoad = createEventSource<string>();
    const refreshSchemas = createEventSource<void>();
    const mergedSchema = [
      {
        database: "app_db",
        schema: "public",
        object: "users",
        columns: [],
      },
      {
        database: "app_db",
        schema: "audit",
        object: "sync_events",
        columns: [],
      },
    ];
    const refreshedSchema = [
      ...mergedSchema,
      {
        database: "app_db",
        schema: "audit",
        object: "sync_event_archive",
        columns: [],
      },
    ];

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: schemaLoad.event,
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: refreshSchemas.event,
      isConnected: vi.fn((id: string) => id === "conn-1"),
      getSchema: vi.fn(() => mergedSchema),
      getSchemaAsync: vi.fn(async () => refreshedSchema),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      getConnectedCount: vi.fn(() => 1),
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

    panel.webview.postMessage.mockClear();

    schemaLoad.fire("conn-1");

    await Promise.resolve();
    await Promise.resolve();

    expect(connectionManager.getSchema).toHaveBeenCalledWith("conn-1");
    expect(connectionManager.getSchemaAsync).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "conn-1", schema: mergedSchema },
    });

    panel.webview.postMessage.mockClear();
    connectionManager.getSchemaAsync.mockClear();

    refreshSchemas.fire(undefined);

    await Promise.resolve();
    await Promise.resolve();

    expect(connectionManager.getSchemaAsync).toHaveBeenCalledWith("conn-1");
    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "schema",
        payload: { connectionId: "conn-1", schema: refreshedSchema },
      }),
    );
  });

  it("disposes the controller so panel-owned queries are cancelled", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-dispose",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const disposeSpy = vi
      .spyOn(QueryPanelController.prototype, "dispose")
      .mockResolvedValue(undefined);
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-dispose",
      "select 1",
      true,
    );
    createdPanel()?.dispose();

    expect(disposeSpy).toHaveBeenCalledOnce();
    disposeSpy.mockRestore();
  });

  it("assigns distinct operation namespaces to separate query panels", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-namespace",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-namespace",
      undefined,
      true,
    );
    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-namespace",
      undefined,
      true,
    );

    const calls = (createWebviewShell as ReturnType<typeof vi.fn>).mock.calls;
    const firstPanelId = calls.at(-2)?.[0]?.initialState?.panelId;
    const secondPanelId = calls.at(-1)?.[0]?.initialState?.panelId;
    expect(firstPanelId).toMatch(/^qp_/);
    expect(secondPanelId).toMatch(/^qp_/);
    expect(secondPanelId).not.toBe(firstPanelId);
  });

  it("swallows postMessage failures without unhandled rejection", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-pm",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-pm",
      "select 1",
      true,
    );
    const panel = createdPanel();
    // Simulate disposed webview: postMessage rejects / resolves false.
    panel.webview.postMessage.mockRejectedValueOnce(new Error("gone"));
    await panel.webview.dispatchMessage({ type: "ready" });
    await new Promise((resolve) => setTimeout(resolve, 10));

    panel.webview.postMessage.mockResolvedValueOnce(false);
    await panel.webview.dispatchMessage({ type: "ready" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    // No throw = no unhandled rejection.
  });

  it("survives controller dispose rejection on panel dispose", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-dispose-err",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const disposeSpy = vi
      .spyOn(QueryPanelController.prototype, "dispose")
      .mockRejectedValueOnce(new Error("dispose boom"));
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-dispose-err",
      "select 1",
      true,
    );
    // Must not throw synchronously; rejection is caught + logged.
    createdPanel()?.dispose();
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(disposeSpy).toHaveBeenCalledOnce();
    disposeSpy.mockRestore();
  });
});
