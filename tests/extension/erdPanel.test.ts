import { beforeEach, describe, expect, it, vi } from "vitest";

const getGraphMock = vi.hoisted(() => vi.fn());
const tableCreateOrShowMock = vi.hoisted(() => vi.fn());
const showErrorMessageMock = vi.hoisted(() => vi.fn());
const showWarningMessageMock = vi.hoisted(() => vi.fn());
const registerWebviewPanelSerializerMock = vi.hoisted(() =>
  vi.fn(() => ({ dispose: vi.fn() })),
);

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
        showErrorMessage: showErrorMessageMock,
        showWarningMessage: showWarningMessageMock,
        registerWebviewPanelSerializer: registerWebviewPanelSerializerMock,
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

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

describe("ErdPanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    showErrorMessageMock.mockReset();
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

    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      1,
      expect.objectContaining({
        enableScripts: true,
        retainContextWhenHidden: false,
      }),
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
      expect.any(AbortSignal),
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "erdGraph",
      }),
    );

    ErdPanel.disposeAll();
  });

  it.each([
    "database",
    "schema",
  ] as const)("preserves exact %s identity in panel keys and requests", async (field) => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const context = { extensionUri: {} } as never;
    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };
    try {
      const names = [undefined, "sales", " sales ", "   "];
      for (const [index, name] of names.entries()) {
        const scope = {
          connectionId: "conn-1",
          database: "app_db",
          [field]: name,
        };
        ErdPanel.createOrShow(context, connectionManager as never, scope);
        expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(index + 1);
        expect(createWebviewShell).toHaveBeenLastCalledWith(
          expect.objectContaining({
            initialState: expect.objectContaining(scope),
          }),
        );
        const panel = vscodeMock.createWebviewPanel.mock.results[index]?.value;
        if (!panel) {
          throw new Error("Expected separate ERD panel instance");
        }
        await panel.webview.dispatchMessage({ type: "ready" });
        expect(getGraphMock).toHaveBeenLastCalledWith(
          { ...scope, schema: field === "schema" ? name : undefined },
          false,
          expect.any(AbortSignal),
        );
        await panel.webview.dispatchMessage({
          type: "openTableData",
          payload: { database: name, schema: name, table: "orders" },
        });
        expect(tableCreateOrShowMock).toHaveBeenLastCalledWith(
          context,
          connectionManager,
          "conn-1",
          name ?? scope.database ?? "",
          name ?? "",
          "orders",
          false,
        );
        ErdPanel.createOrShow(context, connectionManager as never, scope);
        expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(index + 1);
        expect(panel.reveal).toHaveBeenCalledTimes(1);
      }
      ErdPanel.createOrShow(context, connectionManager as never, {
        connectionId: "conn-1",
        database: "app_db",
        [field]: "",
      });
      expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(4);
      expect(createdPanel()?.reveal).toHaveBeenCalledTimes(2);
    } finally {
      ErdPanel.disposeAll();
    }
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
      expect.any(AbortSignal),
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "erdLoading",
      payload: {
        forceReload: true,
      },
    });

    ErdPanel.disposeAll();
  });

  it("warns when the loaded graph is partial", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    getGraphMock.mockResolvedValueOnce({
      graph: { nodes: [], edges: [], scope: {} },
      fromCache: false,
      partial: true,
    });

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
      type: "erdGraph",
      payload: expect.objectContaining({ partial: true }),
    });
    expect(showWarningMessageMock).toHaveBeenCalledWith(
      expect.stringContaining("may be incomplete"),
    );

    getGraphMock.mockResolvedValueOnce({
      graph: { nodes: [], edges: [], scope: {} },
      fromCache: true,
      partial: true,
    });
    await panel.webview.dispatchMessage({ type: "ready" });

    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "erdGraph",
      payload: expect.objectContaining({ fromCache: true, partial: true }),
    });
    expect(showWarningMessageMock).toHaveBeenCalledTimes(1);

    ErdPanel.disposeAll();
  });

  it("warns once when a new panel receives a cached partial graph", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    getGraphMock.mockResolvedValue({
      graph: { nodes: [], edges: [], scope: {} },
      fromCache: true,
      partial: true,
    });

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
    expect(showWarningMessageMock).toHaveBeenCalledTimes(1);

    await panel.webview.dispatchMessage({ type: "ready" });
    expect(showWarningMessageMock).toHaveBeenCalledTimes(1);

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

  it("delegates table data actions", async () => {
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
      type: "openTableData",
      payload: {
        table: "users",
        database: "app_db",
        schema: "public",
        isView: false,
      },
    });

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

  it("reports unexpected panel message errors instead of rejecting", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    const consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    tableCreateOrShowMock.mockImplementationOnce(() => {
      throw new Error("table open failed");
    });

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

    try {
      await expect(
        panel.webview.dispatchMessage({
          type: "openTableData",
          payload: {
            table: "users",
          },
        }),
      ).resolves.toBeUndefined();

      expect(consoleErrorSpy).toHaveBeenCalledWith(
        "[RapiDB] ErdPanel unhandled error:",
        expect.stringContaining("Error: table open failed"),
      );
      expect(showErrorMessageMock).toHaveBeenCalledWith(
        "[RapiDB] Unexpected error: table open failed",
      );
    } finally {
      consoleErrorSpy.mockRestore();
      ErdPanel.disposeAll();
    }
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

  it("aborts the in-flight graph load and stays silent after the panel closes", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    let loadSignal: AbortSignal | undefined;
    let resolveGraph: ((value: unknown) => void) | undefined;
    getGraphMock.mockImplementationOnce(
      (_request: unknown, _forceReload: boolean, signal: AbortSignal) => {
        loadSignal = signal;
        return new Promise((resolve) => {
          resolveGraph = resolve;
        });
      },
    );
    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      { connectionId: "conn-1", database: "app_db" },
    );
    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected ERD panel instance");
    }

    try {
      const pending = panel.webview.dispatchMessage({ type: "ready" });
      expect(loadSignal?.aborted).toBe(false);

      panel.dispose();
      expect(loadSignal?.aborted).toBe(true);

      resolveGraph?.({
        graph: { nodes: [], edges: [], scope: {} },
        fromCache: false,
        partial: true,
      });
      await pending;

      expect(panel.webview.postMessage).toHaveBeenCalledTimes(1);
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "erdLoading",
        payload: { forceReload: false },
      });
      expect(showWarningMessageMock).not.toHaveBeenCalled();

      await panel.webview.dispatchMessage({ type: "reload" });
      expect(getGraphMock).toHaveBeenCalledTimes(1);
      expect(panel.webview.postMessage).toHaveBeenCalledTimes(1);
    } finally {
      ErdPanel.disposeAll();
    }
  });

  it("aborts a superseded graph load without reporting its cancellation", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    const signals: AbortSignal[] = [];
    getGraphMock.mockImplementationOnce(
      (_request: unknown, _forceReload: boolean, signal: AbortSignal) => {
        signals.push(signal);
        return new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        });
      },
    );
    getGraphMock.mockImplementationOnce(
      async (_request: unknown, _forceReload: boolean, signal: AbortSignal) => {
        signals.push(signal);
        return {
          graph: { nodes: [], edges: [], scope: {} },
          fromCache: false,
        };
      },
    );
    const connectionManager = {
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getConnection: vi.fn(() => ({ name: "Primary" })),
    };

    ErdPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      { connectionId: "conn-1" },
    );
    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected ERD panel instance");
    }

    try {
      const first = panel.webview.dispatchMessage({ type: "ready" });
      await panel.webview.dispatchMessage({ type: "reload" });
      await first;

      expect(signals).toHaveLength(2);
      expect(signals[0].aborted).toBe(true);
      expect(signals[1].aborted).toBe(false);
      expect(panel.webview.postMessage).not.toHaveBeenCalledWith(
        expect.objectContaining({ type: "erdError" }),
      );
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith(
        expect.objectContaining({ type: "erdGraph" }),
      );
    } finally {
      ErdPanel.disposeAll();
    }
  });

  describe("registerSerializer", () => {
    function registeredSerializer() {
      const call = registerWebviewPanelSerializerMock.mock.calls.at(-1) as
        | unknown[]
        | undefined;
      if (!call) {
        throw new Error("Expected ERD serializer registration");
      }
      return {
        viewType: call[0],
        serializer: call[1] as {
          deserializeWebviewPanel(
            panel: unknown,
            state: unknown,
          ): Promise<void>;
        },
      };
    }

    function revivedPanel() {
      const panel = vscodeMock.createWebviewPanel();
      const dispose = vi.spyOn(panel, "dispose");
      return { panel, dispose };
    }

    it("restores a persisted ERD panel for its saved scope", async () => {
      const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
      const context = { extensionUri: {} } as never;
      const connectionManager = {
        onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
        getConnection: vi.fn(() => ({ name: "Primary" })),
      };

      try {
        ErdPanel.registerSerializer(context, connectionManager as never);
        const { viewType, serializer } = registeredSerializer();
        expect(viewType).toBe("rapidb.erdPanel");

        const { panel, dispose } = revivedPanel();
        await serializer.deserializeWebviewPanel(panel, {
          initialState: {
            view: "erd",
            connectionId: "conn-1",
            database: "app_db",
            schema: "public",
            panelRetentionMode: "rehydrate",
          },
          viewport: { x: 0, y: 0, zoom: 1 },
        });

        expect(dispose).not.toHaveBeenCalled();
        expect(panel.title).toBe("app_db.public (ERD) [Primary]");
        expect(panel.webview.html).toBe("<html></html>");

        await panel.webview.dispatchMessage({ type: "ready" });
        expect(getGraphMock).toHaveBeenCalledWith(
          { connectionId: "conn-1", database: "app_db", schema: "public" },
          false,
          expect.any(AbortSignal),
        );

        const createCalls = vscodeMock.createWebviewPanel.mock.calls.length;
        ErdPanel.createOrShow(context, connectionManager as never, {
          connectionId: "conn-1",
          database: "app_db",
          schema: "public",
        });
        expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(
          createCalls,
        );
        expect(panel.reveal).toHaveBeenCalledTimes(1);
      } finally {
        ErdPanel.disposeAll();
      }
    });

    it.each([
      ["missing state", undefined],
      ["missing initial state", { viewport: { x: 0, y: 0, zoom: 1 } }],
      ["blank connection id", { initialState: { connectionId: "  " } }],
      [
        "malformed scope",
        { initialState: { connectionId: "conn-1", database: 42 } },
      ],
    ])("disposes a revived panel with %s", async (_label, state) => {
      const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
      const connectionManager = {
        onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
        getConnection: vi.fn(() => ({ name: "Primary" })),
      };

      try {
        ErdPanel.registerSerializer(
          { extensionUri: {} } as never,
          connectionManager as never,
        );
        const { panel, dispose } = revivedPanel();
        await registeredSerializer().serializer.deserializeWebviewPanel(
          panel,
          state,
        );

        expect(dispose).toHaveBeenCalledOnce();
        expect(panel.webview.html).toBe("");
        await panel.webview.dispatchMessage({ type: "ready" });
        expect(getGraphMock).not.toHaveBeenCalled();
      } finally {
        ErdPanel.disposeAll();
      }
    });

    it("restores a panel whose connection is not connected yet and recovers once it connects", async () => {
      const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
      getGraphMock.mockRejectedValueOnce(new Error("Not connected"));
      const connectionManager = createConnectingManager();
      connectionManager.getConnection.mockReturnValue(undefined);

      try {
        ErdPanel.registerSerializer(
          { extensionUri: {} } as never,
          connectionManager as never,
        );
        const { panel, dispose } = revivedPanel();
        await registeredSerializer().serializer.deserializeWebviewPanel(panel, {
          initialState: { view: "erd", connectionId: " conn-1 " },
        });

        expect(dispose).not.toHaveBeenCalled();
        expect(panel.title).toBe("ERD [conn-1]");

        await panel.webview.dispatchMessage({ type: "ready" });
        expect(getGraphMock).toHaveBeenLastCalledWith(
          { connectionId: "conn-1", database: undefined, schema: undefined },
          false,
          expect.any(AbortSignal),
        );
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
          type: "erdError",
          payload: { error: "Not connected" },
        });

        connectionManager.fireConnect();
        await flushMicrotasks();
        expect(getGraphMock).toHaveBeenCalledTimes(1);

        connectionManager.connect("conn-1");
        await flushMicrotasks();
        expect(getGraphMock).toHaveBeenCalledTimes(2);
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith(
          expect.objectContaining({ type: "erdGraph" }),
        );

        connectionManager.fireConnect();
        await flushMicrotasks();
        expect(getGraphMock).toHaveBeenCalledTimes(2);

        ErdPanel.createOrShow(
          { extensionUri: {} } as never,
          connectionManager as never,
          { connectionId: "conn-1" },
        );
        expect(panel.reveal).toHaveBeenCalledTimes(1);
        await flushMicrotasks();
        expect(getGraphMock).toHaveBeenCalledTimes(2);
      } finally {
        ErdPanel.disposeAll();
      }
      expect(connectionManager.connectListenerCount()).toBe(0);
    });

    it("does not retry a failed load on connect when the connection was already up", async () => {
      const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
      getGraphMock.mockRejectedValueOnce(new Error("Metadata fetch failed"));
      const connectionManager = createConnectingManager();
      connectionManager.connect("conn-1");

      try {
        ErdPanel.createOrShow(
          { extensionUri: {} } as never,
          connectionManager as never,
          { connectionId: "conn-1" },
        );
        const panel = createdPanel();
        if (!panel) {
          throw new Error("Expected ERD panel instance");
        }
        await panel.webview.dispatchMessage({ type: "ready" });
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
          type: "erdError",
          payload: { error: "Metadata fetch failed" },
        });

        connectionManager.fireConnect();
        await flushMicrotasks();
        expect(getGraphMock).toHaveBeenCalledTimes(1);
      } finally {
        ErdPanel.disposeAll();
      }
    });
  });

  it("reloads an errored panel when it is opened again", async () => {
    const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
    getGraphMock.mockRejectedValueOnce(new Error("Not connected"));
    const connectionManager = createConnectingManager();
    const context = { extensionUri: {} } as never;
    const scope = { connectionId: "conn-1", database: "app_db" };

    try {
      ErdPanel.createOrShow(context, connectionManager as never, scope);
      const panel = createdPanel();
      if (!panel) {
        throw new Error("Expected ERD panel instance");
      }
      await panel.webview.dispatchMessage({ type: "ready" });
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "erdError",
        payload: { error: "Not connected" },
      });

      ErdPanel.createOrShow(context, connectionManager as never, scope);
      await flushMicrotasks();
      expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(1);
      expect(panel.reveal).toHaveBeenCalledTimes(1);
      expect(getGraphMock).toHaveBeenCalledTimes(2);
      expect(getGraphMock).toHaveBeenLastCalledWith(
        { connectionId: "conn-1", database: "app_db", schema: undefined },
        false,
        expect.any(AbortSignal),
      );
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith(
        expect.objectContaining({ type: "erdGraph" }),
      );

      ErdPanel.createOrShow(context, connectionManager as never, scope);
      await flushMicrotasks();
      expect(panel.reveal).toHaveBeenCalledTimes(2);
      expect(getGraphMock).toHaveBeenCalledTimes(2);
    } finally {
      ErdPanel.disposeAll();
    }
  });
});

function createConnectingManager() {
  const connectListeners = new Set<() => void>();
  const connected = new Set<string>();
  return {
    onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    onDidConnect: vi.fn((listener: () => void) => {
      connectListeners.add(listener);
      return { dispose: () => connectListeners.delete(listener) };
    }),
    getConnection: vi.fn<(id: string) => { name: string } | undefined>(() => ({
      name: "Primary",
    })),
    isConnecting: vi.fn(() => false),
    isConnected: vi.fn((id: string) => connected.has(id)),
    connect(id: string) {
      connected.add(id);
      this.fireConnect();
    },
    fireConnect() {
      for (const listener of [...connectListeners]) {
        listener();
      }
    },
    connectListenerCount: () => connectListeners.size,
  };
}

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 5; index += 1) {
    await Promise.resolve();
  }
}
