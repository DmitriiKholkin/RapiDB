import { beforeEach, describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { REDIS_ALL_KEYS_TABLE } from "../../src/extension/dbDrivers/redisKeyspace";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { QueryPanelController } from "../../src/extension/panels/queryPanelController";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import { TableReadService } from "../../src/extension/table/tableReadService";
import type { ChunkedExportData } from "../../src/extension/utils/exportService";
import type { TableMutationPreviewPayload } from "../../src/shared/webviewContracts";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

type MockColumn = { name: string; isPrimaryKey: boolean };

const getColumnsMock = vi.hoisted(() =>
  vi.fn(async (): Promise<MockColumn[]> => []),
);
const getPageMock = vi.hoisted(() =>
  vi.fn(async () => ({ rows: [], totalCount: 0, columns: [] })),
);
const clearForConnectionMock = vi.hoisted(() => vi.fn<(id: string) => void>());
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
const prepareInsertRowMock = vi.hoisted(() =>
  vi.fn<() => Promise<unknown | null>>(async () => null),
);
const prepareApplyChangesPlanMock = vi.hoisted(() => vi.fn());
const confirmMutationPreviewMock = vi.hoisted(() =>
  vi.fn<
    (previewToken: string, operationId?: string) => Promise<unknown | null>
  >(async () => null),
);
const pendingPreviewControllerState = vi.hoisted(
  () => new Map<string, string>(),
);
const realPreviewControllerMode = vi.hoisted(() => ({ enabled: false }));
const executePreparedDeletePlanMock = vi.hoisted(() =>
  vi.fn<
    (
      plan: import("../../src/extension/tableDataService").PreparedDeletePlan,
    ) => Promise<void>
  >(),
);
const createApplyChangesPreviewMock = vi.hoisted(() => vi.fn());
const createInsertPreviewMock = vi.hoisted(() => vi.fn());
const createDeleteRowsPreviewMock = vi.hoisted(() => vi.fn());
const createWebviewShellMock = vi.hoisted(() => vi.fn(() => "<html></html>"));
const exportTableDataMock = vi.hoisted(() =>
  vi.fn<
    (options: {
      loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>;
    }) => Promise<void>
  >(),
);
const exportAllMock = vi.hoisted(() =>
  vi.fn<(...args: unknown[]) => AsyncIterable<ChunkedExportData>>(),
);

const vscodeMock = vi.hoisted(() => {
  const serializers = new Map<string, unknown>();
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
        postMessage: vi.fn(async (_message: unknown) => true),
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
        registerWebviewPanelSerializer,
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

vi.mock("vscode", async () => {
  const { MockEventEmitter } = await import("../support/mockVscode");
  return { ...vscodeMock.module, EventEmitter: MockEventEmitter };
});

vi.mock("../../src/extension/utils/exportService", () => ({
  exportTableDataAsCsv: exportTableDataMock,
  exportTableDataAsJson: exportTableDataMock,
  exportQueryResultsAsCsv: vi.fn(),
  exportQueryResultsAsJson: vi.fn(),
}));

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
  createWebviewShell: createWebviewShellMock,
}));

vi.mock("../../src/extension/tableDataService", () => ({
  TableDataService: class {
    getColumns = getColumnsMock;
    getPage = getPageMock;
    exportAll = exportAllMock;
    prepareInsertRow = prepareInsertRowMock;
    prepareDeleteRowsPlan = prepareDeleteRowsPlanMock;
    executePreparedDeletePlan = executePreparedDeletePlanMock;
    clearForConnection = clearForConnectionMock;
  },
  prepareApplyChangesPlan: prepareApplyChangesPlanMock,
}));

vi.mock(
  "../../src/extension/panels/tableMutationPreviewController",
  async (importOriginal) => {
    const actual =
      await importOriginal<
        typeof import("../../src/extension/panels/tableMutationPreviewController")
      >();
    class MockTableMutationPreviewController {
      clear = vi.fn(() => pendingPreviewControllerState.clear());
      confirm = vi.fn((previewToken: string, operationId?: string) => {
        const pendingOperationId =
          pendingPreviewControllerState.get(previewToken);
        if (
          pendingOperationId === undefined ||
          (operationId && pendingOperationId !== operationId)
        ) {
          return Promise.resolve(null);
        }
        pendingPreviewControllerState.delete(previewToken);
        return confirmMutationPreviewMock(previewToken, operationId);
      });
      cancel = vi.fn((previewToken: string, operationId?: string) => {
        const pendingOperationId =
          pendingPreviewControllerState.get(previewToken);
        if (
          pendingOperationId !== undefined &&
          (!operationId || pendingOperationId === operationId)
        ) {
          pendingPreviewControllerState.delete(previewToken);
        }
      });
      createApplyChangesPreview = (...args: unknown[]) =>
        this.rememberPreview(createApplyChangesPreviewMock(...args));
      createInsertPreview = (...args: unknown[]) =>
        this.rememberPreview(createInsertPreviewMock(...args));
      createDeleteRowsPreview = (...args: unknown[]) =>
        this.rememberPreview(createDeleteRowsPreviewMock(...args));

      private rememberPreview<T>(preview: T): T {
        if (preview !== null && typeof preview === "object") {
          const payload = preview as {
            previewToken?: unknown;
            operationId?: unknown;
          };
          if (
            typeof payload.previewToken === "string" &&
            typeof payload.operationId === "string"
          ) {
            pendingPreviewControllerState.set(
              payload.previewToken,
              payload.operationId,
            );
          }
        }
        return preview;
      }
    }
    return {
      // biome-ignore lint/complexity/useArrowFunction: Vitest needs a constructible function because TablePanel calls this mock with new.
      TableMutationPreviewController: vi.fn(function (
        options: ConstructorParameters<
          typeof actual.TableMutationPreviewController
        >[0],
      ) {
        return realPreviewControllerMode.enabled
          ? new actual.TableMutationPreviewController(options)
          : new MockTableMutationPreviewController();
      }),
    };
  },
);

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function lastMutationPreview(
  panel: ReturnType<typeof vscodeMock.createWebviewPanel>,
  operationId: string,
): TableMutationPreviewPayload {
  const message = panel.webview.postMessage.mock.calls
    .map(
      ([value]) =>
        value as { type: string; payload: TableMutationPreviewPayload },
    )
    .filter(
      (value) =>
        value.type === "tableMutationPreview" &&
        value.payload.operationId === operationId,
    )
    .at(-1);
  if (!message) throw new Error(`Expected preview for ${operationId}`);
  return message.payload;
}

function deletePreviewPlan(): import("../../src/extension/tableDataService").PreparedDeletePlan {
  return {
    connectionId: "conn-1",
    database: "db1",
    schema: "public",
    table: "restored_items",
    executionMode: "transaction",
    operations: [
      { sql: "DELETE FROM restored_items WHERE id = $1", params: [1] },
    ],
    previewStatements: ["DELETE FROM restored_items WHERE id = 1"],
    rowIdentities: [{ id: 1 }],
    verificationCriteriaList: [{ id: 1 }],
  };
}

describe("TablePanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    TablePanel.disposeAll();
    vscodeMock.serializers.clear();
    pendingPreviewControllerState.clear();
    realPreviewControllerMode.enabled = false;
    executePreparedDeletePlanMock.mockReset();
    getColumnsMock.mockReset();
    getColumnsMock.mockResolvedValue([]);
    clearForConnectionMock.mockReset();
    getPageMock.mockClear();
    exportAllMock.mockReset();
    prepareInsertRowMock.mockReset();
    prepareInsertRowMock.mockResolvedValue(null);
    prepareApplyChangesPlanMock.mockReset();
    confirmMutationPreviewMock.mockReset();
    confirmMutationPreviewMock.mockResolvedValue(null);
    createApplyChangesPreviewMock.mockReset();
    createInsertPreviewMock.mockReset();
    prepareDeleteRowsPlanMock.mockReset();
    prepareDeleteRowsPlanMock.mockResolvedValue(null);
    createApplyChangesPreviewMock.mockReturnValue({
      operationId: "op-1",
      previewToken: "apply-preview-token",
      kind: "applyChanges",
      title: "Apply changes to users",
      sql: "UPDATE users SET name = 'Ada' WHERE id = 1;",
      statementCount: 1,
    });
    createInsertPreviewMock.mockReturnValue({
      operationId: "op-1",
      previewToken: "insert-preview-token",
      kind: "insertRow",
      title: "Insert row into users",
      sql: "INSERT INTO users (id) VALUES (1);",
      statementCount: 1,
    });
    createDeleteRowsPreviewMock.mockReset();
    createDeleteRowsPreviewMock.mockReturnValue({
      operationId: "op-1",
      previewToken: "preview-token",
      kind: "deleteRows",
      title: "Apply changes to users",
      sql: "DELETE FROM users WHERE id = 1;",
      statementCount: 1,
    });
    createWebviewShellMock.mockClear();
  });

  async function openSchemaRefreshPath() {
    let refreshListener: ((connectionId?: string) => void) | undefined;
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main", type: "pg" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(
        (listener: (connectionId?: string) => void) => {
          refreshListener = listener;
          return { dispose: vi.fn() };
        },
      ),
      refreshSchemaCache: vi.fn((connectionId: string) =>
        refreshListener?.(connectionId),
      ),
      getDefaultPageSize: vi.fn(() => 25),
      getQueryRowLimit: vi.fn(() => 100),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => ({
        query: vi.fn(async () => ({ columns: [], rows: [], rowCount: 0 })),
      })),
      addToHistory: vi.fn(async () => undefined),
      getSkipTableMutationPreview: vi.fn(() => false),
    };
    getColumnsMock.mockResolvedValue([
      { name: "id", isPrimaryKey: true },
      { name: "name", isPrimaryKey: false },
    ]);
    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "public",
      "users",
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected table panel");
    await panel.webview.dispatchMessage({ type: "ready" });
    const controller = new QueryPanelController(connectionManager as never, {
      getActiveConnectionId: () => "conn-1",
      getInitialConnectionId: () => "conn-1",
      getLastQueryResult: () => null,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    });
    const refresh = async (
      columns: MockColumn[] = [
        { name: "id", isPrimaryKey: true },
        { name: "renamed", isPrimaryKey: false },
      ],
    ) => {
      getColumnsMock.mockResolvedValue(columns);
      await controller.handleMessage({
        type: "executeQuery",
        payload: {
          queryText: "ALTER TABLE users RENAME COLUMN name TO renamed",
        },
      });
      await vi.waitFor(() =>
        expect(panel.webview.postMessage).toHaveBeenCalledWith({
          type: "tableInit",
          payload: expect.objectContaining({
            intent: "metadataRefresh",
            columns,
          }),
        }),
      );
    };
    return { panel, refresh, connectionManager };
  }

  it("restores serialized table panels from their persisted initial state", async () => {
    const { connectionManager } = await openSchemaRefreshPath();
    TablePanel.registerSerializer(
      { extensionUri: {} } as never,
      connectionManager as never,
    );
    const serializer = vscodeMock.serializers.get("rapidb.tablePanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const panel = vscodeMock.createWebviewPanel();

    await serializer.deserializeWebviewPanel(panel, {
      initialState: {
        view: "table",
        connectionId: "conn-2",
        database: "db2",
        schema: "public",
        table: "restored_items",
        isView: false,
        objectKind: "table",
      },
    });

    expect(panel.webview.html).toBe("<html></html>");
    expect(panel.title).toContain("restored_items");
    TablePanel.disposeAll();
  });

  function lazyRestoreRegistryFixture() {
    const refreshListeners = new Set<(id?: string) => void>();
    const invalidationListeners = new Set<(id: string) => void>();
    const disconnectListeners = new Set<(id: string) => void>();
    const context = { extensionUri: {} };
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main", type: "pg" })),
      getDefaultPageSize: vi.fn(() => 25),
      isConnected: vi.fn(() => true),
      onDidRefreshSchemas: (listener: (id?: string) => void) => {
        refreshListeners.add(listener);
        return { dispose: () => refreshListeners.delete(listener) };
      },
      onDidInvalidateConnectionMetadata: (listener: (id: string) => void) => {
        invalidationListeners.add(listener);
        return { dispose: () => invalidationListeners.delete(listener) };
      },
      onDidDisconnect: (listener: (id: string) => void) => {
        disconnectListeners.add(listener);
        return { dispose: () => disconnectListeners.delete(listener) };
      },
    };
    const initialState = {
      view: "table",
      connectionId: "conn-1",
      database: "db1",
      schema: "public",
      table: "users",
      isView: false,
      objectKind: "table",
    };
    TablePanel.registerSerializer(context as never, connectionManager as never);
    const serializer = vscodeMock.serializers.get("rapidb.tablePanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const open = () => {
      TablePanel.createOrShow(
        context as never,
        connectionManager as never,
        "conn-1",
        "db1",
        "public",
        "users",
      );
    };
    const restore = async (name: string) => {
      // This is the opaque webview-owned state VS Code passes to the actual
      // serializer. Distinct saved drafts must not be merged or discarded.
      const state = {
        initialState: { ...initialState },
        tableDraft: {
          tableKey: JSON.stringify(["conn-1", "db1", "public", "users"]),
          restoreState: { entries: [] },
          newRows: [{ name: { value: name } }],
        },
        tableReadEpoch: 17,
      };
      const snapshot = structuredClone(state);
      const panel = vscodeMock.createWebviewPanel();
      const onDispose = vi.fn();
      panel.onDidDispose(onDispose);
      await serializer.deserializeWebviewPanel(panel, state);
      return { panel, state, snapshot, onDispose };
    };
    return {
      open,
      restore,
      refresh: () => {
        for (const listener of refreshListeners) listener("conn-1");
      },
      invalidate: () => {
        for (const listener of invalidationListeners) listener("conn-1");
      },
      disconnect: () => {
        for (const listener of disconnectListeners) listener("conn-1");
      },
      refreshListeners,
      invalidationListeners,
      disconnectListeners,
    };
  }

  it("preserves distinct late-restored drafts without replacing Explorer's reuse owner", async () => {
    const fixture = lazyRestoreRegistryFixture();
    fixture.open();
    const fresh = createdPanel();
    const onFreshDispose = vi.fn();
    fresh.onDidDispose(onFreshDispose);
    await fresh.webview.dispatchMessage({ type: "ready" });
    const first = await fixture.restore("saved draft A");
    const second = await fixture.restore("saved draft B");

    // Deserialization neither performs reads nor sends a reset to a hidden tab.
    expect(getColumnsMock).toHaveBeenCalledOnce();
    for (const restored of [first, second]) {
      expect(restored.panel.webview.html).toBe("<html></html>");
      expect(restored.panel.title).toContain("public.users");
      expect(restored.panel.webview.postMessage).not.toHaveBeenCalled();
      expect(restored.onDispose).not.toHaveBeenCalled();
      expect(restored.state).toEqual(restored.snapshot);
      await restored.panel.webview.dispatchMessage({ type: "ready" });
      expect(
        restored.panel.webview.postMessage,
      ).toHaveBeenCalledExactlyOnceWith({
        type: "tableInit",
        payload: expect.objectContaining({ intent: "initialize" }),
      });
    }
    fixture.open();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(3);
    expect(fresh.reveal).toHaveBeenCalledExactlyOnceWith(1);
    expect(first.panel.reveal).not.toHaveBeenCalled();
    expect(second.panel.reveal).not.toHaveBeenCalled();
    expect(onFreshDispose).not.toHaveBeenCalled();

    // Metadata and session events must still reach every registered instance,
    // including those not selected by the single-table reuse index.
    fixture.refresh();
    await vi.waitFor(() => {
      for (const panel of [fresh, first.panel, second.panel]) {
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
          type: "tableInit",
          payload: expect.objectContaining({ intent: "metadataRefresh" }),
        });
      }
    });
    fixture.invalidate();
    for (const panel of [fresh, first.panel, second.panel]) {
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableConnectionInvalidated",
        payload: {},
      });
    }
    expect(first.state).toEqual(first.snapshot);
    expect(second.state).toEqual(second.snapshot);
    expect(first.state.tableDraft).not.toEqual(second.state.tableDraft);

    // Promotion is FIFO, not whichever saved panel deserialized most recently.
    fresh.dispose();
    fixture.open();
    expect(first.panel.reveal).toHaveBeenCalledExactlyOnceWith(1);
    expect(second.panel.reveal).not.toHaveBeenCalled();
    first.panel.dispose();
    fixture.open();
    expect(second.panel.reveal).toHaveBeenCalledExactlyOnceWith(1);
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(3);
    TablePanel.disposeAll();
    expect(second.onDispose).toHaveBeenCalledOnce();
  });

  it.each([
    "fresh",
    "restored",
  ] as const)("reuses the surviving same-table panel when closing %s first", async (closeFirst) => {
    const fixture = lazyRestoreRegistryFixture();
    fixture.open();
    const fresh = createdPanel();
    const restored = await fixture.restore("late draft");
    const closing = closeFirst === "fresh" ? fresh : restored.panel;
    const surviving = closeFirst === "fresh" ? restored.panel : fresh;
    const onSurvivorDispose = vi.fn();
    surviving.onDidDispose(onSurvivorDispose);
    closing.dispose();
    fixture.open();
    fixture.open();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);
    expect(surviving.reveal).toHaveBeenCalledTimes(2);
    expect(closing.reveal).not.toHaveBeenCalled();
    expect(onSurvivorDispose).not.toHaveBeenCalled();

    surviving.dispose();
    fixture.open();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(3);
    TablePanel.disposeAll();
    expect(onSurvivorDispose).toHaveBeenCalledOnce();
    expect(fixture.refreshListeners.size).toBe(0);
    expect(fixture.invalidationListeners.size).toBe(0);
    expect(fixture.disconnectListeners.size).toBe(0);
  });

  it.each([
    "create-first",
    "restore-first",
  ] as const)("disposeAll closes every same-table instance after %s registration", async (order) => {
    const fixture = lazyRestoreRegistryFixture();
    let first: ReturnType<typeof vscodeMock.createWebviewPanel>;
    if (order === "create-first") {
      fixture.open();
      first = createdPanel();
    } else {
      first = (await fixture.restore("first saved draft")).panel;
    }
    const onFirstDispose = vi.fn();
    first.onDidDispose(onFirstDispose);
    const second = await fixture.restore("second saved draft");
    const third = await fixture.restore("third saved draft");
    fixture.open();
    expect(first.reveal).toHaveBeenCalledExactlyOnceWith(1);
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(3);
    TablePanel.disposeAll();
    TablePanel.disposeAll();
    expect(onFirstDispose).toHaveBeenCalledOnce();
    expect(second.onDispose).toHaveBeenCalledOnce();
    expect(third.onDispose).toHaveBeenCalledOnce();
    expect(fixture.refreshListeners.size).toBe(0);
    expect(fixture.invalidationListeners.size).toBe(0);
    expect(fixture.disconnectListeners.size).toBe(0);
    fixture.open();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(4);
    TablePanel.disposeAll();
  });

  it("disconnect closes indexed and late-restored same-table panels", async () => {
    const fixture = lazyRestoreRegistryFixture();
    fixture.open();
    const fresh = createdPanel();
    const onFreshDispose = vi.fn();
    fresh.onDidDispose(onFreshDispose);
    const restored = await fixture.restore("saved draft");
    fixture.disconnect();
    TablePanel.disposeAll();
    expect(onFreshDispose).toHaveBeenCalledOnce();
    expect(restored.onDispose).toHaveBeenCalledOnce();
    fixture.open();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(3);
    TablePanel.disposeAll();
  });

  async function restoreDisconnectedPanel() {
    const connected = new Set<string>();
    const connectListeners = new Set<() => void>();
    const disconnectListeners = new Set<(id: string) => void>();
    let reconnectBlock: string | undefined;
    let readOnly = true;
    const columns = [{ name: "id", isPrimaryKey: true }];
    const driver = { describeColumns: vi.fn(async () => columns) };
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Restored", type: "pg", readOnly })),
      getDefaultPageSize: vi.fn(() => 25),
      isConnected: vi.fn((id: string) => connected.has(id)),
      getDriver: vi.fn((id: string) =>
        connected.has(id) ? driver : undefined,
      ),
      getAutomaticReconnectBlockReason: vi.fn(() => reconnectBlock),
      connectTo: vi.fn(async (id: string, intent: string = "automatic") => {
        if (intent === "automatic" && reconnectBlock) {
          throw new Error(reconnectBlock);
        }
        if (intent === "explicit" && id === "conn-1")
          reconnectBlock = undefined;
        connected.add(id);
        for (const listener of connectListeners) listener();
      }),
      onDidConnect: vi.fn((listener: () => void) => {
        connectListeners.add(listener);
        return { dispose: vi.fn(() => connectListeners.delete(listener)) };
      }),
      onDidDisconnect: vi.fn((listener: (id: string) => void) => {
        disconnectListeners.add(listener);
        return { dispose: vi.fn(() => disconnectListeners.delete(listener)) };
      }),
    };
    // Exercise the real disconnected-driver guard, not a columns mock that
    // succeeds without a connection (which hid the serializer regression).
    const reads = new TableReadService(connectionManager as never);
    clearForConnectionMock.mockImplementation((id) =>
      reads.clearForConnection(id),
    );
    getColumnsMock.mockImplementation(async () => {
      await reads.getColumns("conn-1", "db1", "public", "restored_items");
      return columns;
    });
    TablePanel.registerSerializer(
      { extensionUri: {} } as never,
      connectionManager as never,
    );
    const serializer = vscodeMock.serializers.get("rapidb.tablePanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const panel = vscodeMock.createWebviewPanel();
    await serializer.deserializeWebviewPanel(panel, {
      initialState: {
        connectionId: "conn-1",
        database: "db1",
        schema: "public",
        table: "restored_items",
      },
    });
    return {
      panel,
      connectionManager,
      driver,
      columns,
      connected,
      connectListeners,
      setReconnectBlock: (reason: string) => {
        reconnectBlock = reason;
      },
      setReadOnly: (value: boolean) => {
        readOnly = value;
      },
      disconnect: () => {
        connected.delete("conn-1");
        for (const listener of disconnectListeners) listener("conn-1");
      },
    };
  }

  it("connects a restored disconnected panel on actual ready before reading metadata", async () => {
    const { panel, connectionManager, driver, columns } =
      await restoreDisconnectedPanel();
    expect(connectionManager.connectTo).not.toHaveBeenCalled();
    expect(getColumnsMock).not.toHaveBeenCalled();

    await panel.webview.dispatchMessage({ type: "ready" });

    expect(connectionManager.connectTo).toHaveBeenCalledExactlyOnceWith(
      "conn-1",
      "automatic",
    );
    expect(driver.describeColumns).toHaveBeenCalledExactlyOnceWith(
      "db1",
      "public",
      "restored_items",
    );
    expect(panel.webview.postMessage).toHaveBeenCalledExactlyOnceWith({
      type: "tableInit",
      payload: {
        intent: "initialize",
        columns,
        primaryKeyColumns: ["id"],
        isView: false,
        connectionReadOnly: true,
      },
    });
    // Our own connect event must not cause a second initialization.
    await connectionManager.connectTo("conn-2", "explicit");
    expect(getColumnsMock).toHaveBeenCalledOnce();
  });

  it("preserves the automatic reconnect block and retries initialization after explicit Connect", async () => {
    const { panel, connectionManager, setReconnectBlock, setReadOnly } =
      await restoreDisconnectedPanel();
    const reason = "Automatic reconnect blocked. Use Connect explicitly.";
    setReconnectBlock(reason);
    await panel.webview.dispatchMessage({ type: "ready" });
    expect(connectionManager.connectTo).not.toHaveBeenCalled();
    expect(getColumnsMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "tableError",
      payload: { error: reason },
    });

    // onDidConnect is global: a different connection must not retry this one.
    await connectionManager.connectTo("conn-2", "explicit");
    expect(getColumnsMock).not.toHaveBeenCalled();
    setReadOnly(false);
    await connectionManager.connectTo("conn-1", "explicit");
    await vi.waitFor(() =>
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({
          intent: "initialize",
          connectionReadOnly: false,
        }),
      }),
    );
    expect(getColumnsMock).toHaveBeenCalledOnce();
    expect(connectionManager.connectTo.mock.calls).toEqual([
      ["conn-2", "explicit"],
      ["conn-1", "explicit"],
    ]);
  });

  it("retries a restored panel after its automatic connection attempt failed", async () => {
    const { panel, connectionManager } = await restoreDisconnectedPanel();
    connectionManager.connectTo.mockRejectedValueOnce(
      new Error("Connection refused"),
    );
    await panel.webview.dispatchMessage({ type: "ready" });
    expect(getColumnsMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "tableError",
      payload: { error: "Connection refused" },
    });
    await connectionManager.connectTo("conn-1", "explicit");
    await vi.waitFor(() =>
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({ intent: "initialize" }),
      }),
    );
    expect(getColumnsMock).toHaveBeenCalledOnce();
  });

  it("does not initialize a restored panel on Connect until its webview sends ready", async () => {
    const { panel, connectionManager } = await restoreDisconnectedPanel();
    await connectionManager.connectTo("conn-1", "explicit");
    expect(getColumnsMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).not.toHaveBeenCalled();
    await panel.webview.dispatchMessage({ type: "ready" });
    expect(getColumnsMock).toHaveBeenCalledOnce();
    expect(connectionManager.connectTo).toHaveBeenCalledOnce();
  });

  it("does not reload an initialized table when a later Connect event fires", async () => {
    const { panel, connectionManager } = await restoreDisconnectedPanel();
    await panel.webview.dispatchMessage({ type: "ready" });
    await connectionManager.connectTo("conn-1", "explicit");
    expect(getColumnsMock).toHaveBeenCalledOnce();
    expect(panel.webview.postMessage).toHaveBeenCalledOnce();
  });

  it("deduplicates ready and Connect initialization while connecting", async () => {
    const { panel, connectionManager, connected, connectListeners } =
      await restoreDisconnectedPanel();
    const pending = deferred<void>();
    connectionManager.connectTo.mockImplementationOnce(() => pending.promise);
    const first = panel.webview.dispatchMessage({ type: "ready" });
    const second = panel.webview.dispatchMessage({ type: "ready" });
    expect(connectionManager.connectTo).toHaveBeenCalledOnce();
    connected.add("conn-1");
    for (const listener of connectListeners) listener();
    pending.resolve();
    await Promise.all([first, second]);
    expect(getColumnsMock).toHaveBeenCalledOnce();
    expect(panel.webview.postMessage).toHaveBeenCalledOnce();
  });

  it("does not lose Connect while the initial error is still being delivered", async () => {
    const { panel, connectionManager, setReconnectBlock } =
      await restoreDisconnectedPanel();
    setReconnectBlock("Use explicit Connect");
    const delivery = deferred<boolean>();
    panel.webview.postMessage.mockImplementationOnce(() => delivery.promise);
    const ready = panel.webview.dispatchMessage({ type: "ready" });
    await connectionManager.connectTo("conn-1", "explicit");
    expect(getColumnsMock).not.toHaveBeenCalled();
    delivery.resolve(true);
    await ready;
    await vi.waitFor(() =>
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({ intent: "initialize" }),
      }),
    );
    expect(getColumnsMock).toHaveBeenCalledOnce();
  });

  it("rejects a cancelled connect that resolves without establishing a driver", async () => {
    const { panel, connectionManager } = await restoreDisconnectedPanel();
    connectionManager.connectTo.mockResolvedValueOnce();
    await panel.webview.dispatchMessage({ type: "ready" });
    expect(getColumnsMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledExactlyOnceWith({
      type: "tableError",
      payload: { error: "[RapiDB] Not connected: conn-1" },
    });
  });

  it("does not read metadata or revive the panel after manual disconnect during connect", async () => {
    const { panel, connectionManager, disconnect, connectListeners } =
      await restoreDisconnectedPanel();
    const pending = deferred<void>();
    connectionManager.connectTo.mockImplementationOnce(() => pending.promise);
    const ready = panel.webview.dispatchMessage({ type: "ready" });
    disconnect();
    expect(connectListeners.size).toBe(0);
    // Manager cancellation may resolve successfully without a live driver.
    pending.resolve();
    await ready;
    await connectionManager.connectTo("conn-1", "explicit");
    expect(getColumnsMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).not.toHaveBeenCalled();
  });

  it("does not publish late initialization metadata after dispose", async () => {
    const { panel, connectionManager } = await restoreDisconnectedPanel();
    const pending = deferred<MockColumn[]>();
    getColumnsMock.mockImplementationOnce(() => pending.promise);
    const ready = panel.webview.dispatchMessage({ type: "ready" });
    await vi.waitFor(() => expect(getColumnsMock).toHaveBeenCalledOnce());
    panel.dispose();
    pending.resolve([{ name: "late_id", isPrimaryKey: true }]);
    await ready;
    await connectionManager.connectTo("conn-1", "explicit");
    expect(getColumnsMock).toHaveBeenCalledOnce();
    expect(panel.webview.postMessage).not.toHaveBeenCalled();
  });

  async function restoreWithRealConnectionManager({
    realPreviews = false,
    host = "localhost",
  } = {}) {
    realPreviewControllerMode.enabled = realPreviews;
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Restored",
        type: "pg",
        host,
        database: "db1",
        username: "postgres",
        readOnly: !realPreviews,
      },
    ]);
    const context = { ...createExtensionContextStub(), extensionUri: {} };
    const manager = new ConnectionManager(context as never, store);
    let disconnectDelay: Promise<void> | undefined;
    let connectDelay: Promise<void> | undefined;
    let columns = [{ name: "id", isPrimaryKey: true }];
    const driverFactory = vi.fn(() => {
      let connected = false;
      const driverColumns = columns;
      const query = vi.fn(
        async (
          _sql: string,
          _params?: unknown[],
          _options?: { database?: string },
        ) => ({ columns: [], rows: [], rowCount: 1 }),
      );
      return {
        targetHost: manager.getConnection("conn-1")?.host,
        connect: vi.fn(async () => {
          await connectDelay;
          connected = true;
        }),
        disconnect: vi.fn(async () => {
          connected = false;
          await disconnectDelay;
        }),
        isConnected: vi.fn(() => connected),
        describeColumns: vi.fn(async () => driverColumns),
        query,
        qualifiedTableName: vi.fn(() => "restored_items"),
        quoteIdentifier: vi.fn((name: string) => `"${name}"`),
        buildInsertValueExpr: vi.fn(
          (_column: unknown, index: number) => `$${index}`,
        ),
        runTransaction: vi.fn(
          async (operations: Array<{ sql: string; params?: unknown[] }>) => {
            for (const operation of operations)
              await query(operation.sql, operation.params);
          },
        ),
      };
    });
    // Only the actual database transport is fake. Epochs, cancellation, driver
    // replacement, bounded cleanup and all manager events use production code.
    vi.spyOn(
      manager as unknown as { createDriver(): IDBDriver },
      "createDriver",
    ).mockImplementation(() => driverFactory() as unknown as IDBDriver);
    await manager.connectTo("conn-1", "explicit");
    const driver = driverFactory.mock.results[0].value;
    const reads = new TableReadService(manager);
    if (realPreviews) {
      const { TableMutationService } = await import(
        "../../src/extension/table/tableMutationService"
      );
      const mutations = new TableMutationService(manager, reads);
      executePreparedDeletePlanMock.mockImplementation((plan) =>
        mutations.executePreparedDeletePlan(plan),
      );
    }
    clearForConnectionMock.mockImplementation((id) =>
      reads.clearForConnection(id),
    );
    getColumnsMock.mockImplementation(async () => {
      const described = await reads.getColumns(
        "conn-1",
        "db1",
        "public",
        "restored_items",
      );
      return described.map((column) => ({
        name: column.name,
        isPrimaryKey: column.isPrimaryKey === true,
      }));
    });
    TablePanel.registerSerializer(context as never, manager);
    const serializer = vscodeMock.serializers.get("rapidb.tablePanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const panel = vscodeMock.createWebviewPanel();
    await serializer.deserializeWebviewPanel(panel, {
      initialState: {
        connectionId: "conn-1",
        database: "db1",
        schema: "public",
        table: "restored_items",
      },
    });
    const onDispose = vi.fn();
    panel.onDidDispose(onDispose);
    return {
      manager,
      panel,
      driver,
      driverFactory,
      onDispose,
      setNextColumns: (next: MockColumn[]) => {
        columns = next;
      },
      setDisconnectDelay: (delay?: Promise<void>) => {
        disconnectDelay = delay;
      },
      setConnectDelay: (delay?: Promise<void>) => {
        connectDelay = delay;
      },
    };
  }

  it("does not let restored ready reconnect over a manual Disconnect already in progress", async () => {
    const {
      manager,
      panel,
      driver,
      driverFactory,
      onDispose,
      setDisconnectDelay,
    } = await restoreWithRealConnectionManager();
    const cleanup = deferred<void>();
    setDisconnectDelay(cleanup.promise);
    const onConnect = vi.fn();
    const onDisconnect = vi.fn();
    manager.onDidConnect(onConnect);
    manager.onDidDisconnect(onDisconnect);
    const disconnect = manager.disconnectFrom("conn-1");
    try {
      await vi.waitFor(() => expect(driver.disconnect).toHaveBeenCalledOnce());
      expect(manager.isConnected("conn-1")).toBe(false);
      expect(onDisconnect).not.toHaveBeenCalled();
      expect(onDispose).not.toHaveBeenCalled();
      // This is the inverse of ready -> Disconnect: async driver cleanup has
      // already begun, but its final panel-disposal event has not happened yet.
      await panel.webview.dispatchMessage({ type: "ready" });
      expect(driverFactory).toHaveBeenCalledOnce();
      expect(getColumnsMock).not.toHaveBeenCalled();
      expect(manager.isConnecting("conn-1")).toBe(false);
      expect(panel.webview.postMessage).toHaveBeenCalledExactlyOnceWith({
        type: "tableError",
        payload: { error: expect.stringContaining("Disconnect in progress") },
      });
      cleanup.resolve();
      await disconnect;
      expect(onDispose).toHaveBeenCalledOnce();
      expect(onDisconnect).toHaveBeenCalledExactlyOnceWith("conn-1");
      expect(onConnect).not.toHaveBeenCalled();
      expect(manager.getDriver("conn-1")).toBeUndefined();
      expect(driverFactory).toHaveBeenCalledOnce();
    } finally {
      cleanup.resolve();
      setDisconnectDelay();
      await disconnect;
      await manager.dispose();
    }
  });

  it("keeps the restored panel alive when automatic Connect replaces a retained disconnected driver", async () => {
    const {
      manager,
      panel,
      driver,
      driverFactory,
      onDispose,
      setDisconnectDelay,
    } = await restoreWithRealConnectionManager();
    // Simulate transport loss: the registered driver remains in driverMap.
    await driver.disconnect();
    expect(manager.isConnected("conn-1")).toBe(false);
    expect(manager.getDriver("conn-1")).toBe(driver);
    const cleanup = deferred<void>();
    setDisconnectDelay(cleanup.promise);
    const onDisconnect = vi.fn();
    manager.onDidDisconnect(onDisconnect);
    const ready = panel.webview.dispatchMessage({ type: "ready" });
    try {
      await vi.waitFor(() =>
        expect(driver.disconnect).toHaveBeenCalledTimes(2),
      );
      expect(onDispose).not.toHaveBeenCalled();
      cleanup.resolve();
      await ready;
      expect(manager.isConnected("conn-1")).toBe(true);
      expect(driverFactory).toHaveBeenCalledTimes(2);
      expect(onDisconnect).not.toHaveBeenCalled();
      expect(onDispose).not.toHaveBeenCalled();
      expect(getColumnsMock).toHaveBeenCalledOnce();
      expect(panel.webview.postMessage).toHaveBeenCalledExactlyOnceWith({
        type: "tableInit",
        payload: expect.objectContaining({
          intent: "initialize",
          connectionReadOnly: true,
        }),
      });
      // Suppressing internal replacement must not suppress real Disconnect.
      setDisconnectDelay();
      await manager.disconnectFrom("conn-1");
      expect(onDisconnect).toHaveBeenCalledExactlyOnceWith("conn-1");
      expect(onDispose).toHaveBeenCalledOnce();
    } finally {
      cleanup.resolve();
      setDisconnectDelay();
      await ready;
      await manager.dispose();
    }
  });

  it("still closes the restored panel for manual Disconnect during stale-driver replacement", async () => {
    const {
      manager,
      panel,
      driver,
      driverFactory,
      onDispose,
      setDisconnectDelay,
    } = await restoreWithRealConnectionManager();
    await driver.disconnect();
    const cleanup = deferred<void>();
    setDisconnectDelay(cleanup.promise);
    const onDisconnect = vi.fn();
    const onConnect = vi.fn();
    manager.onDidDisconnect(onDisconnect);
    manager.onDidConnect(onConnect);
    const ready = panel.webview.dispatchMessage({ type: "ready" });
    let disconnect: Promise<void> | undefined;
    try {
      await vi.waitFor(() =>
        expect(driver.disconnect).toHaveBeenCalledTimes(2),
      );
      disconnect = manager.disconnectFrom("conn-1");
      cleanup.resolve();
      await Promise.all([ready, disconnect]);
      expect(onDisconnect).toHaveBeenCalledExactlyOnceWith("conn-1");
      expect(onDispose).toHaveBeenCalledOnce();
      expect(onConnect).not.toHaveBeenCalled();
      expect(manager.getDriver("conn-1")).toBeUndefined();
      expect(driverFactory).toHaveBeenCalledOnce();
      expect(getColumnsMock).not.toHaveBeenCalled();
      expect(panel.webview.postMessage).not.toHaveBeenCalledWith({
        type: "tableInit",
        payload: expect.anything(),
      });
    } finally {
      cleanup.resolve();
      setDisconnectDelay();
      await Promise.all([ready, disconnect]);
      await manager.dispose();
    }
  });

  it.each([
    "cached",
    "in-flight refresh",
  ] as const)("reloads an initialized TablePanel's %s metadata after retained-driver replacement", async (mode) => {
    const { manager, panel, driver, onDispose, setNextColumns } =
      await restoreWithRealConnectionManager();
    const oldRefresh = deferred<MockColumn[]>();
    let oldMetadataRequest: Promise<MockColumn[]> | undefined;
    const newColumns = [{ name: "new_id", isPrimaryKey: true }];
    await panel.webview.dispatchMessage({ type: "ready" });
    try {
      if (mode === "in-flight refresh") {
        driver.describeColumns.mockImplementationOnce(() => oldRefresh.promise);
        manager.refreshSchemaCache("conn-1");
        await vi.waitFor(() =>
          expect(driver.describeColumns).toHaveBeenCalledTimes(2),
        );
        oldMetadataRequest = getColumnsMock.mock.results.at(-1)?.value;
      }
      await driver.disconnect();
      setNextColumns(newColumns);
      await manager.connectTo("conn-1");
      await vi.waitFor(() =>
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
          type: "tableInit",
          payload: expect.objectContaining({
            intent: "connectionRefresh",
            columns: newColumns,
          }),
        }),
      );
      expect(onDispose).not.toHaveBeenCalled();
      // Old metadata settles AFTER the new session's metadata was published.
      oldRefresh.resolve([{ name: "obsolete_id", isPrimaryKey: true }]);
      await oldMetadataRequest;
      await Promise.resolve();
      expect(panel.webview.postMessage).toHaveBeenCalledTimes(4);
      expect(panel.webview.postMessage).toHaveBeenNthCalledWith(2, {
        type: "tableConnectionInvalidated",
        payload: {},
      });
      expect(panel.webview.postMessage).toHaveBeenNthCalledWith(3, {
        type: "tableConnectionInvalidated",
        payload: {},
      });
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({
          intent: "connectionRefresh",
          columns: newColumns,
        }),
      });
    } finally {
      oldRefresh.resolve([]);
      await manager.dispose();
    }
  });

  it.each([
    "success",
    "error",
  ] as const)("initializes the new session before a lost initialization settles with %s", async (outcome) => {
    const { manager, panel, driver, setNextColumns } =
      await restoreWithRealConnectionManager();
    const lostMetadata = deferred<MockColumn[]>();
    const laterMetadata = deferred<MockColumn[]>();
    driver.describeColumns.mockImplementationOnce(() => lostMetadata.promise);
    const oldReady = panel.webview.dispatchMessage({ type: "ready" });
    const currentColumns = [{ name: "current_id", isPrimaryKey: true }];
    try {
      await vi.waitFor(() =>
        expect(driver.describeColumns).toHaveBeenCalledOnce(),
      );
      await driver.disconnect();
      setNextColumns(currentColumns);
      await manager.connectTo("conn-1");
      // The old describeColumns is STILL unresolved. Connect alone must start
      // the new initialization, without needing another ready message.
      await vi.waitFor(() =>
        expect(panel.webview.postMessage).toHaveBeenCalledExactlyOnceWith({
          type: "tableInit",
          payload: expect.objectContaining({
            intent: "initialize",
            columns: currentColumns,
          }),
        }),
      );
      expect(getColumnsMock).toHaveBeenCalledTimes(2);

      getColumnsMock.mockImplementationOnce(() => laterMetadata.promise);
      const currentReady = panel.webview.dispatchMessage({ type: "ready" });
      await vi.waitFor(() => expect(getColumnsMock).toHaveBeenCalledTimes(3));
      if (outcome === "success")
        lostMetadata.resolve([{ name: "lost_id", isPrimaryKey: true }]);
      else lostMetadata.reject(new Error("Lost session error"));
      await oldReady;
      // The late old finally must not clear the new, still-pending promise.
      const duplicateReady = panel.webview.dispatchMessage({ type: "ready" });
      expect(getColumnsMock).toHaveBeenCalledTimes(3);
      expect(panel.webview.postMessage).toHaveBeenCalledOnce();
      laterMetadata.resolve(currentColumns);
      await Promise.all([currentReady, duplicateReady]);
      expect(panel.webview.postMessage).toHaveBeenCalledTimes(2);
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({ columns: currentColumns }),
      });
    } finally {
      lostMetadata.resolve([]);
      laterMetadata.resolve(currentColumns);
      await oldReady;
      await manager.dispose();
    }
  });

  it.each([
    "data",
    "error",
  ] as const)("suppresses old-session page %s after a same-columns reconnect and publishes the current read", async (outcome) => {
    const { manager, panel, driver } = await restoreWithRealConnectionManager();
    await panel.webview.dispatchMessage({ type: "ready" });
    const oldPage = deferred<{
      rows: never[];
      totalCount: number;
      columns: never[];
    }>();
    getPageMock.mockImplementationOnce(() => oldPage.promise);
    const fetching = panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: { fetchId: 11, page: 1 },
    });
    try {
      await vi.waitFor(() => expect(getPageMock).toHaveBeenCalledOnce());
      await driver.disconnect();
      await manager.connectTo("conn-1");
      await vi.waitFor(() =>
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
          type: "tableInit",
          payload: expect.objectContaining({
            intent: "connectionRefresh",
            columns: [{ name: "id", isPrimaryKey: true }],
          }),
        }),
      );
      getPageMock.mockResolvedValueOnce({
        rows: [{ id: 2 }] as never,
        totalCount: 1,
        columns: [],
      });
      await panel.webview.dispatchMessage({
        type: "fetchPage",
        payload: { fetchId: 12, page: 1 },
      });
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableData",
        payload: expect.objectContaining({ fetchId: 12, rows: [{ id: 2 }] }),
      });
      const postCount = panel.webview.postMessage.mock.calls.length;
      if (outcome === "data")
        oldPage.resolve({
          rows: [{ id: 91 }] as never,
          totalCount: 1,
          columns: [],
        });
      else oldPage.reject(new Error("Old connection read failed"));
      await fetching;
      expect(panel.webview.postMessage).toHaveBeenCalledTimes(postCount);
      expect(getPageMock).toHaveBeenCalledTimes(2);
    } finally {
      oldPage.resolve({ rows: [], totalCount: 0, columns: [] });
      await fetching;
      await manager.dispose();
    }
  });

  it("does not let old page cleanup delete the new session's same-key pending read", async () => {
    const { manager, panel, driver } = await restoreWithRealConnectionManager();
    await panel.webview.dispatchMessage({ type: "ready" });
    const oldPage = deferred<Awaited<ReturnType<typeof getPageMock>>>();
    const newPage = deferred<Awaited<ReturnType<typeof getPageMock>>>();
    getPageMock.mockImplementationOnce(() => oldPage.promise);
    const oldRead = panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: { fetchId: 11, page: 1 },
    });
    try {
      await vi.waitFor(() => expect(getPageMock).toHaveBeenCalledOnce());
      await driver.disconnect();
      await manager.connectTo("conn-1");
      await vi.waitFor(() =>
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
          type: "tableInit",
          payload: expect.objectContaining({ intent: "connectionRefresh" }),
        }),
      );
      getPageMock.mockImplementationOnce(() => newPage.promise);
      const newRead = panel.webview.dispatchMessage({
        type: "fetchPage",
        payload: { fetchId: 12, page: 1 },
      });
      await vi.waitFor(() => expect(getPageMock).toHaveBeenCalledTimes(2));
      oldPage.resolve({ rows: [], totalCount: 91, columns: [] });
      await oldRead;
      const duplicateRead = panel.webview.dispatchMessage({
        type: "fetchPage",
        payload: { fetchId: 13, page: 1 },
      });
      await Promise.resolve();
      expect(getPageMock).toHaveBeenCalledTimes(2);
      newPage.resolve({ rows: [], totalCount: 2, columns: [] });
      await Promise.all([newRead, duplicateRead]);
      expect(getPageMock).toHaveBeenCalledTimes(2);
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "tableData",
        payload: expect.objectContaining({ fetchId: 13, totalCount: 2 }),
      });
    } finally {
      oldPage.resolve({ rows: [], totalCount: 0, columns: [] });
      newPage.resolve({ rows: [], totalCount: 0, columns: [] });
      await oldRead;
      await manager.dispose();
    }
  });

  it("routes query schema changes into explicit metadata refresh and rejects an old preview with its operation ID", async () => {
    const { panel, refresh, connectionManager } = await openSchemaRefreshPath();
    prepareApplyChangesPlanMock.mockReturnValue({
      executable: true,
      plan: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Edit" } }],
        skippedRows: [],
        previewStatements: ["UPDATE users"],
      },
    });
    await panel.webview.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "op-1",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Edit" } }],
      },
    });
    await refresh();
    expect(connectionManager.refreshSchemaCache).toHaveBeenCalledWith("conn-1");
    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "apply-preview-token" },
    });
    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "applyResult",
      payload: {
        operationId: "op-1",
        success: false,
        error: expect.stringContaining(
          "previous mutation preview cannot be applied",
        ),
      },
    });
  });

  it.each([
    "resolves false",
    "rejects",
  ] as const)("retries the correlated delete evidence when result delivery %s", async (delivery) => {
    const { panel } = await openSchemaRefreshPath();
    prepareDeleteRowsPlanMock.mockResolvedValue({
      previewStatements: ["DELETE FROM users WHERE id=1"],
    });
    await panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
    });

    const execution = deferred<unknown>();
    confirmMutationPreviewMock.mockImplementationOnce(() => execution.promise);
    if (delivery === "resolves false") {
      panel.webview.postMessage.mockResolvedValueOnce(false);
    } else {
      panel.webview.postMessage.mockRejectedValueOnce(
        new Error("Delivery failed"),
      );
    }
    const confirming = panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "preview-token" },
    });
    await vi.waitFor(() =>
      expect(confirmMutationPreviewMock).toHaveBeenCalledOnce(),
    );

    const result = {
      type: "deleteResult",
      payload: {
        operationId: "op-1",
        success: true,
        affectedRows: 1,
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: { id: 1 },
            status: "deleted",
            success: true,
          },
        ],
        changesPossible: true,
        outcomeUnknown: false,
      },
    };
    execution.resolve(result);
    await confirming;

    expect(panel.webview.postMessage).toHaveBeenCalledTimes(4);
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(3, result);
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(4, result);
    // A duplicated confirm cannot rerun the mutation while its result is retried.
    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "preview-token" },
    });
    expect(confirmMutationPreviewMock).toHaveBeenCalledOnce();
    expect(panel.webview.postMessage).toHaveBeenCalledTimes(4);
    expect(vscodeMock.module.window.showWarningMessage).not.toHaveBeenCalled();
  });

  it("preserves apply prevalidation diagnostics on false-to-true delivery without executing writes", async () => {
    const { panel, connectionManager } = await openSchemaRefreshPath();
    const error = 'Column "name" cannot persist the requested value exactly.';
    connectionManager.getDriver.mockReturnValueOnce({
      checkPersistedEdit: vi.fn(() => ({ ok: false, message: error })),
    } as never);
    const postCount = panel.webview.postMessage.mock.calls.length;
    panel.webview.postMessage.mockResolvedValueOnce(false);

    await panel.webview.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "op-validation",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Invalid" } }],
        insertValues: [{ name: "Invalid insert" }],
      },
    });

    const result = {
      type: "applyResult",
      payload: {
        operationId: "op-validation",
        success: false,
        error: `${error} ${error}`,
        failedRows: [0],
        insertApplied: false,
        rowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "prevalidation_failed",
            columns: ["name"],
            message: error,
          },
        ],
        insertRowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "prevalidation_failed",
            columns: ["name"],
            message: error,
          },
        ],
      },
    };
    expect(panel.webview.postMessage).toHaveBeenCalledTimes(postCount + 2);
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(
      postCount + 1,
      result,
    );
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(
      postCount + 2,
      result,
    );
    expect(prepareApplyChangesPlanMock).not.toHaveBeenCalled();
    expect(prepareInsertRowMock).not.toHaveBeenCalled();
    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
    expect(vscodeMock.module.window.showWarningMessage).not.toHaveBeenCalled();
  });

  it.each([
    {
      name: "committed staged INSERT success",
      type: "applyResult",
      payload: {
        success: true,
        insertApplied: true,
        changesPossible: true,
        outcomeUnknown: false,
        rowOutcomes: [],
        insertRowOutcomes: [{ rowIndex: 0, success: true, status: "applied" }],
      },
    },
    {
      name: "committed staged INSERT with rejected UPDATE",
      type: "applyResult",
      payload: {
        success: false,
        error: "Update rejected",
        insertApplied: true,
        changesPossible: true,
        outcomeUnknown: false,
        failedRows: [0],
        rowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "not_applied",
            message: "Update rejected",
          },
        ],
        insertRowOutcomes: [{ rowIndex: 0, success: true, status: "applied" }],
      },
    },
    {
      name: "partial UPDATE with known row outcomes",
      type: "applyResult",
      payload: {
        success: false,
        error: "Second update rejected",
        insertApplied: false,
        changesPossible: true,
        outcomeUnknown: false,
        failedRows: [1],
        rowOutcomes: [
          { rowIndex: 0, success: true, status: "applied" },
          {
            rowIndex: 1,
            success: false,
            status: "not_applied",
            message: "Second update rejected",
          },
        ],
      },
    },
    {
      name: "backend unknown UPDATE (not a delivery error)",
      type: "applyResult",
      payload: {
        success: false,
        error: "Backend acknowledgement lost",
        insertApplied: false,
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [{ rowIndex: 0, success: false, status: "unknown" }],
      },
    },
    {
      name: "committed standalone INSERT",
      type: "insertResult",
      payload: { success: true },
    },
    {
      name: "standalone INSERT validation diagnostics",
      type: "insertResult",
      payload: {
        success: false,
        error: "Invalid name",
        status: "prevalidation_failed",
        columns: ["name"],
      },
    },
    {
      name: "backend unknown DELETE with confirmed row evidence",
      type: "deleteResult",
      payload: {
        success: false,
        error: "Backend delete acknowledgement lost",
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: { id: 1 },
            success: true,
            status: "deleted",
          },
          {
            rowIndex: 1,
            primaryKeys: { id: 2 },
            success: false,
            status: "unknown",
          },
        ],
      },
    },
  ])("preserves $name on false-to-true result delivery", async ({
    type,
    payload,
  }) => {
    const { panel } = await openSchemaRefreshPath();
    prepareApplyChangesPlanMock.mockReturnValue({
      executable: true,
      plan: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Edit" } }],
        skippedRows: [],
        previewStatements: ["UPDATE users"],
      },
    });
    prepareInsertRowMock.mockResolvedValue({
      previewStatements: ["INSERT INTO users DEFAULT VALUES"],
    });
    prepareDeleteRowsPlanMock.mockResolvedValue({
      previewStatements: ["DELETE FROM users"],
    });
    const request =
      type === "applyResult"
        ? {
            type: "applyChanges",
            payload: {
              operationId: "op-1",
              updates: [{ primaryKeys: { id: 1 }, changes: { name: "Edit" } }],
              insertValues: [{}],
            },
          }
        : type === "insertResult"
          ? { type: "insertRow", payload: { operationId: "op-1", values: {} } }
          : {
              type: "deleteRows",
              payload: {
                operationId: "op-1",
                primaryKeysList: [{ id: 1 }, { id: 2 }],
              },
            };
    await panel.webview.dispatchMessage(request);
    const result = { type, payload: { operationId: "op-1", ...payload } };
    confirmMutationPreviewMock.mockResolvedValueOnce(result);
    const delivered: unknown[] = [];
    panel.webview.postMessage.mockImplementationOnce(async () => false);
    panel.webview.postMessage.mockImplementationOnce(
      async (message: unknown) => {
        delivered.push(message);
        return true;
      },
    );
    const postCount = panel.webview.postMessage.mock.calls.length;
    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: {
        operationId: "op-1",
        previewToken:
          type === "applyResult"
            ? "apply-preview-token"
            : type === "insertResult"
              ? "insert-preview-token"
              : "preview-token",
      },
    });
    expect(panel.webview.postMessage).toHaveBeenCalledTimes(postCount + 2);
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(
      postCount + 1,
      result,
    );
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(
      postCount + 2,
      result,
    );
    expect(delivered).toEqual([result]);
    expect((delivered[0] as typeof result).payload).toBe(result.payload);
    expect(confirmMutationPreviewMock).toHaveBeenCalledOnce();
    expect(vscodeMock.module.window.showWarningMessage).not.toHaveBeenCalled();
  });

  it.each([
    "resolves false",
    "rejects",
  ] as const)("cancels an undelivered mutation preview when postMessage %s", async (delivery) => {
    const { panel } = await openSchemaRefreshPath();
    prepareApplyChangesPlanMock.mockReturnValue({
      executable: true,
      plan: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Edit" } }],
        skippedRows: [],
        previewStatements: ["UPDATE users SET name = 'Edit' WHERE id = 1"],
      },
    });
    const postCountBeforeMutation = panel.webview.postMessage.mock.calls.length;
    if (delivery === "resolves false") {
      panel.webview.postMessage.mockImplementationOnce(async () => false);
    } else {
      panel.webview.postMessage.mockRejectedValueOnce(
        new Error("webview delivery failed"),
      );
    }

    await panel.webview.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "op-1",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Edit" } }],
      },
    });

    expect(panel.webview.postMessage).toHaveBeenCalledTimes(
      postCountBeforeMutation + 2,
    );
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(
      postCountBeforeMutation + 1,
      {
        type: "tableMutationPreview",
        payload: expect.objectContaining({
          operationId: "op-1",
          previewToken: "apply-preview-token",
        }),
      },
    );
    expect(panel.webview.postMessage).toHaveBeenNthCalledWith(
      postCountBeforeMutation + 2,
      {
        type: "applyResult",
        payload: {
          operationId: "op-1",
          success: false,
          error: expect.stringContaining("preview could not be delivered"),
        },
      },
    );
    expect(pendingPreviewControllerState.has("apply-preview-token")).toBe(
      false,
    );

    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: {
        operationId: "op-1",
        previewToken: "apply-preview-token",
      },
    });

    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
    expect(pendingPreviewControllerState.has("apply-preview-token")).toBe(
      false,
    );
    expect(panel.webview.postMessage).toHaveBeenCalledTimes(
      postCountBeforeMutation + 2,
    );
  });

  it("does not warn or retry a mutation reply that resolves false after the panel is disposed", async () => {
    const { panel } = await openSchemaRefreshPath();
    prepareDeleteRowsPlanMock.mockResolvedValue({
      previewStatements: ["DELETE FROM users WHERE id=1"],
    });
    await panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
    });
    confirmMutationPreviewMock.mockResolvedValueOnce({
      type: "deleteResult",
      payload: {
        operationId: "op-1",
        success: true,
        affectedRows: 1,
        rowOutcomes: [],
        changesPossible: true,
        outcomeUnknown: false,
      },
    });

    const delivery = deferred<boolean>();
    panel.webview.postMessage.mockImplementationOnce(() => delivery.promise);
    const confirming = panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "preview-token" },
    });
    await vi.waitFor(() =>
      expect(panel.webview.postMessage).toHaveBeenCalledTimes(3),
    );
    panel.dispose();
    delivery.resolve(false);
    await confirming;

    expect(panel.webview.postMessage).toHaveBeenCalledTimes(3);
    expect(vscodeMock.module.window.showWarningMessage).not.toHaveBeenCalled();
  });

  it.each([
    "exportCSV",
    "exportJSON",
  ])("forwards cancellation into the page-only %s read and rejects a late page", async (type) => {
    const { panel } = await openSchemaRefreshPath();
    getPageMock.mockClear();
    const pending = deferred<{ rows: []; totalCount: number; columns: [] }>();
    getPageMock.mockImplementationOnce(() => pending.promise);
    exportTableDataMock.mockImplementationOnce(async ({ loadChunks }) => {
      const controller = new AbortController();
      const iterator = loadChunks(controller.signal)[Symbol.asyncIterator]();
      const next = iterator.next();
      await vi.waitFor(() =>
        expect(getPageMock).toHaveBeenCalledWith(
          "conn-1",
          "db1",
          "public",
          "users",
          3,
          25,
          [],
          null,
          true,
          controller.signal,
        ),
      );
      controller.abort();
      pending.resolve({ rows: [], totalCount: 0, columns: [] });
      await expect(next).rejects.toMatchObject({ name: "AbortError" });
    });
    await panel.webview.dispatchMessage({
      type,
      payload: { limitToPage: { page: 3, pageSize: 25 } },
    });
    expect(exportTableDataMock).toHaveBeenCalledTimes(1);
  });

  it.each([
    "exportCSV",
    "exportJSON",
  ])("keeps visible-column positions and metadata across sparse chunks for full %s", async (type) => {
    const { panel } = await openSchemaRefreshPath();
    const id = { name: "id", category: "integer", nativeType: "int" } as const;
    const name = {
      name: "name",
      category: "text",
      nativeType: "string",
    } as const;
    const hidden = { ...name, name: "hidden" };
    exportAllMock.mockImplementationOnce(async function* () {
      yield {
        columns: [id, name, hidden],
        rows: [{ id: 1, name: "one", hidden: "secret" }],
      };
      yield { columns: [id, hidden], rows: [{ id: 2, hidden: "secret2" }] };
      yield { columns: [name, id], rows: [{ name: "three", id: 3 }] };
    });
    const actual: ChunkedExportData[] = [];
    exportTableDataMock.mockImplementationOnce(async ({ loadChunks }) => {
      for await (const chunk of loadChunks(new AbortController().signal))
        actual.push(chunk);
    });
    await panel.webview.dispatchMessage({
      type,
      payload: {
        columnOrder: ["name", "id"],
        filters: [{ column: "id", operator: "gte", value: "1" }],
        sort: { column: "id", direction: "asc" },
      },
    });
    expect(actual).toEqual([
      { columns: [name, id], rows: [{ name: "one", id: 1 }] },
      { columns: [name, id], rows: [{ name: undefined, id: 2 }] },
      { columns: [name, id], rows: [{ name: "three", id: 3 }] },
    ]);
    expect(exportAllMock).toHaveBeenCalledWith(
      "conn-1",
      "db1",
      "public",
      "users",
      500,
      { column: "id", direction: "asc" },
      [{ column: "id", operator: "gte", value: "1" }],
      expect.any(AbortSignal),
    );
  });

  it("uses described metadata for a selected column absent even from the first page", async () => {
    const { panel } = await openSchemaRefreshPath();
    const name = {
      name: "name",
      isPrimaryKey: false,
      category: "text",
      nativeType: "string",
    } as const;
    getColumnsMock.mockResolvedValue([
      name,
      { name: "id", isPrimaryKey: true },
    ]);
    await panel.webview.dispatchMessage({ type: "ready" });
    exportAllMock.mockImplementationOnce(async function* () {
      yield {
        columns: [{ name: "id", category: "integer", nativeType: "int" }],
        rows: [{ id: 1 }],
      };
    });
    const actual: ChunkedExportData[] = [];
    exportTableDataMock.mockImplementationOnce(async ({ loadChunks }) => {
      for await (const chunk of loadChunks(new AbortController().signal))
        actual.push(chunk);
    });
    await panel.webview.dispatchMessage({
      type: "exportCSV",
      payload: { columnOrder: ["name", "id"] },
    });
    expect(actual[0]?.columns.map((column) => column.name)).toEqual([
      "name",
      "id",
    ]);
    expect(actual[0]?.columns[0]).toEqual(name);
    expect(actual[0]?.rows).toEqual([{ name: undefined, id: 1 }]);
  });

  it.each([
    { selection: ["unknown"] },
    { selection: ["id", "unknown"] },
    { selection: [] },
    { selection: ["id", "id"] },
  ])("rejects invalid explicit export selection $selection rather than exporting all columns", async ({
    selection,
  }) => {
    const { panel } = await openSchemaRefreshPath();
    exportAllMock.mockImplementationOnce(async function* () {
      yield {
        columns: [{ name: "id", category: "integer", nativeType: "int" }],
        rows: [{ id: 1, hidden: "secret" }],
      };
    });
    let exportError: unknown;
    exportTableDataMock.mockImplementationOnce(async ({ loadChunks }) => {
      const iterator = loadChunks(new AbortController().signal)[
        Symbol.asyncIterator
      ]();
      try {
        await iterator.next();
      } catch (error) {
        exportError = error;
      }
    });
    await panel.webview.dispatchMessage({
      type: "exportCSV",
      payload: { columnOrder: selection },
    });
    expect(exportTableDataMock).toHaveBeenCalledOnce();
    expect(exportError).toBeInstanceOf(Error);
    expect((exportError as Error).message).toMatch(
      /(Unknown export column|column selection)/,
    );
  });

  it("passes the complete schema and rows through when no column selection is supplied", async () => {
    const { panel } = await openSchemaRefreshPath();
    const chunk = {
      columns: [
        { name: "id", category: "integer", nativeType: "int" },
      ] as const,
      rows: [{ id: 1, newField: "must not silently disappear" }],
    };
    exportAllMock.mockImplementationOnce(async function* () {
      yield chunk;
    });
    const actual: ChunkedExportData[] = [];
    exportTableDataMock.mockImplementationOnce(async ({ loadChunks }) => {
      for await (const result of loadChunks(new AbortController().signal))
        actual.push(result);
    });
    await panel.webview.dispatchMessage({ type: "exportCSV" });
    expect(actual).toEqual([chunk]);
  });

  it.each([
    false,
    true,
  ])("rejects a server-A preview with its operation ID after reconnect to same-columns server B (install pending=%s)", async (pendingInstall) => {
    const {
      manager,
      panel,
      driver,
      driverFactory,
      onDispose,
      setConnectDelay,
    } = await restoreWithRealConnectionManager({
      realPreviews: true,
      host: "server-a",
    });
    const connecting = deferred<void>();
    let reconnect: Promise<void> | undefined;
    try {
      await panel.webview.dispatchMessage({ type: "ready" });
      prepareDeleteRowsPlanMock.mockResolvedValue(deletePreviewPlan());
      await panel.webview.dispatchMessage({
        type: "deleteRows",
        payload: { operationId: "delete-a", primaryKeysList: [{ id: 1 }] },
      });
      const previewA = lastMutationPreview(panel, "delete-a");
      expect(driver.targetHost).toBe("server-a");
      expect(driver.query).not.toHaveBeenCalled();
      await driver.disconnect(); // Transport loss, NOT panel-closing Disconnect.
      const config = manager.getConnection("conn-1");
      if (!config) throw new Error("Expected connection config");
      await manager.saveConnection({ ...config, host: "server-b" });
      expect(onDispose).not.toHaveBeenCalled();
      if (pendingInstall) setConnectDelay(connecting.promise);
      reconnect = manager.connectTo("conn-1", "explicit");
      await vi.waitFor(() => expect(driverFactory).toHaveBeenCalledTimes(2));
      const driverB = driverFactory.mock.results[1].value;
      expect(driverB.targetHost).toBe("server-b");
      if (!pendingInstall) {
        await reconnect;
        await vi.waitFor(() =>
          expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
            type: "tableInit",
            payload: expect.objectContaining({
              intent: "connectionRefresh",
              columns: [{ name: "id", isPrimaryKey: true }],
            }),
          }),
        );
      } else {
        expect(manager.isConnecting("conn-1")).toBe(true);
        expect(manager.isConnected("conn-1")).toBe(false);
      }
      await panel.webview.dispatchMessage({
        type: "confirmMutationPreview",
        payload: {
          operationId: "delete-a",
          previewToken: previewA.previewToken,
        },
      });
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "deleteResult",
        payload: {
          operationId: "delete-a",
          success: false,
          error: expect.stringContaining("Connection session changed"),
          changesPossible: false,
          outcomeUnknown: false,
          affectedRows: 0,
          rowOutcomes: [],
        },
      });
      expect(executePreparedDeletePlanMock).not.toHaveBeenCalled();
      expect(driverB.runTransaction).not.toHaveBeenCalled();
      expect(driverB.query).not.toHaveBeenCalled();
      expect(driver.query).not.toHaveBeenCalled();

      connecting.resolve();
      await reconnect;
      await vi.waitFor(() =>
        expect(panel.webview.postMessage).toHaveBeenCalledWith({
          type: "tableInit",
          payload: expect.objectContaining({ intent: "connectionRefresh" }),
        }),
      );
      await panel.webview.dispatchMessage({
        type: "deleteRows",
        payload: { operationId: "delete-b", primaryKeysList: [{ id: 1 }] },
      });
      const previewB = lastMutationPreview(panel, "delete-b");
      expect(previewB.previewToken).not.toBe(previewA.previewToken);
      await panel.webview.dispatchMessage({
        type: "confirmMutationPreview",
        payload: {
          operationId: "delete-b",
          previewToken: previewB.previewToken,
        },
      });
      expect(executePreparedDeletePlanMock).toHaveBeenCalledOnce();
      expect(driverB.query).toHaveBeenCalledTimes(2);
      expect(driverB.query).toHaveBeenNthCalledWith(
        1,
        "DELETE FROM restored_items WHERE id = $1",
        [1],
      );
      expect(driverB.query).toHaveBeenNthCalledWith(
        2,
        'SELECT 1 FROM restored_items WHERE "id" = $1',
        [1],
        { database: "db1" },
      );
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "deleteResult",
        payload: expect.objectContaining({
          operationId: "delete-b",
          success: true,
        }),
      });
      expect(onDispose).not.toHaveBeenCalled();
      expect(driver.query).not.toHaveBeenCalled();
    } finally {
      connecting.resolve();
      setConnectDelay();
      await reconnect;
      await manager.dispose();
    }
  });

  it.each([
    "plan",
    "metadata",
  ] as const)("rejects a late server-A preview preparation waiting on %s after server B's metadata is installed", async (waitingOn) => {
    const { manager, panel, driver, driverFactory } =
      await restoreWithRealConnectionManager({
        realPreviews: true,
        host: "server-a",
      });
    const plan = deferred<ReturnType<typeof deletePreviewPlan>>();
    const metadata = deferred<MockColumn[]>();
    let preparing: Promise<void> | undefined;
    try {
      await panel.webview.dispatchMessage({ type: "ready" });
      if (waitingOn === "metadata") {
        getColumnsMock.mockImplementationOnce(() => metadata.promise);
        manager.refreshSchemaCache("conn-1");
        await vi.waitFor(() => expect(getColumnsMock).toHaveBeenCalledTimes(2));
      } else
        prepareDeleteRowsPlanMock.mockImplementationOnce(() => plan.promise);
      preparing = panel.webview.dispatchMessage({
        type: "deleteRows",
        payload: { operationId: "late-delete-a", primaryKeysList: [{ id: 1 }] },
      });
      if (waitingOn === "plan")
        await vi.waitFor(() =>
          expect(prepareDeleteRowsPlanMock).toHaveBeenCalledOnce(),
        );
      await driver.disconnect();
      const config = manager.getConnection("conn-1");
      if (!config) throw new Error("Expected connection config");
      await manager.saveConnection({ ...config, host: "server-b" });
      await manager.connectTo("conn-1", "explicit");
      await vi.waitFor(() =>
        expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
          type: "tableInit",
          payload: expect.objectContaining({ intent: "connectionRefresh" }),
        }),
      );
      // The old build/metadata completes only AFTER the new same-columns session.
      plan.resolve(deletePreviewPlan());
      metadata.resolve([{ name: "id", isPrimaryKey: true }]);
      await preparing;
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: "deleteResult",
        payload: expect.objectContaining({
          operationId: "late-delete-a",
          success: false,
          error: expect.stringContaining("Connection session changed"),
        }),
      });
      expect(panel.webview.postMessage).not.toHaveBeenCalledWith({
        type: "tableMutationPreview",
        payload: expect.anything(),
      });
      if (waitingOn === "metadata")
        expect(prepareDeleteRowsPlanMock).not.toHaveBeenCalled();
      expect(executePreparedDeletePlanMock).not.toHaveBeenCalled();
      expect(driverFactory.mock.results[1].value.query).not.toHaveBeenCalled();
      expect(driver.query).not.toHaveBeenCalled();
    } finally {
      plan.resolve(deletePreviewPlan());
      metadata.resolve([]);
      await preparing;
      await manager.dispose();
    }
  });

  it.each([
    ["deleteRows", "deleteResult"],
    ["insertRow", "insertResult"],
    ["applyChanges", "applyResult"],
  ] as const)("rejects %s during the panel's pending initial Connect with a correlated %s", async (kind, resultType) => {
    const { panel, connectionManager, setReadOnly } =
      await restoreDisconnectedPanel();
    setReadOnly(false);
    const connecting = deferred<void>();
    const connect = connectionManager.connectTo.getMockImplementation();
    if (!connect) throw new Error("Expected connect implementation");
    connectionManager.connectTo.mockImplementationOnce(async (...args) => {
      await connecting.promise;
      await connect(...args);
    });
    const ready = panel.webview.dispatchMessage({ type: "ready" });
    try {
      await vi.waitFor(() =>
        expect(connectionManager.connectTo).toHaveBeenCalledOnce(),
      );
      await panel.webview.dispatchMessage({
        type: kind,
        payload: {
          operationId: "pending-connect-mutation",
        },
      });
      expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
        type: resultType,
        payload: expect.objectContaining({
          operationId: "pending-connect-mutation",
          success: false,
          error: expect.stringContaining("not connected"),
        }),
      });
      expect(prepareDeleteRowsPlanMock).not.toHaveBeenCalled();
      expect(prepareInsertRowMock).not.toHaveBeenCalled();
      expect(prepareApplyChangesPlanMock).not.toHaveBeenCalled();
      expect(createDeleteRowsPreviewMock).not.toHaveBeenCalled();
      expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
    } finally {
      connecting.resolve();
      await ready;
    }
  });

  it("keeps an unchanged preview valid across duplicate metadata refresh", async () => {
    const { panel, refresh } = await openSchemaRefreshPath();
    prepareDeleteRowsPlanMock.mockResolvedValue({
      previewStatements: ["DELETE FROM users"],
    });
    await panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
    });
    await refresh([
      { name: "id", isPrimaryKey: true },
      { name: "name", isPrimaryKey: false },
    ]);
    confirmMutationPreviewMock.mockResolvedValue({
      type: "deleteResult",
      payload: { operationId: "op-1", success: true },
    });
    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "preview-token" },
    });
    expect(confirmMutationPreviewMock).toHaveBeenCalledWith(
      "preview-token",
      "op-1",
    );
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "deleteResult",
      payload: { operationId: "op-1", success: true },
    });
  });

  it.each([
    true,
    false,
  ])("delivers an in-flight execution's late result (success=%s) after query-triggered metadata refresh", async (success) => {
    const { panel, refresh } = await openSchemaRefreshPath();
    prepareDeleteRowsPlanMock.mockResolvedValue({
      previewStatements: ["DELETE FROM users"],
    });
    await panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
    });
    let resolve!: (value: unknown) => void;
    confirmMutationPreviewMock.mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    const execution = panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "preview-token" },
    });
    await vi.waitFor(() =>
      expect(confirmMutationPreviewMock).toHaveBeenCalledTimes(1),
    );
    await refresh();
    const result = {
      type: "deleteResult",
      payload: {
        operationId: "op-1",
        success,
        ...(success ? {} : { error: "Late driver failure" }),
      },
    };
    resolve(result);
    await execution;
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith(result);
  });

  it("does not reuse a pre-refresh read promise for a new schema", async () => {
    const { panel, refresh } = await openSchemaRefreshPath();
    let resolve!: (value: {
      rows: never[];
      totalCount: number;
      columns: never[];
    }) => void;
    getPageMock.mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    const first = panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: { fetchId: 1, page: 1, pageSize: 25 },
    });
    await vi.waitFor(() => expect(getPageMock).toHaveBeenCalledTimes(1));
    await refresh();
    await panel.webview.dispatchMessage({
      type: "fetchPage",
      payload: { fetchId: 2, page: 1, pageSize: 25 },
    });
    expect(getPageMock).toHaveBeenCalledTimes(2);
    resolve({ rows: [], totalCount: 0, columns: [] });
    await first;
  });

  it("rejects a plan whose preparation overlaps metadata refresh", async () => {
    const { panel, refresh } = await openSchemaRefreshPath();
    let resolve!: (value: unknown) => void;
    prepareDeleteRowsPlanMock.mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    const preparing = panel.webview.dispatchMessage({
      type: "deleteRows",
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
    });
    await vi.waitFor(() =>
      expect(prepareDeleteRowsPlanMock).toHaveBeenCalledTimes(1),
    );
    await refresh();
    resolve({ previewStatements: ["DELETE FROM users WHERE id=1"] });
    await preparing;
    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "deleteResult",
      payload: {
        operationId: "op-1",
        success: false,
        error: expect.stringContaining("Schema metadata changed"),
      },
    });
  });

  it.each([
    false,
    true,
  ])("rejects a refresh starting between schema-wait completion and execution (autoSkip=%s)", async (autoSkip) => {
    const { panel, connectionManager } = await openSchemaRefreshPath();
    prepareDeleteRowsPlanMock.mockResolvedValue({
      previewStatements: ["DELETE FROM users WHERE id=1"],
    });
    const metadata = deferred<MockColumn[]>();
    getColumnsMock.mockImplementationOnce(() => metadata.promise);
    const startRefresh = () => connectionManager.refreshSchemaCache("conn-1");
    let execution: Promise<void>;
    if (autoSkip) {
      connectionManager.getSkipTableMutationPreview.mockImplementationOnce(
        () => {
          // The first microtask precedes the helper's continuation; the nested
          // one follows its return but precedes the awaiting caller's continuation.
          queueMicrotask(() => queueMicrotask(startRefresh));
          return true;
        },
      );
      execution = panel.webview.dispatchMessage({
        type: "deleteRows",
        payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
      });
    } else {
      await panel.webview.dispatchMessage({
        type: "deleteRows",
        payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
      });
      execution = panel.webview.dispatchMessage({
        type: "confirmMutationPreview",
        payload: { operationId: "op-1", previewToken: "preview-token" },
      });
      queueMicrotask(startRefresh);
    }
    await execution;
    // Metadata is still unresolved and cached columns still match the old plan.
    expect(getColumnsMock).toHaveBeenCalledTimes(2);
    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "deleteResult",
      payload: {
        operationId: "op-1",
        success: false,
        error: expect.stringContaining("Schema metadata changed"),
      },
    });
    metadata.resolve([{ name: "new_id", isPrimaryKey: true }]);
    await vi.waitFor(() =>
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({
          columns: [{ name: "new_id", isPrimaryKey: true }],
        }),
      }),
    );
    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "preview-token" },
    });
    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
  });

  it.each([
    false,
    true,
  ])("never prepares old columns under a new pending generation (autoSkip=%s)", async (autoSkip) => {
    const { panel, connectionManager } = await openSchemaRefreshPath();
    connectionManager.getSkipTableMutationPreview.mockReturnValue(autoSkip);
    const metadata = deferred<MockColumn[]>();
    const statements = deferred<string[]>();
    const builder = vi.fn(() => statements.promise);
    const driver = {
      ...connectionManager.getDriver(),
      buildMutationPreviewStatements: builder,
    };
    connectionManager.getDriver.mockReturnValue(driver);
    prepareApplyChangesPlanMock.mockReturnValue({
      executable: true,
      plan: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "007" } }],
        skippedRows: [],
        previewStatements: [],
      },
    });
    getColumnsMock.mockImplementationOnce(() => metadata.promise);
    const preparation = panel.webview.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "op-1",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "007" } }],
      },
    });
    queueMicrotask(() => connectionManager.refreshSchemaCache("conn-1"));
    await vi.waitFor(() => expect(getColumnsMock).toHaveBeenCalledTimes(2));
    // In the old implementation preparation captured the new generation but
    // used old columns, then paused in this builder until metadata was replaced.
    metadata.resolve([
      { name: "id", isPrimaryKey: true },
      { name: "new_name", isPrimaryKey: false },
    ]);
    await vi.waitFor(() =>
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({ intent: "metadataRefresh" }),
      }),
    );
    statements.resolve(["UPDATE users SET name='007' WHERE id=1"]);
    await preparation;
    expect(prepareApplyChangesPlanMock).not.toHaveBeenCalled();
    expect(builder).not.toHaveBeenCalled();
    expect(createApplyChangesPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "applyResult",
      payload: {
        operationId: "op-1",
        success: false,
        error: expect.stringContaining("Schema metadata changed"),
      },
    });
    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "apply-preview-token" },
    });
    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
  });

  it.each([
    false,
    true,
  ])("retains the preparation snapshot when metadata resolves during an async preview builder (autoSkip=%s)", async (autoSkip) => {
    const { panel, connectionManager } = await openSchemaRefreshPath();
    connectionManager.getSkipTableMutationPreview.mockReturnValue(autoSkip);
    const metadata = deferred<MockColumn[]>();
    const statements = deferred<string[]>();
    const builder = vi.fn(() => statements.promise);
    const initialColumns = [
      { name: "id", isPrimaryKey: true },
      { name: "name", isPrimaryKey: false },
    ];
    const driver = {
      ...connectionManager.getDriver(),
      buildMutationPreviewStatements: builder,
    };
    connectionManager.getDriver.mockReturnValue(driver);
    prepareApplyChangesPlanMock.mockReturnValue({
      executable: true,
      plan: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "007" } }],
        skippedRows: [],
        previewStatements: [],
      },
    });
    const preparation = panel.webview.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "op-1",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "007" } }],
      },
    });
    await vi.waitFor(() => expect(builder).toHaveBeenCalledTimes(1));
    expect(prepareApplyChangesPlanMock.mock.calls[0]?.[6]).toEqual(
      initialColumns,
    );
    getColumnsMock.mockImplementationOnce(() => metadata.promise);
    connectionManager.refreshSchemaCache("conn-1");
    metadata.resolve([
      { name: "id", isPrimaryKey: true },
      { name: "new_name", isPrimaryKey: false },
    ]);
    await vi.waitFor(() =>
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "tableInit",
        payload: expect.objectContaining({ intent: "metadataRefresh" }),
      }),
    );
    statements.resolve(["UPDATE users SET name='007' WHERE id=1"]);
    await preparation;
    expect(createApplyChangesPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "applyResult",
      payload: {
        operationId: "op-1",
        success: false,
        error: expect.stringContaining("Schema metadata changed"),
      },
    });
    await panel.webview.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "op-1", previewToken: "apply-preview-token" },
    });
    expect(confirmMutationPreviewMock).not.toHaveBeenCalled();
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

  it.each([
    "pg",
    "mongodb",
  ])("wires readonly and %s identity state into the table webview", async (type) => {
    const columns = [{ name: "id", isPrimaryKey: true }];
    getColumnsMock.mockResolvedValueOnce(columns);

    const connectionManager = {
      getConnection: vi.fn(() => ({
        name: "Readonly",
        type,
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
        extraCspDirectives: ["worker-src blob:"],
        initialState: expect.objectContaining({
          view: "table",
          connectionId: "conn-1",
          database: "db1",
          schema: "public",
          table: "users",
          isView: false,
          connectionReadOnly: true,
          mongoRowIdentity: type === "mongodb",
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
        intent: "initialize",
        primaryKeyColumns: ["id"],
        isView: false,
        connectionReadOnly: true,
      },
    });
  });

  it("does not let stale column metadata overwrite a schema refresh", async () => {
    let resolveInitialColumns!: (value: MockColumn[]) => void;
    let resolveRefreshedColumns!: (value: MockColumn[]) => void;
    let refreshListener: ((connectionId?: string) => void) | undefined;
    getColumnsMock
      .mockImplementationOnce(
        () =>
          new Promise<MockColumn[]>((resolve) => {
            resolveInitialColumns = resolve;
          }),
      )
      .mockImplementationOnce(
        () =>
          new Promise<MockColumn[]>((resolve) => {
            resolveRefreshedColumns = resolve;
          }),
      );

    const connectionManager = {
      getConnection: vi.fn(() => ({
        name: "Main",
        type: "pg",
        readOnly: false,
      })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(
        (listener: (connectionId?: string) => void) => {
          refreshListener = listener;
          return { dispose: vi.fn() };
        },
      ),
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

    const readyPromise = panel.webview.dispatchMessage({ type: "ready" });
    refreshListener?.("conn-1");
    resolveRefreshedColumns([{ name: "new_id", isPrimaryKey: true }]);
    await Promise.resolve();
    await Promise.resolve();
    resolveInitialColumns([{ name: "old_id", isPrimaryKey: true }]);
    await readyPromise;

    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "tableInit",
      payload: expect.objectContaining({
        columns: [{ name: "new_id", isPrimaryKey: true }],
      }),
    });
    expect(panel.webview.postMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({
        payload: expect.objectContaining({
          columns: [{ name: "old_id", isPrimaryKey: true }],
        }),
      }),
    );
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

    await vi.waitFor(() => expect(getPageMock).toHaveBeenCalledTimes(1));

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
      getSkipTableMutationPreview: vi.fn(() => false),
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
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
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

  it("executes deleteRows immediately when preview skipping is enabled", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
      getSkipTableMutationPreview: vi.fn(() => true),
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
    confirmMutationPreviewMock.mockResolvedValueOnce({
      type: "deleteResult",
      payload: { success: true },
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
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
    });

    expect(confirmMutationPreviewMock).toHaveBeenCalledWith(
      "preview-token",
      "op-1",
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "deleteResult",
      payload: { success: true },
    });
    expect(panel.webview.postMessage).not.toHaveBeenCalledWith({
      type: "tableMutationPreview",
      payload: expect.anything(),
    });
  });

  it("executes insertRow immediately when preview skipping is enabled", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
      getSkipTableMutationPreview: vi.fn(() => true),
    };

    prepareInsertRowMock.mockResolvedValueOnce({
      connectionId: "conn-1",
      database: "db1",
      schema: "public",
      table: "users",
      previewStatements: ["INSERT INTO users (id) VALUES (1)"],
    });
    confirmMutationPreviewMock.mockResolvedValueOnce({
      type: "insertResult",
      payload: { success: true },
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
      type: "insertRow",
      payload: { operationId: "op-1", values: { id: 1 } },
    });

    expect(createInsertPreviewMock).toHaveBeenCalledOnce();
    expect(confirmMutationPreviewMock).toHaveBeenCalledWith(
      "insert-preview-token",
      "op-1",
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "insertResult",
      payload: { success: true },
    });
    expect(panel.webview.postMessage).not.toHaveBeenCalledWith({
      type: "tableMutationPreview",
      payload: expect.anything(),
    });
  });

  it("executes applyChanges immediately when preview skipping is enabled", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Main" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
      getSkipTableMutationPreview: vi.fn(() => true),
      getDriver: vi.fn(() => undefined),
    };

    prepareApplyChangesPlanMock.mockReturnValueOnce({
      executable: true,
      plan: {
        operations: [{ sql: "UPDATE users SET name = ? WHERE id = ?" }],
        previewStatements: ["UPDATE users SET name = 'Ada' WHERE id = 1"],
        updates: [],
        skippedRows: [],
      },
      result: {
        success: true,
        rowOutcomes: [],
      },
    });
    confirmMutationPreviewMock.mockResolvedValueOnce({
      type: "applyResult",
      payload: { success: true, rowOutcomes: [] },
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
      type: "applyChanges",
      payload: {
        operationId: "op-1",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Ada" } }],
      },
    });

    expect(createApplyChangesPreviewMock).toHaveBeenCalledOnce();
    expect(createApplyChangesPreviewMock).toHaveBeenCalledWith(
      "op-1",
      expect.objectContaining({ inserts: [] }),
    );
    expect(confirmMutationPreviewMock).toHaveBeenCalledWith(
      "apply-preview-token",
      "op-1",
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "applyResult",
      payload: { success: true, rowOutcomes: [] },
    });
    expect(panel.webview.postMessage).not.toHaveBeenCalledWith({
      type: "tableMutationPreview",
      payload: expect.anything(),
    });
  });

  it("builds an Elasticsearch update preview with the driver instance", async () => {
    const driver = new ElasticsearchDriver({ type: "elasticsearch" } as never);
    const connectionManager = {
      getConnection: vi.fn(() => ({ name: "Elastic", type: "elasticsearch" })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      getDefaultPageSize: vi.fn(() => 25),
      getSkipTableMutationPreview: vi.fn(() => false),
      getDriver: vi.fn(() => driver),
    };
    prepareApplyChangesPlanMock.mockReturnValueOnce({
      executable: true,
      plan: {
        updates: [{ primaryKeys: { _id: "doc-1" }, changes: { label: "Ada" } }],
        skippedRows: [],
        previewStatements: [],
      },
    });

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "db1",
      "",
      "users",
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected table panel instance");

    await panel.webview.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "op-1",
        updates: [{ primaryKeys: { _id: "doc-1" }, changes: { label: "Ada" } }],
      },
    });

    expect(createApplyChangesPreviewMock).toHaveBeenCalledWith(
      "op-1",
      expect.objectContaining({
        apply: expect.objectContaining({
          previewStatements: [expect.stringContaining("/users/_doc/doc-1")],
        }),
      }),
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "tableMutationPreview",
      payload: expect.anything(),
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
      payload: { operationId: "op-1", primaryKeysList: [] },
    });

    expect(createDeleteRowsPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "deleteResult",
      payload: {
        operationId: "op-1",
        success: true,
        affectedRows: 0,
        rowOutcomes: [],
        changesPossible: false,
        outcomeUnknown: false,
      },
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
      payload: { operationId: "op-1", primaryKeysList: [{ id: 1 }] },
    });

    expect(createDeleteRowsPreviewMock).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "deleteResult",
      payload: {
        operationId: "op-1",
        success: false,
        error: "Plan failed",
        affectedRows: 0,
        changesPossible: false,
        outcomeUnknown: false,
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: { id: 1 },
            status: "skipped",
            success: false,
            message: "Plan failed",
          },
        ],
      },
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
      "activity:* (keyspace) [Cache]",
      expect.anything(),
      expect.anything(),
    );

    TablePanel.disposeAll();

    TablePanel.createOrShow(
      { extensionUri: {} } as never,
      redisManager as never,
      "conn-redis",
      "db0",
      "db0",
      REDIS_ALL_KEYS_TABLE,
    );
    expect(vscodeMock.createWebviewPanel).toHaveBeenLastCalledWith(
      "rapidb.tablePanel",
      "All keys (keyspace) [Cache]",
      expect.anything(),
      expect.anything(),
    );
    expect(createWebviewShellMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        initialState: expect.objectContaining({
          table: REDIS_ALL_KEYS_TABLE,
          displayTableName: "All keys",
        }),
      }),
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
