import { beforeEach, describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { QueryPanelController } from "../../src/extension/panels/queryPanelController";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { ChunkedExportData } from "../../src/extension/utils/exportService";

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
const prepareInsertRowMock = vi.hoisted(() =>
  vi.fn<() => Promise<unknown | null>>(async () => null),
);
const prepareApplyChangesPlanMock = vi.hoisted(() => vi.fn());
const confirmMutationPreviewMock = vi.hoisted(() =>
  vi.fn<() => Promise<unknown | null>>(async () => null),
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
    prepareInsertRow = prepareInsertRowMock;
    prepareDeleteRowsPlan = prepareDeleteRowsPlanMock;
    clearForConnection = vi.fn();
  },
  prepareApplyChangesPlan: prepareApplyChangesPlanMock,
}));

vi.mock("../../src/extension/panels/tableMutationPreviewController", () => ({
  TableMutationPreviewController: class {
    clear = vi.fn();
    confirm = confirmMutationPreviewMock;
    cancel = vi.fn();
    createApplyChangesPreview = createApplyChangesPreviewMock;
    createInsertPreview = createInsertPreviewMock;
    createDeleteRowsPreview = createDeleteRowsPreviewMock;
  },
}));

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("TablePanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    TablePanel.disposeAll();
    getColumnsMock.mockReset();
    getColumnsMock.mockResolvedValue([]);
    getPageMock.mockClear();
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
