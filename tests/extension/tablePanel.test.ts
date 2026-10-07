import { beforeEach, describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { REDIS_ALL_KEYS_TABLE } from "../../src/extension/dbDrivers/redisKeyspace";
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
  vi.fn<
    (previewToken: string, operationId?: string) => Promise<unknown | null>
  >(async () => null),
);
const pendingPreviewControllerState = vi.hoisted(
  () => new Map<string, string>(),
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
    exportAll = exportAllMock;
    prepareInsertRow = prepareInsertRowMock;
    prepareDeleteRowsPlan = prepareDeleteRowsPlanMock;
    clearForConnection = vi.fn();
  },
  prepareApplyChangesPlan: prepareApplyChangesPlanMock,
}));

vi.mock("../../src/extension/panels/tableMutationPreviewController", () => ({
  TableMutationPreviewController: class {
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
    pendingPreviewControllerState.clear();
    getColumnsMock.mockReset();
    getColumnsMock.mockResolvedValue([]);
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
