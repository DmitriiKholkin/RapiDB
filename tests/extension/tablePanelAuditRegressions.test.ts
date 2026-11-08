import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import {
  type PreparedDeletePlan,
  TableDataService,
} from "../../src/extension/tableDataService";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import type { FakeWebviewPanelHandle } from "../workflow/bridge/fakeWebviewPanel";

const state = vi.hoisted(() => ({
  panel: null as FakeWebviewPanelHandle | null,
  showWarningMessage: vi.fn(),
  writeText: vi.fn(async (_text: string) => undefined),
}));
vi.mock("vscode", async () => {
  const { createFakeWebviewPanel } = await import(
    "../workflow/bridge/fakeWebviewPanel"
  );
  return {
    ViewColumn: { One: 1 },
    window: {
      createWebviewPanel: () => {
        state.panel = createFakeWebviewPanel({
          viewType: "table",
          title: "Table",
        });
        return state.panel.panel;
      },
      showWarningMessage: state.showWarningMessage,
      showErrorMessage: vi.fn(),
    },
    workspace: { onDidChangeConfiguration: () => ({ dispose() {} }) },
    env: { clipboard: { writeText: state.writeText } },
  };
});
vi.mock("../../src/extension/panels/webviewShell", () => ({
  APP_WEBVIEW_SHELL_LAYOUT: {},
  WEBVIEW_SCROLLBAR_STYLES: "",
  createWebviewShell: () => "<html></html>",
}));

function column(
  name: string,
  overrides: Partial<ColumnTypeMeta> = {},
): ColumnTypeMeta {
  return {
    name,
    nativeType: "text",
    type: "text",
    category: "text",
    isPrimaryKey: false,
    nullable: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
    ...overrides,
  };
}

const pgColumns = [
  column("id", {
    nativeType: "integer",
    type: "integer",
    category: "integer",
    isPrimaryKey: true,
    nullable: false,
  }),
  column("name"),
];

beforeEach(() => {
  state.showWarningMessage.mockClear();
});

afterEach(() => {
  TablePanel.disposeAll();
  vi.restoreAllMocks();
});

async function setup(
  driver: IDBDriver,
  config: { id: string; name: string; type: string },
) {
  const disconnectListeners = new Set<(id: string) => void>();
  const refreshListeners = new Set<(id?: string) => void>();
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
    getDefaultPageSize: () => 25,
    getSkipTableMutationPreview: () => false,
    getQueryEditorPresentation: () => undefined,
    isConnected: () => true,
    onDidDisconnect: (listener: (id: string) => void) => {
      disconnectListeners.add(listener);
      return { dispose: () => disconnectListeners.delete(listener) };
    },
    onDidRefreshSchemas: (listener: (id?: string) => void) => {
      refreshListeners.add(listener);
      return { dispose: () => refreshListeners.delete(listener) };
    },
  };
  TablePanel.createOrShow(
    {} as never,
    manager as never,
    config.id,
    "db",
    "public",
    "items",
  );
  const panel = state.panel;
  if (!panel) throw new Error("Missing panel");
  const onDispose = vi.fn();
  panel.onDidDispose(onDispose);
  await panel.dispatchMessage({ type: "ready" });
  await vi.waitFor(() =>
    expect(panel.hostMessages().at(-1)?.type).toBe("tableInit"),
  );
  return {
    panel,
    onDispose,
    disconnect: () => {
      for (const listener of disconnectListeners) listener(config.id);
    },
    refreshSchemas: () => {
      for (const listener of refreshListeners) listener(config.id);
    },
  };
}

async function setupPostgres(columns = pgColumns) {
  const config = { id: "pg-audit", name: "PG", type: "pg" };
  const driver = new PostgresDriver(config as never);
  const describeColumns = vi
    .spyOn(driver, "describeColumns")
    .mockResolvedValue(columns);
  return { ...(await setup(driver, config)), describeColumns };
}

function deletePlan(): PreparedDeletePlan {
  return {
    connectionId: "pg-audit",
    database: "db",
    schema: "public",
    table: "items",
    executionMode: "transaction",
    operations: [{ sql: "DELETE FROM items WHERE id = $1", params: [1] }],
    previewStatements: ["DELETE FROM items WHERE id = 1"],
    rowIdentities: [{ id: 1 }],
    verificationCriteriaList: [{ id: 1 }],
  };
}

describe("PNL-2 unsaved table edits survive disconnect", () => {
  it("closes a clean table panel on disconnect", async () => {
    const s = await setupPostgres();
    s.disconnect();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(state.showWarningMessage).not.toHaveBeenCalled();
  });

  it("keeps a table panel with unsaved edits open and warns", async () => {
    const s = await setupPostgres();
    await s.panel.dispatchMessage({
      type: "unsavedChangesState",
      payload: { hasUnsavedChanges: true },
    });
    s.disconnect();
    expect(s.onDispose).not.toHaveBeenCalled();
    expect(state.showWarningMessage).toHaveBeenCalledWith(
      expect.stringContaining("unsaved changes"),
    );

    await s.panel.dispatchMessage({
      type: "unsavedChangesState",
      payload: { hasUnsavedChanges: false },
    });
    s.disconnect();
    expect(s.onDispose).toHaveBeenCalledOnce();
  });
});

describe("PNL-4 clipboard write acknowledgement", () => {
  it("acknowledges a tracked clipboard write with its outcome", async () => {
    const s = await setupPostgres();
    const request = { requestId: "cut-1", recipient: "monaco:test" };
    await s.panel.dispatchMessage({
      type: "writeClipboard",
      payload: { text: "select 1", ...request },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toEqual({
        type: "clipboardWritten",
        payload: { ...request, success: true },
      }),
    );
    expect(state.writeText).toHaveBeenCalledWith("select 1");

    state.writeText.mockRejectedValueOnce(new Error("denied"));
    await s.panel.dispatchMessage({
      type: "writeClipboard",
      payload: { text: "select 2", ...request, requestId: "cut-2" },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toEqual({
        type: "clipboardWritten",
        payload: { ...request, requestId: "cut-2", success: false },
      }),
    );
  });
});

describe("PNL-7 unrelated schema refresh during mutation preparation", () => {
  async function prepareDeleteAcrossRefresh(
    refreshedColumns: ColumnTypeMeta[],
  ) {
    const s = await setupPostgres();
    let resolvePlan!: (plan: PreparedDeletePlan) => void;
    const prepare = vi
      .spyOn(TableDataService.prototype, "prepareDeleteRowsPlan")
      .mockImplementation(
        () =>
          new Promise((resolve) => {
            resolvePlan = resolve;
          }),
      );
    const deleting = s.panel.dispatchMessage({
      type: "deleteRows",
      payload: { operationId: "del-1", primaryKeysList: [{ id: 1 }] },
    });
    await vi.waitFor(() => expect(prepare).toHaveBeenCalledOnce());
    s.describeColumns.mockResolvedValue(refreshedColumns);
    s.refreshSchemas();
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toMatchObject({
        type: "tableInit",
        payload: { intent: "metadataRefresh" },
      }),
    );
    resolvePlan(deletePlan());
    await deleting;
    await vi.waitFor(() =>
      expect(
        s.panel
          .hostMessages()
          .some(
            (message) =>
              (message.type === "tableMutationPreview" ||
                message.type === "deleteResult") &&
              (message.payload as { operationId?: string }).operationId ===
                "del-1",
          ),
      ).toBe(true),
    );
    return s;
  }

  it("presents the delete preview when the refreshed columns are unchanged", async () => {
    const s = await prepareDeleteAcrossRefresh(structuredClone(pgColumns));
    const messages = s.panel.hostMessages();
    expect(
      messages.filter((message) => message.type === "deleteResult"),
    ).toEqual([]);
    expect(messages.at(-1)).toMatchObject({
      type: "tableMutationPreview",
      payload: { operationId: "del-1", kind: "deleteRows" },
    });
  });

  it("still reports a schema conflict when the refreshed columns differ", async () => {
    const s = await prepareDeleteAcrossRefresh([
      pgColumns[0],
      column("renamed"),
    ]);
    expect(s.panel.hostMessages().at(-1)).toMatchObject({
      type: "deleteResult",
      payload: {
        operationId: "del-1",
        success: false,
        error: expect.stringContaining("Schema metadata changed"),
      },
    });
  });
});

describe("AUD-028 table panel applies per-row Mongo type hints before prevalidation", () => {
  it("previews a non-hex string edit for a row whose field is a string", async () => {
    const config = { id: "mongo-audit", name: "Mongo", type: "mongodb" };
    const driver = new MongoDBDriver(config as never);
    Object.assign(driver, { connected: true });
    vi.spyOn(driver, "describeColumns").mockResolvedValue([
      column("_id", {
        nativeType: "string",
        isPrimaryKey: true,
        nullable: false,
      }),
      column("payload", {
        nativeType: "binData",
        type: "binData",
        category: "binary",
      }),
    ]);
    const s = await setup(driver, config);
    await s.panel.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "apply-1",
        updates: [
          {
            primaryKeys: {
              _id: { $rapidbMongoId: { type: "string", value: "row-1" } },
            },
            changes: { payload: "0xnothex" },
            originalTypes: {
              payload: { category: "text", nativeType: "string" },
            },
          },
        ],
      },
    });
    await vi.waitFor(() =>
      expect(
        s.panel
          .hostMessages()
          .some((message) =>
            ["tableMutationPreview", "applyResult"].includes(message.type),
          ),
      ).toBe(true),
    );
    expect(s.panel.hostMessages().at(-1)).toMatchObject({
      type: "tableMutationPreview",
      payload: { operationId: "apply-1", kind: "applyChanges" },
    });
  });
});
