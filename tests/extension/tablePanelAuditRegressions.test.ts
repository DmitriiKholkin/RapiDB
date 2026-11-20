import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DeleteExecutionError } from "../../src/extension/dbDrivers/deleteOutcomes";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";
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
  config: { id: string; name: string; type: string; password?: string },
  skipPreview = false,
) {
  let connected = true;
  let generation = 0;
  let removed = false;
  const disconnectListeners = new Set<(id: string) => void>();
  const invalidateListeners = new Set<(id: string) => void>();
  const sessionListeners = new Set<(id: string) => void>();
  const connectListeners = new Set<() => void>();
  const refreshListeners = new Set<(id?: string) => void>();
  const manager = {
    getDriver: () => driver,
    getConnection: () => (removed ? undefined : config),
    getConnectionDiagnosticContext: () => config,
    getConnectionSessionGeneration: () => generation,
    getDefaultPageSize: () => 25,
    getSkipTableMutationPreview: () => skipPreview,
    getQueryEditorPresentation: () => undefined,
    isConnected: () => connected,
    onDidInvalidateConnectionMetadata: (listener: (id: string) => void) => {
      invalidateListeners.add(listener);
      return { dispose: () => invalidateListeners.delete(listener) };
    },
    onDidChangeConnectionSession: (listener: (id: string) => void) => {
      sessionListeners.add(listener);
      return { dispose: () => sessionListeners.delete(listener) };
    },
    onDidConnect: (listener: () => void) => {
      connectListeners.add(listener);
      return { dispose: () => connectListeners.delete(listener) };
    },
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
      connected = false;
      for (const listener of invalidateListeners) listener(config.id);
      for (const listener of disconnectListeners) listener(config.id);
    },
    reconnect: () => {
      connected = true;
      for (const listener of invalidateListeners) listener(config.id);
      for (const listener of connectListeners) listener();
    },
    changeTarget: (remove: boolean) => {
      removed = remove;
      generation++;
      for (const listener of sessionListeners) listener(config.id);
    },
    refreshSchemas: () => {
      for (const listener of refreshListeners) listener(config.id);
    },
  };
}

async function setupPostgres(
  columns = pgColumns,
  password?: string,
  skipPreview = false,
) {
  const config = { id: "pg-audit", name: "PG", type: "pg", password };
  const driver = new PostgresDriver(config as never);
  const describeColumns = vi
    .spyOn(driver, "describeColumns")
    .mockResolvedValue(columns);
  return { ...(await setup(driver, config, skipPreview)), describeColumns };
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

it.each([
  "conflict",
  "other no-write error",
  "partial",
  "unknown",
] as const)("only signals confirmed no-write conflicts in an Apply reply (%s)", async (kind) => {
  const config = { id: "mongo-m6", name: "MongoDB", type: "mongodb" };
  const driver = new MongoDBDriver(config as never);
  Object.assign(driver, { connected: true, supportsAtomicUpdateRows: true });
  const columns = [
    column("_id", {
      nativeType: "string",
      isPrimaryKey: true,
      nullable: false,
    }),
    column("name", { nativeType: "string" }),
  ];
  vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
  const update = vi.spyOn(driver, "updateRows");
  const evidence = {
    affectedRows: kind === "partial" ? 1 : 0,
    updateRowOutcomes: [
      {
        rowIndex: 0,
        status: "not_applied" as const,
        ...(kind !== "other no-write error" && { conflict: true as const }),
      },
      ...(kind === "partial"
        ? [{ rowIndex: 1, status: "applied" as const }]
        : []),
      ...(kind === "unknown"
        ? [{ rowIndex: 1, status: "unknown" as const }]
        : []),
    ],
  };
  if (kind === "other no-write error") {
    update.mockRejectedValue(
      new DriverUpdateRowsError("Refresh the table and retry.", evidence),
    );
  } else {
    update.mockResolvedValue(evidence);
  }
  const { panel } = await setup(driver, config, true);
  const operationId = "m6-apply";
  await panel.dispatchMessage({
    type: "applyChanges",
    payload: {
      operationId,
      updates: [
        mongoEdit("skip", {}),
        mongoEdit("one", { name: "draft" }),
        ...(kind === "partial" || kind === "unknown"
          ? [mongoEdit("two", { name: "draft" })]
          : []),
      ],
    },
  });
  await vi.waitFor(() =>
    expect(panel.hostMessages().at(-1)?.type).toBe("applyResult"),
  );
  expect(
    update,
    JSON.stringify(panel.hostMessages().at(-1)?.payload),
  ).toHaveBeenCalledOnce();
  const result = panel.hostMessages().at(-1);
  expect(result).toMatchObject({
    type: "applyResult",
    payload: {
      operationId,
      success: false,
      changesPossible: kind === "partial" || kind === "unknown",
      outcomeUnknown: kind === "unknown",
    },
  });
  if (kind === "conflict")
    expect(result?.payload).toHaveProperty("conflictRows", [1]);
  else expect(result?.payload).not.toHaveProperty("conflictRows");
});

function mongoEdit(id: string, changes: Record<string, unknown>) {
  return {
    primaryKeys: { _id: { $rapidbMongoId: { type: "string", value: id } } },
    changes,
    originalValues: { name: "old" },
  };
}

it("starts a fresh conflict page read without changing the host's pinned target", async () => {
  const config = { id: "mongo-m6-read", name: "MongoDB", type: "mongodb" };
  const driver = new MongoDBDriver(config as never);
  const columns = [column("_id", { isPrimaryKey: true }), column("name")];
  vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
  const pageRead = vi.spyOn(driver, "readTablePage");
  let release = () => {};
  pageRead
    .mockImplementationOnce(async () => {
      await new Promise<void>((resolve) => {
        release = resolve;
      });
      return { columns, rows: [{ _id: "one", name: "old" }], totalCount: 1 };
    })
    .mockResolvedValue({
      columns,
      rows: [{ _id: "one", name: "current" }],
      totalCount: 1,
    });
  const { panel } = await setup(driver, config);
  const oldRead = panel.dispatchMessage({
    type: "fetchPage",
    payload: { fetchId: 1, page: 2, pageSize: 25 },
  });
  try {
    await vi.waitFor(() => expect(pageRead).toHaveBeenCalledOnce());
    await panel.dispatchMessage({
      type: "fetchPage",
      payload: {
        fetchId: 2,
        page: 2,
        pageSize: 25,
        conflictRefresh: true,
        table: "not-the-pinned-table",
      },
    });
    await vi.waitFor(() => expect(pageRead).toHaveBeenCalledTimes(2));
    await vi.waitFor(() =>
      expect(panel.hostMessages().at(-1)?.type).toBe("tableData"),
    );
    expect(pageRead).toHaveBeenLastCalledWith(
      expect.objectContaining({
        database: "db",
        schema: "public",
        table: "items",
        page: 2,
        pageSize: 25,
      }),
    );
    expect(panel.hostMessages().at(-1)).toMatchObject({
      type: "tableData",
      payload: { fetchId: 2, rows: [{ name: "current" }] },
    });
  } finally {
    release();
    await oldRead;
  }
});

it("redacts a confirmed delete's diagnostics without changing reconciliation evidence", async () => {
  const password = "synthetic-delete-password-11";
  const s = await setupPostgres(pgColumns, password);
  const primaryKeys = { id: password };
  const plan = {
    ...deletePlan(),
    rowIdentities: [primaryKeys],
    verificationCriteriaList: [primaryKeys],
  };
  const evidence = {
    success: false,
    error: `Delete failed: ${password}`,
    affectedRows: 0,
    rowOutcomes: [
      {
        rowIndex: 0,
        primaryKeys,
        success: false,
        status: "unknown" as const,
        message: `Remote rejected ${password}`,
      },
    ],
    changesPossible: true,
    outcomeUnknown: true,
  };
  vi.spyOn(
    TableDataService.prototype,
    "prepareDeleteRowsPlan",
  ).mockResolvedValue(plan);
  vi.spyOn(
    TableDataService.prototype,
    "executePreparedDeletePlan",
  ).mockRejectedValue(new DeleteExecutionError(evidence));

  const operationId = "del-redaction";
  await s.panel.dispatchMessage({
    type: "deleteRows",
    payload: { operationId, primaryKeysList: [primaryKeys] },
  });
  await vi.waitFor(() =>
    expect(s.panel.hostMessages().at(-1)?.type).toBe("tableMutationPreview"),
  );
  const { previewToken } = s.panel.hostMessages().at(-1)?.payload as {
    previewToken: string;
  };
  await s.panel.dispatchMessage({
    type: "confirmMutationPreview",
    payload: { operationId, previewToken },
  });

  await vi.waitFor(() =>
    expect(s.panel.hostMessages().at(-1)?.type).toBe("deleteResult"),
  );
  expect(s.panel.hostMessages().at(-1)).toEqual({
    type: "deleteResult",
    payload: {
      ...evidence,
      operationId,
      error: "Delete failed: [REDACTED]",
      rowOutcomes: [
        { ...evidence.rowOutcomes[0], message: "Remote rejected [REDACTED]" },
      ],
    },
  });
  expect(evidence.error).toContain(password);
});

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

describe("M4 clean table delete recovery", () => {
  const operationId = "delete-without-edits";
  const request = {
    type: "deleteRows",
    payload: { operationId, primaryKeysList: [{ id: 1 }] },
  };
  const resolveRecovery = {
    type: "deleteRecoveryResolved",
    payload: { operationId },
  };
  const knownFailure = {
    success: false,
    affectedRows: 0,
    rowOutcomes: [],
    changesPossible: false,
    outcomeUnknown: false,
  };
  const unknownFailure = {
    ...knownFailure,
    changesPossible: true,
    outcomeUnknown: true,
    error: "Transport closed before acknowledgement",
  };

  function prepare() {
    vi.spyOn(
      TableDataService.prototype,
      "prepareDeleteRowsPlan",
    ).mockResolvedValue(deletePlan());
  }

  async function requestPreview(s: Awaited<ReturnType<typeof setupPostgres>>) {
    await s.panel.dispatchMessage(request);
    return vi.waitFor(() => {
      const preview = s.panel.hostMessages().at(-1);
      expect(preview?.type).toBe("tableMutationPreview");
      return preview?.payload;
    });
  }

  it.each([
    false,
    true,
  ])("disposes clean unexecuted previews (cancelled=%s)", async (cancelled) => {
    const s = await setupPostgres();
    prepare();
    const execute = vi.spyOn(
      TableDataService.prototype,
      "executePreparedDeletePlan",
    );
    const preview = await requestPreview(s);
    if (cancelled) {
      await s.panel.dispatchMessage({
        type: "cancelMutationPreview",
        payload: preview,
      });
    }
    s.disconnect();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(execute).not.toHaveBeenCalled();
    expect(state.showWarningMessage).not.toHaveBeenCalled();
  });

  const lateOutcomes = [
    { outcome: "success", skipPreview: false },
    { outcome: "not executed", skipPreview: true },
    { outcome: "unknown", skipPreview: false },
  ] as const;
  it.each(lateOutcomes)("keeps clean viewer for late $outcome", async ({
    outcome,
    skipPreview,
  }) => {
    const s = await setupPostgres(pgColumns, undefined, skipPreview);
    prepare();
    let resolveDelete!: () => void;
    let rejectDelete!: (error: Error) => void;
    const settling = new Promise<void>((resolve, reject) => {
      resolveDelete = resolve;
      rejectDelete = reject;
    });
    const execute = vi
      .spyOn(TableDataService.prototype, "executePreparedDeletePlan")
      .mockImplementation(() => {
        // Synchronous interruption at driver entry, with no UI dirty flag.
        s.disconnect();
        return settling;
      });

    const dispatch = skipPreview
      ? s.panel.dispatchMessage(request)
      : s.panel.dispatchMessage({
          type: "confirmMutationPreview",
          payload: await requestPreview(s),
        });
    await vi.waitFor(() => expect(execute).toHaveBeenCalledOnce());
    expect(s.onDispose).not.toHaveBeenCalled();
    expect(state.showWarningMessage).toHaveBeenCalledWith(
      expect.stringContaining("awaiting verification"),
    );
    s.reconnect();
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toMatchObject({
        type: "tableInit",
        payload: { intent: "connectionRefresh" },
      }),
    );
    await s.panel.dispatchMessage(resolveRecovery);
    const read = vi.spyOn(TableDataService.prototype, "getPage");
    await s.panel.dispatchMessage({
      type: "fetchPage",
      payload: { fetchId: 42, page: 1 },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toMatchObject({
        type: "tableError",
        payload: { fetchId: 42, error: expect.stringContaining("in progress") },
      }),
    );
    expect(read).not.toHaveBeenCalled();

    if (outcome === "success") resolveDelete();
    else {
      rejectDelete(
        new DeleteExecutionError(
          outcome === "unknown" ? unknownFailure : knownFailure,
        ),
      );
    }
    await dispatch;
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toMatchObject({
        type: "deleteResult",
        payload: {
          operationId,
          success: outcome === "success",
          changesPossible: outcome !== "not executed",
          outcomeUnknown: outcome === "unknown",
        },
      }),
    );
    s.disconnect();
    if (outcome === "not executed") {
      expect(s.onDispose).toHaveBeenCalledOnce();
    } else {
      expect(s.onDispose).not.toHaveBeenCalled();
      await s.panel.dispatchMessage({
        type: "deleteRecoveryResolved",
        payload: { operationId: "stale-operation" },
      });
      s.disconnect();
      expect(s.onDispose).not.toHaveBeenCalled();
      await s.panel.dispatchMessage(resolveRecovery);
      s.disconnect();
      expect(s.onDispose).toHaveBeenCalledOnce();
    }
  });

  it.each([
    false,
    true,
  ])("discards old recovery on target change (removed=%s)", async (removed) => {
    const s = await setupPostgres();
    prepare();
    vi.spyOn(
      TableDataService.prototype,
      "executePreparedDeletePlan",
    ).mockRejectedValue(new DeleteExecutionError(unknownFailure));
    await s.panel.dispatchMessage({
      type: "confirmMutationPreview",
      payload: await requestPreview(s),
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toMatchObject({
        type: "deleteResult",
        payload: { operationId, outcomeUnknown: true },
      }),
    );
    s.changeTarget(removed);
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(state.showWarningMessage).toHaveBeenCalledWith(
      expect.stringContaining("verify it there"),
    );
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

  it("reports a schema conflict before confirm as a not-executed delete", async () => {
    const s = await setupPostgres();
    vi.spyOn(
      TableDataService.prototype,
      "prepareDeleteRowsPlan",
    ).mockResolvedValue(deletePlan());
    const execute = vi.spyOn(
      TableDataService.prototype,
      "executePreparedDeletePlan",
    );
    await s.panel.dispatchMessage({
      type: "deleteRows",
      payload: { operationId: "del-1", primaryKeysList: [{ id: 1 }] },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)?.type).toBe("tableMutationPreview"),
    );
    const { previewToken } = s.panel.hostMessages().at(-1)?.payload as {
      previewToken: string;
    };
    s.describeColumns.mockResolvedValue([pgColumns[0], column("renamed")]);
    s.refreshSchemas();
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)).toMatchObject({
        type: "tableInit",
        payload: { intent: "metadataRefresh" },
      }),
    );
    await s.panel.dispatchMessage({
      type: "confirmMutationPreview",
      payload: { operationId: "del-1", previewToken },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)?.type).toBe("deleteResult"),
    );
    expect(execute).not.toHaveBeenCalled();
    expect(s.panel.hostMessages().at(-1)).toEqual({
      type: "deleteResult",
      payload: {
        operationId: "del-1",
        success: false,
        error: expect.stringContaining("Schema metadata changed"),
        affectedRows: 0,
        rowOutcomes: [],
        changesPossible: false,
        outcomeUnknown: false,
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
