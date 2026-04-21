import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  coerceFilterExpressions,
  NULL_SENTINEL,
} from "../../src/shared/tableTypes";

const vscodeMocks = vi.hoisted(() => ({
  createWebviewPanel: vi.fn(),
  onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
  showWarningMessage: vi.fn(),
}));

const tableDataServiceMocks = vi.hoisted(() => ({
  executePreparedApplyPlan: vi.fn(),
  prepareApplyChangesPlan: vi.fn(),
  instances: [] as Array<{
    clearForConnection: ReturnType<typeof vi.fn>;
    getColumns: ReturnType<typeof vi.fn>;
    getPage: ReturnType<typeof vi.fn>;
    prepareInsertRow: ReturnType<typeof vi.fn>;
    executePreparedInsertPlan: ReturnType<typeof vi.fn>;
    deleteRows: ReturnType<typeof vi.fn>;
  }>,
  TableDataService: class {
    clearForConnection = vi.fn();
    getColumns = vi.fn();
    getPage = vi.fn();
    prepareInsertRow = vi.fn();
    executePreparedInsertPlan = vi.fn();
    deleteRows = vi.fn();

    constructor() {
      tableDataServiceMocks.instances.push(this);
    }
  },
}));

const previewMocks = vi.hoisted(() => ({
  formatMutationPreviewSql: vi.fn((statements: string[]) =>
    statements.join("\n\n"),
  ),
}));

vi.mock("vscode", () => ({
  ProgressLocation: { Notification: 1 },
  Uri: {
    file: vi.fn(),
    joinPath: vi.fn(),
  },
  ViewColumn: { One: 1 },
  window: {
    createWebviewPanel: vscodeMocks.createWebviewPanel,
    showWarningMessage: vscodeMocks.showWarningMessage,
  },
  workspace: {
    onDidChangeConfiguration: vscodeMocks.onDidChangeConfiguration,
  },
}));

vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

vi.mock("../../src/extension/tableDataService", () => ({
  TableDataService: tableDataServiceMocks.TableDataService,
  executePreparedApplyPlan: tableDataServiceMocks.executePreparedApplyPlan,
  prepareApplyChangesPlan: tableDataServiceMocks.prepareApplyChangesPlan,
}));

vi.mock("../../src/extension/utils/mutationPreview", () => ({
  formatMutationPreviewSql: previewMocks.formatMutationPreviewSql,
}));

import { TablePanel } from "../../src/extension/panels/tablePanel";

describe("tablePanel structured filter coercion", () => {
  it("keeps structured filter payloads unchanged", () => {
    const filters = coerceFilterExpressions([
      { column: "name", operator: "like", value: "alice" },
    ]);

    expect(filters).toEqual([
      { column: "name", operator: "like", value: "alice" },
    ]);
  });

  it("accepts structured NULL filters", () => {
    const filters = coerceFilterExpressions([
      { column: "name", operator: "is_null" },
    ]);

    expect(filters).toEqual([{ column: "name", operator: "is_null" }]);
  });

  it("drops legacy value-only payloads instead of inferring operators", () => {
    const filters = coerceFilterExpressions([
      { column: "created_on", value: "2026-04-15" },
      { column: "name", value: NULL_SENTINEL },
    ]);

    expect(filters).toEqual([]);
  });

  it("drops malformed between payloads", () => {
    const filters = coerceFilterExpressions([
      { column: "created_on", operator: "between", value: ["2026-04-15"] },
    ]);

    expect(filters).toEqual([]);
  });
});

describe("TablePanel", () => {
  beforeEach(() => {
    vscodeMocks.createWebviewPanel.mockReset();
    vscodeMocks.onDidChangeConfiguration.mockClear();
    vscodeMocks.showWarningMessage.mockReset();
    tableDataServiceMocks.executePreparedApplyPlan.mockReset();
    tableDataServiceMocks.prepareApplyChangesPlan.mockReset();
    tableDataServiceMocks.instances.length = 0;
    previewMocks.formatMutationPreviewSql.mockClear();
  });

  afterEach(() => {
    TablePanel.disposeAll();
  });

  it("shows a warning message and forwards immediate applyResult payloads", async () => {
    let onMessage:
      | ((message: { type: string; payload?: unknown }) => Promise<void>)
      | undefined;
    const postMessage = vi.fn();

    vscodeMocks.createWebviewPanel.mockReturnValue({
      webview: {
        html: "",
        postMessage,
        onDidReceiveMessage: vi.fn((handler) => {
          onMessage = handler;
          return { dispose: vi.fn() };
        }),
      },
      onDidDispose: vi.fn(() => ({ dispose: vi.fn() })),
      reveal: vi.fn(),
      dispose: vi.fn(),
      title: "",
    });

    tableDataServiceMocks.prepareApplyChangesPlan.mockReturnValue({
      executable: false,
      result: {
        success: true,
        warning: "Some edits were written but could not be confirmed exactly.",
        failedRows: [0],
        rowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "verification_failed",
            message: "Rounded by the database.",
          },
        ],
      },
    });

    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local", type: "pg" }),
      getDefaultPageSize: vi.fn().mockReturnValue(25),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    };

    TablePanel.createOrShow(
      { extensionUri: { path: "/extension" } } as never,
      cm as never,
      "conn-1",
      "appdb",
      "public",
      "users",
    );

    await onMessage?.({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { amount: "1234.52" },
          },
        ],
      },
    });

    expect(tableDataServiceMocks.prepareApplyChangesPlan).toHaveBeenCalledWith(
      cm,
      "conn-1",
      "appdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.52" },
        },
      ],
      [],
    );
    expect(vscodeMocks.showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] Some edits were written but could not be confirmed exactly.",
    );
    expect(postMessage).toHaveBeenCalledWith({
      type: "applyResult",
      payload: {
        success: true,
        warning: "Some edits were written but could not be confirmed exactly.",
        failedRows: [0],
        rowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "verification_failed",
            message: "Rounded by the database.",
          },
        ],
      },
    });
  });

  it("posts a SQL preview to the webview and executes prepared edits only after confirm", async () => {
    let onMessage:
      | ((message: { type: string; payload?: unknown }) => Promise<void>)
      | undefined;
    const postMessage = vi.fn();

    vscodeMocks.createWebviewPanel.mockReturnValue({
      webview: {
        html: "",
        postMessage,
        onDidReceiveMessage: vi.fn((handler) => {
          onMessage = handler;
          return { dispose: vi.fn() };
        }),
      },
      onDidDispose: vi.fn(() => ({ dispose: vi.fn() })),
      reveal: vi.fn(),
      dispose: vi.fn(),
      title: "",
    });

    tableDataServiceMocks.prepareApplyChangesPlan.mockReturnValue({
      executable: true,
      plan: {
        connectionId: "conn-1",
        database: "appdb",
        schema: "public",
        table: "users",
        cols: [],
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Alice" },
          },
        ],
        operations: [
          {
            sql: 'UPDATE "public"."users" SET "name" = ? WHERE "id" = ?',
            params: ["Alice", 1],
            checkAffectedRows: true,
          },
        ],
        previewStatements: [
          'UPDATE "public"."users" SET "name" = ? WHERE "id" = ?',
        ],
        skippedRows: [],
        verificationTargets: [],
      },
    });
    tableDataServiceMocks.executePreparedApplyPlan.mockResolvedValue({
      success: true,
      rowOutcomes: [{ rowIndex: 0, success: true, status: "applied" }],
    });

    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local", type: "pg" }),
      getDefaultPageSize: vi.fn().mockReturnValue(25),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    };

    TablePanel.createOrShow(
      { extensionUri: { path: "/extension" } } as never,
      cm as never,
      "conn-1",
      "appdb",
      "public",
      "users",
    );

    await onMessage?.({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Alice" },
          },
        ],
      },
    });

    const previewMessage = postMessage.mock.calls
      .map(([message]) => message)
      .find((message) => message.type === "tableMutationPreview");

    expect(previewMessage).toBeDefined();
    expect(previewMessage.payload).toMatchObject({
      kind: "applyChanges",
      title: "Apply changes to users",
      sql: 'UPDATE "public"."users" SET "name" = ? WHERE "id" = ?',
      statementCount: 1,
    });
    expect(
      tableDataServiceMocks.executePreparedApplyPlan,
    ).not.toHaveBeenCalled();

    await onMessage?.({
      type: "confirmMutationPreview",
      payload: {
        previewToken: previewMessage.payload.previewToken,
      },
    });

    expect(
      tableDataServiceMocks.executePreparedApplyPlan,
    ).toHaveBeenCalledTimes(1);
    expect(postMessage).toHaveBeenCalledWith({
      type: "applyResult",
      payload: {
        success: true,
        rowOutcomes: [{ rowIndex: 0, success: true, status: "applied" }],
      },
    });
  });

  it("posts insert SQL preview and executes the prepared insert only after confirm", async () => {
    let onMessage:
      | ((message: { type: string; payload?: unknown }) => Promise<void>)
      | undefined;
    const postMessage = vi.fn();

    vscodeMocks.createWebviewPanel.mockReturnValue({
      webview: {
        html: "",
        postMessage,
        onDidReceiveMessage: vi.fn((handler) => {
          onMessage = handler;
          return { dispose: vi.fn() };
        }),
      },
      onDidDispose: vi.fn(() => ({ dispose: vi.fn() })),
      reveal: vi.fn(),
      dispose: vi.fn(),
      title: "",
    });

    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local", type: "pg" }),
      getDefaultPageSize: vi.fn().mockReturnValue(25),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    };

    TablePanel.createOrShow(
      { extensionUri: { path: "/extension" } } as never,
      cm as never,
      "conn-1",
      "appdb",
      "public",
      "users",
    );

    const svc = tableDataServiceMocks.instances[0];
    expect(svc).toBeDefined();

    const plan = {
      connectionId: "conn-1",
      operation: {
        sql: 'INSERT INTO "public"."users" ("name") VALUES (?)',
        params: ["Charlie"],
      },
      previewStatements: ['INSERT INTO "public"."users" ("name") VALUES (?)'],
    };

    svc?.prepareInsertRow.mockResolvedValue(plan);
    svc?.executePreparedInsertPlan.mockResolvedValue(undefined);

    await onMessage?.({
      type: "insertRow",
      payload: {
        values: { name: "Charlie" },
      },
    });

    expect(svc?.prepareInsertRow).toHaveBeenCalledWith(
      "conn-1",
      "appdb",
      "public",
      "users",
      { name: "Charlie" },
    );
    expect(previewMocks.formatMutationPreviewSql).toHaveBeenCalledWith(
      ['INSERT INTO "public"."users" ("name") VALUES (?)'],
      "pg",
    );

    const previewMessage = postMessage.mock.calls
      .map(([message]) => message)
      .find((message) => message.type === "tableMutationPreview");

    expect(previewMessage).toBeDefined();
    expect(previewMessage.payload).toMatchObject({
      kind: "insertRow",
      title: "Insert row into users",
      sql: 'INSERT INTO "public"."users" ("name") VALUES (?)',
      statementCount: 1,
    });
    expect(svc?.executePreparedInsertPlan).not.toHaveBeenCalled();

    await onMessage?.({
      type: "confirmMutationPreview",
      payload: {
        previewToken: previewMessage.payload.previewToken,
      },
    });

    expect(svc?.executePreparedInsertPlan).toHaveBeenCalledTimes(1);
    expect(svc?.executePreparedInsertPlan).toHaveBeenCalledWith(plan);
    expect(postMessage).toHaveBeenCalledWith({
      type: "insertResult",
      payload: { success: true },
    });
  });

  it("drops a pending insert preview after cancel so later confirms do nothing", async () => {
    let onMessage:
      | ((message: { type: string; payload?: unknown }) => Promise<void>)
      | undefined;
    const postMessage = vi.fn();

    vscodeMocks.createWebviewPanel.mockReturnValue({
      webview: {
        html: "",
        postMessage,
        onDidReceiveMessage: vi.fn((handler) => {
          onMessage = handler;
          return { dispose: vi.fn() };
        }),
      },
      onDidDispose: vi.fn(() => ({ dispose: vi.fn() })),
      reveal: vi.fn(),
      dispose: vi.fn(),
      title: "",
    });

    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local", type: "pg" }),
      getDefaultPageSize: vi.fn().mockReturnValue(25),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    };

    TablePanel.createOrShow(
      { extensionUri: { path: "/extension" } } as never,
      cm as never,
      "conn-1",
      "appdb",
      "public",
      "users",
    );

    const svc = tableDataServiceMocks.instances[0];
    expect(svc).toBeDefined();

    svc?.prepareInsertRow.mockResolvedValue({
      connectionId: "conn-1",
      operation: {
        sql: 'INSERT INTO "public"."users" ("name") VALUES (?)',
        params: ["Charlie"],
      },
      previewStatements: ['INSERT INTO "public"."users" ("name") VALUES (?)'],
    });

    await onMessage?.({
      type: "insertRow",
      payload: {
        values: { name: "Charlie" },
      },
    });

    const previewMessage = postMessage.mock.calls
      .map(([message]) => message)
      .find((message) => message.type === "tableMutationPreview");

    expect(previewMessage).toBeDefined();

    await onMessage?.({
      type: "cancelMutationPreview",
      payload: { previewToken: previewMessage.payload.previewToken },
    });

    await onMessage?.({
      type: "confirmMutationPreview",
      payload: { previewToken: previewMessage.payload.previewToken },
    });

    expect(svc?.executePreparedInsertPlan).not.toHaveBeenCalled();
    expect(
      postMessage.mock.calls
        .map(([message]) => message)
        .some((message) => message.type === "insertResult"),
    ).toBe(false);
  });
});
