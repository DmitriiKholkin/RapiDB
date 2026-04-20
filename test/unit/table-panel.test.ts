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
  applyChangesTransactional: vi.fn(),
  TableDataService: class {
    clearForConnection = vi.fn();
    getColumns = vi.fn();
    getPage = vi.fn();
    insertRow = vi.fn();
    deleteRows = vi.fn();
  },
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
  applyChangesTransactional: tableDataServiceMocks.applyChangesTransactional,
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
    tableDataServiceMocks.applyChangesTransactional.mockReset();
  });

  afterEach(() => {
    TablePanel.disposeAll();
  });

  it("shows a warning message and forwards applyResult payload when apply returns a warning", async () => {
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

    tableDataServiceMocks.applyChangesTransactional.mockResolvedValue({
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
    });

    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local" }),
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

    expect(
      tableDataServiceMocks.applyChangesTransactional,
    ).toHaveBeenCalledWith(
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
});
