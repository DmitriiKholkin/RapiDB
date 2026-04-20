import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const vscodeMocks = vi.hoisted(() => ({
  createWebviewPanel: vi.fn(),
  onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
  showWarningMessage: vi.fn(),
  showErrorMessage: vi.fn(),
  readClipboardText: vi.fn().mockResolvedValue(""),
}));

vi.mock("vscode", () => ({
  ViewColumn: { One: 1 },
  window: {
    createWebviewPanel: vscodeMocks.createWebviewPanel,
    showWarningMessage: vscodeMocks.showWarningMessage,
    showErrorMessage: vscodeMocks.showErrorMessage,
  },
  workspace: {
    onDidChangeConfiguration: vscodeMocks.onDidChangeConfiguration,
  },
  env: {
    clipboard: {
      readText: vscodeMocks.readClipboardText,
    },
  },
}));

vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

import {
  isLikelyUnboundedResultQuery,
  QueryPanel,
} from "../../src/extension/panels/queryPanel";

describe("QueryPanel", () => {
  beforeEach(() => {
    vscodeMocks.createWebviewPanel.mockReset();
    vscodeMocks.onDidChangeConfiguration.mockClear();
    vscodeMocks.showWarningMessage.mockReset();
    vscodeMocks.showErrorMessage.mockReset();
  });

  afterEach(() => {
    QueryPanel.disposeAll();
  });

  it("detects likely unbounded result queries conservatively", () => {
    expect(isLikelyUnboundedResultQuery("select * from users")).toBe(true);
    expect(isLikelyUnboundedResultQuery("SELECT * FROM users LIMIT 50")).toBe(
      false,
    );
    expect(isLikelyUnboundedResultQuery("select count(*) from users")).toBe(
      false,
    );
    expect(
      isLikelyUnboundedResultQuery(
        "with recent as (select * from users) select * from recent",
      ),
    ).toBe(true);
  });

  it("cancels suspicious unbounded queries unless the user confirms", async () => {
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

    const driver = {
      query: vi.fn(),
    };
    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local", type: "pg" }),
      getConnections: vi.fn().mockReturnValue([]),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(),
      addToHistory: vi.fn(),
      getDriver: vi.fn().mockReturnValue(driver),
    };

    vscodeMocks.showWarningMessage.mockResolvedValue(undefined);

    QueryPanel.createOrShow(
      { extensionUri: { path: "/extension" } } as never,
      cm as never,
      "conn-1",
    );

    await onMessage?.({
      type: "executeQuery",
      payload: {
        sql: "select * from users",
        connectionId: "conn-1",
      },
    });

    expect(vscodeMocks.showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] This query looks unbounded and the extension currently fetches the full result set before truncating it. Continue anyway?",
      { modal: true },
      "Run Anyway",
    );
    expect(cm.addToHistory).not.toHaveBeenCalled();
    expect(driver.query).not.toHaveBeenCalled();
    expect(postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
        error: "Query execution cancelled.",
      },
    });
  });

  it("ignores malformed inbound messages before touching the database", async () => {
    let onMessage:
      | ((message: { type: string; payload?: unknown }) => Promise<void>)
      | undefined;

    vscodeMocks.createWebviewPanel.mockReturnValue({
      webview: {
        html: "",
        postMessage: vi.fn(),
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

    const driver = {
      query: vi.fn(),
    };
    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local", type: "pg" }),
      getConnections: vi.fn().mockReturnValue([]),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(),
      addToHistory: vi.fn(),
      getDriver: vi.fn().mockReturnValue(driver),
    };

    QueryPanel.createOrShow(
      { extensionUri: { path: "/extension" } } as never,
      cm as never,
      "conn-1",
    );

    await onMessage?.({
      type: "executeQuery",
      payload: { connectionId: "conn-1" },
    });

    expect(cm.addToHistory).not.toHaveBeenCalled();
    expect(driver.query).not.toHaveBeenCalled();
    expect(vscodeMocks.showWarningMessage).not.toHaveBeenCalled();
  });
});
