import { describe, expect, it, vi } from "vitest";
import { col } from "./helpers";

const vscodeMocks = vi.hoisted(() => ({
  createWebviewPanel: vi.fn(),
  onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
}));

vi.mock("vscode", () => ({
  ViewColumn: { One: 1 },
  window: {
    createWebviewPanel: vscodeMocks.createWebviewPanel,
  },
  workspace: {
    onDidChangeConfiguration: vscodeMocks.onDidChangeConfiguration,
  },
}));

vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

import { SchemaPanel } from "../../src/extension/panels/schemaPanel";

describe("SchemaPanel", () => {
  it("sends enriched columns from describeColumns on ready", async () => {
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

    const columns = [
      col({
        name: "created_at",
        type: "datetime(6)",
        category: "datetime",
        nativeType: "datetime(6)",
        filterable: true,
        editable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
      }),
    ];
    const driver = {
      describeColumns: vi.fn().mockResolvedValue(columns),
      describeTable: vi.fn(),
      getIndexes: vi.fn().mockResolvedValue([]),
      getForeignKeys: vi.fn().mockResolvedValue([]),
    };
    const cm = {
      getConnection: vi.fn().mockReturnValue({ name: "Local" }),
      getDriver: vi.fn().mockReturnValue(driver),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    };

    SchemaPanel.createOrShow(
      {} as never,
      cm as never,
      "conn-1",
      "appdb",
      "public",
      "users",
    );

    await onMessage?.({ type: "ready" });

    expect(driver.describeColumns).toHaveBeenCalledWith(
      "appdb",
      "public",
      "users",
    );
    expect(driver.describeTable).not.toHaveBeenCalled();
    expect(postMessage).toHaveBeenCalledWith({
      type: "schemaData",
      payload: { columns, indexes: [], foreignKeys: [] },
    });

    SchemaPanel.disposeAll();
  });
});
