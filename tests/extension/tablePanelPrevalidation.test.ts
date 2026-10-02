import { afterEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import type { FakeWebviewPanelHandle } from "../workflow/bridge/fakeWebviewPanel";

const state = vi.hoisted(() => ({
  panel: null as FakeWebviewPanelHandle | null,
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
      showWarningMessage: vi.fn(),
      showErrorMessage: vi.fn(),
    },
    workspace: { onDidChangeConfiguration: () => ({ dispose() {} }) },
  };
});
vi.mock("../../src/extension/panels/webviewShell", () => ({
  APP_WEBVIEW_SHELL_LAYOUT: {},
  WEBVIEW_SCROLLBAR_STYLES: "",
  createWebviewShell: () => "<html></html>",
}));

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    nativeType: "integer",
    type: "integer",
    category: "integer",
    isPrimaryKey: true,
    nullable: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
  {
    name: "amount",
    nativeType: "numeric(4,2)",
    type: "numeric(4,2)",
    category: "decimal",
    isPrimaryKey: false,
    nullable: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];

afterEach(() => {
  TablePanel.disposeAll();
  vi.restoreAllMocks();
});

async function setup(skipPreview: boolean) {
  const config = {
    id: "panel-prevalidation",
    name: "Panel",
    type: "pg" as const,
  };
  const driver = new PostgresDriver(config);
  vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
  const query = vi.spyOn(driver, "query");
  const transaction = vi.spyOn(driver, "runTransaction");
  const preview = vi.spyOn(driver, "materializePreviewColumnSql");
  const insertPreview = vi.spyOn(driver, "materializePreviewSql");
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
    getDefaultPageSize: () => 25,
    getSkipTableMutationPreview: () => skipPreview,
    onDidDisconnect: () => ({ dispose() {} }),
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
  await panel.dispatchMessage({ type: "ready" });
  await vi.waitFor(() =>
    expect(panel.hostMessages().at(-1)?.type).toBe("tableInit"),
  );
  return { panel, query, transaction, preview, insertPreview };
}

describe("B01 real TablePanel and mutation services", () => {
  it.each([
    false,
    true,
  ])("blocks bad updates including staged good inserts (skip preview %s)", async (skipPreview) => {
    const s = await setup(skipPreview);
    await s.panel.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "bad-update",
        insertValues: [{ id: 3, amount: "0.12" }],
        updates: [
          { primaryKeys: { id: 1 }, changes: { amount: "1.239" } },
          { primaryKeys: { id: 2 }, changes: { amount: "2.34" } },
        ],
      },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)?.type).toBe("applyResult"),
    );
    expect(s.panel.hostMessages().at(-1)).toMatchObject({
      type: "applyResult",
      payload: {
        operationId: "bad-update",
        success: false,
        insertApplied: false,
        rowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "prevalidation_failed",
            columns: ["amount"],
          },
          { rowIndex: 1, success: false, status: "skipped" },
        ],
        insertRowOutcomes: [{ rowIndex: 0, success: false, status: "skipped" }],
      },
    });
    expect(
      s.panel
        .hostMessages()
        .some((message) => message.type === "tableMutationPreview"),
    ).toBe(false);
    for (const spy of [s.query, s.transaction, s.preview, s.insertPreview])
      expect(spy).not.toHaveBeenCalled();
  });

  it.each([
    false,
    true,
  ])("blocks a bad insert before good update/insert previews or writes (skip preview %s)", async (skipPreview) => {
    const s = await setup(skipPreview);
    await s.panel.dispatchMessage({
      type: "applyChanges",
      payload: {
        operationId: "bad-insert",
        insertValues: [
          { id: 3, amount: "0.12" },
          { id: 4, amount: "1.239" },
        ],
        updates: [{ primaryKeys: { id: 1 }, changes: { amount: "2.34" } }],
      },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)?.type).toBe("applyResult"),
    );
    expect(s.panel.hostMessages().at(-1)).toMatchObject({
      type: "applyResult",
      payload: {
        operationId: "bad-insert",
        success: false,
        insertApplied: false,
        rowOutcomes: [{ rowIndex: 0, success: false, status: "skipped" }],
        insertRowOutcomes: [
          { rowIndex: 0, success: false, status: "skipped" },
          {
            rowIndex: 1,
            success: false,
            status: "prevalidation_failed",
            columns: ["amount"],
            message: expect.stringContaining("2 fractional digits"),
          },
        ],
      },
    });
    for (const spy of [s.query, s.transaction, s.preview, s.insertPreview])
      expect(spy).not.toHaveBeenCalled();
  });

  it("returns structured diagnostics for the public single-insert message", async () => {
    const s = await setup(false);
    await s.panel.dispatchMessage({
      type: "insertRow",
      payload: { operationId: "insert", values: { amount: "1.239" } },
    });
    await vi.waitFor(() =>
      expect(s.panel.hostMessages().at(-1)?.type).toBe("insertResult"),
    );
    expect(s.panel.hostMessages().at(-1)).toMatchObject({
      type: "insertResult",
      payload: {
        operationId: "insert",
        success: false,
        status: "prevalidation_failed",
        columns: ["amount"],
        error: expect.stringContaining("2 fractional digits"),
      },
    });
    for (const spy of [s.query, s.transaction, s.insertPreview])
      expect(spy).not.toHaveBeenCalled();
  });
});
