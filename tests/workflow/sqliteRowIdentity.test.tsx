import "./harness";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";

vi.doUnmock("../../src/webview/components/table/TableGrid");

it("sqlite table grid updates and deletes the selected stored identity", async () => {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("sqlite");
  const connection = scenario.buildConnection(":memory:");
  scenario.buildConnection = () => connection;
  const context = await bootstrapWorkflowContext({
    scenario,
    state: workflowState as unknown as Parameters<
      typeof bootstrapWorkflowContext
    >[0]["state"],
  });
  let session: BridgeSession | undefined;
  try {
    await context.connect();
    const driver = context.connectionManager.getDriver(connection.id);
    if (!driver) throw new Error("Missing SQLite driver");
    const table = scenario.tableFixture.table;
    await driver.query(
      `CREATE TABLE ${table} (id BOOLEAN PRIMARY KEY, label TEXT, amount INTEGER);
       INSERT INTO ${table} VALUES (1, 'numeric key', 3), ('true', 'text key', 4)`,
    );
    session = (await context.openTableViewer()).session;
    await waitFor(() => {
      expect(getBodyCell("label", 1).textContent).toBe("text key");
    });

    fireEvent.doubleClick(getBodyCell("amount", 1));
    const editor = screen.getByLabelText("Cell value");
    fireEvent.change(editor, { target: { value: "8" } });
    fireEvent.keyDown(editor, { key: "Enter" });
    await session.user.click(
      screen.getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() => {
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
      expect(getBodyCell("amount", 1).textContent).toBe("8");
      expect(
        session
          ?.hostMessages()
          .filter((message) => message.type === "tableData").length,
      ).toBeGreaterThan(1);
    });

    await session.user.click(
      screen.getByRole("checkbox", { name: "Select row 2" }),
    );
    await session.user.click(screen.getByRole("button", { name: /^Delete/ }));
    await waitFor(() => {
      expect(
        session
          ?.hostMessages()
          .find((message) => message.type === "deleteResult")?.payload,
      ).toMatchObject({ success: true, affectedRows: 1 });
    });
    await session.sendFromWebview({
      type: "deleteRows",
      payload: {
        operationId: "stale-display-only-key",
        primaryKeysList: [{ id: true }],
      },
    });
    expect(session.hostMessages()).toContainEqual(
      expect.objectContaining({
        type: "deleteResult",
        payload: expect.objectContaining({
          operationId: "stale-display-only-key",
          affectedRows: 0,
          changesPossible: false,
          error: expect.stringContaining("Refresh the table"),
        }),
      }),
    );
    const remaining = await driver.query(
      `SELECT typeof(id), amount FROM ${table}`,
    );
    expect(remaining.rows).toEqual([{ __col_0: "integer", __col_1: 3 }]);
  } finally {
    session?.unmount();
    TablePanel.disposeAll();
    await context.dispose();
  }
});
