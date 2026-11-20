import "./harness";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";

vi.doUnmock("../../src/webview/components/table/TableGrid");

it("postgres grid applies and deletes only the selected array bounds", async () => {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("postgres");
  const connection = scenario.buildConnection();
  const table = `h3_array_identity_${process.pid}_${Date.now().toString(36)}`;
  scenario.tableFixture.table = table;
  const context = await bootstrapWorkflowContext({
    scenario,
    state: workflowState as unknown as Parameters<
      typeof bootstrapWorkflowContext
    >[0]["state"],
  });
  let session: BridgeSession | undefined;
  let dropTable: (() => Promise<unknown>) | undefined;
  try {
    await context.connect();
    const driver = context.connectionManager.getDriver(connection.id);
    if (!driver) throw new Error("Missing PostgreSQL driver");
    await driver.query(
      `CREATE TABLE ${table} (
        id integer[] PRIMARY KEY, tag text, amount integer,
        "__rapidb_pg_array_key_0" text DEFAULT 'real column'
      )`,
    );
    dropTable = () => driver.query(`DROP TABLE ${table}`);
    await driver.query(
      `INSERT INTO ${table}(id, tag, amount) VALUES
        ('[0:1]={4,5}', 'lower', 3), ('{4,5}', 'ordinary', 3)`,
    );
    const stored = async () => {
      const { rows } = await driver.query(
        `SELECT pg_catalog.array_lower(id, 1) AS lower, tag, amount FROM ${table} ORDER BY id`,
      );
      return rows.map(({ __col_0: lower, __col_1: tag, __col_2: amount }) => ({
        lower,
        tag,
        amount,
      }));
    };
    session = (await context.openTableViewer()).session;
    await waitFor(() => {
      expect(getBodyCell("tag", 0).textContent).toBe("lower");
      expect(getBodyCell("tag", 1).textContent).toBe("ordinary");
      expect(getBodyCell("id", 0).textContent).toBe("[4,5]");
      expect(getBodyCell("id", 1).textContent).toBe("[4,5]");
      expect(getBodyCell("__rapidb_pg_array_key_0", 0).textContent).toBe(
        "real column",
      );
    });

    await session.sendFromWebview({
      type: "applyChanges",
      payload: {
        operationId: "stale-array-apply",
        updates: [{ primaryKeys: { id: "[4,5]" }, changes: { amount: "7" } }],
      },
    });
    await session.sendFromWebview({
      type: "deleteRows",
      payload: {
        operationId: "stale-array-delete",
        primaryKeysList: [{ id: "[4,5]" }],
      },
    });
    for (const [type, operationId] of [
      ["applyResult", "stale-array-apply"],
      ["deleteResult", "stale-array-delete"],
    ]) {
      expect(session.hostMessages()).toContainEqual(
        expect.objectContaining({
          type,
          payload: expect.objectContaining({
            operationId,
            success: false,
            error: expect.stringContaining("Refresh the table"),
          }),
        }),
      );
    }
    expect(await stored()).toEqual([
      { lower: 0, tag: "lower", amount: 3 },
      { lower: 1, tag: "ordinary", amount: 3 },
    ]);

    fireEvent.doubleClick(getBodyCell("amount", 0));
    const editor = screen.getByLabelText("Cell value");
    fireEvent.change(editor, { target: { value: "8" } });
    fireEvent.keyDown(editor, { key: "Enter" });
    await session.user.click(
      screen.getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() => {
      expect(getBodyCell("amount", 0).textContent).toBe("8");
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
      expect(
        session
          ?.hostMessages()
          .filter((message) => message.type === "tableData").length,
      ).toBeGreaterThan(1);
    });
    expect(await stored()).toEqual([
      { lower: 0, tag: "lower", amount: 8 },
      { lower: 1, tag: "ordinary", amount: 3 },
    ]);

    await session.user.click(
      screen.getByRole("checkbox", { name: "Select row 1" }),
    );
    await session.user.click(
      screen.getByRole("button", { name: "Delete (1)" }),
    );
    await waitFor(() => {
      expect(session?.hostMessages()).toContainEqual(
        expect.objectContaining({
          type: "deleteResult",
          payload: expect.objectContaining({ success: true, affectedRows: 1 }),
        }),
      );
    });
    expect(await stored()).toEqual([{ lower: 1, tag: "ordinary", amount: 3 }]);
  } finally {
    session?.unmount();
    TablePanel.disposeAll();
    try {
      await dropTable?.();
    } finally {
      await context.dispose();
    }
  }
});
