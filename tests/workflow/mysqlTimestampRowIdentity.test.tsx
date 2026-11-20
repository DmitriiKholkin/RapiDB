import "./harness";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import type { Pool } from "mysql2/promise";
import { expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";

vi.doUnmock("../../src/webview/components/table/TableGrid");

it("mysql grid applies and deletes the selected TIMESTAMP in a repeated hour", async () => {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("mysql");
  const table = `h4_timestamp_identity_${process.pid}_${Date.now().toString(36)}`;
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
    const driver = context.connectionManager.getDriver(
      scenario.buildConnection().id,
    );
    if (!driver) throw new Error("Missing MySQL driver");
    const zone = "SET SESSION time_zone = 'America/New_York'";
    const pool = (driver as unknown as { pool: Pool }).pool;
    pool.on("connection", (connection) => connection.query(zone));
    const connection = await pool.getConnection();
    try {
      await connection.query(zone);
    } finally {
      connection.release();
    }
    await driver.query(
      `CREATE TABLE ${table} (id TIMESTAMP(6) PRIMARY KEY, amount INT) ENGINE=InnoDB`,
    );
    dropTable = () => driver.query(`DROP TABLE ${table}`);
    await driver.query(
      `INSERT INTO ${table} VALUES
        ('2024-11-03 05:30:00.123456+00:00', 3),
        ('2024-11-03 06:30:00.123456+00:00', 3)`,
    );
    const stored = async () =>
      (
        await driver.query(
          `SELECT UNIX_TIMESTAMP(id), amount FROM ${table} ORDER BY id`,
        )
      ).rows;
    session = (await context.openTableViewer()).session;
    const displayKey = "2024-11-03 01:30:00.123456";
    await waitFor(() => {
      expect(getBodyCell("id", 0).textContent).toContain("01:30:00");
      expect(getBodyCell("id", 1).textContent).toBe(
        getBodyCell("id", 0).textContent,
      );
    });
    await session.sendFromWebview({
      type: "applyChanges",
      payload: {
        operationId: "stale-timestamp-apply",
        updates: [{ primaryKeys: { id: displayKey }, changes: { amount: 9 } }],
      },
    });
    await session.sendFromWebview({
      type: "deleteRows",
      payload: {
        operationId: "stale-timestamp-delete",
        primaryKeysList: [{ id: displayKey }],
      },
    });
    for (const [type, operationId] of [
      ["applyResult", "stale-timestamp-apply"],
      ["deleteResult", "stale-timestamp-delete"],
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
      { __col_0: "1730611800.123456", __col_1: 3 },
      { __col_0: "1730615400.123456", __col_1: 3 },
    ]);

    fireEvent.doubleClick(getBodyCell("amount", 1));
    const editor = screen.getByLabelText("Cell value");
    fireEvent.change(editor, { target: { value: "8" } });
    fireEvent.keyDown(editor, { key: "Enter" });
    await session.user.click(
      screen.getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() => {
      expect(getBodyCell("amount", 1).textContent).toBe("8");
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
      { __col_0: "1730611800.123456", __col_1: 3 },
      { __col_0: "1730615400.123456", __col_1: 8 },
    ]);

    await session.user.click(
      screen.getByRole("checkbox", { name: "Select row 2" }),
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
    expect(await stored()).toEqual([
      { __col_0: "1730611800.123456", __col_1: 3 },
    ]);
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
