import "./harness";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";

vi.doUnmock("../../src/webview/components/table/TableGrid");

it("mssql grid edits and deletes only the selected sql_variant row when keys display alike", async () => {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("mssql");
  const tableName = `h5_variant_${process.pid}_${Date.now().toString(36)}`;
  scenario.tableFixture.table = tableName;
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
    if (!driver) throw new Error("Missing MSSQL driver");
    const table = driver.qualifiedTableName(
      scenario.tableFixture.database,
      scenario.tableFixture.schema ?? "dbo",
      tableName,
    );
    const key = "12345678901234567890.12";
    await driver.query(
      `CREATE TABLE ${table} (id sql_variant PRIMARY KEY, amount int)`,
    );
    dropTable = () => driver.query(`DROP TABLE ${table}`);
    await driver.query(
      `INSERT INTO ${table} VALUES
           (CONVERT(sql_variant, CONVERT(decimal(26,2), '${key}')), 3),
           (CONVERT(sql_variant, CONVERT(nvarchar(40), N'${key}')), 3)`,
    );
    const stored = async () =>
      (
        await driver.query(
          `SELECT CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(id, 'BaseType')), amount FROM ${table} ORDER BY id`,
        )
      ).rows;
    session = (await context.openTableViewer()).session;
    await waitFor(() => {
      expect(getBodyCell("id", 0).textContent).toBe(key);
      expect(getBodyCell("id", 1).textContent).toBe(key);
    });

    fireEvent.doubleClick(getBodyCell("id", 1));
    expect(screen.queryByLabelText("Cell value")).toBeNull();
    expect(
      screen.getByText(/cannot safely change SQL Server sql_variant/),
    ).toBeTruthy();
    fireEvent.doubleClick(getBodyCell("amount", 1));
    const editor = screen.getByLabelText("Cell value");
    fireEvent.change(editor, { target: { value: "8" } });
    fireEvent.keyDown(editor, { key: "Enter" });
    await session.user.click(
      screen.getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() => {
      expect(
        session
          ?.hostMessages()
          .filter((message) => message.type === "tableData").length,
      ).toBeGreaterThan(1);
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
    });
    expect(await stored()).toEqual([
      { __col_0: "nvarchar", __col_1: 3 },
      { __col_0: "decimal", __col_1: 8 },
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
    expect(await stored()).toEqual([{ __col_0: "nvarchar", __col_1: 3 }]);
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
