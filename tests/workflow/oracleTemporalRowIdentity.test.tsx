import "./harness";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";

vi.doUnmock("../../src/webview/components/table/TableGrid");

it("oracle grid does not confuse matching BC and AD displays during edits, deletion, or draft recovery", async () => {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("oracle");
  const tableName = `H6_ERA_${process.pid}_${Date.now().toString(36).toUpperCase()}`;
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
    if (!driver) throw new Error("Missing Oracle driver");
    const table = driver.qualifiedTableName(
      scenario.tableFixture.database,
      scenario.tableFixture.schema ?? "",
      tableName,
    );
    const key = "0001-02-03 04:05:06";
    await driver.query(
      `CREATE TABLE ${table} (ID DATE PRIMARY KEY, AMOUNT NUMBER(10))`,
    );
    dropTable = () => driver.query(`DROP TABLE ${table} PURGE`);
    for (const era of ["BC", "AD"]) {
      await driver.query(
        `INSERT INTO ${table} VALUES (TO_DATE('${key} ${era}', 'YYYY-MM-DD HH24:MI:SS BC'), 3)`,
      );
    }
    const stored = async () =>
      (
        await driver.query(
          `SELECT TO_CHAR(ID, 'YYYY-MM-DD HH24:MI:SS BC'), AMOUNT FROM ${table} ORDER BY ID`,
        )
      ).rows;

    session = (await context.openTableViewer()).session;
    await waitFor(() => {
      expect(getBodyCell("ID", 0).textContent).toBe(key);
      expect(getBodyCell("ID", 1).textContent).toBe(key);
    });
    await session.sendFromWebview({
      type: "applyChanges",
      payload: {
        operationId: "stale-oracle-apply",
        updates: [{ primaryKeys: { ID: key }, changes: { AMOUNT: 99 } }],
      },
    });
    await session.sendFromWebview({
      type: "deleteRows",
      payload: {
        operationId: "stale-oracle-delete",
        primaryKeysList: [{ ID: key }],
      },
    });
    for (const [type, operationId] of [
      ["applyResult", "stale-oracle-apply"],
      ["deleteResult", "stale-oracle-delete"],
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

    function stageAmount(row: number, value: string) {
      fireEvent.doubleClick(getBodyCell("AMOUNT", row));
      const editor = screen.getByLabelText("Cell value");
      fireEvent.change(editor, { target: { value } });
      fireEvent.keyDown(editor, { key: "Enter" });
    }
    stageAmount(0, "8");
    await session.user.click(
      screen.getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() => {
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
      expect(
        session?.hostMessages().filter(({ type }) => type === "tableData")
          .length,
      ).toBeGreaterThan(1);
    });
    expect(await stored()).toEqual([
      { __col_0: `${key} BC`, __col_1: 8 },
      { __col_0: `${key} AD`, __col_1: 3 },
    ]);

    // When BC is deleted, the draft on the visually identical AD key must
    // survive and follow AD from row 2 to row 1.
    stageAmount(1, "9");
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
      expect(getBodyCell("AMOUNT", 0).textContent).toBe("9");
      expect(
        screen.queryByRole("checkbox", { name: "Select row 2" }),
      ).toBeNull();
    });
    expect(await stored()).toEqual([{ __col_0: `${key} AD`, __col_1: 3 }]);
    await session.user.click(
      screen.getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() => {
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
    });
    expect(await stored()).toEqual([{ __col_0: `${key} AD`, __col_1: 9 }]);
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
