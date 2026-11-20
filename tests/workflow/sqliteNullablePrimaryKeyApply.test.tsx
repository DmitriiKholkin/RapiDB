import "./harness";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";

vi.doUnmock("../../src/webview/components/table/TableGrid");

it.each([
  {
    kind: "single",
    definition: "id TEXT PRIMARY KEY, label TEXT",
    values: "(NULL, 'already null'), ('selected', 'before')",
    physicalKey: "rowid",
  },
  {
    kind: "composite with shadowed aliases",
    definition:
      "tenant TEXT, id TEXT, label TEXT, ROWID TEXT, _RowId_ TEXT, PRIMARY KEY (tenant, id)",
    values:
      "('a', NULL, 'already null', 'shadow', 'shadow'), ('a', 'selected', 'before', 'shadow', 'shadow')",
    physicalKey: "oid",
  },
])("sqlite grid applies a NULL edit to a $kind primary key without touching another NULL key", async ({
  definition,
  values,
  physicalKey,
}) => {
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
    await driver.query(`
      CREATE TABLE ${table} (${definition});
      INSERT INTO ${table} VALUES ${values};
      UPDATE ${table} SET ${physicalKey} = 9007199254740993 WHERE id = 'selected';
    `);
    session = (await context.openTableViewer()).session;
    await waitFor(() => {
      expect(getBodyCell("id", 1).textContent).toBe("selected");
    });

    fireEvent.doubleClick(getBodyCell("id", 1));
    const dialog = await screen.findByRole("dialog", { name: "Cell data: id" });
    await session.user.click(
      within(dialog).getByRole("button", { name: "NULL" }),
    );
    await session.user.click(
      within(dialog).getByRole("button", { name: "Apply" }),
    );
    await session.user.click(
      screen.getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() => {
      expect(
        session
          ?.hostMessages()
          .find((message) => message.type === "applyResult")?.payload,
      ).toMatchObject({
        success: true,
        rowOutcomes: [{ status: "applied" }],
      });
    });
    expect(
      (
        await driver.query(
          `SELECT CAST(${physicalKey} AS TEXT), id, label FROM ${table} ORDER BY ${physicalKey}`,
        )
      ).rows,
    ).toEqual([
      { __col_0: "1", __col_1: null, __col_2: "already null" },
      { __col_0: "9007199254740993", __col_1: null, __col_2: "before" },
    ]);
  } finally {
    session?.unmount();
    TablePanel.disposeAll();
    await context.dispose();
  }
});
