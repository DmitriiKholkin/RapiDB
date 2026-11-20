import "./harness";
import { join } from "node:path";
import { screen, waitFor, within } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { SQLiteRowIdentity } from "../../src/shared/sqliteRowIdentity";
import { createProjectTempDir } from "../runtime/tempDirectories";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";

vi.doUnmock("../../src/webview/components/table/TableGrid");

it.each([
  "update",
  "changed before preview",
  "replacement",
  "null-to-empty",
  "identical reinsertion",
  "unchanged",
] as const)("sqlite Table Viewer Delete confirmation compares the displayed row's stored snapshot (%s)", async (change) => {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("sqlite");
  const directory = await createProjectTempDir("sqlite-delete-observation");
  const connection = scenario.buildConnection(join(directory, "h8.sqlite"));
  scenario.buildConnection = () => connection;
  const context = await bootstrapWorkflowContext({
    scenario,
    state: workflowState as unknown as Parameters<
      typeof bootstrapWorkflowContext
    >[0]["state"],
  });
  const external = new SQLiteCoreDriver(connection);
  let session: BridgeSession | undefined;
  const checkConflict =
    change !== "identical reinsertion" && change !== "unchanged";
  try {
    await context.connect();
    await external.connect();
    context.store.setSkipTableMutationPreview(false);
    const table = scenario.tableFixture.table;
    const setup = await external.query(
      `CREATE TABLE ${table} (
           tenant TEXT, id INTEGER, note TEXT COLLATE NOCASE, amount INTEGER, flag BLOB,
           PRIMARY KEY (tenant, id)
         ) WITHOUT ROWID;
         INSERT INTO ${table} VALUES ('team', 1, 'Original', 9007199254740993, NULL)`,
    );
    expect(setup.error).toBeUndefined();
    session = (await context.openTableViewer()).session;
    const viewer = session;
    await waitFor(() =>
      expect(getBodyCell("note", 0).textContent).toBe("Original"),
    );

    if (change === "unchanged") {
      // The host must enforce the requirement as well as the webview,
      // including when the user has disabled preview.
      context.store.setSkipTableMutationPreview(true);
      const page = viewer
        .hostMessages()
        .find(({ type }) => type === "tableData")?.payload as {
        sqliteRowIdentities: SQLiteRowIdentity[];
      };
      const operationId = "missing-observation";
      await viewer.sendFromWebview({
        type: "deleteRows",
        payload: { operationId, primaryKeysList: page.sqliteRowIdentities },
      });
      expect(viewer.hostMessages()).toContainEqual({
        type: "deleteResult",
        payload: expect.objectContaining({
          operationId,
          success: false,
          affectedRows: 0,
          changesPossible: false,
          error: expect.stringMatching(/original row snapshot/),
        }),
      });
      context.store.setSkipTableMutationPreview(false);
    }

    await viewer.user.click(
      screen.getByRole("checkbox", { name: "Select row 1" }),
    );
    if (change === "changed before preview") {
      expect(
        (await external.query(`UPDATE ${table} SET note = 'external'`)).error,
      ).toBeUndefined();
    }
    await viewer.user.click(screen.getByRole("button", { name: /^Delete/ }));
    const dialog = await screen.findByRole("dialog");
    expect(
      viewer.hostMessages().find(({ type }) => type === "tableMutationPreview")
        ?.payload,
    ).toMatchObject({ sql: expect.stringContaining("RAPIDB_DELETE_HASH") });

    const write =
      change === "update"
        ? `UPDATE ${table} SET note = 'original' WHERE tenant = 'team' AND id = 1`
        : change === "replacement"
          ? `DELETE FROM ${table}; INSERT INTO ${table} VALUES ('team', 1, 'Original', 9007199254740992, NULL)`
          : change === "null-to-empty"
            ? `UPDATE ${table} SET flag = X''`
            : change === "identical reinsertion"
              ? `DELETE FROM ${table}; INSERT INTO ${table} VALUES ('team', 1, 'Original', 9007199254740993, NULL)`
              : null;
    if (write) expect((await external.query(write)).error).toBeUndefined();

    await viewer.user.click(
      within(dialog).getByRole("button", { name: "Apply Changes" }),
    );
    await waitFor(() =>
      expect(
        viewer
          .hostMessages()
          .filter(({ type }) => type === "deleteResult")
          .at(-1)?.payload,
      ).toMatchObject(
        checkConflict
          ? {
              success: false,
              affectedRows: 0,
              changesPossible: false,
              outcomeUnknown: false,
              error: expect.stringContaining(
                "changed, was replaced, or was deleted",
              ),
            }
          : { success: true, affectedRows: 1 },
      ),
    );
    const remaining = await external.query(
      `SELECT note, CAST(amount AS TEXT), typeof(flag) FROM ${table}`,
    );
    expect(remaining.rows).toEqual(
      checkConflict
        ? [
            {
              __col_0:
                change === "update"
                  ? "original"
                  : change === "changed before preview"
                    ? "external"
                    : "Original",
              __col_1:
                change === "replacement"
                  ? "9007199254740992"
                  : "9007199254740993",
              __col_2: change === "null-to-empty" ? "blob" : "null",
            },
          ]
        : [],
    );
  } finally {
    session?.unmount();
    TablePanel.disposeAll();
    await external.disconnect();
    await context.dispose();
  }
});
