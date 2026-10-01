import "./harness";
import { join } from "node:path";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { ApplyResultPayload } from "../../src/shared/webviewContracts";
import { createProjectTempDir } from "../runtime/tempDirectories";
import type { WorkflowMessageEnvelope } from "./bridge/fakeWebviewPanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import { getBodyCell } from "./tableUi";
import type { WorkflowContext } from "./workflowContext";

// Exercise the actual draft cells/editors, not the workflow harness's display stub.
vi.doUnmock("../../src/webview/components/table/TableGrid");
vi.doMock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue,
    ariaLabel,
    readOnly,
    onChange,
  }: {
    initialValue?: string;
    ariaLabel?: string;
    readOnly?: boolean;
    onChange?: (value: string) => void;
  }) => (
    <textarea
      aria-label={ariaLabel}
      defaultValue={initialValue}
      readOnly={readOnly}
      onChange={(event) => onChange?.(event.target.value)}
    />
  ),
  connTypeToDialect: () => "sqlite",
}));

async function withTable(
  titleDefinition: string,
  run: (context: WorkflowContext, session: BridgeSession) => Promise<void>,
) {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  TablePanel.disposeAll();
  const scenario = getScenario("sqlite");
  const connection = scenario.buildConnection(
    join(await createProjectTempDir("sqlite-default-insert"), "db.sqlite"),
  );
  scenario.buildConnection = () => connection;
  scenario.tableFixture.table = "default_rows";
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
    await driver.query(`CREATE TABLE default_rows (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title ${titleDefinition},
      title_upper TEXT GENERATED ALWAYS AS (upper(title)) STORED
    ); INSERT INTO default_rows (title) VALUES ('seed')`);
    // Cover the real preview/confirmation route as well as host execution.
    context.store.setSkipTableMutationPreview(false);
    session = (await context.openTableViewer()).session;
    await waitFor(() => expect(getBodyCell("title").textContent).toBe("seed"));
    await run(context, session);
  } finally {
    session?.unmount();
    TablePanel.disposeAll();
    await context.dispose();
  }
}

function editTitle(rowIndex: number, value: string) {
  fireEvent.doubleClick(getBodyCell("title", rowIndex));
  const inlineEditor = screen.queryByLabelText("Cell value");
  const editor = inlineEditor ?? screen.getByLabelText("Cell data");
  fireEvent.change(editor, { target: { value } });
  if (inlineEditor) fireEvent.keyDown(editor, { key: "Enter" });
  else fireEvent.click(screen.getByRole("button", { name: "Apply" }));
}

async function apply(
  session: BridgeSession,
  expectedStatementCount: number,
): Promise<ApplyResultPayload> {
  await session.user.click(
    screen.getByRole("button", { name: "Apply Changes" }),
  );
  await screen.findByRole("dialog");
  const preview = session
    .hostMessages()
    .filter((message) => message.type === "tableMutationPreview")
    .at(-1);
  expect(preview?.payload).toMatchObject({
    kind: "applyChanges",
    statementCount: expectedStatementCount,
  });
  await session.user.click(
    screen.getByRole("button", { name: "Apply Changes" }),
  );
  await waitFor(() => {
    expect(
      session.hostMessages().some((message) => message.type === "applyResult"),
    ).toBe(true);
  });
  return session
    .hostMessages()
    .filter((message) => message.type === "applyResult")
    .at(-1)?.payload as ApplyResultPayload;
}

function driverFor(context: WorkflowContext) {
  const driver = context.connectionManager.getDriver(
    context.scenario.buildConnection().id,
  );
  if (!driver) throw new Error("Missing SQLite driver");
  return driver;
}

async function readRows(context: WorkflowContext) {
  return (
    await driverFor(context).query(
      "SELECT id, title, title_upper FROM default_rows ORDER BY id",
    )
  ).rows.map((row) => ({
    id: row.__col_0,
    title: row.__col_1,
    upper: row.__col_2,
  }));
}

describe("B05 sqlite UI/host/default insert journey", () => {
  it.each([
    { name: "one DEFAULT-only row", count: 1, explicit: false, update: false },
    {
      name: "multiple DEFAULT-only rows",
      count: 3,
      explicit: false,
      update: false,
    },
    {
      name: "mixed DEFAULT-only and explicit rows",
      count: 3,
      explicit: true,
      update: false,
    },
    {
      name: "DEFAULT-only rows plus an update",
      count: 2,
      explicit: false,
      update: true,
    },
  ])("commits $name and refreshes the UI", async ({
    count,
    explicit,
    update,
  }) => {
    await withTable(
      "TEXT NOT NULL DEFAULT 'generated'",
      async (context, session) => {
        const transaction = vi.spyOn(driverFor(context), "runTransaction");
        const postMessage = vi.spyOn(
          window.__vscode as NonNullable<Window["__vscode"]>,
          "postMessage",
        );
        for (let index = 0; index < count; index++) {
          await session.user.click(
            screen.getByRole("button", { name: "Add Row" }),
          );
        }
        if (explicit) editTitle(1, "explicit");
        if (update) editTitle(count, "updated");
        const expectedInserts = Array.from({ length: count }, (_, index) =>
          explicit && index === 1 ? { title: "explicit" } : {},
        );
        const result = await apply(session, count + Number(update));
        const request = postMessage.mock.calls
          .map(([message]) => message as WorkflowMessageEnvelope)
          .find((message) => message.type === "applyChanges");
        expect(request?.payload).toMatchObject({
          insertValues: expectedInserts,
        });
        expect(result).toMatchObject({ success: true, insertApplied: true });
        if (count + Number(update) > 1) {
          expect(transaction).toHaveBeenCalledOnce();
          const operations = transaction.mock.calls[0][0];
          expect(operations).toHaveLength(count + Number(update));
          expect(
            operations.filter((operation) =>
              operation.sql.includes("DEFAULT VALUES"),
            ),
          ).toHaveLength(count - Number(explicit));
        }
        const expectedRows = [
          {
            id: 1,
            title: update ? "updated" : "seed",
            upper: update ? "UPDATED" : "SEED",
          },
          ...Array.from({ length: count }, (_, index) => ({
            id: index + 2,
            title: explicit && index === 1 ? "explicit" : "generated",
            upper: explicit && index === 1 ? "EXPLICIT" : "GENERATED",
          })),
        ];
        expect(await readRows(context)).toEqual(expectedRows);
        await waitFor(() => {
          expect(
            screen.queryByRole("button", { name: "Apply Changes" }),
          ).toBeNull();
          expect(document.querySelector('td[data-row="-1"]')).toBeNull();
          for (let index = 0; index < expectedRows.length; index++) {
            expect(getBodyCell("title", index).textContent).toBe(
              expectedRows[index].title,
            );
            expect(getBodyCell("title_upper", index).textContent).toBe(
              expectedRows[index].upper,
            );
          }
        });
      },
    );
  });

  it.each([
    {
      name: "single DEFAULT constraint failure",
      definition:
        "TEXT NOT NULL DEFAULT 'generated' CHECK (title <> 'generated')",
      count: 1,
      explicit: false,
      update: false,
    },
    {
      name: "multiple DEFAULT unique failure",
      definition: "TEXT NOT NULL DEFAULT 'generated' UNIQUE",
      count: 2,
      explicit: false,
      update: false,
    },
    {
      name: "mixed-row constraint failure",
      definition: "TEXT NOT NULL DEFAULT 'generated' CHECK (title <> '')",
      count: 2,
      explicit: true,
      update: false,
    },
    {
      name: "update constraint failure after DEFAULT insert",
      definition: "TEXT NOT NULL DEFAULT 'generated' CHECK (title <> '')",
      count: 1,
      explicit: false,
      update: true,
    },
  ])("retains all drafts on $name and rolls back SQLite", async ({
    definition,
    count,
    explicit,
    update,
  }) => {
    await withTable(definition, async (context, session) => {
      const transaction = vi.spyOn(driverFor(context), "runTransaction");
      for (let index = 0; index < count; index++) {
        await session.user.click(
          screen.getByRole("button", { name: "Add Row" }),
        );
      }
      // The DEFAULT insert comes first, so a later failure must undo it.
      if (explicit) editTitle(1, "");
      if (update) editTitle(count, "");
      const draftTitles = Array.from(
        { length: count },
        (_, index) => getBodyCell("title", index).textContent,
      );
      const result = await apply(session, count + Number(update));
      expect(result.success).toBe(false);
      expect(result.insertApplied).not.toBe(true);
      expect(result.error).toMatch(/constraint|UNIQUE/i);
      if (count + Number(update) > 1)
        expect(transaction).toHaveBeenCalledOnce();
      expect(await readRows(context)).toEqual([
        { id: 1, title: "seed", upper: "SEED" },
      ]);
      await waitFor(() => {
        expect(
          (
            screen.getByRole("button", {
              name: "Apply Changes",
            }) as HTMLButtonElement
          ).disabled,
        ).toBe(false);
        for (let index = 0; index < count; index++) {
          expect(getBodyCell("title", index).textContent).toBe(
            draftTitles[index],
          );
        }
        expect(getBodyCell("title", count).textContent).toBe(
          update ? "" : "seed",
        );
      });
      // Fix the offending values in the retained drafts/edits and retry via UI.
      if (update) editTitle(count, "recovered");
      else editTitle(count - 1, "recovered");
      await session.user.click(
        screen.getByRole("button", { name: "Apply Changes" }),
      );
      await screen.findByRole("dialog");
      await session.user.click(
        screen.getByRole("button", { name: "Apply Changes" }),
      );
      await waitFor(() =>
        expect(
          screen.queryByRole("button", { name: "Apply Changes" }),
        ).toBeNull(),
      );
      const rows = await readRows(context);
      expect(rows).toHaveLength(count + 1);
      expect(rows.some((row) => row.title === "recovered")).toBe(true);
      expect(document.querySelector('td[data-row="-1"]')).toBeNull();
    });
  });
});
