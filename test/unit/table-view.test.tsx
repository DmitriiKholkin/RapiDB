/**
 * @vitest-environment jsdom
 */

import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta as ColumnMeta } from "../../src/shared/tableTypes";

vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue = "",
    ariaLabel = "SQL editor",
  }: {
    initialValue?: string;
    ariaLabel?: string;
  }) => <textarea aria-label={ariaLabel} readOnly value={initialValue} />,
}));

import { TableView } from "../../src/webview/components/TableView";
import { categoryColor } from "../../src/webview/types";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 26,
        end: (index + 1) * 26,
      })),
    getTotalSize: () => count * 26,
  }),
}));

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

const postMessage = vi.fn();
const getState = vi.fn();
const setState = vi.fn();

interface OutgoingMessage {
  type: string;
  payload?: unknown;
}

function makeColumn(
  overrides: Partial<ColumnMeta> & { name: string; type: string },
): ColumnMeta {
  const { name, type, nativeType, ...rest } = overrides;
  return {
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    editable: true,
    filterOperators: ["like"],
    isBoolean: false,
    ...rest,
    name,
    type,
    nativeType: nativeType ?? type,
  };
}

function emit(type: string, payload: unknown): void {
  window.dispatchEvent(
    new MessageEvent("message", { data: { type, payload } }),
  );
}

function getMessages(type: string): OutgoingMessage[] {
  return postMessage.mock.calls
    .map(([message]) => message as OutgoingMessage)
    .filter((message) => message.type === type);
}

function getLastMessage(type: string): OutgoingMessage | undefined {
  return getMessages(type).at(-1);
}

function fetchMessageCount(): number {
  return getMessages("fetchPage").length;
}

async function renderEditableTable(
  rows: Array<{ id: number; name: string }> = [{ id: 1, name: "Alice" }],
): Promise<void> {
  render(
    <TableView
      connectionId="conn1"
      database="db"
      schema="public"
      table="users"
    />,
  );

  emit("tableInit", {
    columns: [
      makeColumn({
        name: "id",
        type: "integer",
        category: "integer",
        isPrimaryKey: true,
        filterable: false,
        editable: false,
        isAutoIncrement: true,
      }),
      makeColumn({ name: "name", type: "text" }),
    ],
    primaryKeyColumns: ["id"],
  });
  emit("tableData", {
    rows,
    totalCount: rows.length,
  });

  await waitFor(() => {
    expect(screen.getByText(rows[0]?.name ?? "")).toBeDefined();
  });
}

async function editCellValue(
  currentValue: string,
  nextValue: string,
): Promise<void> {
  const cell = screen.getByText(currentValue).closest("td");
  expect(cell).not.toBeNull();

  fireEvent.doubleClick(cell as HTMLElement);

  const input = (await screen.findByDisplayValue(
    currentValue,
  )) as HTMLInputElement;
  fireEvent.change(input, { target: { value: nextValue } });
  fireEvent.keyDown(input, { key: "Enter" });

  await waitFor(() => {
    expect(screen.queryByDisplayValue(currentValue)).toBeNull();
  });
}

async function waitForFetchFilters(
  previousCount: number,
  filters: unknown,
): Promise<void> {
  await waitFor(() => {
    const messages = getMessages("fetchPage");
    expect(messages.length).toBeGreaterThan(previousCount);
    expect(
      (messages.at(-1)?.payload as { filters?: unknown } | undefined)?.filters,
    ).toEqual(filters);
  });
}

describe("TableView", () => {
  beforeEach(() => {
    postMessage.mockReset();
    getState.mockReset();
    setState.mockReset();
    (
      window as Window & {
        __vscode?: {
          postMessage: typeof postMessage;
          getState: typeof getState;
          setState: typeof setState;
        };
      }
    ).__vscode = {
      postMessage,
      getState,
      setState,
    };
  });

  it("renders operator-aware filter controls and disables unsupported columns", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    await waitFor(() => {
      expect(postMessage).toHaveBeenCalledWith({
        type: "ready",
        payload: undefined,
      });
    });

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "id",
          type: "integer",
          category: "integer",
          isPrimaryKey: true,
          filterable: false,
          editable: false,
          isAutoIncrement: true,
        }),
        makeColumn({ name: "name", type: "text" }),
      ],
      primaryKeyColumns: ["id"],
    });
    emit("tableData", {
      rows: [{ id: 1, name: "Alice" }],
      totalCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByText("Alice")).toBeDefined();
    });

    const inputs = screen.getAllByRole("textbox");
    expect(inputs).toHaveLength(2);
    expect((inputs[0] as HTMLInputElement).disabled).toBe(true);
    expect((inputs[1] as HTMLInputElement).disabled).toBe(false);

    const idTrigger = screen.getByRole("button", {
      name: "id filter operator",
    }) as HTMLButtonElement;
    const nameTrigger = screen.getByRole("button", {
      name: "name filter operator",
    }) as HTMLButtonElement;

    expect(idTrigger.disabled).toBe(true);
    expect(nameTrigger.disabled).toBe(false);
    expect(
      (screen.getByLabelText("name filter value") as HTMLInputElement).value,
    ).toBe("");
    expect(
      within(
        (screen.getByLabelText("name filter value") as HTMLInputElement)
          .parentElement as HTMLElement,
      ).getAllByRole("button"),
    ).toHaveLength(1);
  });

  it("blocks editing for read-only auto-increment cells but allows editable cells", async () => {
    const { container } = render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    await waitFor(() => {
      expect(postMessage).toHaveBeenCalledWith({
        type: "ready",
        payload: undefined,
      });
    });

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "id",
          type: "integer",
          category: "integer",
          isPrimaryKey: true,
          filterable: false,
          editable: false,
          isAutoIncrement: true,
        }),
        makeColumn({ name: "name", type: "text" }),
      ],
      primaryKeyColumns: ["id"],
    });
    emit("tableData", {
      rows: [{ id: 1, name: "Alice" }],
      totalCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByText("Alice")).toBeDefined();
    });

    const pkCell = container.querySelector('td[title="PK: 1"]');
    expect(pkCell).not.toBeNull();
    fireEvent.doubleClick(pkCell as HTMLElement);
    expect(container.querySelector('td[title="PK: 1"] input')).toBeNull();

    fireEvent.doubleClick(
      screen.getByText("Alice").closest("td") as HTMLElement,
    );
    expect(screen.getByDisplayValue("Alice")).toBeDefined();
  });

  it("passes type colors through to cells and keeps pending warning color precedence", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="places"
      />,
    );

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "id",
          type: "integer",
          category: "integer",
          isPrimaryKey: true,
          filterable: false,
          editable: false,
          isAutoIncrement: true,
        }),
        makeColumn({
          name: "geom",
          type: "geometry",
          category: "spatial",
          nativeType: "geometry",
        }),
      ],
      primaryKeyColumns: ["id"],
    });
    emit("tableData", {
      rows: [{ id: 1, geom: "POINT(1 2)" }],
      totalCount: 1,
    });

    const originalValue = await screen.findByText("POINT(1 2)");
    expect(originalValue.style.color).toBe(categoryColor("spatial"));

    await editCellValue("POINT(1 2)", "POINT(3 4)");

    const pendingValue = await screen.findByText("POINT(3 4)");
    expect(pendingValue.style.color).toContain("cca700");
  });

  it("serializes scalar filters only after non-empty input", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    emit("tableInit", {
      columns: [makeColumn({ name: "name", type: "text" })],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ name: "Alice" }],
      totalCount: 1,
    });

    const input = (await screen.findByLabelText(
      "name filter value",
    )) as HTMLInputElement;

    await waitFor(() => {
      expect(fetchMessageCount()).toBeGreaterThan(0);
    });

    const whitespaceFetchCount = fetchMessageCount();
    fireEvent.change(input, { target: { value: "   " } });

    await waitForFetchFilters(whitespaceFetchCount, []);

    const valueFetchCount = fetchMessageCount();
    fireEvent.change(input, { target: { value: "Alice" } });

    await waitForFetchFilters(valueFetchCount, [
      { column: "name", operator: "like", value: "Alice" },
    ]);
  });

  it("shows only supported operators and locks the input for nullability filters", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "payload",
          type: "jsonb",
          filterable: true,
          editable: false,
          filterOperators: ["like", "is_null", "is_not_null"],
        }),
      ],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ payload: null }],
      totalCount: 1,
    });

    const trigger = (await screen.findByRole("button", {
      name: "payload filter operator",
    })) as HTMLButtonElement;

    fireEvent.click(trigger);

    expect(screen.getByText("No filter")).toBeDefined();
    expect(screen.getByText("Contains")).toBeDefined();
    expect(screen.getByText("Is NULL")).toBeDefined();
    expect(screen.getByText("Is NOT NULL")).toBeDefined();
    expect(screen.queryByText("Between")).toBeNull();

    const fetchCount = fetchMessageCount();
    fireEvent.click(
      screen.getByText("Is NOT NULL").closest("button") as HTMLElement,
    );

    await waitForFetchFilters(fetchCount, [
      { column: "payload", operator: "is_not_null" },
    ]);

    const input = screen.getByLabelText(
      "payload filter value",
    ) as HTMLInputElement;
    expect(input.disabled).toBe(true);
    expect(input.value).toBe("NOT NULL");
    expect(trigger.textContent).toBe("!N");
  });

  it("allows clearing null-only filters back to a neutral state", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "notes",
          type: "text",
          filterable: false,
          editable: false,
          filterOperators: ["is_null", "is_not_null"],
        }),
      ],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ notes: null }],
      totalCount: 1,
    });

    const trigger = (await screen.findByRole("button", {
      name: "notes filter operator",
    })) as HTMLButtonElement;
    const input = (await screen.findByLabelText(
      "notes filter value",
    )) as HTMLInputElement;

    expect(input.disabled).toBe(true);
    expect(input.value).toBe("");
    expect(trigger.textContent).toBe("x");

    const selectFetchCount = fetchMessageCount();
    fireEvent.click(trigger);
    fireEvent.click(
      screen.getByText("Is NULL").closest("button") as HTMLElement,
    );

    await waitForFetchFilters(selectFetchCount, [
      { column: "notes", operator: "is_null" },
    ]);

    expect(
      (screen.getByLabelText("notes filter value") as HTMLInputElement).value,
    ).toBe("NULL");

    const clearFetchCount = fetchMessageCount();
    fireEvent.click(
      screen.getByRole("button", { name: "notes filter operator" }),
    );
    fireEvent.click(
      screen.getByText("No filter").closest("button") as HTMLElement,
    );

    await waitForFetchFilters(clearFetchCount, []);

    expect(
      (screen.getByLabelText("notes filter value") as HTMLInputElement).value,
    ).toBe("");
    expect(
      (
        screen.getByRole("button", {
          name: "notes filter operator",
        }) as HTMLButtonElement
      ).textContent,
    ).toBe("x");
  });

  it("renders between filters with two inputs and waits for both values", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="events"
      />,
    );

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "created_on",
          type: "date",
          category: "date",
          filterOperators: ["between"],
        }),
      ],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ created_on: "2026-04-15" }],
      totalCount: 1,
    });

    const startInput = (await screen.findByLabelText(
      "created_on filter start",
    )) as HTMLInputElement;
    const endInput = screen.getByLabelText(
      "created_on filter end",
    ) as HTMLInputElement;

    await waitFor(() => {
      expect(fetchMessageCount()).toBeGreaterThan(0);
    });

    const firstFetchCount = fetchMessageCount();
    fireEvent.change(startInput, { target: { value: "2026-04-01" } });

    await waitForFetchFilters(firstFetchCount, []);

    const secondFetchCount = fetchMessageCount();
    fireEvent.change(endInput, { target: { value: "2026-04-30" } });

    await waitForFetchFilters(secondFetchCount, [
      {
        column: "created_on",
        operator: "between",
        value: ["2026-04-01", "2026-04-30"],
      },
    ]);
  });

  it("uses only debounced filters for refresh and export actions", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    emit("tableInit", {
      columns: [makeColumn({ name: "name", type: "text" })],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ name: "Alice" }],
      totalCount: 1,
    });

    const input = (await screen.findByLabelText(
      "name filter value",
    )) as HTMLInputElement;

    await waitFor(() => {
      expect(fetchMessageCount()).toBeGreaterThan(0);
    });

    const activeFilters = [
      { column: "name", operator: "like", value: "Alice" },
    ];

    vi.useFakeTimers();

    fireEvent.change(input, { target: { value: "Alice" } });

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    fireEvent.click(screen.getByRole("button", { name: "Export CSV" }));
    fireEvent.click(screen.getByRole("button", { name: "Export JSON" }));

    expect(
      (
        getLastMessage("fetchPage")?.payload as
          | { filters?: unknown }
          | undefined
      )?.filters,
    ).toEqual([]);
    expect(
      (
        getLastMessage("exportCSV")?.payload as
          | { filters?: unknown }
          | undefined
      )?.filters,
    ).toEqual([]);
    expect(
      (
        getLastMessage("exportJSON")?.payload as
          | { filters?: unknown }
          | undefined
      )?.filters,
    ).toEqual([]);

    const preDebounceFetchCount = fetchMessageCount();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(399);
    });

    expect(fetchMessageCount()).toBe(preDebounceFetchCount);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });

    expect(fetchMessageCount()).toBeGreaterThan(preDebounceFetchCount);
    expect(
      (
        getLastMessage("fetchPage")?.payload as
          | { filters?: unknown }
          | undefined
      )?.filters,
    ).toEqual(activeFilters);

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    fireEvent.click(screen.getByRole("button", { name: "Export CSV" }));
    fireEvent.click(screen.getByRole("button", { name: "Export JSON" }));

    expect(
      (
        getLastMessage("fetchPage")?.payload as
          | { filters?: unknown }
          | undefined
      )?.filters,
    ).toEqual(activeFilters);
    expect(
      (
        getLastMessage("exportCSV")?.payload as
          | { filters?: unknown }
          | undefined
      )?.filters,
    ).toEqual(activeFilters);
    expect(
      (
        getLastMessage("exportJSON")?.payload as
          | { filters?: unknown }
          | undefined
      )?.filters,
    ).toEqual(activeFilters);
  });

  it("clears pending edits after a full successful apply", async () => {
    await renderEditableTable();
    await editCellValue("Alice", "Alicia");

    expect(screen.getByText(/1 row with unsaved changes/i)).toBeDefined();

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastMessage("applyChanges")?.payload).toEqual({
      updates: [
        {
          primaryKeys: { id: 1 },
          changes: { name: "Alicia" },
        },
      ],
    });

    emit("applyResult", {
      success: true,
      rowOutcomes: [{ rowIndex: 0, success: true, status: "applied" }],
    });
    emit("tableData", {
      rows: [{ id: 1, name: "Alicia" }],
      totalCount: 1,
    });

    await waitFor(() => {
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
    });

    expect(screen.queryByText(/unsaved changes/i)).toBeNull();
    expect(screen.queryByText(/could not be confirmed exactly/i)).toBeNull();
    expect(screen.getByText("Alicia")).toBeDefined();
  });

  it("keeps verification-failed rows pending and shows the warning", async () => {
    await renderEditableTable([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
    await editCellValue("Alice", "Alicia");
    await editCellValue("Bob", "Bobby");

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("applyResult", {
      success: true,
      warning: "Some edits were written but could not be confirmed exactly.",
      failedRows: [1],
      rowOutcomes: [
        { rowIndex: 0, success: true, status: "applied" },
        {
          rowIndex: 1,
          success: false,
          status: "verification_failed",
          message: "Rounded by the database.",
        },
      ],
    });
    emit("tableData", {
      rows: [
        { id: 1, name: "Alicia" },
        { id: 2, name: "Bob" },
      ],
      totalCount: 2,
    });

    await waitFor(() => {
      expect(
        screen.getByText(
          /Some edits were written but could not be confirmed exactly\./i,
        ),
      ).toBeDefined();
    });

    expect(screen.getByText(/1 row with unsaved changes/i)).toBeDefined();
    expect(screen.queryByText(/2 rows with unsaved changes/i)).toBeNull();
    expect(screen.getByText("Bobby")).toBeDefined();
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeDefined();
  });

  it("restores retained edits by primary key after the refetch reorders rows", async () => {
    await renderEditableTable([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
    await editCellValue("Alice", "Alicia");
    await editCellValue("Bob", "Bobby");

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("applyResult", {
      success: true,
      warning: "Some edits were written but could not be confirmed exactly.",
      failedRows: [1],
      rowOutcomes: [
        { rowIndex: 0, success: true, status: "applied" },
        {
          rowIndex: 1,
          success: false,
          status: "verification_failed",
          message: "Rounded by the database.",
        },
      ],
    });
    emit("tableData", {
      rows: [
        { id: 2, name: "Bob" },
        { id: 1, name: "Alicia" },
      ],
      totalCount: 2,
    });

    await waitFor(() => {
      expect(screen.getByText(/1 row with unsaved changes/i)).toBeDefined();
    });

    expect(screen.getByText("Bobby")).toBeDefined();
    expect(screen.queryByText("Alicia")).toBeDefined();
    expect(screen.queryByText(/2 rows with unsaved changes/i)).toBeNull();
  });

  it("does not restore retained edits after the user reverts while the apply refetch is in flight", async () => {
    await renderEditableTable();
    await editCellValue("Alice", "Alicia");

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("applyResult", {
      success: true,
      warning: "Some edits were written but could not be confirmed exactly.",
      failedRows: [0],
      rowOutcomes: [
        {
          rowIndex: 0,
          success: false,
          status: "verification_failed",
          message: "Rounded by the database.",
        },
      ],
    });

    await waitFor(() => {
      expect(screen.getByText(/1 row with unsaved changes/i)).toBeDefined();
    });

    fireEvent.click(screen.getByRole("button", { name: "Revert All" }));

    emit("tableData", {
      rows: [{ id: 1, name: "Alice" }],
      totalCount: 1,
    });

    await waitFor(() => {
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
    });

    expect(screen.queryByText(/unsaved changes/i)).toBeNull();
    expect(screen.queryByText(/could not be confirmed exactly/i)).toBeNull();
    expect(screen.getByText("Alice")).toBeDefined();
    expect(screen.queryByText("Alicia")).toBeNull();
  });

  it("keeps edits pending and shows the failure when apply is blocked", async () => {
    await renderEditableTable();
    await editCellValue("Alice", "Alicia");

    const fetchCountBeforeApply = fetchMessageCount();

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("applyResult", {
      success: false,
      error: "One or more edits were rejected before writing.",
      failedRows: [0],
      rowOutcomes: [
        {
          rowIndex: 0,
          success: false,
          status: "prevalidation_failed",
          message: "Scale exceeds the column definition.",
        },
      ],
    });

    await waitFor(() => {
      expect(
        screen.getByText(/One or more edits were rejected before writing\./i),
      ).toBeDefined();
    });

    expect(fetchMessageCount()).toBe(fetchCountBeforeApply);
    expect(screen.getByText("Alicia")).toBeDefined();
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeDefined();
  });

  it("shows a blocking SQL preview for table edits and confirms with previewToken only", async () => {
    await renderEditableTable();
    await editCellValue("Alice", "Alicia");

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("tableMutationPreview", {
      previewToken: "preview-apply-1",
      kind: "applyChanges",
      title: "Apply changes to users",
      sql: 'UPDATE "public"."users"\nSET "name" = \'Alicia\'\nWHERE "id" = 1;',
      statementCount: 1,
    });

    const dialog = await screen.findByRole("dialog", {
      name: "Apply changes to users",
    });
    const cancelButton = within(dialog).getByRole("button", {
      name: "Close SQL preview",
    });

    await waitFor(() => {
      expect(document.activeElement).toBe(cancelButton);
    });

    expect(
      (
        within(dialog).getByRole("textbox", {
          name: "SQL mutation preview",
        }) as HTMLTextAreaElement
      ).value,
    ).toBe('UPDATE "public"."users"\nSET "name" = \'Alicia\'\nWHERE "id" = 1;');

    fireEvent.click(
      within(dialog).getByRole("button", { name: "Apply Changes" }),
    );

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(getLastMessage("confirmMutationPreview")?.payload).toEqual({
      previewToken: "preview-apply-1",
    });
  });

  it("cancels table edit preview without dropping pending edits", async () => {
    await renderEditableTable();
    await editCellValue("Alice", "Alicia");

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("tableMutationPreview", {
      previewToken: "preview-apply-cancel",
      kind: "applyChanges",
      title: "Apply changes to users",
      sql: 'UPDATE "public"."users" SET "name" = \'Alicia\' WHERE "id" = 1;',
      statementCount: 1,
    });

    const dialog = await screen.findByRole("dialog", {
      name: "Apply changes to users",
    });

    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(getLastMessage("cancelMutationPreview")?.payload).toEqual({
      previewToken: "preview-apply-cancel",
    });
    expect(screen.getByText(/1 row with unsaved changes/i)).toBeDefined();
    expect(screen.getByText("Alicia")).toBeDefined();
    expect(screen.queryByText(/cancelled/i)).toBeNull();
  });

  it("cancels table edit preview on Escape without dropping pending edits", async () => {
    await renderEditableTable();
    await editCellValue("Alice", "Alicia");

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("tableMutationPreview", {
      previewToken: "preview-apply-escape",
      kind: "applyChanges",
      title: "Apply changes to users",
      sql: 'UPDATE "public"."users" SET "name" = \'Alicia\' WHERE "id" = 1;',
      statementCount: 1,
    });

    await screen.findByRole("dialog", {
      name: "Apply changes to users",
    });

    fireEvent.keyDown(window, { key: "Escape" });

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(getLastMessage("cancelMutationPreview")?.payload).toEqual({
      previewToken: "preview-apply-escape",
    });
    expect(screen.getByText(/1 row with unsaved changes/i)).toBeDefined();
    expect(screen.getByText("Alicia")).toBeDefined();
  });

  it("cancels insert preview without dropping the drafted row", async () => {
    await renderEditableTable();

    const fetchPayload = getLastMessage("fetchPage")?.payload as
      | { fetchId?: number }
      | undefined;
    if (fetchPayload?.fetchId !== undefined) {
      emit("tableData", {
        fetchId: fetchPayload.fetchId,
        rows: [{ id: 1, name: "Alice" }],
        totalCount: 1,
      });
    }

    await waitFor(() => {
      expect(
        (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
          .disabled,
      ).toBe(false);
    });

    fireEvent.click(screen.getByRole("button", { name: "Add Row" }));

    const newRowLabel = await screen.findByText("New row:");
    const newRowForm = newRowLabel.parentElement as HTMLElement;

    const nameInput = newRowForm.querySelectorAll("input").item(1) as
      | HTMLInputElement
      | undefined;
    expect(nameInput).toBeDefined();

    fireEvent.change(nameInput as HTMLInputElement, {
      target: { value: "Charlie" },
    });

    fireEvent.click(within(newRowForm).getByRole("button", { name: "Insert" }));

    expect(getLastMessage("insertRow")?.payload).toEqual({
      values: { name: "Charlie" },
    });

    emit("tableMutationPreview", {
      previewToken: "preview-insert-1",
      kind: "insertRow",
      title: "Insert row into users",
      sql: 'INSERT INTO "public"."users" ("name")\nVALUES (\'Charlie\');',
      statementCount: 1,
    });

    const dialog = await screen.findByRole("dialog", {
      name: "Insert row into users",
    });

    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(getLastMessage("cancelMutationPreview")?.payload).toEqual({
      previewToken: "preview-insert-1",
    });
    expect(screen.getByDisplayValue("Charlie")).toBeDefined();
    expect(screen.queryByText("Inserting…")).toBeNull();
    expect(screen.queryByText(/insert cancelled/i)).toBeNull();
  });

  it("keeps keyboard focus trapped inside the SQL preview dialog", async () => {
    await renderEditableTable();
    await editCellValue("Alice", "Alicia");

    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));

    emit("tableMutationPreview", {
      previewToken: "preview-apply-focus-trap",
      kind: "applyChanges",
      title: "Apply changes to users",
      sql: 'UPDATE "public"."users" SET "name" = \'Alicia\' WHERE "id" = 1;',
      statementCount: 1,
    });

    const dialog = await screen.findByRole("dialog", {
      name: "Apply changes to users",
    });
    const closeButton = within(dialog).getByRole("button", {
      name: "Close SQL preview",
    });
    const confirmButton = within(dialog).getByRole("button", {
      name: "Apply Changes",
    });

    await waitFor(() => {
      expect(document.activeElement).toBe(closeButton);
    });

    fireEvent.keyDown(dialog, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(confirmButton);

    fireEvent.keyDown(dialog, { key: "Tab" });
    expect(document.activeElement).toBe(closeButton);
  });
});
