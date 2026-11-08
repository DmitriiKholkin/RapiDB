import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, type Mock, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableView } from "../../src/webview/components/TableView";
import { EditInput } from "../../src/webview/components/table/EditInput";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import { PAGE_SIZES } from "../../src/webview/components/table/tableViewHelpers";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
  getPostedMessages,
} from "./testUtils";

vi.mock("@tanstack/react-virtual", () => ({
  defaultRangeExtractor: (range: { startIndex: number; endIndex: number }) =>
    Array.from(
      { length: range.endIndex - range.startIndex + 1 },
      (_, index) => range.startIndex + index,
    ),
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 26,
        end: (index + 1) * 26,
      })),
    getTotalSize: () => count * 26,
    scrollToIndex: () => undefined,
  }),
}));

function col(
  name: string,
  category: ColumnTypeMeta["category"],
  extra: Partial<ColumnTypeMeta> = {},
): ColumnTypeMeta {
  return {
    name,
    type: category.toUpperCase(),
    nativeType: category.toUpperCase(),
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category,
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
    ...extra,
  } as ColumnTypeMeta;
}

const columns = [
  col("id", "integer", { isPrimaryKey: true, nullable: false }),
  col("qty", "integer"),
];
const extraColumn = col("extra", "text");

function fetchPages() {
  return getPostedMessages().filter(
    (message) => message.type === "fetchPage",
  ) as Array<{ type: string; payload: Record<string, unknown> }>;
}

function lastFetch() {
  return fetchPages().at(-1);
}

async function replyToFetch(
  rows: Record<string, unknown>[],
  totalCount: number,
  extra: Record<string, unknown> = {},
) {
  const fetchId = lastFetch()?.payload.fetchId;
  await act(async () => {
    dispatchIncomingMessage("tableData", {
      fetchId,
      rows,
      totalCount,
      ...extra,
    });
  });
}

async function initTable(
  rows: Record<string, unknown>[],
  totalCount = rows.length,
  extra: Record<string, unknown> = {},
) {
  const view = render(
    <TableView connectionId="c" database="d" schema="s" table="t" />,
  );
  act(() => {
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });
  });
  await waitFor(() => expect(lastFetch()).toBeTruthy());
  await replyToFetch(rows, totalCount, extra);
  await waitFor(() => expect(screen.getByRole("table")).toBeTruthy());
  return view;
}

async function refreshConnection() {
  await act(async () => {
    dispatchIncomingMessage("tableConnectionInvalidated", undefined);
  });
  await act(async () => {
    dispatchIncomingMessage("tableInit", {
      intent: "connectionRefresh",
      columns,
      primaryKeyColumns: ["id"],
    });
  });
}

function bodyCell(columnName: string, rowIndex = 0) {
  const table = screen.getByRole("table");
  const headers = Array.from(table.querySelectorAll("thead tr:first-child th"));
  const index = headers.findIndex(
    (header) => header.getAttribute("data-column-id") === columnName,
  );
  return table.querySelectorAll("tbody tr")[rowIndex].querySelectorAll("td")[
    index
  ] as HTMLTableCellElement;
}

describe("Mongo page-discovered columns across metadata refresh", () => {
  it("keeps pending edits applicable when refreshed metadata omits a page-discovered column", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    fireEvent.doubleClick(bodyCell("qty"));
    const input = await screen.findByLabelText("Cell value");
    fireEvent.change(input, { target: { value: "6" } });
    fireEvent.keyDown(input, { key: "Enter" });
    clearPostedMessages();

    await refreshConnection();

    await waitFor(() => expect(lastFetch()).toBeTruthy());
    await replyToFetch([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    expect(screen.queryByText(/Connection changed|Schema conflict/)).toBeNull();
    expect(screen.getByText("1 row with unsaved changes")).toBeTruthy();
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
    expect(
      document.querySelector('thead th[data-column-id="extra"]'),
    ).not.toBeNull();
  });

  it("clears a filter on a page-discovered column after a refresh and says so", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    const filterInput = screen.getByLabelText("extra filter value");
    fireEvent.change(filterInput, { target: { value: "x" } });
    fireEvent.keyDown(filterInput, { key: "Enter" });
    await waitFor(() =>
      expect(JSON.stringify(lastFetch()?.payload.filters)).toContain("extra"),
    );
    await act(async () => {
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetch()?.payload.fetchId,
        error: "[RapiDB Filter] Unknown filter column extra.",
        isFilterError: false,
      });
    });
    clearPostedMessages();

    await refreshConnection();

    await waitFor(() => expect(lastFetch()).toBeTruthy());
    expect(lastFetch()?.payload.filters).toEqual([]);
    await replyToFetch([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    expect(screen.getByRole("table")).toBeTruthy();
    expect(
      (screen.getByLabelText("extra filter value") as HTMLInputElement).value,
    ).toBe("");
    expect(
      screen.getByText(/Cleared the filter on "extra": fields found only/),
    ).toBeTruthy();

    clearPostedMessages();
    await refreshConnection();
    await waitFor(() => expect(lastFetch()).toBeTruthy());
    expect(lastFetch()?.payload.filters).toEqual([]);
  });

  it("says so when a refresh drops a not-yet-applied filter on a page-discovered column", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    clearPostedMessages();
    fireEvent.change(screen.getByLabelText("extra filter value"), {
      target: { value: "x" },
    });

    await refreshConnection();

    await waitFor(() => expect(lastFetch()).toBeTruthy());
    expect(lastFetch()?.payload.filters).toEqual([]);
    await replyToFetch([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    expect(
      (screen.getByLabelText("extra filter value") as HTMLInputElement).value,
    ).toBe("");
    expect(
      screen.getByText(/Cleared the filter on "extra": fields found only/),
    ).toBeTruthy();
  });

  it("does not report a cleared filter for an emptied filter input", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    const filterInput = screen.getByLabelText("extra filter value");
    fireEvent.change(filterInput, { target: { value: "x" } });
    fireEvent.change(filterInput, { target: { value: "" } });
    clearPostedMessages();

    await refreshConnection();

    await waitFor(() => expect(lastFetch()).toBeTruthy());
    await replyToFetch([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    expect(screen.queryByText(/Cleared the filter/)).toBeNull();
  });

  it("keeps a sort on a page-discovered column after a refresh without pending work", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    fireEvent.click(
      document.querySelector(
        'thead tr:first-child th[data-column-id="extra"]',
      ) as HTMLElement,
    );
    await waitFor(() =>
      expect(lastFetch()?.payload.sort).toEqual({
        column: "extra",
        direction: "asc",
      }),
    );
    await replyToFetch([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    clearPostedMessages();

    await refreshConnection();

    await waitFor(() => expect(lastFetch()).toBeTruthy());
    expect(lastFetch()?.payload.sort).toEqual({
      column: "extra",
      direction: "asc",
    });
  });

  it("keeps filter controls when the filtered refetch after a refresh fails", async () => {
    await initTable([{ id: 1, qty: 5 }]);
    const filterInput = screen.getByLabelText("qty filter value");
    fireEvent.change(filterInput, { target: { value: "5" } });
    fireEvent.keyDown(filterInput, { key: "Enter" });
    await waitFor(() =>
      expect(JSON.stringify(lastFetch()?.payload.filters)).toContain("qty"),
    );
    await replyToFetch([{ id: 1, qty: 5 }], 1);
    clearPostedMessages();

    await refreshConnection();

    await waitFor(() =>
      expect(JSON.stringify(lastFetch()?.payload.filters)).toContain("qty"),
    );
    await act(async () => {
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetch()?.payload.fetchId,
        error: "Invalid MongoDB date filter value",
        isFilterError: false,
      });
    });
    expect(screen.getByRole("table")).toBeTruthy();
    expect(screen.getByText(/Invalid MongoDB date filter value/)).toBeTruthy();
    expect(document.querySelectorAll("tbody td[data-row]")).toHaveLength(0);
    expect(screen.queryByText("5")).toBeNull();
    const addRow = screen.queryByRole("button", { name: "Add Row" });
    expect(addRow === null || (addRow as HTMLButtonElement).disabled).toBe(
      true,
    );
    const retainedInput = screen.getByLabelText(
      "qty filter value",
    ) as HTMLInputElement;
    expect(retainedInput.value).toBe("5");

    clearPostedMessages();
    fireEvent.change(retainedInput, { target: { value: "" } });
    fireEvent.keyDown(retainedInput, { key: "Enter" });
    await waitFor(() => expect(lastFetch()?.payload.filters).toEqual([]));
    await replyToFetch([{ id: 1, qty: 5 }], 1);
    expect(bodyCell("qty").textContent).toBe("5");
  });

  it("still reports a conflict when refreshed metadata drops an edited schema column", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    fireEvent.doubleClick(bodyCell("qty"));
    const input = await screen.findByLabelText("Cell value");
    fireEvent.change(input, { target: { value: "6" } });
    fireEvent.keyDown(input, { key: "Enter" });
    clearPostedMessages();

    await act(async () => {
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [columns[0]],
        primaryKeyColumns: ["id"],
      });
    });

    expect(await screen.findByText(/Schema conflict/)).toBeTruthy();
    expect(fetchPages()).toHaveLength(0);
  });

  it("still reports a conflict when a page-discovered column reappears with another type", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    fireEvent.doubleClick(bodyCell("qty"));
    const input = await screen.findByLabelText("Cell value");
    fireEvent.change(input, { target: { value: "6" } });
    fireEvent.keyDown(input, { key: "Enter" });
    clearPostedMessages();

    await act(async () => {
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...columns, col("extra", "integer")],
        primaryKeyColumns: ["id"],
      });
    });

    expect(await screen.findByText(/Schema conflict/)).toBeTruthy();
  });

  it("keeps a page-discovered column through a plain metadata refresh with pending edits", async () => {
    await initTable([{ id: 1, qty: 5, extra: "x" }], 1, {
      extraColumns: [extraColumn],
    });
    fireEvent.doubleClick(bodyCell("qty"));
    const input = await screen.findByLabelText("Cell value");
    fireEvent.change(input, { target: { value: "6" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await act(async () => {
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...columns, col("note", "text")],
        primaryKeyColumns: ["id"],
      });
    });

    await waitFor(() =>
      expect(
        document.querySelector('thead th[data-column-id="note"]'),
      ).not.toBeNull(),
    );
    expect(screen.queryByText(/Schema conflict/)).toBeNull();
    expect(
      document.querySelector('thead th[data-column-id="extra"]'),
    ).not.toBeNull();
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
  });
});

describe("page clamping after the row count shrinks", () => {
  it("steps back to the last valid page without looping", async () => {
    const pageSize = PAGE_SIZES[0];
    await initTable([{ id: 1, qty: 1 }], pageSize * 3);
    expect(lastFetch()?.payload.pageSize).toBe(pageSize);
    fireEvent.click(screen.getByRole("button", { name: "Next →" }));
    await waitFor(() => expect(lastFetch()?.payload.page).toBe(2));
    await replyToFetch([{ id: 2, qty: 1 }], pageSize * 3);
    fireEvent.click(screen.getByRole("button", { name: "Next →" }));
    await waitFor(() => expect(lastFetch()?.payload.page).toBe(3));
    clearPostedMessages();

    await replyToFetch([], pageSize + 1);
    await waitFor(() => expect(lastFetch()?.payload.page).toBe(2));
    expect(fetchPages()).toHaveLength(1);

    clearPostedMessages();
    await replyToFetch([], 0);
    await waitFor(() => expect(lastFetch()?.payload.page).toBe(1));
    expect(fetchPages()).toHaveLength(1);

    clearPostedMessages();
    await replyToFetch([], 0);
    await act(async () => {});
    expect(fetchPages()).toHaveLength(0);
    expect(screen.getByText("Page 1 of 1")).toBeTruthy();
  });

  it("does not step back when the page past the count still has rows", async () => {
    const pageSize = PAGE_SIZES[0];
    await initTable([{ id: 1, qty: 1 }], pageSize * 3);
    fireEvent.click(screen.getByRole("button", { name: "Next →" }));
    await waitFor(() => expect(lastFetch()?.payload.page).toBe(2));
    clearPostedMessages();

    await replyToFetch([{ id: 2, qty: 1 }], 1);
    await act(async () => {});
    expect(fetchPages()).toHaveLength(0);
    expect(bodyCell("id").textContent).toBe("2");
  });

  it("does not step back while edits are pending", async () => {
    const pageSize = PAGE_SIZES[0];
    await initTable([{ id: 1, qty: 1 }], pageSize * 3);
    fireEvent.click(screen.getByRole("button", { name: "Next →" }));
    await waitFor(() => expect(lastFetch()?.payload.page).toBe(2));
    await replyToFetch([{ id: 2, qty: 1 }], pageSize * 3);
    fireEvent.doubleClick(bodyCell("qty"));
    const input = await screen.findByLabelText("Cell value");
    fireEvent.change(input, { target: { value: "6" } });
    fireEvent.keyDown(input, { key: "Enter" });
    clearPostedMessages();

    await refreshConnection();
    await waitFor(() => expect(lastFetch()?.payload.page).toBe(2));
    clearPostedMessages();

    await replyToFetch([], 1);
    await act(async () => {});
    expect(fetchPages()).toHaveLength(0);
  });
});

describe("header drag released away from the header", () => {
  it("does not swallow the next sort click", async () => {
    await initTable([{ id: 1, qty: 5 }]);
    const header = document.querySelector(
      'thead tr:first-child th[data-column-id="qty"]',
    ) as HTMLElement;
    fireEvent.mouseDown(header, { button: 0, clientX: 100 });
    fireEvent.mouseMove(document, { clientX: 200 });
    fireEvent.mouseUp(document, { clientX: 200 });
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    clearPostedMessages();

    fireEvent.click(header);

    await waitFor(() =>
      expect(lastFetch()?.payload.sort).toEqual({
        column: "qty",
        direction: "asc",
      }),
    );
  });
});

describe("EditInput IME composition", () => {
  it("ignores Enter and Escape while composing", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial=""
        nullable
        category="text"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByLabelText("Cell value");
    fireEvent.change(input, { target: { value: "にほん" } });
    fireEvent.keyDown(input, { key: "Enter", isComposing: true });
    fireEvent.keyDown(input, { key: "Enter", keyCode: 229 });
    fireEvent.keyDown(input, { key: "Escape", isComposing: true });
    expect(onCommit).not.toHaveBeenCalled();
    expect(onCancel).not.toHaveBeenCalled();

    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledWith("にほん");
  });
});

function renderDraftGrid(
  newRows: Array<Record<string, { value: string }>>,
  callbacks: {
    onBatchCellEdit: Mock<() => undefined>;
    onMixedBatchEdit: Mock<() => undefined>;
  },
) {
  return render(
    <TableGrid
      canEditRows
      canSelectAndDeleteRows={false}
      colSizes={{}}
      columns={["a", "b"].map((name) => col(name, "text"))}
      editCell={null}
      filterDrafts={{}}
      loading={false}
      loadingRef={{ current: false }}
      fetchEpochRef={{ current: 0 }}
      pasteStructureEpochRef={{ current: 0 }}
      newRows={newRows}
      onCancelEdit={vi.fn()}
      onBatchCellEdit={callbacks.onBatchCellEdit}
      onCommitCellEdit={vi.fn()}
      onCommitDraftCellEdit={vi.fn()}
      onMixedBatchEdit={callbacks.onMixedBatchEdit}
      onFilterDraftChange={vi.fn()}
      onSelectionChange={vi.fn()}
      onSort={vi.fn()}
      onOpenStructuredCell={vi.fn()}
      onStartDraftEdit={vi.fn()}
      onStartEdit={vi.fn()}
      pendingEdits={new Map()}
      rows={[
        { a: "old-a0", b: "old-b0" },
        { a: "old-a1", b: "old-b1" },
      ]}
      scrollRef={createRef<HTMLDivElement>()}
      selected={new Set<number>()}
      sort={null}
    />,
  );
}

function pasteIntoFirstDraft(container: HTMLElement, text: string) {
  const draftRow = -container.querySelectorAll(
    'td[data-row^="-"][data-col="0"]',
  ).length;
  const draftCell = container.querySelector<HTMLElement>(
    `td[data-row="${draftRow}"][data-col="0"]`,
  );
  if (!draftCell) throw new Error("Missing draft cell");
  fireEvent.mouseDown(draftCell, { button: 0 });
  fireEvent.mouseUp(document);
  clearPostedMessages();
  fireEvent.paste(window);
  const request = getLastPostedMessage();
  expect(request?.type).toBe("readClipboard");

  act(() => {
    dispatchIncomingMessage("clipboardText", {
      ...(request?.payload as object),
      text,
    });
  });
}

describe("paste starting on a draft row", () => {
  it("does not overwrite persisted rows past the last draft", () => {
    const onBatchCellEdit = vi.fn<() => undefined>();
    const onMixedBatchEdit = vi.fn<() => undefined>();
    const view = renderDraftGrid([{ a: { value: "draft-a" } }], {
      onBatchCellEdit,
      onMixedBatchEdit,
    });
    pasteIntoFirstDraft(
      view.container,
      "new-a0\tnew-b0\nnew-a1\tnew-b1\nnew-a2\tnew-b2",
    );

    expect(onBatchCellEdit).not.toHaveBeenCalled();
    expect(onMixedBatchEdit).not.toHaveBeenCalled();
    expect(screen.getByText(/Paste failed: 1 error$/)).toBeTruthy();
    expect(
      screen.getByText(
        /cannot continue into existing rows: 2 more pasted rows than new rows/,
      ),
    ).toBeTruthy();
  });

  it("fills several drafts exactly without touching persisted rows", () => {
    const onBatchCellEdit = vi.fn<() => undefined>();
    const onMixedBatchEdit = vi.fn<() => undefined>();
    const view = renderDraftGrid(
      [{ a: { value: "draft-a0" } }, { a: { value: "draft-a1" } }],
      { onBatchCellEdit, onMixedBatchEdit },
    );
    pasteIntoFirstDraft(view.container, "new-a0\tnew-b0\nnew-a1\tnew-b1\n");

    expect(onBatchCellEdit).not.toHaveBeenCalled();
    expect(onMixedBatchEdit).toHaveBeenCalledTimes(1);
    const [draftEdits, persistedEdits] = onMixedBatchEdit.mock
      .calls[0] as unknown as [
      Array<{ rowIdx: number; column: ColumnTypeMeta; newVal: string }>,
      unknown[],
    ];
    expect(persistedEdits).toEqual([]);
    expect(
      draftEdits.map((edit) => [edit.rowIdx, edit.column.name, edit.newVal]),
    ).toEqual([
      [0, "a", "new-a0"],
      [0, "b", "new-b0"],
      [1, "a", "new-a1"],
      [1, "b", "new-b1"],
    ]);
    expect(screen.queryByText(/Paste failed/)).toBeNull();
  });
});
