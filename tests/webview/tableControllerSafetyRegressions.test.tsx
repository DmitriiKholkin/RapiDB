import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
} from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableMutationStatusBar } from "../../src/webview/components/table/TableStatusBanners";
import {
  buildInsertValues,
  buildPendingRestoreState,
  INSERT_DEFAULT_SENTINEL,
  parsePersistedTableDraft,
  restorePendingEditsSafely,
  serializeTableDraft,
} from "../../src/webview/components/table/tableViewHelpers";
import { useTableDataController } from "../../src/webview/components/table/useTableDataController";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import type { Row } from "../../src/webview/types";
import { dispatchIncomingMessage, getPostedMessages } from "./testUtils";

const tableKey = JSON.stringify(["conn", "db", "public", "items"]);
const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    isForeignKey: false,
    category: "integer",
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  },
  {
    name: "name",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  },
];

function lastFetchId(): number {
  const payload = getPostedMessages()
    .filter((message) => message.type === "fetchPage")
    .at(-1)?.payload as { fetchId: number };
  return payload.fetchId;
}

function mountMutationController(rows: Row[]) {
  const refs = {
    loadingRef: { current: false },
    columnsRef: { current: columns },
    fetchPageRef: { current: vi.fn() },
    pkColsRef: { current: ["id"] },
    preserveScrollPositionRef: { current: vi.fn() },
    rowsRef: { current: [] as Row[] },
    mongoIdTypesRef: { current: [] },
    rowValueTypesRef: { current: [] },
  };
  const hook = renderHook(() =>
    useTableMutationController({
      ...refs,
      canEditRows: true,
      selected: new Set<number>(),
    }),
  );
  const commit = (nextRows: Row[]) =>
    act(() => {
      hook.result.current.handleRowsCommitted(nextRows, ["id"], [], []);
      refs.rowsRef.current = nextRows;
    });
  commit(rows);
  return Object.assign(hook, { commit });
}

function pressUndo(target: EventTarget = document.body) {
  let event: KeyboardEvent | undefined;
  act(() => {
    event = new KeyboardEvent("keydown", {
      code: "KeyZ",
      key: "z",
      ctrlKey: true,
      bubbles: true,
      cancelable: true,
    });
    target.dispatchEvent(event);
  });
  return event as KeyboardEvent;
}

function mountWithTwoDrafts() {
  const hook = mountMutationController([{ id: 1, name: "Alice" }]);
  act(() => hook.result.current.startInsertRow());
  act(() => hook.result.current.commitDraftCellEdit(0, columns[1], '{"a":1}'));
  act(() => hook.result.current.startInsertRow());
  expect(hook.result.current.newRows).toHaveLength(2);
  return hook;
}

describe("AUD-005: undo/redo while a modal dialog is open", () => {
  it("performs undo on Ctrl+Z when no dialog is open", () => {
    const hook = mountWithTwoDrafts();
    const event = pressUndo();
    expect(event.defaultPrevented).toBe(true);
    expect(hook.result.current.newRows).toHaveLength(1);
  });

  it("does not intercept Ctrl+Z inside an input", () => {
    const hook = mountWithTwoDrafts();
    const input = document.createElement("input");
    document.body.appendChild(input);
    const event = pressUndo(input);
    input.remove();
    expect(event.defaultPrevented).toBe(false);
    expect(hook.result.current.newRows).toHaveLength(2);
  });

  it("ignores Ctrl+Z while any aria-modal dialog is open, regardless of focus", () => {
    const hook = mountWithTwoDrafts();
    const modal = document.createElement("div");
    modal.setAttribute("role", "dialog");
    modal.setAttribute("aria-modal", "true");
    document.body.appendChild(modal);
    const event = pressUndo(document.body);
    modal.remove();
    expect(event.defaultPrevented).toBe(false);
    expect(hook.result.current.newRows).toHaveLength(2);
  });

  it("ignores table undo shortcuts while the structured dialog is open and closes it on history restore", () => {
    const hook = mountWithTwoDrafts();
    act(() =>
      hook.result.current.openStructuredCellDialog({
        rowKind: "draft",
        rowIdx: 1,
        column: columns[1],
        value: { kind: "json", language: "json", formattedText: '{"a":1}' },
        currentValue: '{"a":1}',
        originalValue: undefined,
        readOnly: false,
      }),
    );
    expect(hook.result.current.structuredCellDialog?.rowIdx).toBe(1);

    pressUndo(document.body);
    pressUndo(window);

    expect(hook.result.current.newRows).toHaveLength(2);
    expect(hook.result.current.structuredCellDialog).not.toBeNull();

    act(() => hook.result.current.undoAction());
    expect(hook.result.current.newRows).toHaveLength(1);
    expect(hook.result.current.structuredCellDialog).toBeNull();
  });
});

describe("unresolved pending edits", () => {
  it("discards unresolved edits locally and unblocks further editing", () => {
    const hook = mountMutationController([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
    act(() => hook.result.current.commitCellEdit(1, columns[1], "x", "Bob"));
    hook.commit([{ id: 1, name: "Alice" }]);
    expect(hook.result.current.unresolvedPendingEdits).toHaveLength(1);
    act(() => hook.result.current.startInsertRow());
    expect(hook.result.current.newRows).toHaveLength(0);

    act(() => hook.result.current.discardUnresolvedPendingEdits());
    expect(hook.result.current.unresolvedPendingEdits).toHaveLength(0);
    expect(hook.result.current.pendingEdits.size).toBe(0);

    act(() => hook.result.current.startInsertRow());
    expect(hook.result.current.newRows).toHaveLength(1);
    act(() => hook.result.current.undoAction());
    expect(hook.result.current.newRows).toHaveLength(0);
    expect(hook.result.current.unresolvedPendingEdits).toHaveLength(0);
  });

  it("explains that edits without row identity cannot be matched by refresh", () => {
    const onDiscardUnresolved = vi.fn();
    render(
      <TableMutationStatusBar
        applyStatus={null}
        applying={false}
        loading={false}
        insertValueCount={0}
        mutErr={null}
        newRowExists={false}
        readOnlyTable={false}
        unsavedRowCount={1}
        unresolvedPendingCount={1}
        unmatchablePendingCount={1}
        onApplyChanges={vi.fn()}
        onDismissApplyStatus={vi.fn()}
        onDismissMutationError={vi.fn()}
        onRevertChanges={vi.fn()}
        onDiscardUnresolved={onDiscardUnresolved}
      />,
    );
    expect(
      screen.getByText(/lost the original row identity.*even after refresh/),
    ).toBeTruthy();
    expect(screen.queryByText(/Refresh to check again/)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Discard Unmatched" }));
    expect(onDiscardUnresolved).toHaveBeenCalledTimes(1);
  });
});

describe("AUD-006: filter errors with pending edits", () => {
  function fetchCount() {
    return getPostedMessages().filter((message) => message.type === "fetchPage")
      .length;
  }

  function mountDataController(hasWork: () => boolean) {
    const initialView = {
      tableKey,
      page: 3,
      pageSize: 25,
      sort: null,
      filters: {},
    };
    const params = {
      pasteStructureEpochRef: { current: 0 },
      initialPageSize: 25,
      initialView,
      readOnlyTable: false,
      columnsRef: { current: [] as ColumnTypeMeta[] },
      rowsRef: { current: [] as Row[] },
      mongoIdTypesRef: { current: [] },
      rowValueTypesRef: { current: [] },
      pkColsRef: { current: [] as string[] },
      scrollRef: { current: null },
      fetchPageRef: { current: () => undefined },
      preserveScrollPositionRef: { current: () => undefined },
    };
    const hook = renderHook(() =>
      useTableDataController({
        ...params,
        onTableInit: () => undefined,
        getMetadataRefreshState: () => ({ busy: false, hasWork: hasWork() }),
        isFilterNavigationBlocked: () => hasWork(),
        onReadFailed: () => undefined,
        onRowsCommitted: () => undefined,
      }),
    );
    act(() =>
      dispatchIncomingMessage("tableInit", {
        columns,
        primaryKeyColumns: ["id"],
      }),
    );
    act(() =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchId(),
        rows: [
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
        ],
        totalCount: 52,
      }),
    );
    expect(hook.result.current.page).toBe(3);
    return hook;
  }

  it("keeps committed rows, page and fetch state while unsaved work exists", () => {
    const hook = mountDataController(() => true);
    act(() => hook.result.current.fetchPage());
    const fetchesBeforeError = fetchCount();
    act(() =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetchId(),
        error: "Invalid filter",
        isFilterError: true,
      }),
    );
    expect(hook.result.current.filterError).toBe("Invalid filter");
    expect(hook.result.current.rows).toHaveLength(2);
    expect(hook.result.current.totalCount).toBe(52);
    expect(hook.result.current.page).toBe(3);
    expect(hook.result.current.requestedPage).toBe(3);
    expect(fetchCount()).toBe(fetchesBeforeError);
  });

  it("still clears rows on a filter error without unsaved work", () => {
    const hook = mountDataController(() => false);
    act(() => hook.result.current.fetchPage());
    act(() =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetchId(),
        error: "Invalid filter",
        isFilterError: true,
      }),
    );
    expect(hook.result.current.filterError).toBe("Invalid filter");
    expect(hook.result.current.rows).toHaveLength(0);
    expect(hook.result.current.page).toBe(1);
  });

  it("keeps edits for missing rows as unresolved restore entries", () => {
    const pending = new Map([[3, new Map<string, unknown>([["name", "x"]])]]);
    const restoreState = buildPendingRestoreState(
      pending,
      [{ id: 1, name: "Alice" }],
      ["id"],
    );
    expect(restoreState.entries).toHaveLength(1);

    const serialized = JSON.parse(
      JSON.stringify(serializeTableDraft(tableKey, restoreState, [])),
    ) as unknown;
    const parsed = parsePersistedTableDraft(serialized, tableKey);
    const restored = restorePendingEditsSafely(
      parsed?.restoreState ?? null,
      [{ id: 1, name: "Alice" }],
      ["id"],
    );
    expect(restored.pendingEdits.size).toBe(0);
    expect(restored.unresolved).toHaveLength(1);
    expect(restored.unresolved[0].changes).toEqual(new Map([["name", "x"]]));
  });
});

describe("AUD-008: read-only mutation status bar", () => {
  it("shows pending info and Revert All without Apply in read-only mode", () => {
    const onRevertChanges = vi.fn();
    render(
      <TableMutationStatusBar
        applyStatus={{
          tone: "warning",
          message:
            "Apply or revert pending changes before loading different rows.",
        }}
        applying={false}
        loading={false}
        insertValueCount={0}
        mutErr={null}
        newRowExists={false}
        readOnlyTable
        unsavedRowCount={2}
        unresolvedPendingCount={1}
        unresolvedPendingColumns={["name"]}
        canUndo
        onApplyChanges={vi.fn()}
        onDismissApplyStatus={vi.fn()}
        onDismissMutationError={vi.fn()}
        onRevertChanges={onRevertChanges}
      />,
    );

    expect(screen.getByText(/2 rows with unsaved changes/)).toBeTruthy();
    expect(screen.getByText(/Unable to match 1 row/)).toBeTruthy();
    expect(
      screen.getByText(/Apply or revert pending changes before loading/),
    ).toBeTruthy();
    expect(screen.queryByText("Apply Changes")).toBeNull();
    expect(screen.queryByRole("button", { name: "Undo" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Revert All" }));
    expect(onRevertChanges).toHaveBeenCalledTimes(1);
  });
});

describe("AUD-027: insert default sentinel", () => {
  it("treats the old typeable sentinel text as a literal value", () => {
    expect(INSERT_DEFAULT_SENTINEL).toContain("\u0000");
    expect(
      buildInsertValues({
        name: { value: "__RAPIDB_INSERT_DEFAULT__" },
        id: { value: INSERT_DEFAULT_SENTINEL },
      }),
    ).toEqual({ name: "__RAPIDB_INSERT_DEFAULT__" });
  });

  it("keeps the old sentinel text literal in a current-format draft after reload", () => {
    const serialized = JSON.parse(
      JSON.stringify(
        serializeTableDraft(tableKey, { entries: [] }, [
          {
            id: { value: INSERT_DEFAULT_SENTINEL },
            name: { value: "__RAPIDB_INSERT_DEFAULT__" },
          },
        ]),
      ),
    ) as unknown;
    const parsed = parsePersistedTableDraft(serialized, tableKey);
    expect(parsed?.newRows).toEqual([
      {
        id: { value: INSERT_DEFAULT_SENTINEL },
        name: { value: "__RAPIDB_INSERT_DEFAULT__" },
      },
    ]);
    expect(buildInsertValues(parsed?.newRows[0] ?? {})).toEqual({
      name: "__RAPIDB_INSERT_DEFAULT__",
    });
  });

  it("migrates persisted drafts that contain the legacy sentinel to DEFAULT", () => {
    const parsed = parsePersistedTableDraft(
      {
        tableKey,
        restoreState: { entries: [] },
        newRows: [
          {
            id: { value: "__RAPIDB_INSERT_DEFAULT__" },
            name: { value: "kept" },
          },
        ],
      },
      tableKey,
    );
    expect(parsed?.newRows).toEqual([
      { id: { value: INSERT_DEFAULT_SENTINEL }, name: { value: "kept" } },
    ]);
    expect(buildInsertValues(parsed?.newRows[0] ?? {})).toEqual({
      name: "kept",
    });
  });

  it("round-trips the new sentinel through persisted state", () => {
    const serialized = JSON.parse(
      JSON.stringify(
        serializeTableDraft(tableKey, { entries: [] }, [
          { id: { value: INSERT_DEFAULT_SENTINEL } },
        ]),
      ),
    ) as unknown;
    expect(parsePersistedTableDraft(serialized, tableKey)?.newRows).toEqual([
      { id: { value: INSERT_DEFAULT_SENTINEL } },
    ]);
  });
});
