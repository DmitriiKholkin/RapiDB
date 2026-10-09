import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
} from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import type { Row } from "../../src/webview/types";

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
vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: () => null,
}));

import { TableView } from "../../src/webview/components/TableView";
import {
  parsePersistedTableDraft,
  serializeTableDraft,
} from "../../src/webview/components/table/tableViewHelpers";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

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
let state: Record<string, unknown>;

function api() {
  const vscode = window.__vscode;
  if (!vscode) throw new Error("VS Code API is unavailable");
  return vscode;
}

beforeEach(() => {
  state = { unrelated: "preserved" };
  vi.mocked(api().getState).mockImplementation(() => state);
  vi.mocked(api().setState).mockImplementation((next) => {
    state = JSON.parse(JSON.stringify(next));
    return next;
  });
});

function messages(type: string) {
  return getPostedMessages().filter((message) => message.type === type);
}
function lastPayload(type: string): Record<string, unknown> {
  return messages(type).at(-1)?.payload as Record<string, unknown>;
}
function draft() {
  return parsePersistedTableDraft(state.tableDraft, tableKey);
}
function commitTableRows(rows: Row[]) {
  act(() =>
    dispatchIncomingMessage("tableData", {
      fetchId: lastPayload("fetchPage").fetchId,
      rows,
      totalCount: rows.length,
    }),
  );
}
function mountTable(rows?: Row[]) {
  const view = render(
    <TableView
      connectionId="conn"
      database="db"
      schema="public"
      table="items"
    />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    }),
  );
  if (rows) commitTableRows(rows);
  return view;
}
function mountController(
  rows: Row[] = [{ id: 1, name: "Alice", extra: "old" }],
  columnDefs = columns,
) {
  const refs = {
    loadingRef: { current: false },
    columnsRef: { current: columnDefs },
    fetchPageRef: { current: vi.fn() },
    pkColsRef: { current: ["id"] },
    preserveScrollPositionRef: { current: vi.fn() },
    rowsRef: { current: [] as Row[] },
    mongoIdTypesRef: { current: [] },
  };
  const initialDraft = draft();
  const hook = renderHook(() =>
    useTableMutationController({
      ...refs,
      initialDraft,
      tableDraftKey: tableKey,
      canEditRows: true,
      selected: new Set([0]),
    }),
  );
  const commit = (nextRows: Row[]) =>
    act(() => {
      hook.result.current.handleRowsCommitted(nextRows, ["id"], [], []);
      refs.rowsRef.current = nextRows;
    });
  act(() => hook.result.current.resetForTableInit());
  commit(rows);
  return { ...hook, refs, commit };
}

describe("table draft recovery across remounts", () => {
  it("preserves an unhydrated UPDATE after first read failure and metadataRefresh retry", () => {
    const first = mountController();
    act(() =>
      first.result.current.commitCellEdit(0, columns[1], "Pending", "Alice"),
    );
    const saved = state.tableDraft;
    expect(draft()?.mutation).toBeUndefined();
    first.unmount();
    clearPostedMessages();
    mountTable();
    act(() =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastPayload("fetchPage").fetchId,
        error: "First read failed",
      }),
    );
    expect(state.tableDraft).toEqual(saved);
    expect(draft()?.restoreState.entries[0].originalValues?.get("name")).toBe(
      "Alice",
    );
    act(() =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...columns, { ...columns[1], name: "extra" }],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(messages("fetchPage")).toHaveLength(2);
    commitTableRows([{ id: 1, name: "Alice", extra: "new" }]);
    expect(screen.getByRole("table").textContent).toContain("Pending");
    expect(draft()?.restoreState.entries[0].changes.get("name")).toBe(
      "Pending",
    );
    expect(draft()?.restoreState.entries[0].originalValues?.get("name")).toBe(
      "Alice",
    );
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(lastPayload("applyChanges").updates).toEqual([
      {
        primaryKeys: { id: 1 },
        changes: { name: "Pending" },
        originalValues: { name: "Alice" },
      },
    ]);
  });

  it.each([
    undefined,
    null,
    "scalar",
    { nested: [1, "two"] },
  ])("preserves an own __proto__ INSERT value through JSON, remount and preview (%j)", (value) => {
    const protoColumns = [columns[0], { ...columns[1], name: "__proto__" }];
    state.tableDraft = JSON.parse(
      JSON.stringify(
        serializeTableDraft(tableKey, { entries: [] }, [
          { ["__proto__"]: { value } },
        ]),
      ),
    );
    const first = mountController([], protoColumns);
    expect(Object.hasOwn(first.result.current.newRows[0], "__proto__")).toBe(
      true,
    );
    expect(first.result.current.newRows[0].__proto__.value).toEqual(value);
    expect(Object.getPrototypeOf(first.result.current.newRows[0])).toBe(
      Object.prototype,
    );
    first.unmount();
    const restored = mountController([], protoColumns);
    expect(Object.hasOwn(restored.result.current.newRows[0], "__proto__")).toBe(
      true,
    );
    expect(restored.result.current.newRows[0].__proto__.value).toEqual(value);
    act(() => restored.result.current.applyChanges());
    const payload = lastPayload("applyChanges");
    const values = (payload.insertValues as Record<string, unknown>[])[0];
    expect(Object.hasOwn(values, "__proto__")).toBe(true);
    expect(values.__proto__).toEqual(value);
    expect(Object.getPrototypeOf(values)).toBe(Object.prototype);
    act(() =>
      dispatchIncomingMessage("tableMutationPreview", {
        operationId: payload.operationId,
        kind: "applyChanges",
        previewToken: "proto-preview",
        title: "INSERT",
        sql: "INSERT",
        statementCount: 1,
      }),
    );
    act(() => restored.result.current.confirmMutationPreview());
    expect(lastPayload("confirmMutationPreview")).toEqual({
      operationId: payload.operationId,
      previewToken: "proto-preview",
    });
    expect(messages("applyChanges")).toHaveLength(1);
    expect(Object.hasOwn(draft()?.newRows[0] ?? {}, "__proto__")).toBe(true);
    expect(draft()?.newRows[0].__proto__.value).toEqual(value);
  });

  it("persists before INSERT dispatch and fences a delayed successful INSERT after remount", () => {
    const view = mountTable([{ id: 1, name: "Alice" }]);
    fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
    const post = vi.mocked(api().postMessage);
    post.mockImplementation((message) => {
      if ((message as { type: string }).type === "applyChanges") {
        expect(draft()?.mutation).toMatchObject({
          kind: "apply",
          unknown: true,
        });
        expect(draft()?.newRows).toHaveLength(1);
      }
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const operationId = lastPayload("applyChanges").operationId;
    view.unmount();
    clearPostedMessages();

    // The initial fresh read happens before the server's delayed INSERT.
    const restored = mountTable([{ id: 1, name: "Alice" }]);
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(
      screen.getByText(/previous table write may have been applied/),
    ).toBeTruthy();
    act(() =>
      dispatchIncomingMessage("applyResult", { operationId, success: true }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(messages("applyChanges")).toHaveLength(0);
    expect(draft()?.mutation).toBeDefined();

    // A failed explicit verification must not unlock writes, even on another remount.
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    act(() =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastPayload("fetchPage").fetchId,
        error: "Read failed",
      }),
    );
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    restored.unmount();
    mountTable([
      { id: 1, name: "Alice" },
      { id: 2, name: null },
    ]);
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    commitTableRows([
      { id: 1, name: "Alice" },
      { id: 2, name: null },
    ]);
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
    expect(messages("applyChanges")).toHaveLength(0);
    // The user verifies the inserted row and abandons the uncertain local draft.
    fireEvent.click(screen.getByRole("button", { name: "Revert All" }));
    expect(state.tableDraft).toBeUndefined();
    expect(state.unrelated).toBe("preserved");
  });

  it("retains a marker without drafts for an in-flight DELETE", () => {
    const first = mountController();
    act(() => first.result.current.deleteSelected());
    expect(draft()?.mutation).toMatchObject({ kind: "delete", unknown: true });
    first.unmount();
    clearPostedMessages();
    const restored = mountController();
    act(() => restored.result.current.deleteSelected());
    expect(messages("deleteRows")).toHaveLength(0);
    expect(restored.result.current.reconciliationRequired).toBe(true);
    act(() => restored.result.current.retryReconciliation());
    restored.commit([]);
    act(() => restored.result.current.revertChanges());
    expect(state.tableDraft).toBeUndefined();
  });

  it.each([
    true,
    false,
  ])("persists an uncertain apply fence after a read (verified=%s)", (verified) => {
    const first = mountController();
    act(() => first.result.current.startInsertRow());
    act(() => first.result.current.applyChanges());
    act(() =>
      dispatchIncomingMessage("applyResult", {
        operationId: lastPayload("applyChanges").operationId,
        success: false,
        changesPossible: true,
        outcomeUnknown: true,
        error: "Acknowledgement lost",
      }),
    );
    if (verified) first.commit([{ id: 1, name: "Alice" }]);
    expect(draft()?.mutation).toEqual({ kind: "apply", unknown: true });
    first.unmount();
    clearPostedMessages();
    const restored = mountController();
    act(() => restored.result.current.applyChanges());
    expect(messages("applyChanges")).toHaveLength(0);
    expect(restored.result.current.reconciliationRequired).toBe(true);
  });

  it.each([
    "success",
    "not-applied",
    "cancel",
  ])("clears the sent marker on confirmed %s", (outcome) => {
    const first = mountController();
    act(() => first.result.current.startInsertRow());
    act(() => first.result.current.applyChanges());
    const operationId = lastPayload("applyChanges").operationId;
    if (outcome === "cancel") {
      act(() =>
        dispatchIncomingMessage("tableMutationPreview", {
          operationId,
          kind: "applyChanges",
          previewToken: "preview",
          title: "INSERT",
          sql: "INSERT",
          statementCount: 1,
        }),
      );
      act(() => first.result.current.cancelMutationPreview());
    } else {
      act(() =>
        dispatchIncomingMessage("applyResult", {
          operationId,
          success: outcome === "success",
          changesPossible: false,
        }),
      );
    }
    expect(draft()?.mutation).toBeUndefined();
    if (outcome === "success") expect(state.tableDraft).toBeUndefined();
    else expect(draft()?.newRows).toHaveLength(1);
  });

  it("restores a known partial-write fence until an explicit verification read", () => {
    const first = mountController();
    act(() => first.result.current.startInsertRow());
    act(() => first.result.current.applyChanges());
    act(() =>
      dispatchIncomingMessage("applyResult", {
        operationId: lastPayload("applyChanges").operationId,
        success: false,
        insertApplied: true,
        changesPossible: true,
      }),
    );
    expect(draft()?.newRows).toHaveLength(0);
    expect(draft()?.mutation).toEqual({ kind: "apply", unknown: false });
    first.unmount();
    const restored = mountController();
    expect(restored.result.current.reconciliationRequired).toBe(true);
    act(() => restored.result.current.startInsertRow());
    expect(restored.result.current.newRows).toHaveLength(0);
    act(() => restored.result.current.retryReconciliation());
    restored.commit([
      { id: 1, name: "Alice" },
      { id: 2, name: null },
    ]);
    expect(restored.result.current.reconciliationRequired).toBe(false);
    expect(state.tableDraft).toBeUndefined();
  });

  it("keeps the original edited-field baseline when only another field changes", () => {
    const first = mountController();
    act(() =>
      first.result.current.commitCellEdit(0, columns[1], "Pending", "Alice"),
    );
    expect(draft()?.restoreState.entries[0].originalValues).toEqual(
      new Map([["name", "Alice"]]),
    );
    first.unmount();
    const restored = mountController([
      { id: 1, name: "Alice", extra: "concurrent" },
    ]);
    expect(restored.result.current.unresolvedPendingEdits).toHaveLength(0);
    act(() => restored.result.current.applyChanges());
    expect(lastPayload("applyChanges").updates).toEqual([
      {
        primaryKeys: { id: 1 },
        changes: { name: "Pending" },
        originalValues: { name: "Alice" },
      },
    ]);
  });

  it("does not rebase concurrent edited-field changes after repeated remounts or refreshes", () => {
    const first = mountController();
    act(() =>
      first.result.current.commitCellEdit(0, columns[1], "Pending", "Alice"),
    );
    first.unmount();
    clearPostedMessages();
    const restored = mountController([{ id: 1, name: "Concurrent" }]);
    expect(restored.result.current.pendingEdits.size).toBe(0);
    expect(restored.result.current.unresolvedPendingEdits).toHaveLength(1);
    act(() => restored.result.current.applyChanges());
    restored.commit([{ id: 1, name: "Concurrent again" }]);
    restored.unmount();
    const again = mountController([{ id: 1, name: "Concurrent again" }]);
    act(() => again.result.current.applyChanges());
    expect(messages("applyChanges")).toHaveLength(0);
    expect(draft()?.restoreState.entries[0].originalValues?.get("name")).toBe(
      "Alice",
    );
    expect(draft()?.restoreState.entries[0].changes.get("name")).toBe(
      "Pending",
    );
    // Only the original baseline, not an arbitrary fresh row, can restore it.
    again.commit([{ id: 1, name: "Alice" }]);
    expect(again.result.current.unresolvedPendingEdits).toHaveLength(0);
    act(() => again.result.current.applyChanges());
    expect(lastPayload("applyChanges").updates).toMatchObject([
      { originalValues: { name: "Alice" } },
    ]);
  });

  it("shows a conflict and disables Apply in the remounted table instead of overwriting a concurrent field", () => {
    const first = mountController();
    act(() =>
      first.result.current.commitCellEdit(0, columns[1], "Pending", "Alice"),
    );
    first.unmount();
    clearPostedMessages();
    mountTable([{ id: 1, name: "Concurrent client value" }]);
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(screen.getByRole("table").textContent).toContain(
      "Concurrent client value",
    );
    expect(screen.getByRole("table").textContent).not.toContain("Pending");
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    commitTableRows([{ id: 1, name: "Concurrent client value" }]);
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(messages("applyChanges")).toHaveLength(0);
    expect(draft()?.restoreState.entries[0].originalValues?.get("name")).toBe(
      "Alice",
    );
  });

  it.each([
    null,
    { nested: [1, "two"] },
  ])("round-trips a non-string original value (%j)", (original) => {
    const first = mountController([{ id: 1, name: original }]);
    act(() =>
      first.result.current.commitCellEdit(0, columns[1], "Pending", original),
    );
    first.unmount();
    const restored = mountController([
      { id: 1, name: JSON.parse(JSON.stringify(original)) },
    ]);
    expect(restored.result.current.unresolvedPendingEdits).toHaveLength(0);
    act(() => restored.result.current.applyChanges());
    expect(lastPayload("applyChanges").updates).toMatchObject([
      { originalValues: { name: original } },
    ]);
  });

  it("retains legacy edits without original values as unresolved rather than inventing a baseline", () => {
    state.tableDraft = {
      tableKey,
      newRows: [],
      restoreState: {
        entries: [
          {
            originalSignature: JSON.stringify([["id", 1]]),
            changes: [["name", "Pending"]],
          },
        ],
      },
    };
    const restored = mountController();
    act(() => restored.result.current.applyChanges());
    expect(restored.result.current.unresolvedPendingEdits).toHaveLength(1);
    expect(messages("applyChanges")).toHaveLength(0);
    expect(draft()?.restoreState.entries[0].changes.get("name")).toBe(
      "Pending",
    );
  });

  it("does not serialize a missing original value as a NULL baseline", () => {
    const first = mountController([{ id: 1 }]);
    act(() =>
      first.result.current.commitCellEdit(0, columns[1], "Pending", undefined),
    );
    expect(draft()?.restoreState.entries[0].originalValues?.has("name")).toBe(
      true,
    );
    expect(
      draft()?.restoreState.entries[0].originalValues?.get("name"),
    ).toBeUndefined();
    first.unmount();
    const restored = mountController([{ id: 1, name: null }]);
    expect(restored.result.current.pendingEdits.size).toBe(0);
    expect(restored.result.current.unresolvedPendingEdits).toHaveLength(1);
    act(() => restored.result.current.applyChanges());
    expect(messages("applyChanges")).toHaveLength(0);
  });
});
