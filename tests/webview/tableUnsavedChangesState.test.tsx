import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import { clearPostedMessages, getPostedMessages } from "./testUtils";

const column: ColumnTypeMeta = {
  name: "value",
  type: "text",
  nativeType: "text",
  category: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq"],
  valueSemantics: "plain",
};

function unsavedStates() {
  return getPostedMessages()
    .filter((message) => message.type === "unsavedChangesState")
    .map((message) => message.payload);
}

describe("PNL-2 table unsaved-changes state reporting", () => {
  it("reports when pending edits or drafts appear and when they are cleared", () => {
    clearPostedMessages();
    const { result } = renderHook(() =>
      useTableMutationController({
        canEditRows: true,
        loadingRef: { current: false },
        columnsRef: { current: [column] },
        fetchPageRef: { current: vi.fn() },
        pkColsRef: { current: ["id"] },
        preserveScrollPositionRef: { current: vi.fn() },
        rowsRef: { current: [{ id: 1, value: "A" }] },
        mongoIdTypesRef: { current: [] },
        rowValueTypesRef: { current: [] },
        selected: new Set<number>(),
      }),
    );
    expect(unsavedStates()).toEqual([]);

    act(() => result.current.commitCellEdit(0, column, "B", "A"));
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    act(() => result.current.startInsertRow());
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    act(() => result.current.revertChanges());
    expect(unsavedStates()).toEqual([
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
    ]);
  });

  it("reports a restored cell-edit draft as unsaved before the first page loads", () => {
    clearPostedMessages();
    const loadingRef = { current: true };
    const { result } = renderHook(() =>
      useTableMutationController({
        initialDraft: {
          tableKey: "draft-key",
          restoreState: {
            entries: [
              {
                originalSignature: "missing-row",
                changes: new Map([["value", "B"]]),
              },
            ],
          },
          newRows: [],
        },
        tableDraftKey: "draft-key",
        canEditRows: true,
        loadingRef,
        columnsRef: { current: [column] },
        fetchPageRef: { current: vi.fn() },
        pkColsRef: { current: ["id"] },
        preserveScrollPositionRef: { current: vi.fn() },
        rowsRef: { current: [] },
        mongoIdTypesRef: { current: [] },
        rowValueTypesRef: { current: [] },
        selected: new Set<number>(),
      }),
    );
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    act(() => result.current.handleRowsCommitted([], ["id"], [], []));
    expect(result.current.unresolvedPendingEdits).toHaveLength(1);
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    loadingRef.current = false;
    act(() => result.current.revertChanges());
    expect(unsavedStates()).toEqual([
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
    ]);
  });
});
