import { act, fireEvent, render, renderHook } from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
} from "./testUtils";

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
    scrollToIndex: () => undefined,
  }),
}));

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

function setup() {
  const params = {
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
  };
  return renderHook(() => useTableMutationController(params));
}

describe("queued table edits", () => {
  it("applies a delayed clipboard reply after a draft edit is queued but before commit", () => {
    const { result } = setup();
    act(() => result.current.startInsertRow());
    act(() => result.current.commitDraftCellEdit(0, column, "A"));
    const view = render(
      <TableGrid
        canEditRows
        canSelectAndDeleteRows={false}
        colSizes={{}}
        columns={[column]}
        editCell={null}
        filterDrafts={{}}
        loading={false}
        loadingRef={{ current: false }}
        fetchEpochRef={{ current: 0 }}
        newRows={result.current.newRows}
        onCancelEdit={vi.fn()}
        onBatchCellEdit={(edits) => result.current.commitBatchCellEdits(edits)}
        onCommitCellEdit={vi.fn()}
        onCommitDraftCellEdit={vi.fn()}
        onMixedBatchEdit={(draftEdits, persistedEdits) =>
          result.current.commitMixedBatchEdits(draftEdits, persistedEdits)
        }
        onFilterDraftChange={vi.fn()}
        onSelectionChange={vi.fn()}
        onSort={vi.fn()}
        onOpenStructuredCell={vi.fn()}
        onStartDraftEdit={vi.fn()}
        onStartEdit={vi.fn()}
        pendingEdits={new Map()}
        rows={[{ id: 1, value: "A" }]}
        scrollRef={createRef()}
        selected={new Set()}
        sort={null}
      />,
    );
    const draft = view.container.querySelector<HTMLElement>(
      'td[data-row="-1"][data-col="0"]',
    );
    if (!draft) throw new Error("Missing draft cell");
    fireEvent.mouseDown(draft, { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    act(() => {
      result.current.commitDraftCellEdit(0, column, "B");
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "A",
      });
    });
    expect(result.current.newRows[0].value.value).toBe("A");
    act(() => result.current.undoAction());
    expect(result.current.newRows[0].value.value).toBe("B");
  });
  it("applies a mixed paste back to the previous draft value before commit, with undo and redo", () => {
    const { result } = setup();
    act(() => result.current.startInsertRow());
    act(() => result.current.commitDraftCellEdit(0, column, "A"));
    act(() => {
      result.current.commitDraftCellEdit(0, column, "B");
      result.current.commitMixedBatchEdits(
        [{ rowIdx: 0, column, newVal: "A" }],
        [],
      );
    });
    expect(result.current.newRows[0].value.value).toBe("A");
    act(() => result.current.undoAction());
    expect(result.current.newRows[0].value.value).toBe("B");
    act(() => result.current.redoAction());
    expect(result.current.newRows[0].value.value).toBe("A");
  });

  it.each([
    "single",
    "batch",
    "mixed",
  ] as const)("applies queued persisted reversion via %s", (kind) => {
    const { result } = setup();
    act(() => {
      result.current.commitCellEdit(0, column, "B", "A");
      const edit = { rowIdx: 0, column, newVal: "A", originalVal: "A" };
      if (kind === "single") result.current.commitCellEdit(0, column, "A", "A");
      if (kind === "batch") result.current.commitBatchCellEdits([edit]);
      if (kind === "mixed") result.current.commitMixedBatchEdits([], [edit]);
    });
    expect(result.current.pendingEdits.size).toBe(0);
    act(() => result.current.undoAction());
    expect(result.current.pendingEdits.get(0)?.get("value")).toBe("B");
  });

  it.each([
    "single",
    "batch",
  ] as const)("applies queued draft reversion via %s", (kind) => {
    const { result } = setup();
    act(() => result.current.startInsertRow());
    act(() => result.current.commitDraftCellEdit(0, column, "A"));
    act(() => {
      result.current.commitDraftCellEdit(0, column, "B");
      if (kind === "single") result.current.commitDraftCellEdit(0, column, "A");
      else
        result.current.commitBatchDraftCellEdits(0, [{ column, newVal: "A" }]);
    });
    expect(result.current.newRows[0].value.value).toBe("A");
    act(() => result.current.undoAction());
    expect(result.current.newRows[0].value.value).toBe("B");
  });

  it("applies queued changes rather than a stale snapshot", () => {
    const { result } = setup();
    act(() => result.current.startInsertRow());
    clearPostedMessages();
    act(() => {
      result.current.commitDraftCellEdit(0, column, "draft-B");
      result.current.commitCellEdit(0, column, "B", "A");
      result.current.applyChanges();
    });
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        insertValues: [{ value: "draft-B" }],
        updates: [{ primaryKeys: { id: 1 }, changes: { value: "B" } }],
      },
    });
  });

  it("keeps undo and redo ordered when called before commit", () => {
    const { result } = setup();
    act(() => result.current.startInsertRow());
    act(() => result.current.commitDraftCellEdit(0, column, "A"));
    act(() => {
      result.current.commitDraftCellEdit(0, column, "B");
      result.current.undoAction();
      result.current.redoAction();
    });
    expect(result.current.newRows[0].value.value).toBe("B");
    act(() => result.current.undoAction());
    expect(result.current.newRows[0].value.value).toBe("A");
  });

  it("keeps mixed draft and persisted paste atomic across undo", () => {
    const { result } = setup();
    act(() => result.current.startInsertRow());
    act(() => result.current.commitDraftCellEdit(0, column, "A"));
    act(() => {
      result.current.commitDraftCellEdit(0, column, "B");
      result.current.commitCellEdit(0, column, "B", "A");
      result.current.commitMixedBatchEdits(
        [{ rowIdx: 0, column, newVal: "A" }],
        [{ rowIdx: 0, column, newVal: "A", originalVal: "A" }],
      );
    });
    expect(result.current.newRows[0].value.value).toBe("A");
    expect(result.current.pendingEdits.size).toBe(0);
    act(() => result.current.undoAction());
    expect(result.current.newRows[0].value.value).toBe("B");
    expect(result.current.pendingEdits.get(0)?.get("value")).toBe("B");
  });
});
