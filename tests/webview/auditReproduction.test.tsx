import { act, renderHook } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import { dispatchIncomingMessage, getPostedMessages } from "./testUtils";

it("B05: sends a DEFAULT-only draft as an empty insert and retains it on failure", () => {
  const idColumn: ColumnTypeMeta = {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
  const fetchPage = vi.fn();
  const { result } = renderHook(() =>
    useTableMutationController({
      canEditRows: true,
      loadingRef: { current: false },
      columnsRef: { current: [idColumn] },
      fetchPageRef: { current: fetchPage },
      pkColsRef: { current: ["id"] },
      preserveScrollPositionRef: { current: () => {} },
      rowsRef: { current: [] },
      mongoIdTypesRef: { current: [] },
      selected: new Set(),
    }),
  );
  act(() => result.current.startInsertRow());
  const drafts = result.current.newRows;
  act(() => result.current.applyChanges());
  const request = getPostedMessages().find(
    (message) => message.type === "applyChanges",
  );
  expect(request?.payload).toEqual({
    operationId: "table-mutation:1",
    updates: [],
    insertValues: [{}],
  });

  act(() =>
    dispatchIncomingMessage("applyResult", {
      operationId: "table-mutation:1",
      success: false,
      error: "Constraint failed",
    }),
  );
  expect(result.current.newRows).toEqual(drafts);
  expect(result.current.applying).toBe(false);
  expect(result.current.applyStatus?.tone).toBe("error");
  expect(fetchPage).not.toHaveBeenCalled();

  act(() => result.current.applyChanges());
  expect(getPostedMessages().at(-1)?.payload).toEqual({
    operationId: "table-mutation:2",
    updates: [],
    insertValues: [{}],
  });
  act(() =>
    dispatchIncomingMessage("applyResult", {
      operationId: "table-mutation:2",
      success: true,
      insertApplied: true,
    }),
  );
  expect(result.current.newRows).toEqual([]);
  expect(fetchPage).toHaveBeenCalledOnce();
});
