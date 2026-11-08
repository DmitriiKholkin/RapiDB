import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

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

type StateApi = {
  getState: ReturnType<typeof vi.fn>;
  setState: ReturnType<typeof vi.fn>;
};

function stubWebviewState(initial: Record<string, unknown>) {
  const api = window.__vscode as unknown as StateApi;
  let current = initial;
  api.getState.mockImplementation(() => current);
  api.setState.mockImplementation((next: Record<string, unknown>) => {
    current = next;
  });
  return () => current;
}

function renderController() {
  return renderHook(() =>
    useTableMutationController({
      tableDraftKey: "draft-key",
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
}

describe("PNL-2 table session key persistence", () => {
  it("stores the session key from each table init in the persisted initial state", () => {
    const state = stubWebviewState({
      initialState: { view: "table", connectionSessionKey: "old" },
      tableView: { page: 2 },
    });
    renderController();

    act(() =>
      dispatchIncomingMessage("tableInit", {
        columns: [column],
        primaryKeyColumns: ["id"],
        connectionSessionKey: "new",
      }),
    );
    expect(state()).toEqual({
      initialState: { view: "table", connectionSessionKey: "new" },
      tableView: { page: 2 },
    });

    act(() =>
      dispatchIncomingMessage("tableInit", {
        columns: [column],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(
      (state().initialState as { connectionSessionKey: string })
        .connectionSessionKey,
    ).toBe("new");
  });

  it("reports a clean state when the host assumed a restored draft that could not be used", () => {
    stubWebviewState({ tableDraft: { tableKey: "another-table" } });
    clearPostedMessages();
    renderController();
    expect(
      getPostedMessages().filter(
        (message) => message.type === "unsavedChangesState",
      ),
    ).toEqual([
      { type: "unsavedChangesState", payload: { hasUnsavedChanges: false } },
    ]);
  });
});
