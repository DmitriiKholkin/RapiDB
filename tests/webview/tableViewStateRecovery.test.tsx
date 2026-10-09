import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

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
  DEBOUNCE,
  type PersistedTableViewState,
} from "../../src/webview/components/table/tableViewHelpers";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

const tableKey = JSON.stringify(["conn", "db", "public", "items"]);
const nameColumn: ColumnTypeMeta = {
  name: "name",
  type: "TEXT",
  nativeType: "TEXT",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  category: "text",
  filterable: true,
  filterOperators: ["eq", "like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};
const idColumn: ColumnTypeMeta = {
  ...nameColumn,
  name: "id",
  type: "INTEGER",
  nativeType: "INTEGER",
  category: "integer",
  nullable: false,
  isPrimaryKey: true,
  filterOperators: ["eq", "gt", "between"],
};

function persistView(view: Omit<PersistedTableViewState, "tableKey">) {
  return persistState({
    unrelated: "retained",
    tableView: { tableKey, ...view },
  });
}

function persistState(initialState: Record<string, unknown>) {
  const api = window.__vscode as unknown as {
    getState: ReturnType<typeof vi.fn>;
    setState: ReturnType<typeof vi.fn>;
    postMessage: ReturnType<typeof vi.fn>;
  };
  let state = JSON.parse(JSON.stringify(initialState)) as Record<
    string,
    unknown
  >;
  api.getState.mockImplementation(() => state);
  api.setState.mockImplementation((next: Record<string, unknown>) => {
    state = next;
  });
  api.postMessage.mockImplementation((message) => {
    if (message.type === "fetchPage") {
      // Check ordering at the host boundary, not after React effects flush.
      expect(state.tableReadEpoch).toBe(message.payload.fetchId);
    }
  });
  return () => state;
}

function renderTable() {
  return render(
    <TableView
      connectionId="conn"
      database="db"
      schema="public"
      table="items"
    />,
  );
}

function fetches() {
  return getPostedMessages()
    .filter((message) => message.type === "fetchPage")
    .map(
      (message) =>
        message.payload as {
          fetchId: number;
          page: number;
          pageSize: number;
          sort: PersistedTableViewState["sort"];
          filters: unknown[];
        },
    );
}

function initialize(columns: ColumnTypeMeta[]) {
  act(() => {
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });
  });
  expect(fetches()).toHaveLength(1);
  return fetches()[0];
}

function lastFetch() {
  const fetch = fetches().at(-1);
  if (!fetch) throw new Error("Expected a fetchPage request");
  return fetch;
}

function isButtonDisabled(name: string) {
  return (screen.getByRole("button", { name }) as HTMLButtonElement).disabled;
}

function commit(fetchId: number) {
  act(() => {
    dispatchIncomingMessage("tableData", {
      fetchId,
      rows: [{ id: 1, name: "Alice", old_name: "Legacy" }],
      totalCount: 400,
    });
  });
  expect(screen.getByRole("table")).toBeTruthy();
}

function mountThirdPage(
  sort: PersistedTableViewState["sort"] = { column: "id", direction: "desc" },
) {
  const view = {
    page: 3,
    pageSize: 100,
    sort,
    filters: { name: { operator: "like" as const, value: "Alice" } },
  };
  const persisted = persistView(view);
  const first = renderTable();
  commit(initialize([idColumn, nameColumn]).fetchId);
  expect(screen.getByText("Page 3 of 4")).toBeTruthy();
  clearPostedMessages();
  return { first, persisted, view };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("TableView persisted state recovery", () => {
  it.each([
    ["before-debounce", "Bob"],
    ["pending-read", "Bob"],
    ["before-debounce", "   "],
    ["pending-read", "   "],
    ["before-debounce", "null"],
    ["pending-read", "null"],
  ])("restores coherent filter intent %s with draft %s", (timing, value) => {
    vi.useFakeTimers();
    const { first, persisted, view } = mountThirdPage();
    const nullFilter = value === "null";
    if (nullFilter) {
      fireEvent.click(
        screen.getByRole("button", { name: "name filter operator" }),
      );
      fireEvent.click(screen.getByRole("menuitemradio", { name: /Is NULL/ }));
    } else {
      fireEvent.change(screen.getByLabelText("name filter value"), {
        target: { value },
      });
      // Persisting page 1 must not cause an early live read with old filters.
      expect(fetches()).toHaveLength(0);
    }
    const draft = nullFilter
      ? { operator: "is_null" }
      : { operator: "like", value };
    expect(persisted().tableView).toEqual({
      tableKey,
      ...view,
      page: 1,
      filters: { name: draft },
    });
    if (timing === "pending-read") {
      act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    }
    const expectedFilters = nullFilter
      ? [{ column: "name", operator: "is_null" }]
      : value.trim()
        ? [{ column: "name", operator: "like", value }]
        : [];
    const oldRead = fetches().at(-1);
    if (nullFilter || timing === "pending-read") {
      expect(fetches()).toHaveLength(1);
      expect(oldRead).toMatchObject({ page: 1, filters: expectedFilters });
    }
    // Displayed rows/page still belong to the old read, not the saved intent.
    expect(screen.getByText("Page 3 of 4")).toBeTruthy();
    first.unmount();
    clearPostedMessages();
    renderTable();
    const restored = initialize([idColumn, nameColumn]);
    expect(restored).toMatchObject({
      page: 1,
      pageSize: 100,
      sort: view.sort,
      filters: expectedFilters,
    });
    if (oldRead) {
      act(() => {
        dispatchIncomingMessage("tableData", {
          fetchId: oldRead.fetchId,
          rows: [],
          totalCount: 0,
        });
      });
      expect(screen.queryByRole("table")).toBeNull();
    }
    act(() => {
      dispatchIncomingMessage("tableData", {
        fetchId: restored.fetchId,
        rows: [{ id: 1, name: "Bob" }],
        totalCount: 1,
      });
    });
    expect(screen.getByText("Page 1 of 1")).toBeTruthy();
    expect(persisted().tableView).toEqual({
      tableKey,
      ...view,
      page: 1,
      filters: { name: draft },
    });
    if (!nullFilter) {
      expect(
        (screen.getByLabelText("name filter value") as HTMLInputElement).value,
      ).toBe(value);
    }
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toEqual([restored]);
  });

  it.each([
    "next",
    "previous",
    "page-size",
    "sort-asc",
    "sort-desc",
    "sort-clear",
  ])("restores requested %s intent before its read response", (action) => {
    vi.useFakeTimers();
    const initialSort: PersistedTableViewState["sort"] | undefined =
      action === "sort-desc" || action === "sort-clear"
        ? {
            column: "name",
            direction: action === "sort-desc" ? "asc" : "desc",
          }
        : undefined;
    const { first, persisted, view } = mountThirdPage(initialSort);
    let expectedPage = 1;
    let expectedSize = view.pageSize;
    let expectedSort: PersistedTableViewState["sort"] = view.sort;
    if (action === "next" || action === "previous") {
      expectedPage = action === "next" ? 4 : 2;
      fireEvent.click(
        screen.getByRole("button", {
          name: action === "next" ? "Next →" : "← Prev",
        }),
      );
    } else if (action === "page-size") {
      expectedSize = 25;
      fireEvent.change(screen.getByLabelText("Rows per page"), {
        target: { value: "25" },
      });
    } else {
      fireEvent.click(screen.getByRole("columnheader", { name: "name" }));
      expectedSort =
        action === "sort-clear"
          ? null
          : {
              column: "name",
              direction: action === "sort-asc" ? "asc" : "desc",
            };
    }
    const expected = {
      tableKey,
      ...view,
      page: expectedPage,
      pageSize: expectedSize,
      sort: expectedSort,
    };
    expect(persisted().tableView).toEqual(expected);
    expect(lastFetch()).toMatchObject({
      page: expectedPage,
      pageSize: expectedSize,
      sort: expectedSort,
    });
    expect(screen.getByText("Page 3 of 4")).toBeTruthy();
    first.unmount();
    clearPostedMessages();
    renderTable();
    const restored = initialize([idColumn, nameColumn]);
    expect(restored).toMatchObject({
      page: expectedPage,
      pageSize: expectedSize,
      sort: expectedSort,
      filters: [{ column: "name", operator: "like", value: "Alice" }],
    });
    commit(restored.fetchId);
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toEqual([restored]);
    expect(persisted().tableView).toEqual(expected);
  });

  it("does not overwrite a pending filter's page intent when an older-filter read commits", () => {
    vi.useFakeTimers();
    const { first, persisted } = mountThirdPage();
    fireEvent.change(screen.getByLabelText("name filter value"), {
      target: { value: "Bob" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    const previousFilters = lastFetch();
    expect(previousFilters).toMatchObject({
      page: 3,
      filters: [{ column: "name", operator: "like", value: "Alice" }],
    });
    commit(previousFilters.fetchId);
    expect(persisted().tableView).toMatchObject({
      page: 1,
      filters: { name: { operator: "like", value: "Bob" } },
    });
    first.unmount();
    clearPostedMessages();
    renderTable();
    expect(initialize([idColumn, nameColumn])).toMatchObject({
      page: 1,
      filters: [{ column: "name", operator: "like", value: "Bob" }],
    });
  });

  it.each([
    [false, "tableData", "tableData"],
    [false, "tableError", "tableError"],
    [true, "tableData", "tableError"],
    [true, "tableError", "tableData"],
  ] as const)("fences same-columns reconnect (committed=%s, old=%s, current=%s)", (committed, oldType, currentType) => {
    const persisted = persistState({ unrelated: "retained" });
    renderTable();
    const initial = initialize([idColumn, nameColumn]);
    if (committed) {
      commit(initial.fetchId);
      fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    }
    const old = lastFetch();
    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    const deliverOld = () =>
      dispatchIncomingMessage(
        oldType,
        oldType === "tableData"
          ? {
              fetchId: old.fetchId,
              rows: [{ id: 91, name: "OLD SESSION" }],
              totalCount: 1,
            }
          : { fetchId: old.fetchId, error: "OLD SESSION ERROR" },
      );
    act(() => dispatchIncomingMessage("tableConnectionInvalidated", {}));
    const invalidationEpoch = persisted().tableReadEpoch as number;
    expect(invalidationEpoch).toBeGreaterThan(old.fetchId);
    // A committed dataset is no longer covered by the abandoned read's overlay.
    // Before the first dataset, the fullscreen placeholder waits for new metadata.
    if (committed)
      expect(
        screen.queryByRole("status", { name: "Loading data..." }),
      ).toBeNull();
    act(deliverOld);
    expect(screen.queryByText("OLD SESSION")).toBeNull();
    expect(screen.queryByText("OLD SESSION ERROR")).toBeNull();
    const count = fetches().length;
    act(() =>
      dispatchIncomingMessage("tableInit", {
        intent: "connectionRefresh",
        columns: [idColumn, nameColumn],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(fetches()).toHaveLength(count + 1);
    const current = lastFetch();
    expect(current.fetchId).toBeGreaterThan(invalidationEpoch);
    expect(persisted().tableReadEpoch).toBe(current.fetchId);
    act(deliverOld);
    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    act(() =>
      dispatchIncomingMessage(
        currentType,
        currentType === "tableData"
          ? {
              fetchId: current.fetchId,
              rows: [{ id: 2, name: "CURRENT SESSION" }],
              totalCount: 1,
            }
          : { fetchId: current.fetchId, error: "CURRENT SESSION ERROR" },
      ),
    );
    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();
    expect(
      screen.getByText(
        currentType === "tableData"
          ? "CURRENT SESSION"
          : "CURRENT SESSION ERROR",
      ),
    ).toBeTruthy();
    act(deliverOld);
    expect(screen.queryByText("OLD SESSION")).toBeNull();
    expect(screen.queryByText("OLD SESSION ERROR")).toBeNull();
  });

  it("automatically reads after reconnect without verifying a restored unknown mutation or losing drafts", () => {
    const persisted = persistState({
      tableReadEpoch: 40,
      unrelated: "retained",
      tableDraft: {
        tableKey,
        restoreState: { entries: [] },
        newRows: [{ name: { value: "Unverified draft" } }],
        mutation: { kind: "apply", unknown: true },
      },
    });
    renderTable();
    commit(initialize([idColumn, nameColumn]).fetchId);
    expect(isButtonDisabled("Apply Changes")).toBe(true);
    expect(isButtonDisabled("Add Row")).toBe(true);
    act(() => {
      dispatchIncomingMessage("tableConnectionInvalidated", {});
      dispatchIncomingMessage("tableInit", {
        intent: "connectionRefresh",
        columns: [idColumn, nameColumn],
        primaryKeyColumns: ["id"],
      });
    });
    expect(fetches()).toHaveLength(2);
    commit(lastFetch().fetchId);
    expect(isButtonDisabled("Apply Changes")).toBe(true);
    expect(isButtonDisabled("Add Row")).toBe(true);
    expect(persisted()).toMatchObject({
      unrelated: "retained",
      tableDraft: {
        newRows: [{ name: { value: "Unverified draft" } }],
        mutation: { kind: "apply", unknown: true },
      },
    });
    // Only an explicit user Refresh may verify this restored fence.
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    expect(fetches()).toHaveLength(3);
    commit(lastFetch().fetchId);
    expect(isButtonDisabled("Apply Changes")).toBe(false);
    expect(isButtonDisabled("Add Row")).toBe(false);
  });

  it.each([
    ["tableData", "tableData"],
    ["tableError", "tableData"],
    ["tableData", "tableError"],
    ["tableError", "tableError"],
  ])("rejects old %s after remount until current verification %s", (oldType, currentType) => {
    const persisted = persistState({ unrelated: "retained" });
    const first = renderTable();
    commit(initialize([idColumn, nameColumn]).fetchId);
    fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const request = getPostedMessages().find(
      (message) => message.type === "applyChanges",
    )?.payload as { operationId: string };
    expect(request.operationId).toBeTruthy();
    act(() => {
      dispatchIncomingMessage("applyResult", {
        operationId: request.operationId,
        success: false,
        error: "Acknowledgement lost",
        changesPossible: true,
        outcomeUnknown: true,
      });
    });
    const oldVerification = lastFetch();
    expect(oldVerification.fetchId).toBe(3);
    expect(persisted()).toMatchObject({
      tableReadEpoch: oldVerification.fetchId,
      tableDraft: {
        mutation: { kind: "apply", unknown: true },
        newRows: [expect.any(Object)],
      },
    });
    first.unmount();

    // Fresh API and JSON state model a webview reload as well as a React remount.
    window.__vscode = {
      postMessage: vi.fn(),
      getState: vi.fn(),
      setState: vi.fn(),
    };
    const reloadedState = persistState(persisted());
    renderTable();
    const initial = initialize([idColumn, nameColumn]);
    expect(initial.fetchId).toBeGreaterThan(oldVerification.fetchId);
    commit(initial.fetchId);
    expect(isButtonDisabled("Apply Changes")).toBe(true);
    expect(isButtonDisabled("Add Row")).toBe(true);

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    const current = lastFetch();
    expect(current.fetchId).toBeGreaterThan(initial.fetchId);
    expect(current.fetchId).not.toBe(oldVerification.fetchId);
    const deliverOld = () => {
      dispatchIncomingMessage(
        oldType,
        oldType === "tableData"
          ? {
              fetchId: oldVerification.fetchId,
              rows: [{ id: 91, name: "OLD VERIFICATION" }],
              totalCount: 1,
            }
          : { fetchId: oldVerification.fetchId, error: "OLD READ ERROR" },
      );
    };
    act(deliverOld);
    expect(screen.queryByText("OLD VERIFICATION")).toBeNull();
    expect(screen.queryByText("OLD READ ERROR")).toBeNull();
    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    expect(isButtonDisabled("Apply Changes")).toBe(true);

    // An uncorrelated legacy response must not verify a mutation either.
    act(() => {
      dispatchIncomingMessage("tableData", {
        rows: [{ id: 92, name: "UNCORRELATED READ" }],
        totalCount: 1,
      });
      dispatchIncomingMessage("tableError", { error: "UNCORRELATED ERROR" });
    });
    expect(screen.queryByText("UNCORRELATED READ")).toBeNull();
    expect(screen.queryByText("UNCORRELATED ERROR")).toBeNull();
    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    act(() => {
      if (currentType === "tableData") {
        dispatchIncomingMessage("tableData", {
          fetchId: current.fetchId,
          rows: [{ id: 1, name: "CURRENT VERIFICATION" }],
          totalCount: 1,
        });
      } else {
        dispatchIncomingMessage("tableError", {
          fetchId: current.fetchId,
          error: "CURRENT READ ERROR",
        });
      }
    });
    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();
    expect(isButtonDisabled("Apply Changes")).toBe(
      currentType === "tableError",
    );
    act(deliverOld);
    expect(screen.queryByText("OLD VERIFICATION")).toBeNull();
    expect(screen.queryByText("OLD READ ERROR")).toBeNull();
    expect(reloadedState()).toMatchObject({
      unrelated: "retained",
      tableReadEpoch: current.fetchId,
      tableDraft: {
        mutation: { kind: "apply", unknown: true },
        newRows: [expect.any(Object)],
      },
    });
    if (currentType === "tableError") {
      expect(screen.getByText("CURRENT READ ERROR")).toBeTruthy();
      // A failed current response is terminal; its late success cannot verify.
      act(() => {
        dispatchIncomingMessage("tableData", {
          fetchId: current.fetchId,
          rows: [{ id: 1, name: "LATE FAILED READ" }],
          totalCount: 1,
        });
      });
      expect(screen.queryByText("LATE FAILED READ")).toBeNull();
      expect(isButtonDisabled("Apply Changes")).toBe(true);
      fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
      const retry = lastFetch();
      expect(retry.fetchId).toBeGreaterThan(current.fetchId);
      commit(retry.fetchId);
    } else {
      expect(screen.getByText("CURRENT VERIFICATION")).toBeTruthy();
    }
    expect(isButtonDisabled("Apply Changes")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(
      getPostedMessages().filter((message) => message.type === "applyChanges"),
    ).toHaveLength(1);
  });

  it("reserves init and metadata invalidation epochs synchronously across remount", () => {
    const persisted = persistState({
      tableReadEpoch: 40,
      unrelated: "retained",
    });
    const first = renderTable();
    const initial = initialize([idColumn, nameColumn]);
    expect(initial.fetchId).toBe(42);
    commit(initial.fetchId);
    fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
    act(() => {
      dispatchIncomingMessage("tableInit", {
        columns: [idColumn],
        primaryKeyColumns: ["id"],
        intent: "metadataRefresh",
      });
    });
    expect(fetches()).toHaveLength(1);
    expect(persisted().tableReadEpoch).toBe(43);
    first.unmount();
    clearPostedMessages();
    renderTable();
    expect(initialize([idColumn, nameColumn]).fetchId).toBe(45);
    expect(persisted().tableReadEpoch).toBe(45);
  });

  it.each([
    Number.MAX_SAFE_INTEGER,
    Number.MAX_SAFE_INTEGER - 1,
    -1,
    1.5,
    "3",
  ])("fails closed rather than reset invalid/exhausted read epoch %s", (epoch) => {
    persistState({
      tableReadEpoch: epoch,
      tableDraft: {
        tableKey,
        restoreState: { entries: [] },
        newRows: [{ name: { value: "Unverified draft" } }],
        mutation: { kind: "apply", unknown: true },
      },
    });
    renderTable();
    act(() => {
      dispatchIncomingMessage("tableInit", {
        columns: [idColumn, nameColumn],
        primaryKeyColumns: ["id"],
      });
    });
    expect(fetches()).toHaveLength(0);
    expect(
      screen.getByText(/Table read identity is invalid or exhausted/),
    ).toBeTruthy();
    act(() => {
      dispatchIncomingMessage("tableData", {
        fetchId: 2,
        rows: [{ id: 1, name: "OLD DATA" }],
        totalCount: 1,
      });
    });
    expect(screen.queryByRole("table")).toBeNull();
    expect(screen.queryByText("OLD DATA")).toBeNull();
  });

  it("does not emit a read or unlock restored work if synchronous identity persistence fails", () => {
    persistState({
      tableReadEpoch: 3,
      tableDraft: {
        tableKey,
        restoreState: { entries: [] },
        newRows: [{ name: { value: "Unverified draft" } }],
        mutation: { kind: "apply", unknown: true },
      },
    });
    renderTable();
    const api = window.__vscode;
    if (!api) throw new Error("VS Code API is unavailable");
    vi.mocked(api.setState).mockImplementation(() => {
      throw new Error("State storage unavailable");
    });
    act(() => {
      dispatchIncomingMessage("tableInit", {
        columns: [idColumn, nameColumn],
        primaryKeyColumns: ["id"],
      });
    });
    expect(fetches()).toHaveLength(0);
    expect(screen.getByText("State storage unavailable")).toBeTruthy();
    act(() => {
      dispatchIncomingMessage("tableData", {
        rows: [{ id: 1, name: "UNCORRELATED DATA" }],
        totalCount: 1,
      });
    });
    expect(screen.queryByRole("table")).toBeNull();
  });

  it.each([
    "deleted",
    "renamed",
    "case-changed",
  ])("drops %s persisted names before the remount fetch", (change) => {
    vi.useFakeTimers();
    const view = {
      page: 3,
      pageSize: 100,
      sort: { column: "old_name", direction: "desc" as const },
      filters: {
        old_name: { operator: "like" as const, value: "Legacy" },
        name: { operator: "like" as const, value: "Alice" },
      },
    };
    const persisted = persistView(view);
    const first = renderTable();
    const initial = initialize([
      idColumn,
      nameColumn,
      { ...nameColumn, name: "old_name" },
    ]);
    commit(initial.fetchId);
    first.unmount();
    clearPostedMessages();

    renderTable();
    expect(fetches()).toHaveLength(0);
    const currentColumns = [idColumn, nameColumn];
    if (change !== "deleted") {
      currentColumns.push({
        ...nameColumn,
        name: change === "renamed" ? "new_name" : "OLD_NAME",
      });
    }
    const restored = initialize(currentColumns);
    expect(restored).toMatchObject({
      page: 3,
      pageSize: 100,
      sort: null,
      filters: [{ column: "name", operator: "like", value: "Alice" }],
    });
    commit(restored.fetchId);
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toEqual([restored]);
    expect(persisted()).toMatchObject({
      unrelated: "retained",
      tableView: { ...view, sort: null, filters: { name: view.filters.name } },
    });
    expect(
      screen.getByRole("textbox", { name: "name filter value" }),
    ).toBeTruthy();
  });

  it("preserves valid sort, filters and incomplete drafts without resetting the restored page", () => {
    vi.useFakeTimers();
    const view = {
      page: 3,
      pageSize: 100,
      sort: { column: "name", direction: "desc" as const },
      filters: {
        name: { operator: "like" as const, value: " Alice " },
        id: {
          operator: "between" as const,
          value: ["1", ""] as [string, string],
        },
      },
    };
    const persisted = persistView(view);
    const first = renderTable();
    commit(initialize([idColumn, nameColumn]).fetchId);
    first.unmount();
    clearPostedMessages();

    renderTable();
    const restored = initialize([nameColumn, idColumn]);
    expect(restored).toMatchObject({
      page: 3,
      pageSize: 100,
      sort: view.sort,
      filters: [{ column: "name", operator: "like", value: "Alice" }],
    });
    commit(restored.fetchId);
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toEqual([restored]);
    expect(persisted()).toMatchObject({ tableView: { tableKey, ...view } });
    expect(screen.getByText("Page 3 of 4")).toBeTruthy();

    fireEvent.change(
      screen.getByRole("textbox", { name: "name filter value" }),
      {
        target: { value: "Bob" },
      },
    );
    act(() => {
      dispatchIncomingMessage("tableInit", {
        columns: [nameColumn, idColumn],
        primaryKeyColumns: ["id"],
      });
    });
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toHaveLength(2);
    expect(fetches()[1]).toMatchObject({
      page: 1,
      sort: view.sort,
      filters: [{ column: "name", operator: "like", value: "Bob" }],
    });
  });

  it("removes unsupported and non-filterable value filters but retains supported null checks", () => {
    vi.useFakeTimers();
    const view = {
      page: 1,
      pageSize: 25,
      sort: null,
      filters: {
        id: { operator: "gt" as const, value: "1" },
        name: { operator: "like" as const, value: "Alice" },
        optional: { operator: "is_null" as const },
      },
    };
    const persisted = persistView(view);
    const first = renderTable();
    commit(
      initialize([idColumn, nameColumn, { ...nameColumn, name: "optional" }])
        .fetchId,
    );
    first.unmount();
    clearPostedMessages();

    renderTable();
    const restored = initialize([
      { ...idColumn, filterOperators: ["eq"] },
      { ...nameColumn, filterable: false },
      { ...nameColumn, name: "optional", filterable: false },
    ]);
    expect(restored.filters).toEqual([
      { column: "optional", operator: "is_null" },
    ]);
    commit(restored.fetchId);
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toEqual([restored]);
    expect(persisted()).toMatchObject({
      tableView: { filters: { optional: { operator: "is_null" } } },
    });
    expect(
      Object.keys((persisted().tableView as PersistedTableViewState).filters),
    ).toEqual(["optional"]);
  });

  it("preserves an existing exact-name sort when initial metadata has duplicate names", () => {
    persistView({
      page: 1,
      pageSize: 25,
      sort: { column: "name", direction: "asc" },
      filters: {},
    });
    const first = renderTable();
    commit(initialize([idColumn, nameColumn]).fetchId);
    first.unmount();
    clearPostedMessages();

    renderTable();
    const restored = initialize([idColumn, nameColumn, { ...nameColumn }]);
    expect(restored.sort).toEqual({ column: "name", direction: "asc" });
    expect(restored.filters).toEqual([]);
  });
});
