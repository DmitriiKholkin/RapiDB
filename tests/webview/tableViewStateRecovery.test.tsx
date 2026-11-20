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
  serializeTableDraft,
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
const rejectedSortMessage =
  "Sorting DynamoDB data with the current request requires materializing up to 5000 rows. Narrow the filters or sort by the key-compatible order instead.";

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

function rejectSort(fetchId: number) {
  act(() =>
    dispatchIncomingMessage("tableError", {
      fetchId,
      error: rejectedSortMessage,
      isFilterError: false,
    }),
  );
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
  ])("restores view state after %s, persisting sort changes only on success", (action) => {
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
    expect(persisted().tableView).toEqual({ ...expected, sort: view.sort });
    expect(lastFetch()).toMatchObject({
      page: expectedPage,
      pageSize: expectedSize,
      sort: expectedSort,
    });
    expect(screen.getByText("Page 3 of 4")).toBeTruthy();
    if (action.startsWith("sort-")) {
      const pending = lastFetch();
      act(() =>
        dispatchIncomingMessage("tableData", {
          rows: [{ id: 91, name: "UNCORRELATED SORT RESPONSE" }],
          totalCount: 1,
        }),
      );
      expect(screen.queryByText("UNCORRELATED SORT RESPONSE")).toBeNull();
      expect(persisted().tableView).toMatchObject({ sort: view.sort });
      commit(pending.fetchId);
    }
    expect(persisted().tableView).toEqual(expected);
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

  it.each([
    null,
    { column: "id", direction: "desc" } as const,
  ])("restores the last accepted sort %j instead of a rejected non-key sort", (sort) => {
    const { first, persisted, view } = mountThirdPage(sort);
    fireEvent.click(screen.getByRole("columnheader", { name: "name" }));
    const rejected = lastFetch();
    expect(rejected.sort).toEqual({ column: "name", direction: "asc" });
    rejectSort(rejected.fetchId);
    expect(screen.getByText(rejectedSortMessage)).toBeTruthy();
    expect(screen.getByRole("cell", { name: "Alice" })).toBeTruthy();
    expect(persisted().tableView).toEqual({ tableKey, ...view, page: 1 });

    act(() =>
      dispatchIncomingMessage("tableData", {
        fetchId: rejected.fetchId,
        rows: [{ id: 91, name: "LATE FAILED SORT" }],
        totalCount: 1,
      }),
    );
    expect(screen.queryByText("LATE FAILED SORT")).toBeNull();
    expect(persisted().tableView).toMatchObject({ sort });

    first.unmount();
    clearPostedMessages();
    renderTable();
    const restored = initialize([idColumn, nameColumn]);
    expect(restored).toMatchObject({
      page: 1,
      pageSize: view.pageSize,
      sort,
      filters: [{ column: "name", ...view.filters.name }],
    });
    rejectSort(rejected.fetchId);
    expect(screen.queryByText(rejectedSortMessage)).toBeNull();
    commit(restored.fetchId);
    expect(fetches()).toEqual([restored]);
  });

  it.each([
    null,
    { column: "id", direction: "desc" } as const,
  ])("retries a first read with saved sort %j after a transient error", (sort) => {
    const { first, persisted, view } = mountThirdPage(sort);
    first.unmount();
    renderTable();
    const failed = initialize([idColumn, nameColumn]);
    act(() =>
      dispatchIncomingMessage("tableError", {
        fetchId: failed.fetchId,
        error: "Temporary read failure",
      }),
    );
    expect(screen.getByRole("alert").textContent).toContain(
      "Temporary read failure",
    );
    expect(screen.queryByRole("table")).toBeNull();
    expect(
      Boolean(screen.queryByRole("button", { name: "Clear Sort and Retry" })),
    ).toBe(sort !== null);
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(fetches()).toHaveLength(2);
    const retry = lastFetch();
    expect(retry).toEqual({ ...failed, fetchId: retry.fetchId });
    expect(retry.fetchId).toBeGreaterThan(failed.fetchId);
    expect(screen.queryByRole("alert")).toBeNull();
    commit(retry.fetchId);
    expect(persisted().tableView).toEqual({ tableKey, ...view });
  });

  it("clears a legacy rejected sort from the initial error without losing restored work or retrying a write", () => {
    const view = {
      tableKey,
      page: 3,
      pageSize: 100,
      sort: { column: "name", direction: "asc" },
      filters: { name: { operator: "like", value: "Alice" } },
    };
    const draft = serializeTableDraft(
      tableKey,
      {
        entries: [
          {
            originalSignature: JSON.stringify([["id", 1]]),
            changes: new Map([["name", "Edited Alice"]]),
            originalValues: new Map([["name", "Alice"]]),
            originalMissingFields: new Set(),
            baselinePresenceVersion: 1,
          },
        ],
      },
      [{ name: { value: "Unverified insert draft" } }],
      { kind: "apply", unknown: true },
    );
    const persisted = persistState({
      unrelated: "retained",
      tableView: view,
      tableDraft: draft,
    });
    const first = renderTable();
    const failed = initialize([idColumn, nameColumn]);
    expect(failed).toMatchObject({ page: 3, sort: view.sort });
    rejectSort(failed.fetchId);
    expect(screen.getByRole("alert").textContent).toContain(
      rejectedSortMessage,
    );
    expect(screen.queryByRole("table")).toBeNull();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(fetches()).toEqual([failed]);
    expect(persisted().tableDraft).toEqual(draft);

    const beforeRetry = getPostedMessages().length;
    fireEvent.click(
      screen.getByRole("button", { name: "Clear Sort and Retry" }),
    );
    expect(fetches()).toHaveLength(2);
    const reset = lastFetch();
    expect(reset).toMatchObject({
      page: 1,
      pageSize: 100,
      sort: null,
      filters: [{ column: "name", ...view.filters.name }],
    });
    expect(persisted()).toMatchObject({
      tableReadEpoch: reset.fetchId,
      tableView: { ...view, page: 1, sort: null },
      tableDraft: draft,
      unrelated: "retained",
    });
    expect(
      getPostedMessages()
        .slice(beforeRetry)
        .map(({ type }) => type),
    ).toEqual(["fetchPage"]);

    first.unmount();
    clearPostedMessages();
    renderTable();
    const restored = initialize([idColumn, nameColumn]);
    expect(restored).toEqual({ ...reset, fetchId: restored.fetchId });
    expect(restored.fetchId).toBeGreaterThan(reset.fetchId);
    rejectSort(failed.fetchId);
    act(() =>
      dispatchIncomingMessage("tableData", {
        fetchId: reset.fetchId,
        rows: [{ id: 91, name: "LATE RESET READ" }],
        totalCount: 1,
      }),
    );
    expect(screen.queryByRole("alert")).toBeNull();
    expect(screen.queryByRole("table")).toBeNull();
    commit(restored.fetchId);
    expect(screen.getByText("Edited Alice")).toBeTruthy();
    expect(screen.getByText("Unverified insert draft")).toBeTruthy();
    expect(isButtonDisabled("Apply Changes")).toBe(true);
    expect(persisted()).toMatchObject({ tableDraft: draft });
    expect(
      getPostedMessages().filter(
        ({ type }) => type !== "ready" && type !== "unsavedChangesState",
      ),
    ).toEqual([{ type: "fetchPage", payload: restored }]);
  });

  it("applies the latest filter on the first Enter before the debounce expires", () => {
    vi.useFakeTimers();
    mountThirdPage();
    const input = screen.getByLabelText("name filter value");
    fireEvent.change(input, { target: { value: "Bob" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(lastFetch()).toMatchObject({
      page: 1,
      filters: [{ column: "name", operator: "like", value: "Bob" }],
    });
    const count = fetches().length;
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toHaveLength(count);
  });

  it("applies both range endpoints on the first Enter", () => {
    vi.useFakeTimers();
    mountThirdPage();
    fireEvent.click(screen.getByLabelText("id filter operator"));
    fireEvent.click(screen.getByRole("menuitemradio", { name: /Between/ }));
    fireEvent.change(screen.getByLabelText("id filter start"), {
      target: { value: "12" },
    });
    const end = screen.getByLabelText("id filter end");
    fireEvent.change(end, { target: { value: "30" } });
    fireEvent.keyDown(end, { key: "Enter" });
    expect(lastFetch()).toMatchObject({
      page: 1,
      filters: expect.arrayContaining([
        { column: "id", operator: "between", value: ["12", "30"] },
      ]),
    });
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
    ["without filters", false],
    ["with an unchanged filter", true],
  ] as const)("keeps page 2 after changed metadata refresh %s", (_label, filtered) => {
    vi.useFakeTimers();
    const draft = { operator: "like", value: "Alice" } as const;
    const view = {
      page: 2,
      pageSize: 100,
      sort: null,
      filters: filtered ? { name: draft } : {},
    };
    const persisted = persistView(view);
    renderTable();
    commit(initialize([idColumn, nameColumn]).fetchId);
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    clearPostedMessages();

    act(() =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [idColumn, nameColumn, { ...nameColumn, name: "old_name" }],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(fetches()).toHaveLength(1);
    const refresh = lastFetch();
    expect(refresh).toMatchObject({
      page: 2,
      filters: filtered ? [{ column: "name", ...draft }] : [],
    });
    commit(refresh.fetchId);

    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toEqual([refresh]);
    expect(screen.getByText("Page 2 of 4")).toBeTruthy();
    expect(persisted().tableView).toEqual({ tableKey, ...view });
  });

  it.each([
    "debouncing",
    "ready",
  ])("applies a %s filter edit after changed metadata refresh", (timing) => {
    vi.useFakeTimers();
    const { persisted } = mountThirdPage();
    fireEvent.change(screen.getByLabelText("name filter value"), {
      target: { value: "Bob" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    if (timing === "ready") {
      act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
      expect(fetches()).toHaveLength(1);
    }
    clearPostedMessages();

    act(() =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [idColumn, nameColumn, { ...nameColumn, name: "old_name" }],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(fetches()).toHaveLength(1);
    const refresh = lastFetch();
    expect(refresh).toMatchObject({
      page: 3,
      filters: [{ column: "name", operator: "like", value: "Alice" }],
    });
    commit(refresh.fetchId);
    expect(persisted().tableView).toMatchObject({
      page: 1,
      filters: { name: { operator: "like", value: "Bob" } },
    });

    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));
    expect(fetches()).toHaveLength(2);
    const filtered = lastFetch();
    expect(filtered).toMatchObject({
      page: 1,
      filters: [{ column: "name", operator: "like", value: "Bob" }],
    });
    commit(filtered.fetchId);
    expect(screen.getByText("Page 1 of 4")).toBeTruthy();
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
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    expect(fetches()).toHaveLength(3);
    commit(lastFetch().fetchId);
    expect(isButtonDisabled("Apply Changes")).toBe(true);
    expect(isButtonDisabled("Add Row")).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Revert All" }));
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
    expect(isButtonDisabled("Apply Changes")).toBe(true);
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
    expect(isButtonDisabled("Apply Changes")).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(
      getPostedMessages().filter((message) => message.type === "applyChanges"),
    ).toHaveLength(0);
    fireEvent.click(screen.getByRole("button", { name: "Revert All" }));
    fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
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
    expect(screen.queryByRole("button", { name: "Retry" })).toBeNull();
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
    expect(screen.queryByRole("button", { name: "Retry" })).toBeNull();
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

  it("recovers from a restored invalid filter without showing stale rows or retrying it", () => {
    vi.useFakeTimers();
    const view = {
      page: 3,
      pageSize: 100,
      sort: { column: "id", direction: "desc" as const },
      filters: { name: { operator: "like" as const, value: "Alice" } },
    };
    const persisted = persistView(view);
    renderTable();
    const failed = initialize([idColumn, nameColumn]);

    expect(failed).toMatchObject({
      page: 3,
      filters: [{ column: "name", operator: "like", value: "Alice" }],
    });

    act(() => {
      dispatchIncomingMessage("tableError", {
        fetchId: failed.fetchId,
        error: "Invalid numeric filter",
        isFilterError: true,
      });
    });

    expect(screen.getByRole("table")).toBeTruthy();
    expect(screen.getByText(/Invalid numeric filter/)).toBeTruthy();
    expect(screen.getByText("0 rows total")).toBeTruthy();
    expect(screen.getByText("Page 1 of 1")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Add Row" })).toBeNull();
    expect(fetches()).toHaveLength(1);
    expect(persisted()).toMatchObject({ tableView: { page: 1 } });

    fireEvent.change(screen.getByLabelText("name filter value"), {
      target: { value: "Bob" },
    });
    act(() => vi.advanceTimersByTime(DEBOUNCE + 1));

    const recovered = lastFetch();
    expect(recovered).toMatchObject({
      page: 1,
      filters: [{ column: "name", operator: "like", value: "Bob" }],
    });
    commit(recovered.fetchId);
    expect(screen.queryByText("Invalid numeric filter")).toBeNull();
    expect(isButtonDisabled("Add Row")).toBe(false);
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
