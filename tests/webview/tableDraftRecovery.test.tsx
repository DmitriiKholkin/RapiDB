import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
} from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { MssqlSqlVariantRowIdentity } from "../../src/shared/mssqlSqlVariantRowIdentity";
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
  MonacoEditor: ({
    initialValue,
    ariaLabel,
    onChange,
  }: {
    initialValue?: string;
    ariaLabel?: string;
    onChange?: (value: string) => void;
  }) => (
    <textarea
      defaultValue={initialValue}
      aria-label={ariaLabel}
      onChange={(event) => onChange?.(event.target.value)}
    />
  ),
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
function commitTableRows(
  rows: Row[],
  mssqlSqlVariantRowIdentities?: MssqlSqlVariantRowIdentity[],
) {
  act(() =>
    dispatchIncomingMessage("tableData", {
      fetchId: lastPayload("fetchPage").fetchId,
      rows,
      mssqlSqlVariantRowIdentities,
      totalCount: rows.length,
    }),
  );
}
function mountTable(rows?: Row[], columnDefs = columns) {
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
      columns: columnDefs,
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
    rowValueTypesRef: { current: [] },
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

function editName(rowIndex: number, value: string) {
  const cell = [...document.querySelectorAll(`td[data-row="${rowIndex}"]`)].at(
    -1,
  );
  if (!cell) throw new Error(`Missing name cell for row ${rowIndex}`);
  fireEvent.doubleClick(cell);
  const inline = screen.queryByLabelText("Cell value");
  const editor = inline ?? screen.getByLabelText("Cell data");
  fireEvent.change(editor, { target: { value } });
  if (inline) fireEvent.keyDown(editor, { key: "Enter" });
  else clickButton("Apply");
}

function clickButton(name: string) {
  const button = screen.getByRole<HTMLButtonElement>("button", { name });
  fireEvent.click(button);
  return button;
}

describe("confirmed Apply conflict refresh", () => {
  it.each([
    "changed",
    "missing",
    "same key and values",
  ] as const)("preserves exact drafts and history when the conflict row is %s", (kind) => {
    const original = [
      { id: 1, name: "Original" },
      { id: 2, name: "Other" },
    ];
    mountTable(original);
    editName(1, "safe draft");
    editName(0, "conflicting draft");
    clickButton("Add Row");
    editName(-1, "insert draft");
    clickButton("Refresh");
    expect(messages("fetchPage")).toHaveLength(1);
    clickButton("Apply Changes");
    const operationId = lastPayload("applyChanges").operationId;
    const noWrite = {
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
      conflictRows: [1],
      rowOutcomes: [
        { rowIndex: 0, success: false, status: "not_applied" },
        { rowIndex: 1, success: false, status: "not_applied" },
      ],
    };
    act(() =>
      dispatchIncomingMessage("applyResult", {
        ...noWrite,
        operationId: "stale-operation",
      }),
    );
    expect(draft()?.mutation).not.toHaveProperty("applyConflict");
    act(() =>
      dispatchIncomingMessage("applyResult", { ...noWrite, operationId }),
    );
    expect(draft()?.mutation).toMatchObject({
      kind: "apply",
      unknown: false,
      applyConflict: true,
    });
    expect(clickButton("Apply Changes").disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("Rows per page"), {
      target: { value: "100" },
    });
    expect(messages("fetchPage")).toHaveLength(1);
    const obsoleteFetch = lastPayload("fetchPage").fetchId;
    clickButton("Refresh");
    const reviewFetch = lastPayload("fetchPage").fetchId;
    expect(lastPayload("fetchPage")).toMatchObject({
      conflictRefresh: true,
      page: 1,
      pageSize: 25,
      filters: [],
      sort: null,
    });
    expect(reviewFetch).not.toBe(obsoleteFetch);
    act(() => {
      dispatchIncomingMessage("tableData", {
        fetchId: obsoleteFetch,
        rows: original,
        totalCount: 2,
      });
      dispatchIncomingMessage("tableData", { rows: original, totalCount: 2 });
      dispatchIncomingMessage("tableError", {
        fetchId: reviewFetch,
        error: "Temporary read failure",
      });
    });
    expect(draft()?.mutation?.applyConflict).toBe(true);
    expect(screen.getByText("Temporary read failure")).toBeTruthy();
    clickButton("Refresh");
    const refreshed = [
      original[1],
      ...(kind === "missing"
        ? [{ id: 3, name: "Unrelated" }]
        : [{ id: 1, name: kind === "changed" ? "Concurrent" : "Original" }]),
    ];
    commitTableRows(refreshed);
    expect(screen.getByText("Review conflicting drafts")).toBeTruthy();
    expect(screen.getByText('"conflicting draft"')).toBeTruthy();
    expect(screen.getByText("safe draft")).toBeTruthy();
    expect(screen.getByText("insert draft")).toBeTruthy();
    expect(draft()?.mutation).toBeUndefined();
    expect(
      draft()?.restoreState.entries.find((entry) => entry.applyConflict),
    ).toMatchObject({
      originalValues: new Map([["name", "Original"]]),
      changes: new Map([["name", "conflicting draft"]]),
    });
    expect(clickButton("Apply Changes").disabled).toBe(true);
    expect(messages("applyChanges")).toHaveLength(1);
    clickButton("Discard Unmatched");
    expect(screen.queryByText("Review conflicting drafts")).toBeNull();
    expect(screen.getByText("safe draft")).toBeTruthy();
    expect(screen.getByText("insert draft")).toBeTruthy();
    clickButton("Undo");
    expect(screen.queryByText("insert draft")).toBeNull();
    clickButton("Redo");
    expect(screen.getByText("insert draft")).toBeTruthy();
    clickButton("Apply Changes");
    expect(lastPayload("applyChanges")).toMatchObject({
      updates: [
        {
          primaryKeys: { id: 2 },
          originalValues: { name: "Other" },
          changes: { name: "safe draft" },
        },
      ],
      insertValues: [{ name: "insert draft" }],
    });
  });

  it("restores the committed page and requires an explicit read after reload", () => {
    const conflictView = {
      tableKey,
      page: 2,
      pageSize: 100,
      sort: { column: "id", direction: "desc" as const },
      filters: { name: { operator: "eq" as const, value: "Original" } },
    };
    state.tableView = { ...conflictView, page: 1, filters: {} };
    state.tableDraft = serializeTableDraft(
      tableKey,
      {
        entries: [
          {
            originalSignature: '[["id",1]]',
            changes: new Map([["name", "draft"]]),
            originalValues: new Map([["name", "Original"]]),
            originalMissingFields: new Set(),
            baselinePresenceVersion: 1,
            applyConflict: true,
          },
        ],
      },
      [],
      { kind: "apply", unknown: false, applyConflict: true, conflictView },
    );
    const original = [{ id: 1, name: "Original" }];
    const view = mountTable(original);
    expect(lastPayload("fetchPage")).toMatchObject({
      page: 2,
      pageSize: 100,
      sort: conflictView.sort,
      filters: [{ column: "name", operator: "eq", value: "Original" }],
    });
    expect(lastPayload("fetchPage")).not.toHaveProperty("conflictRefresh");
    expect(clickButton("Apply Changes").disabled).toBe(true);
    expect(
      screen.queryByRole("button", { name: "Discard Unmatched" }),
    ).toBeNull();
    clickButton("Refresh");
    expect(lastPayload("fetchPage")).toMatchObject({
      page: 2,
      pageSize: 100,
      conflictRefresh: true,
    });
    commitTableRows(original);
    expect(messages("fetchPage")).toHaveLength(2);
    expect(draft()?.mutation).toBeUndefined();
    view.unmount();

    clearPostedMessages();
    mountTable(original);
    expect(lastPayload("fetchPage")).toMatchObject({ page: 2, pageSize: 100 });
    expect(clickButton("Apply Changes").disabled).toBe(true);
    expect(screen.getByText("Review conflicting drafts")).toBeTruthy();
    expect(messages("applyChanges")).toHaveLength(0);
  });

  it("does not open recovery for an ordinary no-write error urging Refresh", () => {
    mountTable([{ id: 1, name: "Original" }]);
    editName(0, "draft");
    clickButton("Apply Changes");
    act(() =>
      dispatchIncomingMessage("applyResult", {
        operationId: lastPayload("applyChanges").operationId,
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
        error: "Unexpected error. Refresh the table and retry.",
      }),
    );
    expect(draft()?.mutation).toBeUndefined();
    clickButton("Refresh");
    expect(messages("fetchPage")).toHaveLength(1);
    expect(
      screen.getByRole<HTMLButtonElement>("button", { name: "Apply Changes" })
        .disabled,
    ).toBe(false);
    clickButton("Apply Changes");
    expect(messages("applyChanges")).toHaveLength(2);
  });
});

describe("table draft recovery across remounts", () => {
  const variantColumns: ColumnTypeMeta[] = [
    {
      ...columns[0],
      type: "sql_variant",
      nativeType: "sql_variant",
      category: "other",
      mssqlSqlVariantPrimaryKey: true,
    },
    {
      ...columns[1],
      name: "amount",
      type: "int",
      nativeType: "int",
      category: "integer",
    },
  ];
  const decimalKey = {
    id: { $rapidbMssqlVariantKey: "v1:decimal:5:2:5::31002E0030003000" },
  };
  const stringKey = {
    id: {
      $rapidbMssqlVariantKey:
        "v1:nvarchar:0:0:16:Latin1_General_100_BIN2:31002E0030003000",
    },
  };
  const variantRows = [
    { id: "1.00", amount: 3 },
    { id: "1.00", amount: 3 },
  ];

  it("restores edits to the exact SQL Server variant key after row order changes", () => {
    const first = mountTable(undefined, variantColumns);
    commitTableRows(variantRows, [stringKey, decimalKey]);
    fireEvent.doubleClick(screen.getAllByRole("cell", { name: "3" })[1]);
    const editor = screen.getByLabelText("Cell value");
    fireEvent.change(editor, { target: { value: "42" } });
    fireEvent.keyDown(editor, { key: "Enter" });
    expect(draft()?.restoreState.entries[0].originalSignature).toContain(
      decimalKey.id.$rapidbMssqlVariantKey,
    );
    first.unmount();
    clearPostedMessages();

    mountTable(undefined, variantColumns);
    commitTableRows(
      variantRows.map((row) => ({ ...row })),
      [decimalKey, stringKey],
    );
    const pending = screen.getByRole("cell", { name: "42" }).closest("tr");
    expect(pending).toBe(
      screen.getByRole("checkbox", { name: "Select row 1" }).closest("tr"),
    );
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(lastPayload("applyChanges").updates).toEqual([
      {
        primaryKeys: decimalKey,
        changes: { amount: "42" },
        originalValues: { amount: 3 },
      },
    ]);
  });

  it("does not attach a legacy display-only draft to the surviving variant row", () => {
    state.tableDraft = serializeTableDraft(
      tableKey,
      {
        entries: [
          {
            originalSignature: JSON.stringify([["id", "1.00"]]),
            changes: new Map([["amount", "42"]]),
            originalValues: new Map([["amount", 3]]),
            originalMissingFields: new Set(),
            baselinePresenceVersion: 1,
          },
        ],
      },
      [],
    );
    mountTable(undefined, variantColumns);
    commitTableRows([{ ...variantRows[0] }], [stringKey]);
    expect(screen.getByRole("cell", { name: "3" })).toBeTruthy();
    expect(screen.queryByRole("cell", { name: "42" })).toBeNull();
    expect(
      screen.getByText(/edits are preserved but not attached/),
    ).toBeTruthy();
    const apply = screen.getByRole<HTMLButtonElement>("button", {
      name: "Apply Changes",
    });
    expect(apply.disabled).toBe(true);
    fireEvent.click(apply);
    expect(messages("applyChanges")).toHaveLength(0);
  });

  it.each([
    undefined,
    [{ id: null }],
  ])("blocks row mutations when the exact variant identity is unavailable (%j)", (identity) => {
    mountTable(undefined, variantColumns);
    commitTableRows([{ ...variantRows[0] }], identity);
    fireEvent.doubleClick(screen.getByRole("cell", { name: "3" }));
    expect(screen.queryByLabelText("Cell value")).toBeNull();
    expect(
      screen.getByText(
        /exact identity for SQL Server sql_variant primary key id is unavailable/,
      ),
    ).toBeTruthy();
    fireEvent.click(screen.getByRole("checkbox", { name: "Select row 1" }));
    fireEvent.click(screen.getByRole("button", { name: /Delete \([01]\)/ }));
    expect(messages("deleteRows")).toHaveLength(0);
    expect(messages("applyChanges")).toHaveLength(0);
  });
  it.each([
    ["tableData", { rows: [null], totalCount: 1 }],
    ["tableError", { error: { message: "not a string" } }],
  ] as const)("releases only the matching malformed %s without clearing the committed page", (type, reply) => {
    mountTable([{ id: 1, name: "Alice" }]);
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    const firstId = lastPayload("fetchPage").fetchId;
    commitTableRows([{ id: 1, name: "Alice" }]);
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    const fetchId = lastPayload("fetchPage").fetchId;
    expect(fetchId).not.toBe(firstId);
    act(() => {
      dispatchIncomingMessage(type, { ...reply, fetchId: firstId });
      dispatchIncomingMessage(type, reply);
      dispatchIncomingMessage(type, null);
    });
    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    act(() => dispatchIncomingMessage(type, { ...reply, fetchId }));
    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();
    expect(screen.getByText(/invalid table read reply/)).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    commitTableRows([{ id: 1, name: "Updated" }]);
    expect(screen.getByText("Updated")).toBeTruthy();
    expect(screen.queryByText(/invalid table read reply/)).toBeNull();
  });

  it.each([
    "apply",
    "delete",
  ] as const)("treats a malformed %s acknowledgement as unknown without clearing edits or drafts", (kind) => {
    const { result, refs, commit } = mountController();
    const active = kind === "apply" ? "applying" : "deleting";
    const request = kind === "apply" ? "applyChanges" : "deleteRows";
    const reply = kind === "apply" ? "applyResult" : "deleteResult";
    const otherReply = kind === "apply" ? "deleteResult" : "applyResult";
    act(() => {
      result.current.commitCellEdit(0, columns[1], "Edited", "Alice");
      result.current.startInsertRow();
    });
    act(() =>
      result.current[kind === "apply" ? "applyChanges" : "deleteSelected"](),
    );
    const { operationId } = lastPayload(request);
    expect(result.current[active]).toBe(true);
    const invalid = {
      success: true,
      affectedRows: 1,
      changesPossible: true,
      outcomeUnknown: false,
      insertApplied: true,
      rowOutcomes: [
        {
          rowIndex: 0,
          status: kind === "apply" ? "applied" : "deleted",
          success: true,
        },
        null,
      ],
    };
    act(() => {
      dispatchIncomingMessage(reply, { ...invalid, operationId: "old" });
      dispatchIncomingMessage(reply, invalid);
      dispatchIncomingMessage(otherReply, { ...invalid, operationId });
    });
    expect(result.current[active]).toBe(true);
    act(() => dispatchIncomingMessage(reply, { ...invalid, operationId }));
    expect(result.current[active]).toBe(false);
    expect(result.current.reconciliationRequired).toBe(true);
    expect(result.current.pendingEdits.get(0)?.get("name")).toBe("Edited");
    expect(result.current.newRows).toHaveLength(1);
    expect(
      result.current.applyStatus?.message ?? result.current.mutErr,
    ).toMatch(/write outcome is unknown/i);
    expect(draft()?.mutation).toMatchObject({ kind, unknown: true });
    expect(refs.fetchPageRef.current).not.toHaveBeenCalled();

    commit([{ id: 1, name: "Alice" }]);
    expect(result.current.reconciliationRequired).toBe(true);
    act(() => result.current.retryReconciliation());
    expect(refs.fetchPageRef.current).toHaveBeenCalledOnce();
    commit([{ id: 1, name: "Alice" }]);
    expect(result.current.reconciliationRequired).toBe(kind === "apply");
    if (kind === "apply") {
      act(() => result.current.applyChanges());
      expect(messages(request)).toHaveLength(1);
    }
  });

  it("requires a fresh read after a malformed verification page and never reapplies unknown inserts", () => {
    mountTable([{ id: 1, name: "Alice" }]);
    fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const { operationId } = lastPayload("applyChanges");
    act(() =>
      dispatchIncomingMessage("applyResult", {
        operationId,
        success: true,
        rowOutcomes: {},
      }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    const { fetchId } = lastPayload("fetchPage");
    act(() => dispatchIncomingMessage("tableData", { fetchId, totalCount: 1 }));
    expect(screen.getByText(/invalid table read reply/)).toBeTruthy();
    act(() =>
      dispatchIncomingMessage("tableData", {
        fetchId,
        rows: [{ id: 1, name: "Alice" }],
        totalCount: 1,
      }),
    );
    expect(screen.getByText(/invalid table read reply/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    commitTableRows([{ id: 1, name: "Alice" }]);
    expect(screen.queryByText(/invalid table read reply/)).toBeNull();
    expect(
      screen.getByRole<HTMLButtonElement>("button", { name: "Apply Changes" })
        .disabled,
    ).toBe(true);
  });

  it.each([
    false,
    true,
  ])("allows INSERT retry only for a proven non-execution (unknown=%s)", (outcomeUnknown) => {
    const hook = mountController();
    act(() => hook.result.current.startInsertRow());
    act(() => hook.result.current.applyChanges());
    const operationId = lastPayload("applyChanges").operationId;
    act(() =>
      dispatchIncomingMessage("applyResult", {
        operationId,
        success: false,
        error: "Lost reply",
        changesPossible: outcomeUnknown,
        outcomeUnknown,
      }),
    );
    hook.commit([{ id: 1, name: "Alice" }]);
    const drafts = hook.result.current.newRows;
    act(() => hook.result.current.applyChanges());
    expect(messages("applyChanges")).toHaveLength(outcomeUnknown ? 1 : 2);
    if (!outcomeUnknown) return;
    expect(hook.result.current.newRows).toEqual(drafts);
    expect(hook.result.current.reconciliationRequired).toBe(true);
    act(() => hook.result.current.retryReconciliation());
    hook.commit([
      { id: 2, name: "Late inserted row" },
      { id: 1, name: "Alice" },
    ]);
    act(() => hook.result.current.applyChanges());
    expect(messages("applyChanges")).toHaveLength(1);
    act(() => hook.result.current.revertChanges());
    expect(hook.result.current.newRows).toHaveLength(0);
    expect(hook.result.current.reconciliationRequired).toBe(false);
  });

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
    ).toBe(true);
    expect(messages("applyChanges")).toHaveLength(0);
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
    expect(lastPayload("unsavedChangesState")).toEqual({
      hasUnsavedChanges: false,
      hasDeleteRecovery: true,
    });
    act(() => restored.result.current.deleteSelected());
    expect(messages("deleteRows")).toHaveLength(0);
    expect(restored.result.current.reconciliationRequired).toBe(true);
    act(() => restored.result.current.retryReconciliation());
    restored.commit([]);
    act(() => restored.result.current.revertChanges());
    expect(messages("deleteRecoveryResolved")).toHaveLength(1);
    expect(state.tableDraft).toBeUndefined();
  });

  it.each([
    false,
    true,
  ])("discards clean preview marker (cancelled=%s)", (cancelled) => {
    const viewer = mountController();
    act(() => viewer.result.current.deleteSelected());
    const { operationId } = lastPayload("deleteRows");
    const preview = {
      operationId,
      kind: "deleteRows",
      previewToken: "preview",
    };
    act(() => dispatchIncomingMessage("tableMutationPreview", preview));
    expect(state.tableDraft).toBeUndefined();
    expect(lastPayload("unsavedChangesState")).toEqual({
      hasUnsavedChanges: false,
    });
    if (cancelled) {
      act(() => viewer.result.current.cancelMutationPreview());
      expect(lastPayload("cancelMutationPreview")).toMatchObject({
        operationId,
      });
      expect(state.tableDraft).toBeUndefined();
    } else {
      act(() => viewer.result.current.confirmMutationPreview());
      expect(lastPayload("confirmMutationPreview")).toMatchObject({
        operationId,
      });
      expect(draft()?.mutation).toEqual({
        kind: "delete",
        unknown: true,
        operationId,
      });
    }
  });

  it("requires explicit verification of a late unknown DELETE on the reconnected viewer", () => {
    const tableRows = [{ id: 1, name: "Alice" }];
    mountTable(tableRows);
    fireEvent.click(screen.getByLabelText("Select row 1"));
    fireEvent.click(screen.getByRole("button", { name: "Delete (1)" }));
    const { operationId } = lastPayload("deleteRows");
    act(() => dispatchIncomingMessage("tableConnectionInvalidated", {}));
    act(() =>
      dispatchIncomingMessage("deleteResult", {
        operationId,
        success: false,
        affectedRows: 0,
        rowOutcomes: [],
        changesPossible: true,
        outcomeUnknown: true,
        error: "The transport closed before acknowledgement.",
      }),
    );
    expect(draft()?.mutation).toEqual({
      kind: "delete",
      unknown: true,
      operationId,
    });
    expect(lastPayload("unsavedChangesState")).toMatchObject({
      hasDeleteRecovery: true,
    });
    clearPostedMessages();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    expect(messages("fetchPage")).toHaveLength(0);

    act(() =>
      dispatchIncomingMessage("tableInit", {
        intent: "connectionRefresh",
        columns,
        primaryKeyColumns: ["id"],
      }),
    );
    expect(messages("fetchPage")).toHaveLength(1);
    commitTableRows(tableRows);
    expect(
      screen.getByRole("button", { name: "Add Row" }).hasAttribute("disabled"),
    ).toBe(true);
    expect(
      screen.queryByRole("button", { name: "Acknowledge delete warning" }),
    ).toBeNull();
    expect(messages("deleteRecoveryResolved")).toHaveLength(0);
    expect(messages("deleteRows")).toHaveLength(0);

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    expect(messages("fetchPage")).toHaveLength(2);
    commitTableRows([]);
    fireEvent.click(
      screen.getByRole("button", { name: "Acknowledge delete warning" }),
    );
    expect(lastPayload("deleteRecoveryResolved")).toEqual({ operationId });
    expect(lastPayload("unsavedChangesState")).toEqual({
      hasUnsavedChanges: false,
    });
    expect(state.tableDraft).toBeUndefined();
    expect(messages("deleteRows")).toHaveLength(0);
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
