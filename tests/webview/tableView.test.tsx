import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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

vi.mock("../../src/webview/components/MonacoEditor", async () => {
  interface MockMonacoEditorProps {
    initialValue?: string;
    ariaLabel?: string;
    readOnly?: boolean;
  }

  function MonacoEditor(props: MockMonacoEditorProps) {
    return (
      <textarea
        aria-label={props.ariaLabel ?? "SQL editor"}
        readOnly={props.readOnly}
        value={props.initialValue ?? ""}
        onChange={() => undefined}
      />
    );
  }

  return { MonacoEditor };
});

import { TableView } from "../../src/webview/components/TableView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  expectNoAxeViolations,
  getLastPostedMessage,
  getPostedMessages,
} from "./testUtils";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
  {
    name: "name",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
];

const rows = [
  { id: 1, name: "Alice" },
  { id: 2, name: "Bob" },
];

const noPkColumns: ColumnTypeMeta[] = [
  {
    name: "code",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
  {
    name: "name",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
];

const noPkRows = [
  { code: "A-1", name: "Alice" },
  { code: "B-2", name: "Bob" },
];

const autoIncrementColumns: ColumnTypeMeta[] = [
  {
    name: "seq",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: true,
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
  {
    name: "label",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
];

const autoIncrementRows = [{ seq: 10, label: "First" }];

const operatorVisibilityColumns: ColumnTypeMeta[] = [
  {
    name: "tags",
    type: "TEXT[]",
    nativeType: "TEXT[]",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "in", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "geom",
    type: "GEOMETRY",
    nativeType: "GEOMETRY",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "json",
    filterable: false,
    filterOperators: ["is_null", "is_not_null"],
    valueSemantics: "plain",
  },
];

function lastFetchPayload(): {
  fetchId?: number;
  page?: number;
  pageSize?: number;
  filters?: unknown;
  sort?: unknown;
} {
  const payload = getLastPostedMessage()?.payload;
  if (!payload || typeof payload !== "object") {
    throw new Error("Expected a fetchPage payload to be posted");
  }

  return payload as {
    fetchId?: number;
    page?: number;
    pageSize?: number;
    filters?: unknown;
    sort?: unknown;
  };
}

function renderTableView() {
  return render(
    <TableView
      connectionId="conn-1"
      database="main"
      schema="public"
      table="users"
    />,
  );
}

afterEach(() => {
  vi.useRealTimers();
});

describe("TableView", () => {
  it("requests pages, debounces filter application, and renders filter errors", async () => {
    renderTableView();

    expect(getPostedMessages()).toEqual([{ type: "ready" }]);

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({
          page: 1,
          pageSize: 25,
          filters: [],
          sort: null,
        }),
      });
    });

    const initialFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: initialFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    expect(screen.getByText("51 rows total")).toBeTruthy();

    clearPostedMessages();

    fireEvent.click(screen.getByRole("button", { name: "Next →" }));

    await act(async () => {
      await Promise.resolve();
    });

    expect(getLastPostedMessage()).toEqual({
      type: "fetchPage",
      payload: expect.objectContaining({ page: 2, pageSize: 25 }),
    });

    const nextPageFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: nextPageFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    clearPostedMessages();

    fireEvent.change(screen.getByLabelText("name filter value"), {
      target: { value: "ali" },
    });

    expect(getPostedMessages()).toHaveLength(0);

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 450));
    });

    expect(getLastPostedMessage()).toEqual({
      type: "fetchPage",
      payload: expect.objectContaining({
        page: 1,
        filters: [{ column: "name", operator: "like", value: "ali" }],
      }),
    });

    const filteredFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableError", {
        fetchId: filteredFetch.fetchId,
        error: "Bad filter expression",
        isFilterError: true,
      });
    });

    expect(screen.getByText(/Bad filter expression/)).toBeTruthy();

    fireEvent.click(screen.getByTitle("Dismiss"));

    expect(screen.queryByText("Bad filter expression")).toBeNull();
  });

  it("retains failed edits across previewed apply flows", async () => {
    const user = userEvent.setup();

    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });
    dispatchIncomingMessage("tableData", { rows, totalCount: rows.length });

    await waitFor(() => {
      expect(screen.getByText("Alice")).toBeTruthy();
    });

    const aliceCell = screen.getByText("Alice").closest("td");
    if (!(aliceCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected Alice cell to be rendered");
    }

    fireEvent.doubleClick(aliceCell);

    const editInput = screen.getByLabelText("Cell value");
    await user.clear(editInput);
    await user.type(editInput, "Alicia");
    fireEvent.blur(editInput);

    expect(screen.getByText(/1 row with unsaved changes/)).toBeTruthy();
    expect(screen.getByText("Alicia")).toBeTruthy();

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
      },
    });

    dispatchIncomingMessage("tableMutationPreview", {
      previewToken: "preview-1",
      kind: "applyChanges",
      title: "Preview changes",
      sql: "update users set name = 'Alicia' where id = 1;",
      statementCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByRole("dialog")).toBeTruthy();
    });

    fireEvent.keyDown(window, { key: "Escape" });

    expect(getLastPostedMessage()).toEqual({
      type: "cancelMutationPreview",
      payload: { previewToken: "preview-1" },
    });

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    dispatchIncomingMessage("tableMutationPreview", {
      previewToken: "preview-2",
      kind: "applyChanges",
      title: "Preview changes",
      sql: "update users set name = 'Alicia' where id = 1;",
      statementCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByRole("dialog")).toBeTruthy();
    });

    const dialog = screen.getByRole("dialog");
    await user.click(
      within(dialog).getByRole("button", { name: "Apply Changes" }),
    );

    expect(getLastPostedMessage()).toEqual({
      type: "confirmMutationPreview",
      payload: { previewToken: "preview-2" },
    });

    clearPostedMessages();

    dispatchIncomingMessage("applyResult", {
      success: true,
      rowOutcomes: [
        {
          rowIndex: 0,
          success: false,
          status: "verification_failed",
          message: "Concurrent update detected",
        },
      ],
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 25 }),
      });
    });

    dispatchIncomingMessage("tableData", {
      rows: [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ],
      totalCount: 2,
    });

    await waitFor(() => {
      expect(screen.getByText(/1 row with unsaved changes/)).toBeTruthy();
    });

    expect(screen.getByText("Alicia")).toBeTruthy();
  });

  it("hides selection and delete for tables without primary key and opens read-only cell editor", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns: noPkColumns,
      primaryKeyColumns: [],
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 25 }),
      });
    });

    const initialFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: initialFetch.fetchId,
        rows: noPkRows,
        totalCount: noPkRows.length,
      });
    });

    expect(screen.getByRole("button", { name: "Add Row" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: /^Delete \(/ })).toBeNull();
    expect(screen.queryByLabelText("Select all rows")).toBeNull();
    expect(screen.queryByLabelText("Select row 1")).toBeNull();

    const aliceCell = screen.getByText("Alice").closest("td");
    if (!(aliceCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected readable cell");
    }

    fireEvent.doubleClick(aliceCell);

    const editInput = screen.getByLabelText("Cell value");
    expect((editInput as HTMLInputElement).readOnly).toBe(true);
    expect(screen.queryByRole("button", { name: "NULL" })).toBeNull();
  });

  it("opens auto-increment cells in read-only mode when table has no primary key", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns: autoIncrementColumns,
      primaryKeyColumns: [],
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 25 }),
      });
    });

    const initialFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: initialFetch.fetchId,
        rows: autoIncrementRows,
        totalCount: autoIncrementRows.length,
      });
    });

    const seqCell = screen.getByText("10").closest("td");
    if (!(seqCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected auto-increment cell");
    }

    fireEvent.doubleClick(seqCell);

    const editInput = screen.getByLabelText("Cell value");
    expect((editInput as HTMLInputElement).readOnly).toBe(true);
  });

  it("requests delete confirmation with selected primary keys", async () => {
    const user = userEvent.setup();

    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 25 }),
      });
    });

    const initialFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: initialFetch.fetchId,
        rows,
        totalCount: rows.length,
      });
    });

    await waitFor(() => {
      expect(screen.getByLabelText("Select row 1")).toBeTruthy();
    });

    await waitFor(() => {
      expect(screen.getByText("2 rows total")).toBeTruthy();
    });

    await expectNoAxeViolations(document.body);

    await user.click(screen.getByLabelText("Select row 1"));

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Delete (1)" })).toBeTruthy();
    });

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Delete (1)" }));

    expect(getLastPostedMessage()).toEqual({
      type: "confirmDelete",
      payload: { count: 1 },
    });

    await act(async () => {
      dispatchIncomingMessage("deleteConfirmed", { confirmed: true });
    });

    expect(getLastPostedMessage()).toEqual({
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 1 }] },
    });
  });

  it("renders filter operator menus according to column filter policy payload", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns: operatorVisibilityColumns,
      primaryKeyColumns: [],
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 25 }),
      });
    });

    fireEvent.click(
      screen.getByRole("button", { name: "tags filter operator" }),
    );

    const tagsMenu = screen.getByRole("menu", {
      name: "tags filter operators",
    });
    expect(
      within(tagsMenu).getByRole("menuitemradio", { name: /Equals/i }),
    ).toBeTruthy();
    expect(
      within(tagsMenu).getByRole("menuitemradio", { name: /In list/i }),
    ).toBeTruthy();
    expect(
      within(tagsMenu).getByRole("menuitemradio", { name: /Is NULL/i }),
    ).toBeTruthy();
    expect(
      within(tagsMenu).getByRole("menuitemradio", { name: /Is NOT NULL/i }),
    ).toBeTruthy();
    expect(
      within(tagsMenu).queryByRole("menuitemradio", { name: /Greater than/i }),
    ).toBeNull();

    fireEvent.click(
      screen.getByRole("button", { name: "geom filter operator" }),
    );

    const geomMenu = screen.getByRole("menu", {
      name: "geom filter operators",
    });
    expect(
      within(geomMenu).getByRole("menuitemradio", { name: /Is NULL/i }),
    ).toBeTruthy();
    expect(
      within(geomMenu).getByRole("menuitemradio", { name: /Is NOT NULL/i }),
    ).toBeTruthy();
    expect(
      within(geomMenu).queryByRole("menuitemradio", { name: /Equals/i }),
    ).toBeNull();

    const geomFilterInput = screen.getByLabelText("geom filter value");
    expect((geomFilterInput as HTMLInputElement).disabled).toBe(true);
  });

  it("renders fatal table errors", async () => {
    renderTableView();

    dispatchIncomingMessage("tableError", { error: "Database offline" });

    await waitFor(() => {
      expect(screen.getByText("Database offline")).toBeTruthy();
    });
  });
});
