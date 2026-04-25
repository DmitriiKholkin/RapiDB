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
import {
  type ColumnTypeMeta,
  NULL_SENTINEL,
} from "../../src/shared/tableTypes";

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
  it("preserves repeated spaces in rendered text cells", async () => {
    renderTableView();

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
        rows: [{ id: 1, name: "wad  23" }],
        totalCount: 1,
      });
    });

    const valueCell = screen.getByText(
      (_, element) =>
        element?.textContent === "wad  23" &&
        element.tagName === "SPAN" &&
        element.getAttribute("style")?.includes("white-space: break-spaces") ===
          true,
    );
    expect(valueCell.getAttribute("style")).toContain(
      "white-space: break-spaces",
    );
  });

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

  it("treats reopened NULL cell as empty unless NULL is clicked again", async () => {
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
        rows: [{ id: 1, name: null }],
        totalCount: 1,
      });
    });

    await waitFor(() => {
      expect(screen.getByText("NULL")).toBeTruthy();
    });

    const nullCell = screen.getByText("NULL").closest("td");
    if (!(nullCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected NULL cell to be rendered");
    }

    fireEvent.doubleClick(nullCell);
    const editInput = screen.getByLabelText("Cell value");
    fireEvent.blur(editInput);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "" } }],
      },
    });
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
    expect((editInput as HTMLInputElement).placeholder).toBe("");
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

  it("supports insert with all DEFAULT fields and explicit draft edits", async () => {
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

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Add Row" }));

    expect(
      (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);

    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);

    const table = screen.getByRole("table");
    const bodyRows = Array.from(table.querySelectorAll("tbody > tr"));
    expect(bodyRows[0]?.textContent ?? "").not.toContain("Alice");
    expect(bodyRows[1]?.textContent ?? "").toContain("Alice");
    expect(bodyRows[2]?.textContent ?? "").toContain("Bob");

    expect(screen.queryByLabelText(/Include .* in insert/i)).toBeNull();

    const tableEl = screen.getByRole("table");
    const headerCells = Array.from(
      tableEl.querySelectorAll("thead tr:first-child th"),
    );
    const nameColumnIndex = headerCells.findIndex((cell) =>
      (cell.textContent ?? "").includes("name"),
    );
    if (nameColumnIndex < 0) {
      throw new Error("Expected name column header");
    }

    const draftRow = tableEl.querySelector("tbody tr");
    const nameCell = draftRow?.querySelectorAll("td")[nameColumnIndex] ?? null;
    if (!(nameCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected inline draft cell");
    }

    expect(nameCell.textContent ?? "").toContain("DEFAULT");

    expect(nameCell.style.background).toContain("rgba(200, 150, 0, 0.23)");

    fireEvent.doubleClick(nameCell);

    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);

    expect(screen.getByLabelText("Cell value")).toBeTruthy();
    expect(
      (screen.getByLabelText("Cell value") as HTMLInputElement).placeholder,
    ).toBe("");
    expect(screen.getByRole("button", { name: "DEF" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "NULL" })).toBeTruthy();

    fireEvent.keyDown(screen.getByLabelText("Cell value"), { key: "Enter" });

    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: { updates: [], insertValues: { name: "" } },
    });

    await act(async () => {
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Insert failed",
      });
    });

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Apply Changes" }),
      ).toBeTruthy();
    });

    clearPostedMessages();

    fireEvent.doubleClick(nameCell);
    await user.click(screen.getByRole("button", { name: "DEF" }));

    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: { updates: [], insertValues: {} },
    });

    await act(async () => {
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Insert failed",
      });
    });

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Apply Changes" }),
      ).toBeTruthy();
    });

    clearPostedMessages();

    fireEvent.doubleClick(nameCell);
    await user.click(screen.getByRole("button", { name: "NULL" }));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: { updates: [], insertValues: { name: NULL_SENTINEL } },
    });
  });

  it("restores normal toolbar state after reverting inline insert mode", async () => {
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
      expect(screen.getByText("Alice")).toBeTruthy();
    });

    await user.click(screen.getByRole("button", { name: "Add Row" }));
    await user.click(screen.getByRole("button", { name: "Revert All" }));

    expect(screen.getByRole("button", { name: "Add Row" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();

    const table = screen.getByRole("table");
    const bodyRows = Array.from(table.querySelectorAll("tbody > tr"));
    expect(bodyRows[0]?.textContent ?? "").toContain("Alice");
  });

  it("exits inline insert mode safely when a new table schema is initialized", async () => {
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

    await user.click(screen.getByRole("button", { name: "Add Row" }));
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();

    dispatchIncomingMessage("tableInit", {
      columns: noPkColumns,
      primaryKeyColumns: [],
    });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Add Row" })).toBeTruthy();
    });

    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
  });

  it("keeps persisted row selection indexes stable while draft row is visible", async () => {
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

    await user.click(screen.getByRole("button", { name: "Add Row" }));
    await user.click(screen.getByLabelText("Select row 1"));

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

  it("combines insert draft with pending edits in shared unsaved state", async () => {
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

    const aliceCell = screen.getByText("Alice").closest("td");
    if (!(aliceCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected Alice cell to be rendered");
    }

    fireEvent.doubleClick(aliceCell);
    const editInput = screen.getByLabelText("Cell value");
    await user.clear(editInput);
    await user.type(editInput, "Alicia");
    fireEvent.blur(editInput);

    await user.click(screen.getByRole("button", { name: "Add Row" }));

    expect(screen.getByText(/2 rows with unsaved changes/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Insert Row" })).toBeNull();

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: {},
      },
    });
  });

  it("clears draft and refreshes when insert is applied but updates fail", async () => {
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

    const aliceCell = screen.getByText("Alice").closest("td");
    if (!(aliceCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected Alice cell to be rendered");
    }

    fireEvent.doubleClick(aliceCell);
    const editInput = screen.getByLabelText("Cell value");
    await user.clear(editInput);
    await user.type(editInput, "Alicia");
    fireEvent.blur(editInput);

    await user.click(screen.getByRole("button", { name: "Add Row" }));

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: {},
      },
    });

    await act(async () => {
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Update failed",
        insertApplied: true,
      });
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 25 }),
      });
    });

    expect(screen.queryByText(/^DEFAULT$/)).toBeNull();
    expect(
      screen.getByText(/Insert was applied, but update changes were not/),
    ).toBeTruthy();
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Revert All" })).toBeTruthy();
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
