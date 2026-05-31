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
  const React = await import("react");

  interface MockMonacoEditorProps {
    initialValue?: string;
    ariaLabel?: string;
    readOnly?: boolean;
    language?: string;
    onChange?: (value: string) => void;
  }

  const MonacoEditor = React.forwardRef(function MonacoEditor(
    props: MockMonacoEditorProps,
    ref: React.ForwardedRef<{
      placeCursor: (options?: {
        reveal?: boolean;
        preserveViewport?: boolean;
      }) => void;
      selectAllKeepCursorEndScrollTop: () => void;
    }>,
  ) {
    const [value, setValue] = React.useState(props.initialValue ?? "");
    const textAreaRef = React.useRef<HTMLTextAreaElement>(null);

    React.useEffect(() => {
      setValue(props.initialValue ?? "");
    }, [props.initialValue]);

    React.useImperativeHandle(ref, () => ({
      placeCursor: () => {
        const textArea = textAreaRef.current;
        if (!textArea) {
          return;
        }

        const end = textArea.value.length;
        textArea.focus();
        textArea.setSelectionRange(end, end);
        textArea.scrollTop = 0;
        textArea.scrollLeft = 0;
      },
      selectAllKeepCursorEndScrollTop: () => {
        const textArea = textAreaRef.current;
        if (!textArea) {
          return;
        }

        textArea.focus();
        const end = textArea.value.length;
        textArea.setSelectionRange(end, end);
        textArea.scrollTop = 0;
        textArea.scrollLeft = 0;
      },
    }));

    return (
      <div>
        <div data-testid="monaco-language">{props.language ?? "sql"}</div>
        <textarea
          ref={textAreaRef}
          aria-label={props.ariaLabel ?? "SQL editor"}
          readOnly={props.readOnly}
          value={value}
          onChange={(event) => {
            setValue(event.target.value);
            props.onChange?.(event.target.value);
          }}
        />
      </div>
    );
  });

  return { MonacoEditor };
});

import { TableView } from "../../src/webview/components/TableView";
import { DEBOUNCE } from "../../src/webview/components/table/tableViewHelpers";
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

const structuredColumns: ColumnTypeMeta[] = [
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
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
  {
    name: "payload",
    type: "JSON",
    nativeType: "JSON",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "json",
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "tags",
    type: "TEXT[]",
    nativeType: "TEXT[]",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "array",
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "xml_doc",
    type: "XML",
    nativeType: "XML",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
];

const structuredRows = [
  {
    id: 1,
    payload: '{"name":"Alice","meta":{"active":true}}',
    tags: '["alpha","beta"]',
    xml_doc: '<root><item id="1">Alice</item></root>',
  },
];

const fkColumns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    primaryKeyRole: "partition",
    isForeignKey: false,
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
  {
    name: "role_id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: true,
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
];

const fkRows = [{ id: 1, role_id: 10 }];

const compositeKeyColumns: ColumnTypeMeta[] = [
  {
    name: "tenant_id",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    primaryKeyRole: "partition",
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
  {
    name: "user_id",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 2,
    primaryKeyRole: "sort",
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
];

const compositeKeyRows = [{ tenant_id: "tenant-1", user_id: "user-1" }];

const noPkColumns: ColumnTypeMeta[] = [
  {
    name: "code",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
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
    identityGeneration: "auto_increment",
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
    category: "text",
    filterable: true,
    filterOperators: ["eq", "in", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "title",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
  {
    name: "geom",
    type: "GEOMETRY",
    nativeType: "GEOMETRY",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
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

function postedMessagesOfType(type: string) {
  return getPostedMessages().filter((message) => message.type === type);
}

function renderTableView(overrides?: {
  connectionReadOnly?: boolean;
  defaultPageSize?: number;
  isView?: boolean;
  table?: string;
}) {
  return render(
    <TableView
      connectionId="conn-1"
      database="main"
      schema="public"
      connectionReadOnly={overrides?.connectionReadOnly}
      defaultPageSize={overrides?.defaultPageSize}
      isView={overrides?.isView}
      table={overrides?.table ?? "users"}
    />,
  );
}

function dragResizeHandle(handle: HTMLElement, deltaX: number): void {
  const startX = 200;
  const endX = Math.max(0, startX + deltaX);
  fireEvent.mouseDown(handle, { clientX: startX, buttons: 1 });
  fireEvent.mouseMove(document, { clientX: endX, buttons: 1 });
  fireEvent.mouseUp(document, { clientX: endX, buttons: 0 });
}

async function initializeCommittedTableData(overrides?: {
  columnDefs?: ColumnTypeMeta[];
  primaryKeyColumns?: string[];
  dataRows?: readonly Record<string, unknown>[];
  renderOverrides?: {
    connectionReadOnly?: boolean;
    defaultPageSize?: number;
    isView?: boolean;
    table?: string;
  };
  totalCount?: number;
}) {
  renderTableView(overrides?.renderOverrides);

  const columnDefs = overrides?.columnDefs ?? columns;
  const primaryKeyColumns = overrides?.primaryKeyColumns ?? ["id"];

  dispatchIncomingMessage("tableInit", {
    columns: columnDefs,
    primaryKeyColumns,
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
  const committedRows = overrides?.dataRows ?? rows;
  const committedCount = overrides?.totalCount ?? committedRows.length;

  await act(async () => {
    dispatchIncomingMessage("tableData", {
      fetchId: initialFetch.fetchId,
      rows: committedRows,
      totalCount: committedCount,
    });
  });

  await waitFor(() => {
    expect(screen.getByRole("table")).toBeTruthy();
  });
}

function getBodyCell(columnName: string, rowIndex = 0): HTMLTableCellElement {
  const tableEl = screen.getByRole("table");
  const headerCells = Array.from(
    tableEl.querySelectorAll("thead tr:first-child th"),
  );
  const columnIndex = headerCells.findIndex((cell) =>
    (cell.textContent ?? "").includes(columnName),
  );

  if (columnIndex < 0) {
    throw new Error(`Expected ${columnName} column header`);
  }

  const bodyRows = Array.from(tableEl.querySelectorAll("tbody tr"));
  const targetCell = bodyRows[rowIndex]?.querySelectorAll("td")[columnIndex];
  if (!(targetCell instanceof HTMLTableCellElement)) {
    throw new Error(`Expected ${columnName} body cell`);
  }

  return targetCell;
}

afterEach(() => {
  vi.useRealTimers();
  clearPostedMessages();
});

describe("TableView", () => {
  it("dispatches table export messages directly when all rows are visible", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData();
    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Export CSV" }));
    await user.click(screen.getByRole("button", { name: "Export JSON" }));

    expect(getPostedMessages()).toEqual([
      {
        type: "exportCSV",
        payload: { sort: null, filters: [] },
      },
      {
        type: "exportJSON",
        payload: { sort: null, filters: [] },
      },
    ]);
  });

  it("dispatches paged export payload when exporting visible rows from the choice dialog", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({ totalCount: rows.length + 10 });
    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Export CSV" }));

    expect(screen.getByRole("dialog")).toBeTruthy();

    await user.click(
      screen.getByRole("button", { name: "Export visible (2 rows)" }),
    );

    expect(getPostedMessages()).toEqual([
      {
        type: "exportCSV",
        payload: {
          sort: null,
          filters: [],
          limitToPage: {
            page: 1,
            pageSize: 25,
          },
        },
      },
    ]);
  });

  it("shows only a fullscreen loader until the first dataset is committed", async () => {
    renderTableView();

    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    expect(screen.queryByRole("table")).toBeNull();
    expect(screen.queryByRole("button", { name: "Add Row" })).toBeNull();

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

    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    expect(screen.queryByRole("table")).toBeNull();
    expect(screen.queryByRole("button", { name: "Add Row" })).toBeNull();

    const initialFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: initialFetch.fetchId,
        rows,
        totalCount: rows.length,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("table")).toBeTruthy();
    });

    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();
    expect(screen.getByRole("button", { name: "Add Row" })).toBeTruthy();
  });

  it("shows key icons for both primary and foreign key columns", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns: fkColumns,
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
        rows: fkRows,
        totalCount: fkRows.length,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("table")).toBeTruthy();
    });

    const pkHeader = screen.getByText("id").closest("th");
    const fkHeader = screen.getByText("role_id").closest("th");

    expect(pkHeader?.querySelectorAll(".codicon-key")).toHaveLength(1);
    expect(fkHeader?.querySelectorAll(".codicon-key")).toHaveLength(1);
    expect(
      (pkHeader?.querySelector(".codicon-key") as HTMLElement | null)?.style
        .color,
    ).toBe("var(--vscode-editorWarning-foreground, #8f5b00)");
  });

  it("uses different key icon colors for partition and sort keys", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns: compositeKeyColumns,
      primaryKeyColumns: ["tenant_id", "user_id"],
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
        rows: compositeKeyRows,
        totalCount: compositeKeyRows.length,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("table")).toBeTruthy();
    });

    const partitionKeyIcon = screen
      .getByText("tenant_id")
      .closest("th")
      ?.querySelector(".codicon-key");
    const sortKeyIcon = screen
      .getByText("user_id")
      .closest("th")
      ?.querySelector(".codicon-key");

    expect((partitionKeyIcon as HTMLElement | null)?.style.color).toBe(
      "var(--vscode-editorWarning-foreground, #8f5b00)",
    );
    expect((sortKeyIcon as HTMLElement | null)?.style.color).toBe(
      "var(--vscode-textLink-foreground, #2f6f9f)",
    );
  });

  it("shows column detail tooltip on header hover text", async () => {
    renderTableView();

    const tooltipColumn: ColumnTypeMeta = {
      ...fkColumns[1],
      defaultValue: "42",
    };

    dispatchIncomingMessage("tableInit", {
      columns: [tooltipColumn],
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
        rows: [{ role_id: 10 }],
        totalCount: 1,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("table")).toBeTruthy();
    });

    const headerCell = screen.getByText("role_id").closest("th");
    if (!(headerCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected role_id header cell to be rendered");
    }

    const headerTitle = headerCell.getAttribute("title");
    expect(headerTitle).toContain("INTEGER, default: 42");
    expect(headerTitle).toContain("Foreign key");
  });

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
        element.getAttribute("style")?.includes("white-space: pre") === true,
    );
    expect(valueCell.getAttribute("style")).toContain("white-space: pre");
  });

  it("collapses and reopens a table column from the resize divider", async () => {
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
        rows,
        totalCount: rows.length,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("table")).toBeTruthy();
    });

    const nameHeader = screen.getByText("name").closest("th");
    if (!(nameHeader instanceof HTMLTableCellElement)) {
      throw new Error("Expected name header cell to be rendered");
    }

    const resizeHandle = screen.getByRole("button", {
      name: "Resize name column",
    });

    dragResizeHandle(resizeHandle, -500);

    await waitFor(() => {
      expect(nameHeader.style.width).toBe("0px");
    });

    expect(screen.queryByText("Alice")).toBeNull();

    dragResizeHandle(resizeHandle, 320);

    await waitFor(() => {
      expect(Number.parseFloat(nameHeader.style.width)).toBeGreaterThan(0);
    });

    expect(screen.getByText("Alice")).toBeTruthy();
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

    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    expect(screen.getByText("51 rows total")).toBeTruthy();
    expect(screen.getByText("Page 1 of 3")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();

    const nextPageFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: nextPageFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    await waitFor(() => {
      expect(screen.getByText("Page 2 of 3")).toBeTruthy();
    });

    clearPostedMessages();

    fireEvent.change(screen.getByLabelText("name filter value"), {
      target: { value: "ali" },
    });

    expect(getPostedMessages()).toHaveLength(0);

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, DEBOUNCE + 50));
    });

    expect(getLastPostedMessage()).toEqual({
      type: "fetchPage",
      payload: expect.objectContaining({
        page: 1,
        filters: [{ column: "name", operator: "like", value: "ali" }],
      }),
    });

    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    expect(screen.getByText("51 rows total")).toBeTruthy();
    expect(screen.getByText("Page 2 of 3")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();

    const filteredFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableError", {
        fetchId: filteredFetch.fetchId,
        error: "Bad filter expression",
        isFilterError: true,
      });
    });

    expect(screen.getByText(/Bad filter expression/)).toBeTruthy();
    expect(screen.getByText("51 rows total")).toBeTruthy();
    expect(screen.getByText("Page 2 of 3")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();
    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();

    fireEvent.click(screen.getByTitle("Dismiss"));

    expect(screen.queryByText("Bad filter expression")).toBeNull();
  });

  it("does not refetch on duplicate tableInit and preserves committed data", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });

    await waitFor(() => {
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
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

    clearPostedMessages();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });

    await act(async () => {
      await Promise.resolve();
    });

    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(screen.getByText("Alice")).toBeTruthy();
    expect(screen.getByRole("table")).toBeTruthy();
    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();
  });

  it("does not refetch on pure rerender with unchanged props", async () => {
    const view = renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });

    await waitFor(() => {
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
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
      expect(screen.getByRole("table")).toBeTruthy();
    });

    clearPostedMessages();

    view.rerender(
      <TableView
        connectionId="conn-1"
        database="main"
        schema="public"
        table="users"
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(screen.getByText("Alice")).toBeTruthy();
    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();
  });

  it("emits exactly one fetchPage message per paging and sorting action", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });

    await waitFor(() => {
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    });

    let currentFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: currentFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("table")).toBeTruthy();
    });

    clearPostedMessages();
    fireEvent.click(screen.getByRole("button", { name: "Next →" }));

    await waitFor(() => {
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 2, pageSize: 25 }),
      });
    });

    currentFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: currentFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    clearPostedMessages();
    fireEvent.change(screen.getByLabelText("Rows per page"), {
      target: { value: "100" },
    });

    await waitFor(() => {
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 100 }),
      });
    });

    currentFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: currentFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    clearPostedMessages();
    fireEvent.click(screen.getByText("id"));

    await waitFor(() => {
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({
          page: 1,
          pageSize: 100,
          sort: { column: "id", direction: "asc" },
        }),
      });
    });
  });

  it("does not refetch when switching to a value-based operator with an empty draft", async () => {
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
        rows,
        totalCount: rows.length,
      });
    });

    clearPostedMessages();

    fireEvent.click(
      screen.getByRole("button", { name: "name filter operator" }),
    );
    fireEvent.click(screen.getByRole("menuitemradio", { name: /Equals/i }));

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, DEBOUNCE + 50));
    });

    expect(getPostedMessages()).toEqual([]);
  });

  it("refetches immediately when changing a value-based operator with an existing value", async () => {
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
        rows,
        totalCount: rows.length,
      });
    });

    clearPostedMessages();

    fireEvent.change(screen.getByLabelText("name filter value"), {
      target: { value: "ali" },
    });

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, DEBOUNCE + 50));
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
      dispatchIncomingMessage("tableData", {
        fetchId: filteredFetch.fetchId,
        rows,
        totalCount: rows.length,
      });
    });

    clearPostedMessages();

    fireEvent.click(
      screen.getByRole("button", { name: "name filter operator" }),
    );
    fireEvent.click(screen.getByRole("menuitemradio", { name: /Equals/i }));

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({
          page: 1,
          filters: [{ column: "name", operator: "eq", value: "ali" }],
        }),
      });
    });
  });

  it("preserves committed rows when a refetch fails with a read error", async () => {
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
        rows,
        totalCount: 51,
      });
    });

    await waitFor(() => {
      expect(screen.getByText("Page 1 of 3")).toBeTruthy();
    });

    clearPostedMessages();

    fireEvent.click(screen.getByRole("button", { name: "Next →" }));

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 2, pageSize: 25 }),
      });
    });

    expect(
      screen.getByRole("status", { name: "Loading data..." }),
    ).toBeTruthy();
    expect(screen.getByText("51 rows total")).toBeTruthy();
    expect(screen.getByText("Page 1 of 3")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();

    const nextPageFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableError", {
        fetchId: nextPageFetch.fetchId,
        error: "Read failed",
      });
    });

    expect(screen.getByText("Read failed")).toBeTruthy();
    expect(screen.getByText("51 rows total")).toBeTruthy();
    expect(screen.getByText("Page 1 of 3")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();
    expect(
      screen.queryByRole("status", { name: "Loading data..." }),
    ).toBeNull();

    fireEvent.click(screen.getByTitle("Dismiss"));

    expect(screen.queryByText("Read failed")).toBeNull();
  });

  it("resets page, sort, and filters when a new table is initialized", async () => {
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
        rows,
        totalCount: 51,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("table")).toBeTruthy();
    });

    clearPostedMessages();

    fireEvent.click(screen.getByText("id"));

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({
          page: 1,
          sort: { column: "id", direction: "asc" },
        }),
      });
    });

    const sortedFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: sortedFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    fireEvent.change(screen.getByLabelText("Rows per page"), {
      target: { value: "100" },
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({
          page: 1,
          pageSize: 100,
          sort: { column: "id", direction: "asc" },
        }),
      });
    });

    const resizedFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: resizedFetch.fetchId,
        rows,
        totalCount: 51,
      });
    });

    fireEvent.change(screen.getByLabelText("name filter value"), {
      target: { value: "ali" },
    });

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, DEBOUNCE + 50));
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({
          page: 1,
          filters: [{ column: "name", operator: "like", value: "ali" }],
          sort: { column: "id", direction: "asc" },
        }),
      });
    });

    clearPostedMessages();

    await act(async () => {
      dispatchIncomingMessage("tableInit", {
        columns: noPkColumns,
        primaryKeyColumns: [],
      });
    });

    await waitFor(() => {
      expect(
        screen.getByRole("status", { name: "Loading data..." }),
      ).toBeTruthy();
      expect(screen.queryByRole("table")).toBeNull();
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
      text: "update users set name = 'Alicia' where id = 1;",
      contentType: "application/sql",
      sql: "update users set name = 'Alicia' where id = 1;",
      statementCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByRole("dialog")).toBeTruthy();
    });

    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "cancelMutationPreview",
        payload: { previewToken: "preview-1" },
      });
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
      text: "update users set name = 'Alicia' where id = 1;",
      contentType: "application/sql",
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

    const refetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: refetch.fetchId,
        rows: [
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
        ],
        totalCount: 2,
      });
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

    const editInput = await waitFor(() => screen.getByLabelText("Cell value"));
    expect((editInput as HTMLInputElement).readOnly).toBe(true);
  });

  it("uses view-style readonly behavior for readonly connections", async () => {
    renderTableView({ connectionReadOnly: true });

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

    expect(screen.queryByRole("button", { name: "Add Row" })).toBeNull();
    expect(screen.queryByRole("button", { name: /^Delete \(/ })).toBeNull();
    expect(screen.queryByLabelText("Select all rows")).toBeNull();
    expect(screen.queryByLabelText("Select row 1")).toBeNull();

    const aliceCell = screen.getByText("Alice").closest("td");
    if (!(aliceCell instanceof HTMLTableCellElement)) {
      throw new Error("Expected readonly cell");
    }

    fireEvent.doubleClick(aliceCell);

    const editInput = screen.getByLabelText("Cell value");
    expect((editInput as HTMLInputElement).readOnly).toBe(true);
    expect(screen.queryByRole("button", { name: "NULL" })).toBeNull();
  });

  it("applies readonly state from a later tableInit payload", async () => {
    renderTableView();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
      connectionReadOnly: false,
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({
          page: 1,
          pageSize: 25,
        }),
      });
    });

    const fetchPayload = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: fetchPayload.fetchId,
        rows,
        totalCount: rows.length,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Add Row" })).toBeTruthy();
    });

    clearPostedMessages();

    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
      connectionReadOnly: true,
    });

    await act(async () => {
      await Promise.resolve();
    });

    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);

    await waitFor(() => {
      expect(screen.queryByRole("button", { name: "Add Row" })).toBeNull();
    });
  });

  it("opens structured JSON cells in the large modal and marks persisted edits as pending on apply", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: structuredRows,
    });

    clearPostedMessages();

    await user.dblClick(getBodyCell("payload"));

    const dialog = screen.getByRole("dialog");
    expect(screen.getByText("Cell data: payload")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("json");

    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: {
        value:
          '{\n  "name": "Alice",\n  "meta": {\n    "active": false\n  }\n}',
      },
    });

    await user.click(within(dialog).getByRole("button", { name: "Apply" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();
    expect(getBodyCell("payload").style.background).toContain(
      "rgba(200, 150, 0, 0.23)",
    );
    expect(getPostedMessages()).toEqual([]);
  });

  it("focuses the structured editor instead of the close button when the modal opens", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: structuredRows,
    });

    await user.dblClick(getBodyCell("payload"));

    const editor = screen.getByLabelText("Cell data") as HTMLTextAreaElement;
    await waitFor(() => {
      expect(document.activeElement).toBe(editor);
    });
    expect(editor.selectionStart).toBe(editor.value.length);
    expect(editor.selectionEnd).toBe(editor.value.length);
    expect(editor.scrollTop).toBe(0);
  });

  it("discards modal-only structured cell edits on cancel", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: structuredRows,
    });

    await user.dblClick(getBodyCell("payload"));

    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: '{\n  "name": "Changed"\n}' },
    });

    await user.click(screen.getByRole("button", { name: "Cancel" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();

    await user.dblClick(getBodyCell("payload"));

    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe('{\n  "name": "Alice",\n  "meta": {\n    "active": true\n  }\n}');
  });

  it("supports nullable structured cells through the modal NULL action", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: structuredRows,
    });

    clearPostedMessages();

    await user.dblClick(getBodyCell("payload"));
    await user.click(screen.getByRole("button", { name: "NULL" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();
    expect(getBodyCell("payload").style.background).toContain(
      "rgba(200, 150, 0, 0.23)",
    );

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { payload: null },
          },
        ],
      },
    });
  });

  it("reopens nullable structured cells as pending NULL after clicking NULL", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: structuredRows,
    });

    await user.dblClick(getBodyCell("payload"));
    await user.click(screen.getByRole("button", { name: "NULL" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();

    await user.dblClick(getBodyCell("payload"));

    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe("");
  });

  it("keeps json-looking text columns on the inline editor path", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: [
        structuredColumns[0],
        {
          ...structuredColumns[1],
          name: "notes",
          type: "TEXT",
          nativeType: "TEXT",
          category: "text",
        },
      ],
      primaryKeyColumns: ["id"],
      dataRows: [{ id: 1, notes: '{"name":"Alice"}' }],
    });

    await user.dblClick(getBodyCell("notes"));

    expect(screen.getByLabelText("Cell value")).toBeTruthy();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("opens structured draft array cells in the large modal and reuses existing insert commit flow", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: structuredRows,
    });

    await user.click(screen.getByRole("button", { name: "Add Row" }));
    await user.dblClick(getBodyCell("tags"));

    expect(screen.getByTestId("monaco-language").textContent).toBe("json");

    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: '[\n  "one",\n  "two"\n]' },
    });

    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [],
        insertValues: {
          tags: '["one","two"]',
        },
      },
    });
  });

  it("opens xml-like structured cells in readonly mode without posting mutations", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: structuredRows,
      renderOverrides: { connectionReadOnly: true },
    });

    clearPostedMessages();

    await user.dblClick(getBodyCell("xml_doc"));

    expect(screen.getByText("Cell data: xml_doc")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("xml");
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).readOnly,
    ).toBe(true);
    expect(screen.queryByRole("button", { name: "Null" })).toBeNull();

    await user.click(screen.getByRole("button", { name: "Apply" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(getPostedMessages()).toEqual([]);
  });

  it("renders XML in table cells as a single line while keeping modal formatting behavior", async () => {
    const xmlWithSpacing =
      '<root>\n  <item id="1">Alice</item>\n  <item id="2">Bob</item>\n</root>';
    const expectedSingleLine = xmlWithSpacing.replace(/\r?\n/g, " ");

    await initializeCommittedTableData({
      columnDefs: structuredColumns,
      primaryKeyColumns: ["id"],
      dataRows: [
        {
          id: 1,
          payload: '{"name":"Alice","meta":{"active":true}}',
          tags: '["alpha","beta"]',
          xml_doc: xmlWithSpacing,
        },
      ],
    });

    const xmlCell = getBodyCell("xml_doc");
    const valueNode = xmlCell.querySelector("span");

    expect(valueNode).toBeTruthy();
    expect((valueNode as HTMLSpanElement).style.whiteSpace).toBe("pre");
    expect(valueNode?.textContent).toBe(expectedSingleLine);
  });

  it("requests delete preview with selected primary keys", async () => {
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
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 1 }] },
    });

    await act(async () => {
      dispatchIncomingMessage("tableMutationPreview", {
        previewToken: "delete-preview-1",
        kind: "deleteRows",
        title: "Apply changes to users",
        text: "delete from users where id = 1;",
        contentType: "application/sql",
        sql: "delete from users where id = 1;",
        statementCount: 1,
      });
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
      payload: { previewToken: "delete-preview-1" },
    });
  });

  it("cancels delete preview and allows sending delete request again", async () => {
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

    await user.click(screen.getByLabelText("Select row 1"));

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));

    expect(getLastPostedMessage()).toEqual({
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 1 }] },
    });

    dispatchIncomingMessage("tableMutationPreview", {
      previewToken: "delete-preview-cancel",
      kind: "deleteRows",
      title: "Apply changes to users",
      text: "delete from users where id = 1;",
      contentType: "application/sql",
      sql: "delete from users where id = 1;",
      statementCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByRole("dialog")).toBeTruthy();
    });

    const dialog = screen.getByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));

    expect(getLastPostedMessage()).toEqual({
      type: "cancelMutationPreview",
      payload: { previewToken: "delete-preview-cancel" },
    });

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));

    expect(getLastPostedMessage()).toEqual({
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 1 }] },
    });
  });

  it("renders JSON mutation previews with the preview text and JSON editor mode", async () => {
    const previewText = JSON.stringify(
      {
        TableName: "Users",
        Key: { userId: { S: "user-1" } },
      },
      null,
      2,
    );

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

    await act(async () => {
      dispatchIncomingMessage("tableMutationPreview", {
        previewToken: "preview-json",
        kind: "deleteRows",
        title: "Apply changes to users",
        text: previewText,
        contentType: "application/json",
        sql: "legacy fallback",
        statementCount: 1,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("dialog")).toBeTruthy();
    });

    expect(screen.getByTestId("monaco-language").textContent).toBe("json");
    expect(
      (screen.getByLabelText("Mutation preview") as HTMLTextAreaElement).value,
    ).toBe(previewText);
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

  it("keeps insert draft editing left-aligned but aligns committed values by column type", async () => {
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

    const tableEl = screen.getByRole("table");
    const headerCells = Array.from(
      tableEl.querySelectorAll("thead tr:first-child th"),
    );
    const idColumnIndex = headerCells.findIndex((cell) =>
      (cell.textContent ?? "").includes("id"),
    );
    const nameColumnIndex = headerCells.findIndex((cell) =>
      (cell.textContent ?? "").includes("name"),
    );

    if (idColumnIndex < 0 || nameColumnIndex < 0) {
      throw new Error("Expected id and name column headers");
    }

    const draftRow = tableEl.querySelector("tbody tr");
    const draftCells = Array.from(draftRow?.querySelectorAll("td") ?? []);
    const idCell = draftCells[idColumnIndex] ?? null;
    const nameCell = draftCells[nameColumnIndex] ?? null;

    if (
      !(idCell instanceof HTMLTableCellElement) ||
      !(nameCell instanceof HTMLTableCellElement)
    ) {
      throw new Error("Expected insert draft cells");
    }

    fireEvent.doubleClick(idCell);
    const idInput = screen.getByLabelText("Cell value") as HTMLInputElement;
    expect(idInput.style.textAlign).not.toBe("right");
    await user.type(idInput, "42");
    fireEvent.blur(idInput);

    await waitFor(() => {
      expect(screen.queryByLabelText("Cell value")).toBeNull();
    });

    const idDisplayContainer = idCell.querySelector("div > div");
    if (!(idDisplayContainer instanceof HTMLDivElement)) {
      throw new Error("Expected numeric draft display container");
    }

    expect(idDisplayContainer.style.justifyContent).toBe("flex-end");
    expect(idCell.textContent ?? "").toContain("42");

    fireEvent.doubleClick(nameCell);
    const nameInput = screen.getByLabelText("Cell value") as HTMLInputElement;
    expect(nameInput.style.textAlign).not.toBe("right");
    await user.type(nameInput, "Alicia");
    fireEvent.blur(nameInput);

    await waitFor(() => {
      expect(screen.queryByLabelText("Cell value")).toBeNull();
    });

    const nameDisplayContainer = nameCell.querySelector("div > div");
    if (!(nameDisplayContainer instanceof HTMLDivElement)) {
      throw new Error("Expected text draft display container");
    }

    expect(nameDisplayContainer.style.justifyContent).toBe("flex-start");
    expect(nameCell.textContent ?? "").toContain("Alicia");
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

    const initialFetch = lastFetchPayload();

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: initialFetch.fetchId,
        rows: [],
        totalCount: 0,
      });
    });

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "tags filter operator" }),
      ).toBeTruthy();
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
      screen.getByRole("button", { name: "title filter operator" }),
    );

    const titleMenu = screen.getByRole("menu", {
      name: "title filter operators",
    });
    expect(
      within(titleMenu).getByRole("menuitemradio", { name: /Equals/i }),
    ).toBeTruthy();
    expect(
      within(titleMenu).getByRole("menuitemradio", { name: /Contains/i }),
    ).toBeTruthy();
    expect(
      within(titleMenu).queryByRole("menuitemradio", { name: /Is NULL/i }),
    ).toBeNull();
    expect(
      within(titleMenu).queryByRole("menuitemradio", {
        name: /Is NOT NULL/i,
      }),
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
