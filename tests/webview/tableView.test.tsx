import {
  act,
  fireEvent,
  render,
  renderHook,
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
import {
  DEBOUNCE,
  parsePersistedTableDraft,
  parsePersistedTableViewState,
  serializeTableDraft,
} from "../../src/webview/components/table/tableViewHelpers";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import { CELL_PREVIEW_LIMIT } from "../../src/webview/utils/cellPreview";
import {
  clearPostedMessages as clearRawPostedMessages,
  dispatchIncomingMessage as dispatchRawIncomingMessage,
  expectNoAxeViolations,
  getPostedMessages,
  getLastPostedMessage as getRawLastPostedMessage,
  type PostedMessage,
} from "./testUtils";

let activeTableMutationOperationId: string | undefined;

describe("table draft persistence", () => {
  const tableKey = JSON.stringify(["conn", "db", "schema", "items"]);

  it("round-trips pending edits and inserted rows as serializable state", () => {
    const source = {
      entries: [
        {
          originalSignature: JSON.stringify([["id", 7]]),
          changes: new Map<string, unknown>([["name", "updated"]]),
        },
      ],
    };
    const serialized = serializeTableDraft(tableKey, source, [
      {
        name: { value: "new item" },
        id: { value: "__RAPIDB_INSERT_DEFAULT__" },
      },
    ]);
    const jsonSafe = JSON.parse(JSON.stringify(serialized)) as unknown;
    const restored = parsePersistedTableDraft(jsonSafe, tableKey);

    expect(restored?.restoreState.entries[0]?.changes).toEqual(
      new Map([["name", "updated"]]),
    );
    expect(restored?.newRows).toEqual([
      {
        name: { value: "new item" },
        id: { value: "__RAPIDB_INSERT_DEFAULT__" },
      },
    ]);
    expect(parsePersistedTableDraft(jsonSafe, "another-table")).toBeNull();
  });

  it("restores bounded page, sorting, and valid filter state", () => {
    expect(
      parsePersistedTableViewState(
        {
          tableKey,
          page: 3,
          pageSize: 100,
          sort: { column: "id", direction: "desc" },
          filters: { name: { operator: "like", value: "%literal%" } },
        },
        tableKey,
      ),
    ).toEqual({
      tableKey,
      page: 3,
      pageSize: 100,
      sort: { column: "id", direction: "desc" },
      filters: { name: { operator: "like", value: "%literal%" } },
    });
    expect(
      parsePersistedTableViewState(
        {
          tableKey,
          page: 0,
          pageSize: 10_000,
          sort: null,
          filters: {},
        },
        tableKey,
      ),
    ).toBeNull();
  });
});

function captureTableMutationOperationId(): void {
  const payload = getRawLastPostedMessage()?.payload;
  if (
    payload &&
    typeof payload === "object" &&
    "operationId" in payload &&
    typeof payload.operationId === "string"
  ) {
    expect(payload.operationId).toMatch(/^table-mutation:[^:]+:\d+$/);
    activeTableMutationOperationId = payload.operationId;
  }
}

function clearPostedMessages(): void {
  captureTableMutationOperationId();
  clearRawPostedMessages();
}

function getLastPostedMessage(): PostedMessage | undefined {
  const message = getRawLastPostedMessage();
  captureTableMutationOperationId();
  if (
    !message?.payload ||
    typeof message.payload !== "object" ||
    ![
      "applyChanges",
      "insertRow",
      "deleteRows",
      "confirmMutationPreview",
      "cancelMutationPreview",
    ].includes(message.type)
  ) {
    return message;
  }
  const { operationId: _operationId, ...payload } = message.payload as Record<
    string,
    unknown
  >;
  return { ...message, payload };
}

function dispatchIncomingMessage<TPayload>(
  type: string,
  payload?: TPayload,
): void {
  captureTableMutationOperationId();
  const mutationResponseTypes = new Set([
    "tableMutationPreview",
    "applyResult",
    "insertResult",
    "deleteResult",
  ]);
  if (
    mutationResponseTypes.has(type) &&
    payload &&
    typeof payload === "object" &&
    activeTableMutationOperationId
  ) {
    dispatchRawIncomingMessage(type, {
      ...payload,
      operationId: activeTableMutationOperationId,
    });
    return;
  }
  dispatchRawIncomingMessage(type, payload);
}

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

const redisKeyColumns: ColumnTypeMeta[] = [
  {
    name: "key",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
  {
    name: "value",
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
  mongoRowIdentity?: boolean;
  connectionReadOnly?: boolean;
  defaultPageSize?: number;
  isView?: boolean;
  table?: string;
  displayTableName?: string;
}) {
  return render(
    <TableView
      connectionId="conn-1"
      database="main"
      schema="public"
      connectionReadOnly={overrides?.connectionReadOnly}
      mongoRowIdentity={overrides?.mongoRowIdentity}
      defaultPageSize={overrides?.defaultPageSize}
      isView={overrides?.isView}
      table={overrides?.table ?? "users"}
      displayTableName={overrides?.displayTableName}
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
  mongoIdTypes?: Array<"objectId" | "string" | null | undefined> | null;
  renderOverrides?: {
    mongoRowIdentity?: boolean;
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
      ...(overrides && Object.hasOwn(overrides, "mongoIdTypes")
        ? { mongoIdTypes: overrides.mongoIdTypes }
        : {}),
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
  it("uses the display name throughout loading and committed accessible labels", async () => {
    renderTableView({
      table: "__rapidb:all-keys",
      displayTableName: "All keys",
    });
    expect(
      screen.getByRole("main", { name: "Table data for All keys" }),
    ).toBeTruthy();
    act(() =>
      dispatchIncomingMessage("tableInit", {
        columns,
        primaryKeyColumns: ["id"],
      }),
    );
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    const fetch = lastFetchPayload();
    act(() =>
      dispatchIncomingMessage("tableData", {
        fetchId: fetch.fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    expect(
      screen.getByRole("main", { name: "Table data for All keys" }),
    ).toBeTruthy();
    expect(
      screen.queryByRole("main", { name: "Table data for __rapidb:all-keys" }),
    ).toBeNull();
  });

  it.each([
    ["absent", undefined],
    ["empty", []],
    ["short", ["string"]],
    ["undefined entry", ["string", undefined]],
    ["null entry", ["string", null]],
    ["null array", null],
  ] as const)("fails closed on Mongo rows with %s ID hints", async (_label, hints) => {
    const user = userEvent.setup();
    const hex = "507f1f77bcf86cd799439011";
    await initializeCommittedTableData({
      renderOverrides: { mongoRowIdentity: true },
      columnDefs: [
        {
          ...columns[0],
          name: "_id",
          nativeType: "objectId",
          type: "objectId",
          category: "text",
        },
        columns[1],
      ],
      primaryKeyColumns: ["_id"],
      dataRows: [
        { _id: "safe", name: "Safe" },
        { _id: hex, name: "Ambiguous" },
      ],
      ...(hints === undefined
        ? {}
        : { mongoIdTypes: hints === null ? null : [...hints] }),
    });
    clearPostedMessages();
    const invalid = screen.getByRole("checkbox", {
      name: "Select row 2",
    }) as HTMLInputElement;
    expect(invalid.disabled).toBe(true);
    expect(invalid.title).toContain("MongoDB _id type");
    await user.click(invalid);
    await user.dblClick(getBodyCell("name", 1));
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).readOnly,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "Apply" }));
    const cell = getBodyCell("name", 1);
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    clearPostedMessages();
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "Wrong document",
      }),
    );
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(getBodyCell("name", 1).textContent).toBe("Ambiguous");
    expect(getPostedMessages()).toEqual([]);
  });

  it("does not send a raw Mongo _id when missing hints are bypassed with an injected selection", () => {
    const { result } = renderHook(() =>
      useTableMutationController({
        mongoRowIdentity: true,
        canEditRows: true,
        loadingRef: { current: false },
        columnsRef: { current: columns },
        fetchPageRef: { current: vi.fn() },
        pkColsRef: { current: ["_id"] },
        preserveScrollPositionRef: { current: vi.fn() },
        rowsRef: {
          current: [{ _id: "507f1f77bcf86cd799439011", name: "Ambiguous" }],
        },
        mongoIdTypesRef: { current: [] },
        selected: new Set([0]),
      }),
    );
    clearPostedMessages();
    act(() => {
      result.current.deleteSelected();
      result.current.commitCellEdit(0, columns[1], "Changed", "Ambiguous");
      result.current.applyChanges();
    });
    expect(result.current.pendingEdits.size).toBe(0);
    expect(result.current.mutErr).toContain("MongoDB _id type");
    expect(getPostedMessages()).toEqual([]);
  });

  it("allows a SQL _id string without Mongo metadata or type guessing", async () => {
    const user = userEvent.setup();
    const hex = "507f1f77bcf86cd799439011";
    await initializeCommittedTableData({
      columnDefs: [
        {
          ...columns[0],
          name: "_id",
          nativeType: "TEXT",
          type: "TEXT",
          category: "text",
        },
        columns[1],
      ],
      primaryKeyColumns: ["_id"],
      dataRows: [{ _id: hex, name: "SQL row" }],
    });
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 1",
        }) as HTMLInputElement
      ).disabled,
    ).toBe(false);
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Edited" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { _id: hex }, changes: { name: "Edited" } }],
      },
    });
  });

  it("blocks a NULL component of a composite row key", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      columnDefs: [...compositeKeyColumns, columns[1]],
      primaryKeyColumns: ["tenant_id", "user_id"],
      dataRows: [
        { tenant_id: null, user_id: "same", name: "Unsafe" },
        { tenant_id: "tenant", user_id: "same", name: "Safe" },
      ],
    });
    clearPostedMessages();
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 1",
        }) as HTMLInputElement
      ).disabled,
    ).toBe(true);
    await user.dblClick(getBodyCell("name", 0));
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).readOnly,
    ).toBe(true);
    expect(
      screen.getByText(/primary key tenant_id is NULL or missing/),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Apply" }));
    expect(getPostedMessages()).toEqual([]);
    await user.click(screen.getByRole("checkbox", { name: "Select all rows" }));
    await user.click(screen.getByRole("button", { name: /Delete/ }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "deleteRows",
      payload: { primaryKeysList: [{ tenant_id: "tenant", user_id: "same" }] },
    });
  });

  it("checks Mongo ID hints per row without blocking supported IDs because sampled native type is null", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      renderOverrides: { mongoRowIdentity: true },
      columnDefs: [
        {
          ...columns[0],
          name: "_id",
          nativeType: "null",
          type: "null",
          category: "text",
          nullable: true,
        },
        columns[1],
      ],
      primaryKeyColumns: ["_id"],
      dataRows: [
        { _id: "null", name: "String" },
        { _id: "123", name: "Unsupported" },
        { _id: "507f1f77bcf86cd799439011", name: "ObjectId" },
        { _id: "invalid", name: "Malformed" },
      ],
      mongoIdTypes: ["string", null, "objectId", "objectId"],
    });
    clearPostedMessages();
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 1",
        }) as HTMLInputElement
      ).disabled,
    ).toBe(false);
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 2",
        }) as HTMLInputElement
      ).disabled,
    ).toBe(true);
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 3",
        }) as HTMLInputElement
      ).disabled,
    ).toBe(false);
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 4",
        }) as HTMLInputElement
      ).disabled,
    ).toBe(true);
    await user.click(screen.getByRole("checkbox", { name: "Select all rows" }));
    await user.click(screen.getByRole("button", { name: /Delete/ }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "deleteRows",
      payload: {
        primaryKeysList: [
          { _id: { $rapidbMongoId: { type: "string", value: "null" } } },
          {
            _id: {
              $rapidbMongoId: {
                type: "objectId",
                value: "507f1f77bcf86cd799439011",
              },
            },
          },
        ],
      },
    });
  });

  it.each([
    null,
    undefined,
  ])("blocks editing and row selection for %s keys while safe rows remain editable", async (id) => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      columnDefs: columns.map((column) =>
        column.name === "id"
          ? {
              ...column,
              type: "TEXT",
              nativeType: "TEXT",
              category: "text",
              nullable: true,
            }
          : column,
      ),
      dataRows: [
        { ...(id === undefined ? {} : { id }), name: "Unsafe" },
        { id: "safe", name: "Safe" },
      ],
    });
    clearPostedMessages();
    const invalidCheckbox = screen.getByRole("checkbox", {
      name: "Select row 1",
    }) as HTMLInputElement;
    expect(invalidCheckbox.disabled).toBe(true);
    expect(invalidCheckbox.title).toContain("NULL or missing");
    await user.click(invalidCheckbox);
    expect(invalidCheckbox.checked).toBe(false);
    await user.dblClick(getBodyCell("id", 0));
    expect(screen.queryByLabelText("Cell value")).toBeNull();
    await user.dblClick(getBodyCell("name", 0));
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).readOnly,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "Apply" }));
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(getPostedMessages()).toEqual([]);
    await user.dblClick(getBodyCell("name", 1));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Edited safe" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          { primaryKeys: { id: "safe" }, changes: { name: "Edited safe" } },
        ],
      },
    });
  });

  it("selects and deletes only safely addressable rows in a mixed page", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      dataRows: [
        { id: null, name: "First null" },
        { id: null, name: "Second null" },
        { id: 3, name: "Safe" },
      ],
    });
    clearPostedMessages();
    await user.click(screen.getByRole("checkbox", { name: "Select all rows" }));
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 1",
        }) as HTMLInputElement
      ).checked,
    ).toBe(false);
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 2",
        }) as HTMLInputElement
      ).checked,
    ).toBe(false);
    expect(
      (
        screen.getByRole("checkbox", {
          name: "Select row 3",
        }) as HTMLInputElement
      ).checked,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: /Delete/ }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 3 }] },
    });
  });

  it("rejects a whole batch paste spanning a NULL-key row without staging its safe prefix", async () => {
    await initializeCommittedTableData({
      dataRows: [
        { id: 1, name: "Safe" },
        { id: null, name: "Unsafe" },
      ],
    });
    const first = getBodyCell("name", 0);
    const second = getBodyCell("name", 1);
    fireEvent.mouseDown(first, { button: 0 });
    fireEvent.mouseUp(first);
    fireEvent.mouseDown(second, { button: 0, shiftKey: true });
    fireEvent.mouseUp(second);
    clearPostedMessages();
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    clearPostedMessages();
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "Changed safe\nChanged unsafe",
      }),
    );
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(getBodyCell("name", 0).textContent).toBe("Safe");
    expect(getBodyCell("name", 1).textContent).toBe("Unsafe");
    expect(screen.getByText(/Row 2:.*NULL or missing/)).toBeTruthy();
    expect(getPostedMessages()).toEqual([]);
  });

  it("guards injected invalid selection and direct single/batch/mixed edits in the mutation hook", () => {
    const { result } = renderHook(() =>
      useTableMutationController({
        canEditRows: true,
        loadingRef: { current: false },
        columnsRef: { current: columns },
        fetchPageRef: { current: vi.fn() },
        pkColsRef: { current: ["id"] },
        preserveScrollPositionRef: { current: vi.fn() },
        rowsRef: {
          current: [
            { id: 1, name: "Safe" },
            { id: null, name: "Unsafe" },
          ],
        },
        mongoIdTypesRef: { current: [] },
        selected: new Set([0, 1]),
      }),
    );
    clearPostedMessages();
    const edits = [
      { rowIdx: 0, column: columns[1], newVal: "Changed", originalVal: "Safe" },
      {
        rowIdx: 1,
        column: columns[1],
        newVal: "Changed",
        originalVal: "Unsafe",
      },
    ];
    act(() => {
      result.current.deleteSelected();
      result.current.handleStartEdit(1, columns[0]);
      result.current.commitCellEdit(1, columns[1], "Changed", "Unsafe");
      result.current.commitBatchCellEdits(edits);
      result.current.commitMixedBatchEdits([], edits);
      result.current.applyChanges();
    });
    expect(result.current.pendingEdits.size).toBe(0);
    expect(result.current.editCell).toBeNull();
    expect(result.current.mutErr).toContain("NULL or missing");
    expect(getPostedMessages()).toEqual([]);
  });

  async function stageMetadataWork() {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Edited Alice" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.doubleClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "Draft name" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    clearPostedMessages();
    return user;
  }

  it.each([
    ["delimiter collision", ["a", "a|a"], ["a|a", "a"]],
    ["ordinary reorder", ["a", "b"], ["b", "a"]],
  ] as const)("resets grid selection and preserves pending edits on %s metadata reorder", async (_label, initialNames, reorderedNames) => {
    const user = userEvent.setup();
    const beforeNames: string[] = [...initialNames];
    const afterNames: string[] = [...reorderedNames];
    const editColumn = beforeNames[1];
    const pendingValue = `Pending ${editColumn}`;
    const columnForName = (name: string): ColumnTypeMeta => ({
      ...(name === "a" ? columns[0] : columns[1]),
      name,
    });

    await initializeCommittedTableData({
      columnDefs: beforeNames.map(columnForName),
      primaryKeyColumns: ["a"],
      dataRows: [{ a: 1, [editColumn]: "Original" }],
    });

    await user.dblClick(getBodyCell(editColumn));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: pendingValue },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));

    const selectedCell = getBodyCell(editColumn);
    fireEvent.mouseDown(selectedCell, { button: 0 });
    fireEvent.mouseUp(selectedCell);
    expect(selectedCell.className).toContain("rdb-tcell-anchor");
    expect(getBodyCell(editColumn).textContent).toContain(pendingValue);

    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: afterNames.map(columnForName),
        primaryKeyColumns: ["a"],
      }),
    );

    const visibleColumnNames = Array.from(
      screen
        .getByRole("table")
        .querySelectorAll("thead tr:first-child th[data-column-id]"),
    )
      .map((header) => header.getAttribute("data-column-id"))
      .filter((name): name is string => name !== null && name !== "__sel");
    expect(visibleColumnNames).toEqual(afterNames);
    expect(
      screen.getByRole("table").querySelectorAll(".rdb-tcell-anchor"),
    ).toHaveLength(0);
    expect(getBodyCell(editColumn).textContent).toContain(pendingValue);
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();

    clearPostedMessages();
    fireEvent.paste(window);
    expect(postedMessagesOfType("readClipboard")).toHaveLength(0);

    const reorderedEditCell = getBodyCell(editColumn);
    fireEvent.mouseDown(reorderedEditCell, { button: 0 });
    fireEvent.mouseUp(reorderedEditCell);
    expect(reorderedEditCell.className).toContain("rdb-tcell-anchor");
    expect(reorderedEditCell.getAttribute("data-col")).toBe(
      String(afterNames.indexOf(editColumn) + 1),
    );
  });

  it.each([
    "duplicate",
    "add",
    "header",
  ])("preserves edits, drafts, history and row identities on %s metadata refresh", async (change) => {
    const user = await stageMetadataWork();
    const nextColumns =
      change === "add"
        ? [...columns, { ...columns[1], name: "extra" }]
        : change === "header"
          ? columns.map((column) => ({ ...column, isForeignKey: true }))
          : columns;
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: nextColumns,
        primaryKeyColumns: ["id"],
      }),
    );
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(screen.queryByText(/Schema conflict/)).toBeNull();
    expect(getBodyCell("name").textContent).toContain("Draft name");
    expect(getBodyCell("name", 1).textContent).toContain("Edited Alice");
    if (change === "add")
      expect(getBodyCell("extra").textContent).toContain("DEFAULT");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name").textContent).toContain("DEFAULT");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    expect(getBodyCell("name").textContent).toContain("Draft name");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Edited Alice" },
            originalValues: { name: "Alice" },
          },
        ],
        insertValues: [{ name: "Draft name" }],
      },
    });
  });

  it.each([
    "drop",
    "rename",
    "type",
    "pk",
    "computed",
  ])("retains work and explicitly blocks old-schema writes on %s metadata conflict until manual revert", async (change) => {
    const user = await stageMetadataWork();
    const nextColumns =
      change === "drop"
        ? columns.filter((column) => column.name !== "name")
        : columns.map((column) =>
            column.name === "name"
              ? {
                  ...column,
                  ...(change === "rename" ? { name: "renamed" } : {}),
                  ...(change === "type"
                    ? {
                        type: "integer",
                        nativeType: "integer",
                        category: "number" as const,
                      }
                    : {}),
                  ...(change === "computed"
                    ? { isComputed: true, computedExpression: "id + 1" }
                    : {}),
                }
              : column,
          );
    const nextKeys = change === "pk" ? ["name"] : ["id"];
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: nextColumns,
        primaryKeyColumns: nextKeys,
      }),
    );
    expect(
      screen.getByText(/Schema conflict: pending work is retained/),
    ).toBeTruthy();
    expect(getBodyCell("name").textContent).toContain("Draft name");
    expect(getBodyCell("name", 1).textContent).toContain("Edited Alice");
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    await act(async () =>
      dispatchRawIncomingMessage("tableData", {
        rows: [{ id: 999, name: "Stale read" }],
        totalCount: 1,
      }),
    );
    expect(getBodyCell("name").textContent).toContain("Draft name");
    expect(getBodyCell("name", 1).textContent).toContain("Edited Alice");
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ id: 1, name: "Alice", renamed: "Renamed Alice" }],
        totalCount: 1,
      }),
    );
    expect(screen.queryByText(/Schema conflict/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    if (change === "rename")
      expect(getBodyCell("renamed").textContent).toContain("Renamed Alice");
    if (change === "drop")
      expect(screen.queryByRole("columnheader", { name: /^name/ })).toBeNull();
    // Old undo snapshots must not be replayed into the new schema/data.
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
    });
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
  });

  it.each([
    "success",
    "error",
    "partial",
  ])("processes late %s using the retained operation ID after metadata refresh during preview/execution", async (outcome) => {
    const user = await stageMetadataWork();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: columns.map((column) =>
          column.name === "name" ? { ...column, name: "renamed" } : column,
        ),
        primaryKeyColumns: ["id"],
      }),
    );
    // A preview prepared before refresh may arrive late and must still be cancellable/confirmable.
    await act(async () =>
      dispatchRawIncomingMessage("tableMutationPreview", {
        operationId,
        previewToken: "schema-preview",
        kind: "applyChanges",
        title: "Apply changes to users",
        sql: "UPDATE users SET name = 'Edited Alice'",
        statementCount: 1,
      }),
    );
    expect(screen.getByRole("dialog")).toBeTruthy();
    await user.click(
      within(screen.getByRole("dialog")).getByRole("button", {
        name: "Apply Changes",
      }),
    );
    expect(getRawLastPostedMessage()).toMatchObject({
      type: "confirmMutationPreview",
      payload: { operationId, previewToken: "schema-preview" },
    });
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [
          ...columns.map((column) =>
            column.name === "name" ? { ...column, name: "renamed" } : column,
          ),
          { ...columns[1], name: "extra" },
        ],
        primaryKeyColumns: ["id"],
      }),
    );
    await act(async () =>
      dispatchRawIncomingMessage("applyResult", {
        operationId,
        success: outcome !== "error",
        error: outcome === "error" ? "Late schema error" : undefined,
        rowOutcomes: [
          {
            rowIndex: 0,
            success: outcome === "success",
            status: outcome === "success" ? "applied" : "verification_failed",
          },
        ],
      }),
    );
    expect(screen.queryByRole("button", { name: "Applying…" })).toBeNull();
    if (outcome === "success") {
      await waitFor(() =>
        expect(getRawLastPostedMessage()?.type).toBe("fetchPage"),
      );
      await act(async () =>
        dispatchRawIncomingMessage("tableData", {
          fetchId: lastFetchPayload().fetchId,
          rows: [{ id: 1, renamed: "Authoritative" }],
          totalCount: 1,
        }),
      );
      expect(getBodyCell("renamed").textContent).toContain("Authoritative");
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
    } else {
      expect(screen.getByText(/Schema conflict/)).toBeTruthy();
      expect(
        getBodyCell("name", outcome === "error" ? 1 : 0).textContent,
      ).toContain("Edited Alice");
      if (outcome === "error") {
        expect(screen.getByText("Late schema error")).toBeTruthy();
        expect(getBodyCell("name").textContent).toContain("Draft name");
      }
    }
  });

  it.each([
    false,
    true,
  ])("reconciles a failed apply only when changes are possible (unknown=%s)", async (outcomeUnknown) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Alice" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()?.type).toBe("applyChanges");

    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: outcomeUnknown
          ? "Transaction outcome is unknown. Refresh and verify before retrying."
          : "UPDATE verification requires rollback support on this table.",
        changesPossible: outcomeUnknown,
        outcomeUnknown,
        rowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "skipped",
            message: outcomeUnknown ? "Outcome unknown" : "Not executed",
          },
        ],
      }),
    );

    expect(postedMessagesOfType("fetchPage")).toHaveLength(
      outcomeUnknown ? 1 : 0,
    );
    expect(getBodyCell("name").textContent).toContain("Pending Alice");
    expect(
      screen.getByText(
        outcomeUnknown
          ? /outcome is unknown/
          : /verification requires rollback support/,
      ),
    ).toBeTruthy();
  });

  it("reconciles applied and uncertain UPDATE rows independently", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();

    await user.dblClick(getBodyCell("name", 0));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Alice" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.dblClick(getBodyCell("name", 1));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Bob" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    const result = {
      operationId,
      success: false,
      error:
        "The update partially succeeded. Refresh and verify before retrying.",
      changesPossible: true,
      outcomeUnknown: true,
      rowOutcomes: [
        { rowIndex: 0, success: true, status: "applied" },
        { rowIndex: 1, success: false, status: "unknown" },
      ],
    };
    await act(async () => {
      // A transport retry can duplicate a result already received by the view.
      dispatchRawIncomingMessage("applyResult", result);
      dispatchRawIncomingMessage("applyResult", result);
    });

    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [
          { id: 1, name: "Authoritative Alice" },
          { id: 2, name: "Bob" },
        ],
        totalCount: 2,
      }),
    );

    expect(getBodyCell("name", 0).textContent).toContain("Authoritative Alice");
    expect(getBodyCell("name", 0).textContent).not.toContain("Pending Alice");
    expect(getBodyCell("name", 1).textContent).toContain("Pending Bob");
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();
    expect(screen.getByText(/partially succeeded/)).toBeTruthy();
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    const retry = getRawLastPostedMessage()?.payload as {
      operationId: string;
      updates: unknown[];
    };
    expect(retry.operationId).not.toBe(operationId);
    expect(retry.updates).toEqual([
      {
        primaryKeys: { id: 2 },
        changes: { name: "Pending Bob" },
        originalValues: { name: "Bob" },
      },
    ]);
    await act(async () => dispatchRawIncomingMessage("applyResult", result));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(screen.getByRole("button", { name: "Applying…" })).toBeTruthy();
  });

  it("keeps an unknown Redis rename unresolved when another client creates the proposed key after a full snapshot", async () => {
    const user = userEvent.setup();
    const stageCell = async (
      column: string,
      rowIndex: number,
      value: string,
    ) => {
      await user.dblClick(getBodyCell(column, rowIndex));
      fireEvent.change(screen.getByLabelText("Cell data"), {
        target: { value },
      });
      await user.click(screen.getByRole("button", { name: "Apply" }));
    };
    await initializeCommittedTableData({
      columnDefs: redisKeyColumns,
      primaryKeyColumns: ["key"],
      dataRows: [
        { key: "users:1", value: "old value" },
        { key: "users:3", value: "old other" },
      ],
    });

    await stageCell("key", 0, "users:2");
    await stageCell("value", 0, "pending value");
    await stageCell("value", 1, "pending other");

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({ type: "applyChanges" });

    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Redis update acknowledgement was lost.",
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [
          { rowIndex: 0, success: false, status: "unknown" },
          { rowIndex: 1, success: true, status: "applied" },
        ],
      }),
    );

    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [
          { key: "users:2", value: "concurrent client value" },
          { key: "users:3", value: "confirmed other" },
        ],
        totalCount: 2,
      }),
    );

    expect(getBodyCell("key", 0).textContent).toContain("users:2");
    expect(getBodyCell("value", 0).textContent).toContain(
      "concurrent client value",
    );
    expect(getBodyCell("value", 0).textContent).not.toContain("pending value");
    expect(getBodyCell("value", 1).textContent).toContain("confirmed other");
    expect(getBodyCell("value", 1).textContent).not.toContain("pending other");
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [
          { key: "users:2", value: "concurrent client value" },
          { key: "users:3", value: "confirmed other" },
        ],
        totalCount: 2,
      }),
    );
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(getBodyCell("value", 0).textContent).not.toContain("pending value");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
  });

  it("keeps unknown apply reconciliation locked after a failed read and restores edits by original identity", async () => {
    const user = userEvent.setup();
    const stageCell = async (
      column: string,
      rowIndex: number,
      value: string,
    ) => {
      await user.dblClick(getBodyCell(column, rowIndex));
      fireEvent.change(screen.getByLabelText("Cell data"), {
        target: { value },
      });
      await user.click(screen.getByRole("button", { name: "Apply" }));
    };
    await initializeCommittedTableData({
      columnDefs: redisKeyColumns,
      primaryKeyColumns: ["key"],
      dataRows: [
        { key: "users:1", value: "old value" },
        { key: "users:3", value: "old other" },
      ],
    });

    await stageCell("key", 0, "users:2");
    await stageCell("value", 0, "pending value");
    await stageCell("value", 1, "pending other");
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(1);

    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Redis update acknowledgement was lost.",
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [
          { rowIndex: 0, success: false, status: "unknown" },
          { rowIndex: 1, success: true, status: "applied" },
        ],
      }),
    );
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    const failedFetchId = lastFetchPayload().fetchId;
    await act(async () =>
      dispatchIncomingMessage("tableError", {
        fetchId: failedFetchId,
        error: "Temporary verification read failure",
      }),
    );

    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "Refresh" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);

    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [
          { key: "users:1", value: "old value" },
          { key: "users:3", value: "confirmed other" },
        ],
        totalCount: 2,
      }),
    );

    expect(getBodyCell("key", 0).textContent).toContain("users:2");
    expect(getBodyCell("value", 0).textContent).toContain("pending value");
    expect(getBodyCell("value", 1).textContent).toContain("confirmed other");
    expect(getBodyCell("value", 1).textContent).not.toContain("pending other");
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { key: "users:1" },
            changes: { key: "users:2", value: "pending value" },
            originalValues: { key: "users:1", value: "old value" },
          },
        ],
      },
    });
  });

  it("does not let Revert All unlock mutations before unknown apply verification", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Alice" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByLabelText("Select row 1"));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Write outcome is unknown.",
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [{ rowIndex: 0, success: false, status: "unknown" }],
      }),
    );
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    expect(
      screen
        .getByRole("button", { name: "Revert All" })
        .hasAttribute("disabled"),
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    expect(getBodyCell("name").textContent).toContain("Pending Alice");
    await act(async () =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetchPayload().fetchId,
        error: "Verification read failed",
      }),
    );

    await user.click(screen.getByRole("button", { name: "Revert All" }));
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(
      (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      screen
        .getByRole("button", { name: "Delete (1)" })
        .hasAttribute("disabled"),
    ).toBe(true);
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    fireEvent.doubleClick(getBodyCell("name"));
    expect(screen.queryByLabelText("Cell data")).toBeNull();
    expect(screen.queryByText(/^DEFAULT$/)).toBeNull();
    expect(postedMessagesOfType("deleteRows")).toHaveLength(0);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(1);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    expect(
      (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
  });

  it.each([
    "apply",
    "delete",
  ] as const)("ends verified unknown %s recovery on Revert All and guards unrelated edits without losing undo", async (kind) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Abandoned edit" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    clearPostedMessages();
    if (kind === "apply") {
      await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    } else {
      await user.click(screen.getByLabelText("Select row 2"));
      await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    }
    await act(async () =>
      dispatchIncomingMessage(
        kind === "apply" ? "applyResult" : "deleteResult",
        {
          success: false,
          error: "Write acknowledgement lost.",
          changesPossible: true,
          outcomeUnknown: true,
        },
      ),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    expect(
      screen.queryByText(/first refresh may precede a late write/),
    ).toBeNull();
    expect(screen.queryByText(/acknowledgement lost/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();

    await user.dblClick(getBodyCell("name", 1));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Unrelated edit" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    expect(postedMessagesOfType("deleteRows")).toHaveLength(0);
    expect(
      screen.getByText(/Apply or revert pending changes before loading/),
    ).toBeTruthy();
    expect(getBodyCell("name", 1).textContent).toContain("Unrelated edit");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name", 1).textContent).toContain("Bob");
    expect(screen.queryByText("Unrelated edit")).toBeNull();
    // Status dismissal works normally after recovery has been abandoned.
    await user.click(screen.getByTitle("Dismiss"));
    expect(
      screen.queryByText(/Apply or revert pending changes before loading/),
    ).toBeNull();
    await user.click(screen.getByRole("button", { name: "Redo" }));
    expect(getBodyCell("name", 1).textContent).toContain("Unrelated edit");
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
  });

  it("preserves unknown DELETE warning through Add Row, dismiss, editors and repeated verification until verified revert", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    await act(async () =>
      dispatchIncomingMessage("deleteResult", {
        success: false,
        error: "Delete acknowledgement lost.",
        changesPossible: true,
        outcomeUnknown: true,
      }),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    const expectLateWriteWarning = () =>
      expect(
        screen.getByText(/first refresh may precede a late write/),
      ).toBeTruthy();
    expectLateWriteWarning();
    await user.click(
      screen.getByRole("button", { name: "Dismiss mutation error" }),
    );
    expectLateWriteWarning();
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    expectLateWriteWarning();
    fireEvent.doubleClick(getBodyCell("name"));
    expectLateWriteWarning();
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "Preserved draft" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    await user.dblClick(getBodyCell("name", 2));
    expectLateWriteWarning();
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Bob" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    clearPostedMessages();
    fireEvent.click(screen.getByText("id", { exact: true }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expectLateWriteWarning();
    await user.click(
      screen.getByRole("button", { name: "Dismiss mutation error" }),
    );
    expectLateWriteWarning();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expectLateWriteWarning();
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [rows[1]],
        totalCount: 1,
      }),
    );
    expect(getBodyCell("name", 0).textContent).toContain("Preserved draft");
    expect(getBodyCell("name", 1).textContent).toContain("Pending Bob");
    expectLateWriteWarning();
    await user.click(
      screen.getByRole("button", { name: "Dismiss mutation error" }),
    );
    expectLateWriteWarning();
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    expect(postedMessagesOfType("deleteRows")).toHaveLength(0);

    await user.click(screen.getByRole("button", { name: "Revert All" }));
    expect(
      screen.queryByText(/first refresh may precede a late write/),
    ).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Dismiss mutation error" }),
    ).toBeNull();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Unrelated Bob edit" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(
      screen.getByText(/Apply or revert pending changes before loading/),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name").textContent).toContain("Bob");
    expect(screen.queryByText("Unrelated Bob edit")).toBeNull();
  });

  it("keeps unknown DELETE warning and write fence when second verification fails and work is reverted", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    await act(async () =>
      dispatchIncomingMessage("deleteResult", {
        success: false,
        error: "Delete acknowledgement lost.",
        changesPossible: true,
        outcomeUnknown: true,
      }),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await act(async () =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetchPayload().fetchId,
        error: "Second verification failed",
      }),
    );
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    await user.click(
      screen.getByRole("button", { name: "Dismiss mutation error" }),
    );
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Add Row" }).hasAttribute("disabled"),
    ).toBe(true);
    expect(screen.queryByText(/^DEFAULT$/)).toBeNull();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(2);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    expect(postedMessagesOfType("deleteRows")).toHaveLength(0);
  });

  it.each([
    "Add Row",
    "Dismiss mutation error",
  ])("clears a known DELETE error normally through %s", async (action) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    await act(async () =>
      dispatchIncomingMessage("deleteResult", {
        success: false,
        error: "Known delete failure",
        changesPossible: true,
        outcomeUnknown: false,
      }),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    expect(screen.getByText("Known delete failure")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: action }));
    expect(screen.queryByText("Known delete failure")).toBeNull();
    expect(
      screen.queryByText(/first refresh may precede a late write/),
    ).toBeNull();
  });

  it("keeps a colliding unknown key rename explicitly unresolved instead of attaching to the other row", async () => {
    const user = userEvent.setup();
    const stageCell = async (
      column: string,
      rowIndex: number,
      value: string,
    ) => {
      await user.dblClick(getBodyCell(column, rowIndex));
      fireEvent.change(screen.getByLabelText("Cell data"), {
        target: { value },
      });
      await user.click(screen.getByRole("button", { name: "Apply" }));
    };
    await initializeCommittedTableData({
      columnDefs: redisKeyColumns,
      primaryKeyColumns: ["key"],
      dataRows: [
        { key: "users:1", value: "source value" },
        { key: "users:2", value: "existing target value" },
      ],
    });

    await stageCell("key", 0, "users:2");
    await stageCell("value", 0, "must not attach");

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Redis update acknowledgement was lost.",
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [{ rowIndex: 0, success: false, status: "unknown" }],
      }),
    );
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ key: "users:2", value: "existing target value" }],
        totalCount: 1,
      }),
    );

    expect(getBodyCell("value").textContent).toContain("existing target value");
    expect(getBodyCell("value").textContent).not.toContain("must not attach");
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ key: "users:2", value: "existing target value" }],
        totalCount: 1,
      }),
    );
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);

    await user.click(screen.getByRole("button", { name: "Revert All" }));
    expect(screen.queryByRole("alert")).toBeNull();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
  });

  it.each([
    [
      "unsafe-target preflight",
      "INSERT verification requires rollback support.",
    ],
    [
      "verification rollback",
      "INSERT verification failed; the transaction was not committed.",
    ],
  ])("preserves a single insert draft and undo without reconciliation after $0", async (_kind, error) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.doubleClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "Pending insert" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [],
        insertValues: [{ name: "Pending insert" }],
      },
    });
    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error,
        changesPossible: false,
        outcomeUnknown: false,
      }),
    );

    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(getBodyCell("name").textContent).toContain("Pending insert");
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
    });
    expect(getBodyCell("name").textContent).toContain("DEFAULT");
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(getBodyCell("name").textContent).toContain("Pending insert");
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
  });

  it("retains editor-only work while metadata is deferred, then preserves the committed draft", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.doubleClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "Still typing" },
    });
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...columns, { ...columns[1], name: "extra" }],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(
      (screen.getByLabelText("Cell value") as HTMLInputElement).value,
    ).toBe("Still typing");
    fireEvent.blur(screen.getByLabelText("Cell value"));
    await waitFor(() =>
      expect(getBodyCell("extra").textContent).toContain("DEFAULT"),
    );
    expect(getBodyCell("name").textContent).toContain("Still typing");
  });

  it("does not permit editing new persisted columns against an unread old-row snapshot", async () => {
    const user = await stageMetadataWork();
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [
          ...columns,
          { ...columns[0], name: "extra", isPrimaryKey: false },
        ],
        primaryKeyColumns: ["id"],
      }),
    );
    fireEvent.doubleClick(getBodyCell("extra", 1));
    expect(screen.queryByLabelText("Cell value")).toBeNull();
    fireEvent.doubleClick(getBodyCell("extra", 0));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "42" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Edited Alice" },
            originalValues: { name: "Alice" },
          },
        ],
        insertValues: [{ name: "Draft name", extra: "42" }],
      },
    });
  });

  it.each([
    "new-column",
    "rectangle",
    "draft-to-persisted",
    "refresh-during-request",
  ])("reports unread-column paste atomically through TableView: %s", async (scenario) => {
    const user = await stageMetadataWork();
    const refresh = () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [
          ...columns,
          { ...columns[0], name: "extra", isPrimaryKey: false },
        ],
        primaryKeyColumns: ["id"],
      });
    if (scenario !== "refresh-during-request") await act(async () => refresh());
    const cell = getBodyCell(
      scenario === "new-column" || scenario === "draft-to-persisted"
        ? "extra"
        : "name",
      scenario === "draft-to-persisted" ? 0 : 1,
    );
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    const selectionClass = cell.className;
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    if (scenario === "refresh-during-request") await act(async () => refresh());
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text:
          scenario === "draft-to-persisted"
            ? "41\n42"
            : scenario === "new-column"
              ? "42"
              : "Overwritten\t42",
      }),
    );
    if (scenario === "refresh-during-request") {
      // Metadata refresh deliberately invalidates the fetch epoch, even
      // while pending work keeps the old rows visible. Keep cancellation.
      expect(screen.queryByText(/Paste failed/)).toBeNull();
    } else {
      expect(screen.getByText(/Paste failed/)).toBeTruthy();
      expect(screen.getByText(/extra.*has not been read/)).toBeTruthy();
    }
    expect(getBodyCell("name", 1).textContent).toBe("Edited Alice");
    expect(getBodyCell("extra", 0).textContent).toContain("DEFAULT");
    expect(cell.className).toBe(selectionClass);
    // A rejected rectangle must not introduce an undo snapshot.
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name", 0).textContent).toContain("DEFAULT");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            changes: { name: "Edited Alice" },
            originalValues: { name: "Alice" },
          },
        ],
        insertValues: [{ name: "Draft name" }],
      },
    });
  });

  it.each([
    "Y",
    "н",
  ])("redos with the physical KeyY for key %s", async (key) => {
    const user = await stageMetadataWork();
    await user.click(screen.getByRole("button", { name: "Undo" }));
    fireEvent.keyDown(document.body, {
      key,
      code: "KeyY",
      ctrlKey: true,
      metaKey: true,
    });
    expect(getBodyCell("name", 0).textContent).toBe("Draft name");
  });

  it("allows draft-only paste into an unread new column with one undo step", async () => {
    const user = await stageMetadataWork();
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [
          ...columns,
          { ...columns[0], name: "extra", isPrimaryKey: false },
        ],
        primaryKeyColumns: ["id"],
      }),
    );
    const cell = getBodyCell("extra", 0);
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "42",
      }),
    );
    expect(screen.queryByText(/Paste failed/)).toBeNull();
    expect(getBodyCell("extra", 0).textContent).toBe("42");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("extra", 0).textContent).toContain("DEFAULT");
    expect(getBodyCell("name", 0).textContent).toBe("Draft name");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: { insertValues: [{ name: "Draft name", extra: "42" }] },
    });
  });

  it("preflights the live baseline before closing an editor or mutating either batch", () => {
    const extra = { ...columns[0], name: "extra", isPrimaryKey: false };
    const columnsRef = { current: columns };
    const committedColumnNamesRef = {
      current: new Set(columns.map((column) => column.name)),
    };
    const { result } = renderHook(() =>
      useTableMutationController({
        canEditRows: true,
        loadingRef: { current: false },
        committedColumnNamesRef,
        columnsRef,
        fetchPageRef: { current: vi.fn() },
        pkColsRef: { current: ["id"] },
        preserveScrollPositionRef: { current: vi.fn() },
        rowsRef: { current: rows },
        mongoIdTypesRef: { current: [] },
        selected: new Set(),
      }),
    );
    act(() => result.current.startInsertRow());
    act(() => result.current.handleStartDraftEdit(0, columns[1]));
    // The callback must consult current metadata/baseline refs, not the
    // schema under which it was obtained.
    const batch = result.current.commitBatchCellEdits;
    const mixed = result.current.commitMixedBatchEdits;
    columnsRef.current = [...columns, extra];
    const persisted = [
      {
        rowIdx: 0,
        column: columns[1],
        newVal: "Safe prefix",
        originalVal: "Alice",
      },
      { rowIdx: 0, column: extra, newVal: "42", originalVal: undefined },
      { rowIdx: 1, column: extra, newVal: "43", originalVal: undefined },
    ];
    act(() => {
      expect(batch(persisted)).toHaveLength(2);
      expect(
        mixed([{ rowIdx: 0, column: extra, newVal: "41" }], persisted),
      ).toHaveLength(2);
    });
    expect(result.current.pendingEdits.size).toBe(0);
    expect(result.current.newRows[0]).not.toHaveProperty("extra");
    expect(result.current.editCell).toEqual({
      kind: "draft",
      rowIdx: 0,
      col: "name",
    });
    act(() => result.current.undoAction());
    expect(result.current.newRows).toHaveLength(0);
  });

  it.each([
    "column-overflow",
    "read-only-row",
  ])("rejects unavailable draft rectangle targets without applying a prefix: %s", async (scenario) => {
    const user = userEvent.setup();
    await initializeCommittedTableData(
      scenario === "read-only-row"
        ? {
            columnDefs: noPkColumns,
            primaryKeyColumns: [],
            dataRows: noPkRows,
          }
        : undefined,
    );
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    const cell = getBodyCell("name");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    // The keyboard route also permits paste into drafts without a PK.
    fireEvent.keyDown(cell, {
      key: "v",
      code: "KeyV",
      ctrlKey: true,
      metaKey: true,
    });
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text:
          scenario === "read-only-row" ? "Draft\nPersisted" : "Draft\tExtra",
      }),
    );
    expect(screen.getByText(/Paste failed/)).toBeTruthy();
    expect(getBodyCell("name").textContent).toContain("DEFAULT");
    expect(getBodyCell("name", 1).textContent).toBe("Alice");
  });

  it("allows persisted paste after authoritative added-column rows commit", async () => {
    const user = await stageMetadataWork();
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [
          ...columns,
          { ...columns[0], name: "extra", isPrimaryKey: false },
        ],
        primaryKeyColumns: ["id"],
      }),
    );
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    await waitFor(() => expect(getLastPostedMessage()?.type).toBe("fetchPage"));
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ id: 1, name: "Alice", extra: 7 }],
        totalCount: 1,
      }),
    );
    const cell = getBodyCell("extra");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "42",
      }),
    );
    expect(screen.queryByText(/Paste failed/)).toBeNull();
    expect(getBodyCell("extra").textContent).toBe("42");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("extra").textContent).toBe("7");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [{ changes: { extra: "42" }, originalValues: { extra: 7 } }],
      },
    });
  });

  it.each([
    "Win32",
    "MacIntel",
  ])("uses the platform redo modifier and preserves Shift+Z on %s", async (platform) => {
    vi.spyOn(navigator, "platform", "get").mockReturnValue(platform);
    const user = await stageMetadataWork();
    await user.click(screen.getByRole("button", { name: "Undo" }));
    const modifier =
      platform === "MacIntel" ? { metaKey: true } : { ctrlKey: true };
    const ignoredEvents = [
      { key: "y", code: "KeyY" },
      {
        key: "y",
        code: "KeyY",
        ...(platform === "MacIntel" ? { ctrlKey: true } : { metaKey: true }),
      },
      { key: "Y", code: "KeyY", ...modifier, shiftKey: true },
      { key: "y", code: "KeyY", ...modifier, altKey: true },
      { key: "y", code: "KeyX", ...modifier },
    ];
    for (const init of ignoredEvents) {
      const event = new KeyboardEvent("keydown", {
        bubbles: true,
        cancelable: true,
        ...init,
      });
      fireEvent(document.body, event);
      expect(event.defaultPrevented).toBe(false);
      expect(getBodyCell("name", 0).textContent).toContain("DEFAULT");
    }
    for (const tag of ["input", "textarea", "select"]) {
      const input = document.createElement(tag);
      document.body.appendChild(input);
      const event = new KeyboardEvent("keydown", {
        bubbles: true,
        cancelable: true,
        key: "н",
        code: "KeyY",
        ...modifier,
      });
      fireEvent(input, event);
      expect(event.defaultPrevented).toBe(false);
      expect(getBodyCell("name", 0).textContent).toContain("DEFAULT");
      input.remove();
    }
    fireEvent.keyDown(document.body, { key: "н", code: "KeyY", ...modifier });
    expect(getBodyCell("name", 0).textContent).toBe("Draft name");
    fireEvent.keyDown(document.body, { key: "я", code: "KeyZ", ...modifier });
    expect(getBodyCell("name", 0).textContent).toContain("DEFAULT");
    fireEvent.keyDown(document.body, {
      key: "Я",
      code: "KeyZ",
      ...modifier,
      shiftKey: true,
    });
    expect(getBodyCell("name", 0).textContent).toBe("Draft name");
    vi.restoreAllMocks();
  });

  it("removes active table resize listeners on unmount without a late width commit", async () => {
    await initializeCommittedTableData();
    const add = vi.spyOn(document, "addEventListener");
    const remove = vi.spyOn(document, "removeEventListener");
    const handle = screen.getByRole("button", { name: "Resize name column" });
    fireEvent.mouseDown(handle, { button: 0, clientX: 200 });
    const listeners = add.mock.calls.filter(
      ([type]) => type === "mousemove" || type === "mouseup",
    );
    expect(listeners).toHaveLength(2);
    fireEvent.mouseMove(document, { clientX: 240 });
    const header = handle.closest("th") as HTMLElement;
    const width = header.style.width;
    // Public route: replacing the table with a new-schema loading view unmounts the grid.
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...columns, { ...columns[1], name: "extra" }],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(screen.queryByRole("table")).toBeNull();
    for (const [type, listener] of listeners) {
      expect(
        remove.mock.calls.some(
          ([removedType, removedListener]) =>
            removedType === type && removedListener === listener,
        ),
      ).toBe(true);
    }
    fireEvent.mouseMove(document, { clientX: 500 });
    fireEvent.mouseUp(document, { clientX: 500 });
    expect(header.style.width).toBe(width);
    add.mockRestore();
    remove.mockRestore();
  });

  it("ends table resize at the mouseup width and replaces gestures without leftover listeners", async () => {
    await initializeCommittedTableData();
    const add = vi.spyOn(document, "addEventListener");
    const remove = vi.spyOn(document, "removeEventListener");
    const handle = screen.getByRole("button", { name: "Resize name column" });
    const header = handle.closest("th") as HTMLElement;
    const startWidth = Number.parseFloat(header.style.width);
    fireEvent.mouseDown(handle, { button: 2, clientX: 200 });
    expect(
      add.mock.calls.filter(
        ([type]) => type === "mousemove" || type === "mouseup",
      ),
    ).toHaveLength(0);
    fireEvent.mouseDown(handle, { button: 0, clientX: 200 });
    fireEvent.mouseMove(document, { clientX: 240 });
    expect(Number.parseFloat(header.style.width)).toBe(startWidth + 40);
    fireEvent.mouseDown(handle, { button: 0, clientX: 240 });
    fireEvent.mouseUp(document, { clientX: 270 });
    expect(Number.parseFloat(header.style.width)).toBe(startWidth + 70);
    for (const [type, listener] of add.mock.calls.filter(
      ([t]) => t === "mousemove" || t === "mouseup",
    )) {
      expect(
        remove.mock.calls.some(([t, l]) => t === type && l === listener),
      ).toBe(true);
    }
    fireEvent.mouseMove(document, { clientX: 500 });
    fireEvent.mouseUp(document, { clientX: 500 });
    expect(Number.parseFloat(header.style.width)).toBe(startWidth + 70);
    expect(document.querySelector(".rapidb-column-drag-ghost")).toBeNull();
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    add.mockRestore();
    remove.mockRestore();
  });

  it.each([
    "unmount",
    "resize",
  ])("cancels active header drag through the table UI on %s", async (end) => {
    await initializeCommittedTableData();
    document.body.style.cursor = "crosshair";
    document.body.style.userSelect = "text";
    const add = vi.spyOn(document, "addEventListener");
    const remove = vi.spyOn(document, "removeEventListener");
    const header = screen
      .getByRole("table")
      .querySelector(
        'thead tr:first-child th[data-column-id="name"]',
      ) as HTMLElement;
    fireEvent.mouseDown(header, { button: 0, clientX: 20 });
    fireEvent.mouseMove(document, { clientX: 40, clientY: 14 });
    expect(document.querySelector(".rapidb-column-drag-ghost")).not.toBeNull();
    const dragListeners = add.mock.calls.filter(
      ([t]) => t === "mousemove" || t === "mouseup",
    );
    if (end === "unmount") {
      await act(async () =>
        dispatchIncomingMessage("tableInit", {
          intent: "metadataRefresh",
          columns: [...columns, { ...columns[1], name: "extra" }],
          primaryKeyColumns: ["id"],
        }),
      );
      expect(screen.queryByRole("table")).toBeNull();
    } else {
      fireEvent.mouseDown(
        within(header).getByRole("button", { name: "Resize name column" }),
        { button: 0, clientX: 40 },
      );
    }
    expect(document.querySelector(".rapidb-column-drag-ghost")).toBeNull();
    expect(document.body.style.cursor).toBe("crosshair");
    expect(document.body.style.userSelect).toBe("text");
    expect(header.hasAttribute("data-column-dragging")).toBe(false);
    for (const [type, listener] of dragListeners) {
      expect(
        remove.mock.calls.some(([t, l]) => t === type && l === listener),
      ).toBe(true);
    }
    clearPostedMessages();
    fireEvent.mouseMove(document, { clientX: 500, clientY: 14 });
    fireEvent.mouseUp(document, { clientX: 500, clientY: 14 });
    expect(getPostedMessages()).toEqual([]);
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
    add.mockRestore();
    remove.mockRestore();
  });

  it("requires authoritative rows after a PK refresh even when the new-schema read fails", async () => {
    await initializeCommittedTableData();
    clearPostedMessages();
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: columns.map((column) => ({
          ...column,
          isPrimaryKey: column.name === "name",
        })),
        primaryKeyColumns: ["name"],
      }),
    );
    await waitFor(() =>
      expect(getRawLastPostedMessage()?.type).toBe("fetchPage"),
    );
    expect(screen.queryByRole("table")).toBeNull();
    await act(async () =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetchPayload().fetchId,
        error: "New schema read failed",
      }),
    );
    expect(screen.getByText("New schema read failed")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Add Row" })).toBeNull();
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
  });

  it("loads added-column values once compatible pending work is manually reverted", async () => {
    const user = await stageMetadataWork();
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [
          ...columns,
          { ...columns[0], name: "extra", isPrimaryKey: false },
        ],
        primaryKeyColumns: ["id"],
      }),
    );
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ id: 1, name: "Alice", extra: 42 }],
        totalCount: 1,
      }),
    );
    expect(getBodyCell("extra").textContent).toContain("42");
    fireEvent.doubleClick(getBodyCell("extra"));
    expect(
      (screen.getByLabelText("Cell value") as HTMLInputElement).value,
    ).toBe("42");
  });

  it.each([
    "undo-before-refresh",
    "refresh-before-last-undo",
  ])("preserves redo-only work across compatible metadata: %s", async (order) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Redo Alice" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    clearPostedMessages();
    const undo = () =>
      fireEvent.keyDown(document.body, {
        key: "z",
        code: "KeyZ",
        ctrlKey: true,
        metaKey: true,
      });
    if (order === "undo-before-refresh") undo();
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...columns, { ...columns[1], name: "extra" }],
        primaryKeyColumns: ["id"],
      }),
    );
    if (order === "refresh-before-last-undo") undo();
    expect(getBodyCell("name").textContent).toContain("Alice");
    expect(getBodyCell("name").textContent).not.toContain("Redo Alice");
    expect(screen.getByText("Undo/redo history retained")).toBeTruthy();
    expect(
      (screen.getByRole("button", { name: "Redo" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Redo" }));
    expect(getBodyCell("name").textContent).toContain("Redo Alice");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Redo Alice" },
            originalValues: { name: "Alice" },
          },
        ],
      },
    });
    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Retained edit",
      }),
    );
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [rows[1], rows[0]],
        totalCount: 2,
      }),
    );
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(getBodyCell("name").textContent).toContain("Bob");
    expect(getBodyCell("name", 1).textContent).toContain("Alice");
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
  });

  it("keeps redo-only schema conflicts explicitly revertible without replaying history into new rows", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Redo Alice" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    clearPostedMessages();
    await act(async () =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: columns.map((column) =>
          column.name === "name" ? { ...column, name: "renamed" } : column,
        ),
        primaryKeyColumns: ["id"],
      }),
    );
    expect(screen.getByText(/Schema conflict/)).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [
          { id: 2, renamed: "Bob" },
          { id: 1, renamed: "Alice" },
        ],
        totalCount: 2,
      }),
    );
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(getBodyCell("renamed").textContent).toContain("Bob");
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
  });

  it.each([
    true,
    false,
  ])("processes a late delete result (success=%s) after PK metadata refresh", async (success) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: noPkColumns,
        primaryKeyColumns: [],
      }),
    );
    await act(async () =>
      dispatchRawIncomingMessage("deleteResult", {
        operationId,
        success,
        error: success ? undefined : "Late delete error",
      }),
    );
    await waitFor(() =>
      expect(getRawLastPostedMessage()?.type).toBe("fetchPage"),
    );
    await act(async () =>
      dispatchRawIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: success ? [rows[1]] : rows,
        totalCount: success ? 1 : 2,
      }),
    );
    expect(screen.queryByRole("button", { name: "Deleting…" })).toBeNull();
    expect(screen.getByText(/Reduced table mode/)).toBeTruthy();
    if (!success) expect(screen.getByText("Late delete error")).toBeTruthy();
  });

  async function stageDeleteMetadataReconciliation() {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    const oldFetchId = lastFetchPayload().fetchId;
    await user.dblClick(getBodyCell("name", 1));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Bob" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.doubleClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "Draft name" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    clearPostedMessages();
    return { user, operationId, oldFetchId };
  }

  function partialDeleteResult(operationId: string) {
    dispatchRawIncomingMessage("deleteResult", {
      operationId,
      success: false,
      affectedRows: 1,
      changesPossible: true,
      outcomeUnknown: false,
      error: "Delete partially changed the data. Refresh and verify.",
      rowOutcomes: [
        {
          rowIndex: 0,
          primaryKeys: { id: 1 },
          status: "deleted",
          success: true,
        },
      ],
    });
  }

  it("refreshes after a committed delete whose verification read was not executed", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    clearPostedMessages();

    await act(async () =>
      dispatchRawIncomingMessage("deleteResult", {
        operationId,
        success: false,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: false,
        error:
          "Delete committed, but verification failed: 1 row was affected. Refresh and verify before retrying. Verification read was not executed.",
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: { id: 1 },
            status: "unknown",
            success: false,
          },
        ],
      }),
    );

    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expect(
      screen.getByText(/Delete committed, but verification failed/),
    ).toBeTruthy();
    expect(screen.queryByText(/late write/)).toBeNull();
  });

  it.each([
    "add",
    "header",
  ])("resumes delete reconciliation through queued compatible %s metadata, read failure and late responses", async (change) => {
    const { user, operationId, oldFetchId } =
      await stageDeleteMetadataReconciliation();
    const nextColumns =
      change === "add"
        ? [...columns, { ...columns[1], name: "extra" }]
        : columns.map((column) => ({ ...column, isForeignKey: true }));
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: nextColumns,
        primaryKeyColumns: ["id"],
      }),
    );
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    await act(async () => {
      partialDeleteResult(operationId);
      fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    });
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    const firstRead = lastFetchPayload().fetchId;
    expect(screen.queryByText(/Schema metadata changed/)).toBeNull();
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    await act(async () => {
      dispatchRawIncomingMessage("tableData", {
        fetchId: oldFetchId,
        rows: [{ id: 99, name: "Stale" }],
        totalCount: 1,
      });
      dispatchRawIncomingMessage("tableError", {
        fetchId: oldFetchId,
        error: "Stale error",
      });
      // A second compatible refresh invalidates the first reconciliation read.
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...nextColumns, { ...columns[1], name: "extra2" }],
        primaryKeyColumns: ["id"],
      });
    });
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(2),
    );
    const failedRead = lastFetchPayload().fetchId;
    await act(async () =>
      dispatchRawIncomingMessage("tableError", {
        fetchId: failedRead,
        error: "Reconciliation read failed",
      }),
    );
    expect(screen.getByText("Reconciliation read failed")).toBeTruthy();
    expect(getBodyCell("name").textContent).toContain("Draft name");
    expect(getBodyCell("name", 2).textContent).toContain("Pending Bob");
    await act(async () => {
      dispatchRawIncomingMessage("tableData", {
        fetchId: firstRead,
        rows: [rows[1]],
        totalCount: 1,
      });
      dispatchRawIncomingMessage("tableData", {
        fetchId: failedRead,
        rows: [rows[1]],
        totalCount: 1,
      });
    });
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    expect(postedMessagesOfType("deleteRows")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(3);
    const finalRead = lastFetchPayload().fetchId;
    await act(async () =>
      dispatchRawIncomingMessage("tableData", {
        fetchId: finalRead,
        rows: [{ ...rows[1], extra: "Fresh", extra2: "Fresh2" }],
        totalCount: 1,
      }),
    );
    expect(getBodyCell("name").textContent).toContain("Draft name");
    expect(getBodyCell("name", 1).textContent).toContain("Pending Bob");
    expect(getBodyCell("extra2", 1).textContent).toContain("Fresh2");
    await act(async () =>
      dispatchRawIncomingMessage("tableError", {
        fetchId: finalRead,
        error: "Duplicate terminal error",
      }),
    );
    expect(screen.queryByText("Duplicate terminal error")).toBeNull();
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name").textContent).toContain("Bob");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    await user.click(screen.getByRole("button", { name: "Redo" }));
    await user.click(screen.getByRole("button", { name: "Redo" }));
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getRawLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 2 },
            changes: { name: "Pending Bob" },
            originalValues: { name: "Bob" },
          },
        ],
        insertValues: [{ name: "Draft name" }],
      },
    });
  });

  it.each([
    "type",
    "drop",
  ])("retains delete reconciliation and work through incompatible %s metadata until manual revert", async (change) => {
    const { user, operationId, oldFetchId } =
      await stageDeleteMetadataReconciliation();
    const nextColumns =
      change === "drop"
        ? [columns[0]]
        : columns.map((column) =>
            column.name === "name"
              ? {
                  ...column,
                  type: "INTEGER",
                  nativeType: "INTEGER",
                  category: "integer",
                }
              : column,
          );
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: nextColumns,
        primaryKeyColumns: ["id"],
      }),
    );
    await act(async () => partialDeleteResult(operationId));
    expect(
      screen.getByText(/Schema conflict: pending work is retained/),
    ).toBeTruthy();
    expect(getBodyCell("name").textContent).toContain("Draft name");
    expect(getBodyCell("name", 2).textContent).toContain("Pending Bob");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    await act(async () =>
      dispatchRawIncomingMessage("tableData", {
        fetchId: oldFetchId,
        rows: [rows[1]],
        totalCount: 1,
      }),
    );
    expect(getBodyCell("name", 2).textContent).toContain("Pending Bob");
    await user.click(screen.getByRole("button", { name: "Revert All" }));
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    await act(async () =>
      dispatchRawIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ id: 2, name: 7 }],
        totalCount: 1,
      }),
    );
    expect(screen.queryByText(/Schema conflict/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(
      (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
  });

  it.each([
    "conflict",
    "readFailure",
  ])("resets deferred delete reconciliation on table initialization after %s", async (timing) => {
    const { operationId, oldFetchId } =
      await stageDeleteMetadataReconciliation();
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        primaryKeyColumns: ["id"],
        columns:
          timing === "conflict"
            ? [columns[0]]
            : [...columns, { ...columns[1], name: "extra" }],
      }),
    );
    await act(async () => partialDeleteResult(operationId));
    let invalidatedFetchId = oldFetchId;
    if (timing === "readFailure") {
      await waitFor(() =>
        expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
      );
      invalidatedFetchId = lastFetchPayload().fetchId;
      await act(async () =>
        dispatchRawIncomingMessage("tableError", {
          fetchId: invalidatedFetchId,
          error: "Delete read failed before reset",
        }),
      );
    }
    const beforeReset = postedMessagesOfType("fetchPage").length;
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        columns: [...columns, { ...columns[1], name: "different" }],
        primaryKeyColumns: ["id"],
      }),
    );
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(beforeReset + 1),
    );
    await act(async () => {
      partialDeleteResult(operationId);
      dispatchRawIncomingMessage("tableData", {
        fetchId: invalidatedFetchId,
        rows: [rows[1]],
        totalCount: 1,
      });
      dispatchRawIncomingMessage("tableError", {
        fetchId: invalidatedFetchId,
        error: "Stale reset error",
      });
    });
    expect(postedMessagesOfType("fetchPage")).toHaveLength(beforeReset + 1);
    await act(async () =>
      dispatchRawIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ id: 100, name: "Reset rows", different: "Loaded" }],
        totalCount: 1,
      }),
    );
    expect(getBodyCell("name").textContent).toContain("Reset rows");
    expect(screen.queryByText(/Schema conflict/)).toBeNull();
    expect(screen.queryByText("Stale reset error")).toBeNull();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(
      (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    expect(screen.queryByRole("button", { name: "Undo" })).toBeNull();
  });

  it.each([
    "cancel",
    "nochange",
    "emptySuccess",
  ])("does not invent reconciliation for %s after compatible metadata was queued", async (outcome) => {
    const { user, operationId } = await stageDeleteMetadataReconciliation();
    if (outcome === "cancel") {
      await act(async () =>
        dispatchRawIncomingMessage("tableMutationPreview", {
          operationId,
          previewToken: "queued-delete",
          kind: "deleteRows",
          title: "Delete rows",
          text: "DELETE FROM users WHERE id = 1",
          sql: "DELETE FROM users WHERE id = 1",
          contentType: "application/sql",
          statementCount: 1,
        }),
      );
    }
    await act(async () =>
      dispatchRawIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [...columns, { ...columns[1], name: "extra" }],
        primaryKeyColumns: ["id"],
      }),
    );
    if (outcome === "cancel") {
      await user.click(
        within(screen.getByRole("dialog")).getByRole("button", {
          name: "Cancel",
        }),
      );
      await act(async () => partialDeleteResult(operationId)); // Cancelled operation's late result is stale.
    } else {
      await act(async () =>
        dispatchRawIncomingMessage("deleteResult", {
          operationId,
          success: outcome === "emptySuccess",
          changesPossible: false,
          outcomeUnknown: false,
          affectedRows: 0,
          rowOutcomes: [],
          error: "Delete was not started",
        }),
      );
    }
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(screen.queryByText(/Schema metadata changed/)).toBeNull();
    expect(getBodyCell("name").textContent).toContain("Draft name");
    expect(getBodyCell("name", 2).textContent).toContain("Pending Bob");
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
  });

  async function stageDirtyDeleteWork(dataRows = rows) {
    const user = userEvent.setup();
    await initializeCommittedTableData({ dataRows });
    for (let index = 0; index < dataRows.length; index++) {
      await user.dblClick(getBodyCell("name", index));
      fireEvent.change(screen.getByLabelText("Cell data"), {
        target: { value: `Pending ${dataRows[index].name}` },
      });
      await user.click(screen.getByRole("button", { name: "Apply" }));
    }
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.doubleClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "Delete survivor draft" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    return user;
  }

  it.each([
    false,
    true,
  ])("removes confirmed dirty delete work from both history stacks, keeps B and drafts (stale read=%s)", async (staleRead) => {
    const user = await stageDirtyDeleteWork();
    // The redo stack also contains A's edit and must be pruned, not cleared.
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    clearPostedMessages();
    const result = {
      operationId,
      success: true,
      affectedRows: 1,
      changesPossible: true,
      outcomeUnknown: false,
      rowOutcomes: [
        {
          rowIndex: 0,
          primaryKeys: { id: 1 },
          status: "deleted",
          success: true,
        },
      ],
    };
    await act(async () => dispatchRawIncomingMessage("deleteResult", result));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    await act(async () => {
      dispatchRawIncomingMessage("deleteResult", result); // Delivery retry is stale.
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: staleRead ? [rows[1], rows[0]] : [rows[1]],
        totalCount: staleRead ? 2 : 1,
      });
    });
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expect(screen.queryByText(/Unable to match/)).toBeNull();
    expect(getBodyCell("name", 1).textContent).toContain("Pending Bob");
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
    // Traverse all preserved past/future snapshots. A must never reappear as
    // an edit, even if the verification read still shows its old identity.
    for (let index = 0; index < 4; index++) {
      const undo = screen.queryByRole("button", {
        name: "Undo",
      }) as HTMLButtonElement | null;
      if (!undo || undo.disabled) break;
      await user.click(undo);
      expect(screen.queryByText("Pending Alice")).toBeNull();
    }
    for (let index = 0; index < 4; index++) {
      const redo = screen.queryByRole("button", {
        name: "Redo",
      }) as HTMLButtonElement | null;
      if (!redo || redo.disabled) break;
      await user.click(redo);
      expect(screen.queryByText("Pending Alice")).toBeNull();
    }
    expect(getBodyCell("name").textContent).toContain("Delete survivor draft");
    expect(getBodyCell("name", 1).textContent).toContain("Pending Bob");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getRawLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 2 },
            changes: { name: "Pending Bob" },
            originalValues: { name: "Bob" },
          },
        ],
        insertValues: [{ name: "Delete survivor draft" }],
      },
    });
  });

  it.each([
    { stack: "past", outcomeUnknown: true },
    { stack: "future", outcomeUnknown: true },
    { stack: "past", outcomeUnknown: false },
  ] as const)("preserves $stack delete history through missing and duplicate original identities until reordered recovery (unknown=$outcomeUnknown)", async ({
    stack,
    outcomeUnknown,
  }) => {
    const user = userEvent.setup();
    const carol = { id: 3, name: "Carol" };
    await initializeCommittedTableData({ dataRows: [...rows, carol] });
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.doubleClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "History survivor draft" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    const edit = async (column: string, index: number, value: string) => {
      await user.dblClick(getBodyCell(column, index));
      const modal = screen.queryByLabelText("Cell data");
      const input = modal ?? screen.getByLabelText("Cell value");
      fireEvent.change(input, {
        target: { value },
      });
      if (modal) {
        await user.click(screen.getByRole("button", { name: "Apply" }));
      } else {
        fireEvent.blur(input);
      }
    };
    await edit("name", 1, "Deleted Alice edit");
    // Proposed PK collides with Carol, but only original id=2 identifies B.
    await edit("id", 2, "3");
    await edit("name", 2, "First Bob edit");
    await edit("name", 2, "Second Bob edit");
    if (stack === "future") {
      await user.click(screen.getByRole("button", { name: "Undo" }));
    }
    const currentEdit = stack === "past" ? "Second Bob edit" : "First Bob edit";
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByLabelText("Select row 2"));
    await user.click(screen.getByRole("button", { name: "Delete (2)" }));
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    clearPostedMessages();
    const result = {
      operationId,
      success: false,
      affectedRows: 1,
      changesPossible: true,
      outcomeUnknown,
      rowOutcomes: [
        {
          rowIndex: 0,
          primaryKeys: { id: 1 },
          status: "deleted",
          success: true,
        },
        {
          rowIndex: 1,
          primaryKeys: { id: 2 },
          status: "unknown",
          success: false,
        },
      ],
    };
    await act(async () => dispatchRawIncomingMessage("deleteResult", result));
    const read = async (dataRows: typeof rows) => {
      await act(async () =>
        dispatchIncomingMessage("tableData", {
          fetchId: lastFetchPayload().fetchId,
          rows: dataRows,
          totalCount: dataRows.length,
        }),
      );
    };
    await read([carol, rows[0]]); // Missing B, stale A, and occupied proposed PK.
    expect(screen.getByText(/Unable to match 1 row/)).toBeTruthy();
    expect(getBodyCell("name", 1).textContent).toBe("Carol");
    expect(getBodyCell("name", 2).textContent).toBe("Alice");
    expect(getBodyCell("name").textContent).toContain("History survivor draft");
    if (outcomeUnknown) {
      expect(
        screen.getByText(/first refresh may precede a late write/),
      ).toBeTruthy();
    }
    // Neither buttons nor keyboard may consume either stack while unresolved.
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await user.click(screen.getByRole("button", { name: "Redo" }));
    fireEvent.keyDown(document.body, {
      code: "KeyZ",
      key: "z",
      ctrlKey: true,
      metaKey: true,
    });
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await read([
      rows[1],
      carol,
      { ...rows[1], name: "Duplicate Bob" },
      rows[0],
    ]);
    expect(screen.getByText(/Unable to match 1 row/)).toBeTruthy();
    expect(screen.queryByText(currentEdit)).toBeNull();
    expect(screen.queryByText("Deleted Alice edit")).toBeNull();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await read([carol, rows[0], rows[1]]); // B returns uniquely at a different index.
    expect(screen.queryByText(/Unable to match/)).toBeNull();
    expect(getBodyCell("name", 3).textContent).toBe(currentEdit);
    const assertSurvivors = () => {
      expect(getBodyCell("name", 1).textContent).toBe("Carol");
      expect(getBodyCell("name", 2).textContent).toBe("Alice");
      expect(getBodyCell("name").textContent).toContain(
        "History survivor draft",
      );
      expect(screen.queryByText("Deleted Alice edit")).toBeNull();
    };
    assertSurvivors();
    if (stack === "past") {
      await user.click(screen.getByRole("button", { name: "Undo" }));
      expect(getBodyCell("name", 3).textContent).toBe("First Bob edit");
      assertSurvivors();
      await user.click(screen.getByRole("button", { name: "Redo" }));
    } else {
      await user.click(screen.getByRole("button", { name: "Redo" }));
      expect(getBodyCell("name", 3).textContent).toBe("Second Bob edit");
      assertSurvivors();
      await user.click(screen.getByRole("button", { name: "Undo" }));
    }
    expect(getBodyCell("name", 3).textContent).toBe(currentEdit);
    assertSurvivors();
    if (outcomeUnknown) {
      expect(
        screen.getByText(/first refresh may precede a late write/),
      ).toBeTruthy();
    }
    const fetchCount = postedMessagesOfType("fetchPage").length;
    await act(async () => dispatchRawIncomingMessage("deleteResult", result));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(fetchCount);
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getRawLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 2 },
            changes: { id: "3", name: currentEdit },
            originalValues: { id: 2, name: "Bob" },
          },
        ],
        insertValues: [{ name: "History survivor draft" }],
      },
    });
  });

  it.each([
    false,
    true,
  ])("clears unresolved DELETE history during partial APPLY recovery without resurrecting confirmed inserts (unknown=%s)", async (outcomeUnknown) => {
    const user = userEvent.setup();
    const carol = { id: 3, name: "Carol" };
    await initializeCommittedTableData({ dataRows: [...rows, carol] });
    const editName = async (index: number, value: string) => {
      await user.dblClick(getBodyCell("name", index));
      fireEvent.change(screen.getByLabelText("Cell data"), {
        target: { value },
      });
      await user.click(screen.getByRole("button", { name: "Apply" }));
    };
    await editName(1, "Historical Bob edit");
    await editName(1, "Bob"); // Only history now contains a B edit.
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    fireEvent.doubleClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "Confirmed draft" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    await editName(3, "Pending Carol");
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    await act(async () =>
      dispatchIncomingMessage("deleteResult", {
        success: true,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: false,
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: { id: 1 },
            status: "deleted",
            success: true,
          },
        ],
      }),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [carol], // Current C resolves; historical B does not.
        totalCount: 1,
      }),
    );
    expect(screen.queryByText(/Unable to match/)).toBeNull();
    expect(getBodyCell("name").textContent).toContain("Confirmed draft");
    expect(getBodyCell("name", 1).textContent).toContain("Pending Carol");
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          { primaryKeys: { id: 3 }, changes: { name: "Pending Carol" } },
        ],
        insertValues: [{ name: "Confirmed draft" }],
      },
    });
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    const result = {
      operationId,
      success: false,
      error: "C update was not applied",
      insertApplied: true,
      changesPossible: true,
      outcomeUnknown,
      rowOutcomes: [{ rowIndex: 0, status: "not_applied", success: false }],
    };
    await act(async () => dispatchRawIncomingMessage("applyResult", result));
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [rows[1], carol, { id: 4, name: "Confirmed draft" }],
        totalCount: 3,
      }),
    );
    const fetchCount = postedMessagesOfType("fetchPage").length;
    await act(async () => dispatchRawIncomingMessage("applyResult", result));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(fetchCount);
    fireEvent.keyDown(document.body, {
      code: "KeyZ",
      key: "z",
      ctrlKey: true,
      metaKey: true,
    });
    expect(getBodyCell("name").dataset.row).toBe("0");
    expect(getBodyCell("name", 1).textContent).toBe("Pending Carol");
    expect(screen.getAllByText("Confirmed draft")).toHaveLength(1);
    fireEvent.keyDown(document.body, {
      code: "KeyZ",
      key: "z",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(getBodyCell("name").dataset.row).toBe("0"); // No draft resurrection.
    expect(getBodyCell("name").textContent).toBe("Bob");
    expect(getBodyCell("name", 1).textContent).toBe("Pending Carol");
    expect(screen.getAllByText("Confirmed draft")).toHaveLength(1);
    if (outcomeUnknown) {
      expect(
        screen.getByText(/first refresh may precede a late write/),
      ).toBeTruthy();
    }
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    const retry = getRawLastPostedMessage();
    expect(retry).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 3 },
            changes: { name: "Pending Carol" },
            originalValues: { name: "Carol" },
          },
        ],
      },
    });
    expect(retry?.payload).not.toHaveProperty("insertValues");
  });

  it.each([
    "unknown",
    "missing",
    "notfound",
    "failed",
    "skipped",
  ] as const)("retains %s delete work and drafts while dropping only confirmed identities", async (status) => {
    const dataRows = [...rows, { id: 3, name: "Carol" }];
    const user = await stageDirtyDeleteWork(dataRows);
    // Request order differs from page order: rowIndex 1 is Alice, not Bob.
    await user.click(screen.getByLabelText("Select row 2"));
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByLabelText("Select row 3"));
    await user.click(screen.getByRole("button", { name: "Delete (3)" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "deleteRows",
      payload: { primaryKeysList: [{ id: 2 }, { id: 1 }, { id: 3 }] },
    });
    await act(async () =>
      dispatchIncomingMessage("deleteResult", {
        success: false,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [
          {
            rowIndex: 1,
            primaryKeys: { id: 1 },
            status: "deleted",
            success: true,
          },
          ...(status === "missing"
            ? []
            : [
                { rowIndex: 0, primaryKeys: { id: 2 }, status, success: false },
              ]),
          // Carol has no outcome and must also survive.
        ],
      }),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [dataRows[2]], // Bob remains unresolved, not discarded.
        totalCount: 1,
      }),
    );
    expect(getBodyCell("name").textContent).toContain("Delete survivor draft");
    expect(getBodyCell("name", 1).textContent).toContain("Pending Carol");
    expect(
      screen.getByText(/Unable to match 1 row of unverified changes/),
    ).toBeTruthy();
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [dataRows[2], dataRows[1]],
        totalCount: 2,
      }),
    );
    expect(getBodyCell("name", 2).textContent).toContain("Pending Bob");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getRawLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          { primaryKeys: { id: 3 }, changes: { name: "Pending Carol" } },
          { primaryKeys: { id: 2 }, changes: { name: "Pending Bob" } },
        ],
        insertValues: [{ name: "Delete survivor draft" }],
      },
    });
  });

  it.each([
    false,
    true,
  ])("uses the delete request identity snapshot after live rows and selection change (target missing=%s)", (targetMissing) => {
    const rowsRef = { current: [...rows] };
    const fetchPage = vi.fn();
    const { result, rerender } = renderHook(
      ({ selected }) =>
        useTableMutationController({
          canEditRows: true,
          loadingRef: { current: false },
          columnsRef: { current: columns },
          fetchPageRef: { current: fetchPage },
          pkColsRef: { current: ["id"] },
          preserveScrollPositionRef: { current: vi.fn() },
          rowsRef,
          mongoIdTypesRef: { current: [] },
          selected,
        }),
      { initialProps: { selected: new Set([0]) } },
    );
    act(() =>
      result.current.commitCellEdit(0, columns[1], "Pending Alice", "Alice"),
    );
    act(() =>
      result.current.commitCellEdit(1, columns[1], "Pending Bob", "Bob"),
    );
    // A proposed/colliding PK must not make Bob the confirmed delete target.
    act(() => result.current.commitCellEdit(0, columns[0], "2", 1));
    act(() => result.current.deleteSelected());
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    act(() => {
      const nextRows = targetMissing ? [rows[1]] : [rows[1], rows[0]];
      result.current.handleRowsCommitted(nextRows, ["id"], [], []);
      rowsRef.current = nextRows;
    });
    rerender({ selected: new Set([0]) }); // Both index 0 and selection now mean Bob.
    expect(result.current.unresolvedPendingEdits).toHaveLength(
      targetMissing ? 1 : 0,
    );
    act(() =>
      dispatchRawIncomingMessage("deleteResult", {
        operationId,
        success: true,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: false,
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: { id: 1 },
            status: "deleted",
            success: true,
          },
        ],
      }),
    );
    expect(result.current.pendingEdits).toEqual(
      new Map([[0, new Map([["name", "Pending Bob"]])]]),
    );
    expect(fetchPage).toHaveBeenCalledTimes(1);
    act(() => result.current.handleRowsCommitted([rows[1]], ["id"], [], []));
    expect(result.current.unresolvedPendingEdits).toEqual([]);
    expect(result.current.pendingEdits.get(0)?.get("name")).toBe("Pending Bob");
  });

  it("prunes only the confirmed Mongo identity when ObjectId and string display values coincide", () => {
    const id = "507f1f77bcf86cd799439011";
    const rowsRef = {
      current: [
        { _id: id, name: "ObjectId row" },
        { _id: id, name: "String row" },
      ],
    };
    const mongoIdTypesRef = {
      current: ["objectId", "string"] as Array<"objectId" | "string" | null>,
    };
    const { result } = renderHook(() =>
      useTableMutationController({
        mongoRowIdentity: true,
        canEditRows: true,
        loadingRef: { current: false },
        columnsRef: { current: columns },
        fetchPageRef: { current: vi.fn() },
        pkColsRef: { current: ["_id"] },
        preserveScrollPositionRef: { current: vi.fn() },
        rowsRef,
        mongoIdTypesRef,
        selected: new Set([0]),
      }),
    );
    act(() =>
      result.current.commitCellEdit(
        0,
        columns[1],
        "Deleted edit",
        "ObjectId row",
      ),
    );
    act(() =>
      result.current.commitCellEdit(1, columns[1], "Kept edit", "String row"),
    );
    act(() => result.current.deleteSelected());
    act(() =>
      dispatchIncomingMessage("deleteResult", {
        success: true,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: false,
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: {
              _id: { $rapidbMongoId: { type: "objectId", value: id } },
            },
            status: "deleted",
            success: true,
          },
        ],
      }),
    );
    expect(result.current.pendingEdits).toEqual(
      new Map([[1, new Map([["name", "Kept edit"]])]]),
    );
    act(() => {
      result.current.handleRowsCommitted(
        [rowsRef.current[1]],
        ["_id"],
        mongoIdTypesRef.current,
        ["string"],
      );
      rowsRef.current = [rowsRef.current[1]];
      mongoIdTypesRef.current = ["string"];
    });
    expect(result.current.unresolvedPendingEdits).toEqual([]);
    act(() => result.current.undoAction());
    expect(result.current.pendingEdits.size).toBe(0);
    act(() => result.current.redoAction());
    expect(result.current.pendingEdits).toEqual(
      new Map([[0, new Map([["name", "Kept edit"]])]]),
    );
  });

  it.each([
    false,
    true,
  ])("reconciles partial deletes (unknown=%s), preserving edits and identity-safe undo/redo", async (outcomeUnknown) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name", 1));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Bob" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    const firstId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    clearPostedMessages();
    await act(async () =>
      dispatchRawIncomingMessage("deleteResult", {
        operationId: firstId,
        success: false,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown,
        error:
          "Delete partially changed the data: 1 row confirmed deleted. Refresh and verify before retrying.",
        rowOutcomes: [
          {
            rowIndex: 0,
            primaryKeys: { id: 1 },
            status: "deleted",
            success: true,
          },
        ],
      }),
    );
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expect(screen.getByText(/partially changed/)).toBeTruthy();
    if (outcomeUnknown)
      expect(
        screen.getByText(/first refresh may precede a late write/),
      ).toBeTruthy();
    // Mutations and undo are fenced while the authoritative page is pending.
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
    });
    expect(getBodyCell("name", 1).textContent).toContain("Pending Bob");
    await act(async () =>
      dispatchRawIncomingMessage("tableError", {
        fetchId: lastFetchPayload().fetchId,
        error: "Refresh connection lost",
      }),
    );
    expect(screen.getByText("Refresh connection lost")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    expect(postedMessagesOfType("deleteRows")).toHaveLength(0);
    expect(screen.getByText(/delete refresh failed/)).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(2);
    await act(async () =>
      dispatchRawIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [rows[1]],
        totalCount: 1,
      }),
    );
    expect(getBodyCell("name").textContent).toContain("Pending Bob");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name").textContent).toContain("Bob");
    expect(getBodyCell("name").textContent).not.toContain("Pending Bob");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    expect(getBodyCell("name").textContent).toContain("Pending Bob");
    // A deliberate retry gets a fresh ID and targets the newly committed identity.
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    const retry = getRawLastPostedMessage()?.payload as {
      operationId: string;
      primaryKeysList: unknown[];
    };
    expect(retry.operationId).not.toBe(firstId);
    expect(retry.primaryKeysList).toEqual([{ id: 2 }]);
    const fetchCount = postedMessagesOfType("fetchPage").length;
    await act(async () =>
      dispatchRawIncomingMessage("deleteResult", {
        operationId: firstId,
        success: true,
        affectedRows: 1,
        changesPossible: true,
      }),
    );
    expect(postedMessagesOfType("fetchPage")).toHaveLength(fetchCount);
    expect(screen.getByRole("button", { name: "Deleting…" })).toBeTruthy();
    await act(async () =>
      dispatchRawIncomingMessage("deleteResult", {
        operationId: retry.operationId,
        success: false,
        affectedRows: 0,
        changesPossible: false,
        outcomeUnknown: false,
        error: "Rejected before mutation",
      }),
    );
    expect(screen.getByText("Rejected before mutation")).toBeTruthy();
    expect(postedMessagesOfType("fetchPage")).toHaveLength(fetchCount);
  });

  it("preserves drafts and redo-only history through delete reconciliation", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    await user.dblClick(getBodyCell("name", 2));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Redo Bob" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    await act(async () =>
      dispatchIncomingMessage("deleteResult", {
        success: true,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: false,
      }),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [rows[1]],
        totalCount: 1,
      }),
    );
    expect(Number(getBodyCell("name").dataset.row)).toBeLessThan(0);
    expect(getBodyCell("name", 1).textContent).toContain("Bob");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    expect(getBodyCell("name", 1).textContent).toContain("Redo Bob");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name").textContent).toContain("Bob");
    expect(Number(getBodyCell("name").dataset.row)).toBe(0);
  });

  it("dispatches table export messages directly when all rows are visible", async () => {
    const user = userEvent.setup();

    await initializeCommittedTableData();
    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Export CSV" }));
    await user.click(screen.getByRole("button", { name: "Export JSON" }));

    expect(getPostedMessages()).toEqual([
      {
        type: "exportCSV",
        payload: { sort: null, filters: [], columnOrder: ["id", "name"] },
      },
      {
        type: "exportJSON",
        payload: { sort: null, filters: [], columnOrder: ["id", "name"] },
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
          columnOrder: ["id", "name"],
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

  it.each([
    "page",
    "sort",
    "filter",
    "refetch",
  ])("does not replay an undone edit against another PK after a %s commit", async (transition) => {
    const user = userEvent.setup();
    await initializeCommittedTableData({ totalCount: 51 });

    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Alice edited" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name").textContent).toBe("Alice");

    clearPostedMessages();
    if (transition === "page") {
      fireEvent.click(screen.getByRole("button", { name: "Next →" }));
    } else if (transition === "sort") {
      fireEvent.click(screen.getByText("id", { exact: true }));
    } else if (transition === "filter") {
      fireEvent.change(screen.getByLabelText("name filter value"), {
        target: { value: "Carol" },
      });
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, DEBOUNCE + 50));
      });
    } else {
      fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    }
    await waitFor(() => {
      expect(getLastPostedMessage()?.type).toBe("fetchPage");
    });
    const fetch = lastFetchPayload();
    if (transition === "page") expect(fetch.page).toBe(2);
    if (transition === "sort") {
      expect(fetch.sort).toEqual({ column: "id", direction: "asc" });
    }
    if (transition === "filter") {
      expect(fetch.filters).toEqual([
        { column: "name", operator: "like", value: "Carol" },
      ]);
    }
    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: fetch.fetchId,
        rows: [{ id: 26, name: "Carol" }],
        totalCount: 51,
      });
      // Exercise the keyboard handler before the commit's React rerender too.
      fireEvent.keyDown(document.body, {
        key: "z",
        code: "KeyZ",
        ctrlKey: true,
        metaKey: true,
        shiftKey: true,
      });
    });
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(getBodyCell("name").textContent).toBe("Carol");
    expect(screen.queryByText(/unsaved changes/)).toBeNull();
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);

    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Carol edited" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: /Apply Changes/ }));
    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 26 },
            changes: { name: "Carol edited" },
            originalValues: { name: "Carol" },
          },
        ],
      },
    });
  });

  it("clears undo snapshots while retaining pending edits by PK on a rows commit", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Alice edited" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));

    await act(async () => {
      dispatchIncomingMessage("tableData", {
        rows: [rows[1], rows[0]],
        totalCount: 2,
      });
    });
    expect(
      (screen.getByRole("button", { name: "Undo" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "Redo" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
    });
    expect(getBodyCell("name", 0).textContent).toBe("Bob");
    expect(getBodyCell("name", 1).textContent).toBe("Alice edited");
    await user.click(screen.getByRole("button", { name: /Apply Changes/ }));
    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Alice edited" },
            originalValues: { name: "Alice" },
          },
        ],
      },
    });
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

  it("preserves a reopened NULL cell when editing is untouched", async () => {
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

    expect(screen.getByText("NULL")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
  });

  it.each([
    "during",
    "after",
    "same-turn",
    "read-error",
  ])("rejects clipboard replies %s a partial-apply refetch", async (timing) => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Retained" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.dblClick(getBodyCell("name", 1));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Saved Bob" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    const cell = getBodyCell("name");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    const reply = () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "Stale paste",
      });
    await act(async () => {
      dispatchIncomingMessage("applyResult", {
        success: true,
        rowOutcomes: [
          { rowIndex: 0, success: false, status: "verification_failed" },
          { rowIndex: 1, success: true, status: "applied" },
        ],
      });
      if (timing === "same-turn") reply();
      fireEvent.paste(window);
      fireEvent.keyDown(document.body, {
        key: "z",
        code: "KeyZ",
        ctrlKey: true,
        metaKey: true,
      });
    });
    expect(postedMessagesOfType("readClipboard")).toHaveLength(1);
    expect(getBodyCell("name").textContent).toBe("Retained");
    if (timing === "during") await act(async () => reply());
    fireEvent.paste(window);
    expect(postedMessagesOfType("readClipboard")).toHaveLength(1);
    if (timing === "read-error") {
      await act(async () =>
        dispatchIncomingMessage("tableError", {
          fetchId: lastFetchPayload().fetchId,
          error: "Read failed",
        }),
      );
      await act(async () => reply());
      expect(getBodyCell("name").textContent).toBe("Retained");
      await user.dblClick(getBodyCell("name"));
      fireEvent.change(screen.getByLabelText("Cell data"), {
        target: { value: "Newer edit" },
      });
      await user.click(screen.getByRole("button", { name: "Apply" }));
    }
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ id: 2, name: "Saved Bob" }, rows[0]],
        totalCount: 2,
      }),
    );
    if (timing === "after") await act(async () => reply());
    expect(getBodyCell("name", 0).textContent).toBe("Saved Bob");
    const retainedValue = timing === "read-error" ? "Newer edit" : "Retained";
    expect(getBodyCell("name", 1).textContent).toBe(retainedValue);
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: retainedValue },
            originalValues: { name: "Alice" },
          },
        ],
      },
    });
  });

  it("blocks undo and redo during refetch and preserves history if the read fails", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.dblClick(getBodyCell("name"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Edited" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
      fireEvent.keyDown(document.body, {
        key: "z",
        code: "KeyZ",
        ctrlKey: true,
        metaKey: true,
        shiftKey: true,
      });
    });
    expect(getBodyCell("name").textContent).toBe("Alice");
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(getBodyCell("name").textContent).toBe("Alice");
    await act(async () =>
      dispatchIncomingMessage("tableError", {
        fetchId: lastFetchPayload().fetchId,
        error: "Read failed",
      }),
    );
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(getBodyCell("name").textContent).toBe("Edited");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name").textContent).toBe("Alice");
  });

  it("clears cell selection when refreshed rows replace the dataset", async () => {
    await initializeCommittedTableData();
    const selectedCell = getBodyCell("name", 0);
    fireEvent.mouseDown(selectedCell, { button: 0 });
    fireEvent.mouseUp(selectedCell);
    expect(selectedCell.className).toContain("rdb-tcell-selected");

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    const refresh = lastFetchPayload();
    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: refresh.fetchId,
        rows: [
          { id: 3, name: "Carol" },
          { id: 4, name: "Dave" },
        ],
        totalCount: 2,
      });
    });

    const replacementCell = getBodyCell("name", 0);
    expect(replacementCell.textContent).toBe("Carol");
    expect(replacementCell.className).not.toContain("rdb-tcell-selected");
  });

  it.each([
    "context",
    "keyboard",
  ])("uses the correct paste target inside a selection (%s)", async (source) => {
    const user = userEvent.setup();
    renderTableView();
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    });
    await waitFor(() => expect(getLastPostedMessage()?.type).toBe("fetchPage"));
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    const first = screen.getByText("Alice").closest("td");
    const second = screen.getByText("Bob").closest("td");
    if (!first || !second) throw new Error("Expected table cells");
    fireEvent.mouseDown(first, { button: 0 });
    fireEvent.mouseUp(first);
    fireEvent.mouseDown(second, { button: 0, shiftKey: true });
    fireEvent.mouseUp(second);
    fireEvent.mouseDown(second, { button: 2 });
    if (source === "context") {
      fireEvent.contextMenu(second);
      await user.click(screen.getByRole("menuitem", { name: "Paste" }));
    } else {
      fireEvent.paste(window);
    }
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "Pasted",
      }),
    );
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: source === "context" ? 2 : 1 },
            changes: { name: "Pasted" },
            originalValues: { name: source === "context" ? "Bob" : "Alice" },
          },
        ],
      },
    });
  });

  it("copies and pastes a multiline cell as one cell", async () => {
    const user = userEvent.setup();
    const multilineText = 'first line\nsecond\tline with "quotes"';

    await initializeCommittedTableData({
      dataRows: [
        { id: 1, name: multilineText },
        { id: 2, name: "Target" },
      ],
    });

    const sourceCell = screen.getByText(/first line.*second/).closest("td");
    if (!sourceCell) throw new Error("Expected multiline source cell");

    fireEvent.mouseDown(sourceCell, { button: 0 });
    fireEvent.mouseUp(sourceCell);
    fireEvent.contextMenu(sourceCell);
    await user.click(screen.getByRole("menuitem", { name: "Copy" }));

    const copyMessage = getLastPostedMessage();
    expect(copyMessage).toEqual({
      type: "writeClipboard",
      payload: {
        text: '"first line\nsecond\tline with ""quotes"""',
      },
    });
    const clipboardText = (copyMessage?.payload as { text: string }).text;

    const targetCell = screen.getByText("Target").closest("td");
    if (!targetCell) throw new Error("Expected paste target cell");
    fireEvent.mouseDown(targetCell, { button: 0 });
    fireEvent.mouseUp(targetCell);
    clearPostedMessages();
    fireEvent.paste(window);

    const pasteRequest = getLastPostedMessage();
    expect(pasteRequest?.type).toBe("readClipboard");
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(pasteRequest?.payload as object),
        text: clipboardText,
      }),
    );

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 2 },
            changes: { name: multilineText },
            originalValues: { name: "Target" },
          },
        ],
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

    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Alicia" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(screen.getByText(/1 row with unsaved changes/)).toBeTruthy();
    expect(screen.getByText("Alicia")).toBeTruthy();

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Alicia" },
            originalValues: { name: "Alice" },
          },
        ],
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

    dispatchRawIncomingMessage("applyResult", {
      operationId: "table-mutation:stale",
      success: true,
    });
    expect(getRawLastPostedMessage()).toBeUndefined();

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

    const editInput = screen.getByLabelText("Cell data");
    expect((editInput as HTMLTextAreaElement).readOnly).toBe(true);
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

  it("keeps generated columns read-only in persisted rows and insert drafts", async () => {
    const user = userEvent.setup();
    renderTableView();
    const generatedColumns: ColumnTypeMeta[] = [
      ...noPkColumns,
      {
        name: "name_lower",
        type: "TEXT",
        nativeType: "TEXT",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
        isComputed: true,
        generatedKind: "stored",
        computedExpression: "lower(name)",
        category: "text",
        filterable: true,
        filterOperators: ["eq", "like"],
        valueSemantics: "plain",
      },
    ];
    dispatchIncomingMessage("tableInit", {
      columns: generatedColumns,
      primaryKeyColumns: [],
    });

    await waitFor(() => {
      expect(getLastPostedMessage()).toEqual({
        type: "fetchPage",
        payload: expect.objectContaining({ page: 1, pageSize: 25 }),
      });
    });
    const initialFetch = lastFetchPayload();
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: initialFetch.fetchId,
        rows: [{ code: "A-1", name: "Alice", name_lower: "alice" }],
        totalCount: 1,
      }),
    );

    fireEvent.doubleClick(getBodyCell("name_lower"));
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).readOnly,
    ).toBe(true);
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe("alice");
    await user.click(screen.getByRole("button", { name: "Apply" }));

    await user.click(screen.getByRole("button", { name: "Add Row" }));
    expect(getBodyCell("name_lower", 0).textContent).toContain("DEFAULT");
    fireEvent.doubleClick(getBodyCell("name_lower", 0));
    expect(screen.queryByLabelText("Cell data")).toBeNull();
  });

  describe("R8 generated full-value inspection", () => {
    const longText = `${"Résumé 😀\ncomputed text\t".repeat(160)}END of generated text`;
    const rawJson = `{"counter":9007199254740993,"text":${JSON.stringify(longText)}}`;
    const formattedJson = `{\n  "counter": 9007199254740993,\n  "text": ${JSON.stringify(longText)}\n}`;
    const generatedColumns: ColumnTypeMeta[] = [
      ...columns,
      {
        ...structuredColumns[1],
        name: "computed_json",
        isComputed: true,
        computedExpression: "json_object('text', name)",
      },
      {
        ...columns[1],
        name: "generated_text",
        nullable: true,
        generatedKind: "stored",
      },
    ];
    const dataRows = [
      {
        id: 1,
        name: "Alice",
        computed_json: rawJson,
        generated_text: longText,
      },
    ];
    const cases = [
      { column: "computed_json", language: "json", value: formattedJson },
      { column: "generated_text", language: "plaintext", value: longText },
    ];

    for (const mode of [
      "writable",
      "readonly-view",
      "readonly-connection",
      "no-pk",
    ]) {
      for (const gesture of ["doubleClick", "F2"]) {
        it.each(
          cases,
        )(`opens full $column read-only via ${gesture} in ${mode}`, async ({
          column,
          language,
          value,
        }) => {
          const user = userEvent.setup();
          await initializeCommittedTableData({
            columnDefs: generatedColumns,
            primaryKeyColumns: mode === "no-pk" ? [] : ["id"],
            dataRows,
            renderOverrides: {
              isView: mode === "readonly-view",
              connectionReadOnly: mode === "readonly-connection",
            },
          });
          const cell = getBodyCell(column);
          expect(cell.textContent?.length).toBeLessThanOrEqual(
            CELL_PREVIEW_LIMIT,
          );
          expect(cell.textContent).not.toContain("END of generated text");
          clearPostedMessages();
          if (gesture === "F2") {
            await user.click(cell);
            await user.keyboard("{F2}");
          } else {
            await user.dblClick(cell);
          }

          const dialog = screen.getByRole("dialog", {
            name: `Cell data: ${column}`,
          });
          const editor = within(dialog).getByLabelText(
            "Cell data",
          ) as HTMLTextAreaElement;
          expect(editor.readOnly).toBe(true);
          expect(editor.value).toBe(value);
          expect(
            within(dialog).getByTestId("monaco-language").textContent,
          ).toBe(language);
          expect(
            within(dialog).queryByRole("button", { name: "NULL" }),
          ).toBeNull();
          expect(
            within(dialog).queryByRole("button", { name: "DEF" }),
          ).toBeNull();
          expect(screen.queryByLabelText("Cell value")).toBeNull();
          // Even an unexpected editor callback cannot stage a generated edit.
          fireEvent.change(editor, { target: { value: "forbidden mutation" } });
          await user.click(
            within(dialog).getByRole("button", { name: "Apply" }),
          );
          expect(screen.queryByRole("dialog")).toBeNull();
          expect(
            screen.queryByRole("button", { name: "Apply Changes" }),
          ).toBeNull();
          expect(screen.queryByText(/unsaved changes/)).toBeNull();
          expect(getPostedMessages()).toEqual([]);

          await user.dblClick(getBodyCell(column));
          expect(
            (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
          ).toBe(value);
          await user.click(
            screen.getByRole("button", { name: "Close cell data" }),
          );
          expect(getPostedMessages()).toEqual([]);

          await user.click(getBodyCell(column));
          fireEvent.paste(window);
          if (mode === "writable") {
            const request = getLastPostedMessage();
            expect(request?.type).toBe("readClipboard");
            await act(async () =>
              dispatchIncomingMessage("clipboardText", {
                ...(request?.payload as object),
                text: "forbidden mutation",
              }),
            );
            expect(screen.getByText(/Paste failed/)).toBeTruthy();
          } else {
            expect(getPostedMessages()).toEqual([]);
          }
          expect(
            screen.queryByRole("button", { name: "Apply Changes" }),
          ).toBeNull();
          expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
          expect(postedMessagesOfType("insertRow")).toHaveLength(0);
          expect(postedMessagesOfType("deleteRows")).toHaveLength(0);
        });
      }
    }

    it.each(
      cases,
    )("blocks $column draft opening and paste, while inspection never enters the insert payload", async ({
      column,
    }) => {
      const user = userEvent.setup();
      await initializeCommittedTableData({
        columnDefs: generatedColumns,
        dataRows,
      });
      await user.dblClick(getBodyCell(column));
      await user.click(screen.getByRole("button", { name: "Apply" }));
      await user.click(screen.getByRole("button", { name: "Add Row" }));
      const cell = getBodyCell(column);
      expect(Number(cell.dataset.row)).toBeLessThan(0);
      expect(cell.textContent).toBe("DEFAULT");
      await user.dblClick(cell);
      await user.click(cell);
      await user.keyboard("{F2}");
      expect(screen.queryByRole("dialog")).toBeNull();
      expect(screen.queryByLabelText("Cell value")).toBeNull();

      clearPostedMessages();
      fireEvent.paste(window);
      const request = getLastPostedMessage();
      expect(request?.type).toBe("readClipboard");
      await act(async () =>
        dispatchIncomingMessage("clipboardText", {
          ...(request?.payload as object),
          text: "forbidden mutation",
        }),
      );
      expect(screen.getByText(/Paste failed/)).toBeTruthy();
      expect(getBodyCell(column).textContent).toBe("DEFAULT");
      expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
      await user.click(screen.getByRole("button", { name: "Apply Changes" }));
      expect(getLastPostedMessage()).toMatchObject({
        type: "applyChanges",
        payload: { updates: [], insertValues: [{}] },
      });
    });

    it.each([
      false,
      true,
    ])("keeps scalar generated inspection read-only (readonly connection: %s)", async (connectionReadOnly) => {
      const user = userEvent.setup();
      await initializeCommittedTableData({
        columnDefs: [
          ...columns,
          {
            ...columns[0],
            name: "computed_number",
            isPrimaryKey: false,
            isComputed: true,
            nullable: true,
          },
        ],
        dataRows: [{ id: 1, name: "Alice", computed_number: 1234 }],
        renderOverrides: { connectionReadOnly },
      });
      clearPostedMessages();
      await user.dblClick(getBodyCell("computed_number"));
      const editor = screen.getByLabelText("Cell value") as HTMLInputElement;
      expect(editor.readOnly).toBe(true);
      expect(editor.value).toBe("1234");
      expect(screen.queryByRole("button", { name: "NULL" })).toBeNull();
      fireEvent.change(editor, { target: { value: "9999" } });
      await user.keyboard("{Enter}");
      expect(getBodyCell("computed_number").textContent).toBe("1234");
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
      expect(getPostedMessages()).toEqual([]);
    });
  });

  it("keeps explicitly supplied identity values pasteable into insert drafts", async () => {
    const user = userEvent.setup();
    const identityColumns = columns.map((column) =>
      column.name === "id"
        ? { ...column, identityGeneration: "by_default" as const }
        : column,
    );
    await initializeCommittedTableData({ columnDefs: identityColumns });
    await user.click(screen.getByRole("button", { name: "Add Row" }));

    const cell = getBodyCell("id");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    clearPostedMessages();
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "17",
      }),
    );

    expect(screen.queryByText(/Paste failed/)).toBeNull();
    expect(getBodyCell("id").textContent).toContain("17");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: { insertValues: [{ id: "17" }] },
    });
  });

  it.each([
    "persisted",
    "draft",
  ] as const)("rejects paste into a virtual generated column in %s rows without staging it", async (rowKind) => {
    const user = userEvent.setup();
    const generatedColumn: ColumnTypeMeta = {
      name: "name_lower",
      type: "TEXT",
      nativeType: "TEXT",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      isComputed: true,
      computedExpression: "lower(name)",
      generatedKind: "virtual",
      isPersisted: false,
      category: "text",
      filterable: true,
      filterOperators: ["eq", "like"],
      valueSemantics: "plain",
    };
    await initializeCommittedTableData({
      columnDefs: [...columns, generatedColumn],
      dataRows: [{ id: 1, name: "Alice", name_lower: "alice" }],
    });
    if (rowKind === "draft") {
      await user.click(screen.getByRole("button", { name: "Add Row" }));
    }

    const cell = getBodyCell("name_lower");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    clearPostedMessages();
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");

    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "overwritten",
      }),
    );

    expect(screen.getByText(/Paste failed/)).toBeTruthy();
    expect(getBodyCell("name_lower").textContent).toContain(
      rowKind === "draft" ? "DEFAULT" : "alice",
    );
    if (rowKind === "persisted") {
      expect(
        screen.queryByRole("button", { name: "Apply Changes" }),
      ).toBeNull();
    } else {
      await user.click(screen.getByRole("button", { name: "Apply Changes" }));
      expect(getLastPostedMessage()).toMatchObject({
        type: "applyChanges",
        payload: { insertValues: [{}] },
      });
    }
  });

  it("continues to stage ordinary editable paste alongside generated columns", async () => {
    const user = userEvent.setup();
    const generatedColumn: ColumnTypeMeta = {
      name: "name_lower",
      type: "TEXT",
      nativeType: "TEXT",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      isComputed: true,
      computedExpression: "lower(name)",
      generatedKind: "virtual",
      isPersisted: false,
      category: "text",
      filterable: true,
      filterOperators: ["eq", "like"],
      valueSemantics: "plain",
    };
    await initializeCommittedTableData({
      columnDefs: [...columns, generatedColumn],
      dataRows: [{ id: 1, name: "Alice", name_lower: "alice" }],
    });

    const cell = getBodyCell("name");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(cell);
    clearPostedMessages();
    fireEvent.paste(window);
    const request = getLastPostedMessage();
    await act(async () =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: "Pasted",
      }),
    );

    expect(screen.queryByText(/Paste failed/)).toBeNull();
    expect(getBodyCell("name").textContent).toContain("Pasted");
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Pasted" } }],
      },
    });
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

    const editInput = screen.getByLabelText("Cell data");
    expect((editInput as HTMLTextAreaElement).readOnly).toBe(true);
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
            originalValues: {
              payload: '{"name":"Alice","meta":{"active":true}}',
            },
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

  it("opens json-looking text columns as plaintext", async () => {
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

    expect(screen.getByRole("dialog")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("plaintext");
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe('{"name":"Alice"}');
  });

  it.each([
    "text",
    "lob",
  ] as const)("opens persisted and draft %s cells with F2 using current values", async (category) => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      columnDefs: [columns[0], { ...columns[1], category }],
    });

    await user.click(getBodyCell("name"));
    await user.keyboard("{F2}");
    expect(screen.getByTestId("monaco-language").textContent).toBe("plaintext");
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe("Alice");
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "pending\ntext" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(getBodyCell("name"));
    await user.keyboard("{F2}");
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe("pending\ntext");
    fireEvent.keyDown(screen.getByLabelText("Cell data"), { key: "Escape" });

    await user.click(screen.getByRole("button", { name: "Add Row" }));
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    const draftCell = getBodyCell("name", 1);
    expect(Number(draftCell.dataset.row)).toBeLessThan(0);
    await user.click(draftCell);
    await user.keyboard("{F2}");
    // DEFAULT text stays inline until it has a value, just like double click.
    expect(screen.queryByRole("dialog")).toBeNull();
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "draft text" },
    });
    await user.keyboard("{Enter}");
    await user.click(getBodyCell("name", 1));
    await user.keyboard("{F2}");
    expect(screen.getByTestId("monaco-language").textContent).toBe("plaintext");
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe("draft text");
    await user.click(screen.getByRole("button", { name: "NULL" }));
    await user.click(getBodyCell("name", 1));
    await user.keyboard("{F2}");
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.getByLabelText("Cell value")).toBeTruthy();
  });

  it.each([
    "persisted",
    "draft",
  ] as const)("preserves structured, numeric and NULL routing with F2 for %s cells", async (rowKind) => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      columnDefs: [...structuredColumns, columns[1]],
      dataRows: [{ ...structuredRows[0], name: null }],
    });
    if (rowKind === "draft") {
      await user.click(screen.getByRole("button", { name: "Add Row" }));
    }
    for (const [name, language] of [
      ["payload", "json"],
      ["xml_doc", "xml"],
      ["tags", "json"],
    ]) {
      await user.click(getBodyCell(name));
      await user.keyboard("{F2}");
      expect(screen.getByTestId("monaco-language").textContent).toBe(language);
      fireEvent.keyDown(screen.getByLabelText("Cell data"), { key: "Escape" });
    }
    for (const name of ["id", "name"]) {
      await user.click(getBodyCell(name));
      await user.keyboard("{F2}");
      expect(screen.queryByRole("dialog")).toBeNull();
      expect(screen.getByLabelText("Cell value")).toBeTruthy();
      await user.keyboard("{Escape}");
    }
  });

  it("ignores F2 outside the grid and in editable controls, and preserves readonly preview", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      renderOverrides: { connectionReadOnly: true },
    });
    await user.click(getBodyCell("name"));
    fireEvent.keyDown(document.body, { key: "F2" });
    expect(screen.queryByRole("dialog")).toBeNull();
    const filter = screen
      .getByRole("table")
      .querySelector("input:not([type='checkbox'])");
    if (!(filter instanceof HTMLInputElement))
      throw new Error("Expected filter input");
    filter.focus();
    fireEvent.keyDown(filter, { key: "F2" });
    fireEvent.keyDown(screen.getByRole("table"), { key: "F2" });
    expect(screen.queryByRole("dialog")).toBeNull();
    await user.click(getBodyCell("name"));
    await user.keyboard("{F2}");
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).readOnly,
    ).toBe(true);
  });

  it("opens multiline text columns in the plaintext dialog and preserves line breaks", async () => {
    const user = userEvent.setup();
    const multilineText = "first line\r\nsecond line";

    await initializeCommittedTableData({
      dataRows: [{ id: 1, name: multilineText }],
    });

    const textCell = getBodyCell("name");
    expect(textCell.textContent).toBe("first line↵second line");

    await user.dblClick(textCell);

    expect(screen.getByRole("dialog")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("plaintext");
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe(multilineText.replace(/\r\n/g, "\n"));

    const editedText = "\nupdated first line\nupdated second line\n";
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: editedText },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.dblClick(getBodyCell("name"));

    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe(editedText);
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
        insertValues: [
          {
            tags: '[\n  "one",\n  "two"\n]',
          },
        ],
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

  it("marks XML line breaks in table cells while keeping modal formatting behavior", async () => {
    const xmlWithSpacing =
      '<root>\n  <item id="1">Alice</item>\n  <item id="2">Bob</item>\n</root>';
    const expectedSingleLine = xmlWithSpacing.replace(/\r\n|\r|\n/g, "↵");

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

  it("sends MongoDB _id types from the selected rows, not the sampled column type", async () => {
    const user = userEvent.setup();
    renderTableView({ mongoRowIdentity: true });
    const mongoColumns = [
      {
        ...columns[0],
        name: "_id",
        nativeType: "objectId",
        isPrimaryKey: true,
      },
    ];
    dispatchIncomingMessage("tableInit", {
      columns: mongoColumns,
      primaryKeyColumns: ["_id"],
    });
    await waitFor(() =>
      expect(postedMessagesOfType("fetchPage")).toHaveLength(1),
    );
    const fetch = lastFetchPayload();
    const id = "507f1f77bcf86cd799439011";
    await act(async () => {
      dispatchIncomingMessage("tableData", {
        fetchId: fetch.fetchId,
        rows: [{ _id: id }, { _id: id }],
        mongoIdTypes: ["objectId", "string"],
        totalCount: 2,
      });
    });
    await user.click(screen.getByLabelText("Select row 2"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));
    expect(getLastPostedMessage()).toEqual({
      type: "deleteRows",
      payload: {
        primaryKeysList: [
          { _id: { $rapidbMongoId: { type: "string", value: id } } },
        ],
      },
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
    const user = userEvent.setup();
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

    await user.click(screen.getByLabelText("Select row 1"));
    await user.click(screen.getByRole("button", { name: "Delete (1)" }));

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

  it("preserves draft undo and redo across no-op commits without conflating DEFAULT, NULL or empty", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    const draft = () => getBodyCell("id", 0);
    fireEvent.doubleClick(draft());
    await user.click(screen.getByRole("button", { name: "DEF" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(screen.queryByText("DEFAULT")).toBeNull();
    fireEvent.keyDown(document.body, {
      key: "z",
      code: "KeyZ",
      ctrlKey: true,
      metaKey: true,
      shiftKey: true,
    });
    expect(draft().textContent).toContain("DEFAULT");

    fireEvent.doubleClick(draft());
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "42" },
    });
    fireEvent.blur(screen.getByLabelText("Cell value"));
    fireEvent.doubleClick(draft());
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "43" },
    });
    fireEvent.keyDown(screen.getByLabelText("Cell value"), { key: "Enter" });
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(draft().textContent).toBe("42");
    fireEvent.doubleClick(draft());
    fireEvent.blur(screen.getByLabelText("Cell value"));
    expect(
      (screen.getByRole("button", { name: "Redo" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    await user.click(screen.getByRole("button", { name: "Redo" }));
    expect(draft().textContent).toBe("43");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(draft().textContent).toContain("DEFAULT");

    fireEvent.doubleClick(getBodyCell("name", 0));
    await user.click(screen.getByRole("button", { name: "NULL" }));
    expect(getBodyCell("name", 0).textContent).toBe("NULL");
    fireEvent.doubleClick(getBodyCell("name", 0));
    await user.click(screen.getByRole("button", { name: "NULL" }));
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name", 0).textContent).toContain("DEFAULT");
    await user.click(screen.getByRole("button", { name: "Redo" }));
    fireEvent.doubleClick(getBodyCell("name", 0));
    fireEvent.keyDown(screen.getByLabelText("Cell value"), { key: "Enter" });
    expect(getBodyCell("name", 0).textContent).toBe("NULL");
    await user.click(screen.getByRole("button", { name: "Undo" }));
    expect(getBodyCell("name", 0).textContent).toContain("DEFAULT");
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
    ).toBe(false);

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
      payload: { updates: [], insertValues: [{ name: "" }] },
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
      payload: { updates: [], insertValues: [{}] },
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
      payload: { updates: [], insertValues: [{ name: null }] },
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

  it.each([
    "update",
    "insert",
  ] as const)("retains mixed pending edits and draft after %s prevalidation failure", async (failedKind) => {
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
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Alicia" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));

    await user.click(screen.getByRole("button", { name: "Add Row" }));

    expect(screen.getByText(/2 rows with unsaved changes/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Insert Row" })).toBeNull();

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        insertValues: [{}],
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Alicia" },
            originalValues: { name: "Alice" },
          },
        ],
      },
    });
    const request = getLastPostedMessage();
    const error = 'Column "name" cannot persist the requested value exactly.';
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    const result = {
      operationId,
      success: false,
      error,
      insertApplied: false,
      rowOutcomes: [
        {
          rowIndex: 0,
          success: false,
          status: failedKind === "update" ? "prevalidation_failed" : "skipped",
          columns: failedKind === "update" ? ["name"] : undefined,
          message: error,
        },
      ],
      insertRowOutcomes: [
        {
          rowIndex: 0,
          success: false,
          status: failedKind === "insert" ? "prevalidation_failed" : "skipped",
          message: error,
        },
      ],
    };
    await act(async () => {
      dispatchRawIncomingMessage("applyResult", result);
      dispatchRawIncomingMessage("applyResult", result);
    });
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(screen.queryByText(/write outcome is unknown/i)).toBeNull();
    expect(
      screen
        .getByRole("button", { name: "Apply Changes" })
        .hasAttribute("disabled"),
    ).toBe(false);
    expect(screen.getByText("Alicia")).toBeTruthy();
    expect(screen.getAllByText(/^DEFAULT$/).length).toBeGreaterThan(0);
    expect(screen.getByText(error)).toBeTruthy();
    expect(screen.queryByRole("dialog")).toBeNull();
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toEqual(request);
  });

  it("clears a committed draft once on duplicate correlated results and retries only failed updates", async () => {
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
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Alicia" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));

    await user.click(screen.getByRole("button", { name: "Add Row" }));

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        insertValues: [{}],
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Alicia" },
            originalValues: { name: "Alice" },
          },
        ],
      },
    });

    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    const result = {
      operationId,
      success: false,
      error: "Update failed",
      insertApplied: true,
      changesPossible: true,
      outcomeUnknown: false,
      rowOutcomes: [{ rowIndex: 0, success: false, status: "not_applied" }],
      insertRowOutcomes: [{ rowIndex: 0, success: true, status: "applied" }],
    };
    await act(async () => {
      dispatchRawIncomingMessage("applyResult", result);
      dispatchRawIncomingMessage("applyResult", result);
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
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expect(screen.queryByText(/write outcome is unknown/i)).toBeNull();
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [...rows, { id: 3, name: "Inserted default" }],
        totalCount: 3,
      }),
    );
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Alicia" },
            originalValues: { name: "Alice" },
          },
        ],
      },
    });
    const retryOperationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    expect(retryOperationId).not.toBe(operationId);
    await act(async () => dispatchRawIncomingMessage("applyResult", result));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(0);
    expect(screen.getByRole("button", { name: "Applying…" })).toBeTruthy();
  });

  it("reconciles a possibly applied draft insert without claiming it succeeded", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData();
    await user.click(screen.getByRole("button", { name: "Add Row" }));

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()?.type).toBe("applyChanges");

    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Driver lost the acknowledgement after the insert.",
        changesPossible: true,
        outcomeUnknown: true,
      }),
    );

    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expect(screen.getAllByText(/^DEFAULT$/).length).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: "Apply Changes" })).toBeTruthy();
    expect(screen.getByText(/write outcome is unknown/i)).toBeTruthy();
    expect(screen.queryByText(/Insert was applied/)).toBeNull();
  });

  it.each([
    false,
    true,
  ])("keeps unknown DEFAULT insert recovery after the first read, preserving live edits/drafts through a late write (second read fails=%s)", async (secondReadFails) => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      columnDefs: columns.map((column) =>
        column.name === "id"
          ? { ...column, identityGeneration: "auto_increment" }
          : column,
      ),
    });
    await user.dblClick(getBodyCell("name", 0));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Alice" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: { insertValues: [{}] },
    });
    const operationId = (
      getRawLastPostedMessage()?.payload as { operationId: string }
    ).operationId;
    const result = {
      operationId,
      success: false,
      error: "Insert acknowledgement lost.",
      changesPossible: true,
      outcomeUnknown: true,
      rowOutcomes: [{ rowIndex: 0, success: false, status: "unknown" }],
    };
    await act(async () => dispatchRawIncomingMessage("applyResult", result));
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows,
        totalCount: rows.length,
      }),
    );
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();
    expect(screen.getAllByText(/^DEFAULT$/)).toHaveLength(2);
    expect(getBodyCell("name", 1).textContent).toContain("Pending Alice");
    expect(
      screen
        .getByRole("button", { name: "Apply Changes" })
        .hasAttribute("disabled"),
    ).toBe(false);

    // Edits made after the first verification must also survive later reads.
    await user.dblClick(getBodyCell("name", 2));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "Pending Bob" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByLabelText("Select row 2"));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    expect(
      screen.queryByText(/Apply or revert pending changes before loading/),
    ).toBeNull();
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();

    if (secondReadFails) {
      await act(async () =>
        dispatchIncomingMessage("tableError", {
          fetchId: lastFetchPayload().fetchId,
          error: "Second verification failed",
        }),
      );
      expect(
        screen.queryByRole("status", { name: "Loading data..." }),
      ).toBeNull();
      expect(screen.getByText(/verification refresh failed/)).toBeTruthy();
      for (const name of ["Apply Changes", "Add Row", "Delete (1)"]) {
        expect(
          screen.getByRole("button", { name }).hasAttribute("disabled"),
        ).toBe(true);
        await user.click(screen.getByRole("button", { name }));
      }
      fireEvent.doubleClick(getBodyCell("name", 1));
      expect(screen.queryByLabelText("Cell data")).toBeNull();
      fireEvent.keyDown(document.body, {
        key: "z",
        code: "KeyZ",
        ctrlKey: true,
        metaKey: true,
      });
      expect(getBodyCell("name", 2).textContent).toContain("Pending Bob");
      expect(screen.getAllByText(/^DEFAULT$/)).toHaveLength(2);
      expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
      expect(postedMessagesOfType("deleteRows")).toHaveLength(0);
      await user.click(screen.getByRole("button", { name: "Refresh" }));
      expect(postedMessagesOfType("fetchPage")).toHaveLength(2);
    }

    const lateRows = [{ id: 3, name: "Late default insert" }, rows[1], rows[0]];
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: lateRows,
        totalCount: lateRows.length,
      }),
    );
    expect(getBodyCell("name", 1).textContent).toContain("Late default insert");
    expect(getBodyCell("name", 2).textContent).toContain("Pending Bob");
    expect(getBodyCell("name", 3).textContent).toContain("Pending Alice");
    expect(screen.getAllByText(/^DEFAULT$/)).toHaveLength(2);
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    const fetchCount = postedMessagesOfType("fetchPage").length;
    await act(async () => dispatchRawIncomingMessage("applyResult", result));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(fetchCount);

    // Verification never infers that a generated/default draft was applied,
    // and never retries it automatically. Retry remains an explicit user choice.
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(1);
    expect(getLastPostedMessage()).toMatchObject({
      type: "applyChanges",
      payload: {
        insertValues: [{}],
        updates: [
          {
            primaryKeys: { id: 1 },
            changes: { name: "Pending Alice" },
            originalValues: { name: "Alice" },
          },
          {
            primaryKeys: { id: 2 },
            changes: { name: "Pending Bob" },
            originalValues: { name: "Bob" },
          },
        ],
      },
    });
    expect(
      (getRawLastPostedMessage()?.payload as { operationId: string })
        .operationId,
    ).not.toBe(operationId);
  });

  it("keeps unknown APPLY original identity unresolved on a second read with a late proposed-key collision", async () => {
    const user = userEvent.setup();
    await initializeCommittedTableData({
      columnDefs: redisKeyColumns,
      primaryKeyColumns: ["key"],
      dataRows: [{ key: "old", value: "original" }],
    });
    await user.dblClick(getBodyCell("key"));
    fireEvent.change(screen.getByLabelText("Cell data"), {
      target: { value: "new" },
    });
    await user.click(screen.getByRole("button", { name: "Apply" }));
    await user.click(screen.getByRole("button", { name: "Add Row" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Acknowledgement lost",
        changesPossible: true,
        outcomeUnknown: true,
      }),
    );
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ key: "old", value: "original" }],
        totalCount: 1,
      }),
    );
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: [{ key: "new", value: "Other client's row" }],
        totalCount: 1,
      }),
    );
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/);
    expect(getBodyCell("value", 1).textContent).toContain("Other client's row");
    expect(screen.getAllByText(/^DEFAULT$/)).toHaveLength(2);
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
  });

  it("preserves confirmed insert/update evidence during repeated unknown APPLY verification", async () => {
    const user = await stageDirtyDeleteWork();
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    await act(async () =>
      dispatchIncomingMessage("applyResult", {
        success: false,
        error: "Update acknowledgement lost.",
        insertApplied: true,
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [
          { rowIndex: 0, success: true, status: "applied" },
          { rowIndex: 1, success: false, status: "unknown" },
        ],
      }),
    );
    const confirmedRows = [
      { id: 1, name: "Pending Alice" },
      rows[1],
      { id: 3, name: "Delete survivor draft" },
    ];
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: confirmedRows,
        totalCount: 3,
      }),
    );
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(postedMessagesOfType("fetchPage")).toHaveLength(1);
    await act(async () =>
      dispatchIncomingMessage("tableData", {
        fetchId: lastFetchPayload().fetchId,
        rows: confirmedRows,
        totalCount: 3,
      }),
    );
    expect(screen.queryByText(/^DEFAULT$/)).toBeNull();
    expect(screen.getByText(/Insert was applied/)).toBeTruthy();
    expect(
      screen.getByText(/first refresh may precede a late write/),
    ).toBeTruthy();
    expect(postedMessagesOfType("applyChanges")).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Apply Changes" }));
    expect(getLastPostedMessage()).toEqual({
      type: "applyChanges",
      payload: {
        updates: [
          {
            primaryKeys: { id: 2 },
            changes: { name: "Pending Bob" },
            originalValues: { name: "Bob" },
          },
        ],
      },
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
