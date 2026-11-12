import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
} from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableView } from "../../src/webview/components/TableView";
import { EditInput } from "../../src/webview/components/table/EditInput";
import { getStructuredCellDialogValue } from "../../src/webview/components/table/structuredCellDialog";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
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
vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: (props: {
    initialValue?: string;
    ariaLabel?: string;
    onChange?: (value: string) => void;
  }) => (
    <textarea
      aria-label={props.ariaLabel}
      defaultValue={props.initialValue}
      onChange={(event) => props.onChange?.(event.target.value)}
    />
  ),
}));

function makeColumn(
  name: string,
  category: ColumnTypeMeta["category"],
  nativeType: string,
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: name !== "id",
    isPrimaryKey: name === "id",
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
}

const idColumn = makeColumn("id", "integer", "integer");
const qtyColumn = makeColumn("qty", "integer", "integer");
const payloadColumn = makeColumn("payload", "json", "jsonb");

function unsavedStates() {
  return getPostedMessages()
    .filter((message) => message.type === "unsavedChangesState")
    .map((message) => message.payload);
}

function postedOfType(type: string) {
  return getPostedMessages().filter((message) => message.type === type);
}

function renderController(canEditRows = true) {
  const controller = renderHook(() =>
    useTableMutationController({
      canEditRows,
      loadingRef: { current: false },
      columnsRef: { current: [idColumn, payloadColumn] },
      fetchPageRef: { current: vi.fn() },
      pkColsRef: { current: ["id"] },
      preserveScrollPositionRef: { current: vi.fn() },
      rowsRef: { current: [{ id: 1, payload: { a: 1 } }] },
      mongoIdTypesRef: { current: [] },
      rowValueTypesRef: { current: [] },
      selected: new Set<number>(),
    }),
  );
  // These hook tests observe transitions; initial synchronization is covered
  // separately by the real TableView reload regressions below.
  clearPostedMessages();
  return controller;
}

function openPayloadDialog(
  result: ReturnType<typeof renderController>["result"],
  readOnly = false,
) {
  const value = getStructuredCellDialogValue({ a: 1 }, payloadColumn);
  if (!value) throw new Error("Expected structured dialog value");
  act(() =>
    result.current.openStructuredCellDialog({
      rowKind: "persisted",
      rowIdx: 0,
      column: payloadColumn,
      value,
      currentValue: { a: 1 },
      originalValue: { a: 1 },
      readOnly,
    }),
  );
}

describe("DIRTY-EDITORS structured cell dialog unsaved state", () => {
  it("reports a modified editable dialog draft and clears it on revert-to-initial or cancel", () => {
    clearPostedMessages();
    const { result } = renderController();
    openPayloadDialog(result);
    const initialText = result.current.structuredCellDialog?.initialText;
    expect(result.current.structuredCellDialog?.readOnly).toBe(false);
    expect(unsavedStates()).toEqual([]);

    act(() => result.current.updateStructuredCellDialogDraft('{"a": 2}'));
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    act(() =>
      result.current.updateStructuredCellDialogDraft(initialText ?? ""),
    );
    expect(unsavedStates()).toEqual([
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
    ]);

    act(() => result.current.updateStructuredCellDialogDraft('{"a": 3}'));
    act(() => result.current.cancelStructuredCellDialog());
    expect(unsavedStates()).toEqual([
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
    ]);
    expect(result.current.pendingEdits.size).toBe(0);
  });

  it("keeps reporting unsaved state without a false gap when a dirty dialog is applied locally", () => {
    clearPostedMessages();
    const { result } = renderController();
    openPayloadDialog(result);
    act(() => result.current.updateStructuredCellDialogDraft('{"a": 2}'));
    act(() => result.current.confirmStructuredCellDialog());

    expect(result.current.structuredCellDialog).toBeNull();
    expect(result.current.pendingEdits.get(0)?.has("payload")).toBe(true);
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);
    expect(postedOfType("applyChanges")).toEqual([]);
  });

  it("never reports a read-only dialog as unsaved", () => {
    clearPostedMessages();
    const { result } = renderController(false);
    openPayloadDialog(result);
    expect(result.current.structuredCellDialog?.readOnly).toBe(true);
    act(() => result.current.updateStructuredCellDialogDraft('{"a": 2}'));

    const explicitlyReadOnly = renderController(true).result;
    openPayloadDialog(explicitlyReadOnly, true);
    act(() =>
      explicitlyReadOnly.current.updateStructuredCellDialogDraft('{"a": 2}'),
    );
    expect(unsavedStates()).toEqual([]);
  });
});

describe("DIRTY-EDITORS inline editor unsaved state", () => {
  it("keeps an edited missing value dirty when its draft becomes an empty string", () => {
    const onDirtyChange = vi.fn();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial=""
        initialMissing
        nullable
        category="text"
        onCommit={onCommit}
        onCancel={vi.fn()}
        onDirtyChange={onDirtyChange}
      />,
    );
    const input = screen.getByLabelText("Cell value");
    expect(onDirtyChange).toHaveBeenLastCalledWith(false);
    fireEvent.change(input, { target: { value: "x" } });
    fireEvent.change(input, { target: { value: "" } });
    expect(onDirtyChange).toHaveBeenLastCalledWith(true);
    fireEvent.keyDown(input, { key: "Enter", ctrlKey: true });
    expect(onCommit).toHaveBeenCalledWith("");
  });

  it("counts a dirty inline editor only while an edit cell is open", () => {
    clearPostedMessages();
    const { result } = renderController();
    act(() => result.current.setInlineEditorDirty(true));
    expect(unsavedStates()).toEqual([]);

    act(() => result.current.handleStartEdit(0, idColumn));
    act(() => result.current.setInlineEditorDirty(true));
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    act(() => result.current.setEditCell(null));
    expect(unsavedStates()).toEqual([
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
    ]);
  });

  it("EditInput reports dirty only for real editable changes and clears on unmount", () => {
    const onDirtyChange = vi.fn();
    const view = render(
      <EditInput
        initial="5"
        nullable
        category="integer"
        onCommit={vi.fn()}
        onCancel={vi.fn()}
        onDirtyChange={onDirtyChange}
      />,
    );
    const input = screen.getByLabelText("Cell value");
    expect(onDirtyChange).not.toHaveBeenCalledWith(true);

    fireEvent.change(input, { target: { value: "6" } });
    expect(onDirtyChange).toHaveBeenLastCalledWith(true);
    fireEvent.change(input, { target: { value: "5" } });
    expect(onDirtyChange).toHaveBeenLastCalledWith(false);
    fireEvent.change(input, { target: { value: "7" } });
    view.unmount();
    expect(onDirtyChange).toHaveBeenLastCalledWith(false);

    const readOnlyDirty = vi.fn();
    render(
      <EditInput
        initial="5"
        nullable
        category="integer"
        readOnly
        onCommit={vi.fn()}
        onCancel={vi.fn()}
        onDirtyChange={readOnlyDirty}
      />,
    );
    fireEvent.change(screen.getAllByLabelText("Cell value").at(-1) as Element, {
      target: { value: "9" },
    });
    expect(readOnlyDirty).not.toHaveBeenCalledWith(true);
  });
});

function setupTableView(connectionReadOnly = false) {
  clearPostedMessages();
  const view = render(
    <TableView
      connectionId="conn"
      database="db"
      schema="public"
      table="items"
      connectionReadOnly={connectionReadOnly}
    />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns: [idColumn, qtyColumn, payloadColumn],
      primaryKeyColumns: ["id"],
    }),
  );
  const fetch = postedOfType("fetchPage").at(-1)?.payload as {
    fetchId: number;
  };
  act(() =>
    dispatchIncomingMessage("tableData", {
      fetchId: fetch.fetchId,
      rows: [{ id: 1, qty: 5, payload: { a: 1 } }],
      totalCount: 1,
    }),
  );
  return view;
}

function bodyCellFor(container: HTMLElement, columnName: string) {
  const headers = Array.from(
    container.querySelectorAll<HTMLElement>("thead tr:first-child th"),
  );
  const index = headers.findIndex(
    (header) => header.dataset.columnId === columnName,
  );
  const cell = container
    .querySelectorAll("tbody tr")[0]
    ?.querySelectorAll("td")[index];
  if (!(cell instanceof HTMLElement)) {
    throw new Error(`Missing body cell ${columnName}`);
  }
  return cell;
}

describe("DIRTY-EDITORS TableView unsaved state before disconnect", () => {
  it.each([
    "inline",
    "structured",
  ])("resynchronizes a clean host flag after reload of a dirty %s editor", (kind) => {
    const first = setupTableView();
    clearPostedMessages();
    fireEvent.doubleClick(
      bodyCellFor(first.container, kind === "inline" ? "qty" : "payload"),
    );
    fireEvent.change(
      screen.getByLabelText(kind === "inline" ? "Cell value" : "Cell data"),
      {
        target: { value: kind === "inline" ? "42" : '{"a": 2}' },
      },
    );
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);
    first.unmount();
    clearPostedMessages();
    setupTableView();
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: false }]);
  });

  it("reports an active dirty inline editor, clears it on cancel and keeps it after commit", () => {
    const view = setupTableView();
    clearPostedMessages();

    fireEvent.doubleClick(bodyCellFor(view.container, "qty"));
    expect(unsavedStates()).toEqual([]);
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "42" },
    });
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    fireEvent.keyDown(screen.getByLabelText("Cell value"), { key: "Escape" });
    expect(screen.queryByLabelText("Cell value")).toBeNull();
    expect(unsavedStates()).toEqual([
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
    ]);

    clearPostedMessages();
    fireEvent.doubleClick(bodyCellFor(view.container, "qty"));
    fireEvent.change(screen.getByLabelText("Cell value"), {
      target: { value: "43" },
    });
    fireEvent.keyDown(screen.getByLabelText("Cell value"), { key: "Enter" });
    expect(screen.queryByLabelText("Cell value")).toBeNull();
    expect(bodyCellFor(view.container, "qty").textContent).toContain("43");
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);
    expect(postedOfType("applyChanges")).toEqual([]);
  });

  it("does not report an unchanged inline editor or a read-only connection editor", () => {
    const view = setupTableView(true);
    clearPostedMessages();
    fireEvent.doubleClick(bodyCellFor(view.container, "qty"));
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    expect(input.readOnly).toBe(true);
    fireEvent.change(input, { target: { value: "99" } });
    fireEvent.keyDown(input, { key: "Escape" });
    expect(unsavedStates()).toEqual([]);

    const editable = setupTableView();
    clearPostedMessages();
    fireEvent.doubleClick(bodyCellFor(editable.container, "qty"));
    const unchanged = screen.getByLabelText("Cell value") as HTMLInputElement;
    expect(unchanged.readOnly).toBe(false);
    fireEvent.keyDown(unchanged, { key: "Enter" });
    expect(unsavedStates()).toEqual([]);
  });

  it("reports a modified JSON dialog draft and clears it on cancel", () => {
    const view = setupTableView();
    clearPostedMessages();
    fireEvent.doubleClick(bodyCellFor(view.container, "payload"));
    const editor = screen.getByLabelText("Cell data");
    expect(unsavedStates()).toEqual([]);

    fireEvent.change(editor, { target: { value: '{"a": 2}' } });
    expect(unsavedStates()).toEqual([{ hasUnsavedChanges: true }]);

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByLabelText("Cell data")).toBeNull();
    expect(unsavedStates()).toEqual([
      { hasUnsavedChanges: true },
      { hasUnsavedChanges: false },
    ]);
  });
});
