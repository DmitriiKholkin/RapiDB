import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import {
  type ColumnTypeMeta,
  NULL_SENTINEL,
} from "../../src/shared/tableTypes";
import type { ClipboardReadPayload } from "../../src/shared/webviewContracts";
import { EditInput } from "../../src/webview/components/table/EditInput";
import { QueryResultsGrid } from "../../src/webview/components/table/grid/QueryResultsGrid";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import { useUndoRedoHistory } from "../../src/webview/components/table/useUndoRedoHistory";
import type { QueryResult } from "../../src/webview/store";
import type { PendingEdits } from "../../src/webview/types";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
  getPostedMessages,
} from "./testUtils";

const virtualWindow = vi.hoisted(() => ({
  start: 0,
  size: Number.POSITIVE_INFINITY,
}));

vi.mock("@tanstack/react-virtual", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@tanstack/react-virtual")>();
  return {
    ...actual,
    useVirtualizer: ({
      count,
      rangeExtractor,
    }: {
      count: number;
      rangeExtractor?: (range: {
        startIndex: number;
        endIndex: number;
        overscan: number;
        count: number;
      }) => number[];
    }) => ({
      getVirtualItems: () => {
        if (count === 0) return [];
        const startIndex = Math.min(virtualWindow.start, count - 1);
        const endIndex = Math.min(startIndex + virtualWindow.size, count) - 1;
        return (rangeExtractor ?? actual.defaultRangeExtractor)({
          startIndex,
          endIndex,
          overscan: 0,
          count,
        }).map((index) => ({
          index,
          key: index,
          start: index * 26,
          end: (index + 1) * 26,
        }));
      },
      getTotalSize: () => count * 26,
      scrollToIndex: () => undefined,
    }),
  };
});

const columns: ColumnTypeMeta[] = ["a", "b", "c"].map((name) => ({
  name,
  type: "text",
  nativeType: "text",
  category: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq"],
  valueSemantics: "plain",
}));

const smallRows = [
  { a: "a0", b: "b0", c: "c0" },
  { a: null, b: "b1", c: "c1" },
  { a: "a2", b: "b2", c: "c2" },
];

type GridProps = React.ComponentProps<typeof TableGrid> & { mode?: never };

function buildProps(overrides: Partial<GridProps> = {}): GridProps {
  return {
    canEditRows: true,
    canSelectAndDeleteRows: false,
    colSizes: {},
    columns,
    editCell: null,
    filterDrafts: {},
    loading: false,
    loadingRef: { current: false },
    fetchEpochRef: { current: 0 },
    pasteStructureEpochRef: { current: 0 },
    newRows: [],
    onCancelEdit: vi.fn(),
    onBatchCellEdit: vi.fn(),
    onCommitCellEdit: vi.fn(),
    onCommitDraftCellEdit: vi.fn(),
    onMixedBatchEdit: vi.fn(),
    onFilterDraftChange: vi.fn(),
    onSelectionChange: vi.fn(),
    onSort: vi.fn(),
    onOpenStructuredCell: vi.fn(),
    onStartDraftEdit: vi.fn(),
    onStartEdit: vi.fn(),
    pendingEdits: new Map() as PendingEdits,
    rows: smallRows,
    scrollRef: createRef<HTMLDivElement>(),
    selected: new Set<number>(),
    sort: null,
    ...overrides,
  } as GridProps;
}

function cellValueEditor(): HTMLInputElement | HTMLTextAreaElement {
  return screen.getByLabelText("Cell value") as
    | HTMLInputElement
    | HTMLTextAreaElement;
}

describe("TableGrid editing regressions", () => {
  it("keeps the inline editor row mounted when it is virtualized out of view", () => {
    const rows = Array.from({ length: 50 }, (_, index) => ({
      a: `a${index}`,
      b: `b${index}`,
      c: `c${index}`,
    }));
    virtualWindow.start = 0;
    virtualWindow.size = 5;
    try {
      const props = buildProps({
        rows,
        editCell: { kind: "persisted", rowIdx: 2, col: "b" },
      });
      const view = render(<TableGrid {...props} />);
      fireEvent.change(cellValueEditor(), { target: { value: "typed" } });

      virtualWindow.start = 30;
      view.rerender(<TableGrid {...props} />);

      const editor = cellValueEditor();
      expect(editor.value).toBe("typed");
      expect(editor.closest("td")?.getAttribute("data-row")).toBe("2");
      expect(
        view.container.querySelector('td[data-row="3"][data-col="0"]'),
      ).toBeNull();
      expect(
        view.container.querySelector('td[data-row="30"][data-col="0"]'),
      ).not.toBeNull();
      const bodyRows = Array.from(
        view.container.querySelectorAll("tbody > tr"),
      );
      const editingRowIndex = bodyRows.findIndex((row) => row.contains(editor));
      const gapCell = bodyRows[editingRowIndex + 1]?.querySelector("td");
      expect(gapCell?.style.height).toBe(`${(30 - 3) * 26}px`);
      expect(props.onCommitCellEdit).not.toHaveBeenCalled();
      expect(props.onCancelEdit).not.toHaveBeenCalled();
    } finally {
      virtualWindow.start = 0;
      virtualWindow.size = Number.POSITIVE_INFINITY;
    }
  });

  it("keeps the draft-row inline editor mounted when it is virtualized out of view", () => {
    const rows = Array.from({ length: 50 }, (_, index) => ({
      a: `a${index}`,
      b: `b${index}`,
      c: `c${index}`,
    }));
    virtualWindow.start = 0;
    virtualWindow.size = 5;
    try {
      const props = buildProps({
        rows,
        newRows: [{}, {}],
        editCell: { kind: "draft", rowIdx: 1, col: "a" },
      });
      const view = render(<TableGrid {...props} />);
      fireEvent.change(cellValueEditor(), { target: { value: "typed" } });

      virtualWindow.start = 30;
      view.rerender(<TableGrid {...props} />);

      const editor = cellValueEditor();
      expect(editor.value).toBe("typed");
      expect(editor.closest("td")?.getAttribute("data-row")).toBe("-1");
      expect(
        view.container.querySelector('td[data-row="-2"][data-col="0"]'),
      ).toBeNull();
      expect(
        view.container.querySelector('td[data-row="1"][data-col="0"]'),
      ).toBeNull();
      expect(props.onCommitDraftCellEdit).not.toHaveBeenCalled();
      expect(props.onCancelEdit).not.toHaveBeenCalled();
    } finally {
      virtualWindow.start = 0;
      virtualWindow.size = Number.POSITIVE_INFINITY;
    }
  });

  it("refuses to copy a MISSING attribute instead of writing NULL, literal or empty text", () => {
    const props = buildProps({ rows: [{ b: "b0", c: "c0" }] });
    const view = render(<TableGrid {...props} />);
    const first = view.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="0"]',
    );
    const second = view.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="1"]',
    );
    if (!first || !second) throw new Error("Missing grid cells");
    expect(first.textContent).toContain("MISSING");
    fireEvent.mouseDown(first, { button: 0 });
    fireEvent.mouseUp(document);
    fireEvent.mouseDown(second, { button: 0, shiftKey: true });
    fireEvent.mouseUp(document);

    clearPostedMessages();
    fireEvent.keyDown(props.scrollRef.current as HTMLElement, {
      key: "c",
      ctrlKey: true,
    });
    expect(getLastPostedMessage()).toBeUndefined();
    expect(view.getByRole("alert").textContent).toContain("Copy blocked");
  });

  it("does not remount the inline editor when parent callbacks change identity", () => {
    const props = buildProps({
      editCell: { kind: "persisted", rowIdx: 0, col: "b" },
    });
    const view = render(<TableGrid {...props} />);
    const editor = cellValueEditor();
    fireEvent.change(editor, { target: { value: "typed" } });

    view.rerender(
      <TableGrid
        {...props}
        onCommitCellEdit={vi.fn()}
        onCancelEdit={vi.fn()}
        getRowMutationBlockReason={() => null}
      />,
    );

    expect(cellValueEditor()).toBe(editor);
    expect(editor.value).toBe("typed");
  });

  it("returns a stable undo/redo history object across renders", () => {
    const { result, rerender } = renderHook(() => useUndoRedoHistory());
    const first = result.current;
    rerender();
    expect(result.current).toBe(first);
  });

  it("clears the cell selection when the row-selection column appears", () => {
    const props = buildProps();
    const view = render(<TableGrid {...props} />);
    const cell = view.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="1"]',
    );
    if (!cell) throw new Error("Missing cell 0:1");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(document);
    expect(cell.classList.contains("rdb-tcell-selected")).toBe(true);

    view.rerender(<TableGrid {...props} canSelectAndDeleteRows />);

    expect(view.container.querySelectorAll(".rdb-tcell-selected")).toHaveLength(
      0,
    );
    clearPostedMessages();
    fireEvent.keyDown(props.scrollRef.current as HTMLElement, {
      key: "c",
      ctrlKey: true,
    });
    expect(
      getPostedMessages().filter(
        (message) => message.type === "writeClipboard",
      ),
    ).toHaveLength(0);
  });

  it("opens a multiline-capable editor for NULL text cells", () => {
    render(
      <TableGrid
        {...buildProps({
          editCell: { kind: "persisted", rowIdx: 1, col: "a" },
        })}
      />,
    );
    expect(cellValueEditor().tagName).toBe("TEXTAREA");
  });

  it.each([
    ["persisted NULL", { kind: "persisted", rowIdx: 1, col: "a" }],
    ["draft", { kind: "draft", rowIdx: 0, col: "a" }],
  ] as const)("lets Shift+Enter type a line break in a %s text cell", async (_label, editCell) => {
    const user = userEvent.setup();
    const props = buildProps({ newRows: [{}], editCell });
    render(<TableGrid {...props} />);
    const editor = cellValueEditor();
    await user.keyboard("a{Shift>}{Enter}{/Shift}b");
    expect(editor.value).toBe("a\nb");
    expect(props.onCommitCellEdit).not.toHaveBeenCalled();
    expect(props.onCommitDraftCellEdit).not.toHaveBeenCalled();
    await user.keyboard("{Enter}");
    const commit =
      editCell.kind === "draft"
        ? props.onCommitDraftCellEdit
        : props.onCommitCellEdit;
    expect(commit).toHaveBeenCalledOnce();
    expect(vi.mocked(commit).mock.calls[0][2]).toBe("a\nb");
  });
});

describe("EditInput multiline text", () => {
  it("inserts a line break on Shift+Enter without committing", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="text"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const editor = cellValueEditor();
    await user.keyboard("a{Shift>}{Enter}{/Shift}b");
    expect(editor.value).toBe("a\nb");
    expect(onCommit).not.toHaveBeenCalled();
    expect(document.activeElement).toBe(editor);
  });

  it("commits on plain Enter after typing a line break", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial=""
        initialMissing
        nullable
        category="text"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    await user.keyboard("a{Shift>}{Enter}{/Shift}b{Enter}");
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("a\nb");
  });

  it("still commits on Shift+Enter in a single-line integer editor", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="integer"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    expect(cellValueEditor().tagName).toBe("INPUT");
    await user.keyboard("42{Shift>}{Enter}{/Shift}");
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("42");
  });

  it.each([
    ["NULL", NULL_SENTINEL, false],
    ["missing", "", true],
  ])("preserves pasted line breaks into an initially %s text cell", async (_label, initial, initialMissing) => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={initial}
        initialMissing={initialMissing}
        nullable
        category="text"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const editor = cellValueEditor();
    expect(editor.tagName).toBe("TEXTAREA");
    await user.click(editor);
    await user.paste("line1\nline2");
    expect(editor.value).toBe("line1\nline2");
    fireEvent.keyDown(editor, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("line1\nline2");
  });

  it("preserves line breaks from the context-menu paste", () => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="lob"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const editor = cellValueEditor();
    fireEvent.contextMenu(editor);
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const request = getLastPostedMessage()?.payload as ClipboardReadPayload;
    act(() => {
      dispatchIncomingMessage("clipboardText", {
        ...request,
        text: "line1\nline2",
      });
    });
    expect(editor.value).toBe("line1\nline2");
    fireEvent.blur(editor);
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("line1\nline2");
  });

  it("normalizes CRLF from the context-menu paste", () => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="text"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const editor = cellValueEditor();
    fireEvent.contextMenu(editor);
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const request = getLastPostedMessage()?.payload as ClipboardReadPayload;
    act(() => {
      dispatchIncomingMessage("clipboardText", {
        ...request,
        text: "line1\r\nline2\rline3",
      });
    });
    fireEvent.keyDown(editor, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("line1\nline2\nline3");
  });

  it("cancels the textarea editor on Escape", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="text"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const editor = cellValueEditor();
    expect(editor.tagName).toBe("TEXTAREA");
    fireEvent.change(editor, { target: { value: "line1\nline2" } });
    fireEvent.keyDown(editor, { key: "Escape" });
    expect(onCancel).toHaveBeenCalledOnce();
    expect(onCommit).not.toHaveBeenCalled();
  });

  it("keeps NULL, DEFAULT and empty-string semantics for untouched text editors", () => {
    const onCommitNull = vi.fn();
    const { unmount } = render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="text"
        onCommit={onCommitNull}
        onCancel={vi.fn()}
      />,
    );
    fireEvent.blur(cellValueEditor());
    expect(onCommitNull).toHaveBeenCalledExactlyOnceWith(NULL_SENTINEL);
    unmount();

    const onCommitDefault = vi.fn();
    const onCancelDefault = vi.fn();
    const defaultView = render(
      <EditInput
        initial=""
        initialMissing
        nullable
        category="text"
        onCommit={onCommitDefault}
        onCancel={onCancelDefault}
      />,
    );
    fireEvent.blur(cellValueEditor());
    expect(onCommitDefault).not.toHaveBeenCalled();
    expect(onCancelDefault).toHaveBeenCalledOnce();
    defaultView.unmount();

    const onCommitEmpty = vi.fn();
    render(
      <EditInput
        initial="x"
        nullable
        category="text"
        onCommit={onCommitEmpty}
        onCancel={vi.fn()}
      />,
    );
    fireEvent.change(cellValueEditor(), { target: { value: "" } });
    fireEvent.blur(cellValueEditor());
    expect(onCommitEmpty).toHaveBeenCalledExactlyOnceWith("");
  });

  it.each([
    ["NULL", NULL_SENTINEL, false],
    ["missing", "", true],
  ])("preserves pasted line breaks into an initially %s opaque-type cell", async (_label, initial, initialMissing) => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={initial}
        initialMissing={initialMissing}
        nullable
        category="other"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const editor = cellValueEditor();
    expect(editor.tagName).toBe("TEXTAREA");
    await user.click(editor);
    await user.paste("line1\nline2");
    fireEvent.keyDown(editor, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("line1\nline2");
  });

  it("keeps existing line breaks when editing an opaque-type cell", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={"line1\nline2"}
        nullable
        category="other"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const editor = cellValueEditor();
    expect(editor.value).toBe("line1\nline2");
    await user.click(editor);
    await user.keyboard("{End}!");
    fireEvent.blur(editor);
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("line1\nline2!");
  });

  it("keeps a single-line input for non-text categories", () => {
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="integer"
        onCommit={vi.fn()}
        onCancel={vi.fn()}
      />,
    );
    expect(cellValueEditor().tagName).toBe("INPUT");
  });
});

describe("Query results copy", () => {
  function QueryResultsGridHarness({ result }: { result: QueryResult }) {
    const [columnOrder, setColumnOrder] = useState(["__col_0", "__col_1"]);
    const [sorting, setSorting] = useState<
      React.ComponentProps<typeof QueryResultsGrid>["sorting"]
    >([]);
    const [columnSizing, setColumnSizing] = useState<Record<string, number>>(
      {},
    );
    return (
      <QueryResultsGrid
        result={result}
        columnOrder={columnOrder}
        onColumnOrderChange={setColumnOrder}
        sorting={sorting}
        onSortingChange={setSorting}
        columnSizing={columnSizing}
        onColumnSizingChange={setColumnSizing}
      />
    );
  }

  it("refuses to copy a MISSING attribute instead of writing NULL, literal or empty text", () => {
    const result: QueryResult = {
      columns: ["optional", "name"],
      columnMeta: [{ category: "text" }, { category: "text" }],
      rows: [{ __col_1: "x" }],
      missingCells: [{ rowIndex: 0, columnIndex: 0 }],
      rowCount: 1,
      executionTimeMs: 1,
    };
    const { container, getByRole } = render(
      <QueryResultsGridHarness result={result} />,
    );
    const first = container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="0"]',
    );
    const second = container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="1"]',
    );
    if (!first || !second) throw new Error("Missing result cells");
    fireEvent.mouseDown(first, { button: 0 });
    fireEvent.mouseUp(document);
    fireEvent.mouseDown(second, { button: 0, shiftKey: true });
    fireEvent.mouseUp(document);

    clearPostedMessages();
    fireEvent.keyDown(second, { key: "c", ctrlKey: true });
    expect(getLastPostedMessage()).toBeUndefined();
    expect(getByRole("alert").textContent).toContain("Copy blocked");
  });
});
