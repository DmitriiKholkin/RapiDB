import { act, fireEvent, render } from "@testing-library/react";
import { createRef } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  type ColumnTypeMeta,
  NULL_SENTINEL,
} from "../../src/shared/tableTypes";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import { INSERT_DEFAULT_SENTINEL } from "../../src/webview/components/table/tableViewHelpers";
import type { PendingEdits } from "../../src/webview/types";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
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

const rows = [
  { a: "a0", b: "b0", c: "c0" },
  { a: "a1", b: "b1", c: "c1" },
  { a: "a2", b: "b2", c: "c2" },
];

type GridProps = React.ComponentProps<typeof TableGrid> & { mode?: never };

function setup(overrides: Partial<GridProps> = {}) {
  const props = {
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
    rows,
    scrollRef: createRef<HTMLDivElement>(),
    selected: new Set<number>(),
    sort: null,
    ...overrides,
  } as GridProps;
  const view = render(<TableGrid {...props} />);
  const cell = (row: number, col: number) => {
    const element = view.container.querySelector<HTMLElement>(
      `td[data-row="${row}"][data-col="${col}"]`,
    );
    if (!element) throw new Error(`Missing cell ${row}:${col}`);
    return element;
  };
  const select = (
    from: [number, number],
    to: [number, number] = from,
  ): void => {
    fireEvent.mouseDown(cell(...from), { button: 0 });
    fireEvent.mouseUp(document);
    if (to !== from) {
      fireEvent.mouseDown(cell(...to), { button: 0, shiftKey: true });
      fireEvent.mouseUp(document);
    }
  };
  const copy = (): string | undefined => {
    clearPostedMessages();
    fireEvent.keyDown(props.scrollRef.current as HTMLElement, {
      key: "c",
      ctrlKey: true,
    });
    const message = getLastPostedMessage();
    expect(message?.type).toBe("writeClipboard");
    return (message?.payload as { text: string }).text;
  };
  const paste = (text: string): void => {
    clearPostedMessages();
    fireEvent.paste(props.scrollRef.current as HTMLElement);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    act(() => {
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text,
      });
    });
  };
  const headerOrder = () =>
    Array.from(
      view.container.querySelectorAll(
        "thead tr:first-child th[data-column-id]",
      ),
    ).map((th) => th.getAttribute("data-column-id"));
  return { ...view, props, cell, select, copy, paste, headerOrder };
}

function installHeaderRects(width = 100) {
  const original = HTMLElement.prototype.getBoundingClientRect;
  HTMLElement.prototype.getBoundingClientRect = function (this: HTMLElement) {
    const id = this.getAttribute("data-column-id");
    const order = Array.from(
      document.querySelectorAll("thead tr:first-child th[data-column-id]"),
    ).map((th) => th.getAttribute("data-column-id"));
    const index = id === null ? -1 : order.indexOf(id);
    const left = Math.max(index, 0) * width;
    return {
      x: left,
      y: 0,
      left,
      top: 0,
      right: left + width,
      bottom: 28,
      width,
      height: 28,
      toJSON: () => ({}),
    } as DOMRect;
  };
  return () => {
    HTMLElement.prototype.getBoundingClientRect = original;
  };
}

function dragHeader(
  container: HTMLElement,
  columnId: string,
  fromX: number,
  toX: number,
) {
  const th = container.querySelector<HTMLElement>(
    `th[data-column-id="${columnId}"]`,
  );
  if (!th) throw new Error(`Missing header ${columnId}`);
  fireEvent.mouseDown(th, { button: 0, clientX: fromX, clientY: 14 });
  for (let x = fromX; x !== toX; x += Math.sign(toX - fromX) * 5) {
    fireEvent.mouseMove(document, { clientX: x, clientY: 14 });
  }
  fireEvent.mouseMove(document, { clientX: toX, clientY: 14 });
  fireEvent.mouseUp(document, { clientX: toX, clientY: 14 });
}

let restoreRects: (() => void) | null = null;

afterEach(() => {
  restoreRects?.();
  restoreRects = null;
  document.querySelectorAll(".rapidb-column-drag-ghost").forEach((node) => {
    node.remove();
  });
});

describe("TableGrid selection column stability", () => {
  it("keeps the checkbox column and a dragged column order while row selection is disabled", () => {
    restoreRects = installHeaderRects();
    const grid = setup({ canSelectAndDeleteRows: true });
    const selectionId = grid.headerOrder()[0];
    expect(selectionId).toBe("__sel");
    dragHeader(grid.container, "c", 350, 120);
    expect(grid.headerOrder()).toEqual(["__sel", "c", "a", "b"]);

    grid.rerender(
      <TableGrid
        {...grid.props}
        columns={columns.map((column) => ({ ...column }))}
        rowSelectionDisabled
      />,
    );
    expect(grid.headerOrder()).toEqual(["__sel", "c", "a", "b"]);
    const checkboxes = Array.from(
      grid.container.querySelectorAll<HTMLInputElement>(
        'input[type="checkbox"]',
      ),
    );
    expect(checkboxes).toHaveLength(rows.length + 1);
    expect(checkboxes.every((checkbox) => checkbox.disabled)).toBe(true);

    grid.rerender(<TableGrid {...grid.props} canSelectAndDeleteRows={false} />);
    expect(grid.headerOrder()).toEqual(["c", "a", "b"]);
    grid.rerender(<TableGrid {...grid.props} />);
    expect(grid.headerOrder()).toEqual(["__sel", "c", "a", "b"]);
  });

  it("does not let a data column be dropped before the checkbox column", () => {
    restoreRects = installHeaderRects();
    const grid = setup({ canSelectAndDeleteRows: true });
    dragHeader(grid.container, "a", 150, 10);
    expect(grid.headerOrder()).toEqual(["__sel", "a", "b", "c"]);
  });
});

describe("TableGrid draft DEFAULT editing", () => {
  it("keeps DEFAULT when a DEFAULT draft cell is opened and left unedited", () => {
    const onCommitDraftCellEdit = vi.fn();
    const onCancelEdit = vi.fn();
    const grid = setup({
      newRows: [{ a: { value: INSERT_DEFAULT_SENTINEL } }],
      editCell: { kind: "draft", rowIdx: 0, col: "a" },
      onCommitDraftCellEdit,
      onCancelEdit,
    });
    const input = grid.getByRole("textbox", { name: "Cell value" });
    expect((input as HTMLInputElement).value).toBe("");
    fireEvent.blur(input);
    expect(onCommitDraftCellEdit).not.toHaveBeenCalled();
    expect(onCancelEdit).toHaveBeenCalled();
  });

  it("still commits an empty string typed into a DEFAULT draft cell", () => {
    const onCommitDraftCellEdit = vi.fn();
    const grid = setup({
      newRows: [{ a: { value: INSERT_DEFAULT_SENTINEL } }],
      editCell: { kind: "draft", rowIdx: 0, col: "a" },
      onCommitDraftCellEdit,
    });
    const input = grid.getByRole("textbox", { name: "Cell value" });
    fireEvent.change(input, { target: { value: "x" } });
    fireEvent.change(input, { target: { value: "" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommitDraftCellEdit).toHaveBeenCalledWith(
      0,
      expect.objectContaining({ name: "a" }),
      "",
    );
  });
});

describe("TableGrid clipboard", () => {
  it("pastes from the top-left of a bottom-up, right-to-left selection", () => {
    const onBatchCellEdit = vi.fn();
    const grid = setup({ onBatchCellEdit });
    grid.select([2, 2], [1, 1]);
    grid.paste("x\ty\nz\tw");
    expect(onBatchCellEdit).toHaveBeenCalledTimes(1);
    expect(
      onBatchCellEdit.mock.calls[0][0].map(
        (edit: { rowIdx: number; column: ColumnTypeMeta; newVal: string }) => [
          edit.rowIdx,
          edit.column.name,
          edit.newVal,
        ],
      ),
    ).toEqual([
      [1, "b", "x"],
      [1, "c", "y"],
      [2, "b", "z"],
      [2, "c", "w"],
    ]);
  });

  it("copies staged pending values, including a pending NULL", () => {
    const pendingEdits: PendingEdits = new Map([
      [
        0,
        new Map<string, unknown>([
          ["a", "staged"],
          ["b", null],
        ]),
      ],
    ]);
    const grid = setup({ pendingEdits });
    grid.select([0, 0], [0, 2]);
    expect(grid.copy()).toBe("staged\tNULL\tc0");
  });

  it("quotes literal NULL text on copy and keeps it distinct from SQL NULL on paste", () => {
    const onBatchCellEdit = vi.fn();
    const grid = setup({
      rows: [{ a: "NULL", b: null, c: "c0" }],
      onBatchCellEdit,
    });
    grid.select([0, 0], [0, 1]);
    const copied = grid.copy();
    expect(copied).toBe('"NULL"\tNULL');

    grid.select([0, 1]);
    grid.paste(copied as string);
    expect(
      onBatchCellEdit.mock.calls[0][0].map(
        (edit: { column: ColumnTypeMeta; newVal: string }) => [
          edit.column.name,
          edit.newVal,
        ],
      ),
    ).toEqual([
      ["b", "NULL"],
      ["c", NULL_SENTINEL],
    ]);
  });

  it("keeps quoted literal NULL text when pasting into draft rows", () => {
    const onMixedBatchEdit = vi.fn();
    const grid = setup({
      newRows: [{}],
      onMixedBatchEdit,
    });
    grid.select([-1, 0]);
    grid.paste('"NULL"\tNULL');
    expect(
      onMixedBatchEdit.mock.calls[0][0].map(
        (edit: { column: ColumnTypeMeta; newVal: string }) => [
          edit.column.name,
          edit.newVal,
        ],
      ),
    ).toEqual([
      ["a", "NULL"],
      ["b", NULL_SENTINEL],
    ]);
  });

  it("refuses to copy DEFAULT draft cells and missing fields, keeping empty text distinct", () => {
    const onBatchCellEdit = vi.fn();
    const grid = setup({
      rows: [{ a: "a0" }],
      newRows: [{ a: { value: INSERT_DEFAULT_SENTINEL }, b: { value: "x" } }],
      onBatchCellEdit,
    });
    grid.select([-1, 0], [0, 2]);
    clearPostedMessages();
    fireEvent.keyDown(grid.props.scrollRef.current as HTMLElement, {
      key: "c",
      ctrlKey: true,
    });
    expect(getLastPostedMessage()).toBeUndefined();
    grid.select([-1, 1]);
    expect(grid.copy()).toBe("x");

    grid.select([0, 0]);
    grid.paste('""\t""');
    expect(
      onBatchCellEdit.mock.calls[0][0].map(
        (edit: { column: ColumnTypeMeta; newVal: string }) => [
          edit.column.name,
          edit.newVal,
        ],
      ),
    ).toEqual([
      ["a", ""],
      ["b", ""],
    ]);
  });
});
