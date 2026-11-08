import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
} from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
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
const rows = [{ a: "old-a", b: "old-b", c: "old-c" }];
const drafts = [{ a: { value: "draft-a" }, b: { value: "draft-b" } }];

function setup() {
  const onBatchCellEdit = vi.fn();
  const onMixedBatchEdit = vi.fn();
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
    newRows: drafts,
    onCancelEdit: vi.fn(),
    onBatchCellEdit,
    onCommitCellEdit: vi.fn(),
    onCommitDraftCellEdit: vi.fn(),
    onMixedBatchEdit,
    onFilterDraftChange: vi.fn(),
    onSelectionChange: vi.fn(),
    onSort: vi.fn(),
    onOpenStructuredCell: vi.fn(),
    onStartDraftEdit: vi.fn(),
    onStartEdit: vi.fn(),
    pendingEdits: new Map(),
    rows,
    scrollRef: createRef<HTMLDivElement>(),
    selected: new Set<number>(),
    sort: null,
  };
  const view = render(<TableGrid {...props} />);
  const request = (row: number) => {
    const cell = view.container.querySelector<HTMLElement>(
      `td[data-row="${row}"][data-col="0"]`,
    );
    if (!cell) throw new Error("Missing cell");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    fireEvent.paste(window);
    const message = getLastPostedMessage();
    expect(message?.type).toBe("readClipboard");
    return message?.payload as object;
  };
  const reply = (payload: object, text = "new-a\tnew-b") =>
    act(() => {
      dispatchIncomingMessage("clipboardText", { ...payload, text });
    });
  return { ...view, props, request, reply, onBatchCellEdit, onMixedBatchEdit };
}

describe("delayed grid clipboard replies", () => {
  it.each([
    "button",
    "select",
    "window",
  ] as const)("does not capture native paste with focus outside the grid (%s)", (target) => {
    const grid = setup();
    grid.request(0);
    const controls = render(
      <>
        <button type="button">Outside</button>
        <select aria-label="Outside select">
          <option>One</option>
        </select>
      </>,
    );
    const button = controls.getByRole("button", { name: "Outside" });
    const select = controls.getByRole("combobox", { name: "Outside select" });
    const focused = target === "select" ? select : button;
    focused.focus();
    clearPostedMessages();
    expect(fireEvent.paste(target === "window" ? window : focused)).toBe(true);
    expect(getLastPostedMessage()).toBeUndefined();
    expect(grid.onBatchCellEdit).not.toHaveBeenCalled();
  });

  it("does not capture native paste or its shortcut on controls inside the grid", () => {
    const grid = setup();
    grid.request(0);
    const button = grid.container.querySelector<HTMLButtonElement>(
      'button[aria-label="Resize a column"]',
    );
    if (!button) throw new Error("Missing grid control");
    button.focus();
    clearPostedMessages();
    expect(fireEvent.keyDown(button, { key: "v", ctrlKey: true })).toBe(true);
    expect(fireEvent.paste(button)).toBe(true);
    expect(getLastPostedMessage()).toBeUndefined();
  });

  it("does not capture an outside event merely because the grid still has focus", () => {
    const grid = setup();
    grid.request(0);
    const outside = render(<button type="button">Outside</button>).getByRole(
      "button",
      { name: "Outside" },
    );
    clearPostedMessages();
    expect(fireEvent.paste(outside)).toBe(true);
    expect(getLastPostedMessage()).toBeUndefined();
  });

  it("delivers native paste only to the focused grid when two grids retain selections", () => {
    const first = setup();
    const second = setup();
    first.request(0);
    const payload = second.request(0);
    second.reply(payload, "second");
    expect(first.onBatchCellEdit).not.toHaveBeenCalled();
    expect(second.onBatchCellEdit.mock.calls[0][0]).toMatchObject([
      { newVal: "second", column: { name: "a" } },
    ]);
  });

  it("allows native paste into a draft without persisted-row edit permission", () => {
    const grid = setup();
    grid.rerender(<TableGrid {...grid.props} canEditRows={false} />);
    const draft = grid.container.querySelector<HTMLElement>(
      'td[data-row="-1"][data-col="0"]',
    );
    if (!draft) throw new Error("Missing draft");
    fireEvent.mouseDown(draft, { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    expect(fireEvent.paste(grid.props.scrollRef.current as HTMLElement)).toBe(
      false,
    );
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    grid.reply(request?.payload as object, "draft value");
    expect(grid.onMixedBatchEdit.mock.calls[0][0]).toMatchObject([
      { rowIdx: 0, column: { name: "a" }, newVal: "draft value" },
    ]);
    expect(grid.onMixedBatchEdit.mock.calls[0][1]).toEqual([]);
  });

  it("does not stage a paste spanning draft and persisted rows without edit permission", () => {
    const grid = setup();
    grid.rerender(<TableGrid {...grid.props} canEditRows={false} />);
    const draft = grid.container.querySelector<HTMLElement>(
      'td[data-row="-1"][data-col="0"]',
    );
    if (!draft) throw new Error("Missing draft");
    fireEvent.mouseDown(draft, { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    fireEvent.paste(grid.props.scrollRef.current as HTMLElement);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    grid.reply(request?.payload as object, "draft\npersisted");
    expect(grid.onMixedBatchEdit).not.toHaveBeenCalled();
    expect(grid.onBatchCellEdit).not.toHaveBeenCalled();
    expect(screen.getByText(/Paste failed/)).toBeTruthy();
  });

  it("does not intercept persisted-row paste without edit permission even when drafts exist", () => {
    const grid = setup();
    grid.rerender(<TableGrid {...grid.props} canEditRows={false} />);
    const cell = grid.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="0"]',
    );
    const scroll = grid.props.scrollRef.current;
    if (!cell || !scroll) throw new Error("Missing grid cell");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    expect(fireEvent.keyDown(scroll, { key: "v", ctrlKey: true })).toBe(true);
    expect(fireEvent.paste(scroll)).toBe(true);
    expect(getLastPostedMessage()).toBeUndefined();
    expect(grid.onBatchCellEdit).not.toHaveBeenCalled();
  });

  it("supports the keyboard paste shortcut for a draft without edit permission", () => {
    const grid = setup();
    grid.rerender(<TableGrid {...grid.props} canEditRows={false} />);
    const cell = grid.container.querySelector<HTMLElement>(
      'td[data-row="-1"][data-col="0"]',
    );
    const scroll = grid.props.scrollRef.current;
    if (!cell || !scroll) throw new Error("Missing grid cell");
    fireEvent.mouseDown(cell, { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    expect(fireEvent.keyDown(scroll, { key: "v", metaKey: true })).toBe(false);
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    grid.reply(request?.payload as object, "draft");
    expect(grid.onMixedBatchEdit.mock.calls[0][0]).toMatchObject([
      { rowIdx: 0, newVal: "draft" },
    ]);
  });

  it("pastes normally into the selected persisted and draft rows", () => {
    const grid = setup();
    grid.reply(grid.request(0));
    expect(grid.onBatchCellEdit.mock.calls[0][0]).toMatchObject([
      {
        rowIdx: 0,
        column: { name: "a" },
        newVal: "new-a",
        originalVal: "old-a",
      },
      {
        rowIdx: 0,
        column: { name: "b" },
        newVal: "new-b",
        originalVal: "old-b",
      },
    ]);
    grid.reply(grid.request(-1));
    expect(grid.onMixedBatchEdit.mock.calls[0][0]).toMatchObject([
      { rowIdx: 0, column: { name: "a" }, newVal: "new-a" },
      { rowIdx: 0, column: { name: "b" }, newVal: "new-b" },
    ]);
  });

  it.each([
    "drafts",
    "rows",
    "columns",
    "collapse",
    "epoch",
    "loading",
  ] as const)("ignores a reply after a change to %s", (change) => {
    const grid = setup();
    const payload = grid.request(change === "drafts" ? -1 : 0);
    if (change === "epoch") grid.props.fetchEpochRef.current++;
    else
      grid.rerender(
        <TableGrid
          {...grid.props}
          {...(change === "drafts"
            ? { newRows: [{ a: { value: "different" } }, ...drafts] }
            : change === "rows"
              ? { rows: [{ a: "different" }] }
              : change === "columns"
                ? { columns: [columns[1], columns[0], columns[2]] }
                : change === "collapse"
                  ? { colSizes: { b: 1 } }
                  : { loading: true })}
        />,
      );
    grid.reply(payload);
    expect(grid.onBatchCellEdit).not.toHaveBeenCalled();
    expect(grid.onMixedBatchEdit).not.toHaveBeenCalled();
  });

  it("allows a reply after draft value changes and non-collapsing resize", () => {
    const grid = setup();
    const payload = grid.request(-1);
    grid.rerender(
      <TableGrid
        {...grid.props}
        newRows={[{ a: { value: "edited" } }]}
        colSizes={{ b: 120 }}
      />,
    );
    grid.reply(payload);
    expect(grid.onMixedBatchEdit).toHaveBeenCalledOnce();
  });

  it("keeps the newer context menu target when an older response arrives", () => {
    const grid = setup();
    const cell = (col: number) => {
      const found = grid.container.querySelector<HTMLElement>(
        `td[data-row="0"][data-col="${col}"]`,
      );
      if (!found) throw new Error("Missing cell");
      return found;
    };
    fireEvent.mouseDown(cell(0), { button: 0 });
    fireEvent.mouseUp(document);
    fireEvent.mouseDown(cell(2), { button: 0, shiftKey: true });
    fireEvent.mouseUp(document);
    fireEvent.mouseDown(cell(1), { button: 2 });
    fireEvent.contextMenu(cell(1));
    clearPostedMessages();
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const first = getLastPostedMessage()?.payload as object;
    fireEvent.mouseDown(cell(2), { button: 2 });
    fireEvent.contextMenu(cell(2));
    grid.reply(first, "first");
    expect(grid.onBatchCellEdit.mock.calls[0][0][0].column.name).toBe("b");
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    grid.reply(getLastPostedMessage()?.payload as object, "second");
    expect(grid.onBatchCellEdit.mock.calls[1][0][0].column.name).toBe("c");
  });

  it.each([
    "header",
    "blank",
  ] as const)("uses the current selection when opening a new menu on the %s", (target) => {
    const grid = setup();
    const a = grid.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="0"]',
    );
    const header = grid.container.querySelector<HTMLElement>(
      'thead th[data-column-id="a"]',
    );
    const scroll = grid.props.scrollRef.current;
    if (!a || !header || !scroll) throw new Error("Missing grid element");
    fireEvent.mouseDown(a, { button: 2 });
    fireEvent.contextMenu(a);
    fireEvent.pointerDown(document.body);
    fireEvent.keyDown(scroll, { key: "ArrowRight" });
    fireEvent.contextMenu(target === "header" ? header : scroll);
    clearPostedMessages();
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const request = getLastPostedMessage();
    expect(request?.type).toBe("readClipboard");
    grid.reply(request?.payload as object, "value");
    expect(grid.onBatchCellEdit.mock.calls[0][0][0].column.name).toBe("b");
  });

  it("retains a pending cell target while a newer header menu uses the current selection", () => {
    const grid = setup();
    const a = grid.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="0"]',
    );
    const header = grid.container.querySelector<HTMLElement>(
      'thead th[data-column-id="a"]',
    );
    const scroll = grid.props.scrollRef.current;
    if (!a || !header || !scroll) throw new Error("Missing grid element");
    fireEvent.mouseDown(a, { button: 2 });
    fireEvent.contextMenu(a);
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const first = getLastPostedMessage()?.payload as object;
    fireEvent.keyDown(scroll, { key: "ArrowRight" });
    fireEvent.contextMenu(header);
    grid.reply(first, "first");
    expect(grid.onBatchCellEdit.mock.calls[0][0][0].column.name).toBe("a");
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    grid.reply(getLastPostedMessage()?.payload as object, "second");
    expect(grid.onBatchCellEdit.mock.calls[1][0][0].column.name).toBe("b");
  });

  it("binds a cell menu to its contextmenu target even without a mouse down", () => {
    const grid = setup();
    const a = grid.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="0"]',
    );
    const b = grid.container.querySelector<HTMLElement>(
      'td[data-row="0"][data-col="1"]',
    );
    if (!a || !b) throw new Error("Missing cells");
    fireEvent.mouseDown(a, { button: 0 });
    fireEvent.mouseUp(document);
    fireEvent.contextMenu(b);
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    grid.reply(getLastPostedMessage()?.payload as object, "value");
    expect(grid.onBatchCellEdit.mock.calls[0][0][0].column.name).toBe("b");
  });

  it("rejects replies and new requests between structural enqueue and commit", () => {
    const grid = setup();
    const { result } = renderHook(() =>
      useTableMutationController({
        pasteStructureEpochRef: grid.props.pasteStructureEpochRef,
        canEditRows: true,
        loadingRef: grid.props.loadingRef,
        columnsRef: { current: columns },
        fetchPageRef: { current: vi.fn() },
        pkColsRef: { current: [] },
        preserveScrollPositionRef: { current: vi.fn() },
        rowsRef: { current: rows },
        mongoIdTypesRef: { current: [] },
        rowValueTypesRef: { current: [] },
        selected: new Set(),
      }),
    );
    const payload = grid.request(-1);
    act(() => {
      result.current.startInsertRow();
      dispatchIncomingMessage("clipboardText", { ...payload, text: "stale" });
      clearPostedMessages();
      fireEvent.paste(window);
      expect(getLastPostedMessage()).toBeUndefined();
    });
    expect(grid.onMixedBatchEdit).not.toHaveBeenCalled();
    grid.rerender(
      <TableGrid {...grid.props} newRows={[{ a: { value: "replacement" } }]} />,
    );
    grid.reply(payload);
    expect(grid.onMixedBatchEdit).not.toHaveBeenCalled();
  });

  it("ignores a reply after the user drags a column to a new position", () => {
    const grid = setup();
    const payload = grid.request(0);
    const a = grid.container.querySelector<HTMLElement>(
      'thead th[data-column-id="a"]',
    );
    const b = grid.container.querySelector<HTMLElement>(
      'thead th[data-column-id="b"]',
    );
    if (!a || !b) throw new Error("Missing column headers");
    vi.spyOn(b, "getBoundingClientRect").mockReturnValue({
      left: 100,
      right: 200,
      top: 0,
      bottom: 50,
      width: 100,
    } as DOMRect);
    fireEvent.mouseDown(a, { button: 0, clientX: 0 });
    document.addEventListener(
      "mousemove",
      () => {
        dispatchIncomingMessage("clipboardText", { ...payload, text: "stale" });
      },
      { once: true },
    );
    fireEvent.mouseMove(document, { clientX: 160 });
    fireEvent.mouseUp(document, { clientX: 160 });
    expect(
      grid.container
        .querySelector("thead th[data-column-id]")
        ?.getAttribute("data-column-id"),
    ).toBe("b");
    grid.reply(payload);
    expect(grid.onBatchCellEdit).not.toHaveBeenCalled();
    expect(grid.onMixedBatchEdit).not.toHaveBeenCalled();
    vi.restoreAllMocks();
  });
});
