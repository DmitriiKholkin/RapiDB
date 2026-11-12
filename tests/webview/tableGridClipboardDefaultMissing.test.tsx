import { fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import { INSERT_DEFAULT_SENTINEL } from "../../src/webview/components/table/tableViewHelpers";
import {
  clearPostedMessages,
  getLastPostedMessage,
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

const BLOCKED = /Copy blocked: the selection contains DEFAULT or MISSING/;

function setup() {
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
    newRows: [
      {
        a: { value: INSERT_DEFAULT_SENTINEL },
        b: { value: NULL_SENTINEL },
        c: { value: "" },
      },
    ],
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
    pendingEdits: new Map(),
    rows: [
      { a: "DEFAULT", b: "MISSING" },
      { a: "NULL", b: null, c: "" },
    ],
    scrollRef: createRef<HTMLDivElement>(),
    selected: new Set<number>(),
    sort: null,
  } satisfies React.ComponentProps<typeof TableGrid>;
  const view = render(<TableGrid {...props} />);
  const cell = (row: number, col: number) => {
    const node = view.container.querySelector<HTMLElement>(
      `td[data-row="${row}"][data-col="${col}"]`,
    );
    if (!node) throw new Error(`Missing cell ${row}:${col}`);
    return node;
  };
  const select = (from: [number, number], to: [number, number] = from) => {
    fireEvent.mouseDown(cell(...from), { button: 0 });
    fireEvent.mouseUp(document);
    fireEvent.mouseDown(cell(...to), { button: 0, shiftKey: true });
    fireEvent.mouseUp(document);
  };
  const press = (key: "c" | "x") => {
    clearPostedMessages();
    fireEvent.keyDown(props.scrollRef.current as HTMLElement, {
      key,
      ctrlKey: true,
    });
  };
  const copiedText = () => {
    const message = getLastPostedMessage();
    expect(message?.type).toBe("writeClipboard");
    return (message?.payload as { text: string }).text;
  };
  const contextCopy = () => {
    fireEvent.contextMenu(cell(0, 0));
    clearPostedMessages();
    fireEvent.click(screen.getByRole("menuitem", { name: /copy/i }));
  };
  return { select, press, copiedText, contextCopy, view, props };
}

describe("TableGrid copy of DEFAULT and MISSING cells", () => {
  it.each([
    "mouse",
    "keyboard",
    "clear",
    "extend",
  ] as const)("clears a stale copy error when selection changes via %s", (change) => {
    const grid = setup();
    grid.select([0, 2]);
    grid.press("c");
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    if (change === "mouse") grid.select([0, 1]);
    else
      fireEvent.keyDown(grid.props.scrollRef.current as HTMLElement, {
        key: change === "clear" ? "Escape" : "ArrowLeft",
        shiftKey: change === "extend",
      });
    expect(screen.queryByRole("alert")).toBeNull();
    expect(getPostedMessages()).toEqual([]);
    if (change === "extend") {
      grid.press("c");
      expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
      expect(getPostedMessages()).toEqual([]);
    }
  });

  it.each([
    "page",
    "refresh",
  ])("clears the copy error on %s instead of carrying it onto other data", (change) => {
    const grid = setup();
    grid.select([0, 2]);
    grid.press("c");
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    grid.view.rerender(<TableGrid {...grid.props} />);
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    grid.view.rerender(
      <TableGrid
        {...grid.props}
        rows={change === "page" ? [...grid.props.rows] : grid.props.rows}
        loading={change === "refresh"}
      />,
    );
    expect(screen.queryByRole("alert")).toBeNull();
    grid.view.rerender(<TableGrid {...grid.props} />);
    grid.select([0, 2]);
    grid.press("c");
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    expect(getPostedMessages()).toEqual([]);
  });

  it.each([
    ["DEFAULT draft cell", [-1, 0], [-1, 0]],
    ["draft range with DEFAULT", [-1, 0], [-1, 2]],
    ["MISSING persisted cell", [0, 2], [0, 2]],
    ["range across MISSING", [0, 0], [1, 2]],
  ] as const)("refuses copy and cut of a %s without writing the clipboard", (_, from, to) => {
    const grid = setup();
    grid.select([...from], [...to]);
    for (const key of ["c", "x"] as const) {
      grid.press(key);
      expect(getPostedMessages()).toEqual([]);
      expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    }
  });

  it("refuses context-menu copy of a range with MISSING cells", () => {
    const grid = setup();
    grid.select([0, 0], [0, 2]);
    grid.contextCopy();
    expect(
      getPostedMessages().filter((m) => m.type === "writeClipboard"),
    ).toEqual([]);
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
  });

  it("still copies NULL, empty and literal NULL/DEFAULT/MISSING text and clears the notice", () => {
    const grid = setup();
    grid.select([0, 2]);
    grid.press("c");
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);

    grid.select([0, 0], [0, 1]);
    grid.press("c");
    expect(grid.copiedText()).toBe("DEFAULT\tMISSING");
    expect(screen.queryByText(BLOCKED)).toBeNull();

    grid.select([1, 0], [1, 2]);
    grid.press("c");
    expect(grid.copiedText()).toBe('"NULL"\tNULL\t""');

    grid.select([-1, 1], [-1, 2]);
    grid.press("x");
    expect(grid.copiedText()).toBe('NULL\t""');
  });
});
