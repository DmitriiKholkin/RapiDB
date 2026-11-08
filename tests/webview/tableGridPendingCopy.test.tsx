import { fireEvent, render } from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import type { PendingEdits } from "../../src/webview/types";
import { clearPostedMessages, getLastPostedMessage } from "./testUtils";

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
  nativeType: name === "b" ? "string" : "text",
  category: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq"],
  valueSemantics: "plain",
}));

function setup(rows: Record<string, unknown>[], pendingEdits: PendingEdits) {
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
    pendingEdits,
    rows,
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
  const select = (from: [number, number], to?: [number, number]) => {
    fireEvent.mouseDown(cell(...from), { button: 0 });
    fireEvent.mouseUp(document);
    if (to) {
      fireEvent.mouseDown(cell(...to), { button: 0, shiftKey: true });
      fireEvent.mouseUp(document);
    }
  };
  const copy = () => {
    clearPostedMessages();
    fireEvent.keyDown(props.scrollRef.current as HTMLElement, {
      key: "c",
      ctrlKey: true,
    });
    const message = getLastPostedMessage();
    expect(message?.type).toBe("writeClipboard");
    return (message?.payload as { text: string }).text;
  };
  return { ...view, props, cell, select, copy };
}

describe("TableGrid pending clipboard values", () => {
  it("copies current structured and binary values in single cells and ranges", () => {
    const binary = {
      __rapidbRedisBinary: 1,
      binaryHex: "ff",
      displayValue: "RedisBinary(0xff)",
    };
    const grid = setup(
      [{ a: "old", b: "old", c: "old" }],
      new Map([
        [
          0,
          new Map<string, unknown>([
            ["a", { x: 1 }],
            ["b", binary],
            ["c", null],
          ]),
        ],
      ]),
    );
    grid.select([0, 0]);
    expect(grid.copy()).toBe('"{""x"":1}"');
    grid.select([0, 1]);
    expect(grid.cell(0, 1).textContent).toContain("RedisBinary(0xff)");
    expect(grid.copy()).toBe("RedisBinary(0xff)");
    grid.select([0, 0], [0, 2]);
    expect(grid.copy()).toBe('"{""x"":1}"\tRedisBinary(0xff)\tNULL');
  });

  it("copies missing and inherited fields as NULL while retaining pending null, empty and falsy values", () => {
    const row = Object.assign(Object.create({ a: "inherited" }), {
      b: "old",
      c: "old",
    });
    const grid = setup(
      [row, { a: "old", b: "old" }],
      new Map([
        [
          0,
          new Map<string, unknown>([
            ["b", null],
            ["c", false],
          ]),
        ],
        [
          1,
          new Map<string, unknown>([
            ["a", 0],
            ["b", ""],
          ]),
        ],
      ]),
    );
    expect(grid.cell(0, 0).textContent).toContain("MISSING");
    grid.select([0, 0], [1, 2]);
    expect(grid.copy()).toBe('NULL\tNULL\tfalse\n0\t""\tNULL');
  });

  it("uses the current row indexes and pending values after externally sorted rows change", () => {
    const grid = setup(
      [{ a: "first" }, { a: "second" }],
      new Map([[0, new Map([["a", "first edit"]])]]),
    );
    grid.select([0, 0]);
    expect(grid.copy()).toBe("first edit");
    grid.rerender(
      <TableGrid
        {...grid.props}
        rows={[{ a: "second" }, { a: "first" }]}
        sort={{ column: "a", direction: "desc" }}
        pendingEdits={new Map([[1, new Map([["a", "first sorted edit"]])]])}
      />,
    );
    grid.select([0, 0], [1, 0]);
    expect(grid.copy()).toBe("second\nfirst sorted edit");
  });
});
