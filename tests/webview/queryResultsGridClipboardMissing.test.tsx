import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { QueryResultsGrid } from "../../src/webview/components/table/grid/QueryResultsGrid";
import type { QueryResult } from "../../src/webview/store";
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

const BLOCKED = /Copy blocked: the selection contains MISSING cells/;

const result: QueryResult = {
  columns: ["a", "b", "c"],
  columnMeta: [
    { category: "text" },
    { category: "text" },
    { category: "text" },
  ],
  rows: [
    { __col_0: "MISSING", __col_1: null, __col_2: "NULL" },
    { __col_1: "DEFAULT", __col_2: "" },
    { __col_0: "z" },
  ],
  missingCells: [{ rowIndex: 1, columnIndex: 0 }],
  rowCount: 3,
  executionTimeMs: 1,
};

function Harness({ data = result }: { data?: QueryResult }) {
  const [columnOrder, setColumnOrder] = useState([
    "__col_0",
    "__col_1",
    "__col_2",
  ]);
  const [sorting, setSorting] = useState<
    React.ComponentProps<typeof QueryResultsGrid>["sorting"]
  >([]);
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});
  return (
    <QueryResultsGrid
      result={data}
      columnOrder={columnOrder}
      onColumnOrderChange={setColumnOrder}
      sorting={sorting}
      onSortingChange={setSorting}
      columnSizing={columnSizing}
      onColumnSizingChange={setColumnSizing}
    />
  );
}

function setup() {
  const view = render(<Harness />);
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
    return cell(...to);
  };
  const press = (target: HTMLElement, key: "c" | "x") => {
    clearPostedMessages();
    fireEvent.keyDown(target, { key, ctrlKey: true });
  };
  const copy = (from: [number, number], to?: [number, number]) => {
    press(select(from, to), "c");
    const message = getLastPostedMessage();
    expect(message?.type).toBe("writeClipboard");
    return (message?.payload as { text: string }).text;
  };
  return {
    cell,
    select,
    press,
    copy,
    rerenderResult: (data: QueryResult) =>
      view.rerender(<Harness data={data} />),
  };
}

describe("QueryResultsGrid copy of MISSING cells", () => {
  it.each([
    "mouse",
    "keyboard",
    "clear",
    "extend",
  ] as const)("clears a stale copy error when selection changes via %s", (change) => {
    const grid = setup();
    const target = grid.select([1, 0]);
    grid.press(target, "c");
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    if (change === "mouse") grid.select([0, 1]);
    else
      fireEvent.keyDown(target, {
        key: change === "clear" ? "Escape" : "ArrowRight",
        shiftKey: change === "extend",
      });
    expect(screen.queryByRole("alert")).toBeNull();
    expect(getPostedMessages()).toEqual([]);
    if (change === "extend") {
      grid.press(target, "c");
      expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
      expect(getPostedMessages()).toEqual([]);
    }
  });

  it("clears a stale copy error on a new result, even when row identities are reused", () => {
    const grid = setup();
    grid.press(grid.select([1, 0]), "c");
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    grid.rerenderResult(result);
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    grid.rerenderResult({ ...result, missingCells: [] });
    expect(screen.queryByRole("alert")).toBeNull();
    expect(grid.copy([1, 0])).toBe("NULL");
  });

  it.each([
    ["MISSING cell", [1, 0], [1, 0]],
    ["range across MISSING", [0, 0], [2, 2]],
  ] as const)("refuses copy and cut of a %s without writing the clipboard", (_, from, to) => {
    const grid = setup();
    const target = grid.select([...from], [...to]);
    for (const key of ["c", "x"] as const) {
      grid.press(target, key);
      expect(getPostedMessages()).toEqual([]);
      expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
    }
  });

  it("refuses context-menu copy of a range with MISSING cells", () => {
    const grid = setup();
    grid.select([0, 0], [1, 0]);
    fireEvent.contextMenu(grid.cell(1, 0));
    clearPostedMessages();
    fireEvent.click(screen.getByRole("menuitem", { name: /copy/i }));
    expect(
      getPostedMessages().filter((m) => m.type === "writeClipboard"),
    ).toEqual([]);
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);
  });

  it("still copies NULL, empty and literal NULL/DEFAULT/MISSING text and clears the notice", () => {
    const grid = setup();
    grid.press(grid.select([1, 0]), "c");
    expect(screen.getByRole("alert").textContent).toMatch(BLOCKED);

    expect(grid.copy([0, 0], [0, 2])).toBe('MISSING\tNULL\t"NULL"');
    expect(screen.queryByText(BLOCKED)).toBeNull();
    expect(grid.copy([1, 1], [1, 2])).toBe('DEFAULT\t""');
    expect(grid.copy([2, 0], [2, 2])).toBe("z\tNULL\tNULL");
  });
});
