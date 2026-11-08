import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useMemo, useRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { QueryModeTableGrid } from "../../src/webview/components/table/grid/QueryModeTableGrid";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import type { EditTarget } from "../../src/webview/types";
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

type GridKind = "query" | "table";

function GridHarness({
  kind,
  columnCount = 3,
}: {
  kind: GridKind;
  columnCount?: number;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const loadingRef = useRef(false);
  const fetchEpochRef = useRef(0);
  const [editCell, setEditCell] = useState<EditTarget | null>(null);
  const { columns, rows, result } = useMemo(() => {
    const columns: ColumnTypeMeta[] = Array.from(
      { length: columnCount },
      (_, index) => ({
        name: `c${index}`,
        type: "integer",
        nativeType: "integer",
        category: "integer",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
        filterable: true,
        filterOperators: ["eq"],
        valueSemantics: "plain",
      }),
    );
    const rows = [10, 20].map((base) =>
      Object.fromEntries(
        columns.map(({ name }, index) => [name, base + index]),
      ),
    );
    return {
      columns,
      rows,
      result: {
        columns: columns.map(({ name }) => name),
        columnMeta: columns.map(() => ({ category: "integer" as const })),
        rows: rows.map((row) =>
          Object.fromEntries(
            columns.map(({ name }, index) => [`__col_${index}`, row[name]]),
          ),
        ),
        rowCount: rows.length,
        executionTimeMs: 1,
      },
    };
  }, [columnCount]);

  return (
    <>
      <button type="button">Before grid</button>
      {kind === "query" ? (
        <QueryModeTableGrid status="success" result={result} />
      ) : (
        <TableGrid
          canEditRows
          canSelectAndDeleteRows
          colSizes={{}}
          columns={columns}
          editCell={editCell}
          filterDrafts={{}}
          loading={false}
          loadingRef={loadingRef}
          fetchEpochRef={fetchEpochRef}
          newRows={[]}
          onCancelEdit={() => setEditCell(null)}
          onBatchCellEdit={() => undefined}
          onCommitCellEdit={() => setEditCell(null)}
          onCommitDraftCellEdit={() => undefined}
          onMixedBatchEdit={() => undefined}
          onFilterDraftChange={() => undefined}
          onSelectionChange={() => undefined}
          onSort={() => undefined}
          onOpenStructuredCell={() => undefined}
          onStartDraftEdit={() => undefined}
          onStartEdit={(rowIdx, column) =>
            setEditCell({ kind: "persisted", rowIdx, col: column.name })
          }
          pendingEdits={new Map()}
          rows={rows}
          scrollRef={scrollRef}
          selected={new Set()}
          sort={null}
        />
      )}
      <button type="button">After grid</button>
    </>
  );
}

function setupGrid(kind: GridKind, columnCount = 3) {
  const user = userEvent.setup();
  const { container } = render(
    <GridHarness kind={kind} columnCount={columnCount} />,
  );
  const table = container.querySelector("table");
  if (!table?.parentElement) throw new Error("Missing grid scroll container");
  const scroll = table.parentElement;
  scroll.scrollTo = vi.fn();
  for (const cell of table.querySelectorAll<HTMLElement>("td")) {
    cell.scrollIntoView = vi.fn();
  }
  const offset = kind === "table" ? 1 : 0;
  const cell = (col: number, row = 1) => {
    const element = table.querySelector<HTMLElement>(
      `td[data-row="${row}"][data-col="${col + offset}"]`,
    );
    if (!element) throw new Error(`Missing cell ${row}:${col}`);
    return element;
  };
  const select = (col: number) => {
    fireEvent.mouseDown(cell(col), { button: 0 });
    fireEvent.mouseUp(document);
    expect(document.activeElement).toBe(scroll);
  };
  const anchorClass =
    kind === "table" ? "rdb-tcell-anchor" : "rdb-rcell-anchor";
  const expectAnchor = (col: number) => {
    expect(cell(col).classList.contains(anchorClass)).toBe(true);
  };
  const controls = Array.from(
    scroll.querySelectorAll<HTMLButtonElement | HTMLInputElement>(
      "button,input",
    ),
  ).filter((control) => control.tabIndex >= 0 && !control.disabled);
  return { user, scroll, cell, select, expectAnchor, controls };
}

describe.each(["query", "table"] as const)("%s grid Tab navigation", (kind) => {
  it("keeps Tab/Shift+Tab inside the current row until a column boundary", async () => {
    const { user, scroll, select, expectAnchor } = setupGrid(kind);
    select(0);
    await user.tab();
    expect(document.activeElement).toBe(scroll);
    expectAnchor(1);
    await user.tab();
    expect(document.activeElement).toBe(scroll);
    expectAnchor(2);
    await user.tab({ shift: true });
    expect(document.activeElement).toBe(scroll);
    expectAnchor(1);
    await user.tab({ shift: true });
    expect(document.activeElement).toBe(scroll);
    expectAnchor(0);
  });

  it("does not cancel native Tab at either boundary or change the selection", () => {
    const { scroll, select, expectAnchor } = setupGrid(kind);
    select(2);
    expect(fireEvent.keyDown(scroll, { key: "Tab" })).toBe(true);
    expectAnchor(2);
    select(0);
    expect(fireEvent.keyDown(scroll, { key: "Tab", shiftKey: true })).toBe(
      true,
    );
    expectAnchor(0);
  });

  it.each([
    1, 3,
  ])("tabs through controls and out (%i columns)", async (columnCount) => {
    const { user, select, expectAnchor, controls } = setupGrid(
      kind,
      columnCount,
    );
    select(columnCount - 1);
    expect(controls.length).toBeGreaterThan(0);
    for (const control of controls) {
      await user.tab();
      expect(document.activeElement).toBe(control);
      expectAnchor(columnCount - 1);
    }
    await user.tab();
    expect(document.activeElement).toBe(
      screen.getByRole("button", { name: "After grid" }),
    );
    expectAnchor(columnCount - 1);
  });

  it.each([
    1, 3,
  ])("tabs backwards out from the first column (%i columns)", async (columnCount) => {
    const { user, scroll, select, expectAnchor } = setupGrid(kind, columnCount);
    select(0);
    await user.tab({ shift: true });
    expect(scroll.contains(document.activeElement)).toBe(false);
    expect(document.activeElement).toBe(
      kind === "query"
        ? screen.getByTitle("Export results as JSON file")
        : screen.getByRole("button", { name: "Before grid" }),
    );
    expectAnchor(0);
  });

  it("does not hijack native Tab/Shift+Tab on nested controls with an interior selection", async () => {
    const { user, select, expectAnchor, controls } = setupGrid(kind);
    select(1);
    controls[0].focus();
    await user.tab();
    expect(document.activeElement).toBe(controls[1]);
    expectAnchor(1);
    await user.tab({ shift: true });
    expect(document.activeElement).toBe(controls[0]);
    expectAnchor(1);
  });

  it("preserves range copy, Tab selection collapse, and native editing focus", async () => {
    const { user, scroll, cell, select, expectAnchor } = setupGrid(kind);
    select(0);
    await user.keyboard("{Shift>}{ArrowRight}{/Shift}");
    clearPostedMessages();
    fireEvent.keyDown(scroll, { key: "c", ctrlKey: true });
    expect(getLastPostedMessage()).toEqual({
      type: "writeClipboard",
      payload: { text: "20\t21" },
    });
    await user.tab();
    expectAnchor(2);
    clearPostedMessages();
    fireEvent.keyDown(scroll, { key: "c", ctrlKey: true });
    expect(getLastPostedMessage()).toEqual({
      type: "writeClipboard",
      payload: { text: "22" },
    });
    await user.keyboard("{F2}");
    const input = cell(2).querySelector("input");
    if (!input) throw new Error("Missing cell editor");
    expect(document.activeElement).toBe(input);
    expect(fireEvent.keyDown(input, { key: "Tab" })).toBe(true);
    expect(fireEvent.keyDown(input, { key: "Tab", shiftKey: true })).toBe(true);
  });
});
