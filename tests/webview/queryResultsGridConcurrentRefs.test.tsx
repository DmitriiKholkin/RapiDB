import type { SortingState } from "@tanstack/react-table";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { Suspense, startTransition, useRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { QueryResultsGrid } from "../../src/webview/components/table/grid/QueryResultsGrid";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import type { QueryResult } from "../../src/webview/store";
import type { Row } from "../../src/webview/types";
import { clearPostedMessages, getLastPostedMessage } from "./testUtils";

const cellGate = vi.hoisted(() => ({
  valueToSuspend: null as unknown,
  promise: new Promise<never>(() => {}),
}));

vi.mock("../../src/webview/components/table/CellDisplay", () => ({
  CellDisplay: ({ value }: { value: unknown }) => {
    if (value === cellGate.valueToSuspend) {
      throw cellGate.promise;
    }
    return <span>{String(value)}</span>;
  },
}));

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
    scrollToIndex: vi.fn(),
  }),
}));

const initialResult: QueryResult = {
  columns: ["value"],
  columnMeta: [],
  rows: [{ __col_0: "committed row" }],
  rowCount: 1,
  executionTimeMs: 1,
};

const pendingResult: QueryResult = {
  ...initialResult,
  rows: [{ __col_0: "uncommitted row" }],
};

function QueryGridHarness() {
  const [result, setResult] = useState(initialResult);
  const [columnOrder, setColumnOrder] = useState(["__col_0"]);
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});

  updateResult = (nextResult) => {
    startTransition(() => setResult(nextResult));
  };

  return (
    <Suspense fallback={<div>Loading results</div>}>
      <QueryResultsGrid
        result={result}
        columnOrder={columnOrder}
        onColumnOrderChange={setColumnOrder}
        sorting={sorting}
        onSortingChange={setSorting}
        columnSizing={columnSizing}
        onColumnSizingChange={setColumnSizing}
      />
    </Suspense>
  );
}

let updateResult: ((result: QueryResult) => void) | undefined;
let updateTableRows: ((rows: Row[]) => void) | undefined;

const tableColumn: ColumnTypeMeta = {
  name: "value",
  type: "text",
  category: "text",
  nativeType: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: false,
  filterOperators: [],
  valueSemantics: "plain",
};

function TableGridHarness() {
  const [rows, setRows] = useState<Row[]>([{ value: "committed row" }]);
  const scrollRef = useRef<HTMLDivElement>(null);

  updateTableRows = (nextRows) => {
    startTransition(() => setRows(nextRows));
  };

  return (
    <Suspense fallback={<div>Loading table</div>}>
      <TableGrid
        canEditRows={false}
        canSelectAndDeleteRows={false}
        colSizes={{ value: 160 }}
        columns={[tableColumn]}
        editCell={null}
        filterDrafts={{}}
        loading={false}
        loadingRef={{ current: false }}
        fetchEpochRef={{ current: 0 }}
        newRows={[]}
        onCancelEdit={() => undefined}
        onBatchCellEdit={() => undefined}
        onCommitCellEdit={() => undefined}
        onCommitDraftCellEdit={() => undefined}
        onMixedBatchEdit={() => undefined}
        onFilterDraftChange={() => undefined}
        onSelectionChange={() => undefined}
        onSort={() => undefined}
        onOpenStructuredCell={() => undefined}
        onStartDraftEdit={() => undefined}
        onStartEdit={() => undefined}
        pendingEdits={new Map()}
        rows={rows}
        scrollRef={scrollRef}
        selected={new Set()}
        sort={null}
      />
    </Suspense>
  );
}

describe("table grid concurrent render refs", () => {
  it("keeps clipboard reads aligned with the committed rows while a transition suspends", () => {
    cellGate.valueToSuspend = "uncommitted row";
    updateResult = undefined;
    const { container } = render(<QueryGridHarness />);

    fireEvent.mouseDown(screen.getByRole("cell"), { button: 0 });
    act(() => updateResult?.(pendingResult));

    expect(screen.getByText("committed row")).toBeTruthy();
    expect(screen.queryByText("Loading results")).toBeNull();

    clearPostedMessages();
    fireEvent.keyDown(
      container.querySelector('[tabindex="0"]') as HTMLElement,
      {
        key: "c",
        ctrlKey: true,
      },
    );

    expect(getLastPostedMessage()).toEqual({
      type: "writeClipboard",
      payload: { text: "committed row" },
    });
  });

  it("keeps table-mode clipboard reads aligned with committed rows while suspended", () => {
    cellGate.valueToSuspend = "uncommitted row";
    updateTableRows = undefined;
    const { container } = render(<TableGridHarness />);

    fireEvent.mouseDown(screen.getByRole("cell"), { button: 0 });
    act(() => updateTableRows?.([{ value: "uncommitted row" }]));

    expect(screen.getByText("committed row")).toBeTruthy();
    expect(screen.queryByText("Loading table")).toBeNull();

    clearPostedMessages();
    fireEvent.keyDown(
      container.querySelector('[tabindex="0"]') as HTMLElement,
      { key: "c", ctrlKey: true },
    );

    expect(getLastPostedMessage()).toEqual({
      type: "writeClipboard",
      payload: { text: "committed row" },
    });
  });
});
