import type { SortingState } from "@tanstack/react-table";
import { fireEvent, render } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { compareExactNumericValues } from "../../src/shared/exactNumericComparison";
import { QueryResultsGrid } from "../../src/webview/components/table/grid/QueryResultsGrid";
import type { QueryResult } from "../../src/webview/store";

vi.mock("../../src/webview/components/table/CellDisplay", () => ({
  CellDisplay: ({ value }: { value: unknown }) => <span>{String(value)}</span>,
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
    scrollToIndex: () => undefined,
  }),
}));

const decimalResult: QueryResult = {
  columns: ["amount"],
  columnMeta: [{ category: "decimal" }],
  rows: [{ __col_0: "10" }, { __col_0: "2" }],
  rowCount: 2,
  executionTimeMs: 1,
};

function QueryResultsGridHarness({ result }: { result: QueryResult }) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnOrder, setColumnOrder] = useState(["__col_0"]);
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});

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

function readRowValues(container: HTMLElement): string[] {
  return Array.from(
    container.querySelectorAll("tr[data-even]"),
    (row) => row.textContent?.trim() ?? "",
  );
}

describe("Query Results exact numeric sorting", () => {
  it("sorts text case-insensitively and naturally when the grid detects alphanumeric values", () => {
    const values = [
      "item10",
      "item2",
      "A",
      "a",
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
      "item3",
    ];
    const result: QueryResult = {
      ...decimalResult,
      columnMeta: [{ category: "text" }],
      rows: values.map((value) => ({ __col_0: value })),
      rowCount: values.length,
    };
    const { container } = render(<QueryResultsGridHarness result={result} />);
    const header = container.querySelector(
      'th[data-column-id="__col_0"]',
    ) as HTMLTableCellElement;
    fireEvent.click(header);
    expect(readRowValues(container)).toEqual([
      "A",
      "a",
      "item2",
      "item3",
      "item10",
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
    ]);
    fireEvent.click(header);
    expect(readRowValues(container)).toEqual([
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
      "item10",
      "item3",
      "item2",
      "A",
      "a",
    ]);
  });

  it("sorts exact decimal strings numerically in both directions", () => {
    const { container } = render(
      <QueryResultsGridHarness result={decimalResult} />,
    );
    const header = container.querySelector('th[data-column-id="__col_0"]');

    expect(header).not.toBeNull();
    fireEvent.click(header as HTMLTableCellElement);
    expect(readRowValues(container)).toEqual(["2", "10"]);

    fireEvent.click(header as HTMLTableCellElement);
    expect(readRowValues(container)).toEqual(["10", "2"]);
  });

  it("keeps large exact integer digits and compares their numeric order", () => {
    const result: QueryResult = {
      ...decimalResult,
      columns: ["value"],
      columnMeta: [{ category: "integer" }],
      rows: [
        { __col_0: "1234567890123456789" },
        { __col_0: "9007199254740993" },
      ],
    };
    const { container } = render(<QueryResultsGridHarness result={result} />);
    const header = container.querySelector(
      'th[data-column-id="__col_0"]',
    ) as HTMLTableCellElement;

    fireEvent.click(header);
    expect(readRowValues(container)).toEqual([
      "9007199254740993",
      "1234567890123456789",
    ]);

    fireEvent.click(header);
    expect(readRowValues(container)).toEqual([
      "1234567890123456789",
      "9007199254740993",
    ]);
  });

  it("uses the shared numeric null/undefined order in both directions", () => {
    const result: QueryResult = {
      ...decimalResult,
      rows: [
        { __col_0: "10" },
        { __col_0: undefined },
        { __col_0: "2" },
        { __col_0: null },
      ],
      rowCount: 4,
    };
    const { container } = render(<QueryResultsGridHarness result={result} />);
    const header = container.querySelector(
      'th[data-column-id="__col_0"]',
    ) as HTMLTableCellElement;
    fireEvent.click(header);
    expect(readRowValues(container)).toEqual(["undefined", "null", "2", "10"]);
    fireEvent.click(header);
    expect(readRowValues(container)).toEqual(["10", "2", "undefined", "null"]);
  });

  it("does not guess that numeric-looking text is a number without metadata", () => {
    const result: QueryResult = {
      ...decimalResult,
      columnMeta: [{ category: null }],
      rows: [{ __col_0: "2" }, { __col_0: "10" }],
    };
    const { container } = render(<QueryResultsGridHarness result={result} />);
    const header = container.querySelector(
      'th[data-column-id="__col_0"]',
    ) as HTMLTableCellElement;

    fireEvent.click(header);
    expect(readRowValues(container)).toEqual(["10", "2"]);
  });

  it("sorts mixed SQLite integer and real values using exact numeric comparison", () => {
    const result: QueryResult = {
      ...decimalResult,
      columnMeta: [{ category: "decimal" }],
      rows: [{ __col_0: "9007199254740993" }, { __col_0: 9007199254740992 }],
    };
    const { container } = render(<QueryResultsGridHarness result={result} />);
    const header = container.querySelector(
      'th[data-column-id="__col_0"]',
    ) as HTMLTableCellElement;

    fireEvent.click(header);
    expect(readRowValues(container)).toEqual([
      "9007199254740992",
      "9007199254740993",
    ]);
  });

  it.each([
    ["-10", "-2", -1],
    ["1.25", "1.2500", 0],
    ["0.001", "0.01", -1],
    ["9e19", "100000000000000000000", -1],
    ["-2.5e2", "-249", -1],
    [9007199254740993n, "9007199254740992", 1],
    [Number.NEGATIVE_INFINITY, 0, -1],
    [0, Number.POSITIVE_INFINITY, -1],
    ["-Infinity", "-100000000000000000000", -1],
    ["NaN", "Infinity", 1],
    ["$1,234.56", "€1.234,56", 0],
    ["($1,234.56)", "-1234.56", 0],
    ["1 234,56 €", "1234.56", 0],
    ["5-", "-5", 0],
    ["2024-01-05", "-20240105", 1],
    ["v2", "2", 1],
    ["1-2", "-12", 1],
    ["$0.125", "0.125", 0],
    ["€0,1234", "0.1234", 0],
    ["$1,2345", "1.2345", 0],
    ["$1234,567", "1234.567", 0],
    ["$1,234", "1234", 0],
    ["€1.234.567", "1234567", 0],
  ])("compares exact numeric tokens %s and %s", (left, right, expected) => {
    expect(compareExactNumericValues(left, right)).toBe(expected);
  });
});
