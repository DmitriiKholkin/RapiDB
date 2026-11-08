import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { QueryModeTableGrid } from "../../src/webview/components/table/grid/QueryModeTableGrid";
import type { QueryResult } from "../../src/webview/store";
import { clearPostedMessages, getLastPostedMessage } from "./testUtils";

vi.mock("../../src/webview/components/table/grid/QueryResultsGrid", () => ({
  QueryEmptyState: ({ primary }: { primary: string }) => <div>{primary}</div>,
  QueryResultsGrid: ({
    result,
    columnOrder,
    onColumnSizingChange,
  }: {
    result: QueryResult;
    columnOrder: string[];
    onColumnOrderChange: (value: string[]) => void;
    sorting: unknown[];
    onSortingChange: (value: unknown[]) => void;
    columnSizing: Record<string, number>;
    onColumnSizingChange: (value: Record<string, number>) => void;
  }) => (
    <div data-testid="query-result-grid">
      {result.columns.join(",")}: {JSON.stringify(result.rows)}
      <button
        type="button"
        onClick={() =>
          onColumnSizingChange(
            Object.fromEntries(columnOrder.map((id) => [id, 1])),
          )
        }
      >
        Collapse all columns
      </button>
    </div>
  ),
  QuerySpinner: () => <div>Executing…</div>,
}));

const result: QueryResult = {
  operationId: "operation-1",
  connectionId: "mssql-qa",
  requestToken: 1,
  columns: ["first_value"],
  columnMeta: [],
  rows: [{ __col_0: 1 }],
  rowCount: 3,
  executionTimeMs: 12,
  error: "Divide by zero error encountered. (SQL Server line 2)",
  resultSets: [
    {
      id: "result-1",
      columns: ["first_value"],
      rows: [{ __col_0: 1 }],
      rowCount: 1,
    },
    {
      id: "result-2",
      columns: ["second_value"],
      rows: [{ __col_0: 2 }],
      rowCount: 1,
      partial: true,
    },
    {
      id: "result-3",
      columns: ["third_value"],
      rows: [{ __col_0: 3 }],
      rowCount: 1,
    },
  ],
};

describe("QueryModeTableGrid result sets", () => {
  it("shows sets before and after an error, marking the failing set partial", () => {
    render(<QueryModeTableGrid status="success" result={result} />);

    expect(screen.getByRole("alert").textContent).toContain(
      "Divide by zero error encountered. (SQL Server line 2)",
    );
    expect(screen.getByRole("alert").textContent).toContain(
      "received before the error are shown below",
    );
    expect(screen.getByRole("alert").textContent).toContain(
      "marked partial may contain only rows received",
    );
    expect(screen.getByTestId("query-result-grid").textContent).toContain(
      'first_value: [{"__col_0":1}]',
    );

    fireEvent.click(screen.getByRole("tab", { name: "Result 2 (partial, 1)" }));

    expect(screen.getByTestId("query-result-grid").textContent).toContain(
      'second_value: [{"__col_0":2}]',
    );
    fireEvent.click(screen.getByRole("tab", { name: "Result 3 (1)" }));
    expect(screen.getByTestId("query-result-grid").textContent).toContain(
      'third_value: [{"__col_0":3}]',
    );
  });

  it("exports the currently selected result set", () => {
    render(<QueryModeTableGrid status="success" result={result} />);
    fireEvent.click(screen.getByRole("tab", { name: "Result 2 (partial, 1)" }));
    clearPostedMessages();

    fireEvent.click(screen.getByTitle("Export results as JSON file"));

    expect(getLastPostedMessage()).toEqual({
      type: "exportResultsJSON",
      payload: {
        columnOrder: ["__col_0"],
        resultSetIndex: 1,
      },
    });
  });

  it("sends an empty column order when every column is collapsed", () => {
    render(
      <QueryModeTableGrid
        status="success"
        result={{
          ...result,
          error: undefined,
          resultSets: undefined,
          columns: ["first_value", "second_value"],
          rows: [{ __col_0: 1, __col_1: 2 }],
        }}
      />,
    );
    fireEvent.click(
      screen.getByRole("button", { name: "Collapse all columns" }),
    );
    clearPostedMessages();

    fireEvent.click(screen.getByTitle("Export results as JSON file"));

    expect(getLastPostedMessage()).toEqual({
      type: "exportResultsJSON",
      payload: { columnOrder: [] },
    });
  });

  it("shows when result sets were omitted by the metadata limit", () => {
    render(
      <QueryModeTableGrid
        status="success"
        result={{ ...result, error: undefined, resultSetsTruncated: true }}
      />,
    );

    expect(screen.getByRole("status").textContent).toContain(
      "Some result sets were omitted",
    );
  });

  it("selects and exports the last of 100 result sets", () => {
    const sets = Array.from({ length: 100 }, (_, index) => ({
      id: `result-${index + 1}`,
      columns: ["value"],
      rows: [{ __col_0: index + 1 }],
      rowCount: 1,
    }));
    render(
      <QueryModeTableGrid
        status="success"
        result={{ ...result, error: undefined, resultSets: sets }}
      />,
    );
    fireEvent.click(screen.getByRole("tab", { name: "Result 100 (1)" }));
    expect(screen.getByTestId("query-result-grid").textContent).toContain(
      'value: [{"__col_0":100}]',
    );
    clearPostedMessages();
    fireEvent.click(screen.getByTitle("Export results as JSON file"));
    expect(getLastPostedMessage()).toEqual({
      type: "exportResultsJSON",
      payload: { columnOrder: ["__col_0"], resultSetIndex: 99 },
    });
  });
});
