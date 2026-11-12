import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { QueryModeTableGrid } from "../../src/webview/components/table/grid/QueryModeTableGrid";
import { useQueryStore } from "../../src/webview/store";

describe("MSSQL ambiguous affected row total", () => {
  it("does not present the numeric rowCount placeholder as zero rows affected", () => {
    const result = {
      columns: [],
      columnMeta: [],
      rows: [],
      rowCount: 0,
      affectedRowsUnknown: true,
      executionTimeMs: 3,
    };
    // Store normalization must preserve the marker received from the driver.
    useQueryStore.getState().setResult(result);
    render(
      <QueryModeTableGrid
        status="success"
        result={useQueryStore.getState().result}
      />,
    );
    expect(screen.getByText("Query executed successfully")).toBeTruthy();
    expect(screen.getByText(/Affected row count unavailable/)).toBeTruthy();
    expect(screen.queryByText(/0 rows affected/)).toBeNull();
  });

  it("keeps exact ordinary DML counts visible", () => {
    render(
      <QueryModeTableGrid
        status="success"
        result={{
          columns: [],
          columnMeta: [],
          rows: [],
          rowCount: 5,
          executionTimeMs: 3,
        }}
      />,
    );
    expect(screen.getByText(/5 rows affected/)).toBeTruthy();
  });

  it.each([
    false,
    true,
  ])("keeps the script-level uncertainty visible with multiple/GO rowsets (partial=%s)", (partial) => {
    const sets = [1, 2].map((index) => ({
      id: `result-${index}`,
      columns: ["v"],
      columnMeta: [],
      rows: [{ __col_0: index }],
      rowCount: 1,
      ...(partial && index === 2 ? { partial: true } : {}),
    }));
    render(
      <QueryModeTableGrid
        status={partial ? "error" : "success"}
        result={{
          columns: ["v"],
          columnMeta: [],
          rows: sets[0].rows,
          resultSets: sets,
          rowCount: 2,
          affectedRowsUnknown: true,
          executionTimeMs: 3,
          ...(partial ? { error: "later failure" } : {}),
        }}
      />,
    );
    expect(
      screen.getByText(/1 row · Affected row count unavailable/),
    ).toBeTruthy();
    fireEvent.click(screen.getByRole("tab", { name: /^Result 2/ }));
    expect(
      screen.getByText(/1 row · Affected row count unavailable/),
    ).toBeTruthy();
    expect(screen.queryByText(/0 rows affected/)).toBeNull();
    if (partial)
      expect(screen.getByRole("alert").textContent).toContain("later failure");
  });
});
