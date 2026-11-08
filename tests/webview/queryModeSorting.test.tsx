import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { QueryModeTableGrid } from "../../src/webview/components/table/grid/QueryModeTableGrid";
import type { QueryResult } from "../../src/webview/store";
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

const result: QueryResult = {
  columns: ["n", "label"],
  columnMeta: [{ category: "integer" }, { category: "text" }],
  rows: [
    { __col_0: 2, __col_1: "βeta" },
    { __col_0: 1, __col_1: "<script>alert(1)</script>" },
    { __col_0: 3, __col_1: "猫🙂" },
  ],
  rowCount: 3,
  executionTimeMs: 1,
};

function readNumbers(container: HTMLElement) {
  return Array.from(
    container.querySelectorAll('tbody td[data-column-id="__col_0"]'),
    (cell) => cell.textContent,
  );
}

describe("query mode header sorting gestures", () => {
  it("sorts with Enter and Space and announces the current direction", async () => {
    const user = userEvent.setup();
    const { container } = render(
      <QueryModeTableGrid status="success" result={result} />,
    );
    const sortButton = screen.getByRole("button", { name: "Sort by n" });
    sortButton.focus();
    await user.keyboard("{Enter}");
    expect(readNumbers(container)).toEqual(["3", "2", "1"]);
    expect(sortButton.closest("th")?.getAttribute("aria-sort")).toBe(
      "descending",
    );
    await user.keyboard(" ");
    expect(readNumbers(container)).toEqual(["1", "2", "3"]);
    expect(sortButton.closest("th")?.getAttribute("aria-sort")).toBe(
      "ascending",
    );
  });

  it("does not start a column drag when the sort button is pressed", () => {
    const { container } = render(
      <QueryModeTableGrid status="success" result={result} />,
    );
    const sortButton = screen.getByRole("button", { name: "Sort by n" });
    fireEvent.mouseDown(sortButton, { button: 0, clientX: 20 });
    fireEvent.mouseMove(document, { clientX: 30 });
    expect(sortButton.closest("th")?.hasAttribute("data-column-dragging")).toBe(
      false,
    );
    fireEvent.mouseUp(document);
    fireEvent.click(sortButton);
    expect(readNumbers(container)).toEqual(["3", "2", "1"]);
  });

  it("keeps ascending/descending state in the parent and exports the same sort", async () => {
    const user = userEvent.setup();
    const { container } = render(
      <QueryModeTableGrid status="success" result={result} />,
    );
    expect(readNumbers(container)).toEqual(["2", "1", "3"]);
    await user.click(screen.getByText("n"));
    expect(readNumbers(container)).toEqual(["3", "2", "1"]);
    await user.click(screen.getByText("n"));
    expect(readNumbers(container)).toEqual(["1", "2", "3"]);
    clearPostedMessages();
    await user.click(screen.getByTitle("Export results as JSON file"));
    expect(getLastPostedMessage()?.payload).toMatchObject({
      sort: [{ id: "__col_0", desc: false }],
    });
    await user.click(screen.getByText("n"));
    expect(readNumbers(container)).toEqual(["2", "1", "3"]);
  });

  it("allows sorting after a column drag without treating the drag as a click", async () => {
    const user = userEvent.setup();
    const { container } = render(
      <QueryModeTableGrid status="success" result={result} />,
    );
    const header = screen.getByText("n").closest("th");
    if (!header) throw new Error("Missing numeric column header");
    fireEvent.mouseDown(header, { button: 0, clientX: 20 });
    fireEvent.mouseMove(document, { clientX: 30 });
    fireEvent.mouseUp(document);
    fireEvent.click(header);
    expect(readNumbers(container)).toEqual(["2", "1", "3"]);
    await user.click(screen.getByText("n"));
    expect(readNumbers(container)).toEqual(["3", "2", "1"]);
  });

  it("sorts the selected result set and exports its index with the sorting state", async () => {
    const user = userEvent.setup();
    const { container } = render(
      <QueryModeTableGrid
        status="success"
        result={{
          ...result,
          resultSets: [
            { ...result, id: "first", columns: [...result.columns] },
            {
              ...result,
              id: "second",
              columns: [...result.columns],
              rows: [...result.rows].reverse(),
            },
          ],
        }}
      />,
    );
    await user.click(screen.getByRole("tab", { name: "Result 2 (3)" }));
    expect(readNumbers(container)).toEqual(["3", "1", "2"]);
    await user.click(screen.getByText("n"));
    expect(readNumbers(container)).toEqual(["3", "2", "1"]);
    await user.click(screen.getByText("n"));
    expect(readNumbers(container)).toEqual(["1", "2", "3"]);
    clearPostedMessages();
    await user.click(screen.getByTitle(/^Export results as CSV file\./));
    expect(getLastPostedMessage()?.payload).toMatchObject({
      resultSetIndex: 1,
      sort: [{ id: "__col_0", desc: false }],
    });
    await user.click(screen.getByRole("tab", { name: "Result 1 (3)" }));
    expect(readNumbers(container)).toEqual(["2", "1", "3"]);
  });
});
