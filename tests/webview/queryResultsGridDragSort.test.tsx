import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TableGrid } from "../../src/webview/components/table/TableGrid";

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
  }),
}));

function headerRect(left: number): DOMRect {
  return {
    x: left,
    y: 0,
    width: 50,
    height: 28,
    left,
    top: 0,
    right: left + 50,
    bottom: 28,
    toJSON: () => ({}),
  } as DOMRect;
}

function renderGrid() {
  render(
    <TableGrid
      mode="query"
      status="success"
      result={{
        columns: ["id", "name"],
        columnMeta: [],
        rows: [
          { __col_0: 1, __col_1: "Charlie" },
          { __col_0: 2, __col_1: "Alice" },
          { __col_0: 3, __col_1: "Bob" },
        ],
        rowCount: 3,
        executionTimeMs: 5,
      }}
    />,
  );
  const table = screen.getByRole("table");
  const header = (name: string) => {
    const th = Array.from(
      table.querySelectorAll<HTMLElement>("thead tr:first-child th"),
    ).find((element) => element.textContent === name);
    if (!th) throw new Error(`Missing header ${name}`);
    return th;
  };
  const names = () =>
    Array.from(table.querySelectorAll("tbody tr"))
      .map(
        (row) =>
          Array.from(row.querySelectorAll("td")).find((td) =>
            ["Alice", "Bob", "Charlie"].includes(td.textContent ?? ""),
          )?.textContent,
      )
      .filter(Boolean);
  const dragNameAfterId = () => {
    header("name").getBoundingClientRect = vi.fn(() => headerRect(0));
    header("id").getBoundingClientRect = vi.fn(() => headerRect(50));
    fireEvent.mouseDown(header("name"), {
      clientX: 25,
      clientY: 14,
      buttons: 1,
    });
    fireEvent.mouseMove(document, { clientX: 90, clientY: 14, buttons: 1 });
    fireEvent.mouseUp(document, { clientX: 90, clientY: 14, buttons: 0 });
  };
  return { header, names, dragNameAfterId };
}

describe("GRID-4 query results header drag does not swallow later sort clicks", () => {
  it("suppresses only the click produced by the drag release", async () => {
    const grid = renderGrid();
    grid.dragNameAfterId();
    fireEvent.click(grid.header("name"));
    expect(grid.names()).toEqual(["Charlie", "Alice", "Bob"]);

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    fireEvent.click(grid.header("name"));
    expect(grid.names()).toEqual(["Alice", "Bob", "Charlie"]);
  });

  it("sorts on the next header activation after a drag released elsewhere", async () => {
    const grid = renderGrid();
    grid.dragNameAfterId();
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    fireEvent.click(grid.header("name"));
    expect(grid.names()).toEqual(["Alice", "Bob", "Charlie"]);
  });
});
