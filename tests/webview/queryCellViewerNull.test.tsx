import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TableGrid } from "../../src/webview/components/table/TableGrid";

vi.mock("@tanstack/react-virtual", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@tanstack/react-virtual")>();
  return {
    ...actual,
    useVirtualizer: (options: Parameters<typeof actual.useVirtualizer>[0]) =>
      actual.useVirtualizer({
        ...options,
        initialRect: { width: 800, height: 260 },
        observeElementRect: (_instance, callback) => {
          callback({ width: 800, height: 260 });
          return () => undefined;
        },
      }),
  };
});

vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue,
    ariaLabel,
  }: {
    initialValue: string;
    ariaLabel: string;
  }) => <textarea aria-label={ariaLabel} value={initialValue} readOnly />,
}));

describe("PNL-5 query cell viewer distinguishes NULL from empty text", () => {
  it.each([
    [null, "NULL", "name is NULL (no value)."],
    [undefined, "NULL", "name is NULL (no value)."],
    ["", "", "Text data in name."],
    ["NULL", "NULL", "Text data in name."],
  ])("opens %j with value %j and description %j", (value, expectedText, description) => {
    const { container } = render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["name"],
          columnMeta: [{ category: "text" }],
          rows: [{ __col_0: value }],
          rowCount: 1,
          executionTimeMs: 1,
        }}
      />,
    );
    const cell = container.querySelector<HTMLTableCellElement>("td[data-row]");
    if (!cell) throw new Error("Missing query cell");
    fireEvent.doubleClick(cell);
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe(expectedText);
    expect(screen.getByText(description)).toBeDefined();
  });
});
