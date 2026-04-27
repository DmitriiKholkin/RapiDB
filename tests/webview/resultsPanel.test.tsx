import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

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

import { ResultsPanel } from "../../src/webview/components/ResultsPanel";

describe("ResultsPanel", () => {
  it("left-aligns numeric query results", () => {
    render(
      <ResultsPanel
        status="success"
        result={{
          columns: ["id", "name"],
          columnMeta: [],
          rows: [{ __col_0: 123, __col_1: "Alice" }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    const cells = screen.getAllByRole("cell");

    expect((cells[0] as HTMLElement).style.textAlign).toBe("left");
    expect((cells[1] as HTMLElement).style.textAlign).toBe("left");
    expect((cells[0] as HTMLElement).style.userSelect).toBe("none");
    expect((cells[1] as HTMLElement).style.userSelect).toBe("none");
  });

  it("opens the cell editor on double click", async () => {
    const user = userEvent.setup();

    render(
      <ResultsPanel
        status="success"
        result={{
          columns: ["id"],
          columnMeta: [],
          rows: [{ __col_0: 123 }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    await user.dblClick(screen.getByRole("cell"));

    expect(screen.getByLabelText("Cell value")).toBeDefined();
  });
});
