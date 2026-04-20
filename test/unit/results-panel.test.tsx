/**
 * @vitest-environment jsdom
 */

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { QueryResult } from "../../src/webview/store";
import { ResultsPanel } from "../../src/webview/components/ResultsPanel";
import { categoryColor } from "../../src/webview/types";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 24,
        end: (index + 1) * 24,
      })),
    getTotalSize: () => count * 24,
  }),
}));

afterEach(cleanup);

describe("ResultsPanel", () => {
  it("uses metadata-driven and fallback type colors in query results", () => {
    const result: QueryResult = {
      columns: ["geom", "count", "raw"],
      columnMeta: [
        { category: "spatial" },
        { category: "integer" },
        { category: null },
      ],
      rows: [
        {
          __col_0: "POINT(1 2)",
          __col_1: 42,
          __col_2: "\\xDEADBEEF",
        },
      ],
      rowCount: 1,
      executionTimeMs: 7,
    };

    render(<ResultsPanel status="success" result={result} />);

    const geomValue = screen.getByText("POINT(1 2)") as HTMLElement;
    const countValue = screen.getByText("42") as HTMLElement;
    const rawValue = screen.getByText("\\xDEADBEEF") as HTMLElement;

    expect(geomValue.style.color).toBe(categoryColor("spatial"));
    expect(rawValue.style.color).toBe(categoryColor("binary"));
    expect(countValue.closest("td")?.style.textAlign).toBe("right");
  });

  it("falls back safely when runtime payload omits column metadata", () => {
    const result = {
      columns: ["geom", "count"],
      rows: [
        {
          __col_0: "POINT(1 2)",
          __col_1: 42,
        },
      ],
      rowCount: 1,
      executionTimeMs: 7,
    } as unknown as QueryResult;

    render(<ResultsPanel status="success" result={result} />);

    const geomValue = screen.getByText("POINT(1 2)") as HTMLElement;
    const countValue = screen.getByText("42") as HTMLElement;

    expect(geomValue.style.color).toBe(categoryColor("spatial"));
    expect(countValue.closest("td")?.style.textAlign).toBe("right");
  });
});