import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

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

import { TableGrid } from "../../src/webview/components/table/TableGrid";
import { clearPostedMessages, getPostedMessages } from "./testUtils";

afterEach(() => {
  clearPostedMessages();
});

function dragResizeHandle(handle: HTMLElement, deltaX: number): void {
  const startX = 200;
  const endX = Math.max(0, startX + deltaX);
  fireEvent.mouseDown(handle, { clientX: startX, buttons: 1 });
  fireEvent.mouseMove(document, { clientX: endX, buttons: 1 });
  fireEvent.mouseUp(document, { clientX: endX, buttons: 0 });
}

describe("TableGrid query mode", () => {
  it("left-aligns numeric query results", () => {
    render(
      <TableGrid
        mode="query"
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
      <TableGrid
        mode="query"
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

  it("keeps structured-looking query results on the inline editor path", async () => {
    const user = userEvent.setup();

    render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["payload"],
          columnMeta: [{ category: "json" }],
          rows: [{ __col_0: '{"nested":{"ok":true}}' }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    await user.dblClick(screen.getByRole("cell"));

    expect(screen.getByLabelText("Cell value")).toBeDefined();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("renders JSON query result cells as a single line", () => {
    const rawJson = '{\n  "name": "Alice",\n  "meta": { "active":  true }\n}';
    const expectedSingleLine = rawJson.replace(/\r?\n/g, " ");

    render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["payload"],
          columnMeta: [{ category: "json" }],
          rows: [{ __col_0: rawJson }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    const cell = screen.getByRole("cell");
    const valueNode = cell.querySelector("span");

    expect(valueNode).toBeTruthy();
    expect((valueNode as HTMLSpanElement).style.whiteSpace).toBe("pre");
    expect(valueNode?.textContent).toBe(expectedSingleLine);
  });

  it("renders XML-like query result cells as a single line without native type metadata", () => {
    const rawXml =
      '<root>\n  <item id="1">Alice</item>\n  <item id="2">Bob</item>\n</root>';
    const expectedSingleLine = rawXml.replace(/\r?\n/g, " ");

    render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["payload"],
          columnMeta: [{ category: "text" }],
          rows: [{ __col_0: rawXml }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    const cell = screen.getByRole("cell");
    const valueNode = cell.querySelector("span");

    expect(valueNode).toBeTruthy();
    expect((valueNode as HTMLSpanElement).style.whiteSpace).toBe("pre");
    expect(valueNode?.textContent).toBe(expectedSingleLine);
  });

  it("collapses and reopens a result column from the resize divider", async () => {
    render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["id", "name"],
          columnMeta: [],
          rows: [{ __col_0: 1, __col_1: "Alice" }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    const nameHeader = screen.getByText("name").closest("th");
    if (!(nameHeader instanceof HTMLTableCellElement)) {
      throw new Error("Expected name header cell to be rendered");
    }

    const resizeHandle = screen.getByRole("button", {
      name: "Resize name column",
    });

    dragResizeHandle(resizeHandle, -500);

    await waitFor(() => {
      expect(nameHeader.style.width).toBe("0px");
    });

    expect(screen.queryByText("Alice")).toBeNull();

    dragResizeHandle(resizeHandle, 320);

    await waitFor(() => {
      expect(Number.parseFloat(nameHeader.style.width)).toBeGreaterThan(0);
    });

    expect(screen.getByText("Alice")).toBeTruthy();
  });

  it("posts exportResults messages from result toolbar actions", async () => {
    const user = userEvent.setup();

    render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["id"],
          columnMeta: [],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Export CSV" }));
    await user.click(screen.getByRole("button", { name: "Export JSON" }));

    expect(getPostedMessages()).toEqual([
      { type: "exportResultsCSV" },
      { type: "exportResultsJSON" },
    ]);
  });

  it("shows export actions on the left, metrics on the right, and no refresh button", () => {
    render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["id"],
          columnMeta: [],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          executionTimeMs: 5,
        }}
      />,
    );

    const exportCsvButton = screen.getByRole("button", { name: "Export CSV" });
    const exportJsonButton = screen.getByRole("button", {
      name: "Export JSON",
    });
    const queryMetrics = screen.getByText("1 row");

    expect(screen.queryByRole("button", { name: "Refresh" })).toBeNull();

    const exportGroup = exportCsvButton.parentElement;
    const toolbar = exportGroup?.parentElement;

    expect(exportGroup).not.toBeNull();
    expect(toolbar).not.toBeNull();
    expect(exportGroup?.contains(exportCsvButton)).toBe(true);
    expect(exportGroup?.contains(exportJsonButton)).toBe(true);
    expect(toolbar?.firstElementChild).toBe(exportGroup);
    expect(toolbar?.lastElementChild).toBe(queryMetrics);
    expect(screen.getByText("5 ms")).toBeTruthy();
  });
});
