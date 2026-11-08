import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableView } from "../../src/webview/components/TableView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

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
vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: (props: {
    initialValue?: string;
    ariaLabel?: string;
    onChange?: (value: string) => void;
  }) => (
    <textarea
      aria-label={props.ariaLabel}
      defaultValue={props.initialValue}
      onChange={(event) => props.onChange?.(event.target.value)}
    />
  ),
}));

const columns: ColumnTypeMeta[] = ["id", "name", "note"].map((name) => ({
  name,
  type: "TEXT",
  nativeType: "TEXT",
  nullable: name !== "id",
  isPrimaryKey: name === "id",
  isForeignKey: false,
  category: "text",
  filterable: true,
  filterOperators: ["eq"],
  valueSemantics: "plain",
}));

function messages(type: string) {
  return getPostedMessages().filter((message) => message.type === type);
}

function setup() {
  clearPostedMessages();
  const view = render(
    <TableView
      connectionId="conn"
      database="db"
      schema="public"
      table="items"
    />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    }),
  );
  const fetch = messages("fetchPage").at(-1)?.payload as { fetchId: number };
  act(() =>
    dispatchIncomingMessage("tableData", {
      fetchId: fetch.fetchId,
      rows: [
        { id: "1", name: "Alice", note: "source" },
        { id: "2", name: "Bob", note: "target" },
      ],
      totalCount: 2,
    }),
  );
  const cell = (row: number, col: number) => {
    const node = view.container.querySelector<HTMLElement>(
      `td[data-row="${row}"][data-col="${col}"]`,
    );
    if (!node) throw new Error(`Missing cell ${row}:${col}`);
    return node;
  };
  const select = (row: number, col: number, endCol?: number) => {
    fireEvent.mouseDown(cell(row, col), { button: 0 });
    fireEvent.mouseUp(document);
    if (endCol !== undefined) {
      fireEvent.mouseDown(cell(row, endCol), { button: 0, shiftKey: true });
      fireEvent.mouseUp(document);
    }
  };
  const copy = () => {
    clearPostedMessages();
    fireEvent.keyDown(document.activeElement as HTMLElement, {
      key: "c",
      ctrlKey: true,
    });
    const message = messages("writeClipboard").at(-1);
    expect(message).toBeDefined();
    return (message?.payload as { text: string }).text;
  };
  fireEvent.doubleClick(cell(0, 2));
  fireEvent.change(screen.getByLabelText("Cell data"), {
    target: { value: "Pending Alice" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Apply" }));
  expect(cell(0, 2).textContent).toContain("Pending Alice");
  return { ...view, cell, select, copy };
}

describe("TableView pending edits and clipboard", () => {
  it("blocks sorting with pending edits and copies the unchanged visible row", () => {
    const grid = setup();
    clearPostedMessages();
    const header = grid.container.querySelector<HTMLElement>(
      'thead th[data-column-id="name"]',
    );
    if (!header) throw new Error("Missing name header");
    fireEvent.click(header);
    expect(messages("fetchPage")).toHaveLength(0);
    expect(
      screen.getByText(/Apply or revert pending changes before loading/),
    ).toBeTruthy();
    expect(grid.cell(0, 2).textContent).toContain("Pending Alice");
    expect(grid.cell(1, 2).textContent).toContain("Bob");
    grid.select(0, 2);
    expect(grid.copy()).toBe("Pending Alice");
    fireEvent.click(screen.getByRole("button", { name: "Revert All" }));
    clearPostedMessages();
    fireEvent.click(header);
    expect(messages("fetchPage")).toHaveLength(1);
  });

  it("copies pending values with the selection column present and pastes them into another row", () => {
    const grid = setup();
    expect(screen.getByLabelText("Select row 1")).toBeTruthy();
    grid.select(0, 2, 3);
    const copied = grid.copy();
    expect(copied).toBe("Pending Alice\tsource");
    grid.select(1, 2);
    clearPostedMessages();
    fireEvent.paste(document.activeElement as HTMLElement);
    const request = messages("readClipboard").at(-1);
    expect(request).toBeDefined();
    act(() =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text: copied,
      }),
    );
    expect(grid.cell(1, 2).textContent).toContain("Pending Alice");
    expect(grid.cell(1, 3).textContent).toContain("source");
    grid.select(1, 2, 3);
    expect(grid.copy()).toBe(copied);
  });
});
