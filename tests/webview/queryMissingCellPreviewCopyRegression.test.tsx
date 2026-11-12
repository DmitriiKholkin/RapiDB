import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { QueryModeTableGrid } from "../../src/webview/components/table/grid/QueryModeTableGrid";
import { QueryResultsGrid } from "../../src/webview/components/table/grid/QueryResultsGrid";
import type { StructuredCellDialogState } from "../../src/webview/components/table/structuredCellDialog";
import { TableDialogs } from "../../src/webview/components/table/TableDialogs";
import type { QueryResult } from "../../src/webview/store";
import { clearPostedMessages, getPostedMessages } from "./testUtils";

// Keep the real MonacoEditor component and its clipboard handlers/actions.
// Only the editor engine and virtual layout need a DOM-test implementation.
vi.mock("monaco-editor", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../mocks/monaco-editor")>();
  return {
    ...actual,
    editor: {
      ...actual.editor,
      create: (container: HTMLElement, options: { value?: string }) => {
        const base = actual.editor.create(container, options);
        const actions = new Map<string, () => void>();
        base.getDomNode().addEventListener("keydown", (event) => {
          if ((event.ctrlKey || event.metaKey) && event.key === "c") {
            actions.get("rapidb.copy")?.();
          }
        });
        return {
          ...base,
          addAction: (action: { id: string; run: () => void }) => {
            actions.set(action.id, action.run);
            return { dispose: () => actions.delete(action.id) };
          },
        };
      },
    },
  };
});

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
  columns: ["id", "value"],
  columnMeta: [{ category: "integer" }, { category: "text" }],
  rows: [
    { __col_0: 2, __col_1: null },
    { __col_0: 1, __col_1: "MISSING" },
    { __col_0: 3, __col_1: "NULL" },
    { __col_0: 4, __col_1: "" },
    { __col_0: 5, __col_1: "ordinary" },
    { __col_0: 6, __col_1: null },
    { __col_0: 7 },
  ],
  // An explicit marker wins even over a normalized null raw value.
  missingCells: [{ rowIndex: 0, columnIndex: 1 }],
  rowCount: 7,
  executionTimeMs: 1,
};

function Harness({ data = result }: { data?: QueryResult }) {
  const [columnOrder, setColumnOrder] = useState(["__col_0", "__col_1"]);
  const [sorting, setSorting] = useState<
    React.ComponentProps<typeof QueryResultsGrid>["sorting"]
  >([]);
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});
  return (
    <>
      <button
        type="button"
        onClick={() => setColumnOrder(["__col_1", "__col_0"])}
      >
        Reorder columns
      </button>
      <QueryResultsGrid
        result={data}
        columnOrder={columnOrder}
        onColumnOrderChange={setColumnOrder}
        sorting={sorting}
        onSortingChange={setSorting}
        columnSizing={columnSizing}
        onColumnSizingChange={setColumnSizing}
      />
    </>
  );
}

function cell(container: HTMLElement, row: number, column = 1) {
  const node = container.querySelector<HTMLElement>(
    `td[data-row="${row}"][data-col="${column}"]`,
  );
  if (!node) throw new Error(`Missing grid cell ${row}:${column}`);
  return node;
}

async function openCell(node: HTMLElement) {
  fireEvent.mouseDown(node, { button: 0 });
  fireEvent.mouseUp(document);
  fireEvent.doubleClick(node);
  await act(
    () =>
      new Promise<void>((resolve) => requestAnimationFrame(() => resolve())),
  );
  return screen.getByRole("dialog", { name: "Cell data: value" });
}

type CopyRoute = "ctrl-c" | "cmd-c" | "native" | "context";
const routes: CopyRoute[] = ["ctrl-c", "cmd-c", "native", "context"];

function copy(dialog: HTMLElement, route: CopyRoute) {
  clearPostedMessages();
  const target = within(dialog).queryByTestId("mock-monaco-dom") ?? dialog;
  if (route === "ctrl-c" || route === "cmd-c") {
    fireEvent.keyDown(target, {
      key: "c",
      ctrlKey: route === "ctrl-c",
      metaKey: route === "cmd-c",
    });
  } else if (route === "native") {
    fireEvent(target, new Event("copy", { bubbles: true, cancelable: true }));
  } else {
    fireEvent.contextMenu(target);
    const action = within(dialog).queryByRole("menuitem", { name: "Copy" });
    if (action) fireEvent.click(action);
  }
  // A viewer-level Copy button must not be an untested escape hatch.
  const button = within(dialog).queryByRole("button", { name: /^Copy$/ });
  if (button) fireEvent.click(button);
  return getPostedMessages()
    .filter((message) => message.type === "writeClipboard")
    .map((message) => (message.payload as { text: string }).text);
}

describe("query missing-cell preview copy regression", () => {
  beforeEach(clearPostedMessages);

  it.each(
    routes,
  )("double click -> preview -> %s cannot copy absent data", async (route) => {
    const { container } = render(<Harness />);
    const dialog = await openCell(cell(container, 0));

    expect(copy(dialog, route)).toEqual([]);
    expect(within(dialog).queryByTestId("mock-monaco-dom")).toBeNull();
    expect(dialog.textContent).toMatch(/does not contain the attribute/);
    expect(dialog.textContent).toMatch(/Copy is unavailable/);
    expect(within(dialog).queryByRole("menuitem")).toBeNull();
    expect(within(dialog).queryByRole("button", { name: /^Copy$/ })).toBeNull();
    expect(document.activeElement).toBe(
      within(dialog).getByRole("button", { name: "Close cell data" }),
    );
    fireEvent.keyDown(document.activeElement as HTMLElement, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it.each(
    routes,
  )("keeps %s copy for literal MISSING/NULL, empty, ordinary and nullish cells", async (route) => {
    const { container } = render(<Harness />);
    for (const [row, text, isNull] of [
      [1, "MISSING", false],
      [2, "NULL", false],
      [3, "", false],
      [4, "ordinary", false],
      [5, "NULL", true],
      [6, "NULL", true],
    ] as const) {
      const dialog = await openCell(cell(container, row));
      expect(within(dialog).getByTestId("mock-monaco-dom")).toBeDefined();
      expect(dialog.textContent).toContain(
        isNull ? "value is NULL (no value)." : "Text data in value.",
      );
      // The existing context menu requires a selection; empty text has none.
      expect(copy(dialog, route)).toEqual(
        route === "context" && text === "" ? [] : [text || "\n"],
      );
      if (route === "context" && text === "") {
        expect(
          within(dialog).getByRole("menuitem", { name: "Copy" }),
        ).toHaveProperty("disabled", true);
      }
      fireEvent.click(within(dialog).getByRole("button", { name: "Close" }));
    }
  });

  it("uses source row/column markers after sorting and column reorder, not display labels", async () => {
    const { container } = render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "Sort by id" }));
    fireEvent.click(screen.getByRole("button", { name: "Sort by id" }));
    fireEvent.click(screen.getByRole("button", { name: "Reorder columns" }));
    expect(cell(container, 0, 0).textContent).toBe("MISSING");
    expect(cell(container, 1, 0).textContent).toBe("MISSING");

    const literal = await openCell(cell(container, 0, 0));
    expect(copy(literal, "native")).toEqual(["MISSING"]);
    fireEvent.click(within(literal).getByRole("button", { name: "Close" }));
    const absent = await openCell(cell(container, 1, 0));
    expect(copy(absent, "native")).toEqual([]);
    expect(within(absent).queryByTestId("mock-monaco-dom")).toBeNull();
  });

  it("does not retain an old preview or missing marker when the result is replaced", async () => {
    const view = render(<Harness />);
    await openCell(cell(view.container, 0));
    view.rerender(<Harness data={{ ...result, missingCells: [] }} />);
    expect(screen.queryByRole("dialog")).toBeNull();
    const dialog = await openCell(cell(view.container, 0));
    expect(copy(dialog, "native")).toEqual(["NULL"]);
    expect(dialog.textContent).toContain("value is NULL (no value).");
    const oldEditor = within(dialog).getByTestId("mock-monaco-dom");
    view.rerender(<Harness />);
    expect(screen.queryByRole("dialog")).toBeNull();
    const absent = await openCell(cell(view.container, 0));
    expect(copy(absent, "native")).toEqual([]);
    // A previously mounted editor must not retain a live clipboard handler.
    fireEvent(
      oldEditor,
      new Event("copy", { bubbles: true, cancelable: true }),
    );
    expect(getPostedMessages()).toEqual([]);
  });

  it("opens absent data with F2 and keeps focus trapping and Close available", () => {
    const { container } = render(<Harness />);
    const target = cell(container, 0);
    fireEvent.mouseDown(target, { button: 0 });
    fireEvent.mouseUp(document);
    fireEvent.keyDown(target, { key: "F2" });
    const dialog = screen.getByRole("dialog", { name: "Cell data: value" });
    expect(within(dialog).queryByTestId("mock-monaco-dom")).toBeNull();
    const closeIcon = within(dialog).getByRole("button", {
      name: "Close cell data",
    });
    const close = within(dialog).getByRole("button", { name: "Close" });
    expect(document.activeElement).toBe(closeIcon);
    fireEvent.keyDown(closeIcon, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(close);
    fireEvent.keyDown(close, { key: "Tab" });
    expect(document.activeElement).toBe(closeIcon);
    fireEvent.click(close);
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it.each([
    false,
    true,
  ])("table structured preview readOnly=%s retains JSON precision and copy", async (readOnly) => {
    const json = '{"n": 9007199254740993, "d": 1.2300}';
    const dialog: StructuredCellDialogState = {
      rowKind: "persisted",
      rowIdx: 0,
      column: {
        name: "payload",
        type: "json",
        nativeType: "json",
        category: "json",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
        filterable: false,
        filterOperators: [],
        valueSemantics: "plain",
      },
      title: "Cell data: payload",
      description: "JSON data in payload.",
      language: "json",
      initialText: json,
      draftText: json,
      originalValue: json,
      nullable: true,
      readOnly,
      initialIsNull: false,
      isNull: false,
    };
    const onConfirm = vi.fn();
    render(
      <TableDialogs
        exportChoice={null}
        mutationPreview={null}
        structuredCellDialog={dialog}
        rowsLength={1}
        totalCount={1}
        onCancelExport={vi.fn()}
        onCancelMutationPreview={vi.fn()}
        onCancelStructuredCellDialog={vi.fn()}
        onChangeStructuredCellDialog={vi.fn()}
        onConfirmMutationPreview={vi.fn()}
        onConfirmStructuredCellDialog={onConfirm}
        onExportAll={vi.fn()}
        onExportVisible={vi.fn()}
        onSetStructuredCellDialogNull={vi.fn()}
        onSetStructuredCellDialogDefault={vi.fn()}
      />,
    );
    await act(
      () =>
        new Promise<void>((resolve) => requestAnimationFrame(() => resolve())),
    );
    const modal = screen.getByRole("dialog", { name: "Cell data: payload" });
    for (const route of routes) expect(copy(modal, route)).toEqual([json]);
    fireEvent.click(within(modal).getByRole("button", { name: "Apply" }));
    expect(onConfirm).toHaveBeenCalledOnce();
  });
});

describe("query result-set preview lifecycle through QueryModeTableGrid", () => {
  function dataWithSets(missing = true): QueryResult {
    const rows = [
      { __col_0: 2, __col_1: "MISSING" },
      { __col_0: 1, __col_1: "MISSING" },
      { __col_0: 3, __col_1: "ordinary" },
    ];
    const shared = {
      columns: result.columns,
      columnMeta: result.columnMeta,
      rows,
      rowCount: rows.length,
    };
    return {
      ...shared,
      executionTimeMs: 1,
      missingCells: missing ? [{ rowIndex: 0, columnIndex: 1 }] : [],
      resultSets: [
        { ...shared, id: "first" },
        { ...shared, id: "second" },
      ],
    };
  }

  it.each([
    "missing",
    "literal",
  ])("keeps %s preview, focus and copy semantics across same-props parent renders", async (kind) => {
    const data = dataWithSets(kind === "missing");
    const view = render(<QueryModeTableGrid status="success" result={data} />);
    if (kind === "literal") {
      fireEvent.click(screen.getByRole("tab", { name: "Result 2 (3)" }));
    }
    const dialog = await openCell(cell(view.container, 0));
    const focus = within(dialog).getByRole("button", {
      name: "Close cell data",
    });
    focus.focus();

    view.rerender(<QueryModeTableGrid status="success" result={data} />);
    expect(screen.queryByRole("dialog")).toBe(dialog);
    expect(document.activeElement).toBe(focus);
    // Selecting the already-active tab is another real parent state update.
    fireEvent.click(
      screen.getByRole("tab", {
        name: kind === "missing" ? "Result 1 (3)" : "Result 2 (3)",
      }),
    );
    expect(screen.queryByRole("dialog")).toBe(dialog);
    for (const route of routes) {
      expect(copy(dialog, route)).toEqual(
        kind === "missing" ? [] : ["MISSING"],
      );
    }
    expect(within(dialog).queryByTestId("mock-monaco-dom") === null).toBe(
      kind === "missing",
    );
  });

  it("preserves a preview through real sort/reorder updates without stale source coordinates", async () => {
    const view = render(
      <QueryModeTableGrid status="success" result={dataWithSets()} />,
    );
    const dialog = await openCell(cell(view.container, 0));
    fireEvent.click(screen.getByRole("button", { name: "Sort by id" }));
    fireEvent.click(screen.getByRole("button", { name: "Sort by id" }));
    expect(screen.queryByRole("dialog")).toBe(dialog);

    const [idHeader, valueHeader] = Array.from(
      view.container.querySelectorAll<HTMLElement>("th[data-column-id]"),
    );
    vi.spyOn(idHeader, "getBoundingClientRect").mockReturnValue(
      new DOMRect(0, 0, 50, 28),
    );
    vi.spyOn(valueHeader, "getBoundingClientRect").mockReturnValue(
      new DOMRect(50, 0, 50, 28),
    );
    fireEvent.mouseDown(valueHeader, { clientX: 75, clientY: 14, buttons: 1 });
    fireEvent.mouseMove(document, { clientX: 10, clientY: 14, buttons: 1 });
    fireEvent.mouseUp(document, { clientX: 10, clientY: 14, buttons: 0 });
    expect(screen.queryByRole("dialog")).toBe(dialog);
    expect(cell(view.container, 0, 0).textContent).toBe("MISSING");
    expect(cell(view.container, 1, 0).textContent).toBe("MISSING");
    for (const route of routes) expect(copy(dialog, route)).toEqual([]);
    fireEvent.click(within(dialog).getByRole("button", { name: "Close" }));
    const literal = await openCell(cell(view.container, 0, 0));
    expect(copy(literal, "native")).toEqual(["MISSING"]);
    fireEvent.click(within(literal).getByRole("button", { name: "Close" }));
    const missing = await openCell(cell(view.container, 1, 0));
    expect(copy(missing, "native")).toEqual([]);
  });

  it("closes on result-set changes even with shared rows/columns and distinguishes missing from literal", async () => {
    const view = render(
      <QueryModeTableGrid status="success" result={dataWithSets()} />,
    );
    const missing = await openCell(cell(view.container, 0));
    expect(copy(missing, "native")).toEqual([]);
    fireEvent.click(screen.getByRole("tab", { name: "Result 2 (3)" }));
    expect(screen.queryByRole("dialog")).toBeNull();
    const secondMissing = await openCell(cell(view.container, 0));
    expect(copy(secondMissing, "native")).toEqual([]);
    fireEvent.click(
      within(secondMissing).getByRole("button", { name: "Close" }),
    );
    const literal = await openCell(cell(view.container, 1));
    const oldEditor = within(literal).getByTestId("mock-monaco-dom");
    expect(copy(literal, "native")).toEqual(["MISSING"]);
    fireEvent.click(screen.getByRole("tab", { name: "Result 1 (3)" }));
    expect(screen.queryByRole("dialog")).toBeNull();
    const absent = await openCell(cell(view.container, 0));
    for (const route of routes) expect(copy(absent, route)).toEqual([]);
    expect(document.activeElement).toBe(
      within(absent).getByRole("button", { name: "Close cell data" }),
    );
    fireEvent(
      oldEditor,
      new Event("copy", { bubbles: true, cancelable: true }),
    );
    expect(getPostedMessages()).toEqual([]);
  });

  it("closes on a new source result even if its sets and rows are reused", async () => {
    const data = dataWithSets(false);
    const view = render(<QueryModeTableGrid status="success" result={data} />);
    fireEvent.click(screen.getByRole("tab", { name: "Result 2 (3)" }));
    const literal = await openCell(cell(view.container, 0));
    expect(copy(literal, "native")).toEqual(["MISSING"]);
    view.rerender(
      <QueryModeTableGrid
        status="success"
        result={{ ...data, missingCells: [{ rowIndex: 0, columnIndex: 1 }] }}
      />,
    );
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(
      screen
        .getByRole("tab", { name: "Result 1 (3)" })
        .getAttribute("aria-selected"),
    ).toBe("true");
    const missing = await openCell(cell(view.container, 0));
    expect(within(missing).queryByTestId("mock-monaco-dom")).toBeNull();
    for (const route of routes) expect(copy(missing, route)).toEqual([]);
  });
});
