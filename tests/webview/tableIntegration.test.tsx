import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useRef, useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  type CellRange,
  type UseCellSelectionOptions,
  type UseCellSelectionReturn,
  useCellSelection,
} from "../../src/webview/components/table/useCellSelection";

function TableIntegrationHarness({
  onReady,
  onPasteCallback,
}: {
  onReady: (api: UseCellSelectionReturn) => void;
  onPasteCallback?: (cellRow: number, cellCol: number) => void;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [editingCell, setEditingCell] = useState<{
    row: number;
    col: number;
  } | null>(null);
  const [filterValues, setFilterValues] = useState<Record<string, string>>({
    col0: "",
    col1: "",
  });

  const options: UseCellSelectionOptions = {
    rowCount: 3,
    colCount: 2,
    getCellValue: (rowIndex: number, colIndex: number) =>
      `R${rowIndex}C${colIndex}`,
    scrollRef,
    getCellFromPoint: () => ({ row: 0, col: 0 }),
    onPaste: onPasteCallback
      ? () => {
          if (selectionRef.current?.range) {
            onPasteCallback(
              selectionRef.current.range.anchorRow,
              selectionRef.current.range.anchorCol,
            );
          }
        }
      : undefined,
  };

  const api = useCellSelection(options);
  const selectionRef = useRef(api);
  selectionRef.current = api;

  onReady(api);

  const handleCellDoubleClick = (row: number, col: number) => {
    setEditingCell({ row, col });
  };

  return (
    <div>
      <div
        ref={scrollRef}
        tabIndex={0}
        data-testid="scroll-container"
        style={{ outline: "none" }}
      >
        <table>
          <thead>
            <tr>
              <th>
                <input
                  data-testid="filter-col0"
                  aria-label="Column 0 filter value"
                  type="text"
                  value={filterValues.col0}
                  onChange={(e) =>
                    setFilterValues((prev) => ({
                      ...prev,
                      col0: e.target.value,
                    }))
                  }
                />
              </th>
              <th>
                <input
                  data-testid="filter-col1"
                  aria-label="Column 1 filter value"
                  type="text"
                  value={filterValues.col1}
                  onChange={(e) =>
                    setFilterValues((prev) => ({
                      ...prev,
                      col1: e.target.value,
                    }))
                  }
                />
              </th>
            </tr>
          </thead>
          <tbody>
            {[0, 1, 2].map((row) => (
              <tr key={row}>
                {[0, 1].map((col) => {
                  const isEditing =
                    editingCell?.row === row && editingCell?.col === col;
                  return (
                    <td
                      key={col}
                      data-testid={`cell-${row}-${col}`}
                      data-row={row}
                      data-col={col}
                      onMouseDown={(event) =>
                        api.handleCellMouseDown(row, col, event)
                      }
                      onMouseEnter={(event) =>
                        api.handleCellMouseEnter(row, col, event)
                      }
                      onDoubleClick={() => handleCellDoubleClick(row, col)}
                    >
                      {isEditing ? (
                        <input
                          data-testid={`edit-input-${row}-${col}`}
                          aria-label="Cell value"
                          defaultValue={`R${row}C${col}`}
                          autoFocus
                          onBlur={() => setEditingCell(null)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter" || e.key === "Escape") {
                              setEditingCell(null);
                            }
                          }}
                        />
                      ) : (
                        <span>
                          R{row}C{col}
                        </span>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <span data-testid="range-display">
        {api.range
          ? `${api.range.anchorRow},${api.range.anchorCol}-${api.range.activeRow},${api.range.activeCol}`
          : "null"}
      </span>
    </div>
  );
}

function createMouseEvent(
  target: HTMLElement,
  options: {
    button?: number;
    shiftKey?: boolean;
    clientX?: number;
    clientY?: number;
  } = {},
): React.MouseEvent {
  let prevented = false;
  return {
    target,
    button: options.button ?? 0,
    shiftKey: options.shiftKey ?? false,
    clientX: options.clientX ?? 0,
    clientY: options.clientY ?? 0,
    preventDefault: () => {
      prevented = true;
    },
    get defaultPrevented() {
      return prevented;
    },
  } as unknown as React.MouseEvent;
}

function getDisplayedRange(): CellRange | null {
  const text = screen.getByTestId("range-display").textContent;
  if (text === "null") return null;
  const match = text?.match(/^(\d+),(\d+)-(\d+),(\d+)$/);
  if (!match) return null;
  return {
    anchorRow: Number.parseInt(match[1], 10),
    anchorCol: Number.parseInt(match[2], 10),
    activeRow: Number.parseInt(match[3], 10),
    activeCol: Number.parseInt(match[4], 10),
  };
}

function renderHarness(options?: {
  onPasteCallback?: (cellRow: number, cellCol: number) => void;
}) {
  let hookApi: UseCellSelectionReturn | undefined;
  const readyCallback = vi.fn((api: UseCellSelectionReturn) => {
    hookApi = api;
  });

  render(
    <TableIntegrationHarness
      onReady={readyCallback}
      onPasteCallback={options?.onPasteCallback}
    />,
  );

  const scrollContainer = screen.getByTestId(
    "scroll-container",
  ) as HTMLDivElement;

  function getApi(): UseCellSelectionReturn {
    if (!hookApi) {
      throw new Error(
        "Hook API not initialized. Ensure render() has completed.",
      );
    }
    return hookApi;
  }

  function pressKeyOnGrid(
    key: string,
    keyOptions: { ctrlKey?: boolean; shiftKey?: boolean } = {},
  ) {
    const event = new KeyboardEvent("keydown", {
      key,
      ctrlKey: keyOptions.ctrlKey,
      shiftKey: keyOptions.shiftKey,
      bubbles: true,
      cancelable: true,
    });
    Object.defineProperty(event, "target", {
      value: scrollContainer,
      writable: false,
    });
    act(() => {
      getApi().handleKeyDown(event);
    });
  }

  function pressKeyOnInput(
    input: HTMLElement,
    key: string,
    keyOptions: { ctrlKey?: boolean; shiftKey?: boolean } = {},
  ) {
    const event = new KeyboardEvent("keydown", {
      key,
      ctrlKey: keyOptions.ctrlKey,
      shiftKey: keyOptions.shiftKey,
      bubbles: true,
      cancelable: true,
    });
    Object.defineProperty(event, "target", {
      value: input,
      writable: false,
    });
    act(() => {
      getApi().handleKeyDown(event);
    });
  }

  return {
    get api() {
      return getApi();
    },
    scrollContainer,
    pressKeyOnGrid,
    pressKeyOnInput,
  };
}

describe("Table focus management — integration tests", () => {
  let harness: ReturnType<typeof renderHarness>;

  beforeEach(() => {
    harness = renderHarness();
  });

  describe("Bug scenario: filter input retains focus after cell click", () => {
    it("filter input loses focus when data cell is clicked, preventing paste into filter", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      const dataCell = screen.getByTestId("cell-1-0");
      const event = createMouseEvent(dataCell);
      act(() => {
        harness.api.handleCellMouseDown(1, 0, event);
      });

      expect(document.activeElement).not.toBe(filterInput);
      expect(document.activeElement).toBe(harness.scrollContainer);
      expect(event.defaultPrevented).toBe(true);
    });

    it("paste event on scroll container does NOT propagate to filter input", async () => {
      const user = userEvent.setup();

      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
      await user.click(filterInput);
      await user.type(filterInput, "original-filter-value");
      expect(filterInput.value).toBe("original-filter-value");

      const dataCell = screen.getByTestId("cell-0-1");
      const clickEvent = createMouseEvent(dataCell);
      act(() => {
        harness.api.handleCellMouseDown(0, 1, clickEvent);
      });

      expect(document.activeElement).not.toBe(filterInput);

      const pasteEvent = new Event("paste", {
        bubbles: true,
        cancelable: true,
      });
      harness.scrollContainer.dispatchEvent(pasteEvent);

      expect(filterInput.value).toBe("original-filter-value");
      expect(document.activeElement).not.toBe(filterInput);
    });
  });

  describe("End-to-end: filter → select cell → copy → paste to another cell", () => {
    it("complete workflow: filter, click cell, click another cell, verify focus and filter unchanged", () => {
      const filterCol0 = screen.getByTestId("filter-col0") as HTMLInputElement;
      const filterCol1 = screen.getByTestId("filter-col1") as HTMLInputElement;

      act(() => {
        filterCol0.focus();
      });
      expect(document.activeElement).toBe(filterCol0);

      const cell00 = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell00));
      });
      expect(document.activeElement).not.toBe(filterCol0);
      expect(getDisplayedRange()).toEqual({
        anchorRow: 0,
        anchorCol: 0,
        activeRow: 0,
        activeCol: 0,
      });

      const cell11 = screen.getByTestId("cell-1-1");
      act(() => {
        harness.api.handleCellMouseDown(1, 1, createMouseEvent(cell11));
      });
      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 1,
        activeRow: 1,
        activeCol: 1,
      });

      expect(filterCol0.value).toBe("");
      expect(filterCol1.value).toBe("");

      act(() => {
        filterCol1.focus();
      });
      expect(document.activeElement).toBe(filterCol1);
    });
  });

  describe("Edge case: filter → cell → another filter", () => {
    it("focus moves correctly from filter to cell and back to another filter", () => {
      const filterCol0 = screen.getByTestId("filter-col0") as HTMLInputElement;
      const filterCol1 = screen.getByTestId("filter-col1") as HTMLInputElement;

      act(() => {
        filterCol0.focus();
      });
      expect(document.activeElement).toBe(filterCol0);

      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });
      expect(document.activeElement).not.toBe(filterCol0);

      act(() => {
        filterCol1.focus();
      });
      expect(document.activeElement).toBe(filterCol1);
      expect(document.activeElement).not.toBe(filterCol0);
    });
  });

  describe("Edge case: filter → cell → double-click another cell to edit", () => {
    it("double-clicking a cell after clicking from filter opens EditInput with focus", async () => {
      const user = userEvent.setup();
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      const cell00 = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell00));
      });
      expect(document.activeElement).not.toBe(filterInput);

      const cell11 = screen.getByTestId("cell-1-1");
      await user.dblClick(cell11);

      const editInput = await screen.findByTestId("edit-input-1-1");
      expect(editInput).toBeTruthy();
      expect(document.activeElement).toBe(editInput);
    });
  });

  describe("Edge case: filter → cell → Tab key navigation", () => {
    it("Tab key moves selection to next cell when scroll container has focus", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      act(() => {
        filterInput.focus();
      });
      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      expect(getDisplayedRange()).toEqual({
        anchorRow: 0,
        anchorCol: 0,
        activeRow: 0,
        activeCol: 0,
      });

      harness.pressKeyOnGrid("Tab");

      expect(getDisplayedRange()).toEqual({
        anchorRow: 0,
        anchorCol: 1,
        activeRow: 0,
        activeCol: 1,
      });
    });

    it("Tab key does NOT move focus away from filter input when filter is focused", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      harness.pressKeyOnInput(filterInput, "Tab");

      expect(getDisplayedRange()).toBeNull();
      expect(document.activeElement).toBe(filterInput);
    });
  });

  describe("Cross-component: arrow key navigation after cell click from filter", () => {
    it("arrow keys navigate cells after filter → cell click sequence", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      act(() => {
        filterInput.focus();
      });
      const cell = screen.getByTestId("cell-1-0");
      act(() => {
        harness.api.handleCellMouseDown(1, 0, createMouseEvent(cell));
      });

      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 0,
        activeRow: 1,
        activeCol: 0,
      });

      harness.pressKeyOnGrid("ArrowDown");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 2,
        anchorCol: 0,
        activeRow: 2,
        activeCol: 0,
      });

      harness.pressKeyOnGrid("ArrowRight");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 2,
        anchorCol: 1,
        activeRow: 2,
        activeCol: 1,
      });

      harness.pressKeyOnGrid("ArrowUp");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 1,
        activeRow: 1,
        activeCol: 1,
      });

      harness.pressKeyOnGrid("ArrowLeft");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 0,
        activeRow: 1,
        activeCol: 0,
      });
    });

    it("arrow keys are bounded by table dimensions", () => {
      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      harness.pressKeyOnGrid("ArrowUp");
      expect(getDisplayedRange()?.activeRow).toBe(0);

      harness.pressKeyOnGrid("ArrowLeft");
      expect(getDisplayedRange()?.activeCol).toBe(0);

      harness.pressKeyOnGrid("ArrowDown");
      harness.pressKeyOnGrid("ArrowDown");
      harness.pressKeyOnGrid("ArrowRight");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 2,
        anchorCol: 1,
        activeRow: 2,
        activeCol: 1,
      });

      harness.pressKeyOnGrid("ArrowDown");
      expect(getDisplayedRange()?.activeRow).toBe(2);

      harness.pressKeyOnGrid("ArrowRight");
      expect(getDisplayedRange()?.activeCol).toBe(1);
    });
  });

  describe("Cross-component: EditInput retains focus when clicked directly", () => {
    it("clicking on an active EditInput does NOT blur it or prevent default", async () => {
      const user = userEvent.setup();

      const cell00 = screen.getByTestId("cell-0-0");
      await user.dblClick(cell00);

      const editInput = await screen.findByTestId("edit-input-0-0");
      expect(document.activeElement).toBe(editInput);

      const clickEvent = createMouseEvent(editInput);
      act(() => {
        harness.api.handleCellMouseDown(0, 0, clickEvent);
      });

      expect(clickEvent.defaultPrevented).toBe(false);
      expect(document.activeElement).toBe(editInput);
    });
  });

  describe("Cross-component: right-click context menu after filter interaction", () => {
    it("right-click on cell after filter click sets context menu ref and focuses scroll container", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      const cell = screen.getByTestId("cell-1-1");
      const rightClickEvent = createMouseEvent(cell, { button: 2 });
      act(() => {
        harness.api.handleCellMouseDown(1, 1, rightClickEvent);
      });

      expect(document.activeElement).not.toBe(filterInput);
      expect(document.activeElement).toBe(harness.scrollContainer);
      expect(harness.api.contextMenuCellRef.current).toEqual({
        row: 1,
        col: 1,
      });
      expect(rightClickEvent.defaultPrevented).toBe(true);
    });
  });

  describe("Cross-component: shift-click range selection after filter interaction", () => {
    it("shift-click extends range correctly after filter → cell click sequence", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      act(() => {
        filterInput.focus();
      });

      const cell00 = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell00));
      });
      expect(getDisplayedRange()).toEqual({
        anchorRow: 0,
        anchorCol: 0,
        activeRow: 0,
        activeCol: 0,
      });

      const cell21 = screen.getByTestId("cell-2-1");
      act(() => {
        harness.api.handleCellMouseDown(
          2,
          1,
          createMouseEvent(cell21, { shiftKey: true }),
        );
      });
      expect(getDisplayedRange()).toEqual({
        anchorRow: 0,
        anchorCol: 0,
        activeRow: 2,
        activeCol: 1,
      });
    });
  });

  describe("Cross-component: Escape clears selection without affecting filter", () => {
    it("Escape key clears selection range", () => {
      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });
      expect(getDisplayedRange()).not.toBeNull();

      harness.pressKeyOnGrid("Escape");
      expect(getDisplayedRange()).toBeNull();
    });
  });

  describe("Cross-component: Enter key moves to next row", () => {
    it("Enter key moves selection down one row", () => {
      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      harness.pressKeyOnGrid("Enter");

      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 0,
        activeRow: 1,
        activeCol: 0,
      });
    });
  });

  describe("Filter inputs remain functional after cell interactions", () => {
    it("filter input can still be typed into after cell click cycle", async () => {
      const user = userEvent.setup();
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      await user.click(filterInput);
      await user.type(filterInput, "test");
      expect(filterInput.value).toBe("test");

      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      await user.click(filterInput);
      await user.type(filterInput, "-more");
      expect(filterInput.value).toBe("test-more");
    });

    it("both filter inputs can be used independently after cell interactions", async () => {
      const user = userEvent.setup();
      const filterCol0 = screen.getByTestId("filter-col0") as HTMLInputElement;
      const filterCol1 = screen.getByTestId("filter-col1") as HTMLInputElement;

      await user.click(filterCol0);
      await user.type(filterCol0, "alpha");

      const cell = screen.getByTestId("cell-1-0");
      act(() => {
        harness.api.handleCellMouseDown(1, 0, createMouseEvent(cell));
      });

      await user.click(filterCol1);
      await user.type(filterCol1, "beta");

      expect(filterCol0.value).toBe("alpha");
      expect(filterCol1.value).toBe("beta");
    });
  });
});

describe("Table paste handler — integration tests (isolated renders)", () => {
  it("paste callback receives correct cell coordinates after filter → cell click", () => {
    const pasteTargets: Array<{ row: number; col: number }> = [];
    const pasteHarness = renderHarness({
      onPasteCallback: (row, col) => pasteTargets.push({ row, col }),
    });

    const filterInput = screen.getByTestId("filter-col1") as HTMLInputElement;
    act(() => {
      filterInput.focus();
    });

    const cell = screen.getByTestId("cell-2-1");
    act(() => {
      pasteHarness.api.handleCellMouseDown(2, 1, createMouseEvent(cell));
    });

    pasteHarness.pressKeyOnGrid("v", { ctrlKey: true });

    expect(pasteTargets).toEqual([{ row: 2, col: 1 }]);
  });

  it("Ctrl+V while filter is focused does NOT trigger cell paste", () => {
    const pasteTargets: Array<{ row: number; col: number }> = [];
    const pasteHarness = renderHarness({
      onPasteCallback: (row, col) => pasteTargets.push({ row, col }),
    });

    const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
    act(() => {
      filterInput.focus();
    });

    pasteHarness.pressKeyOnInput(filterInput, "v", { ctrlKey: true });

    expect(pasteTargets).toHaveLength(0);
  });
});

describe("Table clipboard shortcuts — focus-aware handling", () => {
  describe("Ctrl+X (cut) behaves like copy when grid is focused", () => {
    it("triggers onCopy callback with TSV when grid has selection", () => {
      const cutHarness = (() => {
        let hookApi: UseCellSelectionReturn | undefined;
        const readyCallback = vi.fn((api: UseCellSelectionReturn) => {
          hookApi = api;
        });
        render(
          <TableIntegrationHarness
            onReady={readyCallback}
            onPasteCallback={undefined}
          />,
        );
        return {
          get api() {
            if (!hookApi) throw new Error("not ready");
            return hookApi;
          },
        };
      })();

      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        cutHarness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      const beforeRange = cutHarness.api.range;
      const event = new KeyboardEvent("keydown", {
        key: "x",
        ctrlKey: true,
        bubbles: true,
        cancelable: true,
      });
      Object.defineProperty(event, "target", {
        value: screen.getByTestId("scroll-container"),
        writable: false,
      });
      act(() => {
        cutHarness.api.handleKeyDown(event);
      });

      expect(cutHarness.api.range).toEqual(beforeRange);
    });
  });

  describe("Ctrl+C/V/X with focus on a filter input", () => {
    it("Ctrl+C on filter input does NOT trigger cell copy and does NOT preventDefault", () => {
      const pasteTargets: Array<{ row: number; col: number }> = [];
      const copyHarness = renderHarness({
        onPasteCallback: (r, c) => pasteTargets.push({ row: r, col: c }),
      });

      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        copyHarness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      const event = new KeyboardEvent("keydown", {
        key: "c",
        ctrlKey: true,
        bubbles: true,
        cancelable: true,
      });
      Object.defineProperty(event, "target", {
        value: filterInput,
        writable: false,
      });
      act(() => {
        copyHarness.api.handleKeyDown(event);
      });

      expect(pasteTargets).toHaveLength(0);
      expect(event.defaultPrevented).toBe(false);
    });

    it("Ctrl+X on filter input does NOT preventDefault (native cut allowed)", () => {
      const pasteTargets: Array<{ row: number; col: number }> = [];
      const cutHarness = renderHarness({
        onPasteCallback: (r, c) => pasteTargets.push({ row: r, col: c }),
      });

      const cell = screen.getByTestId("cell-1-0");
      act(() => {
        cutHarness.api.handleCellMouseDown(1, 0, createMouseEvent(cell));
      });

      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
      act(() => {
        filterInput.focus();
      });

      const event = new KeyboardEvent("keydown", {
        key: "x",
        ctrlKey: true,
        bubbles: true,
        cancelable: true,
      });
      Object.defineProperty(event, "target", {
        value: filterInput,
        writable: false,
      });
      act(() => {
        cutHarness.api.handleKeyDown(event);
      });

      expect(event.defaultPrevented).toBe(false);
    });

    it("Ctrl+V on filter input does NOT trigger cell paste and does NOT preventDefault", () => {
      const pasteTargets: Array<{ row: number; col: number }> = [];
      const pasteHarness = renderHarness({
        onPasteCallback: (r, c) => pasteTargets.push({ row: r, col: c }),
      });

      const cell = screen.getByTestId("cell-2-1");
      act(() => {
        pasteHarness.api.handleCellMouseDown(2, 1, createMouseEvent(cell));
      });

      const filterInput = screen.getByTestId("filter-col1") as HTMLInputElement;
      act(() => {
        filterInput.focus();
      });

      const event = new KeyboardEvent("keydown", {
        key: "v",
        ctrlKey: true,
        bubbles: true,
        cancelable: true,
      });
      Object.defineProperty(event, "target", {
        value: filterInput,
        writable: false,
      });
      act(() => {
        pasteHarness.api.handleKeyDown(event);
      });

      expect(pasteTargets).toHaveLength(0);
      expect(event.defaultPrevented).toBe(false);
    });
  });

  describe("Ctrl+C/V/X with focus on a non-input target (grid container)", () => {
    it("Ctrl+V on grid with selection calls onPaste callback", () => {
      const pasteTargets: Array<{ row: number; col: number }> = [];
      const pasteHarness = renderHarness({
        onPasteCallback: (r, c) => pasteTargets.push({ row: r, col: c }),
      });

      const cell = screen.getByTestId("cell-0-1");
      act(() => {
        pasteHarness.api.handleCellMouseDown(0, 1, createMouseEvent(cell));
      });

      pasteHarness.pressKeyOnGrid("v", { ctrlKey: true });

      expect(pasteTargets).toEqual([{ row: 0, col: 1 }]);
    });

    it("Ctrl+C on grid does not change selection state", () => {
      const harness = renderHarness();

      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      const before = harness.api.range;

      const event = new KeyboardEvent("keydown", {
        key: "c",
        ctrlKey: true,
        bubbles: true,
        cancelable: true,
      });
      Object.defineProperty(event, "target", {
        value: harness.scrollContainer,
        writable: false,
      });
      act(() => {
        harness.api.handleKeyDown(event);
      });

      expect(harness.api.range).toEqual(before);
    });
  });
});
