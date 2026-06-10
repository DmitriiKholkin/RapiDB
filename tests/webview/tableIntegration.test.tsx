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

/**
 * Integration test harness that simulates a realistic table layout:
 * - A scroll container (tabindex=0) that receives focus on cell click
 * - Two filter inputs (one per column) in a header row
 * - A grid of data cells (3 rows x 2 columns)
 * - An EditInput that appears when a cell is double-clicked
 *
 * This mirrors the real DOM structure of TableGrid.tsx to verify
 * cross-component focus management after the preventDefault() fix.
 */
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
          // Simulate the paste being directed at the currently selected cell
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
        // biome-ignore lint/a11y/noNoninteractiveTabindex: Test harness needs focusable container to test focus management
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
                          // biome-ignore lint/a11y/noAutofocus: Test harness needs autoFocus to verify EditInput focus behavior
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

/**
 * Renders the harness and returns a helper to call handleKeyDown
 * with the scroll container as target (simulating keyboard events
 * when the scroll container has focus).
 */
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

  // Helper to get the hook API, throwing if not yet initialized
  function getApi(): UseCellSelectionReturn {
    if (!hookApi) {
      throw new Error(
        "Hook API not initialized. Ensure render() has completed.",
      );
    }
    return hookApi;
  }

  /**
   * Calls handleKeyDown with a synthetic event whose target is the
   * scroll container (non-input element), simulating keyboard events
   * when the grid has focus.
   */
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
    // The hook reads event.target — we need to set it to the scroll container
    // to simulate the event originating from a non-input element.
    Object.defineProperty(event, "target", {
      value: scrollContainer,
      writable: false,
    });
    act(() => {
      getApi().handleKeyDown(event);
    });
  }

  /**
   * Calls handleKeyDown with a synthetic event whose target is a
   * specific input element, simulating keyboard events when that
   * input has focus.
   */
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
      // Arrange — user types into filter
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      // Act — user clicks on a data cell
      const dataCell = screen.getByTestId("cell-1-0");
      const event = createMouseEvent(dataCell);
      act(() => {
        harness.api.handleCellMouseDown(1, 0, event);
      });

      // Assert — filter input should no longer have focus
      expect(document.activeElement).not.toBe(filterInput);
      // Scroll container should have focus instead
      expect(document.activeElement).toBe(harness.scrollContainer);
      // The mousedown default should be prevented (the fix)
      expect(event.defaultPrevented).toBe(true);
    });

    it("paste event on scroll container does NOT propagate to filter input", async () => {
      const user = userEvent.setup();

      // Arrange — focus filter, type a value, then click cell
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
      await user.click(filterInput);
      await user.type(filterInput, "original-filter-value");
      expect(filterInput.value).toBe("original-filter-value");

      const dataCell = screen.getByTestId("cell-0-1");
      const clickEvent = createMouseEvent(dataCell);
      act(() => {
        harness.api.handleCellMouseDown(0, 1, clickEvent);
      });

      // The filter input should be blurred
      expect(document.activeElement).not.toBe(filterInput);

      // Act — simulate a paste event on the scroll container
      // (ClipboardEvent is not available in jsdom, so use a generic Event)
      const pasteEvent = new Event("paste", {
        bubbles: true,
        cancelable: true,
      });
      harness.scrollContainer.dispatchEvent(pasteEvent);

      // Assert — filter value should remain unchanged
      expect(filterInput.value).toBe("original-filter-value");
      // Filter should still not be focused
      expect(document.activeElement).not.toBe(filterInput);
    });
  });

  describe("End-to-end: filter → select cell → copy → paste to another cell", () => {
    it("complete workflow: filter, click cell, click another cell, verify focus and filter unchanged", () => {
      const filterCol0 = screen.getByTestId("filter-col0") as HTMLInputElement;
      const filterCol1 = screen.getByTestId("filter-col1") as HTMLInputElement;

      // Step 1: User filters column 0
      act(() => {
        filterCol0.focus();
      });
      expect(document.activeElement).toBe(filterCol0);

      // Step 2: User clicks on cell (0,0) to select it
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

      // Step 3: User clicks on cell (1,1) to select it
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

      // Step 4: Verify filter values are unchanged throughout
      expect(filterCol0.value).toBe("");
      expect(filterCol1.value).toBe("");

      // Step 5: Verify filter inputs can still be focused and typed into
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

      // Focus first filter
      act(() => {
        filterCol0.focus();
      });
      expect(document.activeElement).toBe(filterCol0);

      // Click on a data cell
      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });
      expect(document.activeElement).not.toBe(filterCol0);

      // Click on second filter — native focus should work
      act(() => {
        filterCol1.focus();
      });
      expect(document.activeElement).toBe(filterCol1);
      // First filter should have lost focus
      expect(document.activeElement).not.toBe(filterCol0);
    });
  });

  describe("Edge case: filter → cell → double-click another cell to edit", () => {
    it("double-clicking a cell after clicking from filter opens EditInput with focus", async () => {
      const user = userEvent.setup();
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      // Focus filter
      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      // Click on cell (0,0) to deselect filter
      const cell00 = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell00));
      });
      expect(document.activeElement).not.toBe(filterInput);

      // Double-click on cell (1,1) to edit
      const cell11 = screen.getByTestId("cell-1-1");
      await user.dblClick(cell11);

      // EditInput should appear and receive focus
      // findByTestId throws if not found, so no need for toBeInTheDocument
      const editInput = await screen.findByTestId("edit-input-1-1");
      expect(editInput).toBeTruthy();
      // The EditInput uses autoFocus, so it should be focused
      expect(document.activeElement).toBe(editInput);
    });
  });

  describe("Edge case: filter → cell → Tab key navigation", () => {
    it("Tab key moves selection to next cell when scroll container has focus", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      // Focus filter, then click cell
      act(() => {
        filterInput.focus();
      });
      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      // Selection should be at (0,0)
      expect(getDisplayedRange()).toEqual({
        anchorRow: 0,
        anchorCol: 0,
        activeRow: 0,
        activeCol: 0,
      });

      // Press Tab — should move selection to next column
      harness.pressKeyOnGrid("Tab");

      // Selection should advance to column 1
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

      // The handleKeyDown in useCellSelection checks if target is INPUT
      // and returns early, so Tab should NOT change cell selection
      harness.pressKeyOnInput(filterInput, "Tab");

      // No selection range should be set (hook bails out for input targets)
      expect(getDisplayedRange()).toBeNull();
      // Filter should still be focused
      expect(document.activeElement).toBe(filterInput);
    });
  });

  describe("Cross-component: arrow key navigation after cell click from filter", () => {
    it("arrow keys navigate cells after filter → cell click sequence", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      // Focus filter then click cell (1, 0)
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

      // ArrowDown
      harness.pressKeyOnGrid("ArrowDown");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 2,
        anchorCol: 0,
        activeRow: 2,
        activeCol: 0,
      });

      // ArrowRight
      harness.pressKeyOnGrid("ArrowRight");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 2,
        anchorCol: 1,
        activeRow: 2,
        activeCol: 1,
      });

      // ArrowUp
      harness.pressKeyOnGrid("ArrowUp");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 1,
        activeRow: 1,
        activeCol: 1,
      });

      // ArrowLeft
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

      // ArrowUp from row 0 should stay at row 0
      harness.pressKeyOnGrid("ArrowUp");
      expect(getDisplayedRange()?.activeRow).toBe(0);

      // ArrowLeft from col 0 should stay at col 0
      harness.pressKeyOnGrid("ArrowLeft");
      expect(getDisplayedRange()?.activeCol).toBe(0);

      // Navigate to bottom-right corner
      harness.pressKeyOnGrid("ArrowDown");
      harness.pressKeyOnGrid("ArrowDown");
      harness.pressKeyOnGrid("ArrowRight");
      expect(getDisplayedRange()).toEqual({
        anchorRow: 2,
        anchorCol: 1,
        activeRow: 2,
        activeCol: 1,
      });

      // ArrowDown from last row should stay
      harness.pressKeyOnGrid("ArrowDown");
      expect(getDisplayedRange()?.activeRow).toBe(2);

      // ArrowRight from last col should stay
      harness.pressKeyOnGrid("ArrowRight");
      expect(getDisplayedRange()?.activeCol).toBe(1);
    });
  });

  describe("Cross-component: EditInput retains focus when clicked directly", () => {
    it("clicking on an active EditInput does NOT blur it or prevent default", async () => {
      const user = userEvent.setup();

      // Double-click cell to open edit
      const cell00 = screen.getByTestId("cell-0-0");
      await user.dblClick(cell00);

      const editInput = await screen.findByTestId("edit-input-0-0");
      expect(document.activeElement).toBe(editInput);

      // Click on the edit input — should NOT blur it
      const clickEvent = createMouseEvent(editInput);
      act(() => {
        harness.api.handleCellMouseDown(0, 0, clickEvent);
      });

      // EditInput is an <input>, so preventDefault should NOT be called
      expect(clickEvent.defaultPrevented).toBe(false);
      // EditInput should still be focused
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

      // Right-click on cell
      const cell = screen.getByTestId("cell-1-1");
      const rightClickEvent = createMouseEvent(cell, { button: 2 });
      act(() => {
        harness.api.handleCellMouseDown(1, 1, rightClickEvent);
      });

      // Filter should lose focus
      expect(document.activeElement).not.toBe(filterInput);
      // Scroll container should be focused
      expect(document.activeElement).toBe(harness.scrollContainer);
      // Context menu ref should be set
      expect(harness.api.contextMenuCellRef.current).toEqual({
        row: 1,
        col: 1,
      });
      // preventDefault should be called
      expect(rightClickEvent.defaultPrevented).toBe(true);
    });
  });

  describe("Cross-component: shift-click range selection after filter interaction", () => {
    it("shift-click extends range correctly after filter → cell click sequence", () => {
      const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;

      // Focus filter
      act(() => {
        filterInput.focus();
      });

      // Click cell (0,0) — filter loses focus
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

      // Shift-click cell (2,1) — extend range
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

      // Type into filter
      await user.click(filterInput);
      await user.type(filterInput, "test");
      expect(filterInput.value).toBe("test");

      // Click on cell
      const cell = screen.getByTestId("cell-0-0");
      act(() => {
        harness.api.handleCellMouseDown(0, 0, createMouseEvent(cell));
      });

      // Click back on filter and type more
      await user.click(filterInput);
      await user.type(filterInput, "-more");
      expect(filterInput.value).toBe("test-more");
    });

    it("both filter inputs can be used independently after cell interactions", async () => {
      const user = userEvent.setup();
      const filterCol0 = screen.getByTestId("filter-col0") as HTMLInputElement;
      const filterCol1 = screen.getByTestId("filter-col1") as HTMLInputElement;

      // Type in col0 filter
      await user.click(filterCol0);
      await user.type(filterCol0, "alpha");

      // Click cell
      const cell = screen.getByTestId("cell-1-0");
      act(() => {
        harness.api.handleCellMouseDown(1, 0, createMouseEvent(cell));
      });

      // Type in col1 filter
      await user.click(filterCol1);
      await user.type(filterCol1, "beta");

      // Both filters should have their independent values
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

    // Focus filter then click cell (2, 1)
    const filterInput = screen.getByTestId("filter-col1") as HTMLInputElement;
    act(() => {
      filterInput.focus();
    });

    const cell = screen.getByTestId("cell-2-1");
    act(() => {
      pasteHarness.api.handleCellMouseDown(2, 1, createMouseEvent(cell));
    });

    // Simulate Ctrl+V — the hook's handleKeyDown triggers onPaste
    pasteHarness.pressKeyOnGrid("v", { ctrlKey: true });

    // Paste should be directed at cell (2, 1) — NOT the filter
    expect(pasteTargets).toEqual([{ row: 2, col: 1 }]);
  });

  it("Ctrl+V while filter is focused does NOT trigger cell paste", () => {
    const pasteTargets: Array<{ row: number; col: number }> = [];
    const pasteHarness = renderHarness({
      onPasteCallback: (row, col) => pasteTargets.push({ row, col }),
    });

    // Focus filter input
    const filterInput = screen.getByTestId("filter-col0") as HTMLInputElement;
    act(() => {
      filterInput.focus();
    });

    // Simulate Ctrl+V — handleKeyDown should bail out because target is INPUT
    pasteHarness.pressKeyOnInput(filterInput, "v", { ctrlKey: true });

    // No cell paste should have occurred
    expect(pasteTargets).toHaveLength(0);
  });
});
