import { act, render, screen } from "@testing-library/react";
import { useRef } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  type CellRange,
  type UseCellSelectionOptions,
  type UseCellSelectionReturn,
  useCellSelection,
} from "../../src/webview/components/table/useCellSelection";

function TestHarness({
  onReady,
}: {
  onReady: (api: UseCellSelectionReturn) => void;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);

  const options: UseCellSelectionOptions = {
    rowCount: 10,
    colCount: 5,
    getCellValue: () => "value",
    scrollRef,
    getCellFromPoint: () => ({ row: 0, col: 0 }),
  };

  const api = useCellSelection(options);

  onReady(api);

  return (
    <div>
      <div ref={scrollRef} tabIndex={-1} data-testid="scroll-container" />
      <input data-testid="filter-input" type="text" aria-label="Filter" />
      <div data-testid="data-cell" />
      <input data-testid="edit-input" type="text" aria-label="Cell editor" />
      <textarea data-testid="edit-textarea" aria-label="Cell editor textarea" />
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
  if (text === "null") {
    return null;
  }
  const match = text?.match(/^(\d+),(\d+)-(\d+),(\d+)$/);
  if (!match) {
    return null;
  }
  return {
    anchorRow: Number.parseInt(match[1], 10),
    anchorCol: Number.parseInt(match[2], 10),
    activeRow: Number.parseInt(match[3], 10),
    activeCol: Number.parseInt(match[4], 10),
  };
}

describe("useCellSelection — focus management on cell click", () => {
  let hookApi: UseCellSelectionReturn;
  let scrollContainer: HTMLDivElement;
  let filterInput: HTMLInputElement;
  let dataCell: HTMLDivElement;
  let editInput: HTMLInputElement;
  let editTextarea: HTMLTextAreaElement;

  beforeEach(() => {
    const readyCallback = vi.fn((api: UseCellSelectionReturn) => {
      hookApi = api;
    });

    render(<TestHarness onReady={readyCallback} />);

    scrollContainer = screen.getByTestId("scroll-container") as HTMLDivElement;
    filterInput = screen.getByTestId("filter-input") as HTMLInputElement;
    dataCell = screen.getByTestId("data-cell") as HTMLDivElement;
    editInput = screen.getByTestId("edit-input") as HTMLInputElement;
    editTextarea = screen.getByTestId("edit-textarea") as HTMLTextAreaElement;
  });

  describe("filter input loses focus when data cell is clicked", () => {
    it("blurs filter input and focuses scroll container on left-click of data cell", () => {
      act(() => {
        filterInput.focus();
      });
      expect(document.activeElement).toBe(filterInput);

      const event = createMouseEvent(dataCell);
      act(() => {
        hookApi.handleCellMouseDown(2, 3, event);
      });

      expect(document.activeElement).not.toBe(filterInput);
      expect(document.activeElement).toBe(scrollContainer);
    });
  });

  describe("mousedown default is prevented on data cell click", () => {
    it("calls preventDefault when left-clicking a non-input data cell", () => {
      const event = createMouseEvent(dataCell);

      act(() => {
        hookApi.handleCellMouseDown(2, 3, event);
      });

      expect(event.defaultPrevented).toBe(true);
    });

    it("calls preventDefault for right-click on non-input data cell", () => {
      const event = createMouseEvent(dataCell, { button: 2 });

      act(() => {
        hookApi.handleCellMouseDown(2, 3, event);
      });

      expect(event.defaultPrevented).toBe(true);
    });
  });

  describe("EditInput retains focus when clicked directly", () => {
    it("does NOT prevent default when mousedown target is an <input>", () => {
      const event = createMouseEvent(editInput);

      act(() => {
        hookApi.handleCellMouseDown(2, 3, event);
      });

      expect(event.defaultPrevented).toBe(false);
    });

    it("does NOT prevent default when mousedown target is a <textarea>", () => {
      const event = createMouseEvent(editTextarea);

      act(() => {
        hookApi.handleCellMouseDown(2, 3, event);
      });

      expect(event.defaultPrevented).toBe(false);
    });

    it("does NOT blur the active input when clicking directly on it", () => {
      act(() => {
        editInput.focus();
      });
      expect(document.activeElement).toBe(editInput);

      const blurSpy = vi.spyOn(editInput, "blur");

      const event = createMouseEvent(editInput);

      act(() => {
        hookApi.handleCellMouseDown(2, 3, event);
      });

      expect(blurSpy).not.toHaveBeenCalled();
      expect(document.activeElement).toBe(editInput);
    });
  });

  describe("cell selection still updates after preventDefault", () => {
    it("sets selection range on left-click of data cell", () => {
      const event = createMouseEvent(dataCell);
      expect(getDisplayedRange()).toBeNull();

      act(() => {
        hookApi.handleCellMouseDown(3, 2, event);
      });

      expect(getDisplayedRange()).toEqual({
        anchorRow: 3,
        anchorCol: 2,
        activeRow: 3,
        activeCol: 2,
      } satisfies CellRange);
    });

    it("sets selection range on right-click of data cell", () => {
      const event = createMouseEvent(dataCell, { button: 2 });

      act(() => {
        hookApi.handleCellMouseDown(5, 1, event);
      });

      expect(getDisplayedRange()).toEqual({
        anchorRow: 5,
        anchorCol: 1,
        activeRow: 5,
        activeCol: 1,
      } satisfies CellRange);
    });

    it("sets selection range even when clicking on an input target", () => {
      const event = createMouseEvent(editInput);

      act(() => {
        hookApi.handleCellMouseDown(4, 0, event);
      });

      expect(getDisplayedRange()).toEqual({
        anchorRow: 4,
        anchorCol: 0,
        activeRow: 4,
        activeCol: 0,
      } satisfies CellRange);
    });
  });

  describe("drag selection still works after preventDefault", () => {
    it("extends range with shift-click after initial selection", () => {
      const firstClick = createMouseEvent(dataCell);
      act(() => {
        hookApi.handleCellMouseDown(1, 1, firstClick);
      });

      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 1,
        activeRow: 1,
        activeCol: 1,
      });

      const shiftClick = createMouseEvent(dataCell, { shiftKey: true });
      act(() => {
        hookApi.handleCellMouseDown(4, 3, shiftClick);
      });

      expect(getDisplayedRange()).toEqual({
        anchorRow: 1,
        anchorCol: 1,
        activeRow: 4,
        activeCol: 3,
      } satisfies CellRange);
    });

    it("calls preventDefault for each non-input click in a drag sequence", () => {
      const firstEvent = createMouseEvent(dataCell);
      const shiftEvent = createMouseEvent(dataCell, { shiftKey: true });

      act(() => {
        hookApi.handleCellMouseDown(0, 0, firstEvent);
      });
      act(() => {
        hookApi.handleCellMouseDown(2, 2, shiftEvent);
      });

      expect(firstEvent.defaultPrevented).toBe(true);
      expect(shiftEvent.defaultPrevented).toBe(true);
    });
  });
});
