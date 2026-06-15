import type { MutableRefObject } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { isEditableElement } from "../../utils/editableElement";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

export interface CellRange {
  anchorRow: number;
  anchorCol: number;
  activeRow: number;
  activeCol: number;
}

export interface UseCellSelectionOptions {
  rowCount: number;
  colCount: number;
  minRow?: number;
  getCellValue: (rowIndex: number, colIndex: number) => unknown;
  scrollRef: React.RefObject<HTMLDivElement | null>;
  getCellFromPoint: (
    x: number,
    y: number,
  ) => { row: number; col: number } | null;
  onCopy?: (text: string) => void;
  onPaste?: () => void;
  isColumnCollapsed?: (colIndex: number) => boolean;
}

export interface UseCellSelectionReturn {
  range: CellRange | null;
  handleCellMouseDown: (
    rowIndex: number,
    colIndex: number,
    event: React.MouseEvent,
  ) => void;
  handleCellMouseEnter: (
    rowIndex: number,
    colIndex: number,
    event: React.MouseEvent,
  ) => void;
  handleKeyDown: (event: KeyboardEvent) => void;
  isCellSelected: (rowIndex: number, colIndex: number) => boolean;
  isCellAnchor: (rowIndex: number, colIndex: number) => boolean;
  clearSelection: () => void;
  copySelection: () => string | null;
  contextMenuCellRef: MutableRefObject<{
    row: number;
    col: number;
  } | null>;
}

function normalizeRange(range: CellRange): {
  minRow: number;
  maxRow: number;
  minCol: number;
  maxCol: number;
} {
  return {
    minRow: Math.min(range.anchorRow, range.activeRow),
    maxRow: Math.max(range.anchorRow, range.activeRow),
    minCol: Math.min(range.anchorCol, range.activeCol),
    maxCol: Math.max(range.anchorCol, range.activeCol),
  };
}

function serializeToTsv(
  range: CellRange,
  getCellValue: (rowIndex: number, colIndex: number) => unknown,
  isColumnCollapsed?: (colIndex: number) => boolean,
): string {
  const { minRow, maxRow, minCol, maxCol } = normalizeRange(range);
  const rows: string[] = [];

  for (let row = minRow; row <= maxRow; row++) {
    const cells: string[] = [];
    for (let col = minCol; col <= maxCol; col++) {
      if (isColumnCollapsed?.(col)) continue;
      const value = getCellValue(row, col);
      if (value === null || value === undefined) {
        cells.push("NULL");
      } else {
        cells.push(formatScalarValueForDisplay(value));
      }
    }
    rows.push(cells.join("\t"));
  }

  return rows.join("\n");
}

export function useCellSelection({
  rowCount,
  colCount,
  minRow = 0,
  getCellValue,
  scrollRef,
  getCellFromPoint,
  onCopy,
  onPaste,
  isColumnCollapsed,
}: UseCellSelectionOptions): UseCellSelectionReturn {
  const [range, setRange] = useState<CellRange | null>(null);
  const isDraggingRef = useRef(false);
  const isMouseDownRef = useRef(false);
  const mouseDownPosRef = useRef({ x: 0, y: 0 });
  const autoScrollIntervalRef = useRef<number | null>(null);

  const rangeRef = useRef(range);
  rangeRef.current = range;
  const rowCountRef = useRef(rowCount);
  rowCountRef.current = rowCount;
  const minRowRef = useRef(minRow);
  minRowRef.current = minRow;
  const colCountRef = useRef(colCount);
  colCountRef.current = colCount;
  const getCellValueRef = useRef(getCellValue);
  getCellValueRef.current = getCellValue;
  const onCopyRef = useRef(onCopy);
  onCopyRef.current = onCopy;
  const onPasteRef = useRef(onPaste);
  onPasteRef.current = onPaste;
  const isColumnCollapsedRef = useRef(isColumnCollapsed);
  isColumnCollapsedRef.current = isColumnCollapsed;

  const contextMenuCellRef = useRef<{ row: number; col: number } | null>(null);

  const DRAG_THRESHOLD = 5;

  const stopAutoScroll = useCallback(() => {
    if (autoScrollIntervalRef.current !== null) {
      clearInterval(autoScrollIntervalRef.current);
      autoScrollIntervalRef.current = null;
    }
  }, []);

  const startAutoScroll = useCallback(
    (clientX: number, clientY: number) => {
      stopAutoScroll();

      const scrollElement = scrollRef.current;
      if (!scrollElement) {
        return;
      }

      const rect = scrollElement.getBoundingClientRect();
      const edgeThreshold = 30;
      const scrollSpeed = 8;

      autoScrollIntervalRef.current = window.setInterval(() => {
        let dx = 0;
        let dy = 0;

        if (clientY < rect.top + edgeThreshold) {
          dy = -scrollSpeed;
        } else if (clientY > rect.bottom - edgeThreshold) {
          dy = scrollSpeed;
        }

        if (clientX < rect.left + edgeThreshold) {
          dx = -scrollSpeed;
        } else if (clientX > rect.right - edgeThreshold) {
          dx = scrollSpeed;
        }

        if (dx !== 0 || dy !== 0) {
          scrollElement.scrollBy(dx, dy);

          const updatedCell = getCellFromPoint(clientX, clientY);
          if (updatedCell) {
            setRange((prev) =>
              prev
                ? {
                    ...prev,
                    activeRow: updatedCell.row,
                    activeCol: updatedCell.col,
                  }
                : null,
            );
          }
        }
      }, 16);
    },
    [scrollRef, getCellFromPoint, stopAutoScroll],
  );

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      if (!isMouseDownRef.current) {
        return;
      }

      if (!isDraggingRef.current) {
        const dx = event.clientX - mouseDownPosRef.current.x;
        const dy = event.clientY - mouseDownPosRef.current.y;
        if (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD) {
          isDraggingRef.current = true;
        } else {
          return;
        }
      }

      const cell = getCellFromPoint(event.clientX, event.clientY);
      if (cell) {
        setRange((prev) =>
          prev
            ? {
                ...prev,
                activeRow: cell.row,
                activeCol: cell.col,
              }
            : null,
        );
      }

      startAutoScroll(event.clientX, event.clientY);
    };

    const handleMouseUp = () => {
      isMouseDownRef.current = false;
      isDraggingRef.current = false;
      stopAutoScroll();
    };

    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);

    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
      stopAutoScroll();
    };
  }, [getCellFromPoint, startAutoScroll, stopAutoScroll]);

  const handleCellMouseDown = useCallback(
    (rowIndex: number, colIndex: number, event: React.MouseEvent) => {
      const target = event.target as HTMLElement;
      const isInput =
        target.tagName === "INPUT" || target.tagName === "TEXTAREA";

      if (event.button !== 0) {
        if (event.button === 2) {
          if (isInput) {
            return;
          }

          const activeElement = document.activeElement as HTMLElement | null;
          if (activeElement && activeElement !== target) {
            activeElement.blur();
          }
          event.preventDefault();
          scrollRef.current?.focus({ preventScroll: true });

          contextMenuCellRef.current = { row: rowIndex, col: colIndex };

          if (range) {
            const { minRow, maxRow, minCol, maxCol } = normalizeRange(range);
            const inside =
              rowIndex >= minRow &&
              rowIndex <= maxRow &&
              colIndex >= minCol &&
              colIndex <= maxCol;

            if (!inside) {
              setRange({
                anchorRow: rowIndex,
                anchorCol: colIndex,
                activeRow: rowIndex,
                activeCol: colIndex,
              });
            }
          } else {
            setRange({
              anchorRow: rowIndex,
              anchorCol: colIndex,
              activeRow: rowIndex,
              activeCol: colIndex,
            });
          }
        }
        return;
      }

      if (!isInput) {
        event.preventDefault();
        const activeElement = document.activeElement as HTMLElement | null;
        if (activeElement && activeElement !== target) {
          activeElement.blur();
        }
        scrollRef.current?.focus({ preventScroll: true });
      }

      contextMenuCellRef.current = null;

      if (event.shiftKey && range) {
        setRange({
          ...range,
          activeRow: rowIndex,
          activeCol: colIndex,
        });
      } else {
        setRange({
          anchorRow: rowIndex,
          anchorCol: colIndex,
          activeRow: rowIndex,
          activeCol: colIndex,
        });
        isMouseDownRef.current = true;
        mouseDownPosRef.current = { x: event.clientX, y: event.clientY };
        isDraggingRef.current = false;
      }
    },
    [range, scrollRef],
  );

  const handleCellMouseEnter = useCallback(
    (rowIndex: number, colIndex: number) => {
      if (!isDraggingRef.current) {
        return;
      }

      setRange((prev) =>
        prev
          ? {
              ...prev,
              activeRow: rowIndex,
              activeCol: colIndex,
            }
          : null,
      );
    },
    [],
  );

  const handleKeyDown = useCallback((event: KeyboardEvent) => {
    const currentRange = rangeRef.current;
    const currentRowCount = rowCountRef.current;
    const currentMinRow = minRowRef.current;
    const currentColCount = colCountRef.current;
    const currentGetCellValue = getCellValueRef.current;
    const currentOnCopy = onCopyRef.current;
    const currentOnPaste = onPasteRef.current;
    const currentIsColumnCollapsed = isColumnCollapsedRef.current;

    const target = event.target as HTMLElement | null;
    const isCut = event.key === "x" || event.key === "X";
    const isCopy = event.key === "c" || event.key === "C";
    const isPaste = event.key === "v" || event.key === "V";
    const isClipboardShortcut =
      (isCopy || isCut || isPaste) && (event.ctrlKey || event.metaKey);

    if (scrollRef.current && !scrollRef.current.contains(target as Node)) {
      return;
    }

    if (
      isEditableElement(target) ||
      isEditableElement(document.activeElement)
    ) {
      return;
    }

    if (!currentRange) {
      return;
    }

    if (isClipboardShortcut) {
      event.stopPropagation();
    }

    const { activeRow, activeCol } = currentRange;

    switch (event.key) {
      case "ArrowUp": {
        event.preventDefault();
        const newRow = Math.max(currentMinRow, activeRow - 1);
        if (event.shiftKey) {
          setRange({ ...currentRange, activeRow: newRow });
        } else {
          setRange({
            anchorRow: newRow,
            anchorCol: activeCol,
            activeRow: newRow,
            activeCol: activeCol,
          });
        }
        break;
      }
      case "ArrowDown": {
        event.preventDefault();
        const newRow = Math.min(currentRowCount - 1, activeRow + 1);
        if (event.shiftKey) {
          setRange({ ...currentRange, activeRow: newRow });
        } else {
          setRange({
            anchorRow: newRow,
            anchorCol: activeCol,
            activeRow: newRow,
            activeCol: activeCol,
          });
        }
        break;
      }
      case "ArrowLeft": {
        event.preventDefault();
        const newCol = Math.max(0, activeCol - 1);
        if (event.shiftKey) {
          setRange({ ...currentRange, activeCol: newCol });
        } else {
          setRange({
            anchorRow: activeRow,
            anchorCol: newCol,
            activeRow: activeRow,
            activeCol: newCol,
          });
        }
        break;
      }
      case "ArrowRight": {
        event.preventDefault();
        const newCol = Math.min(currentColCount - 1, activeCol + 1);
        if (event.shiftKey) {
          setRange({ ...currentRange, activeCol: newCol });
        } else {
          setRange({
            anchorRow: activeRow,
            anchorCol: newCol,
            activeRow: activeRow,
            activeCol: newCol,
          });
        }
        break;
      }
      case "Tab": {
        event.preventDefault();
        const newCol = event.shiftKey
          ? Math.max(0, activeCol - 1)
          : Math.min(currentColCount - 1, activeCol + 1);
        setRange({
          anchorRow: activeRow,
          anchorCol: newCol,
          activeRow: activeRow,
          activeCol: newCol,
        });
        break;
      }
      case "Enter": {
        event.preventDefault();
        const newRow = Math.min(currentRowCount - 1, activeRow + 1);
        setRange({
          anchorRow: newRow,
          anchorCol: activeCol,
          activeRow: newRow,
          activeCol: activeCol,
        });
        break;
      }
      case "c":
      case "C": {
        if (event.ctrlKey || event.metaKey) {
          event.preventDefault();
          const text = serializeToTsv(currentRange, currentGetCellValue, currentIsColumnCollapsed);
          if (text && currentOnCopy) {
            currentOnCopy(text);
          } else if (text) {
            navigator.clipboard.writeText(text).catch(() => {});
          }
        }
        break;
      }
      case "x":
      case "X": {
        if (event.ctrlKey || event.metaKey) {
          event.preventDefault();
          const text = serializeToTsv(currentRange, currentGetCellValue, currentIsColumnCollapsed);
          if (text && currentOnCopy) {
            currentOnCopy(text);
          } else if (text) {
            navigator.clipboard.writeText(text).catch(() => {});
          }
        }
        break;
      }
      case "v":
      case "V": {
        if (event.ctrlKey || event.metaKey) {
          event.preventDefault();
          if (currentOnPaste) {
            currentOnPaste();
          }
        }
        break;
      }
      case "Escape": {
        setRange(null);
        break;
      }
    }
  }, []);

  const isCellSelected = useCallback(
    (rowIndex: number, colIndex: number): boolean => {
      if (!range) {
        return false;
      }
      const { minRow, maxRow, minCol, maxCol } = normalizeRange(range);
      return (
        rowIndex >= minRow &&
        rowIndex <= maxRow &&
        colIndex >= minCol &&
        colIndex <= maxCol
      );
    },
    [range],
  );

  const isCellAnchor = useCallback(
    (rowIndex: number, colIndex: number): boolean => {
      if (!range) {
        return false;
      }
      return range.anchorRow === rowIndex && range.anchorCol === colIndex;
    },
    [range],
  );

  const clearSelection = useCallback(() => {
    setRange(null);
  }, []);

  const copySelection = useCallback((): string | null => {
    if (!range) {
      return null;
    }
    return serializeToTsv(range, getCellValue, isColumnCollapsedRef.current);
  }, [range, getCellValue]);

  return {
    range,
    handleCellMouseDown,
    handleCellMouseEnter,
    handleKeyDown,
    isCellSelected,
    isCellAnchor,
    clearSelection,
    copySelection,
    contextMenuCellRef,
  };
}
