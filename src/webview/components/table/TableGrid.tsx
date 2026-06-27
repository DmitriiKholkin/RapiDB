import {
  type CellContext,
  flexRender,
  getCoreRowModel,
  type ColumnDef as TanColumnDef,
  type Column as TanStackColumn,
  type Row as TanStackRow,
  useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraft,
  type FilterDraftMap,
  isNumericCategory,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import type { QueryResult, QueryStatus } from "../../store";
import type {
  EditTarget,
  InsertDraftRow,
  PendingEdits,
  Row,
} from "../../types";
import { isEditableElement } from "../../utils/editableElement";
import { onMessage, postMessage } from "../../utils/messaging";
import {
  formatNormalizedPasteValue,
  type PasteValidationError,
  parseTsv,
  validatePasteData,
  validatePasteValue,
} from "../../utils/pasteUtils";
import { Icon } from "../Icon";
import { CellDisplay } from "./CellDisplay";
import { ColumnFilterControl } from "./ColumnFilterControl";
import { EditInput, valueToEditString } from "./EditInput";
import { GridContextMenu } from "./GridContextMenu";
import {
  getStructuredCellDialogValue,
  type StructuredCellDialogValue,
} from "./structuredCellDialog";
import { QueryModeTableGrid } from "./grid/QueryModeTableGrid";
import {
  buildColumnHeaderTitle,
  canEditColumn,
  FILTER_H,
  HEADER_H,
  INSERT_DEFAULT_SENTINEL,
  keyIconColor,
  ROW_H,
  SR_ONLY_STYLE,
  type TableSortState,
} from "./tableViewHelpers";
import { useCellSelection } from "./useCellSelection";
import { useColumnDragReorder } from "./useColumnDragReorder";

const TABLE_ROW_STYLE_ID = "rapidb-table-row-style";
if (
  typeof document !== "undefined" &&
  !document.getElementById(TABLE_ROW_STYLE_ID)
) {
  const styleElement = document.createElement("style");
  styleElement.id = TABLE_ROW_STYLE_ID;
  styleElement.textContent = [
    ".rdb-trow { transition: background 60ms; }",
    '.rdb-trow[data-even="true"]  { background: var(--vscode-editor-background); }',
    '.rdb-trow[data-even="false"] { background: var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04)); }',
    '.rdb-trow:not([data-selected="true"]):hover { background: var(--vscode-list-hoverBackground); }',
    ".rdb-tcell-selected { background: var(--vscode-editor-selectionBackground, rgba(38, 79, 120, 0.6)) !important; }",
    ".rdb-tcell-anchor { outline: 2px solid var(--vscode-focusBorder, #007fd4); outline-offset: -2px; }",
    ".rdb-tcell-border-top { box-shadow: inset 0 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-bottom { box-shadow: inset 0 -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-left { box-shadow: inset 2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-right { box-shadow: inset -2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-top.rdb-tcell-border-left { box-shadow: inset 2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-top.rdb-tcell-border-right { box-shadow: inset -2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-bottom.rdb-tcell-border-left { box-shadow: inset 2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-bottom.rdb-tcell-border-right { box-shadow: inset -2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
  ].join("\n");
  document.head.appendChild(styleElement);
}

const RESIZE_HANDLE_WIDTH = 12;
const RESIZE_HANDLE_OVERHANG = 6;

const QUERY_RESULTS_ROW_STYLE_ID = "rapidb-results-row-style";
if (
  typeof document !== "undefined" &&
  !document.getElementById(QUERY_RESULTS_ROW_STYLE_ID)
) {
  const styleElement = document.createElement("style");
  styleElement.id = QUERY_RESULTS_ROW_STYLE_ID;
  styleElement.textContent = [
    ".rdb-rrow { transition: background 60ms; }",
    '.rdb-rrow[data-even="true"]  { background: var(--vscode-editor-background); }',
    '.rdb-rrow[data-even="false"] { background: var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04)); }',
    ".rdb-rrow:hover { background: var(--vscode-list-hoverBackground); }",
    ".rdb-rcell-selected { background: var(--vscode-editor-selectionBackground, rgba(38, 79, 120, 0.6)) !important; }",
    ".rdb-rcell-anchor { outline: 2px solid var(--vscode-focusBorder, #007fd4); outline-offset: -2px; }",
    ".rdb-rcell-border-top { box-shadow: inset 0 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-bottom { box-shadow: inset 0 -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-left { box-shadow: inset 2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-right { box-shadow: inset -2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-top.rdb-rcell-border-left { box-shadow: inset 2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-top.rdb-rcell-border-right { box-shadow: inset -2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-bottom.rdb-rcell-border-left { box-shadow: inset 2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-bottom.rdb-rcell-border-right { box-shadow: inset -2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
  ].join("\n");
  document.head.appendChild(styleElement);
}

const COLUMN_DRAG_STYLE_ID = "rapidb-column-drag-style";
if (
  typeof document !== "undefined" &&
  !document.getElementById(COLUMN_DRAG_STYLE_ID)
) {
  const styleElement = document.createElement("style");
  styleElement.id = COLUMN_DRAG_STYLE_ID;
  styleElement.textContent = [
    "th[data-column-id] { cursor: grab; }",
    "th[data-column-id]:active { cursor: grabbing; }",
    '[data-column-dragging="true"] {',
    "  cursor: grabbing !important;",
    "  user-select: none !important;",
    "  background: var(--vscode-editor-selectionBackground, rgba(38, 79, 120, 0.6)) !important;",
    "  border-color: transparent !important;",
    "}",
    ".rapidb-column-drag-ghost {",
    "  position: fixed;",
    "  z-index: 10000;",
    "  pointer-events: none;",
    "  background: var(--vscode-editorGroupHeader-tabsBackground);",
    "  border: 1px solid var(--vscode-focusBorder);",
    "  box-shadow: 0 4px 12px rgba(0,0,0,0.3);",
    "  display: flex;",
    "  align-items: center;",
    "  gap: 4px;",
    "  padding: 0 8px;",
    "  height: 26px;",
    "  font-size: 12px;",
    "  font-weight: 600;",
    "  font-family: var(--vscode-editor-font-family, monospace);",
    "  white-space: nowrap;",
    "  opacity: 0.95;",
    "  border-radius: 3px;",
    "}",
  ].join("\n");
  document.head.appendChild(styleElement);
}

function isCollapsedWidth(width: number): boolean {
  return width <= 1;
}

function getDisplaySize(width: number): number {
  return isCollapsedWidth(width) ? 0 : width;
}

function getBaseTableStyle(width: number): React.CSSProperties {
  return {
    width,
    borderCollapse: "collapse",
    tableLayout: "fixed",
    fontSize: 12,
    fontFamily: "var(--vscode-editor-font-family, monospace)",
  };
}

function HeaderContent({
  isCollapsed,
  justifyContent,
  children,
}: {
  isCollapsed: boolean;
  justifyContent: "flex-start" | "center";
  children: React.ReactNode;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent,
        gap: 4,
        width: "100%",
        overflow: "hidden",
        opacity: isCollapsed ? 0 : 1,
        pointerEvents: isCollapsed ? "none" : "auto",
      }}
    >
      {children}
    </div>
  );
}

function ColumnResizeHandle({
  ariaLabel,
  onMouseDown,
  onTouchStart,
  onClick,
  isResizing,
  tabIndex,
}: {
  ariaLabel: string;
  onMouseDown: React.MouseEventHandler<HTMLButtonElement>;
  onTouchStart?: React.TouchEventHandler<HTMLButtonElement>;
  onClick?: React.MouseEventHandler<HTMLButtonElement>;
  isResizing: boolean;
  tabIndex?: number;
}) {
  return (
    <button
      type="button"
      aria-label={ariaLabel}
      tabIndex={tabIndex}
      onMouseDown={onMouseDown}
      onTouchStart={onTouchStart}
      onClick={onClick}
      style={{
        position: "absolute",
        right: -RESIZE_HANDLE_OVERHANG,
        top: 0,
        height: "100%",
        width: RESIZE_HANDLE_WIDTH,
        padding: 0,
        border: "none",
        cursor: "col-resize",
        userSelect: "none",
        touchAction: "none",
        zIndex: 3,
        background: isResizing ? "var(--vscode-focusBorder)" : "transparent",
      }}
    />
  );
}

function TopSpacerRow({
  height,
  colSpan,
}: {
  height: number;
  colSpan: number;
}) {
  if (height <= 0) {
    return null;
  }

  return (
    <tr>
      <td
        colSpan={colSpan}
        style={{
          height,
          padding: 0,
          border: "none",
        }}
      />
    </tr>
  );
}

function BottomSpacerRow({
  virtualItems,
  totalVirtualHeight,
  colSpan,
}: {
  virtualItems: readonly { end: number }[];
  totalVirtualHeight: number;
  colSpan: number;
}) {
  if (virtualItems.length === 0) {
    return null;
  }

  const lastVirtualRow = virtualItems[virtualItems.length - 1];
  const remainingHeight = totalVirtualHeight - lastVirtualRow.end;
  if (remainingHeight <= 0) {
    return null;
  }

  return (
    <tr>
      <td
        colSpan={colSpan}
        style={{
          height: remainingHeight,
          padding: 0,
          border: "none",
        }}
      />
    </tr>
  );
}

function toDraftEditInitialValue(value: unknown): string {
  if (value === INSERT_DEFAULT_SENTINEL) {
    return "";
  }

  return valueToEditString(value);
}

interface TableGridProps {
  canEditRows: boolean;
  canSelectAndDeleteRows: boolean;
  colSizes: Record<string, number>;
  columns: readonly ColumnMeta[];
  editCell: EditTarget | null;
  filterDrafts: FilterDraftMap;
  loading: boolean;
  newRow: InsertDraftRow | null;
  onCancelEdit: () => void;
  onBatchCellEdit: (
    edits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => void;
  onCommitCellEdit: (
    rowIdx: number,
    column: ColumnMeta,
    newVal: string,
    originalVal: unknown,
  ) => void;
  onCommitDraftCellEdit: (column: ColumnMeta, value: string) => void;
  onBatchDraftCellEdit: (
    edits: Array<{ column: ColumnMeta; newVal: string }>,
  ) => void;
  onMixedBatchEdit: (
    draftEdits: Array<{ column: ColumnMeta; newVal: string }>,
    persistedEdits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => void;
  onFilterDraftChange: (
    columnName: string,
    nextDraft: FilterDraft | undefined,
    options?: { applyImmediately?: boolean },
  ) => void;
  onSelectionChange: (selection: Set<number>) => void;
  onSort: (column: string) => void;
  onOpenStructuredCell: (options: {
    rowKind: "persisted" | "draft";
    rowIdx?: number;
    column: ColumnMeta;
    value: StructuredCellDialogValue;
    currentValue: unknown;
    originalValue: unknown;
    readOnly: boolean;
  }) => void;
  onStartDraftEdit: (column: ColumnMeta) => void;
  onStartEdit: (rowIdx: number, column: ColumnMeta) => void;
  pendingEdits: PendingEdits;
  rows: Row[];
  scrollRef: React.RefObject<HTMLDivElement | null>;
  selected: ReadonlySet<number>;
  sort: TableSortState;
  columnOrderRef?: React.MutableRefObject<string[]>;
  hiddenColumnIdsRef?: React.MutableRefObject<Set<string>>;
}

interface QueryModeTableGridProps {
  mode: "query";
  status: QueryStatus;
  result: QueryResult | null;
}

type UnifiedTableGridProps = TableGridProps | QueryModeTableGridProps;

export function TableGrid(props: UnifiedTableGridProps) {
  if ("mode" in props && props.mode === "query") {
    return <QueryModeTableGrid status={props.status} result={props.result} />;
  }

  const tableProps = props as TableGridProps;
  return <TableDataGrid {...tableProps} />;
}

function TableDataGrid({
  canEditRows,
  canSelectAndDeleteRows,
  colSizes,
  columns,
  editCell,
  filterDrafts,
  loading,
  newRow,
  onCancelEdit,
  onBatchCellEdit,
  onCommitCellEdit,
  onCommitDraftCellEdit,
  onBatchDraftCellEdit,
  onMixedBatchEdit,
  onFilterDraftChange,
  onSelectionChange,
  onSort,
  onOpenStructuredCell,
  onStartDraftEdit,
  onStartEdit,
  pendingEdits,
  rows,
  scrollRef,
  selected,
  sort,
  columnOrderRef: exportColumnOrderRef,
  hiddenColumnIdsRef,
}: TableGridProps) {
  const columnsMap = useMemo(
    () => new Map(columns.map((column) => [column.name, column])),
    [columns],
  );

  const columnsMapRef = useRef(columnsMap);
  columnsMapRef.current = columnsMap;

  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const columnOrderRef = useRef(columnOrder);
  columnOrderRef.current = columnOrder;
  const columnSizingRef = useRef(columnSizing);
  columnSizingRef.current = columnSizing;
  const colSizesRef = useRef(colSizes);
  colSizesRef.current = colSizes;

  useEffect(() => {
    if (exportColumnOrderRef) {
      exportColumnOrderRef.current = columnOrder;
    }
  }, [columnOrder, exportColumnOrderRef]);

  useEffect(() => {
    if (!hiddenColumnIdsRef) return;
    const hidden = new Set<string>();
    for (const id of columnOrder) {
      const size = columnSizing[id] ?? colSizes[id] ?? 160;
      if (size <= 1) {
        hidden.add(id);
      }
    }
    hiddenColumnIdsRef.current = hidden;
  }, [columnOrder, columnSizing, colSizes, hiddenColumnIdsRef]);

  const containerRef = React.useRef<HTMLDivElement>(null);
  const [pasteErrors, setPasteErrors] = useState<PasteValidationError[]>([]);
  const newRowRef = useRef(newRow);
  newRowRef.current = newRow;
  const scrollToCellRef = useRef<(row: number, col: number) => void>(() => {});

  const dataColCount = columns.length;
  const selColOffset = canSelectAndDeleteRows ? 1 : 0;
  const totalColCount = dataColCount + selColOffset;

  const onCellNavigate = useCallback((row: number, col: number) => {
    scrollToCellRef.current(row, col);
  }, []);

  const getCellValue = useCallback(
    (rowIndex: number, colIndex: number) => {
      const colId = columnOrderRef.current[colIndex];
      if (!colId || colId === "__sel") return undefined;
      if (rowIndex === -1) {
        const draft = newRowRef.current;
        if (!draft) return undefined;
        const dv = draft[colId]?.value;
        if (dv === INSERT_DEFAULT_SENTINEL) return undefined;
        if (dv === NULL_SENTINEL) return null;
        return dv;
      }
      const row = rows[rowIndex];
      if (!row) return undefined;
      return row[colId];
    },
    [rows],
  );

  const getCellFromPoint = useCallback((x: number, y: number) => {
    const element = document.elementFromPoint(x, y) as HTMLElement | null;
    if (!element) return null;
    const td = element.closest("td[data-row][data-col]");
    if (!td) return null;
    const row = Number.parseInt(td.getAttribute("data-row") ?? "", 10);
    const col = Number.parseInt(td.getAttribute("data-col") ?? "", 10);
    if (Number.isNaN(row) || Number.isNaN(col)) return null;
    return { row, col };
  }, []);

  const handleCopyText = useCallback((text: string) => {
    postMessage("writeClipboard", { text });
  }, []);

  const handlePaste = useCallback(() => {
    if (!canEditRows) return;
    postMessage("readClipboard");
  }, [canEditRows]);

  const isColumnCollapsed = useCallback(
    (visualColIndex: number) => {
      const colId = columnOrderRef.current[visualColIndex];
      if (!colId || colId === "__sel") return false;
      const size = columnSizing[colId] ?? colSizes[colId] ?? 160;
      return isCollapsedWidth(size);
    },
    [columnSizing, colSizes],
  );

  const selection = useCellSelection({
    rowCount: rows.length,
    colCount: totalColCount,
    minRow: newRow ? -1 : 0,
    minCol: selColOffset,
    getCellValue,
    scrollRef,
    getCellFromPoint,
    onCopy: handleCopyText,
    onPaste: handlePaste,
    isColumnCollapsed,
    onCellNavigate,
  });

  const selectionRangeRef = useRef(selection.range);
  selectionRangeRef.current = selection.range;

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      selection.handleKeyDown(event);
    };
    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [selection.handleKeyDown]);

  useEffect(() => {
    const handlePasteEvent = (event: ClipboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (
        isEditableElement(target) ||
        isEditableElement(document.activeElement)
      ) {
        return;
      }
      if (!canEditRows || !selectionRangeRef.current) return;
      event.preventDefault();
      event.stopPropagation();
      handlePaste();
    };
    window.addEventListener("paste", handlePasteEvent, true);
    return () => window.removeEventListener("paste", handlePasteEvent, true);
  }, [canEditRows, handlePaste]);

  const handleCopy = useCallback(() => {
    const text = selection.copySelection();
    if (text) {
      postMessage("writeClipboard", { text });
    }
  }, [selection.copySelection]);

  useEffect(() => {
    const unsubscribe = onMessage<string>("clipboardText", (text) => {
      if (!canEditRows || !selectionRangeRef.current) return;

      const pasteData = parseTsv(text);
      if (pasteData.rows.length === 0) return;

      const ctxCell = selection.contextMenuCellRef.current;
      const startRow = ctxCell
        ? ctxCell.row
        : selectionRangeRef.current.anchorRow;
      const anchorCol = ctxCell
        ? ctxCell.col
        : selectionRangeRef.current.anchorCol;
      const startCol = anchorCol - selColOffset;

      selection.contextMenuCellRef.current = null;

      // Build the list of visible data columns starting from the anchor position,
      // skipping collapsed (hidden) columns and the selection column.
      const visiblePasteColumns: ColumnMeta[] = [];
      for (let i = anchorCol; i < columnOrderRef.current.length; i++) {
        const colId = columnOrderRef.current[i];
        if (colId === "__sel") continue;
        const size =
          columnSizingRef.current[colId] ?? colSizesRef.current[colId] ?? 160;
        if (isCollapsedWidth(size)) continue;
        const meta = columnsMapRef.current.get(colId);
        if (meta) {
          visiblePasteColumns.push(meta);
        }
      }

      if (visiblePasteColumns.length === 0) {
        setPasteErrors([
          {
            rowIndex: startRow,
            columnIndex: anchorCol,
            columnName: "",
            value: "",
            message: "No visible columns to paste into",
          },
        ]);
        return;
      }

      if (startCol < 0) {
        setPasteErrors([
          {
            rowIndex: startRow,
            columnIndex: ctxCell
              ? ctxCell.col
              : selectionRangeRef.current.anchorCol,
            columnName: "",
            value: "",
            message: "Cannot paste into selection column",
          },
        ]);
        return;
      }

      if (startRow === -1) {
        const draft = newRowRef.current;
        if (!draft) return;

        const errors: PasteValidationError[] = [];
        const normalizedCells: Array<{
          targetRow: number;
          column: ColumnMeta;
          value: string;
          normalized: unknown;
        }> = [];

        for (let r = 0; r < pasteData.rows.length; r++) {
          const row = pasteData.rows[r];
          const targetRow = startRow + r;

          for (let c = 0; c < row.length; c++) {
            const value = row[c];
            const targetCol = startCol + c;
            const column = visiblePasteColumns[c];
            if (!column) continue;

            const validation = validatePasteValue(value, column);
            if (!validation.valid) {
              errors.push({
                rowIndex: targetRow,
                columnIndex: targetCol + selColOffset,
                columnName: column.name,
                value,
                message: validation.error ?? "Validation failed",
              });
              continue;
            }

            if (targetRow >= rows.length) {
              errors.push({
                rowIndex: targetRow,
                columnIndex: targetCol + selColOffset,
                columnName: column.name,
                value,
                message: `Row ${targetRow + 1} does not exist`,
              });
              continue;
            }

            normalizedCells.push({
              targetRow,
              column,
              value,
              normalized: validation.coercedValue,
            });
          }
        }

        if (errors.length > 0) {
          setPasteErrors(errors);
          return;
        }

        setPasteErrors([]);

        const batchEdits: Array<{
          rowIdx: number;
          column: ColumnMeta;
          newVal: string;
          originalVal: unknown;
        }> = [];

        const draftEdits: Array<{
          column: ColumnMeta;
          newVal: string;
        }> = [];

        for (const cell of normalizedCells) {
          const coercedValue = formatNormalizedPasteValue(
            cell.value,
            cell.normalized,
          );

          if (cell.targetRow === -1) {
            draftEdits.push({ column: cell.column, newVal: coercedValue });
          } else {
            const originalValue = rows[cell.targetRow]?.[cell.column.name];
            batchEdits.push({
              rowIdx: cell.targetRow,
              column: cell.column,
              newVal: coercedValue,
              originalVal: originalValue,
            });
          }
        }

        if (draftEdits.length > 0 && batchEdits.length > 0) {
          onMixedBatchEdit(draftEdits, batchEdits);
        } else if (draftEdits.length > 0) {
          onBatchDraftCellEdit(draftEdits);
        } else if (batchEdits.length > 0) {
          onBatchCellEdit(batchEdits);
        }

        return;
      }

      const validationResult = validatePasteData(
        pasteData,
        startRow,
        0,
        visiblePasteColumns,
        rows.length,
      );

      if (validationResult.errors.length > 0) {
        setPasteErrors(validationResult.errors);
        return;
      }

      setPasteErrors([]);

      const edits: Array<{
        rowIdx: number;
        column: ColumnMeta;
        newVal: string;
        originalVal: unknown;
      }> = [];

      for (let r = 0; r < validationResult.rows.length; r++) {
        const normalizedRow = validationResult.rows[r];
        const targetRow = startRow + r;

        for (const cell of normalizedRow) {
          const originalValue = rows[targetRow]?.[cell.column.name];
          const coercedValue = formatNormalizedPasteValue(
            cell.value,
            cell.normalized,
          );

          edits.push({
            rowIdx: targetRow,
            column: cell.column,
            newVal: coercedValue,
            originalVal: originalValue,
          });
        }
      }

      onBatchCellEdit(edits);
    });

    return unsubscribe;
  }, [
    canEditRows,
    rows,
    selColOffset,
    onBatchCellEdit,
    onBatchDraftCellEdit,
    onMixedBatchEdit,
    selection.contextMenuCellRef.current,
    selection.contextMenuCellRef,
  ]);

  useEffect(() => {
    selection.clearSelection();
  }, [selection.clearSelection]);

  useEffect(() => {
    setColumnOrder([
      ...(canSelectAndDeleteRows ? ["__sel"] : []),
      ...columns.map((column) => column.name),
    ]);
  }, [columns, canSelectAndDeleteRows]);

  const wasDraggedRef = React.useRef(false);

  const { onHeaderMouseDown: handleHeaderMouseDown } = useColumnDragReorder({
    getColumnOrder: () => columnOrderRef.current,
    setColumnOrder: (updater) => setColumnOrder(updater),
    excludedIds: ["__sel"],
    onDragActivated: () => {
      wasDraggedRef.current = true;
    },
    onDragEnded: (activated) => {
      if (!activated) {
        wasDraggedRef.current = false;
      }
    },
  });

  const tanColumns = useMemo<TanColumnDef<Row>[]>(
    () => [
      ...(canSelectAndDeleteRows
        ? [
            {
              id: "__sel",
              size: 36,
              header: () => (
                <input
                  type="checkbox"
                  aria-label="Select all rows"
                  checked={rows.length > 0 && selected.size === rows.length}
                  ref={(element) => {
                    if (element) {
                      element.indeterminate =
                        selected.size > 0 && selected.size < rows.length;
                    }
                  }}
                  onChange={(event) =>
                    onSelectionChange(
                      event.target.checked
                        ? new Set(rows.map((_, index) => index))
                        : new Set(),
                    )
                  }
                  style={{
                    cursor: "pointer",
                    accentColor: "var(--vscode-button-background)",
                    margin: 0,
                  }}
                />
              ),
              cell: ({ row }: CellContext<Row, unknown>) => (
                <input
                  type="checkbox"
                  aria-label={`Select row ${row.index + 1}`}
                  checked={selected.has(row.index)}
                  onChange={(event: React.ChangeEvent<HTMLInputElement>) => {
                    const nextSelection = new Set(selected);
                    if (event.target.checked) {
                      nextSelection.add(row.index);
                    } else {
                      nextSelection.delete(row.index);
                    }
                    onSelectionChange(nextSelection);
                  }}
                  style={{
                    cursor: "pointer",
                    accentColor: "var(--vscode-button-background)",
                    margin: 0,
                  }}
                />
              ),
            } as TanColumnDef<Row>,
          ]
        : []),
      ...columns.map(
        (column): TanColumnDef<Row> => ({
          id: column.name,
          accessorKey: column.name,
          meta: column,
          size: colSizes[column.name] ?? colSizes[`${column.name}key`] ?? 160,
          minSize: 1,
          maxSize: 800,
          header: () => (
            <span
              title={buildColumnHeaderTitle(column)}
              style={{ display: "flex", alignItems: "center", gap: 4 }}
            >
              {column.name}
              {column.isPrimaryKey && (
                <Icon
                  name="key"
                  size={13}
                  color={keyIconColor(column.primaryKeyRole)}
                  style={{ marginLeft: 2 }}
                />
              )}
              {column.isForeignKey && (
                <Icon
                  name="key"
                  size={13}
                  color="var(--vscode-foreground)"
                  style={{ marginLeft: 2 }}
                />
              )}
            </span>
          ),
          cell: ({ row, getValue }) => {
            const rowIdx = row.index;
            const isEditing =
              editCell?.kind === "persisted" &&
              editCell.rowIdx === rowIdx &&
              editCell.col === column.name;
            const pendingRow = pendingEdits.get(rowIdx);
            const hasPending = pendingRow?.has(column.name) ?? false;
            const pendingValue = pendingRow?.get(column.name);
            const displayValue = hasPending ? pendingValue : getValue();

            if (isEditing) {
              const startValue = hasPending ? pendingValue : getValue();
              const startString = valueToEditString(startValue);
              return (
                <EditInput
                  initial={startString}
                  nullable={column.nullable}
                  category={column.category}
                  readOnly={!canEditRows}
                  onCommit={(value) =>
                    onCommitCellEdit(rowIdx, column, value, getValue())
                  }
                  onCancel={onCancelEdit}
                />
              );
            }

            return (
              <CellDisplay
                value={displayValue}
                isPending={hasPending}
                category={column.category}
                nativeType={column.nativeType}
              />
            );
          },
        }),
      ),
    ],
    [
      canEditRows,
      canSelectAndDeleteRows,
      colSizes,
      columns,
      editCell,
      onCancelEdit,
      onCommitCellEdit,
      onSelectionChange,
      pendingEdits,
      rows,
      selected,
    ],
  );

  const tanTable = useReactTable({
    data: rows,
    columns: tanColumns,
    state: { columnSizing, columnOrder },
    onColumnSizingChange: setColumnSizing,
    onColumnOrderChange: setColumnOrder,
    getCoreRowModel: getCoreRowModel(),
    columnResizeMode: "onChange",
    enableColumnResizing: true,
  });
  const tableRows = tanTable.getRowModel().rows;
  const visibleColumns = tanTable.getVisibleLeafColumns();
  const hasDraftRow = newRow !== null;
  const virtualizer = useVirtualizer({
    count: tableRows.length + (hasDraftRow ? 1 : 0),
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_H,
    overscan: 15,
  });
  const virtualItems = virtualizer.getVirtualItems();
  const totalVirtualHeight = virtualizer.getTotalSize();

  scrollToCellRef.current = (row: number, col: number) => {
    virtualizer.scrollToIndex(row, { align: "auto" });
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (col === selColOffset) {
          scrollRef.current?.scrollTo({ left: 0 });
        }
        const cell = scrollRef.current?.querySelector(
          `td[data-row="${row}"][data-col="${col}"]`,
        ) as HTMLElement | null;
        cell?.scrollIntoView({ block: "nearest", inline: "nearest" });
      });
    });
  };

  return (
    <div
      ref={containerRef}
      style={{ flex: 1, overflow: "hidden", position: "relative" }}
    >
      <div
        ref={scrollRef}
        style={{
          width: "100%",
          height: "100%",
          overflow: "auto",
          outline: "none",
        }}
        // biome-ignore lint/a11y/noNoninteractiveTabindex: needed for webview keyboard focus
        tabIndex={0}
      >
        <table style={getBaseTableStyle(tanTable.getTotalSize())}>
          <thead>
            {tanTable.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const isSelectionColumn = header.column.id === "__sel";
                  const columnId = header.column.id;
                  const headerSize = header.getSize();
                  const isCollapsed = isCollapsedWidth(headerSize);
                  const displayHeaderSize = getDisplaySize(headerSize);
                  const isSorted = sort?.column === columnId;
                  const sortDirection = isSorted
                    ? (sort?.direction ?? null)
                    : null;
                  const columnMeta = header.column.columnDef.meta as
                    | ColumnMeta
                    | undefined;

                  return (
                    <th
                      key={header.id}
                      data-column-id={columnId}
                      title={
                        isSelectionColumn || !columnMeta
                          ? undefined
                          : buildColumnHeaderTitle(columnMeta)
                      }
                      style={{
                        width: displayHeaderSize,
                        height: HEADER_H,
                        padding: isSelectionColumn
                          ? "0 6px"
                          : isCollapsed
                            ? 0
                            : "0 8px",
                        textAlign: isSelectionColumn ? "center" : "left",
                        background: isSorted
                          ? "var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.1))"
                          : "var(--vscode-editorGroupHeader-tabsBackground)",
                        borderRight: "1px solid var(--vscode-panel-border)",
                        borderLeft: "1px solid var(--vscode-panel-border)",
                        position: "sticky",
                        top: 0,
                        zIndex: 2,
                        whiteSpace: "nowrap",
                        overflow: "visible",
                        fontWeight: 600,
                        boxSizing: "border-box",
                        userSelect: "none",
                        cursor: isSelectionColumn ? "default" : "grab",
                      }}
                      onMouseDown={(event) => {
                        if (!isSelectionColumn) {
                          handleHeaderMouseDown(columnId, event);
                        }
                      }}
                      onClick={() => {
                        if (!isSelectionColumn) {
                          const wasDragged = wasDraggedRef.current;
                          wasDraggedRef.current = false;
                          if (!wasDragged) {
                            onSort(columnId);
                          }
                        }
                      }}
                    >
                      <HeaderContent
                        isCollapsed={isCollapsed}
                        justifyContent={
                          isSelectionColumn ? "center" : "flex-start"
                        }
                      >
                        <span
                          style={{
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                          }}
                        >
                          {flexRender(
                            header.column.columnDef.header,
                            header.getContext(),
                          )}
                        </span>
                        {!isSelectionColumn && (
                          <span
                            style={{
                              opacity: isSorted ? 1 : 0.25,
                              fontSize: 10,
                              flexShrink: 0,
                            }}
                          >
                            {sortDirection === "asc" ? (
                              <Icon name="triangle-up" size={10} />
                            ) : sortDirection === "desc" ? (
                              <Icon name="triangle-down" size={10} />
                            ) : (
                              <Icon name="unfold" size={10} />
                            )}
                          </span>
                        )}
                      </HeaderContent>
                      {!isSelectionColumn && header.column.getCanResize() && (
                        <ColumnResizeHandle
                          ariaLabel={`Resize ${columnId} column`}
                          tabIndex={-1}
                          onMouseDown={(event) => {
                            event.preventDefault();
                            event.stopPropagation();

                            const startSize = Math.max(headerSize, 1);
                            const startX = event.clientX;

                            const applyWidth = (clientX: number) => {
                              const nextWidth = Math.max(
                                1,
                                startSize + (clientX - startX),
                              );
                              setColumnSizing((previous) => ({
                                ...previous,
                                [columnId]: nextWidth,
                              }));
                            };

                            const onMove = (moveEvent: MouseEvent) => {
                              applyWidth(moveEvent.clientX);
                            };

                            const onUp = (upEvent: MouseEvent) => {
                              applyWidth(upEvent.clientX);
                              document.removeEventListener("mousemove", onMove);
                              document.removeEventListener("mouseup", onUp);
                            };

                            document.addEventListener("mousemove", onMove);
                            document.addEventListener("mouseup", onUp);
                          }}
                          onClick={(event) => event.stopPropagation()}
                          isResizing={header.column.getIsResizing()}
                        />
                      )}
                    </th>
                  );
                })}
              </tr>
            ))}
            <tr>
              {tanTable.getHeaderGroups()[0]?.headers.map((header) => {
                const isSelectionColumn = header.column.id === "__sel";
                const column = columnsMap.get(header.column.id);
                const headerSize = header.getSize();
                const isCollapsed = isCollapsedWidth(headerSize);
                const displayHeaderSize = getDisplaySize(headerSize);

                return (
                  <th
                    key={`${header.id}_f`}
                    data-column-id={header.column.id}
                    style={{
                      width: displayHeaderSize,
                      height: FILTER_H,
                      padding: isSelectionColumn || isCollapsed ? 0 : "2px 4px",
                      background:
                        "var(--vscode-editorGroupHeader-tabsBackground)",
                      borderRight: "1px solid var(--vscode-panel-border)",
                      borderLeft: "1px solid var(--vscode-panel-border)",
                      borderBottom: "1px solid var(--vscode-panel-border)",
                      position: "sticky",
                      top: HEADER_H,
                      zIndex: 2,
                      boxSizing: "border-box",
                      boxShadow:
                        "0 -1px 0 0 var(--vscode-editorGroupHeader-tabsBackground)",
                      overflow: "visible",
                    }}
                  >
                    {isSelectionColumn || isCollapsed ? (
                      <span style={SR_ONLY_STYLE}>Selection column</span>
                    ) : column ? (
                      <ColumnFilterControl
                        column={column}
                        draft={filterDrafts[header.column.id]}
                        onChange={(nextDraft, options) =>
                          onFilterDraftChange(
                            header.column.id,
                            nextDraft,
                            options,
                          )
                        }
                      />
                    ) : null}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            <TopSpacerRow
              colSpan={tanColumns.length}
              height={virtualItems[0]?.start ?? 0}
            />
            {virtualItems.map((virtualRow) => {
              if (hasDraftRow && virtualRow.index === 0) {
                return (
                  <DraftTableRow
                    key={virtualRow.key}
                    columns={columns}
                    visibleColumns={visibleColumns}
                    draft={newRow}
                    editingCol={
                      editCell?.kind === "draft" ? editCell.col : null
                    }
                    onOpenStructuredCell={onOpenStructuredCell}
                    onStartEdit={onStartDraftEdit}
                    onCommit={onCommitDraftCellEdit}
                    onCancelEdit={onCancelEdit}
                    selection={selection}
                  />
                );
              }

              const persistedIndex = hasDraftRow
                ? virtualRow.index - 1
                : virtualRow.index;
              const row = tableRows[persistedIndex];
              const isSelected = selected.has(persistedIndex);
              const editingCol =
                editCell?.kind === "persisted" &&
                editCell.rowIdx === persistedIndex
                  ? editCell.col
                  : null;

              return (
                <TableRow
                  key={virtualRow.key}
                  row={row}
                  visualIndex={virtualRow.index}
                  rowIndex={persistedIndex}
                  isSelected={isSelected}
                  pendingCols={pendingEdits.get(persistedIndex)}
                  columnsMap={columnsMap}
                  editingCol={editingCol}
                  onOpenStructuredCell={onOpenStructuredCell}
                  onStartEdit={onStartEdit}
                  readOnly={!canEditRows}
                  selection={selection}
                />
              );
            })}
            <BottomSpacerRow
              virtualItems={virtualItems}
              totalVirtualHeight={totalVirtualHeight}
              colSpan={tanColumns.length}
            />
          </tbody>
        </table>
      </div>

      {!loading && rows.length === 0 && !hasDraftRow && (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            height: 200,
            opacity: 0.4,
            userSelect: "none",
          }}
        >
          <Icon name="inbox" size={28} style={{ opacity: 0.4 }} />
          <div style={{ fontSize: 13, marginTop: 8 }}>No rows found</div>
        </div>
      )}

      {pasteErrors.length > 0 && (
        <div
          style={{
            position: "absolute",
            bottom: 0,
            left: 0,
            right: 0,
            padding: "8px 12px",
            background:
              "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.15))",
            borderTop:
              "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.5))",
            color: "var(--vscode-errorForeground)",
            fontSize: 12,
            zIndex: 10,
            display: "flex",
            alignItems: "flex-start",
            gap: 8,
          }}
        >
          <Icon
            name="error"
            size={14}
            style={{ flexShrink: 0, marginTop: 2 }}
          />
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 600, marginBottom: 4 }}>
              Paste failed: {pasteErrors.length} error
              {pasteErrors.length > 1 ? "s" : ""}
            </div>
            <div style={{ opacity: 0.9, maxHeight: 60, overflow: "auto" }}>
              {pasteErrors.slice(0, 3).map((error) => (
                <div
                  key={`${error.rowIndex}-${error.columnIndex}-${error.columnName}`}
                  style={{ marginBottom: 2 }}
                >
                  {error.columnName
                    ? `Row ${error.rowIndex + 1}, ${error.columnName}: `
                    : `Row ${error.rowIndex + 1}: `}
                  {error.message}
                </div>
              ))}
              {pasteErrors.length > 3 && (
                <div style={{ opacity: 0.7, fontStyle: "italic" }}>
                  ...and {pasteErrors.length - 3} more error
                  {pasteErrors.length - 3 > 1 ? "s" : ""}
                </div>
              )}
            </div>
          </div>
          <button
            type="button"
            onClick={() => setPasteErrors([])}
            style={{
              background: "transparent",
              border: "none",
              color: "var(--vscode-errorForeground)",
              cursor: "pointer",
              padding: "2px 6px",
              fontSize: 14,
              opacity: 0.7,
            }}
            title="Dismiss"
          >
            ×
          </button>
        </div>
      )}

      <GridContextMenu
        containerRef={containerRef}
        onCopy={handleCopy}
        onPaste={handlePaste}
        canPaste={canEditRows}
      />
    </div>
  );
}

function TableRow({
  row,
  visualIndex,
  rowIndex,
  isSelected,
  pendingCols,
  columnsMap,
  editingCol,
  onOpenStructuredCell,
  onStartEdit,
  readOnly,
  selection,
}: {
  row: TanStackRow<Row>;
  visualIndex: number;
  rowIndex: number;
  isSelected: boolean;
  pendingCols?: Map<string, unknown>;
  columnsMap: Map<string, ColumnMeta>;
  editingCol: string | null;
  onOpenStructuredCell: (options: {
    rowKind: "persisted" | "draft";
    rowIdx?: number;
    column: ColumnMeta;
    value: StructuredCellDialogValue;
    currentValue: unknown;
    originalValue: unknown;
    readOnly: boolean;
  }) => void;
  onStartEdit: (rowIndex: number, column: ColumnMeta) => void;
  readOnly: boolean;
  selection: {
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
    isCellSelected: (rowIndex: number, colIndex: number) => boolean;
    isCellAnchor: (rowIndex: number, colIndex: number) => boolean;
    range: {
      anchorRow: number;
      anchorCol: number;
      activeRow: number;
      activeCol: number;
    } | null;
  };
}) {
  const { range } = selection;
  const minRow = range ? Math.min(range.anchorRow, range.activeRow) : -1;
  const maxRow = range ? Math.max(range.anchorRow, range.activeRow) : -1;
  const minCol = range ? Math.min(range.anchorCol, range.activeCol) : -1;
  const maxCol = range ? Math.max(range.anchorCol, range.activeCol) : -1;

  return (
    <tr
      className="rdb-trow"
      data-even={String(visualIndex % 2 === 0)}
      data-editing-col={editingCol ?? ""}
      data-selected={String(isSelected)}
      style={{
        height: ROW_H,
        ...(isSelected
          ? { background: "var(--vscode-list-activeSelectionBackground)" }
          : {}),
      }}
    >
      {row.getVisibleCells().map((cell, cellIndex) => {
        const columnId = cell.column.id;
        const columnDef = columnsMap.get(columnId);
        const isPrimaryKey = columnDef?.isPrimaryKey ?? false;
        const cellSize = cell.column.getSize();
        const isCollapsed = isCollapsedWidth(cellSize);
        const displayCellSize = isCollapsed ? 0 : cellSize;
        const primaryKeyLabel = isPrimaryKey
          ? columnDef?.primaryKeyRole === "sort"
            ? "Sort key"
            : columnDef?.primaryKeyRole === "partition"
              ? "Partition key"
              : "Primary key"
          : undefined;
        const isSelectionColumn = columnId === "__sel";
        const isCellPending = pendingCols?.has(columnId) ?? false;
        const isEditing = columnId === editingCol;
        const canOpenCellEditor =
          !isSelectionColumn && canEditColumn(columnDef);
        const currentValue = isCellPending
          ? pendingCols?.get(columnId)
          : cell.getValue();

        const isDataCol = !isSelectionColumn;

        const isCellSelected =
          isDataCol && selection.isCellSelected(rowIndex, cellIndex);
        const isCellAnchor =
          isDataCol && selection.isCellAnchor(rowIndex, cellIndex);
        const isTopBorder = isCellSelected && rowIndex === minRow;
        const isBottomBorder = isCellSelected && rowIndex === maxRow;
        const isLeftBorder = isCellSelected && cellIndex === minCol;
        const isRightBorder = isCellSelected && cellIndex === maxCol;

        const cellClasses = ["rdb-trow-cell"];
        if (isCellSelected) cellClasses.push("rdb-tcell-selected");
        if (isCellAnchor) cellClasses.push("rdb-tcell-anchor");
        if (isTopBorder) cellClasses.push("rdb-tcell-border-top");
        if (isBottomBorder) cellClasses.push("rdb-tcell-border-bottom");
        if (isLeftBorder) cellClasses.push("rdb-tcell-border-left");
        if (isRightBorder) cellClasses.push("rdb-tcell-border-right");

        return (
          <td
            key={cell.id}
            data-row={isDataCol ? rowIndex : undefined}
            data-col={isDataCol ? cellIndex : undefined}
            className={cellClasses.join(" ")}
            style={{
              width: displayCellSize,
              height: ROW_H,
              padding: isEditing
                ? "0"
                : isSelectionColumn
                  ? "0 6px"
                  : isCollapsed
                    ? 0
                    : "0 0 0 8px",
              textAlign: isSelectionColumn
                ? "center"
                : columnDef && isNumericCategory(columnDef.category)
                  ? "right"
                  : "left",
              border: "1px solid var(--vscode-panel-border)",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              boxSizing: "border-box",
              verticalAlign: "middle",
              cursor: canOpenCellEditor && !isCollapsed ? "pointer" : "default",
              userSelect: isSelectionColumn ? "auto" : "none",
              background: isCellPending ? "rgba(200, 150, 0, 0.23)" : undefined,
            }}
            title={
              !isCollapsed && isPrimaryKey
                ? `${primaryKeyLabel}: ${String(cell.getValue())}`
                : undefined
            }
            onMouseDown={(event) => {
              if (isDataCol && !isCollapsed && !isEditing) {
                selection.handleCellMouseDown(rowIndex, cellIndex, event);
              }
            }}
            onMouseEnter={(event) => {
              if (isDataCol) {
                selection.handleCellMouseEnter(rowIndex, cellIndex, event);
              }
            }}
            onDoubleClick={() => {
              if (
                !isEditing &&
                columnDef &&
                canOpenCellEditor &&
                !isCollapsed
              ) {
                const structuredValue = getStructuredCellDialogValue(
                  currentValue,
                  columnDef,
                );

                if (structuredValue) {
                  onOpenStructuredCell({
                    rowKind: "persisted",
                    rowIdx: rowIndex,
                    column: columnDef,
                    value: structuredValue,
                    currentValue,
                    originalValue: cell.getValue(),
                    readOnly,
                  });
                  return;
                }

                onStartEdit(rowIndex, columnDef);
              }
            }}
          >
            {!isCollapsed &&
              flexRender(cell.column.columnDef.cell, cell.getContext())}
          </td>
        );
      })}
    </tr>
  );
}

function DraftTableRow({
  columns,
  visibleColumns,
  draft,
  editingCol,
  onOpenStructuredCell,
  onStartEdit,
  onCommit,
  onCancelEdit,
  selection,
}: {
  columns: readonly ColumnMeta[];
  visibleColumns: readonly TanStackColumn<Row, unknown>[];
  draft: InsertDraftRow;
  editingCol: string | null;
  onOpenStructuredCell: (options: {
    rowKind: "persisted" | "draft";
    rowIdx?: number;
    column: ColumnMeta;
    value: StructuredCellDialogValue;
    currentValue: unknown;
    originalValue: unknown;
    readOnly: boolean;
  }) => void;
  onStartEdit: (column: ColumnMeta) => void;
  onCommit: (column: ColumnMeta, value: string) => void;
  onCancelEdit: () => void;
  selection?: {
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
    isCellSelected: (rowIndex: number, colIndex: number) => boolean;
    isCellAnchor: (rowIndex: number, colIndex: number) => boolean;
    range: {
      anchorRow: number;
      anchorCol: number;
      activeRow: number;
      activeCol: number;
    } | null;
  };
}) {
  const columnsMap = useMemo(
    () => new Map(columns.map((column) => [column.name, column])),
    [columns],
  );

  return (
    <tr className="rdb-trow" data-even="true" data-selected="false">
      {visibleColumns.map((column) => {
        const columnId = column.id;
        const columnDef = columnsMap.get(columnId);
        const isSelectionColumn = columnId === "__sel";

        if (isSelectionColumn || !columnDef) {
          return (
            <td
              key={columnId}
              style={{
                width: column.getSize(),
                height: ROW_H,
                padding: "0 6px",
                textAlign: "center",
                border: "1px solid var(--vscode-panel-border)",
                boxSizing: "border-box",
                background: "rgba(200, 150, 0, 0.23)",
              }}
            />
          );
        }

        const draftCell = draft[columnId] ?? { value: INSERT_DEFAULT_SENTINEL };
        const isEditing = editingCol === columnId;
        const isDefault = draftCell.value === INSERT_DEFAULT_SENTINEL;
        const displayValue =
          draftCell.value === NULL_SENTINEL ? null : draftCell.value;
        const columnSize = column.getSize();
        const isCollapsed = isCollapsedWidth(columnSize);
        const displayColumnSize = isCollapsed ? 0 : columnSize;
        const colIndex = visibleColumns.indexOf(column);
        const isDataCol = !isSelectionColumn;
        const selRow = -1;
        const isCellSelected =
          isDataCol && selection?.isCellSelected(selRow, colIndex);
        const isCellAnchor =
          isDataCol && selection?.isCellAnchor(selRow, colIndex);
        const range = selection?.range;
        const minRow = range ? Math.min(range.anchorRow, range.activeRow) : -1;
        const maxRow = range ? Math.max(range.anchorRow, range.activeRow) : -1;
        const minCol = range ? Math.min(range.anchorCol, range.activeCol) : -1;
        const maxCol = range ? Math.max(range.anchorCol, range.activeCol) : -1;
        const isTopBorder = isCellSelected && selRow === minRow;
        const isBottomBorder = isCellSelected && selRow === maxRow;
        const isLeftBorder = isCellSelected && colIndex === minCol;
        const isRightBorder = isCellSelected && colIndex === maxCol;

        const cellClasses = ["rdb-trow-cell"];
        if (isCellSelected) cellClasses.push("rdb-tcell-selected");
        if (isCellAnchor) cellClasses.push("rdb-tcell-anchor");
        if (isTopBorder) cellClasses.push("rdb-tcell-border-top");
        if (isBottomBorder) cellClasses.push("rdb-tcell-border-bottom");
        if (isLeftBorder) cellClasses.push("rdb-tcell-border-left");
        if (isRightBorder) cellClasses.push("rdb-tcell-border-right");

        return (
          <td
            key={columnId}
            data-row={isDataCol ? selRow : undefined}
            data-col={isDataCol ? colIndex : undefined}
            className={cellClasses.join(" ")}
            style={{
              width: displayColumnSize,
              height: ROW_H,
              padding: isEditing ? "0" : isCollapsed ? "0" : "0 0 0 8px",
              textAlign: isNumericCategory(columnDef.category)
                ? "right"
                : "left",
              border: "1px solid var(--vscode-panel-border)",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              boxSizing: "border-box",
              verticalAlign: "middle",
              cursor: isCollapsed ? "default" : "pointer",
              userSelect: "none",
              background: "rgba(200, 150, 0, 0.23)",
            }}
            onMouseDown={(event) => {
              if (isDataCol && !isCollapsed && selection && !isEditing) {
                selection.handleCellMouseDown(selRow, colIndex, event);
              }
            }}
            onMouseEnter={(event) => {
              if (isDataCol && selection) {
                selection.handleCellMouseEnter(selRow, colIndex, event);
              }
            }}
            onDoubleClick={() => {
              if (!isCollapsed) {
                const structuredValue = getStructuredCellDialogValue(
                  isDefault ? null : displayValue,
                  columnDef,
                );

                if (structuredValue) {
                  onOpenStructuredCell({
                    rowKind: "draft",
                    column: columnDef,
                    value: structuredValue,
                    currentValue: isDefault
                      ? INSERT_DEFAULT_SENTINEL
                      : displayValue,
                    originalValue: isDefault
                      ? INSERT_DEFAULT_SENTINEL
                      : displayValue,
                    readOnly: false,
                  });
                  return;
                }

                onStartEdit(columnDef);
              }
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                width: "100%",
                height: "100%",
              }}
            >
              <div
                style={{
                  minWidth: 0,
                  flex: 1,
                  height: "100%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: isNumericCategory(columnDef.category)
                    ? "flex-end"
                    : "flex-start",
                }}
              >
                {!isCollapsed && isEditing ? (
                  <EditInput
                    initial={toDraftEditInitialValue(draftCell.value)}
                    nullable={columnDef.nullable}
                    category={columnDef.category}
                    suppressPlaceholder
                    showDefaultButton
                    onSetDefault={() =>
                      onCommit(columnDef, INSERT_DEFAULT_SENTINEL)
                    }
                    onCommit={(value) => onCommit(columnDef, value)}
                    onCancel={onCancelEdit}
                  />
                ) : !isCollapsed && isDefault ? (
                  <span style={{ fontStyle: "italic", opacity: 0.65 }}>
                    DEFAULT
                  </span>
                ) : !isCollapsed ? (
                  <CellDisplay
                    value={displayValue}
                    isPending
                    category={columnDef.category}
                    nativeType={columnDef.nativeType}
                  />
                ) : null}
              </div>
            </div>
          </td>
        );
      })}
    </tr>
  );
}
