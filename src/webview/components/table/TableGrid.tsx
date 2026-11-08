import {
  type CellContext,
  flexRender,
  getCoreRowModel,
  type ColumnDef as TanColumnDef,
  type Cell as TanStackCell,
  type Column as TanStackColumn,
  type Row as TanStackRow,
  useReactTable,
} from "@tanstack/react-table";
import {
  defaultRangeExtractor,
  useVirtualizer,
  type Range as VirtualRange,
} from "@tanstack/react-virtual";
import React, {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { redisBinarySnapshotDisplay } from "../../../shared/redisValueSnapshot";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraft,
  type FilterDraftMap,
  isNumericCategory,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import type { ClipboardTextPayload } from "../../../shared/webviewContracts";
import type { QueryResult, QueryStatus } from "../../store";
import type {
  EditTarget,
  InsertDraftRow,
  PendingEdits,
  Row,
} from "../../types";
import { getCellPreviewTitle } from "../../utils/cellPreview";
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
import { QueryModeTableGrid } from "./grid/QueryModeTableGrid";
import {
  getStructuredCellDialogValue,
  type StructuredCellDialogValue,
} from "./structuredCellDialog";
import {
  buildColumnHeaderTitle,
  canEditColumn,
  canOpenColumn,
  FILTER_H,
  HEADER_H,
  INSERT_DEFAULT_SENTINEL,
  keyIconColor,
  ROW_H,
  SR_ONLY_STYLE,
  type TableSortState,
} from "./tableViewHelpers";
import { type CellRange, useCellSelection } from "./useCellSelection";
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

function SpacerRow({ height, colSpan }: { height: number; colSpan: number }) {
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

function renderBodyCell(cell: TanStackCell<Row, unknown>): React.ReactNode {
  const renderer = cell.column.columnDef.cell;
  return typeof renderer === "function"
    ? renderer(cell.getContext())
    : flexRender(renderer, cell.getContext());
}

function toDraftEditInitialValue(value: unknown): string {
  if (value === INSERT_DEFAULT_SENTINEL) {
    return "";
  }

  return valueToEditString(value);
}

interface TableGridProps {
  getRowMutationBlockReason?: (rowIdx: number) => string | null;
  canEditRows: boolean;
  canSelectAndDeleteRows: boolean;
  rowSelectionDisabled?: boolean;
  colSizes: Record<string, number>;
  columns: readonly ColumnMeta[];
  editCell: EditTarget | null;
  filterDrafts: FilterDraftMap;
  loading: boolean;
  loadingRef: React.MutableRefObject<boolean>;
  fetchEpochRef: React.MutableRefObject<number>;
  newRows: InsertDraftRow[];
  onCancelEdit: () => void;
  onBatchCellEdit: (
    edits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => PasteValidationError[] | undefined;
  onCommitCellEdit: (
    rowIdx: number,
    column: ColumnMeta,
    newVal: string,
    originalVal: unknown,
  ) => void;
  onCommitDraftCellEdit: (
    rowIdx: number,
    column: ColumnMeta,
    value: string,
  ) => void;
  onMixedBatchEdit: (
    draftEdits: Array<{ rowIdx: number; column: ColumnMeta; newVal: string }>,
    persistedEdits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => PasteValidationError[] | undefined;
  onFilterDraftChange: (
    columnName: string,
    nextDraft: FilterDraft | undefined,
    options?: { applyImmediately?: boolean },
  ) => void;
  onSelectionChange: (selection: Set<number>) => void;
  onSort: (column: string) => void;
  getRowCellColumn?: (rowIdx: number, column: ColumnMeta) => ColumnMeta;
  onOpenStructuredCell: (options: {
    rowKind: "persisted" | "draft";
    rowIdx?: number;
    column: ColumnMeta;
    value: StructuredCellDialogValue;
    currentValue: unknown;
    originalValue: unknown;
    readOnly: boolean;
  }) => void;
  onStartDraftEdit: (rowIdx: number, column: ColumnMeta) => void;
  onStartEdit: (rowIdx: number, column: ColumnMeta) => void;
  pendingEdits: PendingEdits;
  rows: Row[];
  scrollRef: React.RefObject<HTMLDivElement | null>;
  selected: ReadonlySet<number>;
  sort: TableSortState;
  columnOrderRef?: React.MutableRefObject<string[]>;
  hiddenColumnIdsRef?: React.MutableRefObject<Set<string>>;
  pasteStructureEpochRef?: React.MutableRefObject<number>;
  mongoColumnTypes?: boolean;
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
  getRowCellColumn,
  getRowMutationBlockReason,
  canEditRows,
  canSelectAndDeleteRows,
  rowSelectionDisabled = false,
  colSizes,
  columns,
  editCell,
  filterDrafts,
  loading,
  loadingRef,
  fetchEpochRef,
  newRows,
  onCancelEdit,
  onBatchCellEdit,
  onCommitCellEdit,
  onCommitDraftCellEdit,
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
  pasteStructureEpochRef,
  mongoColumnTypes = false,
}: TableGridProps) {
  const selectableRowIndexes = useMemo(
    () =>
      rows.flatMap((_, index) =>
        getRowMutationBlockReason?.(index) ? [] : [index],
      ),
    [rows, getRowMutationBlockReason],
  );
  const columnsMap = useMemo(
    () => new Map(columns.map((column) => [column.name, column])),
    [columns],
  );

  const columnsMapRef = useRef(columnsMap);

  const selectionColumnId = useMemo(() => {
    let id = "__sel";
    while (columnsMap.has(id)) id += "_";
    return id;
  }, [columnsMap]);
  const [columnSizingState, setColumnSizing] = useState<Record<string, number>>(
    {},
  );
  const columnSizing = useMemo<Record<string, number>>(
    () => Object.assign(Object.create(null), columnSizingState),
    [columnSizingState],
  );
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const columnOrderRef = useRef(columnOrder);
  const columnSizingRef = useRef(columnSizing);
  const colSizesRef = useRef(colSizes);

  useLayoutEffect(() => {
    columnsMapRef.current = columnsMap;
    columnOrderRef.current = columnOrder;
    columnSizingRef.current = columnSizing;
    colSizesRef.current = colSizes;
  }, [columnOrder, columnSizing, colSizes, columnsMap]);

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
  const newRowsRef = useRef(newRows);
  const scrollToCellRef = useRef<(row: number, col: number) => void>(() => {});

  useLayoutEffect(() => {
    newRowsRef.current = newRows;
  }, [newRows]);

  const dataColCount = columns.length;
  const selColOffset = canSelectAndDeleteRows ? 1 : 0;
  const totalColCount = dataColCount + selColOffset;

  const onCellNavigate = useCallback((row: number, col: number) => {
    scrollToCellRef.current(row, col);
  }, []);

  const getCellValue = useCallback(
    (rowIndex: number, colIndex: number) => {
      const colId = columnOrderRef.current[colIndex];
      if (!colId || colId === selectionColumnId) return undefined;
      if (rowIndex < 0) {
        const draftIdx = rowIndex + newRowsRef.current.length;
        const draft = newRowsRef.current[draftIdx];
        if (!draft) return undefined;
        const dv = Object.hasOwn(draft, colId)
          ? draft[colId]?.value
          : undefined;
        if (dv === INSERT_DEFAULT_SENTINEL) return undefined;
        if (dv === NULL_SENTINEL) return null;
        return dv;
      }
      const row = rows[rowIndex];
      if (!row) return undefined;
      const pendingRow = pendingEdits.get(rowIndex);
      const value = pendingRow?.has(colId)
        ? pendingRow.get(colId)
        : Object.hasOwn(row, colId)
          ? row[colId]
          : undefined;
      return (
        redisBinarySnapshotDisplay(
          value,
          columnsMapRef.current.get(colId)?.nativeType,
        ) ?? value
      );
    },
    [rows, pendingEdits, selectionColumnId],
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

  const clipboardRecipientRef = useRef(`table-grid:${crypto.randomUUID()}`);
  const clipboardRequestRef = useRef<string | null>(null);
  const clipboardEpochRef = useRef(fetchEpochRef.current);
  const pendingPasteRangeRef = useRef<CellRange | null>(null);
  const pendingPasteContextRef = useRef<{ row: number; col: number } | null>(
    null,
  );
  const clipboardStructureEpochRef = useRef(pasteStructureEpochRef?.current);
  const committedStructureEpochRef = useRef(pasteStructureEpochRef?.current);
  const layoutUpdatePendingRef = useRef(false);
  const selectionRangeRef = useRef<CellRange | null>(null);
  const pasteContextRef = useRef<{
    current: { row: number; col: number } | null;
  } | null>(null);

  const pasteLayout = JSON.stringify(
    columnOrder.map((id) => [
      id,
      isCollapsedWidth(columnSizing[id] ?? colSizes[id] ?? 160),
    ]),
  );
  useLayoutEffect(() => {
    committedStructureEpochRef.current = pasteStructureEpochRef?.current;
    layoutUpdatePendingRef.current = false;
  });
  // biome-ignore lint/correctness/useExhaustiveDependencies: these values invalidate an outstanding request, not a derived render value.
  useLayoutEffect(() => {
    clipboardRequestRef.current = null;
    pendingPasteRangeRef.current = null;
    pendingPasteContextRef.current = null;
  }, [
    pasteLayout,
    columns,
    rows,
    newRows.length,
    pasteStructureEpochRef ? null : newRows,
    pasteStructureEpochRef,
    canEditRows,
    canSelectAndDeleteRows,
    loading,
    fetchEpochRef,
  ]);

  const handlePaste = useCallback(
    (fromContextMenu = false) => {
      if (
        loadingRef.current ||
        layoutUpdatePendingRef.current ||
        committedStructureEpochRef.current !== pasteStructureEpochRef?.current
      )
        return;
      const range = selectionRangeRef.current;
      const contextCell = fromContextMenu
        ? pasteContextRef.current?.current
        : null;
      if (!range && !contextCell) return;
      const targetRow =
        contextCell?.row ??
        (range ? Math.min(range.anchorRow, range.activeRow) : undefined);
      if (targetRow === undefined || (!canEditRows && targetRow >= 0)) return;
      if (targetRow < -newRowsRef.current.length) return;
      const requestId = crypto.randomUUID();
      clipboardRequestRef.current = requestId;
      clipboardEpochRef.current = fetchEpochRef.current;
      clipboardStructureEpochRef.current = pasteStructureEpochRef?.current;
      pendingPasteContextRef.current = contextCell ?? null;
      pendingPasteRangeRef.current = contextCell
        ? {
            anchorRow: contextCell.row,
            anchorCol: contextCell.col,
            activeRow: contextCell.row,
            activeCol: contextCell.col,
          }
        : range
          ? { ...range }
          : null;
      postMessage("readClipboard", {
        requestId,
        recipient: clipboardRecipientRef.current,
      });
    },
    [loadingRef, fetchEpochRef, pasteStructureEpochRef, canEditRows],
  );

  const isColumnCollapsed = useCallback(
    (visualColIndex: number) => {
      const colId = columnOrderRef.current[visualColIndex];
      if (!colId || colId === selectionColumnId) return false;
      const size = columnSizing[colId] ?? colSizes[colId] ?? 160;
      return isCollapsedWidth(size);
    },
    [columnSizing, colSizes, selectionColumnId],
  );

  const selection = useCellSelection({
    rowCount: rows.length,
    colCount: totalColCount,
    minRow: newRows.length > 0 ? -newRows.length : 0,
    minCol: selColOffset,
    getCellValue,
    scrollRef,
    getCellFromPoint,
    onCopy: handleCopyText,
    onPaste: handlePaste,
    isColumnCollapsed,
    onCellNavigate,
  });

  useLayoutEffect(() => {
    selectionRangeRef.current = selection.range;
  }, [selection.range]);

  pasteContextRef.current = selection.contextMenuCellRef;

  const handleContextMenuOpen = useCallback(
    (target: HTMLElement) => {
      const cell = target.closest("td[data-row][data-col]");
      if (!cell || !containerRef.current?.contains(cell)) {
        selection.contextMenuCellRef.current = null;
        return;
      }
      const row = Number(cell.getAttribute("data-row"));
      const col = Number(cell.getAttribute("data-col"));
      selection.contextMenuCellRef.current =
        Number.isInteger(row) && Number.isInteger(col) ? { row, col } : null;
    },
    [selection.contextMenuCellRef],
  );
  const handleContextMenuClose = useCallback(() => {
    selection.contextMenuCellRef.current = null;
  }, [selection.contextMenuCellRef]);

  const openCell = useCallback(
    (row: number, columnId: string) => {
      const schemaColumn = columnsMap.get(columnId);
      const isDraft = row < 0;
      const rowIdx = isDraft ? row + newRows.length : row;
      const column =
        schemaColumn && !isDraft
          ? (getRowCellColumn?.(rowIdx, schemaColumn) ?? schemaColumn)
          : schemaColumn;
      const rowKind = isDraft ? "draft" : "persisted";
      if (
        !canOpenColumn(column, rowKind) ||
        isCollapsedWidth(columnSizing[columnId] ?? colSizes[columnId] ?? 160) ||
        (editCell?.kind === rowKind &&
          editCell.rowIdx === rowIdx &&
          editCell.col === columnId)
      ) {
        return;
      }
      if (isDraft ? !newRows[rowIdx] : !rows[rowIdx]) return;

      const originalValue = isDraft
        ? ((Object.hasOwn(newRows[rowIdx], columnId)
            ? newRows[rowIdx][columnId]?.value
            : undefined) ?? INSERT_DEFAULT_SENTINEL)
        : rows[rowIdx][columnId];
      const pendingRow = pendingEdits.get(rowIdx);
      const currentValue = isDraft
        ? originalValue === NULL_SENTINEL
          ? null
          : originalValue
        : pendingRow?.has(columnId)
          ? pendingRow.get(columnId)
          : originalValue;
      const structuredValue = getStructuredCellDialogValue(
        isDraft && currentValue === INSERT_DEFAULT_SENTINEL
          ? null
          : currentValue,
        column,
      );
      if (structuredValue) {
        onOpenStructuredCell({
          rowKind,
          rowIdx,
          column,
          value: structuredValue,
          currentValue,
          originalValue: isDraft ? currentValue : originalValue,
          readOnly:
            !isDraft &&
            (!canEditRows ||
              !canEditColumn(column) ||
              redisBinarySnapshotDisplay(currentValue, column.nativeType) !==
                undefined ||
              Boolean(getRowMutationBlockReason?.(rowIdx))),
        });
      } else {
        (isDraft ? onStartDraftEdit : onStartEdit)(rowIdx, column);
      }
    },
    [
      columnsMap,
      getRowCellColumn,
      newRows,
      rows,
      columnSizing,
      colSizes,
      editCell,
      pendingEdits,
      canEditRows,
      getRowMutationBlockReason,
      onOpenStructuredCell,
      onStartDraftEdit,
      onStartEdit,
    ],
  );

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target instanceof HTMLElement ? event.target : null;
      if (
        event.key === "F2" &&
        selection.range &&
        target &&
        scrollRef.current?.contains(target) &&
        target.tagName !== "SELECT" &&
        document.activeElement?.tagName !== "SELECT" &&
        !isEditableElement(target) &&
        !isEditableElement(document.activeElement)
      ) {
        const columnId = columnOrderRef.current[selection.range.activeCol];
        if (columnId) {
          event.preventDefault();
          openCell(selection.range.activeRow, columnId);
        }
        return;
      }
      if (
        (event.key === "v" || event.key === "V") &&
        (event.ctrlKey || event.metaKey) &&
        ((target !== scrollRef.current &&
          !(
            target?.matches("td[data-row][data-col]") &&
            scrollRef.current?.contains(target)
          )) ||
          document.activeElement !== scrollRef.current ||
          (!canEditRows &&
            (selection.range
              ? Math.min(selection.range.anchorRow, selection.range.activeRow)
              : 0) >= 0))
      ) {
        return;
      }
      selection.handleKeyDown(event);
    };
    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [
    canEditRows,
    openCell,
    scrollRef,
    selection.handleKeyDown,
    selection.range,
  ]);

  useEffect(() => {
    const handlePasteEvent = (event: ClipboardEvent) => {
      const scroll = scrollRef.current;
      const target = event.target;
      if (
        !scroll ||
        document.activeElement !== scroll ||
        (target !== scroll &&
          target !== event.currentTarget &&
          target !== document)
      ) {
        return;
      }
      const range = selectionRangeRef.current;
      if (!range) return;
      const topRow = Math.min(range.anchorRow, range.activeRow);
      if (!canEditRows && topRow >= 0) return;
      if (topRow < -newRowsRef.current.length) return;
      event.preventDefault();
      event.stopPropagation();
      handlePaste();
    };
    window.addEventListener("paste", handlePasteEvent, true);
    return () => window.removeEventListener("paste", handlePasteEvent, true);
  }, [canEditRows, handlePaste, scrollRef]);

  const handleCopy = useCallback(() => {
    const text = selection.copySelection();
    if (text) {
      postMessage("writeClipboard", { text });
    }
  }, [selection.copySelection]);

  useEffect(() => {
    const handleClipboardText = ({
      requestId,
      recipient,
      text,
    }: ClipboardTextPayload) => {
      if (
        recipient !== clipboardRecipientRef.current ||
        requestId !== clipboardRequestRef.current
      )
        return;
      clipboardRequestRef.current = null;
      const requestedRange = pendingPasteRangeRef.current;
      const requestedContext = pendingPasteContextRef.current;
      pendingPasteRangeRef.current = null;
      pendingPasteContextRef.current = null;
      if (
        loadingRef.current ||
        clipboardEpochRef.current !== fetchEpochRef.current ||
        clipboardStructureEpochRef.current !== pasteStructureEpochRef?.current
      ) {
        return;
      }
      if (!requestedRange) return;

      const pasteData = parseTsv(text);
      if (pasteData.rows.length === 0) return;

      const startRow = Math.min(
        requestedRange.anchorRow,
        requestedRange.activeRow,
      );
      const anchorCol = Math.min(
        requestedRange.anchorCol,
        requestedRange.activeCol,
      );
      const startCol = anchorCol - selColOffset;

      if (!canEditRows && startRow >= 0) return;

      const visiblePasteColumns: ColumnMeta[] = [];
      for (let i = anchorCol; i < columnOrderRef.current.length; i++) {
        const colId = columnOrderRef.current[i];
        if (colId === selectionColumnId) continue;
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
            columnIndex: anchorCol,
            columnName: "",
            value: "",
            message: "Cannot paste into selection column",
          },
        ]);
        return;
      }

      if (startRow < 0) {
        const draftCount = newRowsRef.current.length;
        if (draftCount === 0) return;

        const startVirtualIndex = startRow + draftCount;

        const errors: PasteValidationError[] = [];
        const normalizedCells: Array<{
          targetVirtualIndex: number;
          column: ColumnMeta;
          value: string;
          normalized: unknown;
        }> = [];

        for (let r = 0; r < pasteData.rows.length; r++) {
          const row = pasteData.rows[r];
          const targetVirtualIndex = startVirtualIndex + r;
          if (targetVirtualIndex >= draftCount) {
            errors.push({
              rowIndex: targetVirtualIndex,
              columnIndex: anchorCol,
              columnName: "",
              value: row.join("\t"),
              message: `Paste starting in a new row cannot continue into existing rows: ${pasteData.rows.length - r} more pasted row${pasteData.rows.length - r === 1 ? "" : "s"} than new rows. Add more new rows first.`,
            });
            break;
          }

          for (let c = 0; c < row.length; c++) {
            const value = row[c];
            const targetCol = startCol + c;
            const column = visiblePasteColumns[c];
            if (!column) {
              errors.push({
                rowIndex: targetVirtualIndex,
                columnIndex: targetCol + selColOffset,
                columnName: "",
                value,
                message: "Paste would exceed the visible column bounds",
              });
              continue;
            }

            const columnName = column.name;
            if (!canEditColumn(column)) {
              errors.push({
                rowIndex: targetVirtualIndex,
                columnIndex: targetCol + selColOffset,
                columnName,
                value,
                message: `Cannot paste into read-only column "${columnName}"`,
              });
              continue;
            }

            const validation = validatePasteValue(
              value,
              column,
              pasteData.quoted?.[r]?.[c],
              { mongoTypes: mongoColumnTypes },
            );
            if (!validation.valid) {
              errors.push({
                rowIndex: targetVirtualIndex,
                columnIndex: targetCol + selColOffset,
                columnName: column.name,
                value,
                message: validation.error ?? "Validation failed",
              });
              continue;
            }

            normalizedCells.push({
              targetVirtualIndex,
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

        const draftEdits = normalizedCells.map((cell) => ({
          rowIdx: cell.targetVirtualIndex,
          column: cell.column,
          newVal: formatNormalizedPasteValue(cell.value, cell.normalized),
        }));

        const commitErrors =
          draftEdits.length > 0 ? onMixedBatchEdit(draftEdits, []) : undefined;
        setPasteErrors(commitErrors ?? []);
        if (
          !commitErrors?.length &&
          requestedContext &&
          selection.contextMenuCellRef.current === requestedContext
        )
          selection.contextMenuCellRef.current = null;

        return;
      }

      const validationResult = validatePasteData(
        pasteData,
        startRow,
        0,
        visiblePasteColumns,
        rows.length,
        canEditColumn,
        { mongoTypes: mongoColumnTypes },
      );

      if (validationResult.errors.length > 0) {
        setPasteErrors(validationResult.errors);
        return;
      }

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
          const persistedRow = rows[targetRow];
          const originalValue =
            persistedRow && Object.hasOwn(persistedRow, cell.column.name)
              ? persistedRow[cell.column.name]
              : undefined;
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

      const commitErrors = onBatchCellEdit(edits);
      setPasteErrors(commitErrors ?? []);
      if (
        !commitErrors?.length &&
        requestedContext &&
        selection.contextMenuCellRef.current === requestedContext
      )
        selection.contextMenuCellRef.current = null;
    };

    const unsubscribe = onMessage<ClipboardTextPayload>(
      "clipboardText",
      handleClipboardText,
    );

    return unsubscribe;
  }, [
    canEditRows,
    rows,
    selectionColumnId,
    selColOffset,
    onBatchCellEdit,
    onMixedBatchEdit,
    loadingRef,
    fetchEpochRef,
    pasteStructureEpochRef,
    mongoColumnTypes,
    selection.contextMenuCellRef.current,
    selection.contextMenuCellRef,
  ]);

  useEffect(() => {
    void rows;
    void selColOffset;
    selection.clearSelection();
    selection.contextMenuCellRef.current = null;
  }, [
    rows,
    selColOffset,
    selection.clearSelection,
    selection.contextMenuCellRef,
  ]);

  useEffect(() => {
    setColumnOrder((previous) => {
      const columnNames = columns.map((column) => column.name);
      const available = new Set(columnNames);
      const retained = previous.filter((id) => available.has(id));
      const retainedSet = new Set(retained);
      const next = [
        ...(canSelectAndDeleteRows ? [selectionColumnId] : []),
        ...retained,
        ...columnNames.filter((name) => !retainedSet.has(name)),
      ];
      return next.length === previous.length &&
        next.every((id, index) => id === previous[index])
        ? previous
        : next;
    });
  }, [columns, canSelectAndDeleteRows, selectionColumnId]);

  const wasDraggedRef = React.useRef(false);
  const resizeCleanupRef = useRef<(() => void) | null>(null);
  const cancelResize = useCallback(() => {
    resizeCleanupRef.current?.();
  }, []);
  useEffect(() => cancelResize, [cancelResize]);

  const { onHeaderMouseDown: handleHeaderMouseDown, cancelDrag } =
    useColumnDragReorder({
      getColumnOrder: () => columnOrderRef.current,
      setColumnOrder: (updater) => {
        clipboardRequestRef.current = null;
        layoutUpdatePendingRef.current = true;
        setColumnOrder(updater);
      },
      excludedIds: [selectionColumnId],
      onDragActivated: () => {
        wasDraggedRef.current = true;
      },
      onDragEnded: (activated) => {
        if (!activated) {
          wasDraggedRef.current = false;
          return;
        }
        setTimeout(() => {
          wasDraggedRef.current = false;
        }, 0);
      },
    });

  const tanColumns = useMemo<TanColumnDef<Row>[]>(
    () => [
      ...(canSelectAndDeleteRows
        ? [
            {
              id: selectionColumnId,
              size: 36,
              header: () => (
                <input
                  type="checkbox"
                  aria-label="Select all rows"
                  disabled={
                    rowSelectionDisabled || selectableRowIndexes.length === 0
                  }
                  checked={
                    selectableRowIndexes.length > 0 &&
                    selected.size === selectableRowIndexes.length
                  }
                  ref={(element) => {
                    if (element) {
                      element.indeterminate =
                        selected.size > 0 &&
                        selected.size < selectableRowIndexes.length;
                    }
                  }}
                  onChange={(event) => {
                    if (rowSelectionDisabled) return;
                    onSelectionChange(
                      event.target.checked
                        ? new Set(selectableRowIndexes)
                        : new Set(),
                    );
                  }}
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
                  disabled={
                    rowSelectionDisabled ||
                    Boolean(getRowMutationBlockReason?.(row.index))
                  }
                  title={getRowMutationBlockReason?.(row.index) ?? undefined}
                  checked={selected.has(row.index)}
                  onChange={(event: React.ChangeEvent<HTMLInputElement>) => {
                    if (
                      rowSelectionDisabled ||
                      getRowMutationBlockReason?.(row.index)
                    )
                      return;
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
          accessorFn: (row) =>
            Object.hasOwn(row, column.name) ? row[column.name] : undefined,
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
          cell: ({ row }) => {
            const originalValue = Object.hasOwn(row.original, column.name)
              ? row.original[column.name]
              : undefined;
            const rowIdx = row.index;
            const isEditing =
              editCell?.kind === "persisted" &&
              editCell.rowIdx === rowIdx &&
              editCell.col === column.name;
            const pendingRow = pendingEdits.get(rowIdx);
            const hasPending = pendingRow?.has(column.name) ?? false;
            const pendingValue = pendingRow?.get(column.name);
            const displayValue = hasPending ? pendingValue : originalValue;

            if (isEditing) {
              const startValue = hasPending ? pendingValue : originalValue;
              const startString = valueToEditString(startValue);
              return (
                <EditInput
                  initial={startString}
                  initialMissing={startValue === undefined}
                  nullable={column.nullable}
                  category={column.category}
                  readOnly={
                    !canEditRows ||
                    !canEditColumn(column) ||
                    Boolean(getRowMutationBlockReason?.(rowIdx))
                  }
                  onCommit={(value) =>
                    onCommitCellEdit(rowIdx, column, value, originalValue)
                  }
                  onCancel={onCancelEdit}
                />
              );
            }

            return (
              <div title={getRowMutationBlockReason?.(rowIdx) ?? undefined}>
                <CellDisplay
                  value={displayValue}
                  isMissing={
                    !hasPending && !Object.hasOwn(row.original, column.name)
                  }
                  isPending={hasPending}
                  category={column.category}
                  nativeType={column.nativeType}
                />
              </div>
            );
          },
        }),
      ),
    ],
    [
      canEditRows,
      selectionColumnId,
      canSelectAndDeleteRows,
      rowSelectionDisabled,
      getRowMutationBlockReason,
      selectableRowIndexes,
      colSizes,
      columns,
      editCell,
      onCancelEdit,
      onCommitCellEdit,
      onSelectionChange,
      pendingEdits,
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
  const draftRowCount = newRows.length;
  const hasDraftRow = draftRowCount > 0;
  const editingVirtualIndex = editCell
    ? editCell.kind === "draft"
      ? editCell.rowIdx
      : editCell.rowIdx + draftRowCount
    : null;
  const rangeExtractor = useCallback(
    (range: VirtualRange) => {
      const indexes = defaultRangeExtractor(range);
      if (
        editingVirtualIndex === null ||
        editingVirtualIndex < 0 ||
        editingVirtualIndex >= range.count ||
        indexes.includes(editingVirtualIndex)
      ) {
        return indexes;
      }
      return [...indexes, editingVirtualIndex].sort((a, b) => a - b);
    },
    [editingVirtualIndex],
  );
  const virtualizer = useVirtualizer({
    count: tableRows.length + draftRowCount,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_H,
    overscan: 15,
    rangeExtractor,
  });
  const virtualItems = virtualizer.getVirtualItems();
  const totalVirtualHeight = virtualizer.getTotalSize();

  const scrollToCell = useCallback(
    (row: number, col: number) => {
      const virtualIndex = row + draftRowCount;
      virtualizer.scrollToIndex(virtualIndex, { align: "auto" });
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
    },
    [draftRowCount, scrollRef, selColOffset, virtualizer],
  );

  useLayoutEffect(() => {
    scrollToCellRef.current = scrollToCell;
  }, [scrollToCell]);

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
        tabIndex={0}
      >
        <table style={getBaseTableStyle(tanTable.getTotalSize())}>
          <thead>
            {tanTable.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const isSelectionColumn =
                    header.column.id === selectionColumnId;
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
                        if (!isSelectionColumn && event.button === 0) {
                          cancelResize();
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
                            if (event.button !== 0) return;
                            event.preventDefault();
                            event.stopPropagation();
                            cancelResize();
                            cancelDrag();

                            const startSize = Math.max(headerSize, 1);
                            const startX = event.clientX;

                            const applyWidth = (clientX: number) => {
                              const nextWidth = Math.max(
                                1,
                                startSize + (clientX - startX),
                              );
                              const oldWidth =
                                columnSizingRef.current[columnId] ??
                                colSizesRef.current[columnId] ??
                                160;
                              if (
                                isCollapsedWidth(oldWidth) !==
                                isCollapsedWidth(nextWidth)
                              ) {
                                clipboardRequestRef.current = null;
                                layoutUpdatePendingRef.current = true;
                              }
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
                              cleanup();
                            };
                            const cleanup = () => {
                              document.removeEventListener("mousemove", onMove);
                              document.removeEventListener("mouseup", onUp);
                              resizeCleanupRef.current = null;
                            };

                            resizeCleanupRef.current = cleanup;
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
                const isSelectionColumn =
                  header.column.id === selectionColumnId;
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
            <SpacerRow
              colSpan={tanColumns.length}
              height={virtualItems[0]?.start ?? 0}
            />
            {virtualItems.flatMap((virtualRow, itemIndex) => {
              const previousEnd =
                itemIndex > 0 ? virtualItems[itemIndex - 1].end : null;
              const gapRow =
                previousEnd !== null && virtualRow.start > previousEnd ? (
                  <SpacerRow
                    key={`gap-${virtualRow.key}`}
                    colSpan={tanColumns.length}
                    height={virtualRow.start - previousEnd}
                  />
                ) : null;
              if (hasDraftRow && virtualRow.index < draftRowCount) {
                const draftIdx = virtualRow.index;
                return [
                  gapRow,
                  <DraftTableRow
                    key={virtualRow.key}
                    rowIndex={draftIdx}
                    draftRowCount={draftRowCount}
                    columns={columns}
                    visibleColumns={visibleColumns}
                    draft={newRows[draftIdx]}
                    editingCol={
                      editCell?.kind === "draft" && editCell.rowIdx === draftIdx
                        ? editCell.col
                        : null
                    }
                    onOpenCell={openCell}
                    onCommit={onCommitDraftCellEdit}
                    onCancelEdit={onCancelEdit}
                    selection={selection}
                  />,
                ];
              }

              const persistedIndex = virtualRow.index - draftRowCount;
              const row = tableRows[persistedIndex];
              const isSelected = selected.has(persistedIndex);
              const editingCol =
                editCell?.kind === "persisted" &&
                editCell.rowIdx === persistedIndex
                  ? editCell.col
                  : null;

              return [
                gapRow,
                <TableRow
                  key={virtualRow.key}
                  row={row}
                  visualIndex={virtualRow.index}
                  rowIndex={persistedIndex}
                  isSelected={isSelected}
                  pendingCols={pendingEdits.get(persistedIndex)}
                  columnsMap={columnsMap}
                  editingCol={editingCol}
                  onOpenCell={openCell}
                  selection={selection}
                />,
              ];
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
        onOpen={handleContextMenuOpen}
        onClose={handleContextMenuClose}
        onCopy={handleCopy}
        onPaste={() => handlePaste(true)}
        canPaste={canEditRows || newRows.length > 0}
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
  onOpenCell,
  selection,
}: {
  row: TanStackRow<Row>;
  visualIndex: number;
  rowIndex: number;
  isSelected: boolean;
  pendingCols?: Map<string, unknown>;
  columnsMap: Map<string, ColumnMeta>;
  editingCol: string | null;
  onOpenCell: (rowIndex: number, columnId: string) => void;
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
        const isSelectionColumn = !columnDef;
        const isCellPending = pendingCols?.has(columnId) ?? false;
        const isEditing = columnId === editingCol;
        const canOpenCellEditor =
          !isSelectionColumn && canOpenColumn(columnDef, "persisted");

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
                ? `${primaryKeyLabel}: ${getCellPreviewTitle(Object.hasOwn(row.original, columnId) ? row.original[columnId] : undefined)}`
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
                onOpenCell(rowIndex, columnId);
              }
            }}
          >
            {!isCollapsed && renderBodyCell(cell)}
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
  rowIndex,
  draftRowCount,
  editingCol,
  onOpenCell,
  onCommit,
  onCancelEdit,
  selection,
}: {
  columns: readonly ColumnMeta[];
  visibleColumns: readonly TanStackColumn<Row, unknown>[];
  draft: InsertDraftRow;
  rowIndex: number;
  draftRowCount: number;
  editingCol: string | null;
  onOpenCell: (rowIndex: number, columnId: string) => void;
  onCommit: (rowIdx: number, column: ColumnMeta, value: string) => void;
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
        const isSelectionColumn = !columnDef;

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

        const draftCell = (Object.hasOwn(draft, columnId)
          ? draft[columnId]
          : undefined) ?? { value: INSERT_DEFAULT_SENTINEL };
        const canOpenCellEditor = canOpenColumn(columnDef, "draft");
        const isEditing = editingCol === columnId;
        const isDefault = draftCell.value === INSERT_DEFAULT_SENTINEL;
        const displayValue =
          draftCell.value === NULL_SENTINEL ? null : draftCell.value;
        const columnSize = column.getSize();
        const isCollapsed = isCollapsedWidth(columnSize);
        const displayColumnSize = isCollapsed ? 0 : columnSize;
        const colIndex = visibleColumns.indexOf(column);
        const isDataCol = !isSelectionColumn;
        const selRow = rowIndex - draftRowCount;
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
              cursor: canOpenCellEditor && !isCollapsed ? "pointer" : "default",
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
              if (canOpenCellEditor && !isCollapsed) {
                onOpenCell(selRow, columnId);
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
                    initialMissing={isDefault}
                    nullable={columnDef.nullable}
                    category={columnDef.category}
                    suppressPlaceholder
                    showDefaultButton
                    onSetDefault={() =>
                      onCommit(rowIndex, columnDef, INSERT_DEFAULT_SENTINEL)
                    }
                    onCommit={(value) => onCommit(rowIndex, columnDef, value)}
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
