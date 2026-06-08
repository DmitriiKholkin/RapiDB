import {
  type CellContext,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type OnChangeFn,
  type SortingState,
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
import {
  calcColWidths,
  type Column as SizingColumn,
} from "../../utils/columnSizing";
import { onMessage, postMessage } from "../../utils/messaging";
import {
  type PasteValidationError,
  parseTsv,
  validatePasteData,
} from "../../utils/pasteUtils";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";
import { Icon } from "../Icon";
import { CellDisplay } from "./CellDisplay";
import { ColumnFilterControl } from "./ColumnFilterControl";
import { EditInput, valueToEditString } from "./EditInput";
import { GridContextMenu } from "./GridContextMenu";
import {
  getStructuredCellDialogValue,
  type StructuredCellDialogValue,
} from "./structuredCellDialog";
import { TableExportActions } from "./TableExportActions";
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
  tableButtonStyle,
} from "./tableViewHelpers";
import { useCellSelection } from "./useCellSelection";

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
const QUERY_TOOLBAR_H = 28;

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

interface QueryResultsGridProps {
  result: QueryResult;
}

function QueryResultsGrid({ result }: QueryResultsGridProps) {
  const { columns: colNames, columnMeta, rows } = result;
  const [sorting, setSorting] = useState<SortingState>([]);
  const [activeCell, setActiveCell] = useState<{
    rowIndex: number;
    columnId: string;
  } | null>(null);
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});
  const scrollRef = React.useRef<HTMLDivElement>(null);
  const containerRef = React.useRef<HTMLDivElement>(null);

  const colCount = colNames.length;

  const getCellValue = useCallback(
    (rowIndex: number, colIndex: number) => {
      const row = rows[rowIndex];
      if (!row) return undefined;
      return row[`__col_${colIndex}`];
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

  const selection = useCellSelection({
    rowCount: rows.length,
    colCount,
    getCellValue,
    scrollRef,
    getCellFromPoint,
    onCopy: handleCopyText,
  });

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      selection.handleKeyDown(event);
    };
    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [selection.handleKeyDown]);

  const handleCopy = useCallback(() => {
    const text = selection.copySelection();
    if (text) {
      postMessage("writeClipboard", { text });
    }
  }, [selection.copySelection]);

  useEffect(() => {
    selection.clearSelection();
  }, [selection.clearSelection]);

  const colSizes = useMemo(
    () =>
      calcColWidths(
        colNames.map(
          (name, index): SizingColumn => ({
            name,
            dataKey: `__col_${index}`,
            isPrimaryKey: false,
            isForeignKey: false,
          }),
        ),
        rows,
        { hPad: 19 },
      ),
    [colNames, rows],
  );

  const columns = useMemo<TanColumnDef<Record<string, unknown>>[]>(
    () =>
      colNames.map((name, index) => {
        const key = `__col_${index}`;
        const category = columnMeta[index]?.category;
        return {
          id: key,
          accessorKey: key,
          header: name,
          size: colSizes[key] ?? 160,
          minSize: 1,
          maxSize: 800,
          cell: (info) => (
            <CellDisplay
              value={info.getValue()}
              isPending={false}
              category={category ?? undefined}
            />
          ),
        };
      }),
    [colNames, colSizes, columnMeta],
  );

  const handleSortingChange = React.useCallback<OnChangeFn<SortingState>>(
    (updater) => {
      // Sorted position changes invalidate index-based active cell tracking.
      setActiveCell(null);
      setSorting(updater);
    },
    [],
  );

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting, columnSizing },
    onSortingChange: handleSortingChange,
    onColumnSizingChange: setColumnSizing,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    columnResizeMode: "onChange",
    enableColumnResizing: true,
  });

  const tableRows = table.getRowModel().rows;
  const virtualizer = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_H,
    overscan: 20,
  });
  const virtualItems = virtualizer.getVirtualItems();
  const totalVirtualHeight = virtualizer.getTotalSize();

  return (
    <div
      ref={containerRef}
      style={{ flex: 1, overflow: "hidden", position: "relative" }}
    >
      <div
        ref={scrollRef}
        style={{ width: "100%", height: "100%", overflow: "auto" }}
      >
        <table style={getBaseTableStyle(table.getTotalSize())}>
          <thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const sorted = header.column.getIsSorted();
                  const headerSize = header.getSize();
                  const isCollapsed = isCollapsedWidth(headerSize);
                  const displayHeaderSize = getDisplaySize(headerSize);

                  return (
                    <th
                      key={header.id}
                      style={{
                        width: displayHeaderSize,
                        height: HEADER_H,
                        padding: isCollapsed ? 0 : "0 8px",
                        textAlign: "left",
                        background:
                          "var(--vscode-editorGroupHeader-tabsBackground)",
                        borderRight: "1px solid var(--vscode-panel-border)",
                        borderLeft: "1px solid var(--vscode-panel-border)",
                        position: "sticky",
                        top: 0,
                        zIndex: 2,
                        whiteSpace: "nowrap",
                        overflow: "visible",
                        textOverflow: "ellipsis",
                        userSelect: "none",
                        cursor: header.column.getCanSort()
                          ? "pointer"
                          : "default",
                        fontWeight: 600,
                        color: "var(--vscode-foreground)",
                        boxSizing: "border-box",
                      }}
                      onClick={header.column.getToggleSortingHandler()}
                    >
                      <HeaderContent
                        isCollapsed={isCollapsed}
                        justifyContent="flex-start"
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
                        {sorted === "asc" && (
                          <Icon
                            name="triangle-up"
                            size={10}
                            style={{ opacity: 0.7 }}
                          />
                        )}
                        {sorted === "desc" && (
                          <Icon
                            name="triangle-down"
                            size={10}
                            style={{ opacity: 0.7 }}
                          />
                        )}
                        {!sorted && header.column.getCanSort() && (
                          <Icon
                            name="unfold"
                            size={10}
                            style={{ opacity: 0.2 }}
                          />
                        )}
                      </HeaderContent>

                      {header.column.getCanResize() && (
                        <ColumnResizeHandle
                          ariaLabel={`Resize ${typeof header.column.columnDef.header === "string" ? header.column.columnDef.header : header.column.id} column`}
                          onMouseDown={header.getResizeHandler()}
                          onTouchStart={header.getResizeHandler()}
                          onClick={(event) => event.stopPropagation()}
                          isResizing={header.column.getIsResizing()}
                        />
                      )}
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody>
            <TopSpacerRow
              colSpan={columns.length}
              height={virtualItems[0]?.start ?? 0}
            />

            {virtualItems.map((virtualRow) => {
              const row = tableRows[virtualRow.index];
              return (
                <QueryTableRow
                  key={virtualRow.key}
                  row={row}
                  index={virtualRow.index}
                  columnMeta={columnMeta}
                  activeCell={activeCell}
                  onActivateCell={(rowIndex, columnId) =>
                    setActiveCell({ rowIndex, columnId })
                  }
                  onDeactivateCell={() => setActiveCell(null)}
                  selection={selection}
                />
              );
            })}

            <BottomSpacerRow
              virtualItems={virtualItems}
              totalVirtualHeight={totalVirtualHeight}
              colSpan={columns.length}
            />
          </tbody>
        </table>
      </div>
      <GridContextMenu containerRef={containerRef} onCopy={handleCopy} />
    </div>
  );
}

function toDraftEditInitialValue(value: unknown): string {
  if (value === INSERT_DEFAULT_SENTINEL) {
    return "";
  }

  return valueToEditString(value);
}

const QueryTableRow = React.memo(function QueryTableRow({
  row,
  index,
  columnMeta,
  activeCell,
  onActivateCell,
  onDeactivateCell,
  selection,
}: {
  row: TanStackRow<Record<string, unknown>>;
  index: number;
  columnMeta: QueryResult["columnMeta"];
  activeCell: { rowIndex: number; columnId: string } | null;
  onActivateCell: (rowIndex: number, columnId: string) => void;
  onDeactivateCell: () => void;
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
      className="rdb-rrow"
      data-even={String(index % 2 === 0)}
      style={{ height: ROW_H }}
    >
      {row.getVisibleCells().map((cell) => {
        const raw = cell.getValue();
        const isNull = raw === null || raw === undefined;
        const isEditing =
          activeCell?.rowIndex === index &&
          activeCell.columnId === cell.column.id;
        const cellSize = cell.column.getSize();
        const isCollapsed = isCollapsedWidth(cellSize);
        const displayCellSize = isCollapsed ? 0 : cellSize;
        const columnIndex = Number.parseInt(
          cell.column.id.replace("__col_", ""),
          10,
        );
        const category = Number.isNaN(columnIndex)
          ? undefined
          : (columnMeta[columnIndex]?.category ?? undefined);

        const isSelected = selection.isCellSelected(index, columnIndex);
        const isAnchor = selection.isCellAnchor(index, columnIndex);
        const isTopBorder = isSelected && index === minRow;
        const isBottomBorder = isSelected && index === maxRow;
        const isLeftBorder = isSelected && columnIndex === minCol;
        const isRightBorder = isSelected && columnIndex === maxCol;

        const cellClasses = ["rdb-rrow-cell"];
        if (isSelected) cellClasses.push("rdb-rcell-selected");
        if (isAnchor) cellClasses.push("rdb-rcell-anchor");
        if (isTopBorder) cellClasses.push("rdb-rcell-border-top");
        if (isBottomBorder) cellClasses.push("rdb-rcell-border-bottom");
        if (isLeftBorder) cellClasses.push("rdb-rcell-border-left");
        if (isRightBorder) cellClasses.push("rdb-rcell-border-right");

        return (
          <td
            key={cell.id}
            data-row={index}
            data-col={columnIndex}
            className={cellClasses.join(" ")}
            style={{
              width: displayCellSize,
              height: ROW_H,
              padding: isEditing ? "0" : isCollapsed ? 0 : "0 8px",
              border: "1px solid var(--vscode-panel-border)",
              textAlign: "left",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              boxSizing: "border-box",
              verticalAlign: "middle",
              cursor: isCollapsed ? "default" : "pointer",
              userSelect: "none",
            }}
            title={isNull ? "" : formatScalarValueForDisplay(raw)}
            onMouseDown={(event) => {
              if (!isCollapsed) {
                selection.handleCellMouseDown(index, columnIndex, event);
              }
            }}
            onMouseEnter={(event) => {
              selection.handleCellMouseEnter(index, columnIndex, event);
            }}
            onDoubleClick={() => {
              if (!isCollapsed && !isEditing) {
                onActivateCell(index, cell.column.id);
              }
            }}
          >
            {!isCollapsed && isEditing ? (
              <EditInput
                initial={valueToEditString(raw)}
                category={category}
                nullable
                readOnly
                onCommit={onDeactivateCell}
                onCancel={onDeactivateCell}
              />
            ) : (
              !isCollapsed &&
              flexRender(cell.column.columnDef.cell, cell.getContext())
            )}
          </td>
        );
      })}
    </tr>
  );
});

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
}: TableGridProps) {
  const columnsMap = useMemo(
    () => new Map(columns.map((column) => [column.name, column])),
    [columns],
  );

  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});
  const containerRef = React.useRef<HTMLDivElement>(null);
  const [pasteErrors, setPasteErrors] = useState<PasteValidationError[]>([]);

  const dataColCount = columns.length;
  const selColOffset = canSelectAndDeleteRows ? 1 : 0;
  const totalColCount = dataColCount + selColOffset;

  const getCellValue = useCallback(
    (rowIndex: number, colIndex: number) => {
      const dataColIndex = colIndex - selColOffset;
      if (dataColIndex < 0 || dataColIndex >= dataColCount) return undefined;
      const row = rows[rowIndex];
      if (!row) return undefined;
      const columnName = columns[dataColIndex]?.name;
      if (!columnName) return undefined;
      return row[columnName];
    },
    [rows, columns, selColOffset, dataColCount],
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

  const selection = useCellSelection({
    rowCount: rows.length,
    colCount: totalColCount,
    getCellValue,
    scrollRef,
    getCellFromPoint,
    onCopy: handleCopyText,
    onPaste: handlePaste,
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
      const startCol = ctxCell
        ? ctxCell.col - selColOffset
        : selectionRangeRef.current.anchorCol - selColOffset;

      selection.contextMenuCellRef.current = null;

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

      const errors = validatePasteData(
        pasteData,
        startRow,
        startCol,
        [...columns],
        rows.length,
      );

      if (errors.length > 0) {
        setPasteErrors(errors);
        return;
      }

      setPasteErrors([]);

      const edits: Array<{
        rowIdx: number;
        column: ColumnMeta;
        newVal: string;
        originalVal: unknown;
      }> = [];

      for (let r = 0; r < pasteData.rows.length; r++) {
        const row = pasteData.rows[r];
        const targetRow = startRow + r;

        for (let c = 0; c < row.length; c++) {
          const value = row[c];
          const targetCol = startCol + c;
          const column = columns[targetCol];

          if (!column) continue;

          const originalValue = rows[targetRow]?.[column.name];
          const coercedValue = value === "" ? NULL_SENTINEL : value;

          edits.push({
            rowIdx: targetRow,
            column,
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
    columns,
    rows,
    selColOffset,
    onBatchCellEdit,
    selection.contextMenuCellRef.current,
    selection.contextMenuCellRef,
  ]);

  useEffect(() => {
    selection.clearSelection();
  }, [selection.clearSelection]);

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
    state: { columnSizing },
    onColumnSizingChange: setColumnSizing,
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

  return (
    <div
      ref={containerRef}
      style={{ flex: 1, overflow: "hidden", position: "relative" }}
    >
      <div
        ref={scrollRef}
        style={{ width: "100%", height: "100%", overflow: "auto" }}
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
                        cursor: isSelectionColumn ? "default" : "pointer",
                      }}
                      onClick={() => {
                        if (!isSelectionColumn) {
                          onSort(columnId);
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

function QueryModeTableGrid({
  status,
  result,
}: {
  status: QueryStatus;
  result: QueryResult | null;
}) {
  if (status === "idle") {
    return (
      <QueryEmptyState
        icon="run"
        primary="Run a query to see results"
        secondary="Ctrl+Enter or F5"
      />
    );
  }

  if (status === "running") {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "12px 16px",
          opacity: 0.7,
          fontSize: 13,
        }}
      >
        <QuerySpinner /> Executing…
      </div>
    );
  }

  if (status === "error" || result?.error) {
    return (
      <div
        style={{
          margin: 10,
          padding: "10px 14px",
          borderRadius: 3,
          fontSize: 13,
          background:
            "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.15))",
          border:
            "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.5))",
          color: "var(--vscode-errorForeground)",
          fontFamily: "var(--vscode-editor-font-family, monospace)",
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        <div style={{ fontWeight: 600, marginBottom: 4 }}>Error</div>
        <div style={{ opacity: 0.9 }}>{result?.error}</div>
      </div>
    );
  }

  if (!result || result.columns.length === 0) {
    return (
      <QueryEmptyState
        icon="pass"
        primary="Query executed successfully"
        secondary={`${result?.rowCount ?? 0} rows affected · ${result?.executionTimeMs ?? 0} ms`}
      />
    );
  }

  const { rowCount, executionTimeMs, truncated, truncatedAt } = result;
  const truncatedCount = truncatedAt ?? rowCount;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        overflow: "hidden",
        position: "relative",
      }}
    >
      <div
        style={{
          height: QUERY_TOOLBAR_H,
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 10px",
          gap: 8,
          borderBottom: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editorGroupHeader-tabsBackground)",
          fontSize: 11,
        }}
      >
        <TableExportActions
          onExport={(format) =>
            postMessage(
              format === "csv" ? "exportResultsCSV" : "exportResultsJSON",
            )
          }
          titleByFormat={{
            csv: "Export results as CSV file",
            json: "Export results as JSON file",
          }}
          buttonStyle={() => ({
            ...tableButtonStyle("ghost"),
            height: 22,
            padding: "0 8px",
            fontSize: 11,
          })}
          iconSize={12}
          iconMarginRight={3}
        />
        <span style={{ opacity: 0.7 }}>
          {truncated
            ? `${truncatedCount.toLocaleString()} rows (truncated — query returned more)`
            : `${rowCount.toLocaleString()} row${rowCount !== 1 ? "s" : ""}`}
          <span style={{ opacity: 0.5, marginLeft: 6 }}>
            {executionTimeMs} ms
          </span>
        </span>
      </div>

      {truncated && (
        <div
          style={{
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "6px 12px",
            fontSize: 12,
            background:
              "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
            borderBottom:
              "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
            color: "var(--vscode-editorWarning-foreground, #CCA700)",
          }}
        >
          <Icon
            name="warning"
            size={12}
            style={{ opacity: 0.8, flexShrink: 0 }}
          />
          <span>
            Result limited to <strong>{truncatedCount.toLocaleString()}</strong>{" "}
            rows. The query returned more data. Use <code>LIMIT</code> in your
            query or increase <em>RapiDB: Query Row Limit</em> in settings.
          </span>
        </div>
      )}

      <QueryResultsGrid result={result} />
    </div>
  );
}

function QueryEmptyState({
  icon,
  primary,
  secondary,
}: {
  icon: string;
  primary: string;
  secondary?: string;
}) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        height: "100%",
        gap: 6,
        opacity: 0.45,
        userSelect: "none",
      }}
    >
      <Icon name={icon} size={28} />
      <div style={{ fontSize: 13 }}>{primary}</div>
      {secondary && <div style={{ fontSize: 11 }}>{secondary}</div>}
    </div>
  );
}

function QuerySpinner() {
  return <Icon name="sync" size={14} spin />;
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
              if (isDataCol && !isCollapsed) {
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

        return (
          <td
            key={columnId}
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
