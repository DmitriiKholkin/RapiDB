import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type OnChangeFn,
  type SortingState,
  type ColumnDef as TanColumnDef,
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
import type { QueryResult } from "../../../store";
import {
  buildCellSelectionClassName,
  classifyCellSelection,
} from "../../../utils/cellSelectionBorders";
import {
  calcColWidths,
  type Column as SizingColumn,
} from "../../../utils/columnSizing";
import { postMessage } from "../../../utils/messaging";
import { formatScalarValueForDisplay } from "../../../utils/valueFormatting";
import { Icon } from "../../Icon";
import { CellDisplay } from "../CellDisplay";
import { EditInput, valueToEditString } from "../EditInput";
import { GridContextMenu } from "../GridContextMenu";
import {
  BottomSpacerRow,
  ColumnResizeHandle,
  getBaseTableStyle,
  getDisplaySize,
  HeaderContent,
  isCollapsedWidth,
  TopSpacerRow,
} from "../gridSubComponents";
import { HEADER_H, ROW_H } from "../tableConstants";
import { useCellSelection } from "../useCellSelection";
import { useColumnDragReorder } from "../useColumnDragReorder";

interface QueryResultsGridProps {
  result: QueryResult;
  columnOrder: string[];
  onColumnOrderChange: React.Dispatch<React.SetStateAction<string[]>>;
  sorting: SortingState;
  onSortingChange: React.Dispatch<React.SetStateAction<SortingState>>;
  columnSizing: Record<string, number>;
  onColumnSizingChange: React.Dispatch<
    React.SetStateAction<Record<string, number>>
  >;
}

export function QueryResultsGrid({
  result,
  columnOrder,
  onColumnOrderChange: setColumnOrder,
  sorting,
  onSortingChange: setSorting,
  columnSizing,
  onColumnSizingChange: setColumnSizing,
}: QueryResultsGridProps) {
  const { columns: colNames, columnMeta, rows } = result;
  const [activeCell, setActiveCell] = useState<{
    rowIndex: number;
    columnId: string;
  } | null>(null);
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
      setActiveCell(null);
      setSorting(updater);
    },
    [setSorting],
  );

  const wasDraggedRef = React.useRef(false);
  const columnOrderRef = React.useRef(columnOrder);
  columnOrderRef.current = columnOrder;

  const { onHeaderMouseDown: handleHeaderMouseDown } = useColumnDragReorder({
    getColumnOrder: () => columnOrderRef.current,
    setColumnOrder: (updater) => setColumnOrder(updater),
    onDragActivated: () => {
      wasDraggedRef.current = true;
    },
    onDragEnded: (activated) => {
      if (!activated) {
        wasDraggedRef.current = false;
      }
    },
  });

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting, columnSizing, columnOrder },
    onSortingChange: handleSortingChange,
    onColumnSizingChange: setColumnSizing,
    onColumnOrderChange: setColumnOrder,
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
        style={{
          width: "100%",
          height: "100%",
          overflow: "auto",
          outline: "none",
        }}
        // biome-ignore lint/a11y/noNoninteractiveTabindex: needed for webview keyboard focus
        tabIndex={0}
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
                      data-column-id={header.id}
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
                        cursor: header.column.getCanSort() ? "grab" : "default",
                        fontWeight: 600,
                        color: "var(--vscode-foreground)",
                        boxSizing: "border-box",
                      }}
                      onMouseDown={(event) =>
                        handleHeaderMouseDown(header.id, event)
                      }
                      onClick={(event) => {
                        const wasDragged = wasDraggedRef.current;
                        wasDraggedRef.current = false;
                        if (!wasDragged) {
                          header.column.getToggleSortingHandler()?.(event);
                        }
                      }}
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
                          onMouseDown={(event) => {
                            event.stopPropagation();
                            header.getResizeHandler()(event);
                          }}
                          onTouchStart={(event) => {
                            event.stopPropagation();
                            header.getResizeHandler()(event);
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

export const QueryTableRow = React.memo(function QueryTableRow({
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

        const cellSelectionState = classifyCellSelection(
          range,
          index,
          columnIndex,
          selection.isCellSelected(index, columnIndex),
          selection.isCellAnchor(index, columnIndex),
        );
        const cellClasses = buildCellSelectionClassName(cellSelectionState, {
          baseClass: "rdb-rrow-cell",
          classSet: "results",
        });

        return (
          <td
            key={cell.id}
            data-column-id={cell.column.id}
            data-row={index}
            data-col={columnIndex}
            className={cellClasses}
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

export function QueryEmptyState({
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

export function QuerySpinner() {
  return <Icon name="sync" size={14} spin />;
}
