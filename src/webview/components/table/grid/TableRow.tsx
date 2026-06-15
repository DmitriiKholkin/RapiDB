import { flexRender, type Row as TanStackRow } from "@tanstack/react-table";
import React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  isNumericCategory,
} from "../../../../shared/tableTypes";
import type { Row } from "../../../types";
import {
  buildCellSelectionClassName,
  classifyCellSelection,
} from "../../../utils/cellSelectionBorders";
import { Icon } from "../../Icon";
import { isCollapsedWidth } from "../gridSubComponents";
import {
  getStructuredCellDialogValue,
  type StructuredCellDialogValue,
} from "../structuredCellDialog";
import { canEditColumn } from "../tableCellUtils";
import { ROW_H } from "../tableConstants";

export function TableRow({
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

        const cellSelectionState = isDataCol
          ? classifyCellSelection(
              range,
              rowIndex,
              cellIndex,
              selection.isCellSelected(rowIndex, cellIndex),
              selection.isCellAnchor(rowIndex, cellIndex),
            )
          : {
              selected: false,
              anchor: false,
              top: false,
              bottom: false,
              left: false,
              right: false,
            };

        const cellClasses = buildCellSelectionClassName(cellSelectionState);

        return (
          <td
            key={cell.id}
            data-row={isDataCol ? rowIndex : undefined}
            data-col={isDataCol ? cellIndex : undefined}
            className={cellClasses}
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
