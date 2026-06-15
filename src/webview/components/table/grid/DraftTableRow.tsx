import {
  flexRender,
  type Column as TanStackColumn,
} from "@tanstack/react-table";
import React, { useMemo } from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  isNumericCategory,
  NULL_SENTINEL,
} from "../../../../shared/tableTypes";
import type { InsertDraftRow, Row } from "../../../types";
import {
  buildCellSelectionClassName,
  classifyCellSelection,
} from "../../../utils/cellSelectionBorders";
import { CellDisplay } from "../CellDisplay";
import { EditInput, valueToEditString } from "../EditInput";
import { isCollapsedWidth } from "../gridSubComponents";
import {
  getStructuredCellDialogValue,
  type StructuredCellDialogValue,
} from "../structuredCellDialog";
import { HEADER_H, INSERT_DEFAULT_SENTINEL, ROW_H } from "../tableConstants";

function toDraftEditInitialValue(value: unknown): string {
  if (value === INSERT_DEFAULT_SENTINEL) {
    return "";
  }

  return valueToEditString(value);
}

export function DraftTableRow({
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
        const cellSelectionState = isDataCol
          ? classifyCellSelection(
              selection?.range ?? null,
              selRow,
              colIndex,
              selection?.isCellSelected(selRow, colIndex) ?? false,
              selection?.isCellAnchor(selRow, colIndex) ?? false,
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
            key={columnId}
            data-row={isDataCol ? selRow : undefined}
            data-col={isDataCol ? colIndex : undefined}
            className={cellClasses}
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
