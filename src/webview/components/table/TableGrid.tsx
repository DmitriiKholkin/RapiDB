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
import React, { useMemo } from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraft,
  type FilterDraftMap,
  isNumericCategory,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import type {
  EditTarget,
  InsertDraftRow,
  PendingEdits,
  Row,
} from "../../types";
import { Icon } from "../Icon";
import { CellDisplay } from "./CellDisplay";
import { ColumnFilterControl } from "./ColumnFilterControl";
import { EditInput, valueToEditString } from "./EditInput";
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
  ].join("\n");
  document.head.appendChild(styleElement);
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
  onStartDraftEdit: (column: ColumnMeta) => void;
  onStartEdit: (rowIdx: number, column: ColumnMeta) => void;
  pendingEdits: PendingEdits;
  rows: Row[];
  scrollRef: React.RefObject<HTMLDivElement | null>;
  selected: ReadonlySet<number>;
  sort: TableSortState;
}

export function TableGrid({
  canEditRows,
  canSelectAndDeleteRows,
  colSizes,
  columns,
  editCell,
  filterDrafts,
  loading,
  newRow,
  onCancelEdit,
  onCommitCellEdit,
  onCommitDraftCellEdit,
  onFilterDraftChange,
  onSelectionChange,
  onSort,
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
          minSize: 40,
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
      ref={scrollRef}
      style={{ flex: 1, overflow: "auto", position: "relative" }}
    >
      <table
        style={{
          width: tanTable.getTotalSize(),
          borderCollapse: "collapse",
          tableLayout: "fixed",
          fontSize: 12,
          fontFamily: "var(--vscode-editor-font-family, monospace)",
        }}
      >
        <thead>
          {tanTable.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                const isSelectionColumn = header.column.id === "__sel";
                const columnId = header.column.id;
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
                      width: header.getSize(),
                      height: HEADER_H,
                      padding: isSelectionColumn ? "0 6px" : "0 8px",
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
                      overflow: "hidden",
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
                    <div
                      style={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: isSelectionColumn
                          ? "center"
                          : "flex-start",
                        gap: 4,
                        overflow: "hidden",
                      }}
                    >
                      <span
                        style={{ overflow: "hidden", textOverflow: "ellipsis" }}
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
                    </div>
                    {!isSelectionColumn && header.column.getCanResize() && (
                      <button
                        type="button"
                        aria-label={`Resize ${columnId} column`}
                        tabIndex={-1}
                        onMouseDown={(event) => {
                          event.stopPropagation();
                          header.getResizeHandler()(event);
                        }}
                        onClick={(event) => event.stopPropagation()}
                        style={{
                          position: "absolute",
                          right: 0,
                          top: 0,
                          height: "100%",
                          width: 5,
                          padding: 0,
                          border: "none",
                          cursor: "col-resize",
                          background: header.column.getIsResizing()
                            ? "var(--vscode-focusBorder)"
                            : "transparent",
                        }}
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

              return (
                <th
                  key={`${header.id}_f`}
                  style={{
                    width: header.getSize(),
                    height: FILTER_H,
                    padding: isSelectionColumn ? 0 : "2px 4px",
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
                  }}
                >
                  {isSelectionColumn ? (
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
          {virtualItems.length > 0 && virtualItems[0].start > 0 && (
            <tr>
              <td
                colSpan={tanColumns.length}
                style={{
                  height: virtualItems[0].start,
                  padding: 0,
                  border: "none",
                }}
              />
            </tr>
          )}
          {virtualItems.map((virtualRow) => {
            if (hasDraftRow && virtualRow.index === 0) {
              return (
                <DraftTableRow
                  key={virtualRow.key}
                  columns={columns}
                  visibleColumns={visibleColumns}
                  draft={newRow}
                  editingCol={editCell?.kind === "draft" ? editCell.col : null}
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
                onStartEdit={onStartEdit}
              />
            );
          })}
          {virtualItems.length > 0 &&
            (() => {
              const lastVirtualRow = virtualItems[virtualItems.length - 1];
              const remainingHeight = totalVirtualHeight - lastVirtualRow.end;

              return remainingHeight > 0 ? (
                <tr>
                  <td
                    colSpan={tanColumns.length}
                    style={{
                      height: remainingHeight,
                      padding: 0,
                      border: "none",
                    }}
                  />
                </tr>
              ) : null;
            })()}
        </tbody>
      </table>

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
    </div>
  );
}

const TableRow = React.memo(function TableRow({
  row,
  visualIndex,
  rowIndex,
  isSelected,
  pendingCols,
  columnsMap,
  editingCol,
  onStartEdit,
}: {
  row: TanStackRow<Row>;
  visualIndex: number;
  rowIndex: number;
  isSelected: boolean;
  pendingCols?: Map<string, unknown>;
  columnsMap: Map<string, ColumnMeta>;
  editingCol: string | null;
  onStartEdit: (rowIndex: number, column: ColumnMeta) => void;
}) {
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
      {row.getVisibleCells().map((cell) => {
        const columnId = cell.column.id;
        const columnDef = columnsMap.get(columnId);
        const isPrimaryKey = columnDef?.isPrimaryKey ?? false;
        const keyLabel = isPrimaryKey
          ? columnDef
            ? ""
            : undefined
          : undefined;
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

        return (
          <td
            key={cell.id}
            style={{
              width: cell.column.getSize(),
              height: ROW_H,
              padding: isEditing
                ? "0"
                : isSelectionColumn
                  ? "0 6px"
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
              cursor: canOpenCellEditor ? "pointer" : "default",
              userSelect: isSelectionColumn ? "auto" : "none",
              background: isCellPending ? "rgba(200, 150, 0, 0.23)" : undefined,
            }}
            title={
              isPrimaryKey
                ? `${primaryKeyLabel ?? keyLabel}: ${String(cell.getValue())}`
                : undefined
            }
            onDoubleClick={() => {
              if (columnDef && canOpenCellEditor) {
                onStartEdit(rowIndex, columnDef);
              }
            }}
          >
            {flexRender(cell.column.columnDef.cell, cell.getContext())}
          </td>
        );
      })}
    </tr>
  );
});

const DraftTableRow = React.memo(function DraftTableRow({
  columns,
  visibleColumns,
  draft,
  editingCol,
  onStartEdit,
  onCommit,
  onCancelEdit,
}: {
  columns: readonly ColumnMeta[];
  visibleColumns: readonly TanStackColumn<Row, unknown>[];
  draft: InsertDraftRow;
  editingCol: string | null;
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

        return (
          <td
            key={columnId}
            style={{
              width: column.getSize(),
              height: ROW_H,
              padding: isEditing ? "0" : "0 0 0 8px",
              textAlign: isNumericCategory(columnDef.category)
                ? "right"
                : "left",
              border: "1px solid var(--vscode-panel-border)",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              boxSizing: "border-box",
              verticalAlign: "middle",
              cursor: "pointer",
              userSelect: "none",
              background: "rgba(200, 150, 0, 0.23)",
            }}
            onDoubleClick={() => onStartEdit(columnDef)}
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
                {isEditing ? (
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
                ) : isDefault ? (
                  <span style={{ fontStyle: "italic", opacity: 0.65 }}>
                    DEFAULT
                  </span>
                ) : (
                  <CellDisplay
                    value={displayValue}
                    isPending
                    category={columnDef.category}
                  />
                )}
              </div>
            </div>
          </td>
        );
      })}
    </tr>
  );
});
