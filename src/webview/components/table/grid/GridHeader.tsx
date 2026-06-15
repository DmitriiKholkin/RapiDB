import { flexRender, type Header } from "@tanstack/react-table";
import React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraftMap,
} from "../../../../shared/tableTypes";
import type { Row } from "../../../types";
import { Icon } from "../../Icon";
import { ColumnFilterControl } from "../ColumnFilterControl";
import {
  ColumnResizeHandle,
  getDisplaySize,
  HeaderContent,
  isCollapsedWidth,
} from "../gridSubComponents";
import { buildColumnHeaderTitle } from "../tableCellUtils";
import {
  FILTER_H,
  HEADER_H,
  SR_ONLY_STYLE,
  type TableSortState,
} from "../tableConstants";

interface GridHeaderProps {
  headers: Header<Row, unknown>[];
  columnsMap: Map<string, ColumnMeta>;
  filterDrafts: FilterDraftMap;
  sort: TableSortState;
  columnSizing: Record<string, number>;
  onSort: (column: string) => void;
  onFilterDraftChange: (
    columnName: string,
    nextDraft: FilterDraftMap[string],
    options?: { applyImmediately?: boolean },
  ) => void;
  setColumnSizing: React.Dispatch<React.SetStateAction<Record<string, number>>>;
  wasDraggedRef: React.MutableRefObject<boolean>;
  handleHeaderMouseDown: (columnId: string, event: React.MouseEvent) => void;
}

export function GridHeader({
  headers,
  columnsMap,
  filterDrafts,
  sort,
  columnSizing,
  onSort,
  onFilterDraftChange,
  setColumnSizing,
  wasDraggedRef,
  handleHeaderMouseDown,
}: GridHeaderProps) {
  return (
    <thead>
      {/* Header row */}
      <tr>
        {headers.map((header) => {
          const isSelectionColumn = header.column.id === "__sel";
          const columnId = header.column.id;
          const headerSize = header.getSize();
          const isCollapsed = isCollapsedWidth(headerSize);
          const displayHeaderSize = getDisplaySize(headerSize);
          const isSorted = sort?.column === columnId;
          const sortDirection = isSorted ? (sort?.direction ?? null) : null;
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
                justifyContent={isSelectionColumn ? "center" : "flex-start"}
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

      {/* Filter row */}
      <tr>
        {headers.map((header) => {
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
                background: "var(--vscode-editorGroupHeader-tabsBackground)",
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
                    onFilterDraftChange(header.column.id, nextDraft, options)
                  }
                />
              ) : null}
            </th>
          );
        })}
      </tr>
    </thead>
  );
}
