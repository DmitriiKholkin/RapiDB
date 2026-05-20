import React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraftMap,
  serializeFilterDrafts,
} from "../../../shared/tableTypes";
import { Icon } from "../Icon";
import {
  type TableSortState,
  TOOLBAR_H,
  tableButtonStyle,
} from "./tableViewHelpers";

interface TableToolbarProps {
  canSelectAndDeleteRows: boolean;
  columns: readonly ColumnMeta[];
  debouncedFilterDrafts: FilterDraftMap;
  deleting: boolean;
  mutationBusy: boolean;
  newRowExists: boolean;
  readOnlyTable: boolean;
  selectedCount: number;
  totalCount: number;
  onAddRow: () => void;
  onDeleteSelected: () => void;
  onExport: (format: "csv" | "json", filters: unknown[]) => void;
  onRefresh: () => void;
}

export function TableToolbar({
  canSelectAndDeleteRows,
  columns,
  debouncedFilterDrafts,
  deleting,
  mutationBusy,
  newRowExists,
  readOnlyTable,
  selectedCount,
  totalCount,
  onAddRow,
  onDeleteSelected,
  onExport,
  onRefresh,
}: TableToolbarProps) {
  const activeFilters = serializeFilterDrafts(columns, debouncedFilterDrafts);

  return (
    <div
      style={{
        height: TOOLBAR_H,
        flexShrink: 0,
        display: "flex",
        alignItems: "center",
        gap: 6,
        padding: "0 10px",
        borderBottom: "1px solid var(--vscode-panel-border)",
        background: "var(--vscode-editorGroupHeader-tabsBackground)",
      }}
    >
      {!readOnlyTable && (
        <>
          <button
            type="button"
            style={tableButtonStyle("primary", mutationBusy || newRowExists)}
            disabled={mutationBusy || newRowExists}
            onClick={onAddRow}
          >
            <Icon name="add" size={13} style={{ marginRight: 4 }} />
            Add Row
          </button>
          {canSelectAndDeleteRows && (
            <button
              type="button"
              style={tableButtonStyle(
                "danger",
                selectedCount === 0 || mutationBusy,
              )}
              disabled={selectedCount === 0 || mutationBusy}
              onClick={onDeleteSelected}
            >
              <Icon name="trash" size={13} style={{ marginRight: 4 }} />
              {deleting ? "Deleting…" : `Delete (${selectedCount})`}
            </button>
          )}
        </>
      )}

      <button
        type="button"
        style={tableButtonStyle("ghost", mutationBusy)}
        disabled={mutationBusy}
        onClick={onRefresh}
      >
        <Icon name="refresh" size={13} style={{ marginRight: 4 }} />
        Refresh
      </button>
      <button
        type="button"
        style={tableButtonStyle("ghost", mutationBusy)}
        disabled={mutationBusy}
        onClick={() => onExport("csv", activeFilters)}
      >
        <Icon name="export" size={13} style={{ marginRight: 4 }} />
        Export CSV
      </button>
      <button
        type="button"
        style={tableButtonStyle("ghost", mutationBusy)}
        disabled={mutationBusy}
        onClick={() => onExport("json", activeFilters)}
      >
        <Icon name="export" size={13} style={{ marginRight: 4 }} />
        Export JSON
      </button>
      <div style={{ flex: 1 }} />
      <span style={{ fontSize: 11, opacity: 0.5 }}>
        {`${totalCount.toLocaleString()} rows total`}
      </span>
    </div>
  );
}
