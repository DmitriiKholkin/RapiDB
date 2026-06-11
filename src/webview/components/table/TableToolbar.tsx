import React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraftMap,
  serializeFilterDrafts,
} from "../../../shared/tableTypes";
import { Icon } from "../Icon";
import { type ExportFormat, TableExportActions } from "./TableExportActions";
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
  executionTimeMs?: number;
  mutationBusy: boolean;
  newRowExists: boolean;
  readOnlyTable: boolean;
  selectedCount: number;
  totalCount: number;
  onAddRow: () => void;
  onDeleteSelected: () => void;
  onExport: (format: ExportFormat, filters: unknown[]) => void;
  onRefresh: () => void;
}

export function TableToolbar({
  canSelectAndDeleteRows,
  columns,
  debouncedFilterDrafts,
  deleting,
  executionTimeMs,
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
      <TableExportActions
        disabled={mutationBusy}
        onExport={(format) => onExport(format, activeFilters)}
      />
      <div style={{ flex: 1 }} />
      <span style={{ fontSize: 11, opacity: 0.5 }}>
        {executionTimeMs !== undefined && (
          <span style={{ marginRight: 8 }}>
            {executionTimeMs < 1000
              ? `${executionTimeMs}ms`
              : `${(executionTimeMs / 1000).toFixed(1)}s`}
          </span>
        )}
        {`${totalCount.toLocaleString()} rows total`}
      </span>
    </div>
  );
}
