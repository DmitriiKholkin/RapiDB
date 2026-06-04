import React, { useEffect, useRef, useState } from "react";
import type { ColumnTypeMeta as ColumnMeta } from "../../shared/tableTypes";
import type { Row } from "../types";
import { postMessage } from "../utils/messaging";
import { GridLoadingOverlay } from "./GridOverlay";
import { TableDialogs } from "./table/TableDialogs";
import { type ExportFormat } from "./table/TableExportActions";
import { TableFooter } from "./table/TableFooter";
import { TableGrid } from "./table/TableGrid";
import {
  TableMutationStatusBar,
  TableStatusBanners,
} from "./table/TableStatusBanners";
import { TableToolbar } from "./table/TableToolbar";
import {
  getInitialPageSize,
  INSERT_DEFAULT_SENTINEL,
  type TableSortState,
} from "./table/tableViewHelpers";
import { useTableDataController } from "./table/useTableDataController";
import { useTableMutationController } from "./table/useTableMutationController";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  isView?: boolean;
  connectionReadOnly?: boolean;
  defaultPageSize?: number;
}

interface ExportChoiceState {
  format: ExportFormat;
  filters: unknown[];
  sort: TableSortState;
}

function resolveTableExportMessageType(format: ExportFormat) {
  return format === "csv" ? "exportCSV" : "exportJSON";
}

export function TableView({
  table,
  isView = false,
  connectionReadOnly = false,
  defaultPageSize,
}: Props) {
  const initialPageSize = getInitialPageSize(defaultPageSize);
  const effectiveReadOnly = isView || connectionReadOnly;
  const columnsRef = useRef<ColumnMeta[]>([]);
  const rowsRef = useRef<Row[]>([]);
  const pkColsRef = useRef<string[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);
  const fetchPageRef = useRef<() => void>(() => undefined);
  const preserveScrollPositionRef = useRef<() => void>(() => undefined);
  const mutationBridgeRef = useRef<{
    handleRowsCommitted: (
      rows: readonly Row[],
      primaryKeyColumns: readonly string[],
    ) => void;
    resetForTableInit: () => void;
  }>({
    handleRowsCommitted: () => undefined,
    resetForTableInit: () => undefined,
  });

  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [exportChoice, setExportChoice] = useState<ExportChoiceState | null>(
    null,
  );

  const data = useTableDataController({
    initialPageSize,
    readOnlyTable: effectiveReadOnly,
    columnsRef,
    rowsRef,
    pkColsRef,
    scrollRef,
    fetchPageRef,
    preserveScrollPositionRef,
    onTableInit: () => mutationBridgeRef.current.resetForTableInit(),
    onRowsCommitted: (rows, primaryKeyColumns) =>
      mutationBridgeRef.current.handleRowsCommitted(rows, primaryKeyColumns),
  });

  const hasPrimaryKey = data.pkCols.length > 0;
  const canEditRows = !data.readOnlyTable && hasPrimaryKey;
  const canSelectAndDeleteRows = !data.readOnlyTable && hasPrimaryKey;

  const mutation = useTableMutationController({
    canEditRows,
    columnsRef,
    fetchPageRef,
    pkColsRef,
    preserveScrollPositionRef,
    rowsRef,
    selected,
  });

  mutationBridgeRef.current.resetForTableInit = mutation.resetForTableInit;
  mutationBridgeRef.current.handleRowsCommitted = mutation.handleRowsCommitted;

  useEffect(() => {
    void data.rows;
    setSelected(new Set());
  }, [data.rows]);

  const totalPages = Math.max(1, Math.ceil(data.totalCount / data.pageSize));
  const pendingCount = mutation.pendingEdits.size;
  const unsavedRowCount = pendingCount + (mutation.newRow ? 1 : 0);
  const insertValueCount = mutation.newRow
    ? Object.values(mutation.newRow).filter(
        (cell) => cell.value !== INSERT_DEFAULT_SENTINEL,
      ).length
    : 0;
  const showMissingPrimaryKeyNotice =
    data.hasCommittedData &&
    !data.readOnlyTable &&
    data.isInitialized &&
    !hasPrimaryKey;
  const mutationBusy =
    mutation.applying || mutation.deleting || mutation.inserting;
  const showRefetchOverlay = data.loading && data.hasCommittedData;

  if (data.error) {
    return (
      <div
        style={{
          margin: 12,
          padding: "10px 14px",
          borderRadius: 3,
          fontSize: 13,
          background: "var(--vscode-inputValidation-errorBackground)",
          border: "1px solid var(--vscode-inputValidation-errorBorder)",
          color: "var(--vscode-errorForeground)",
        }}
      >
        <strong>Error:</strong> {data.error}
      </div>
    );
  }

  if (!data.hasCommittedData) {
    return (
      <main
        aria-label={`Table data for ${table}`}
        aria-busy="true"
        style={{
          display: "flex",
          flexDirection: "column",
          height: "100vh",
          overflow: "hidden",
          position: "relative",
        }}
      >
        <GridLoadingOverlay mode="fullscreen" message="Loading data..." />
      </main>
    );
  }

  return (
    <main
      aria-label={`Table data for ${table}`}
      aria-busy={showRefetchOverlay}
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100vh",
        overflow: "hidden",
        position: "relative",
      }}
    >
      {showRefetchOverlay && (
        <GridLoadingOverlay
          mode="overlay"
          message="Loading data..."
          trapFocus
        />
      )}

      <TableStatusBanners
        filterError={data.filterError}
        readError={data.readError}
        showMissingPrimaryKeyNotice={showMissingPrimaryKeyNotice}
        onDismissFilterError={() => data.setFilterError(null)}
        onDismissReadError={() => data.setReadError(null)}
      />

      <TableToolbar
        canSelectAndDeleteRows={canSelectAndDeleteRows}
        columns={data.columns}
        debouncedFilterDrafts={data.debouncedFilterDrafts}
        deleting={mutation.deleting}
        mutationBusy={mutationBusy}
        newRowExists={mutation.newRow !== null}
        readOnlyTable={data.readOnlyTable}
        selectedCount={selected.size}
        totalCount={data.totalCount}
        onAddRow={mutation.startInsertRow}
        onDeleteSelected={mutation.deleteSelected}
        onExport={(format, filters) => {
          if (data.totalCount > data.rows.length) {
            setExportChoice({ format, filters, sort: data.sort });
            return;
          }

          postMessage(resolveTableExportMessageType(format), {
            sort: data.sort,
            filters,
          });
        }}
        onRefresh={data.fetchPage}
      />

      <TableMutationStatusBar
        applyStatus={mutation.applyStatus}
        applying={mutation.applying}
        inserting={mutation.inserting}
        insertValueCount={insertValueCount}
        mutErr={mutation.mutErr}
        newRowExists={mutation.newRow !== null}
        readOnlyTable={data.readOnlyTable}
        unsavedRowCount={unsavedRowCount}
        canUndo={mutation.canUndo}
        canRedo={mutation.canRedo}
        onApplyChanges={mutation.applyChanges}
        onDismissApplyStatus={mutation.dismissApplyStatus}
        onDismissMutationError={mutation.dismissMutationError}
        onRevertChanges={mutation.revertChanges}
        onUndo={mutation.undoAction}
        onRedo={mutation.redoAction}
      />

      <TableGrid
        key={data.columns.map((column) => column.name).join("|")}
        canEditRows={canEditRows}
        canSelectAndDeleteRows={canSelectAndDeleteRows}
        colSizes={data.colSizes}
        columns={data.columns}
        editCell={mutation.editCell}
        filterDrafts={data.filterDrafts}
        loading={data.loading}
        newRow={mutation.newRow}
        onCancelEdit={() => mutation.setEditCell(null)}
        onCommitCellEdit={mutation.commitCellEdit}
        onCommitDraftCellEdit={mutation.commitDraftCellEdit}
        onFilterDraftChange={data.updateFilterDraft}
        onOpenStructuredCell={mutation.openStructuredCellDialog}
        onSelectionChange={setSelected}
        onSort={data.handleSort}
        onStartDraftEdit={mutation.handleStartDraftEdit}
        onStartEdit={mutation.handleStartEdit}
        pendingEdits={mutation.pendingEdits}
        rows={data.rows}
        scrollRef={scrollRef}
        selected={selected}
        sort={data.sort}
      />

      <TableFooter
        page={data.page}
        pageSize={data.pageSize}
        totalPages={totalPages}
        onNextPage={() =>
          data.setRequestedPage((currentPage) =>
            Math.min(totalPages, currentPage + 1),
          )
        }
        onPreviousPage={() =>
          data.setRequestedPage((currentPage) => Math.max(1, currentPage - 1))
        }
        onPageSizeChange={(pageSize) => {
          data.setRequestedPageSize(pageSize);
          data.setRequestedPage(1);
        }}
      />

      <TableDialogs
        exportChoice={exportChoice}
        mutationPreview={mutation.mutationPreview}
        structuredCellDialog={mutation.structuredCellDialog}
        rowsLength={data.rows.length}
        totalCount={data.totalCount}
        onCancelExport={() => setExportChoice(null)}
        onCancelMutationPreview={mutation.cancelMutationPreview}
        onCancelStructuredCellDialog={mutation.cancelStructuredCellDialog}
        onChangeStructuredCellDialog={mutation.updateStructuredCellDialogDraft}
        onConfirmMutationPreview={mutation.confirmMutationPreview}
        onConfirmStructuredCellDialog={mutation.confirmStructuredCellDialog}
        onExportAll={() => {
          if (!exportChoice) {
            return;
          }

          postMessage(resolveTableExportMessageType(exportChoice.format), {
            sort: exportChoice.sort,
            filters: exportChoice.filters,
          });
          setExportChoice(null);
        }}
        onExportVisible={() => {
          if (!exportChoice) {
            return;
          }

          postMessage(resolveTableExportMessageType(exportChoice.format), {
            sort: exportChoice.sort,
            filters: exportChoice.filters,
            limitToPage: {
              page: data.page,
              pageSize: data.pageSize,
            },
          });
          setExportChoice(null);
        }}
        onSetStructuredCellDialogNull={mutation.setStructuredCellDialogNull}
      />
    </main>
  );
}
