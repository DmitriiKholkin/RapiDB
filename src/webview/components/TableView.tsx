import { useEffect, useRef, useState } from "react";
import type { ColumnTypeMeta as ColumnMeta } from "../../shared/tableTypes";
import type { Row } from "../types";
import { postMessage } from "../utils/messaging";
import { readWebviewState, updateWebviewState } from "../utils/vscodeState";
import { GridLoadingOverlay } from "./GridOverlay";
import { TableDialogs } from "./table/TableDialogs";
import type { ExportFormat } from "./table/TableExportActions";
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
  parsePersistedTableDraft,
  parsePersistedTableViewState,
  type TableSortState,
} from "./table/tableViewHelpers";
import { useTableDataController } from "./table/useTableDataController";
import { useTableMutationController } from "./table/useTableMutationController";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  displayTableName?: string;
  isView?: boolean;
  connectionReadOnly?: boolean;
  mongoRowIdentity?: boolean;
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
  connectionId,
  database,
  schema,
  table,
  displayTableName = table,
  isView = false,
  connectionReadOnly = false,
  mongoRowIdentity = false,
  defaultPageSize,
}: Props) {
  const initialPageSize = getInitialPageSize(defaultPageSize);
  const tableDraftKey = JSON.stringify([connectionId, database, schema, table]);
  const [restoredState] = useState(() => {
    const persisted = readWebviewState<Record<string, unknown>>({});
    return {
      draft: parsePersistedTableDraft(persisted.tableDraft, tableDraftKey),
      view: parsePersistedTableViewState(persisted.tableView, tableDraftKey),
    };
  });
  const effectiveReadOnly = isView || connectionReadOnly;
  const columnsRef = useRef<ColumnMeta[]>([]);
  const rowsRef = useRef<Row[]>([]);
  const mongoIdTypesRef = useRef<Array<"objectId" | "string" | null>>([]);
  const pkColsRef = useRef<string[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);
  const fetchPageRef = useRef<() => void>(() => undefined);
  const preserveScrollPositionRef = useRef<() => void>(() => undefined);
  const mutationBridgeRef = useRef<{
    handleRowsCommitted: (
      rows: readonly Row[],
      primaryKeyColumns: readonly string[],
      previousMongoIdTypes: readonly ("objectId" | "string" | null)[],
      mongoIdTypes: readonly ("objectId" | "string" | null)[],
    ) => void;
    resetForTableInit: () => void;
    handleReadFailed: () => void;
    getMetadataRefreshState: () => {
      busy: boolean;
      hasWork: boolean;
      reconciliationPending?: boolean;
    };
  }>({
    handleRowsCommitted: () => undefined,
    resetForTableInit: () => undefined,
    handleReadFailed: () => undefined,
    getMetadataRefreshState: () => ({ busy: false, hasWork: false }),
  });

  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [exportChoice, setExportChoice] = useState<ExportChoiceState | null>(
    null,
  );
  const columnOrderRef = useRef<string[]>([]);
  const hiddenColumnIdsRef = useRef<Set<string>>(new Set());
  const getExportColumnOrder = () =>
    columnOrderRef.current.filter(
      (id) => id !== "__sel" && !hiddenColumnIdsRef.current.has(id),
    );

  const data = useTableDataController({
    initialPageSize,
    initialView: restoredState.view,
    readOnlyTable: effectiveReadOnly,
    columnsRef,
    rowsRef,
    mongoIdTypesRef,
    pkColsRef,
    scrollRef,
    fetchPageRef,
    preserveScrollPositionRef,
    onTableInit: () => mutationBridgeRef.current.resetForTableInit(),
    getMetadataRefreshState: () =>
      mutationBridgeRef.current.getMetadataRefreshState(),
    onReadFailed: () => mutationBridgeRef.current.handleReadFailed(),
    onRowsCommitted: (rows, primaryKeyColumns, previousTypes, nextTypes) =>
      mutationBridgeRef.current.handleRowsCommitted(
        rows,
        primaryKeyColumns,
        previousTypes,
        nextTypes,
      ),
  });

  const hasPrimaryKey = data.pkCols.length > 0;
  const canEditRows = !data.readOnlyTable && hasPrimaryKey;
  const canSelectAndDeleteRows = !data.readOnlyTable && hasPrimaryKey;

  const mutation = useTableMutationController({
    initialDraft: restoredState.draft,
    tableDraftKey,
    mongoRowIdentity,
    canEditRows,
    loadingRef: data.loadingRef,
    metadataBlockedRef: data.metadataBlockedRef,
    committedColumnNamesRef: data.committedColumnNamesRef,
    columnsRef,
    fetchPageRef,
    pkColsRef,
    preserveScrollPositionRef,
    rowsRef,
    mongoIdTypesRef,
    selected,
  });

  mutationBridgeRef.current.resetForTableInit = mutation.resetForTableInit;
  mutationBridgeRef.current.handleRowsCommitted = mutation.handleRowsCommitted;
  mutationBridgeRef.current.handleReadFailed = mutation.handleReadFailed;
  mutationBridgeRef.current.getMetadataRefreshState =
    mutation.getMetadataRefreshState;

  useEffect(() => {
    if (!data.hasCommittedData) return;
    updateWebviewState((state) => ({
      ...state,
      tableView: {
        tableKey: tableDraftKey,
        page: data.intendedPage,
        pageSize: data.requestedPageSize,
        sort: data.requestedSort,
        filters: data.filterDrafts,
      },
    }));
  }, [
    data.filterDrafts,
    data.hasCommittedData,
    data.intendedPage,
    data.requestedPageSize,
    data.requestedSort,
    tableDraftKey,
  ]);

  // Retry deferred metadata only after mutation/editor state has committed.
  useEffect(() => {
    void data.metadataTick;
    void mutation.pendingEdits;
    void mutation.newRows;
    void mutation.applying;
    void mutation.deleting;
    void mutation.editCell;
    void mutation.structuredCellDialog;
    void mutation.canUndo;
    void mutation.canRedo;
    data.retryMetadataRefresh();
  }, [
    data.metadataTick,
    data.retryMetadataRefresh,
    mutation.pendingEdits,
    mutation.newRows,
    mutation.applying,
    mutation.deleting,
    mutation.editCell,
    mutation.structuredCellDialog,
    mutation.canUndo,
    mutation.canRedo,
  ]);

  useEffect(() => {
    void data.rows;
    setSelected(new Set());
  }, [data.rows]);

  const totalPages = Math.max(1, Math.ceil(data.totalCount / data.pageSize));
  const unresolvedPendingCount = mutation.unresolvedPendingEdits.length;
  const unresolvedPendingColumns = [
    ...new Set(
      mutation.unresolvedPendingEdits.flatMap((entry) => [
        ...entry.changes.keys(),
      ]),
    ),
  ];
  const pendingCount = mutation.pendingEdits.size + unresolvedPendingCount;
  const unsavedRowCount = pendingCount + mutation.newRows.length;
  const insertValueCount = mutation.newRows.reduce(
    (sum, row) =>
      sum +
      Object.values(row).filter(
        (cell) => cell.value !== INSERT_DEFAULT_SENTINEL,
      ).length,
    0,
  );
  const showMissingPrimaryKeyNotice =
    data.hasCommittedData &&
    !data.readOnlyTable &&
    data.isInitialized &&
    !hasPrimaryKey;
  const mutationBusy = data.loading || mutation.applying || mutation.deleting;
  const canLoadDifferentRows =
    unsavedRowCount === 0 && !mutationBusy && !mutation.reconciliationRequired;
  const guardRowNavigation = (action: () => void) => {
    if (data.loadingRef.current) return;
    if (!canLoadDifferentRows) {
      mutation.blockNavigationWithUnsavedChanges();
      return;
    }
    action();
  };
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
        aria-label={`Table data for ${displayTableName}`}
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
      aria-label={`Table data for ${displayTableName}`}
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
        schemaWarning={data.schemaWarning}
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
        executionTimeMs={data.executionTimeMs}
        mutationBusy={mutationBusy}
        schemaBlocked={
          Boolean(data.schemaWarning) ||
          mutation.reconciliationRequired ||
          unresolvedPendingCount > 0
        }
        draftRowCount={mutation.newRows.length}
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
            columnOrder: getExportColumnOrder(),
          });
        }}
        onRefresh={() => {
          if (!mutation.retryReconciliation()) {
            if (unresolvedPendingCount > 0) data.fetchPage();
            else guardRowNavigation(data.fetchPage);
          }
        }}
      />

      <TableMutationStatusBar
        applyStatus={mutation.applyStatus}
        schemaBlocked={
          Boolean(data.schemaWarning) ||
          mutation.reconciliationRequired ||
          unresolvedPendingCount > 0
        }
        reconciliationPending={mutation.reconciliationRequired}
        applying={mutation.applying}
        loading={data.loading}
        insertValueCount={insertValueCount}
        mutErr={mutation.mutErr}
        newRowExists={mutation.newRows.length > 0}
        readOnlyTable={data.readOnlyTable}
        unsavedRowCount={unsavedRowCount}
        unresolvedPendingCount={unresolvedPendingCount}
        unresolvedPendingColumns={unresolvedPendingColumns}
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
        key={JSON.stringify(data.columns.map((column) => column.name))}
        columnOrderRef={columnOrderRef}
        hiddenColumnIdsRef={hiddenColumnIdsRef}
        canEditRows={
          canEditRows &&
          !mutationBusy &&
          !mutation.reconciliationRequired &&
          unresolvedPendingCount === 0
        }
        getRowMutationBlockReason={mutation.getRowMutationBlockReason}
        canSelectAndDeleteRows={
          canSelectAndDeleteRows &&
          !mutationBusy &&
          !data.schemaWarning &&
          !mutation.reconciliationRequired &&
          unresolvedPendingCount === 0
        }
        colSizes={data.colSizes}
        columns={data.columns}
        editCell={mutation.editCell}
        filterDrafts={data.filterDrafts}
        loading={data.loading}
        loadingRef={data.loadingRef}
        fetchEpochRef={data.fetchEpochRef}
        newRows={mutation.newRows}
        onCancelEdit={() => mutation.setEditCell(null)}
        onBatchCellEdit={mutation.commitBatchCellEdits}
        onCommitCellEdit={mutation.commitCellEdit}
        onCommitDraftCellEdit={mutation.commitDraftCellEdit}
        onMixedBatchEdit={mutation.commitMixedBatchEdits}
        onFilterDraftChange={(column, draft, options) =>
          guardRowNavigation(() =>
            data.updateFilterDraft(column, draft, options),
          )
        }
        onOpenStructuredCell={mutation.openStructuredCellDialog}
        onSelectionChange={setSelected}
        onSort={(column) => guardRowNavigation(() => data.handleSort(column))}
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
          guardRowNavigation(() =>
            data.setRequestedPage((currentPage) =>
              Math.min(totalPages, currentPage + 1),
            ),
          )
        }
        onPreviousPage={() =>
          guardRowNavigation(() =>
            data.setRequestedPage((currentPage) =>
              Math.max(1, currentPage - 1),
            ),
          )
        }
        onPageSizeChange={(pageSize) =>
          guardRowNavigation(() => {
            data.setRequestedPageSize(pageSize);
            data.setRequestedPage(1);
          })
        }
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
            columnOrder: getExportColumnOrder(),
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
            columnOrder: getExportColumnOrder(),
          });
          setExportChoice(null);
        }}
        onSetStructuredCellDialogNull={mutation.setStructuredCellDialogNull}
        onSetStructuredCellDialogDefault={
          mutation.setStructuredCellDialogDefault
        }
      />
    </main>
  );
}
