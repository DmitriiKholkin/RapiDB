import { useCallback, useEffect, useRef, useState } from "react";
import type {
  ColumnTypeMeta as ColumnMeta,
  MongoValueTypeHint,
} from "../../shared/tableTypes";
import type { Row } from "../types";
import { postMessage } from "../utils/messaging";
import { readWebviewState, updateWebviewState } from "../utils/vscodeState";
import { GridLoadingOverlay } from "./GridOverlay";
import { TableDialogs } from "./table/TableDialogs";
import type { ExportFormat } from "./table/TableExportActions";
import { TableFooter } from "./table/TableFooter";
import { TableGrid, type TableGridLayout } from "./table/TableGrid";
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
  type TableFetchOptions,
  type TableSortState,
  tableButtonStyle,
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
  page: number;
  pageSize: number;
  columnOrder: string[];
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
    const draft = parsePersistedTableDraft(persisted.tableDraft, tableDraftKey);
    return {
      draft,
      view:
        draft?.mutation?.conflictView ??
        parsePersistedTableViewState(persisted.tableView, tableDraftKey),
    };
  });
  const effectiveReadOnly = isView || connectionReadOnly;
  const columnsRef = useRef<ColumnMeta[]>([]);
  const rowsRef = useRef<Row[]>([]);
  const mongoIdTypesRef = useRef<Array<"objectId" | "string" | null>>([]);
  const rowValueTypesRef = useRef<Array<Record<string, MongoValueTypeHint>>>(
    [],
  );
  const pkColsRef = useRef<string[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);
  const fetchPageRef = useRef<(options?: TableFetchOptions) => void>(
    () => undefined,
  );
  const preserveScrollPositionRef = useRef<() => void>(() => undefined);
  const mutationBridgeRef = useRef<{
    handleRowsCommitted: (
      rows: readonly Row[],
      primaryKeyColumns: readonly string[],
      previousMongoIdTypes: readonly ("objectId" | "string" | null)[],
      mongoIdTypes: readonly ("objectId" | "string" | null)[],
      previousRowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
      nextRowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
      conflictRefresh?: boolean,
    ) => void;
    resetForTableInit: () => void;
    handleReadFailed: () => void;
    isFilterNavigationBlocked: () => boolean;
    getMetadataRefreshState: () => {
      busy: boolean;
      hasWork: boolean;
      reconciliationPending?: boolean;
      applyConflict?: boolean;
      applying?: boolean;
    };
  }>({
    handleRowsCommitted: () => undefined,
    resetForTableInit: () => undefined,
    handleReadFailed: () => undefined,
    isFilterNavigationBlocked: () => false,
    getMetadataRefreshState: () => ({ busy: false, hasWork: false }),
  });

  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [exportChoice, setExportChoice] = useState<ExportChoiceState | null>(
    null,
  );
  const columnOrderRef = useRef<string[]>([]);
  const hiddenColumnIdsRef = useRef<Set<string>>(new Set());
  const gridLayoutRef = useRef<TableGridLayout | null>(null);
  const pasteStructureEpochRef = useRef(0);
  const data = useTableDataController({
    pasteStructureEpochRef,
    initialPageSize,
    initialView: restoredState.view,
    readOnlyTable: effectiveReadOnly,
    columnsRef,
    rowsRef,
    mongoIdTypesRef,
    rowValueTypesRef,
    pkColsRef,
    scrollRef,
    fetchPageRef,
    preserveScrollPositionRef,
    onTableInit: () => mutationBridgeRef.current.resetForTableInit(),
    getMetadataRefreshState: () =>
      mutationBridgeRef.current.getMetadataRefreshState(),
    isFilterNavigationBlocked: () =>
      mutationBridgeRef.current.isFilterNavigationBlocked(),
    onReadFailed: () => mutationBridgeRef.current.handleReadFailed(),
    onRowsCommitted: (
      rows,
      primaryKeyColumns,
      previousTypes,
      nextTypes,
      previousRowTypes,
      nextRowTypes,
      conflictRefresh,
    ) =>
      mutationBridgeRef.current.handleRowsCommitted(
        rows,
        primaryKeyColumns,
        previousTypes,
        nextTypes,
        previousRowTypes,
        nextRowTypes,
        conflictRefresh,
      ),
  });
  const getExportColumnOrder = () => {
    const columnNames = new Set(data.columns.map((column) => column.name));
    return columnOrderRef.current.filter(
      (id) => columnNames.has(id) && !hiddenColumnIdsRef.current.has(id),
    );
  };

  const hasPrimaryKey = data.pkCols.length > 0;
  const canEditRows = !data.readOnlyTable && hasPrimaryKey;
  const canSelectAndDeleteRows = !data.readOnlyTable && hasPrimaryKey;

  const mutation = useTableMutationController({
    pasteStructureEpochRef,
    initialDraft: restoredState.draft,
    tableDraftKey,
    mongoRowIdentity,
    canEditRows,
    canInsertRows: !data.readOnlyTable,
    loadingRef: data.loadingRef,
    metadataBlockedRef: data.metadataBlockedRef,
    committedColumnNamesRef: data.committedColumnNamesRef,
    columnsRef,
    fetchPageRef,
    getCommittedView: data.getCommittedView,
    pkColsRef,
    preserveScrollPositionRef,
    rowsRef,
    mongoIdTypesRef,
    rowValueTypesRef,
    selected,
  });

  mutationBridgeRef.current.resetForTableInit = mutation.resetForTableInit;
  mutationBridgeRef.current.handleRowsCommitted = mutation.handleRowsCommitted;
  mutationBridgeRef.current.handleReadFailed = mutation.handleReadFailed;
  mutationBridgeRef.current.isFilterNavigationBlocked =
    mutation.isFilterNavigationBlocked;
  mutationBridgeRef.current.getMetadataRefreshState =
    mutation.getMetadataRefreshState;
  const { setEditCell } = mutation;
  const cancelCellEdit = useCallback(() => setEditCell(null), [setEditCell]);

  // Remember an explicit sort reset while its replacement read is pending,
  // so a reload won't reissue the old sort.
  const canPersistTableView =
    data.hasCommittedData || (data.initialSortCleared && data.loading);
  const reviewingConflict =
    mutation.applyConflictRefreshRequired ||
    mutation.unresolvedPendingEdits.some((entry) => entry.applyConflict);
  const { getCommittedView } = data;
  useEffect(() => {
    if (!canPersistTableView) return;
    updateWebviewState((state) => ({
      ...state,
      tableView: (reviewingConflict && getCommittedView(tableDraftKey)) || {
        tableKey: tableDraftKey,
        page: data.intendedPage,
        pageSize: data.requestedPageSize,
        sort: data.sort,
        filters: data.filterDrafts,
      },
    }));
  }, [
    canPersistTableView,
    data.filterDrafts,
    data.intendedPage,
    data.requestedPageSize,
    data.sort,
    getCommittedView,
    reviewingConflict,
    tableDraftKey,
  ]);

  useEffect(() => {
    void data.metadataTick;
    void data.loading;
    void mutation.pendingEdits;
    void mutation.newRows;
    void mutation.applying;
    void mutation.deleting;
    void mutation.editCell;
    void mutation.structuredCellDialog;
    void mutation.canUndo;
    void mutation.canRedo;
    void mutation.reconciliationRequired;
    data.retryMetadataRefresh();
    data.retryPendingFilter();
  }, [
    data.metadataTick,
    data.retryMetadataRefresh,
    data.retryPendingFilter,
    data.loading,
    mutation.pendingEdits,
    mutation.newRows,
    mutation.applying,
    mutation.deleting,
    mutation.editCell,
    mutation.structuredCellDialog,
    mutation.canUndo,
    mutation.canRedo,
    mutation.reconciliationRequired,
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
  const unmatchablePendingCount = mutation.unresolvedPendingEdits.filter(
    (entry) => entry.baselineMissing,
  ).length;
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
    unsavedRowCount === 0 &&
    !mutationBusy &&
    !mutation.reconciliationRequired &&
    !mutation.editCell &&
    !mutation.structuredCellDialog;
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
        <div role="alert">
          <strong>Error:</strong> {data.error}
        </div>
        {data.canRetryInitialRead && (
          <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
            <button
              type="button"
              style={tableButtonStyle("primary")}
              onClick={() => data.retryInitialRead()}
            >
              Retry
            </button>
            {data.requestedSort && (
              <button
                type="button"
                style={tableButtonStyle()}
                onClick={() => data.retryInitialRead(true)}
              >
                Clear Sort and Retry
              </button>
            )}
          </div>
        )}
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
        onDismissFilterError={data.clearFilterError}
        onDismissReadError={() => data.setReadError(null)}
      />

      <TableToolbar
        canSelectAndDeleteRows={canSelectAndDeleteRows}
        committedFilters={data.committedFilters}
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
            setExportChoice({
              format,
              filters,
              sort: data.sort,
              page: data.page,
              pageSize: data.pageSize,
              columnOrder: getExportColumnOrder(),
            });
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
            if (unresolvedPendingCount > 0)
              data.fetchPage({ refreshData: true });
            else
              guardRowNavigation(() =>
                data.fetchPage({ refreshMetadata: true }),
              );
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
        conflictDrafts={mutation.unresolvedPendingEdits.filter(
          (entry) => entry.applyConflict,
        )}
        unmatchablePendingCount={unmatchablePendingCount}
        canUndo={mutation.canUndo}
        canRedo={mutation.canRedo}
        onApplyChanges={mutation.applyChanges}
        onDismissApplyStatus={mutation.dismissApplyStatus}
        onDismissMutationError={mutation.dismissMutationError}
        onAcknowledgeDeleteRecovery={
          mutation.canAcknowledgeDeleteRecovery
            ? mutation.acknowledgeDeleteRecovery
            : undefined
        }
        onRevertChanges={mutation.revertChanges}
        onDiscardUnresolved={
          mutation.applyConflictRefreshRequired
            ? undefined
            : mutation.discardUnresolvedPendingEdits
        }
        onUndo={mutation.undoAction}
        onRedo={mutation.redoAction}
      />

      <TableGrid
        key={tableDraftKey}
        columnOrderRef={columnOrderRef}
        hiddenColumnIdsRef={hiddenColumnIdsRef}
        layoutRef={gridLayoutRef}
        layoutKey={tableDraftKey}
        pasteStructureEpochRef={pasteStructureEpochRef}
        mongoColumnTypes={mongoRowIdentity}
        canEditRows={
          canEditRows &&
          !mutationBusy &&
          !mutation.reconciliationRequired &&
          unresolvedPendingCount === 0
        }
        canPasteIntoPersistedRows={canEditRows && unresolvedPendingCount === 0}
        getPasteBlockReason={mutation.getPasteBlockReason}
        getRowMutationBlockReason={mutation.getRowMutationBlockReason}
        canSelectAndDeleteRows={
          canSelectAndDeleteRows &&
          !data.schemaWarning &&
          !mutation.reconciliationRequired &&
          unresolvedPendingCount === 0
        }
        rowSelectionDisabled={mutationBusy}
        colSizes={data.colSizes}
        columns={data.columns}
        nonSortableColumnNames={data.pageScopedColumnNames}
        editCell={mutation.editCell}
        filterDrafts={data.filterDrafts}
        loading={data.loading}
        loadingRef={data.loadingRef}
        fetchEpochRef={data.fetchEpochRef}
        newRows={mutation.newRows}
        onCancelEdit={cancelCellEdit}
        onEditorDirtyChange={mutation.setInlineEditorDirty}
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
        getRowCellColumn={mutation.getRowCellColumn}
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
            columnOrder: exportChoice.columnOrder,
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
              page: exportChoice.page,
              pageSize: exportChoice.pageSize,
            },
            columnOrder: exportChoice.columnOrder,
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
