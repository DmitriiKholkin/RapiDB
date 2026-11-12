import {
  type MutableRefObject,
  type RefObject,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraft,
  type FilterDraftMap,
  type FilterExpression,
  type MongoValueTypeHint,
  serializeFilterDrafts,
} from "../../../shared/tableTypes";
import type { TableInitPayload } from "../../../shared/webviewContracts";
import type { Row } from "../../types";
import {
  calcColWidths,
  type Column as WidthColumn,
} from "../../utils/columnSizing";
import { onMessage, postMessage } from "../../utils/messaging";
import {
  buildActiveFilterDrafts,
  DEBOUNCE,
  type FetchSnapshot,
  getFilterDraftValidationError,
  type PersistedTableViewState,
  type TableSortState,
} from "./tableViewHelpers";

interface UseTableDataControllerParams {
  pasteStructureEpochRef: MutableRefObject<number>;
  initialPageSize: number;
  initialView?: PersistedTableViewState | null;
  readOnlyTable: boolean;
  columnsRef: MutableRefObject<ColumnMeta[]>;
  rowsRef: MutableRefObject<Row[]>;
  mongoIdTypesRef: MutableRefObject<Array<"objectId" | "string" | null>>;
  rowValueTypesRef: MutableRefObject<Array<Record<string, MongoValueTypeHint>>>;
  pkColsRef: MutableRefObject<string[]>;
  scrollRef: RefObject<HTMLDivElement | null>;
  fetchPageRef: MutableRefObject<
    (options?: { refreshMetadata?: boolean }) => void
  >;
  preserveScrollPositionRef: MutableRefObject<() => void>;
  onTableInit: () => void;
  getMetadataRefreshState: () => {
    busy: boolean;
    hasWork: boolean;
    reconciliationPending?: boolean;
  };
  isFilterNavigationBlocked: () => boolean;
  onReadFailed: () => void;
  onRowsCommitted: (
    rows: readonly Row[],
    primaryKeyColumns: readonly string[],
    previousMongoIdTypes: readonly ("objectId" | "string" | null)[],
    mongoIdTypes: readonly ("objectId" | "string" | null)[],
    previousRowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
    nextRowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
  ) => void;
}

function buildTableInitSignature(
  columns: readonly ColumnMeta[],
  primaryKeyColumns: readonly string[],
): string {
  return JSON.stringify({
    columns: columns.map((column) => ({
      name: column.name,
      type: column.type,
      nativeType: column.nativeType,
      nullable: column.nullable,
      isPrimaryKey: column.isPrimaryKey,
      primaryKeyRole: column.primaryKeyRole ?? null,
      isForeignKey: column.isForeignKey,
      category: column.category,
      filterable: column.filterable,
      filterOperators: column.filterOperators,
      valueSemantics: column.valueSemantics,
      identityGeneration: column.identityGeneration ?? null,
      isReadOnly: column.isReadOnly ?? false,
      defaultValue: column.defaultValue,
      isComputed: column.isComputed,
      computedExpression: column.computedExpression,
      generatedKind: column.generatedKind,
      onUpdateExpression: column.onUpdateExpression,
      isPersisted: column.isPersisted,
      bsonSubtype: column.bsonSubtype,
      primaryKeyOrdinal: column.primaryKeyOrdinal,
    })),
    primaryKeyColumns,
  });
}

function hasFilterDraftInput(draft: FilterDraft | undefined): boolean {
  if (!draft) return false;
  if (!("value" in draft)) return true;
  return (typeof draft.value === "string" ? [draft.value] : draft.value).some(
    (value) => value.trim() !== "",
  );
}

function reserveTableReadEpoch(previousEpoch: number): number {
  const vscode = window.__vscode;
  if (!vscode) throw new Error("Table read identity could not be saved.");
  const state = vscode.getState<Record<string, unknown>>() ?? {};
  const savedEpoch =
    state.tableReadEpoch === undefined ? 0 : state.tableReadEpoch;
  if (
    typeof savedEpoch !== "number" ||
    !Number.isSafeInteger(savedEpoch) ||
    savedEpoch < 0 ||
    Math.max(previousEpoch, savedEpoch) >= Number.MAX_SAFE_INTEGER
  ) {
    throw new Error(
      "Table read identity is invalid or exhausted. Reopen this table in a new panel.",
    );
  }
  const epoch = Math.max(previousEpoch, savedEpoch) + 1;
  vscode.setState({ ...state, tableReadEpoch: epoch });
  return epoch;
}

export function useTableDataController({
  pasteStructureEpochRef,
  initialPageSize,
  initialView,
  readOnlyTable: initialReadOnlyTable,
  columnsRef,
  rowsRef,
  mongoIdTypesRef,
  rowValueTypesRef,
  pkColsRef,
  scrollRef,
  fetchPageRef,
  preserveScrollPositionRef,
  onTableInit,
  getMetadataRefreshState,
  isFilterNavigationBlocked,
  onReadFailed,
  onRowsCommitted,
}: UseTableDataControllerParams) {
  const [columns, setColumns] = useState<ColumnMeta[]>([]);
  const [pkCols, setPkCols] = useState<string[]>([]);
  const [rows, setRows] = useState<Row[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const loadingRef = useRef(true);
  const [hasCommittedData, setHasCommittedData] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [readError, setReadError] = useState<string | null>(null);
  const [filterError, setFilterError] = useState<string | null>(null);
  const [filterDraftError, setFilterDraftError] = useState<string | null>(null);
  const [filterNotice, setFilterNotice] = useState<string | null>(null);
  const [page, setPage] = useState(initialView?.page ?? 1);
  const [pageSize, setPageSize] = useState(
    initialView?.pageSize ?? initialPageSize,
  );
  const [requestedPage, setRequestedPage] = useState(initialView?.page ?? 1);
  const [filterDraftPending, setFilterDraftPending] = useState(false);
  const intendedPage = filterDraftPending ? 1 : requestedPage;
  const [requestedPageSize, setRequestedPageSize] = useState(
    initialView?.pageSize ?? initialPageSize,
  );
  const [filterDrafts, setFilterDrafts] = useState<FilterDraftMap>(
    initialView?.filters ?? {},
  );
  const [debouncedFilterDrafts, setDebouncedFilterDrafts] =
    useState<FilterDraftMap>(initialView?.filters ?? {});
  const [sort, setSort] = useState<TableSortState>(initialView?.sort ?? null);
  const [requestedSort, setRequestedSort] = useState<TableSortState>(
    initialView?.sort ?? null,
  );
  const [committedFilters, setCommittedFilters] = useState<FilterExpression[]>(
    [],
  );
  const [readOnlyTable, setReadOnlyTable] = useState(initialReadOnlyTable);
  const [colSizes, setColSizes] = useState<Record<string, number>>({});
  const [isInitialized, setIsInitialized] = useState(false);
  const [executionTimeMs, setExecutionTimeMs] = useState<number | undefined>(
    undefined,
  );

  const colSizesInitedRef = useRef(false);
  const pendingPrimaryKeyColumnsRef = useRef<string[]>([]);
  const pendingReadOnlyTableRef = useRef(initialReadOnlyTable);
  const scrollPreserveRef = useRef<number | null>(null);
  const requestedSortRef = useRef<TableSortState>(requestedSort);
  const fetchSnapshotsRef = useRef<Map<number, FetchSnapshot>>(new Map());
  const hasCommittedDataRef = useRef(hasCommittedData);
  const requestedPageRef = useRef(requestedPage);
  const requestedPageSizeRef = useRef(requestedPageSize);
  const skipNextFetchRef = useRef(false);
  const debouncedFilterDraftsRef = useRef(debouncedFilterDrafts);
  const initializedRef = useRef(false);
  const fetchEpochRef = useRef(0);
  const readIdentityFailedRef = useRef(false);
  const previousFilterDraftsRef = useRef(filterDrafts);
  const filterDraftsRef = useRef(filterDrafts);
  const filterDraftRevisionRef = useRef(0);
  const visibleFilterDraftRevisionRef = useRef<number | null>(null);
  const dismissedFilterDraftRevisionRef = useRef<number | null>(null);
  const [initTick, setInitTick] = useState(0);
  const readOnlyTableRef = useRef(initialReadOnlyTable);
  const initialPageSizeRef = useRef(initialPageSize);
  const preserveRestoredViewRef = useRef(
    initialView !== null && initialView !== undefined,
  );
  const onTableInitRef = useRef(onTableInit);
  const onReadFailedRef = useRef(onReadFailed);
  const onRowsCommittedRef = useRef(onRowsCommitted);
  const tableInitSignatureRef = useRef<string | null>(null);
  const metadataStateRef = useRef(getMetadataRefreshState);
  metadataStateRef.current = getMetadataRefreshState;
  const filterNavigationBlockedRef = useRef(isFilterNavigationBlocked);
  filterNavigationBlockedRef.current = isFilterNavigationBlocked;
  const readyFilterDraftsRef = useRef<FilterDraftMap | null>(null);
  const queuedMetadataRef = useRef<TableInitPayload | null>(null);
  const [metadataTick, setMetadataTick] = useState(0);
  const [schemaWarning, setSchemaWarning] = useState<string | null>(null);
  const metadataBlockedRef = useRef(false);
  const committedColumnNamesRef = useRef<ReadonlySet<string>>(new Set());
  const pageDiscoveredColumnNamesRef = useRef<ReadonlySet<string>>(new Set());
  const metadataNeedsReadRef = useRef(false);
  const refreshRefetchRef = useRef(false);
  const fetchHadFiltersRef = useRef(false);
  const connectionInvalidatedRef = useRef(false);
  const deferredReadRef = useRef(false);
  const reconciliationFetchIdRef = useRef<number | null>(null);

  const getVisibleFilterDraftError = useCallback(
    (drafts: FilterDraftMap) => {
      if (
        dismissedFilterDraftRevisionRef.current ===
        filterDraftRevisionRef.current
      ) {
        return null;
      }
      const error = getFilterDraftValidationError(columnsRef.current, drafts);
      visibleFilterDraftRevisionRef.current = error
        ? filterDraftRevisionRef.current
        : null;
      return error;
    },
    [columnsRef],
  );

  const getRequestedSnapshot = useCallback(
    (): FetchSnapshot => ({
      page: requestedPageRef.current,
      pageSize: requestedPageSizeRef.current,
      sort: requestedSortRef.current,
      filters: serializeFilterDrafts(
        columnsRef.current,
        debouncedFilterDraftsRef.current,
      ),
    }),
    [columnsRef],
  );

  const advanceFetchEpoch = useCallback(() => {
    try {
      const epoch = reserveTableReadEpoch(fetchEpochRef.current);
      fetchEpochRef.current = epoch;
      readIdentityFailedRef.current = false;
      return epoch;
    } catch (cause) {
      readIdentityFailedRef.current = true;
      fetchSnapshotsRef.current.clear();
      loadingRef.current = false;
      setLoading(false);
      const message =
        cause instanceof Error
          ? cause.message
          : "Table read identity could not be saved.";
      if (hasCommittedDataRef.current) setReadError(message);
      else setError(message);
      onReadFailedRef.current();
      return null;
    }
  }, []);

  const retryMetadataRefresh = useCallback(() => {
    if (connectionInvalidatedRef.current) return;
    const metadata = queuedMetadataRef.current;
    const { busy, hasWork } = metadataStateRef.current();
    if (!metadata) {
      if (
        metadataNeedsReadRef.current &&
        !busy &&
        !hasWork &&
        !loadingRef.current
      ) {
        metadataNeedsReadRef.current = false;
        setInitTick((tick) => tick + 1);
      }
      return;
    }
    const metadataColumnNames = new Set(
      metadata.columns.map((column) => column.name),
    );
    const retainedColumns = columnsRef.current.filter(
      (column) =>
        pageDiscoveredColumnNamesRef.current.has(column.name) &&
        !metadataColumnNames.has(column.name),
    );
    const nextColumns = [...metadata.columns, ...retainedColumns];
    const incompatible =
      JSON.stringify(
        hasCommittedDataRef.current
          ? pkColsRef.current
          : pendingPrimaryKeyColumnsRef.current,
      ) !== JSON.stringify(metadata.primaryKeyColumns) ||
      columnsRef.current.some((column) => {
        const next = nextColumns.find(
          (candidate) => candidate.name === column.name,
        );
        if (!next) return true;
        const writeShape = (value: ColumnMeta) =>
          JSON.stringify([
            value.type,
            value.nativeType,
            value.category,
            value.valueSemantics,
            value.nullable,
            value.identityGeneration,
            value.isReadOnly,
            value.primaryKeyRole,
            value.isComputed,
            value.computedExpression,
            value.generatedKind,
            value.bsonSubtype,
          ]);
        return writeShape(column) !== writeShape(next);
      });
    if (busy || (hasWork && incompatible)) {
      metadataBlockedRef.current = true;
      setSchemaWarning(
        busy
          ? "Schema metadata changed during an active operation or editor. Pending work and the displayed schema are retained until it finishes."
          : "Schema conflict: pending work is retained with the previous schema. Revert All manually to load the new schema before applying changes.",
      );
      return;
    }

    pasteStructureEpochRef.current++;
    queuedMetadataRef.current = null;
    metadataBlockedRef.current = false;
    setSchemaWarning(null);
    metadataNeedsReadRef.current = hasWork;
    columnsRef.current = nextColumns;
    pageDiscoveredColumnNamesRef.current = new Set(
      retainedColumns.map((column) => column.name),
    );
    pkColsRef.current = metadata.primaryKeyColumns;
    pendingPrimaryKeyColumnsRef.current = metadata.primaryKeyColumns;
    tableInitSignatureRef.current = buildTableInitSignature(
      metadata.columns,
      metadata.primaryKeyColumns,
    );
    setColumns(nextColumns);
    setPkCols(metadata.primaryKeyColumns);
    setColSizes((previous) => {
      const defaults = calcColWidths(nextColumns, rowsRef.current);
      return Object.fromEntries(
        nextColumns.map((column) => [
          column.name,
          previous[column.name] ?? defaults[column.name],
        ]),
      );
    });
    if (hasWork && deferredReadRef.current) {
      setInitTick((tick) => tick + 1);
    }
    if (!hasWork) {
      loadingRef.current = true;
      setLoading(true);
      setHasCommittedData(false);
      setRows([]);
      mongoIdTypesRef.current = [];
      rowValueTypesRef.current = [];
      setError(null);
      refreshRefetchRef.current = true;
      const nextSort =
        requestedSortRef.current &&
        nextColumns.some(
          (column) => column.name === requestedSortRef.current?.column,
        )
          ? requestedSortRef.current
          : null;
      requestedSortRef.current = nextSort;
      setRequestedSort(nextSort);
      setSort(nextSort);
      const nextFilters = buildActiveFilterDrafts(
        metadata.columns,
        debouncedFilterDraftsRef.current,
      );
      const clearedFilterColumns = retainedColumns
        .map((column) => column.name)
        .filter(
          (name) =>
            debouncedFilterDraftsRef.current[name] ||
            hasFilterDraftInput(filterDraftsRef.current[name]),
        );
      if (clearedFilterColumns.length > 0) {
        setFilterNotice(
          `Cleared the ${clearedFilterColumns.length === 1 ? "filter" : "filters"} on ${clearedFilterColumns.map((name) => JSON.stringify(name)).join(", ")}: fields found only in loaded documents cannot be filtered after a schema refresh.`,
        );
      }
      debouncedFilterDraftsRef.current = nextFilters;
      setFilterDrafts((drafts) =>
        buildActiveFilterDrafts(metadata.columns, drafts),
      );
      setDebouncedFilterDrafts(nextFilters);
      setInitTick((tick) => tick + 1);
    }
  }, [columnsRef, pkColsRef, rowsRef, mongoIdTypesRef, rowValueTypesRef]);

  const syncRequestedFilterState = useCallback(
    (nextDrafts: FilterDraftMap) => {
      readyFilterDraftsRef.current = null;
      setFilterError(null);
      setFilterDraftError(getVisibleFilterDraftError(nextDrafts));
      setFilterDraftPending(false);
      setRequestedPage(1);
      setDebouncedFilterDrafts(
        buildActiveFilterDrafts(columnsRef.current, nextDrafts),
      );
    },
    [columnsRef, getVisibleFilterDraftError],
  );

  const retryPendingFilter = useCallback(() => {
    const drafts = readyFilterDraftsRef.current;
    if (!drafts || loadingRef.current || filterNavigationBlockedRef.current())
      return;
    syncRequestedFilterState(drafts);
  }, [syncRequestedFilterState]);

  initialPageSizeRef.current = initialPageSize;
  requestedSortRef.current = requestedSort;
  hasCommittedDataRef.current = hasCommittedData;
  requestedPageRef.current = requestedPage;
  requestedPageSizeRef.current = requestedPageSize;
  debouncedFilterDraftsRef.current = debouncedFilterDrafts;
  filterDraftsRef.current = filterDrafts;
  readOnlyTableRef.current = readOnlyTable;
  onTableInitRef.current = onTableInit;
  onReadFailedRef.current = onReadFailed;
  onRowsCommittedRef.current = onRowsCommitted;
  rowsRef.current = rows;
  pkColsRef.current = pkCols;

  const fetchPage = useCallback(
    (options?: { refreshMetadata?: boolean }) => {
      if (!initializedRef.current) {
        return;
      }
      if (queuedMetadataRef.current || connectionInvalidatedRef.current) {
        deferredReadRef.current = true;
        return;
      }

      deferredReadRef.current = false;

      const epoch = advanceFetchEpoch();
      if (epoch === null) return;
      reconciliationFetchIdRef.current = metadataStateRef.current()
        .reconciliationPending
        ? epoch
        : null;
      const snapshot = getRequestedSnapshot();

      fetchSnapshotsRef.current.clear();
      fetchSnapshotsRef.current.set(epoch, snapshot);
      loadingRef.current = true;
      setLoading(true);
      setReadError(null);

      fetchHadFiltersRef.current = snapshot.filters.length > 0;

      postMessage("fetchPage", {
        fetchId: epoch,
        page: snapshot.page,
        pageSize: snapshot.pageSize,
        filters: snapshot.filters,
        sort: snapshot.sort,
        ...(options?.refreshMetadata ? { refreshMetadata: true } : {}),
      });
    },
    [advanceFetchEpoch, getRequestedSnapshot],
  );

  fetchPageRef.current = fetchPage;
  useEffect(() => {
    loadingRef.current = loading;
  });
  preserveScrollPositionRef.current = () => {
    scrollPreserveRef.current = scrollRef.current?.scrollTop ?? null;
  };

  useEffect(() => {
    setReadOnlyTable(initialReadOnlyTable);
    pendingReadOnlyTableRef.current = initialReadOnlyTable;
  }, [initialReadOnlyTable]);

  useEffect(() => {
    const clearErrors = (preserveFilterDraftError = false) => {
      setError(null);
      setReadError(null);
      setFilterError(null);
      if (!preserveFilterDraftError) {
        dismissedFilterDraftRevisionRef.current = null;
        setFilterDraftError(null);
      }
    };

    const unInit = onMessage<TableInitPayload>(
      "tableInit",
      ({
        columns: nextColumns,
        primaryKeyColumns,
        isView,
        connectionReadOnly,
        intent,
      }) => {
        const nextReadOnlyTable =
          isView !== undefined || connectionReadOnly !== undefined
            ? Boolean(isView) || Boolean(connectionReadOnly)
            : readOnlyTableRef.current;
        const nextInitSignature = buildTableInitSignature(
          nextColumns,
          primaryKeyColumns,
        );
        const isDuplicateInit =
          tableInitSignatureRef.current !== null &&
          tableInitSignatureRef.current === nextInitSignature;

        pendingReadOnlyTableRef.current = nextReadOnlyTable;

        const connectionRefresh = intent === "connectionRefresh";
        if (connectionRefresh) connectionInvalidatedRef.current = false;
        if (
          (intent === "metadataRefresh" || connectionRefresh) &&
          initializedRef.current
        ) {
          setReadOnlyTable(nextReadOnlyTable);
          if (
            !connectionRefresh &&
            isDuplicateInit &&
            !queuedMetadataRef.current
          )
            return;
          queuedMetadataRef.current = {
            columns: nextColumns,
            primaryKeyColumns,
          };
          metadataBlockedRef.current = true;
          if (connectionRefresh || fetchSnapshotsRef.current.size > 0)
            deferredReadRef.current = true;
          if (advanceFetchEpoch() === null) return;
          fetchSnapshotsRef.current.clear();
          loadingRef.current = false;
          setLoading(false);
          setMetadataTick((tick) => tick + 1);
          return;
        }

        columnsRef.current = nextColumns;
        pageDiscoveredColumnNamesRef.current = new Set();
        connectionInvalidatedRef.current = false;
        pendingPrimaryKeyColumnsRef.current = primaryKeyColumns;

        if (isDuplicateInit) {
          setReadOnlyTable(nextReadOnlyTable);
          return;
        }

        tableInitSignatureRef.current = nextInitSignature;
        queuedMetadataRef.current = null;
        metadataNeedsReadRef.current = false;
        refreshRefetchRef.current = false;
        setFilterNotice(null);
        deferredReadRef.current = false;
        reconciliationFetchIdRef.current = null;
        metadataBlockedRef.current = false;
        setSchemaWarning(null);

        initializedRef.current = true;
        setIsInitialized(true);
        if (advanceFetchEpoch() === null) return;
        fetchSnapshotsRef.current.clear();
        scrollPreserveRef.current = null;
        colSizesInitedRef.current = false;

        loadingRef.current = true;
        setLoading(true);
        setHasCommittedData(false);
        setColumns([]);
        setPkCols([]);
        setRows([]);
        mongoIdTypesRef.current = [];
        rowValueTypesRef.current = [];
        committedColumnNamesRef.current = new Set();
        setTotalCount(0);
        clearErrors();
        const restoredView = preserveRestoredViewRef.current
          ? initialView
          : null;
        preserveRestoredViewRef.current = false;
        const nextPage = restoredView?.page ?? 1;
        const nextPageSize =
          restoredView?.pageSize ?? initialPageSizeRef.current;
        const nextSort =
          restoredView?.sort &&
          nextColumns.some(
            (column) => column.name === restoredView.sort?.column,
          )
            ? restoredView.sort
            : null;
        const nextFilters = Object.fromEntries(
          Object.entries(restoredView?.filters ?? {}).filter(
            ([name, draft]) =>
              draft &&
              nextColumns.some(
                (column) =>
                  column.name === name &&
                  column.filterOperators.includes(draft.operator) &&
                  (column.filterable ||
                    draft.operator === "is_null" ||
                    draft.operator === "is_not_null"),
              ),
          ),
        );
        const nextActiveFilters = buildActiveFilterDrafts(
          nextColumns,
          nextFilters,
        );
        requestedPageRef.current = nextPage;
        requestedPageSizeRef.current = nextPageSize;
        requestedSortRef.current = nextSort;
        debouncedFilterDraftsRef.current = nextActiveFilters;
        previousFilterDraftsRef.current = nextFilters;
        readyFilterDraftsRef.current = null;
        setFilterDraftError(getVisibleFilterDraftError(nextFilters));
        setPage(nextPage);
        setPageSize(nextPageSize);
        setRequestedPage(nextPage);
        setRequestedPageSize(nextPageSize);
        setReadOnlyTable(nextReadOnlyTable);
        setSort(nextSort);
        setRequestedSort(nextSort);
        setFilterDrafts(nextFilters);
        setFilterDraftPending(false);
        setDebouncedFilterDrafts(nextActiveFilters);
        setColSizes({});

        onTableInitRef.current();
        setInitTick((tick) => tick + 1);
      },
    );

    const unConnectionInvalidated = onMessage(
      "tableConnectionInvalidated",
      () => {
        connectionInvalidatedRef.current = true;
        queuedMetadataRef.current = null;
        metadataNeedsReadRef.current = false;
        metadataBlockedRef.current = true;
        deferredReadRef.current = true;
        if (advanceFetchEpoch() === null) return;
        fetchSnapshotsRef.current.clear();
        reconciliationFetchIdRef.current = null;
        loadingRef.current = false;
        setLoading(false);
        setSchemaWarning(
          "Connection changed. Waiting for metadata from the new session.",
        );
      },
    );

    const unData = onMessage<{
      fetchId?: number;
      extraColumns?: ColumnMeta[];
      rows: Row[];
      mongoIdTypes?: Array<"objectId" | "string" | null>;
      mongoValueTypes?: Array<Record<string, MongoValueTypeHint>>;
      rowValueTypes?: Array<Record<string, MongoValueTypeHint>>;
      totalCount: number;
      executionTimeMs?: number;
    }>(
      "tableData",
      ({
        fetchId,
        extraColumns,
        rows: receivedRows,
        mongoIdTypes,
        mongoValueTypes,
        rowValueTypes,
        totalCount: nextTotalCount,
        executionTimeMs: nextExecutionTimeMs,
      }) => {
        if (
          connectionInvalidatedRef.current ||
          queuedMetadataRef.current ||
          readIdentityFailedRef.current
        )
          return;
        if (
          fetchId === undefined
            ? reconciliationFetchIdRef.current !== null
            : fetchId !== fetchEpochRef.current ||
              (fetchId === reconciliationFetchIdRef.current &&
                !fetchSnapshotsRef.current.has(fetchId))
        ) {
          return;
        }

        const nextRowValueTypes = Array.isArray(rowValueTypes)
          ? rowValueTypes
          : Array.isArray(mongoValueTypes)
            ? mongoValueTypes
            : [];
        const nextRows = receivedRows.map((row, index) => {
          const hints = nextRowValueTypes[index];
          const missingUndefined = hints
            ? Object.keys(hints).filter(
                (name) =>
                  hints[name]?.nativeType === "undefined" &&
                  !Object.hasOwn(row, name),
              )
            : [];
          if (missingUndefined.length === 0) return row;
          const copy = { ...row };
          for (const name of missingUndefined) {
            Object.defineProperty(copy, name, {
              value: undefined,
              enumerable: true,
              writable: true,
              configurable: true,
            });
          }
          return copy;
        });

        const snapshot =
          (fetchId !== undefined
            ? fetchSnapshotsRef.current.get(fetchId)
            : fetchSnapshotsRef.current.get(fetchEpochRef.current)) ??
          getRequestedSnapshot();

        const knownColumnNames = new Set(
          columnsRef.current.map((column) => column.name),
        );
        const addedColumns = (extraColumns ?? []).filter(
          (column) => !knownColumnNames.has(column.name),
        );
        if (addedColumns.length > 0) {
          columnsRef.current = [...columnsRef.current, ...addedColumns];
          pageDiscoveredColumnNamesRef.current = new Set([
            ...pageDiscoveredColumnNamesRef.current,
            ...addedColumns.map((column) => column.name),
          ]);
        }

        if (!colSizesInitedRef.current && columnsRef.current.length > 0) {
          colSizesInitedRef.current = true;
          setColSizes(
            calcColWidths(
              columnsRef.current.map(
                (column): WidthColumn => ({
                  name: column.name,
                  isPrimaryKey: column.isPrimaryKey,
                  primaryKeyRole: column.primaryKeyRole,
                  isForeignKey: column.isForeignKey,
                }),
              ),
              nextRows,
            ),
          );
        }

        setColumns(columnsRef.current);
        metadataNeedsReadRef.current = false;
        refreshRefetchRef.current = false;
        committedColumnNamesRef.current = new Set(
          columnsRef.current.map((column) => column.name),
        );
        setPkCols(pendingPrimaryKeyColumnsRef.current);
        setReadOnlyTable(pendingReadOnlyTableRef.current);
        const previousMongoIdTypes = mongoIdTypesRef.current;
        const previousRowValueTypes = rowValueTypesRef.current;
        const nextMongoIdTypes = Array.isArray(mongoIdTypes)
          ? mongoIdTypes
          : [];
        pasteStructureEpochRef.current++;
        setRows(nextRows);
        mongoIdTypesRef.current = nextMongoIdTypes;
        rowValueTypesRef.current = nextRowValueTypes;
        setTotalCount(nextTotalCount);
        setExecutionTimeMs(nextExecutionTimeMs);
        setPage(snapshot.page);
        setPageSize(snapshot.pageSize);
        setSort(snapshot.sort);
        setCommittedFilters(snapshot.filters);
        loadingRef.current = false;
        setLoading(false);
        setHasCommittedData(true);
        clearErrors(true);

        onRowsCommittedRef.current(
          nextRows,
          pendingPrimaryKeyColumnsRef.current,
          previousMongoIdTypes,
          nextMongoIdTypes,
          previousRowValueTypes,
          nextRowValueTypes,
        );
        fetchSnapshotsRef.current.delete(fetchId ?? fetchEpochRef.current);

        const savedScroll = scrollPreserveRef.current;
        scrollPreserveRef.current = null;
        if (savedScroll !== null && savedScroll > 0) {
          requestAnimationFrame(() => {
            scrollRef.current?.scrollTo?.({ top: savedScroll });
          });
        } else {
          scrollRef.current?.scrollTo?.({ top: 0 });
        }
      },
    );

    const unError = onMessage<{
      fetchId?: number;
      error: string;
      isFilterError?: boolean;
    }>("tableError", ({ fetchId, error: nextError, isFilterError }) => {
      if (queuedMetadataRef.current || readIdentityFailedRef.current) return;
      if (
        fetchId === undefined
          ? reconciliationFetchIdRef.current !== null
          : fetchId !== fetchEpochRef.current ||
            (fetchId === reconciliationFetchIdRef.current &&
              !fetchSnapshotsRef.current.has(fetchId))
      ) {
        return;
      }

      const keepFilterControls =
        !hasCommittedDataRef.current &&
        refreshRefetchRef.current &&
        fetchHadFiltersRef.current;
      if (isFilterError || keepFilterControls) {
        setError(null);
        if (isFilterError) setFilterError(nextError);
        else setReadError(nextError);
        if (!hasCommittedDataRef.current) {
          setColumns(columnsRef.current);
          setPkCols(pendingPrimaryKeyColumnsRef.current);
          setReadOnlyTable(true);
          setHasCommittedData(true);
        }
        const { busy, hasWork } = metadataStateRef.current();
        if (!hasCommittedDataRef.current || (!busy && !hasWork)) {
          setCommittedFilters(
            (
              fetchSnapshotsRef.current.get(fetchId ?? fetchEpochRef.current) ??
              getRequestedSnapshot()
            ).filters,
          );
          pasteStructureEpochRef.current++;
          setRows([]);
          setTotalCount(0);
          setExecutionTimeMs(undefined);
          if (requestedPageRef.current !== 1) {
            skipNextFetchRef.current = true;
            requestedPageRef.current = 1;
            setRequestedPage(1);
          }
          setPage(1);
        }
      } else if (!hasCommittedDataRef.current) {
        setError(nextError);
      } else {
        setReadError(nextError);
      }

      loadingRef.current = false;
      setLoading(false);
      onReadFailedRef.current();
      fetchSnapshotsRef.current.delete(fetchId ?? fetchEpochRef.current);
    });

    postMessage("ready");
    return () => {
      unInit();
      unConnectionInvalidated();
      unData();
      unError();
    };
  }, [
    advanceFetchEpoch,
    columnsRef,
    getRequestedSnapshot,
    initialView,
    mongoIdTypesRef,
    rowValueTypesRef,
    scrollRef,
  ]);

  const fetchTrigger = useMemo(
    () =>
      JSON.stringify({
        initTick,
        page: requestedPage,
        pageSize: requestedPageSize,
        filters: debouncedFilterDrafts,
        sortColumn: requestedSort?.column ?? null,
        sortDirection: requestedSort?.direction ?? null,
      }),
    [
      debouncedFilterDrafts,
      initTick,
      requestedPage,
      requestedPageSize,
      requestedSort,
    ],
  );

  useEffect(() => {
    if (!initializedRef.current) {
      return;
    }

    if (skipNextFetchRef.current) {
      skipNextFetchRef.current = false;
      return;
    }

    fetchPage();
  }, [fetchPage, fetchTrigger]);

  useEffect(() => {
    if (
      loading ||
      !hasCommittedData ||
      rows.length > 0 ||
      page <= 1 ||
      requestedPage !== page ||
      requestedPageSize !== pageSize
    )
      return;
    const lastPage = Math.max(1, Math.ceil(totalCount / pageSize));
    if (page <= lastPage || filterNavigationBlockedRef.current()) return;
    setRequestedPage((current) => (current === page ? lastPage : current));
  }, [
    hasCommittedData,
    loading,
    page,
    pageSize,
    requestedPage,
    requestedPageSize,
    rows,
    totalCount,
  ]);

  useEffect(() => {
    if (previousFilterDraftsRef.current === filterDrafts) {
      return;
    }
    previousFilterDraftsRef.current = filterDrafts;
    readyFilterDraftsRef.current = null;

    const timeoutId = setTimeout(() => {
      readyFilterDraftsRef.current = filterDrafts;
      retryPendingFilter();
    }, DEBOUNCE);

    return () => clearTimeout(timeoutId);
  }, [filterDrafts, retryPendingFilter]);

  const clearFilterError = useCallback(() => {
    setFilterError(null);
    setFilterNotice(null);
    if (!filterError && filterDraftError) {
      dismissedFilterDraftRevisionRef.current =
        visibleFilterDraftRevisionRef.current;
    }
    setFilterDraftError(null);
  }, [filterError, filterDraftError]);

  const handleSort = useCallback((column: string) => {
    setRequestedPage(1);
    setRequestedSort((previousSort) => {
      if (previousSort?.column !== column) {
        return { column, direction: "asc" };
      }

      return previousSort.direction === "asc"
        ? { column, direction: "desc" }
        : null;
    });
  }, []);

  const updateFilterDraft = useCallback(
    (
      columnName: string,
      nextDraft: FilterDraft | undefined,
      options?: { applyImmediately?: boolean },
    ) => {
      setFilterNotice(null);
      setFilterDrafts((currentDrafts) => {
        let nextDrafts = currentDrafts;

        if (!nextDraft) {
          if (currentDrafts[columnName] === undefined) {
            return currentDrafts;
          }

          nextDrafts = { ...currentDrafts };
          delete nextDrafts[columnName];
        } else {
          nextDrafts = {
            ...currentDrafts,
            [columnName]: nextDraft,
          };
        }

        if (nextDrafts !== currentDrafts) {
          filterDraftRevisionRef.current += 1;
        }

        setFilterDraftPending(true);
        if (options?.applyImmediately) {
          previousFilterDraftsRef.current = nextDrafts;
          syncRequestedFilterState(nextDrafts);
        }

        return nextDrafts;
      });
    },
    [syncRequestedFilterState],
  );

  return {
    committedColumnNamesRef,
    metadataBlockedRef,
    metadataTick,
    retryMetadataRefresh,
    retryPendingFilter,
    schemaWarning,
    loadingRef,
    fetchEpochRef,
    columns,
    colSizes,
    committedFilters,
    error,
    executionTimeMs,
    fetchPage,
    filterDrafts,
    filterError: filterError ?? filterDraftError ?? filterNotice,
    hasCommittedData,
    isInitialized,
    intendedPage,
    loading,
    page,
    pageSize,
    pkCols,
    readError,
    readOnlyTable,
    requestedPage,
    requestedPageSize,
    requestedSort,
    rows,
    sort,
    totalCount,
    handleSort,
    setFilterError,
    clearFilterError,
    setReadError,
    setRequestedPage,
    setRequestedPageSize,
    updateFilterDraft,
  };
}
