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
  type TableSortState,
} from "./tableViewHelpers";

interface UseTableDataControllerParams {
  initialPageSize: number;
  readOnlyTable: boolean;
  columnsRef: MutableRefObject<ColumnMeta[]>;
  rowsRef: MutableRefObject<Row[]>;
  mongoIdTypesRef: MutableRefObject<Array<"objectId" | "string" | null>>;
  pkColsRef: MutableRefObject<string[]>;
  scrollRef: RefObject<HTMLDivElement | null>;
  fetchPageRef: MutableRefObject<() => void>;
  preserveScrollPositionRef: MutableRefObject<() => void>;
  onTableInit: () => void;
  getMetadataRefreshState: () => {
    busy: boolean;
    hasWork: boolean;
    reconciliationPending?: boolean;
  };
  onReadFailed: () => void;
  onRowsCommitted: (
    rows: readonly Row[],
    primaryKeyColumns: readonly string[],
    previousMongoIdTypes: readonly ("objectId" | "string" | null)[],
    mongoIdTypes: readonly ("objectId" | "string" | null)[],
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

export function useTableDataController({
  initialPageSize,
  readOnlyTable: initialReadOnlyTable,
  columnsRef,
  rowsRef,
  mongoIdTypesRef,
  pkColsRef,
  scrollRef,
  fetchPageRef,
  preserveScrollPositionRef,
  onTableInit,
  getMetadataRefreshState,
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
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialPageSize);
  const [requestedPage, setRequestedPage] = useState(1);
  const [requestedPageSize, setRequestedPageSize] = useState(initialPageSize);
  const [filterDrafts, setFilterDrafts] = useState<FilterDraftMap>({});
  const [debouncedFilterDrafts, setDebouncedFilterDrafts] =
    useState<FilterDraftMap>({});
  const [sort, setSort] = useState<TableSortState>(null);
  const [requestedSort, setRequestedSort] = useState<TableSortState>(null);
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
  const debouncedFilterDraftsRef = useRef(debouncedFilterDrafts);
  const initializedRef = useRef(false);
  const fetchEpochRef = useRef(0);
  const filtersMountedRef = useRef(false);
  const [initTick, setInitTick] = useState(0);
  const readOnlyTableRef = useRef(initialReadOnlyTable);
  const initialPageSizeRef = useRef(initialPageSize);
  const onTableInitRef = useRef(onTableInit);
  const onReadFailedRef = useRef(onReadFailed);
  const onRowsCommittedRef = useRef(onRowsCommitted);
  const tableInitSignatureRef = useRef<string | null>(null);
  const metadataStateRef = useRef(getMetadataRefreshState);
  metadataStateRef.current = getMetadataRefreshState;
  const queuedMetadataRef = useRef<TableInitPayload | null>(null);
  const [metadataTick, setMetadataTick] = useState(0);
  const [schemaWarning, setSchemaWarning] = useState<string | null>(null);
  const metadataBlockedRef = useRef(false);
  const committedColumnNamesRef = useRef<ReadonlySet<string>>(new Set());
  const metadataNeedsReadRef = useRef(false);
  // A reconciliation read must survive a schema queue (or invalidation of
  // an already-issued read), even while edits/drafts/history are retained.
  const deferredReadRef = useRef(false);
  // Delete reconciliation reads have terminal outcomes: a late response after
  // failure must not unlock writes or replace the retained work before retry.
  const reconciliationFetchIdRef = useRef<number | null>(null);

  const retryMetadataRefresh = useCallback(() => {
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
    const incompatible =
      JSON.stringify(pkColsRef.current) !==
        JSON.stringify(metadata.primaryKeyColumns) ||
      columnsRef.current.some((column) => {
        const next = metadata.columns.find(
          (candidate) => candidate.name === column.name,
        );
        if (!next) return true;
        // Filter/header-only metadata can change without invalidating write values.
        const writeShape = (value: ColumnMeta) =>
          JSON.stringify([
            value.type,
            value.nativeType,
            value.category,
            value.valueSemantics,
            value.nullable,
            value.identityGeneration,
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

    queuedMetadataRef.current = null;
    metadataBlockedRef.current = false;
    setSchemaWarning(null);
    metadataNeedsReadRef.current = hasWork;
    columnsRef.current = metadata.columns;
    pkColsRef.current = metadata.primaryKeyColumns;
    pendingPrimaryKeyColumnsRef.current = metadata.primaryKeyColumns;
    tableInitSignatureRef.current = buildTableInitSignature(
      metadata.columns,
      metadata.primaryKeyColumns,
    );
    setColumns(metadata.columns);
    setPkCols(metadata.primaryKeyColumns);
    setColSizes((previous) => {
      const defaults = calcColWidths(metadata.columns, rowsRef.current);
      return Object.fromEntries(
        metadata.columns.map((column) => [
          column.name,
          previous[column.name] ?? defaults[column.name],
        ]),
      );
    });
    // Ordinary metadata refresh retains the committed dataset while work exists.
    // A requested mutation reconciliation instead rebases that work by identity.
    if (hasWork && deferredReadRef.current) {
      setInitTick((tick) => tick + 1);
    }
    if (!hasWork) {
      // No local work remains: require an authoritative dataset before exposing
      // the new schema for editing (especially after PK/type changes or read errors).
      loadingRef.current = true;
      setLoading(true);
      setHasCommittedData(false);
      setRows([]);
      mongoIdTypesRef.current = [];
      setError(null);
      const nextSort =
        requestedSortRef.current &&
        metadata.columns.some(
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
      debouncedFilterDraftsRef.current = nextFilters;
      setFilterDrafts((drafts) =>
        buildActiveFilterDrafts(metadata.columns, drafts),
      );
      setDebouncedFilterDrafts(nextFilters);
      setInitTick((tick) => tick + 1);
    }
  }, [columnsRef, pkColsRef, rowsRef, mongoIdTypesRef]);

  const syncRequestedFilterState = useCallback(
    (nextDrafts: FilterDraftMap) => {
      setFilterError(null);
      setRequestedPage(1);
      setDebouncedFilterDrafts(
        buildActiveFilterDrafts(columnsRef.current, nextDrafts),
      );
    },
    [columnsRef],
  );

  initialPageSizeRef.current = initialPageSize;
  requestedSortRef.current = requestedSort;
  hasCommittedDataRef.current = hasCommittedData;
  requestedPageRef.current = requestedPage;
  requestedPageSizeRef.current = requestedPageSize;
  debouncedFilterDraftsRef.current = debouncedFilterDrafts;
  readOnlyTableRef.current = readOnlyTable;
  onTableInitRef.current = onTableInit;
  onReadFailedRef.current = onReadFailed;
  onRowsCommittedRef.current = onRowsCommitted;
  rowsRef.current = rows;
  pkColsRef.current = pkCols;

  const fetchPage = useCallback(() => {
    if (!initializedRef.current) {
      return;
    }
    if (queuedMetadataRef.current) {
      deferredReadRef.current = true;
      return;
    }

    deferredReadRef.current = false;

    const epoch = ++fetchEpochRef.current;
    reconciliationFetchIdRef.current = metadataStateRef.current()
      .reconciliationPending
      ? epoch
      : null;
    const snapshot: FetchSnapshot = {
      page: requestedPageRef.current,
      pageSize: requestedPageSizeRef.current,
      sort: requestedSortRef.current,
    };

    fetchSnapshotsRef.current.clear();
    fetchSnapshotsRef.current.set(epoch, snapshot);
    loadingRef.current = true;
    setLoading(true);
    setReadError(null);

    const activeFilters = serializeFilterDrafts(
      columnsRef.current,
      debouncedFilterDraftsRef.current,
    );

    postMessage("fetchPage", {
      fetchId: epoch,
      page: snapshot.page,
      pageSize: snapshot.pageSize,
      filters: activeFilters,
      sort: snapshot.sort,
    });
  }, [columnsRef]);

  fetchPageRef.current = fetchPage;
  // Keep callbacks blocked until the committed rows and restored edits render.
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
    const clearErrors = () => {
      setError(null);
      setReadError(null);
      setFilterError(null);
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

        if (intent === "metadataRefresh" && initializedRef.current) {
          setReadOnlyTable(nextReadOnlyTable);
          if (isDuplicateInit && !queuedMetadataRef.current) return;
          queuedMetadataRef.current = {
            columns: nextColumns,
            primaryKeyColumns,
          };
          metadataBlockedRef.current = true;
          // Invalidate reads issued under the old metadata, without touching mutations.
          if (fetchSnapshotsRef.current.size > 0)
            deferredReadRef.current = true;
          fetchEpochRef.current += 1;
          fetchSnapshotsRef.current.clear();
          loadingRef.current = false;
          setLoading(false);
          setMetadataTick((tick) => tick + 1);
          return;
        }

        columnsRef.current = nextColumns;
        pendingPrimaryKeyColumnsRef.current = primaryKeyColumns;

        if (isDuplicateInit) {
          setReadOnlyTable(nextReadOnlyTable);
          return;
        }

        tableInitSignatureRef.current = nextInitSignature;
        queuedMetadataRef.current = null;
        metadataNeedsReadRef.current = false;
        deferredReadRef.current = false;
        reconciliationFetchIdRef.current = null;
        metadataBlockedRef.current = false;
        setSchemaWarning(null);

        initializedRef.current = true;
        setIsInitialized(true);
        fetchEpochRef.current += 1;
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
        committedColumnNamesRef.current = new Set();
        setTotalCount(0);
        clearErrors();
        setPage(1);
        setPageSize(initialPageSizeRef.current);
        setRequestedPage(1);
        setRequestedPageSize(initialPageSizeRef.current);
        setReadOnlyTable(nextReadOnlyTable);
        setSort(null);
        setRequestedSort(null);
        setFilterDrafts({});
        setDebouncedFilterDrafts({});
        setColSizes({});

        onTableInitRef.current();
        setInitTick((tick) => tick + 1);
      },
    );

    const unData = onMessage<{
      fetchId?: number;
      rows: Row[];
      mongoIdTypes?: Array<"objectId" | "string" | null>;
      totalCount: number;
      executionTimeMs?: number;
    }>(
      "tableData",
      ({
        fetchId,
        rows: nextRows,
        mongoIdTypes,
        totalCount: nextTotalCount,
        executionTimeMs: nextExecutionTimeMs,
      }) => {
        if (queuedMetadataRef.current) return;
        if (
          fetchId !== undefined &&
          (fetchId !== fetchEpochRef.current ||
            (fetchId === reconciliationFetchIdRef.current &&
              !fetchSnapshotsRef.current.has(fetchId)))
        ) {
          return;
        }

        const snapshot = (fetchId !== undefined
          ? fetchSnapshotsRef.current.get(fetchId)
          : fetchSnapshotsRef.current.get(fetchEpochRef.current)) ?? {
          page: requestedPageRef.current,
          pageSize: requestedPageSizeRef.current,
          sort: requestedSortRef.current,
        };

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
        committedColumnNamesRef.current = new Set(
          columnsRef.current.map((column) => column.name),
        );
        setPkCols(pendingPrimaryKeyColumnsRef.current);
        setReadOnlyTable(pendingReadOnlyTableRef.current);
        const previousMongoIdTypes = mongoIdTypesRef.current;
        const nextMongoIdTypes = Array.isArray(mongoIdTypes)
          ? mongoIdTypes
          : [];
        setRows(nextRows);
        mongoIdTypesRef.current = nextMongoIdTypes;
        setTotalCount(nextTotalCount);
        setExecutionTimeMs(nextExecutionTimeMs);
        setPage(snapshot.page);
        setPageSize(snapshot.pageSize);
        setSort(snapshot.sort);
        loadingRef.current = false;
        setLoading(false);
        setHasCommittedData(true);
        clearErrors();

        onRowsCommittedRef.current(
          nextRows,
          pendingPrimaryKeyColumnsRef.current,
          previousMongoIdTypes,
          nextMongoIdTypes,
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
      if (queuedMetadataRef.current) return;
      if (
        fetchId !== undefined &&
        (fetchId !== fetchEpochRef.current ||
          (fetchId === reconciliationFetchIdRef.current &&
            !fetchSnapshotsRef.current.has(fetchId)))
      ) {
        return;
      }

      if (!hasCommittedDataRef.current) {
        setError(nextError);
      } else if (isFilterError) {
        setFilterError(nextError);
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
      unData();
      unError();
    };
  }, [columnsRef, mongoIdTypesRef, scrollRef]);

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

  // biome-ignore lint/correctness/useExhaustiveDependencies: fetchTrigger is a synthetic trigger that encodes page/sort/filter state; fetchPage reads those values from stable refs at call time.
  useEffect(() => {
    if (!initializedRef.current) {
      return;
    }

    fetchPage();
  }, [fetchPage, fetchTrigger]);

  useEffect(() => {
    if (!filtersMountedRef.current) {
      filtersMountedRef.current = true;
      return;
    }

    const timeoutId = setTimeout(() => {
      setFilterError(null);
      setRequestedPage(1);
      setDebouncedFilterDrafts(
        buildActiveFilterDrafts(columnsRef.current, filterDrafts),
      );
    }, DEBOUNCE);

    return () => clearTimeout(timeoutId);
  }, [columnsRef, filterDrafts]);

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

        if (options?.applyImmediately) {
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
    schemaWarning,
    loadingRef,
    fetchEpochRef,
    columns,
    colSizes,
    debouncedFilterDrafts,
    error,
    executionTimeMs,
    fetchPage,
    filterDrafts,
    filterError,
    hasCommittedData,
    isInitialized,
    loading,
    page,
    pageSize,
    pkCols,
    readError,
    readOnlyTable,
    requestedPage,
    requestedPageSize,
    rows,
    sort,
    totalCount,
    handleSort,
    setFilterError,
    setReadError,
    setRequestedPage,
    setRequestedPageSize,
    updateFilterDraft,
  };
}
