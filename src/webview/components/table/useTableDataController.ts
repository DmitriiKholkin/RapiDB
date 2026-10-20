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
  type PersistedTableViewState,
  type TableSortState,
} from "./tableViewHelpers";

interface UseTableDataControllerParams {
  initialPageSize: number;
  initialView?: PersistedTableViewState | null;
  readOnlyTable: boolean;
  columnsRef: MutableRefObject<ColumnMeta[]>;
  rowsRef: MutableRefObject<Row[]>;
  mongoIdTypesRef: MutableRefObject<Array<"objectId" | "string" | null>>;
  mongoValueTypesRef: MutableRefObject<
    Array<Record<string, MongoValueTypeHint>>
  >;
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
  initialPageSize,
  initialView,
  readOnlyTable: initialReadOnlyTable,
  columnsRef,
  rowsRef,
  mongoIdTypesRef,
  mongoValueTypesRef,
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
  const queuedMetadataRef = useRef<TableInitPayload | null>(null);
  const [metadataTick, setMetadataTick] = useState(0);
  const [schemaWarning, setSchemaWarning] = useState<string | null>(null);
  const metadataBlockedRef = useRef(false);
  const committedColumnNamesRef = useRef<ReadonlySet<string>>(new Set());
  const metadataNeedsReadRef = useRef(false);
  const connectionInvalidatedRef = useRef(false);
  const deferredReadRef = useRef(false);
  const reconciliationFetchIdRef = useRef<number | null>(null);

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
    const incompatible =
      JSON.stringify(
        hasCommittedDataRef.current
          ? pkColsRef.current
          : pendingPrimaryKeyColumnsRef.current,
      ) !== JSON.stringify(metadata.primaryKeyColumns) ||
      columnsRef.current.some((column) => {
        const next = metadata.columns.find(
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
    if (hasWork && deferredReadRef.current) {
      setInitTick((tick) => tick + 1);
    }
    if (!hasWork) {
      loadingRef.current = true;
      setLoading(true);
      setHasCommittedData(false);
      setRows([]);
      mongoIdTypesRef.current = [];
      mongoValueTypesRef.current = [];
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
  }, [columnsRef, pkColsRef, rowsRef, mongoIdTypesRef, mongoValueTypesRef]);

  const syncRequestedFilterState = useCallback(
    (nextDrafts: FilterDraftMap) => {
      setFilterError(null);
      setFilterDraftPending(false);
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
        ...(options?.refreshMetadata ? { refreshMetadata: true } : {}),
      });
    },
    [advanceFetchEpoch, columnsRef],
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
        connectionInvalidatedRef.current = false;
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
        mongoValueTypesRef.current = [];
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
      rows: Row[];
      mongoIdTypes?: Array<"objectId" | "string" | null>;
      mongoValueTypes?: Array<Record<string, MongoValueTypeHint>>;
      totalCount: number;
      executionTimeMs?: number;
    }>(
      "tableData",
      ({
        fetchId,
        rows: nextRows,
        mongoIdTypes,
        mongoValueTypes,
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
        mongoValueTypesRef.current = Array.isArray(mongoValueTypes)
          ? mongoValueTypes
          : [];
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

      if (isFilterError) {
        setError(null);
        setFilterError(nextError);
        if (!hasCommittedDataRef.current) {
          setColumns(columnsRef.current);
          setPkCols(pendingPrimaryKeyColumnsRef.current);
          setReadOnlyTable(true);
          setHasCommittedData(true);
        }
        setRows([]);
        setTotalCount(0);
        setExecutionTimeMs(undefined);
        if (requestedPageRef.current !== 1) {
          skipNextFetchRef.current = true;
          requestedPageRef.current = 1;
          setRequestedPage(1);
        }
        setPage(1);
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
    initialView,
    mongoIdTypesRef,
    mongoValueTypesRef,
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
    if (previousFilterDraftsRef.current === filterDrafts) {
      return;
    }
    previousFilterDraftsRef.current = filterDrafts;

    const timeoutId = setTimeout(() => {
      setFilterError(null);
      setFilterDraftPending(false);
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

        setFilterDraftPending(true);
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
    setReadError,
    setRequestedPage,
    setRequestedPageSize,
    updateFilterDraft,
  };
}
