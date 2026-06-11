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
  pkColsRef: MutableRefObject<string[]>;
  scrollRef: RefObject<HTMLDivElement | null>;
  fetchPageRef: MutableRefObject<() => void>;
  preserveScrollPositionRef: MutableRefObject<() => void>;
  onTableInit: () => void;
  onRowsCommitted: (
    rows: readonly Row[],
    primaryKeyColumns: readonly string[],
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
    })),
    primaryKeyColumns,
  });
}

export function useTableDataController({
  initialPageSize,
  readOnlyTable: initialReadOnlyTable,
  columnsRef,
  rowsRef,
  pkColsRef,
  scrollRef,
  fetchPageRef,
  preserveScrollPositionRef,
  onTableInit,
  onRowsCommitted,
}: UseTableDataControllerParams) {
  const [columns, setColumns] = useState<ColumnMeta[]>([]);
  const [pkCols, setPkCols] = useState<string[]>([]);
  const [rows, setRows] = useState<Row[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
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
  const onRowsCommittedRef = useRef(onRowsCommitted);
  const tableInitSignatureRef = useRef<string | null>(null);

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
  onRowsCommittedRef.current = onRowsCommitted;
  rowsRef.current = rows;
  pkColsRef.current = pkCols;

  const fetchPage = useCallback(() => {
    if (!initializedRef.current) {
      return;
    }

    const epoch = ++fetchEpochRef.current;
    const snapshot: FetchSnapshot = {
      page: requestedPageRef.current,
      pageSize: requestedPageSizeRef.current,
      sort: requestedSortRef.current,
    };

    fetchSnapshotsRef.current.clear();
    fetchSnapshotsRef.current.set(epoch, snapshot);
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

    const unInit = onMessage<{
      columns: ColumnMeta[];
      primaryKeyColumns: string[];
      isView?: boolean;
      connectionReadOnly?: boolean;
    }>(
      "tableInit",
      ({
        columns: nextColumns,
        primaryKeyColumns,
        isView,
        connectionReadOnly,
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

        columnsRef.current = nextColumns;
        pendingPrimaryKeyColumnsRef.current = primaryKeyColumns;
        pendingReadOnlyTableRef.current = nextReadOnlyTable;

        if (isDuplicateInit) {
          setReadOnlyTable(nextReadOnlyTable);
          return;
        }

        tableInitSignatureRef.current = nextInitSignature;

        initializedRef.current = true;
        setIsInitialized(true);
        fetchEpochRef.current += 1;
        fetchSnapshotsRef.current.clear();
        scrollPreserveRef.current = null;
        colSizesInitedRef.current = false;

        setLoading(true);
        setHasCommittedData(false);
        setColumns([]);
        setPkCols([]);
        setRows([]);
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
      totalCount: number;
      executionTimeMs?: number;
    }>(
      "tableData",
      ({
        fetchId,
        rows: nextRows,
        totalCount: nextTotalCount,
        executionTimeMs: nextExecutionTimeMs,
      }) => {
        if (fetchId !== undefined && fetchId !== fetchEpochRef.current) {
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
        setPkCols(pendingPrimaryKeyColumnsRef.current);
        setReadOnlyTable(pendingReadOnlyTableRef.current);
        setRows(nextRows);
        setTotalCount(nextTotalCount);
        setExecutionTimeMs(nextExecutionTimeMs);
        setPage(snapshot.page);
        setPageSize(snapshot.pageSize);
        setSort(snapshot.sort);
        setLoading(false);
        setHasCommittedData(true);
        clearErrors();

        onRowsCommittedRef.current(
          nextRows,
          pendingPrimaryKeyColumnsRef.current,
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
      if (fetchId !== undefined && fetchId !== fetchEpochRef.current) {
        return;
      }

      if (!hasCommittedDataRef.current) {
        setError(nextError);
      } else if (isFilterError) {
        setFilterError(nextError);
      } else {
        setReadError(nextError);
      }

      setLoading(false);
      fetchSnapshotsRef.current.delete(fetchId ?? fetchEpochRef.current);
    });

    postMessage("ready");
    return () => {
      unInit();
      unData();
      unError();
    };
  }, [columnsRef, scrollRef]);

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
