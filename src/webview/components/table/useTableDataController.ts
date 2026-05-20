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
  isView: boolean;
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

export function useTableDataController({
  initialPageSize,
  isView,
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
  const [readOnlyTable, setReadOnlyTable] = useState(isView);
  const [colSizes, setColSizes] = useState<Record<string, number>>({});
  const [isInitialized, setIsInitialized] = useState(false);

  const colSizesInitedRef = useRef(false);
  const pendingPrimaryKeyColumnsRef = useRef<string[]>([]);
  const pendingReadOnlyTableRef = useRef(isView);
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
  const isViewRef = useRef(isView);
  const initialPageSizeRef = useRef(initialPageSize);
  const onTableInitRef = useRef(onTableInit);
  const onRowsCommittedRef = useRef(onRowsCommitted);

  isViewRef.current = isView;
  initialPageSizeRef.current = initialPageSize;
  requestedSortRef.current = requestedSort;
  hasCommittedDataRef.current = hasCommittedData;
  requestedPageRef.current = requestedPage;
  requestedPageSizeRef.current = requestedPageSize;
  debouncedFilterDraftsRef.current = debouncedFilterDrafts;
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
    setReadOnlyTable(isView);
    pendingReadOnlyTableRef.current = isView;
  }, [isView]);

  useEffect(() => {
    const unInit = onMessage<{
      columns: ColumnMeta[];
      primaryKeyColumns: string[];
    }>("tableInit", ({ columns: nextColumns, primaryKeyColumns }) => {
      columnsRef.current = nextColumns;
      pendingPrimaryKeyColumnsRef.current = primaryKeyColumns;
      pendingReadOnlyTableRef.current = isViewRef.current;

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
      setError(null);
      setReadError(null);
      setFilterError(null);
      setPage(1);
      setPageSize(initialPageSizeRef.current);
      setRequestedPage(1);
      setRequestedPageSize(initialPageSizeRef.current);
      setSort(null);
      setRequestedSort(null);
      setFilterDrafts({});
      setDebouncedFilterDrafts({});
      setColSizes({});

      onTableInitRef.current();
      setInitTick((tick) => tick + 1);
    });

    const unData = onMessage<{
      fetchId?: number;
      rows: Row[];
      totalCount: number;
    }>(
      "tableData",
      ({ fetchId, rows: nextRows, totalCount: nextTotalCount }) => {
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
        setPage(snapshot.page);
        setPageSize(snapshot.pageSize);
        setSort(snapshot.sort);
        setLoading(false);
        setHasCommittedData(true);
        setError(null);
        setReadError(null);
        setFilterError(null);

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

  useEffect(() => {
    if (!initializedRef.current || fetchTrigger === "") {
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
      if (previousSort?.column === column) {
        if (previousSort.direction === "asc") {
          return { column, direction: "desc" };
        }

        return null;
      }

      return { column, direction: "asc" };
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
          setFilterError(null);
          setRequestedPage(1);
          setDebouncedFilterDrafts(
            buildActiveFilterDrafts(columnsRef.current, nextDrafts),
          );
        }

        return nextDrafts;
      });
    },
    [columnsRef],
  );

  return {
    columns,
    colSizes,
    debouncedFilterDrafts,
    error,
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
