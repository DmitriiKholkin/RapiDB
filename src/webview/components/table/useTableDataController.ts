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
  isMssqlSqlVariantPrimaryKeyValue,
  type MssqlSqlVariantRowIdentity,
} from "../../../shared/mssqlSqlVariantRowIdentity";
import {
  isMySQLTimestampPrimaryKeyValue,
  type MySQLTimestampRowIdentity,
} from "../../../shared/mysqlTimestampRowIdentity";
import {
  isOracleTemporalPrimaryKeyValue,
  type OracleTemporalRowIdentity,
} from "../../../shared/oracleTemporalRowIdentity";
import {
  isPostgresArrayPrimaryKeyValue,
  type PostgresArrayRowIdentity,
} from "../../../shared/postgresArrayRowIdentity";
import {
  isPostgresIntervalPrimaryKeyValue,
  type PostgresIntervalRowIdentity,
} from "../../../shared/postgresIntervalRowIdentity";
import {
  isSqlDeleteObservation,
  type SqlDeleteObservation,
} from "../../../shared/sqlDeleteObservation";
import {
  isSQLitePrimaryKeyValue,
  type SQLiteRowIdentity,
} from "../../../shared/sqliteRowIdentity";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraft,
  type FilterDraftMap,
  type FilterExpression,
  type MongoValueTypeHint,
  serializeFilterDrafts,
} from "../../../shared/tableTypes";
import {
  isRecord,
  type TableInitPayload,
} from "../../../shared/webviewContracts";
import type { Row } from "../../types";
import {
  calcColWidths,
  type Column as WidthColumn,
} from "../../utils/columnSizing";
import { onMessage, postMessage } from "../../utils/messaging";
import { isArrayOf } from "../../utils/replyShape";
import {
  buildActiveFilterDrafts,
  DEBOUNCE,
  type FetchSnapshot,
  getFilterDraftValidationError,
  type PersistedTableViewState,
  registerMssqlSqlVariantRowIdentity,
  registerMySQLTimestampRowIdentity,
  registerOracleTemporalRowIdentity,
  registerPostgresArrayRowIdentity,
  registerPostgresIntervalRowIdentity,
  registerSQLiteRowIdentity,
  registerSqlDeleteObservation,
  type TableFetchOptions,
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
  fetchPageRef: MutableRefObject<(options?: TableFetchOptions) => void>;
  preserveScrollPositionRef: MutableRefObject<() => void>;
  onTableInit: () => void;
  getMetadataRefreshState: () => {
    busy: boolean;
    hasWork: boolean;
    reconciliationPending?: boolean;
    applyConflict?: boolean;
    applying?: boolean;
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
    conflictRefresh?: boolean,
  ) => void;
}

interface TableDataReply {
  extraColumns?: ColumnMeta[];
  pageScopedExtraColumns?: true;
  rows: Row[];
  mongoIdTypes?: unknown;
  mongoValueTypes?: Array<Record<string, MongoValueTypeHint>>;
  rowValueTypes?: Array<Record<string, MongoValueTypeHint>>;
  sqliteRowIdentities?: SQLiteRowIdentity[];
  postgresArrayRowIdentities?: PostgresArrayRowIdentity[];
  postgresIntervalRowIdentities?: PostgresIntervalRowIdentity[];
  mysqlTimestampRowIdentities?: MySQLTimestampRowIdentity[];
  mssqlSqlVariantRowIdentities?: MssqlSqlVariantRowIdentity[];
  oracleTemporalRowIdentities?: OracleTemporalRowIdentity[];
  sqlDeleteObservations?: Array<SqlDeleteObservation | null>;
  totalCount: number;
  executionTimeMs?: number;
}

const INVALID_TABLE_REPLY = "The host returned an invalid table read reply.";

function isValueTypeMap(
  value: unknown,
): value is Record<string, MongoValueTypeHint> {
  return (
    isRecord(value) &&
    Object.values(value).every(
      (hint) =>
        isRecord(hint) &&
        typeof hint.category === "string" &&
        typeof hint.nativeType === "string",
    )
  );
}

function isTableDataReply(
  payload: Record<string, unknown>,
): payload is Record<string, unknown> & TableDataReply {
  return (
    isArrayOf(payload.rows, isRecord) &&
    typeof payload.totalCount === "number" &&
    Number.isFinite(payload.totalCount) &&
    payload.totalCount >= 0 &&
    (payload.executionTimeMs === undefined ||
      Number.isFinite(payload.executionTimeMs)) &&
    (payload.pageScopedExtraColumns === undefined ||
      payload.pageScopedExtraColumns === true) &&
    (payload.extraColumns === undefined ||
      (isArrayOf(payload.extraColumns, isRecord) &&
        payload.extraColumns.every(
          (column) =>
            typeof column.name === "string" &&
            typeof column.type === "string" &&
            typeof column.nativeType === "string" &&
            Array.isArray(column.filterOperators),
        ))) &&
    (payload.mongoValueTypes === undefined ||
      isArrayOf(payload.mongoValueTypes, isValueTypeMap)) &&
    (payload.rowValueTypes === undefined ||
      isArrayOf(payload.rowValueTypes, isValueTypeMap)) &&
    (payload.sqlDeleteObservations === undefined ||
      (Array.isArray(payload.sqlDeleteObservations) &&
        payload.sqlDeleteObservations.length === payload.rows.length &&
        isArrayOf(
          payload.sqlDeleteObservations,
          (value): value is SqlDeleteObservation | null =>
            value === null || isSqlDeleteObservation(value),
        ))) &&
    isRowIdentityList(
      payload.sqliteRowIdentities,
      payload.rows.length,
      isSQLitePrimaryKeyValue,
    ) &&
    isRowIdentityList(
      payload.postgresArrayRowIdentities,
      payload.rows.length,
      isPostgresArrayPrimaryKeyValue,
    ) &&
    isRowIdentityList(
      payload.postgresIntervalRowIdentities,
      payload.rows.length,
      isPostgresIntervalPrimaryKeyValue,
    ) &&
    isRowIdentityList(
      payload.mysqlTimestampRowIdentities,
      payload.rows.length,
      isMySQLTimestampPrimaryKeyValue,
    ) &&
    isRowIdentityList(
      payload.mssqlSqlVariantRowIdentities,
      payload.rows.length,
      isMssqlSqlVariantPrimaryKeyValue,
    ) &&
    isRowIdentityList(
      payload.oracleTemporalRowIdentities,
      payload.rows.length,
      isOracleTemporalPrimaryKeyValue,
    )
  );
}

function isRowIdentityList(
  value: unknown,
  rowCount: number,
  isKey: (key: unknown) => boolean,
): boolean {
  return (
    value === undefined ||
    (isArrayOf(value, isRecord) &&
      value.length === rowCount &&
      value.every((keys) =>
        Object.values(keys).every((key) => key === null || isKey(key)),
      ))
  );
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
      postgresArrayPrimaryKey: column.postgresArrayPrimaryKey ?? false,
      postgresIntervalPrimaryKey: column.postgresIntervalPrimaryKey ?? null,
      mysqlTimestampPrimaryKey: column.mysqlTimestampPrimaryKey ?? false,
      mssqlSqlVariantPrimaryKey: column.mssqlSqlVariantPrimaryKey ?? false,
      oracleTemporalPrimaryKey: column.oracleTemporalPrimaryKey ?? false,
      isForeignKey: column.isForeignKey,
      category: column.category,
      redisValue: column.redisValue,
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
  const [filterDraftError, setFilterDraftError] = useState<{
    message: string;
    revision: number;
  } | null>(null);
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
  const [initialSortCleared, setInitialSortCleared] = useState(false);
  const [committedFilters, setCommittedFilters] = useState<FilterExpression[]>(
    [],
  );
  const [readOnlyTable, setReadOnlyTable] = useState(initialReadOnlyTable);
  const [colSizes, setColSizes] = useState<Record<string, number>>({});
  // These columns describe the committed page, not the table's write schema.
  const [pageColumns, setPageColumns] = useState<ColumnMeta[]>([]);
  const visiblePageColumns = useMemo(() => {
    const schemaNames = new Set(columns.map((column) => column.name));
    return pageColumns.filter((column) => !schemaNames.has(column.name));
  }, [columns, pageColumns]);
  const gridColumns = useMemo(
    () =>
      visiblePageColumns.length ? [...columns, ...visiblePageColumns] : columns,
    [columns, visiblePageColumns],
  );
  const pageScopedColumnNames = useMemo(
    () => new Set(visiblePageColumns.map((column) => column.name)),
    [visiblePageColumns],
  );
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
  const committedSnapshotRef = useRef<FetchSnapshot | null>(null);
  const hasCommittedDataRef = useRef(hasCommittedData);
  const requestedPageRef = useRef(requestedPage);
  const requestedPageSizeRef = useRef(requestedPageSize);
  const skipNextFetchRef = useRef(false);
  const debouncedFilterDraftsRef = useRef(debouncedFilterDrafts);
  const initializedRef = useRef(false);
  const fetchEpochRef = useRef(0);
  const readIdentityFailedRef = useRef(false);
  const filterDraftsRef = useRef(filterDrafts);
  const filterDraftsToDebounceRef = useRef<FilterDraftMap | null>(null);
  const filterDraftRevisionRef = useRef(0);
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
  const conflictFetchIdRef = useRef<number | null>(null);

  const replaceFilterDrafts = useCallback(
    (nextDrafts: FilterDraftMap, debounce: boolean) => {
      // Compose edits against the latest write, even before React commits it.
      filterDraftsRef.current = nextDrafts;
      filterDraftsToDebounceRef.current = debounce ? nextDrafts : null;
      readyFilterDraftsRef.current = null;
      setFilterDrafts(nextDrafts);
    },
    [],
  );

  const getVisibleFilterDraftError = useCallback(
    (drafts: FilterDraftMap) => {
      const revision = filterDraftRevisionRef.current;
      if (dismissedFilterDraftRevisionRef.current === revision) return null;
      const message = getFilterDraftValidationError(columnsRef.current, drafts);
      return message ? { message, revision } : null;
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

  const getCommittedView = useCallback((tableKey: string) => {
    const snapshot = committedSnapshotRef.current;
    if (!snapshot) return undefined;
    return {
      ...snapshot,
      tableKey,
      filters: Object.fromEntries(
        snapshot.filters.map(({ column, ...draft }) => [column, draft]),
      ),
    };
  }, []);

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
      setPageColumns([]);
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
      const nextDrafts = buildActiveFilterDrafts(
        metadata.columns,
        filterDraftsRef.current,
      );
      // Metadata cleanup is not a filter edit, but an outstanding edit still
      // needs to apply after the refresh read.
      const hasPendingFilterEdit =
        filterDraftsToDebounceRef.current !== null ||
        readyFilterDraftsRef.current !== null;
      replaceFilterDrafts(nextDrafts, hasPendingFilterEdit);
      if (!hasPendingFilterEdit) {
        setFilterDraftError(getVisibleFilterDraftError(nextDrafts));
      }
      setDebouncedFilterDrafts(nextFilters);
      setInitTick((tick) => tick + 1);
    }
  }, [
    columnsRef,
    pkColsRef,
    rowsRef,
    mongoIdTypesRef,
    rowValueTypesRef,
    replaceFilterDrafts,
    getVisibleFilterDraftError,
  ]);

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
  readOnlyTableRef.current = readOnlyTable;
  onTableInitRef.current = onTableInit;
  onReadFailedRef.current = onReadFailed;
  onRowsCommittedRef.current = onRowsCommitted;
  rowsRef.current = rows;
  pkColsRef.current = pkCols;

  const fetchPage = useCallback(
    (options?: TableFetchOptions) => {
      if (!initializedRef.current) {
        return;
      }
      if (queuedMetadataRef.current || connectionInvalidatedRef.current) {
        if (options?.conflictRefresh) return;
        deferredReadRef.current = true;
        return;
      }

      const mutationState = metadataStateRef.current();
      const snapshot =
        options?.conflictRefresh || mutationState.applyConflict
          ? (committedSnapshotRef.current ?? getRequestedSnapshot())
          : getRequestedSnapshot();
      deferredReadRef.current = false;

      const epoch = advanceFetchEpoch();
      if (epoch === null) return;
      reconciliationFetchIdRef.current = mutationState.reconciliationPending
        ? epoch
        : null;
      conflictFetchIdRef.current = options?.conflictRefresh ? epoch : null;

      fetchSnapshotsRef.current.clear();
      fetchSnapshotsRef.current.set(epoch, snapshot);
      loadingRef.current = true;
      setLoading(true);
      setError(null);
      setReadError(null);

      fetchHadFiltersRef.current = snapshot.filters.length > 0;

      postMessage("fetchPage", {
        fetchId: epoch,
        page: snapshot.page,
        pageSize: snapshot.pageSize,
        filters: snapshot.filters,
        sort: snapshot.sort,
        ...(options?.refreshMetadata ? { refreshMetadata: true } : {}),
        ...(options?.refreshData ? { refreshData: true } : {}),
        ...(options?.conflictRefresh ? { conflictRefresh: true } : {}),
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
    const isCurrentFetch = (fetchId: unknown): fetchId is number =>
      fetchId === fetchEpochRef.current &&
      fetchSnapshotsRef.current.has(fetchEpochRef.current);

    const finishReadError = (fetchId: number) => {
      fetchSnapshotsRef.current.delete(fetchId);
      loadingRef.current = false;
      setLoading(false);
      onReadFailedRef.current();
    };

    const rejectMalformedRead = (fetchId: unknown) => {
      if (!isCurrentFetch(fetchId)) return;
      if (hasCommittedDataRef.current) setReadError(INVALID_TABLE_REPLY);
      else setError(INVALID_TABLE_REPLY);
      scrollPreserveRef.current = null;
      finishReadError(fetchId);
    };

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
        conflictFetchIdRef.current = null;
        committedSnapshotRef.current = null;
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
        setPageColumns([]);
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
        setFilterDraftError(getVisibleFilterDraftError(nextFilters));
        setPage(nextPage);
        setPageSize(nextPageSize);
        setRequestedPage(nextPage);
        setRequestedPageSize(nextPageSize);
        setReadOnlyTable(nextReadOnlyTable);
        // Seed the saved sort baseline; later requests cannot change it until
        // their rows commit (or the user explicitly clears it).
        setSort(nextSort);
        setRequestedSort(nextSort);
        setInitialSortCleared(false);
        replaceFilterDrafts(nextFilters, false);
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
        conflictFetchIdRef.current = null;
        loadingRef.current = false;
        setLoading(false);
        setSchemaWarning(
          "Connection changed. Waiting for metadata from the new session.",
        );
      },
    );

    const unData = onMessage<unknown>(
      "tableData",
      // Only a reply with a current fetch ID can fail the read.
      (payload) => {
        if (
          !isRecord(payload) ||
          connectionInvalidatedRef.current ||
          queuedMetadataRef.current ||
          readIdentityFailedRef.current
        )
          return;
        const { fetchId } = payload;
        const mutationState = metadataStateRef.current();
        if (fetchId !== undefined && typeof fetchId !== "number") return;
        if (
          fetchId === undefined
            ? reconciliationFetchIdRef.current !== null ||
              mutationState.applyConflict ||
              mutationState.applying ||
              fetchSnapshotsRef.current.size > 0 ||
              !hasCommittedDataRef.current
            : !isCurrentFetch(fetchId)
        )
          return;
        if (!isTableDataReply(payload)) {
          rejectMalformedRead(fetchId);
          return;
        }
        const {
          extraColumns,
          pageScopedExtraColumns,
          rows: receivedRows,
          mongoIdTypes,
          mongoValueTypes,
          rowValueTypes,
          sqliteRowIdentities,
          postgresArrayRowIdentities,
          postgresIntervalRowIdentities,
          mysqlTimestampRowIdentities,
          mssqlSqlVariantRowIdentities,
          oracleTemporalRowIdentities,
          sqlDeleteObservations,
          totalCount: nextTotalCount,
          executionTimeMs: nextExecutionTimeMs,
        } = payload;

        const nextRowValueTypes = rowValueTypes ?? mongoValueTypes ?? [];
        const postgresArrayKeyNames = columnsRef.current
          .filter((column) => column.postgresArrayPrimaryKey)
          .map((column) => column.name);
        const postgresIntervalKeyNames = columnsRef.current
          .filter((column) => column.postgresIntervalPrimaryKey)
          .map((column) => column.name);
        const mysqlTimestampKeyNames = columnsRef.current
          .filter((column) => column.mysqlTimestampPrimaryKey)
          .map((column) => column.name);
        const mssqlVariantKeyNames = columnsRef.current
          .filter((column) => column.mssqlSqlVariantPrimaryKey)
          .map((column) => column.name);
        const oracleTemporalKeyNames = columnsRef.current
          .filter((column) => column.oracleTemporalPrimaryKey)
          .map((column) => column.name);
        const nextRows = receivedRows.map((row, index) => {
          const hints = nextRowValueTypes[index];
          const missingUndefined = hints
            ? Object.keys(hints).filter(
                (name) =>
                  hints[name]?.nativeType === "undefined" &&
                  !Object.hasOwn(row, name),
              )
            : [];
          const copy = missingUndefined.length > 0 ? { ...row } : row;
          for (const name of missingUndefined) {
            Object.defineProperty(copy, name, {
              value: undefined,
              enumerable: true,
              writable: true,
              configurable: true,
            });
          }
          if (sqliteRowIdentities) {
            registerSQLiteRowIdentity(copy, sqliteRowIdentities[index]);
          }
          const deleteObservation = sqlDeleteObservations?.[index];
          if (deleteObservation) {
            registerSqlDeleteObservation(copy, deleteObservation);
          }
          if (postgresArrayKeyNames.length > 0) {
            const identity = postgresArrayRowIdentities?.[index];
            registerPostgresArrayRowIdentity(
              copy,
              Object.fromEntries(
                postgresArrayKeyNames.map((name) => [
                  name,
                  identity && Object.hasOwn(identity, name)
                    ? identity[name]
                    : null,
                ]),
              ),
            );
          }
          if (postgresIntervalKeyNames.length > 0) {
            const identity = postgresIntervalRowIdentities?.[index];
            registerPostgresIntervalRowIdentity(
              copy,
              Object.fromEntries(
                postgresIntervalKeyNames.map((name) => [
                  name,
                  identity && Object.hasOwn(identity, name)
                    ? identity[name]
                    : null,
                ]),
              ),
            );
          }
          if (mysqlTimestampKeyNames.length > 0) {
            const identity = mysqlTimestampRowIdentities?.[index];
            registerMySQLTimestampRowIdentity(
              copy,
              Object.fromEntries(
                mysqlTimestampKeyNames.map((name) => [
                  name,
                  identity && Object.hasOwn(identity, name)
                    ? identity[name]
                    : null,
                ]),
              ),
            );
          }
          if (mssqlVariantKeyNames.length > 0) {
            const identity = mssqlSqlVariantRowIdentities?.[index];
            registerMssqlSqlVariantRowIdentity(
              copy,
              Object.fromEntries(
                mssqlVariantKeyNames.map((name) => [
                  name,
                  identity && Object.hasOwn(identity, name)
                    ? identity[name]
                    : null,
                ]),
              ),
            );
          }
          if (oracleTemporalKeyNames.length > 0) {
            const identity = oracleTemporalRowIdentities?.[index];
            registerOracleTemporalRowIdentity(
              copy,
              Object.fromEntries(
                oracleTemporalKeyNames.map((name) => [
                  name,
                  identity && Object.hasOwn(identity, name)
                    ? identity[name]
                    : null,
                ]),
              ),
            );
          }
          return copy;
        });

        const replyEpoch = fetchId ?? fetchEpochRef.current;
        const requestedSnapshot = fetchSnapshotsRef.current.get(replyEpoch);
        const snapshot = requestedSnapshot ?? getRequestedSnapshot();
        if (requestedSnapshot) committedSnapshotRef.current = requestedSnapshot;

        const knownColumnNames = new Set(
          columnsRef.current.map((column) => column.name),
        );
        const addedColumns = (extraColumns ?? []).filter((column) => {
          if (knownColumnNames.has(column.name)) return false;
          knownColumnNames.add(column.name);
          return true;
        });
        if (pageScopedExtraColumns) {
          setPageColumns(addedColumns);
        } else if (addedColumns.length > 0) {
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
        // Missing or unrecognized ID hints stay uneditable, but don't hide rows.
        const nextMongoIdTypes = Array.isArray(mongoIdTypes)
          ? mongoIdTypes.map((type) =>
              type === "objectId" || type === "string" ? type : null,
            )
          : [];
        pasteStructureEpochRef.current++;
        setRows(nextRows);
        mongoIdTypesRef.current = nextMongoIdTypes;
        rowValueTypesRef.current = nextRowValueTypes;
        setTotalCount(nextTotalCount);
        setExecutionTimeMs(nextExecutionTimeMs);
        setPage(snapshot.page);
        setPageSize(snapshot.pageSize);
        // An unsolicited refresh cannot confirm a requested sort.
        if (requestedSnapshot) setSort(requestedSnapshot.sort);
        setInitialSortCleared(false);
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
          fetchId !== undefined && fetchId === conflictFetchIdRef.current,
        );
        fetchSnapshotsRef.current.delete(replyEpoch);

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

    const unError = onMessage<unknown>("tableError", (payload) => {
      if (
        !isRecord(payload) ||
        queuedMetadataRef.current ||
        readIdentityFailedRef.current
      )
        return;
      const { fetchId, error: nextError, isFilterError } = payload;
      if (
        fetchId === undefined
          ? reconciliationFetchIdRef.current !== null ||
            metadataStateRef.current().applyConflict ||
            fetchSnapshotsRef.current.size > 0
          : connectionInvalidatedRef.current || !isCurrentFetch(fetchId)
      ) {
        return;
      }
      if (
        typeof nextError !== "string" ||
        nextError.trim() === "" ||
        (isFilterError !== undefined && typeof isFilterError !== "boolean")
      ) {
        rejectMalformedRead(fetchId);
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
              fetchSnapshotsRef.current.get(fetchEpochRef.current) ??
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

      finishReadError(fetchEpochRef.current);
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
    getVisibleFilterDraftError,
    initialView,
    mongoIdTypesRef,
    replaceFilterDrafts,
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
    if (filterDraftsToDebounceRef.current !== filterDrafts) return;

    const timeoutId = setTimeout(() => {
      if (filterDraftsToDebounceRef.current !== filterDrafts) return;
      filterDraftsToDebounceRef.current = null;
      readyFilterDraftsRef.current = filterDrafts;
      retryPendingFilter();
    }, DEBOUNCE);

    return () => clearTimeout(timeoutId);
  }, [filterDrafts, retryPendingFilter]);

  const clearFilterError = useCallback(() => {
    setFilterError(null);
    setFilterNotice(null);
    if (!filterError && filterDraftError) {
      dismissedFilterDraftRevisionRef.current = filterDraftError.revision;
    }
    setFilterDraftError((current) =>
      current === filterDraftError ? null : current,
    );
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

  const canRetryInitialRead =
    isInitialized &&
    !hasCommittedData &&
    !loading &&
    !readIdentityFailedRef.current &&
    !metadataBlockedRef.current;

  const retryInitialRead = (clearSort = false) => {
    if (
      !canRetryInitialRead ||
      loadingRef.current ||
      metadataBlockedRef.current ||
      readIdentityFailedRef.current
    )
      return;
    // This is only a page read; restored mutation recovery still requires its
    // separate explicit verification once the table is visible.
    if (clearSort && requestedSortRef.current) {
      setRequestedPage(1);
      setRequestedSort(null);
      setSort(null);
      setInitialSortCleared(true);
    } else {
      fetchPage();
    }
  };

  const updateFilterDraft = useCallback(
    (
      columnName: string,
      nextDraft: FilterDraft | undefined,
      options?: { applyImmediately?: boolean },
    ) => {
      setFilterNotice(null);
      const currentDrafts = filterDraftsRef.current;
      if (!nextDraft && currentDrafts[columnName] === undefined) return;

      const nextDrafts = nextDraft
        ? { ...currentDrafts, [columnName]: nextDraft }
        : { ...currentDrafts };
      if (!nextDraft) delete nextDrafts[columnName];

      filterDraftRevisionRef.current += 1;
      replaceFilterDrafts(nextDrafts, !options?.applyImmediately);
      if (options?.applyImmediately) syncRequestedFilterState(nextDrafts);
      else setFilterDraftPending(true);
    },
    [replaceFilterDrafts, syncRequestedFilterState],
  );

  return {
    committedColumnNamesRef,
    metadataBlockedRef,
    metadataTick,
    canRetryInitialRead,
    retryInitialRead,
    retryMetadataRefresh,
    retryPendingFilter,
    schemaWarning,
    loadingRef,
    fetchEpochRef,
    columns: gridColumns,
    pageScopedColumnNames,
    colSizes,
    committedFilters,
    error,
    executionTimeMs,
    fetchPage,
    getCommittedView,
    filterDrafts,
    filterError: filterError ?? filterDraftError?.message ?? filterNotice,
    hasCommittedData,
    initialSortCleared,
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
