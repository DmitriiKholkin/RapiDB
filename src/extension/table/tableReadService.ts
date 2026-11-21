import { isMssqlSqlVariantPrimaryKeyValue } from "../../shared/mssqlSqlVariantRowIdentity";
import { observedNoSqlDeleteBackend } from "../../shared/noSqlDeleteObservation";
import { isOracleTemporalPrimaryKeyValue } from "../../shared/oracleTemporalRowIdentity";
import { isPostgresIntervalPrimaryKeyValue } from "../../shared/postgresIntervalRowIdentity";
import { isSQLitePrimaryKeyValue } from "../../shared/sqliteRowIdentity";
import type { QueryMissingCell } from "../../shared/tableTypes";
import type { ConnectionManager } from "../connectionManager";
import {
  buildSqlDeleteSnapshot,
  readSqlDeleteObservations,
} from "../dbDrivers/sqlDeleteSnapshot";
import {
  SQLITE_ATTACHMENT_CHANGED,
  type SQLiteAttachmentTarget,
} from "../dbDrivers/sqliteAttachmentTarget";
import type {
  ColumnTypeMeta,
  DriverTableExportChunk,
  DriverTableExportFormat,
  FilterExpression,
  QueryResult,
} from "../dbDrivers/types";
import { normalizeUnknownError } from "../utils/errorHandling";
import { buildWhere, validateFilterExpressions } from "./filterSql";
import type { SortConfig, TablePage } from "./tableDataContracts";
import { tableDataReference } from "./tableDataReference";

type ExportOrderColumn = {
  column: ColumnTypeMeta;
  direction: "asc" | "desc";
};

type ExportCursor = Record<string, unknown>;

export class TableReadService {
  private readonly columnCache = new Map<string, ColumnTypeMeta[]>();
  private readonly columnCacheGenerations = new Map<string, number>();
  private readonly rowIdentityOrderCache = new Map<string, string[]>();
  private sqliteAttachmentTarget?: SQLiteAttachmentTarget;

  private encodeKeyPart(value: string): string {
    return `${value.length}:${value}`;
  }

  constructor(private readonly connectionManager: ConnectionManager) {}

  /** Set before the first metadata read on a Table Viewer-scoped service. */
  bindSQLiteAttachmentTarget(target: SQLiteAttachmentTarget): void {
    this.sqliteAttachmentTarget ??= target;
  }

  private getSQLiteAttachmentScope() {
    return this.sqliteAttachmentTarget
      ? { sqliteAttachmentTarget: this.sqliteAttachmentTarget }
      : {};
  }

  clearForConnection(connectionId: string): void {
    this.columnCacheGenerations.set(
      connectionId,
      (this.columnCacheGenerations.get(connectionId) ?? 0) + 1,
    );
    this.connectionManager.getDriver(connectionId)?.clearTableReadCache?.();
    for (const key of this.columnCache.keys()) {
      if (key.startsWith(`${this.encodeKeyPart(connectionId)}|`)) {
        this.columnCache.delete(key);
      }
    }
    for (const key of this.rowIdentityOrderCache.keys()) {
      if (key.startsWith(`${this.encodeKeyPart(connectionId)}|`)) {
        this.rowIdentityOrderCache.delete(key);
      }
    }
  }

  async getColumns(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const cacheKey = this.columnCacheKey(connectionId, database, schema, table);
    const cached = this.columnCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const generation = this.columnCacheGenerations.get(connectionId) ?? 0;
    const { driver } = this.getConnectionDriver(connectionId);
    const target = this.sqliteAttachmentTarget;
    const columns = target
      ? await driver.describeSQLiteAttachmentColumns?.(target, table)
      : await driver.describeColumns(database, schema, table);
    if (!columns) throw new Error(SQLITE_ATTACHMENT_CHANGED);
    if ((this.columnCacheGenerations.get(connectionId) ?? 0) === generation) {
      this.columnCache.set(cacheKey, columns);
    }
    return columns;
  }

  private async getRowIdentityOrderExpressions(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    columns: readonly ColumnTypeMeta[],
  ): Promise<string[]> {
    const { driver } = this.getConnectionDriver(connectionId);
    if (
      !driver.getTableRowIdentityOrderExpressions ||
      columns.some((column) => column.isPrimaryKey)
    ) {
      return [];
    }
    const cacheKey = this.columnCacheKey(connectionId, database, schema, table);
    const cached = this.rowIdentityOrderCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const generation = this.columnCacheGenerations.get(connectionId) ?? 0;
    let expressions: string[];
    try {
      expressions = await driver.getTableRowIdentityOrderExpressions(
        database,
        schema,
        table,
        columns,
      );
    } catch (error: unknown) {
      console.error(
        "[RapiDB] Row identity lookup failed, reading without a row tie-breaker:",
        normalizeUnknownError(
          error,
          this.connectionManager.getConnectionDiagnosticContext?.(
            connectionId,
            driver,
          ),
        ).message,
      );
      return [];
    }
    if ((this.columnCacheGenerations.get(connectionId) ?? 0) === generation) {
      this.rowIdentityOrderCache.set(cacheKey, expressions);
    }
    return expressions;
  }

  async getPage(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    page: number,
    pageSize: number,
    filters: FilterExpression[],
    sort: SortConfig | null = null,
    skipCount = false,
    signal?: AbortSignal,
    assertExportSession?: () => void,
    observeSqlDelete = false,
    conflictRefresh = false,
    browsingSnapshot?: AbortSignal,
  ): Promise<TablePage> {
    assertExportSession?.();
    signal?.throwIfAborted();
    browsingSnapshot?.throwIfAborted();
    if (!Array.isArray(filters)) {
      throw new Error("[RapiDB Filter] Filters must be an array.");
    }

    const { driver } = this.getConnectionDriver(connectionId);
    const columns =
      filters.length > 0
        ? await this.getColumns(connectionId, database, schema, table)
        : undefined;
    assertExportSession?.();
    signal?.throwIfAborted();
    browsingSnapshot?.throwIfAborted();
    if (columns) {
      validateFilterExpressions(filters, columns);
    }

    if (driver.readTablePage) {
      const connection = this.connectionManager.getConnection(connectionId);
      const observeNoSqlDelete =
        !assertExportSession &&
        !connection?.readOnly &&
        driver.prepareObservedDelete &&
        observedNoSqlDeleteBackend(connection?.type);
      return driver.readTablePage({
        database,
        schema,
        table,
        page,
        pageSize,
        filters,
        sort,
        skipCount,
        ...(assertExportSession ? { omitRowIdentity: true } : {}),
        ...(observeSqlDelete ? { observeSqlDelete: true } : {}),
        ...(observeNoSqlDelete ? { observeNoSqlDelete: true } : {}),
        ...(conflictRefresh ? { conflictRefresh: true } : {}),
        ...(browsingSnapshot ? { browsingSnapshot } : {}),
        ...(signal ? { signal } : {}),
      });
    }

    return this.readFallbackPage(
      connectionId,
      database,
      schema,
      table,
      page,
      pageSize,
      filters,
      sort,
      skipCount,
      columns,
      assertExportSession,
      signal,
      observeSqlDelete,
    );
  }

  async *getPageChunks(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    page: number,
    pageSize: number,
    filters: FilterExpression[],
    sort: SortConfig | null = null,
    signal?: AbortSignal,
    assertExportSession?: () => void,
  ): AsyncGenerator<DriverTableExportChunk> {
    assertExportSession?.();
    const { driver } = this.getConnectionDriver(connectionId);
    if (!driver.readTablePageChunks) {
      const result = await this.getPage(
        connectionId,
        database,
        schema,
        table,
        page,
        pageSize,
        filters,
        sort,
        true,
        signal,
        assertExportSession,
      );
      yield { columns: result.columns, rows: result.rows };
      return;
    }
    signal?.throwIfAborted();
    if (!Array.isArray(filters)) {
      throw new Error("[RapiDB Filter] Filters must be an array.");
    }
    if (filters.length > 0) {
      const columns = await this.getColumns(
        connectionId,
        database,
        schema,
        table,
      );
      assertExportSession?.();
      signal?.throwIfAborted();
      validateFilterExpressions(filters, columns);
    }
    assertExportSession?.();
    signal?.throwIfAborted();
    yield* driver.readTablePageChunks({
      database,
      schema,
      table,
      page,
      pageSize,
      filters,
      sort,
      skipCount: true,
      ...(signal ? { signal } : {}),
    });
  }

  private async readFallbackPage(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    page: number,
    pageSize: number,
    filters: FilterExpression[],
    sort: SortConfig | null,
    skipCount: boolean,
    knownColumns?: ColumnTypeMeta[],
    assertExportSession?: () => void,
    signal?: AbortSignal,
    observeSqlDelete = false,
  ): Promise<TablePage> {
    assertExportSession?.();
    signal?.throwIfAborted();
    const startTime = performance.now();
    const { driver } = this.getConnectionDriver(connectionId);

    const qualifiedTableName = driver.qualifiedTableName(
      database,
      schema,
      table,
    );
    const columns =
      knownColumns ??
      (await this.getColumns(connectionId, database, schema, table));
    assertExportSession?.();
    signal?.throwIfAborted();
    const { clause: whereClause, params: whereParams } = buildWhere(
      driver,
      filters,
      columns,
    );
    const from = tableDataReference(driver, database, schema, table, columns);
    const offset = (page - 1) * pageSize;
    const orderByClause = this.resolveFallbackOrderBy(
      driver,
      columns,
      sort,
      qualifiedTableName,
      await this.getRowIdentityOrderExpressions(
        connectionId,
        database,
        schema,
        table,
        columns,
      ),
    );
    assertExportSession?.();
    signal?.throwIfAborted();
    const count = await this.readFallbackTotalCount(
      connectionId,
      driver,
      database,
      from,
      whereClause,
      whereParams,
      skipCount,
      signal,
    );
    assertExportSession?.();
    signal?.throwIfAborted();
    const fetchPageSize = count.countFailed ? pageSize + 1 : pageSize;
    const pagination = driver.buildPagination(
      offset,
      fetchPageSize,
      whereParams.length + 1,
    );
    const effectiveOrderBy =
      orderByClause ||
      driver.buildOrderByDefault(
        columns,
        driver.buildTableDataOrderExpression ? qualifiedTableName : undefined,
      );
    const baseParams = [...whereParams, ...pagination.params];
    const projection = driver.buildTableDataProjection?.(columns) ?? "*";
    const identityColumns = columns.flatMap((column) => {
      const expression =
        column.isPrimaryKey &&
        !(
          assertExportSession &&
          (column.mssqlSqlVariantPrimaryKey || column.oracleTemporalPrimaryKey)
        ) &&
        driver.buildTablePrimaryKeyExpr?.(column);
      return expression ? [{ column, expression }] : [];
    });
    const displayNames = new Set(columns.map(({ name }) => name));
    let identityAliasIndex = 0;
    const identityProjection = identityColumns
      .map(({ column, expression }) => {
        const prefix = column.postgresArrayPrimaryKey
          ? "pg_array"
          : column.postgresIntervalPrimaryKey
            ? "pg_interval"
            : column.oracleTemporalPrimaryKey
              ? "oracle_temporal"
              : undefined;
        if (!prefix) return `, ${expression}`;
        // Do not shadow any real column in the result or ORDER BY.
        let alias: string;
        do {
          alias = `__rapidb_${prefix}_key_${identityAliasIndex++}`;
        } while (displayNames.has(alias));
        return `, ${expression} AS ${driver.quoteIdentifier(alias)}`;
      })
      .join("");
    const deleteSnapshot = observeSqlDelete
      ? buildSqlDeleteSnapshot(
          this.connectionManager.getConnection(connectionId)?.type,
          driver,
          columns,
        )
      : undefined;
    const displayNamesFolded = new Set(
      columns.map(({ name }) => name.toLowerCase()),
    );
    let deleteAliasIndex = 0;
    const deleteProjectionColumns = deleteSnapshot?.expressions.map(
      (expression) => {
        let alias: string;
        do {
          alias = `__rapidb_delete_${deleteAliasIndex++}`;
        } while (displayNamesFolded.has(alias));
        return { alias, expression };
      },
    );
    const deleteProjection = deleteProjectionColumns
      ?.map(
        ({ alias, expression }) =>
          `, ${expression} AS ${driver.quoteIdentifier(alias)}`,
      )
      .join("");
    const pageQuery = (base: string, snapshot = "") =>
      `SELECT ${base}${identityProjection}${snapshot} FROM ${from} ${whereClause} ${effectiveOrderBy} ${pagination.sql}`;
    const read = (snapshot?: string) =>
      this.readFallbackDataResult(
        driver,
        database,
        pageQuery(projection, snapshot),
        baseParams,
        columns,
        projection === "*" ? undefined : pageQuery("*", snapshot),
        assertExportSession,
        signal,
      );
    // E.g. a foreign relation cannot expose PostgreSQL heap system columns,
    // or an older server does not implement the snapshot function. Keep reads
    // and edits available, but never turn the fallback into key-only delete.
    let hasDeleteProjection = Boolean(deleteProjection);
    const dataResult = await read(deleteProjection).catch((error: unknown) => {
      if (!deleteProjection) throw error;
      signal?.throwIfAborted();
      hasDeleteProjection = false;
      return read();
    });
    signal?.throwIfAborted();
    const deleteColumnCount =
      hasDeleteProjection &&
      deleteProjectionColumns?.length &&
      deleteProjectionColumns.every(
        ({ alias }, index) =>
          dataResult.columns.at(index - deleteProjectionColumns.length) ===
          alias,
      )
        ? deleteProjectionColumns.length
        : 0;
    // Identity expressions are positional, not aliases that a table column
    // might shadow. Never include these values in displayed/exported rows.
    const displayColumnCount =
      dataResult.columns.length - identityColumns.length - deleteColumnCount;
    const fetchedRows = this.formatQueryRows(driver, columns, {
      ...dataResult,
      columns: dataResult.columns.slice(0, displayColumnCount),
    });
    const hasMoreRows = count.countFailed && fetchedRows.length > pageSize;
    const rows = hasMoreRows ? fetchedRows.slice(0, pageSize) : fetchedRows;
    const readIdentities = <T>(
      encode: (value: unknown) => T,
      includes: (column: ColumnTypeMeta) => boolean = () => true,
    ): Record<string, T>[] =>
      dataResult.rows
        .slice(0, rows.length)
        .map((row) =>
          Object.fromEntries(
            identityColumns.flatMap(({ column }, index) =>
              includes(column)
                ? [
                    [
                      column.name,
                      encode(row[`__col_${displayColumnCount + index}`]),
                    ],
                  ]
                : [],
            ),
          ),
        );
    const hasMssqlSqlVariantIdentities = identityColumns.some(
      ({ column }) => column.mssqlSqlVariantPrimaryKey,
    );
    const hasPostgresArrayIdentities = identityColumns.some(
      ({ column }) => column.postgresArrayPrimaryKey,
    );
    const hasPostgresIntervalIdentities = identityColumns.some(
      ({ column }) => column.postgresIntervalPrimaryKey,
    );
    const hasOracleTemporalIdentities = identityColumns.some(
      ({ column }) => column.oracleTemporalPrimaryKey,
    );
    const identities = hasMssqlSqlVariantIdentities
      ? {
          mssqlSqlVariantRowIdentities: readIdentities((value) => {
            const key = { $rapidbMssqlVariantKey: value };
            return isMssqlSqlVariantPrimaryKeyValue(key) ? key : null;
          }),
        }
      : hasPostgresArrayIdentities
        ? {
            postgresArrayRowIdentities: readIdentities(
              (value) =>
                typeof value === "string" ? { $rapidbPgArrayKey: value } : null,
              (column) => column.postgresArrayPrimaryKey === true,
            ),
          }
        : hasOracleTemporalIdentities
          ? {
              oracleTemporalRowIdentities: readIdentities((value) => {
                const key = { $rapidbOracleTemporalKey: value };
                return isOracleTemporalPrimaryKeyValue(key) ? key : null;
              }),
            }
          : identityColumns.length > 0 && !hasPostgresIntervalIdentities
            ? {
                sqliteRowIdentities: readIdentities((value) => {
                  const key = { $rapidbSqliteKey: value };
                  return isSQLitePrimaryKeyValue(key) ? key : null;
                }),
              }
            : {};
    const executionTimeMs = Math.round(performance.now() - startTime);

    return {
      columns,
      rows,
      ...identities,
      ...(hasPostgresIntervalIdentities
        ? {
            postgresIntervalRowIdentities: readIdentities(
              (value) => {
                const key = {
                  $rapidbPgIntervalKey: Buffer.isBuffer(value)
                    ? value.toString("hex")
                    : null,
                };
                return isPostgresIntervalPrimaryKeyValue(key) ? key : null;
              },
              (column) => !!column.postgresIntervalPrimaryKey,
            ),
          }
        : {}),
      ...(deleteSnapshot && deleteColumnCount > 0
        ? {
            sqlDeleteObservations: readSqlDeleteObservations(
              deleteSnapshot,
              dataResult.rows.slice(0, rows.length),
              dataResult.columns.length - deleteColumnCount,
            ),
          }
        : {}),
      totalCount: count.countFailed
        ? offset + rows.length + (hasMoreRows ? 1 : 0)
        : count.totalCount,
      executionTimeMs,
    };
  }

  private isArithmeticOverflowError(error: unknown): boolean {
    return /arithmetic overflow/i.test(this.readErrorMessage(error));
  }

  async *exportAll(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    chunkSize = 500,
    sort: SortConfig | null = null,
    filters: FilterExpression[] = [],
    signal?: AbortSignal,
    selectedColumns?: readonly string[],
    format?: DriverTableExportFormat,
  ): AsyncGenerator<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
    missingCells?: QueryMissingCell[];
  }> {
    const { driver } = this.getConnectionDriver(connectionId);
    let invalidated = false;
    const subscription =
      this.connectionManager.onDidInvalidateConnectionMetadata?.((id) => {
        if (id === connectionId) invalidated = true;
      });
    const assertSession = () => {
      signal?.throwIfAborted();
      if (
        invalidated ||
        this.connectionManager.getDriver(connectionId) !== driver
      ) {
        throw new Error(
          "Export connection changed. The export was stopped; retry using the current connection.",
        );
      }
    };
    try {
      assertSession();
      for await (const chunk of this.exportAllChunks(
        connectionId,
        database,
        schema,
        table,
        chunkSize,
        sort,
        filters,
        signal,
        selectedColumns,
        assertSession,
        format,
      )) {
        assertSession();
        yield chunk;
        assertSession();
      }
      assertSession();
    } finally {
      subscription?.dispose();
    }
  }

  private async *exportAllChunks(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    chunkSize: number,
    sort: SortConfig | null,
    filters: FilterExpression[],
    signal?: AbortSignal,
    selectedColumns?: readonly string[],
    assertSession?: () => void,
    format?: DriverTableExportFormat,
  ): AsyncGenerator<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
    missingCells?: QueryMissingCell[];
  }> {
    const { driver } = this.getConnectionDriver(connectionId);
    if (!Array.isArray(filters)) {
      throw new Error("[RapiDB Filter] Filters must be an array.");
    }
    if (driver.exportTableChunks) {
      if (filters.length > 0) {
        const columns = await this.getColumns(
          connectionId,
          database,
          schema,
          table,
        );
        assertSession?.();
        validateFilterExpressions(filters, columns);
      }
      assertSession?.();
      yield* driver.exportTableChunks(
        { database, schema, table, chunkSize, filters, sort },
        signal,
      );
      return;
    }
    const fallbackColumns = !driver.readTablePage
      ? await this.getColumns(connectionId, database, schema, table)
      : undefined;
    assertSession?.();
    if (fallbackColumns) {
      const keysetOrder = this.resolveKeysetExportOrder(fallbackColumns, sort);
      if (keysetOrder) {
        yield* this.exportAllWithKeysetPagination(
          connectionId,
          database,
          schema,
          table,
          chunkSize,
          filters,
          fallbackColumns,
          keysetOrder,
          signal,
          assertSession,
        );
        return;
      }
    }

    let page = 1;

    while (true) {
      assertSession?.();
      if (signal?.aborted) {
        throw new DOMException("Export cancelled by user", "AbortError");
      }

      let rowCount = 0;
      let emptyColumns: ColumnTypeMeta[] | undefined;
      const chunks =
        fallbackColumns && !driver.readTablePageChunks
          ? [
              await this.readFallbackPage(
                connectionId,
                database,
                schema,
                table,
                page,
                chunkSize,
                filters,
                sort,
                true,
                fallbackColumns,
                assertSession,
                signal,
              ),
            ]
          : this.getPageChunks(
              connectionId,
              database,
              schema,
              table,
              page,
              chunkSize,
              filters,
              sort,
              signal,
              assertSession,
            );
      for await (const chunk of chunks) {
        assertSession?.();
        if (chunk.rows.length === 0) {
          emptyColumns ??= chunk.columns;
          continue;
        }
        rowCount += chunk.rows.length;
        yield driver.formatTableExportChunk?.(chunk, selectedColumns, format) ??
          chunk;
      }

      if (rowCount === 0) {
        if (page === 1) {
          yield { columns: emptyColumns ?? [], rows: [] };
        }
        break;
      }

      if (rowCount < chunkSize) {
        break;
      }

      page += 1;
    }
  }

  private resolveFallbackOrderBy(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    columns: ColumnTypeMeta[],
    sort: SortConfig | null,
    tableReference?: string,
    rowIdentityOrder: readonly string[] = [],
  ): string {
    if (!sort) {
      const defaultOrderBy = driver.buildOrderByDefault(
        columns,
        driver.buildTableDataOrderExpression ? tableReference : undefined,
      );
      return defaultOrderBy || rowIdentityOrder.length === 0
        ? defaultOrderBy
        : `ORDER BY ${rowIdentityOrder.map((expression) => `${expression} ASC`).join(", ")}`;
    }
    const name = (columnName: string) =>
      tableReference && driver.buildTableDataOrderExpression
        ? driver.buildTableDataOrderExpression(columnName, tableReference)
        : driver.quoteIdentifier(columnName);
    const clauses = [
      `${name(sort.column)} ${sort.direction === "desc" ? "DESC" : "ASC"}`,
      ...columns
        .filter((column) => column.isPrimaryKey && column.name !== sort.column)
        .sort(
          (left, right) =>
            (left.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER) -
            (right.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER),
        )
        .map((column) => `${name(column.name)} ASC`),
      ...rowIdentityOrder
        .filter((expression) => expression !== name(sort.column))
        .map((expression) => `${expression} ASC`),
    ];
    return `ORDER BY ${clauses.join(", ")}`;
  }

  private async readFallbackTotalCount(
    connectionId: string,
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    database: string,
    from: string,
    whereClause: string,
    whereParams: unknown[],
    skipCount: boolean,
    signal?: AbortSignal,
  ): Promise<{ totalCount: number; countFailed: boolean }> {
    if (skipCount) {
      return { totalCount: 0, countFailed: false };
    }

    try {
      const countSql = `SELECT COUNT(*) AS cnt FROM ${from} ${whereClause}`;
      const countResult = await driver.query(countSql, whereParams, {
        database,
        ...this.getSQLiteAttachmentScope(),
        ...(signal ? { signal } : {}),
      });
      signal?.throwIfAborted();
      if (countResult.error) {
        throw new Error(countResult.error);
      }
      return {
        totalCount: this.readCountQueryValue(countResult),
        countFailed: false,
      };
    } catch (error: unknown) {
      signal?.throwIfAborted();
      if (error instanceof Error && error.message === SQLITE_ATTACHMENT_CHANGED)
        throw error;
      console.error(
        "[RapiDB] COUNT query failed, falling back to a lower-bound totalCount:",
        normalizeUnknownError(
          error,
          this.connectionManager.getConnectionDiagnosticContext?.(
            connectionId,
            driver,
          ),
        ).message,
      );
      return { totalCount: 0, countFailed: true };
    }
  }

  private readCountQueryValue(result: QueryResult): number {
    const countRow = result.rows[0] as Record<string, unknown> | undefined;
    return Number(
      countRow?.__col_0 ??
        countRow?.cnt ??
        countRow?.CNT ??
        countRow?.count ??
        0,
    );
  }

  private async readFallbackDataResult(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    database: string,
    dataSql: string,
    params: unknown[],
    columns: ColumnTypeMeta[],
    projectionFallbackSql?: string,
    assertExportSession?: () => void,
    signal?: AbortSignal,
  ): Promise<QueryResult> {
    try {
      assertExportSession?.();
      signal?.throwIfAborted();
      const result = await driver.query(dataSql, params, {
        database,
        ...this.getSQLiteAttachmentScope(),
        ...(signal ? { signal } : {}),
      });
      signal?.throwIfAborted();
      if (result.error) {
        throw new Error(result.error);
      }
      return result;
    } catch (error: unknown) {
      signal?.throwIfAborted();
      this.rethrowComputedColumnOverflow(error, columns);
      if (
        projectionFallbackSql &&
        driver.shouldFallbackTableDataProjection?.(error)
      ) {
        try {
          assertExportSession?.();
          signal?.throwIfAborted();
          const fallbackResult = await driver.query(
            projectionFallbackSql,
            params,
            {
              database,
              ...this.getSQLiteAttachmentScope(),
              ...(signal ? { signal } : {}),
            },
          );
          signal?.throwIfAborted();
          if (fallbackResult.error) {
            throw new Error(fallbackResult.error);
          }
          return fallbackResult;
        } catch (fallbackError: unknown) {
          this.rethrowComputedColumnOverflow(fallbackError, columns);
          throw fallbackError;
        }
      }
      throw error;
    }
  }

  private rethrowComputedColumnOverflow(
    error: unknown,
    columns: ColumnTypeMeta[],
  ): void {
    if (!this.isArithmeticOverflowError(error)) {
      return;
    }

    const computedColumns = columns
      .filter((column) => column.isComputed)
      .map((column) => column.name);
    if (computedColumns.length === 0) {
      return;
    }

    const message = this.readErrorMessage(error);
    throw new Error(
      `${message} (computed columns: ${computedColumns.join(", ")})`,
    );
  }

  private readErrorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error ?? "");
  }

  private formatQueryRows(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    columns: ColumnTypeMeta[],
    result: QueryResult,
  ): Record<string, unknown>[] {
    return result.rows.map((row) => {
      const projectionValues = Object.fromEntries(
        result.columns.map((columnName, index) => [
          columnName,
          row[`__col_${index}`],
        ]),
      );
      return Object.fromEntries(
        columns.map((column, columnIndex) => {
          const value = projectionValues[column.name];
          const formatted = driver.formatTableDataValue
            ? driver.formatTableDataValue(
                value,
                column,
                projectionValues,
                columnIndex,
                columns,
              )
            : driver.formatOutputValue(value, column);
          return [column.name, formatted];
        }),
      );
    });
  }

  private async *exportAllWithKeysetPagination(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    chunkSize: number,
    filters: FilterExpression[],
    columns: ColumnTypeMeta[],
    order: readonly ExportOrderColumn[],
    signal?: AbortSignal,
    assertSession?: () => void,
  ): AsyncGenerator<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
  }> {
    const { driver } = this.getConnectionDriver(connectionId);
    const qualifiedTableName = driver.qualifiedTableName(
      database,
      schema,
      table,
    );
    assertSession?.();
    const from = tableDataReference(driver, database, schema, table, columns);
    const orderByClause = this.buildOrderByClause(
      driver,
      order,
      qualifiedTableName,
    );
    const cursorExpressions = order.flatMap(({ column }) => {
      const expression = driver.buildCursorValueExpr?.(column);
      return expression ? [{ column, expression }] : [];
    });
    const displayNames = new Set(columns.map(({ name }) => name));
    let cursorAliasIndex = 0;
    const cursorProjection = cursorExpressions
      .map(({ expression }) => {
        let alias: string;
        do {
          alias = `__rapidb_cursor_${cursorAliasIndex++}`;
        } while (displayNames.has(alias));
        return `, ${expression} AS ${driver.quoteIdentifier(alias)}`;
      })
      .join("");
    let cursor: ExportCursor | null = null;

    while (true) {
      assertSession?.();
      if (signal?.aborted) {
        throw new DOMException("Export cancelled by user", "AbortError");
      }

      const { clause: baseWhereClause, params: baseWhereParams } = buildWhere(
        driver,
        filters,
        columns,
      );
      const cursorCondition = cursor
        ? this.buildCursorCondition(
            driver,
            order,
            cursor,
            baseWhereParams.length + 1,
          )
        : null;
      const whereClause = this.combineWhereClauses(
        baseWhereClause,
        cursorCondition?.clause,
      );
      const pagination = driver.buildPagination(
        0,
        chunkSize,
        baseWhereParams.length + (cursorCondition?.params.length ?? 0) + 1,
      );
      const projection = driver.buildTableDataProjection?.(columns) ?? "*";
      const sql = `SELECT ${projection}${cursorProjection} FROM ${from} ${whereClause} ${orderByClause} ${pagination.sql}`;
      const fallbackSql =
        projection === "*"
          ? undefined
          : `SELECT *${cursorProjection} FROM ${from} ${whereClause} ${orderByClause} ${pagination.sql}`;
      const params = [
        ...baseWhereParams,
        ...(cursorCondition?.params ?? []),
        ...pagination.params,
      ];
      const dataResult = await this.readFallbackDataResult(
        driver,
        database,
        sql,
        params,
        columns,
        fallbackSql,
        assertSession,
        signal,
      );
      const displayCount = dataResult.columns.length - cursorExpressions.length;
      const displayColumns = dataResult.columns.slice(0, displayCount);
      const rows = this.formatQueryRows(driver, columns, {
        ...dataResult,
        columns: displayColumns,
      });

      if (rows.length === 0) {
        if (cursor === null) {
          yield { columns, rows: [] };
        }
        break;
      }

      const lastRawRow = dataResult.rows[dataResult.rows.length - 1];
      const nextCursor = this.createCursorFromRow(
        Object.fromEntries([
          ...displayColumns.map((name, index) => [
            name,
            lastRawRow[`__col_${index}`],
          ]),
          ...cursorExpressions.map(({ column }, index) => [
            column.name,
            lastRawRow[`__col_${displayCount + index}`],
          ]),
        ]),
        order,
      );
      const previousCursor = cursor;
      if (
        previousCursor &&
        order.every(
          ({ column }) =>
            this.toCursorFilterValue(previousCursor[column.name]) ===
            this.toCursorFilterValue(nextCursor[column.name]),
        )
      ) {
        throw new Error(
          "Export cursor did not advance; export was stopped to avoid repeating rows.",
        );
      }
      yield { columns, rows };

      if (rows.length < chunkSize) break;
      cursor = nextCursor;
    }
  }

  private resolveKeysetExportOrder(
    columns: ColumnTypeMeta[],
    sort: SortConfig | null,
  ): ExportOrderColumn[] | null {
    const byName = new Map(columns.map((column) => [column.name, column]));
    const primaryKeys = columns
      .filter((column) => column.isPrimaryKey)
      .sort(
        (left, right) =>
          (left.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER) -
          (right.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER),
      );

    if (primaryKeys.length === 0) {
      return null;
    }

    const order: ExportOrderColumn[] = [];
    if (sort) {
      const sortedColumn = byName.get(sort.column);
      if (!sortedColumn || sortedColumn.nullable) {
        return null;
      }

      order.push({
        column: sortedColumn,
        direction: sort.direction,
      });
    }

    for (const primaryKeyColumn of primaryKeys) {
      if (!order.some((entry) => entry.column.name === primaryKeyColumn.name)) {
        order.push({ column: primaryKeyColumn, direction: "asc" });
      }
    }

    const canBuildCursor = order.every((entry) => {
      const operators = new Set(entry.column.filterOperators);
      return (
        entry.column.filterable &&
        !entry.column.nullable &&
        operators.has("eq") &&
        (entry.direction === "asc" ? operators.has("gt") : operators.has("lt"))
      );
    });

    return canBuildCursor ? order : null;
  }

  private buildOrderByClause(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    order: readonly ExportOrderColumn[],
    tableReference?: string,
  ): string {
    const clauses = order.map(
      (entry) =>
        `${tableReference && driver.buildTableDataOrderExpression ? driver.buildTableDataOrderExpression(entry.column.name, tableReference) : driver.quoteIdentifier(entry.column.name)} ${entry.direction === "desc" ? "DESC" : "ASC"}`,
    );
    return `ORDER BY ${clauses.join(", ")}`;
  }

  private createCursorFromRow(
    row: Record<string, unknown>,
    order: readonly ExportOrderColumn[],
  ): ExportCursor {
    return Object.fromEntries(
      order.map((entry) => [entry.column.name, row[entry.column.name]]),
    );
  }

  private buildCursorCondition(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    order: readonly ExportOrderColumn[],
    cursor: ExportCursor,
    paramIndex: number,
  ): { clause: string; params: unknown[] } {
    const disjunctionParts: string[] = [];
    const params: unknown[] = [];

    for (let index = 0; index < order.length; index += 1) {
      const prefixParts: string[] = [];
      for (let prefix = 0; prefix < index; prefix += 1) {
        const prefixColumn = order[prefix].column;
        prefixParts.push(
          `(${this.buildExactCursorComparison(
            driver,
            prefixColumn,
            "eq",
            cursor[prefixColumn.name],
            paramIndex,
            params,
          )})`,
        );
      }

      const current = order[index];
      const comparison = this.buildExactCursorComparison(
        driver,
        current.column,
        current.direction === "desc" ? "lt" : "gt",
        cursor[current.column.name],
        paramIndex,
        params,
      );
      const segment = [...prefixParts, `(${comparison})`].join(" AND ");
      disjunctionParts.push(`(${segment})`);
    }

    return {
      clause: disjunctionParts.join(" OR "),
      params,
    };
  }

  private buildExactCursorComparison(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    column: ColumnTypeMeta,
    operator: "eq" | "gt" | "lt",
    value: unknown,
    paramIndex: number,
    params: unknown[],
  ): string {
    if (value === null || value === undefined) {
      throw new Error(
        `Export cursor is missing a non-null value for ${column.name}.`,
      );
    }
    const index = paramIndex + params.length;
    const comparison = driver.buildCursorComparison
      ? driver.buildCursorComparison(column, operator, value, index)
      : {
          sql: `${driver.quoteIdentifier(column.name)} ${operator === "eq" ? "=" : operator === "gt" ? ">" : "<"} ${driver.buildInsertValueExpr(column, index)}`,
          params: [driver.coerceInputValue(value, column)],
        };
    params.push(...comparison.params);
    return comparison.sql;
  }

  private toCursorFilterValue(value: unknown): string | undefined {
    if (value === null || value === undefined) {
      return undefined;
    }

    if (value instanceof Date) {
      return value.toISOString();
    }

    return String(value);
  }

  private combineWhereClauses(
    baseWhereClause: string,
    cursorClause?: string,
  ): string {
    const trimmedCursorClause = cursorClause?.trim();
    if (!trimmedCursorClause) {
      return baseWhereClause;
    }

    if (!baseWhereClause) {
      return `WHERE (${trimmedCursorClause})`;
    }

    const normalizedBase = baseWhereClause.trim();
    const withoutWhere = /^where\s+/i.test(normalizedBase)
      ? normalizedBase.replace(/^where\s+/i, "")
      : normalizedBase;
    return `WHERE (${withoutWhere}) AND (${trimmedCursorClause})`;
  }

  private columnCacheKey(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): string {
    return [connectionId, database, schema, table]
      .map((value) => this.encodeKeyPart(value))
      .join("|");
  }

  private getConnectionDriver(connectionId: string): {
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>;
  } {
    const reconnectBlock =
      this.connectionManager.getAutomaticReconnectBlockReason?.(connectionId);
    if (reconnectBlock) throw new Error(reconnectBlock);
    const connection = this.connectionManager.getConnection(connectionId);
    const driver = this.connectionManager.getDriver(connectionId);
    if (!connection || !driver) {
      throw new Error(`[RapiDB] Not connected: ${connectionId}`);
    }

    return { driver };
  }
}
