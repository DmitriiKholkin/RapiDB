import type { QueryMissingCell } from "../../shared/tableTypes";
import type { ConnectionManager } from "../connectionManager";
import type {
  ColumnTypeMeta,
  DriverTableExportChunk,
  FilterExpression,
  QueryResult,
} from "../dbDrivers/types";
import { normalizeUnknownError } from "../utils/errorHandling";
import { buildWhere, validateFilterExpressions } from "./filterSql";
import type { SortConfig, TablePage } from "./tableDataContracts";

type ExportOrderColumn = {
  column: ColumnTypeMeta;
  direction: "asc" | "desc";
};

type ExportCursor = Record<string, unknown>;

export class TableReadService {
  private readonly columnCache = new Map<string, ColumnTypeMeta[]>();
  private readonly columnCacheGenerations = new Map<string, number>();
  private readonly rowIdentityOrderCache = new Map<string, string[]>();

  private encodeKeyPart(value: string): string {
    return `${value.length}:${value}`;
  }

  constructor(private readonly connectionManager: ConnectionManager) {}

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
    const columns = await driver.describeColumns(database, schema, table);
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
        normalizeUnknownError(error).message,
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
  ): Promise<TablePage> {
    assertExportSession?.();
    signal?.throwIfAborted();
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
    if (columns) {
      validateFilterExpressions(filters, columns);
    }

    if (driver.readTablePage) {
      return driver.readTablePage({
        database,
        schema,
        table,
        page,
        pageSize,
        filters,
        sort,
        skipCount,
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
      driver,
      database,
      qualifiedTableName,
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
    const dataSql = `SELECT ${projection} FROM ${qualifiedTableName} ${whereClause} ${effectiveOrderBy} ${pagination.sql}`;
    const fallbackSql =
      projection === "*"
        ? undefined
        : `SELECT * FROM ${qualifiedTableName} ${whereClause} ${effectiveOrderBy} ${pagination.sql}`;
    const dataResult = await this.readFallbackDataResult(
      driver,
      database,
      dataSql,
      baseParams,
      columns,
      fallbackSql,
      assertExportSession,
      signal,
    );
    signal?.throwIfAborted();
    const fetchedRows = this.formatQueryRows(driver, columns, dataResult);
    const hasMoreRows = count.countFailed && fetchedRows.length > pageSize;
    const rows = hasMoreRows ? fetchedRows.slice(0, pageSize) : fetchedRows;
    const executionTimeMs = Math.round(performance.now() - startTime);

    return {
      columns,
      rows,
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
  ): AsyncGenerator<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
    missingCells?: QueryMissingCell[];
  }> {
    const { driver } = this.getConnectionDriver(connectionId);
    if (driver.exportTableChunks) {
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
    if (!driver.readTablePage) {
      const keysetOrder = await this.resolveKeysetExportOrder(
        connectionId,
        database,
        schema,
        table,
        sort,
      );
      assertSession?.();
      if (keysetOrder) {
        yield* this.exportAllWithKeysetPagination(
          connectionId,
          database,
          schema,
          table,
          chunkSize,
          filters,
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
      for await (const chunk of this.getPageChunks(
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
      )) {
        assertSession?.();
        if (chunk.rows.length === 0) {
          emptyColumns ??= chunk.columns;
          continue;
        }
        rowCount += chunk.rows.length;
        yield driver.formatTableExportChunk?.(chunk, selectedColumns) ?? chunk;
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
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    database: string,
    qualifiedTableName: string,
    whereClause: string,
    whereParams: unknown[],
    skipCount: boolean,
    signal?: AbortSignal,
  ): Promise<{ totalCount: number; countFailed: boolean }> {
    if (skipCount) {
      return { totalCount: 0, countFailed: false };
    }

    try {
      const countSql = `SELECT COUNT(*) AS cnt FROM ${qualifiedTableName} ${whereClause}`;
      const countResult = await driver.query(countSql, whereParams, {
        database,
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
      console.error(
        "[RapiDB] COUNT query failed, falling back to a lower-bound totalCount:",
        normalizeUnknownError(error).message,
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
    const columns = await this.getColumns(
      connectionId,
      database,
      schema,
      table,
    );
    assertSession?.();
    const orderByClause = this.buildOrderByClause(
      driver,
      order,
      qualifiedTableName,
    );
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
      const sql = `SELECT ${projection} FROM ${qualifiedTableName} ${whereClause} ${orderByClause} ${pagination.sql}`;
      const fallbackSql =
        projection === "*"
          ? undefined
          : `SELECT * FROM ${qualifiedTableName} ${whereClause} ${orderByClause} ${pagination.sql}`;
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
      const rows = this.formatQueryRows(driver, columns, dataResult);

      if (rows.length === 0) {
        if (cursor === null) {
          yield { columns, rows: [] };
        }
        break;
      }

      const lastRawRow = dataResult.rows[dataResult.rows.length - 1];
      const nextCursor = this.createCursorFromRow(
        Object.fromEntries(
          dataResult.columns.map((name, index) => [
            name,
            lastRawRow[`__col_${index}`],
          ]),
        ),
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

  private async resolveKeysetExportOrder(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    sort: SortConfig | null,
  ): Promise<ExportOrderColumn[] | null> {
    const columns = await this.getColumns(
      connectionId,
      database,
      schema,
      table,
    );
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
