import type { ConnectionManager } from "../connectionManager";
import type {
  ColumnTypeMeta,
  FilterExpression,
  QueryResult,
} from "../dbDrivers/types";
import { buildWhere } from "./filterSql";
import type { SortConfig, TablePage } from "./tableDataContracts";

type ExportOrderColumn = {
  column: ColumnTypeMeta;
  direction: "asc" | "desc";
};

type ExportCursor = Record<string, unknown>;

export class TableReadService {
  private readonly columnCache = new Map<string, ColumnTypeMeta[]>();

  constructor(private readonly connectionManager: ConnectionManager) {}

  clearForConnection(connectionId: string): void {
    for (const key of this.columnCache.keys()) {
      if (key.startsWith(`${connectionId}::`)) {
        this.columnCache.delete(key);
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

    const { driver } = this.getConnectionDriver(connectionId);
    const columns = await driver.describeColumns(database, schema, table);
    this.columnCache.set(cacheKey, columns);
    return columns;
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
  ): Promise<TablePage> {
    const { driver } = this.getConnectionDriver(connectionId);
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
    );
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
  ): Promise<TablePage> {
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

    const { clause: whereClause, params: whereParams } = buildWhere(
      driver,
      filters,
      columns,
    );
    const offset = (page - 1) * pageSize;
    const orderByClause = this.resolveFallbackOrderBy(driver, columns, sort);
    const count = await this.readFallbackTotalCount(
      driver,
      qualifiedTableName,
      whereClause,
      whereParams,
      skipCount,
    );
    const pagination = driver.buildPagination(
      offset,
      pageSize,
      whereParams.length + 1,
    );
    const effectiveOrderBy =
      orderByClause || driver.buildOrderByDefault(columns);
    const baseParams = [...whereParams, ...pagination.params];
    const dataSql = `SELECT * FROM ${qualifiedTableName} ${whereClause} ${effectiveOrderBy} ${pagination.sql}`;
    const dataResult = await this.readFallbackDataResult(
      driver,
      dataSql,
      baseParams,
      columns,
    );
    const rows = this.formatQueryRows(driver, columns, dataResult);

    return {
      columns,
      rows,
      totalCount: count.countFailed ? offset + rows.length : count.totalCount,
    };
  }

  private isArithmeticOverflowError(error: unknown): boolean {
    const message =
      error instanceof Error ? error.message : String(error ?? "");
    return /arithmetic overflow/i.test(message);
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
  ): AsyncGenerator<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
  }> {
    const { driver } = this.getConnectionDriver(connectionId);
    if (!driver.readTablePage) {
      const keysetOrder = await this.resolveKeysetExportOrder(
        connectionId,
        database,
        schema,
        table,
        sort,
      );
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
        );
        return;
      }
    }

    let page = 1;

    while (true) {
      if (signal?.aborted) {
        throw new DOMException("Export cancelled by user", "AbortError");
      }

      const result = await this.getPage(
        connectionId,
        database,
        schema,
        table,
        page,
        chunkSize,
        filters,
        sort,
        true,
      );

      if (result.rows.length === 0) {
        break;
      }

      yield { columns: result.columns, rows: result.rows };

      if (result.rows.length < chunkSize) {
        break;
      }

      page += 1;
    }
  }

  private resolveFallbackOrderBy(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    columns: ColumnTypeMeta[],
    sort: SortConfig | null,
  ): string {
    return sort
      ? `ORDER BY ${driver.quoteIdentifier(sort.column)} ${sort.direction === "desc" ? "DESC" : "ASC"}`
      : driver.buildOrderByDefault(columns);
  }

  private async readFallbackTotalCount(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    qualifiedTableName: string,
    whereClause: string,
    whereParams: unknown[],
    skipCount: boolean,
  ): Promise<{ totalCount: number; countFailed: boolean }> {
    if (skipCount) {
      return { totalCount: 0, countFailed: false };
    }

    try {
      const countSql = `SELECT COUNT(*) AS cnt FROM ${qualifiedTableName} ${whereClause}`;
      const countResult = await driver.query(countSql, whereParams);
      return {
        totalCount: this.readCountQueryValue(countResult),
        countFailed: false,
      };
    } catch (error: unknown) {
      console.error(
        "[RapiDB] COUNT query failed, falling back to a lower-bound totalCount:",
        error instanceof Error ? error.message : error,
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
    dataSql: string,
    params: unknown[],
    columns: ColumnTypeMeta[],
  ): Promise<QueryResult> {
    try {
      return await driver.query(dataSql, params);
    } catch (error: unknown) {
      this.rethrowComputedColumnOverflow(error, columns);
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

    const message =
      error instanceof Error ? error.message : String(error ?? "");
    throw new Error(
      `${message} (computed columns: ${computedColumns.join(", ")})`,
    );
  }

  private formatQueryRows(
    driver: ReturnType<TableReadService["getConnectionDriver"]>["driver"],
    columns: ColumnTypeMeta[],
    result: QueryResult,
  ): Record<string, unknown>[] {
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );

    return result.rows.map((row) => {
      const formattedRow: Record<string, unknown> = {};
      result.columns.forEach((columnName, index) => {
        const value = row[`__col_${index}`];
        const column = columnMetaByName.get(columnName);
        formattedRow[columnName] = column
          ? driver.formatOutputValue(value, column)
          : value;
      });
      return formattedRow;
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
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const orderByClause = this.buildOrderByClause(driver, order);
    let cursor: ExportCursor | null = null;

    while (true) {
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
      const sql = `SELECT * FROM ${qualifiedTableName} ${whereClause} ${orderByClause} ${pagination.sql}`;
      const params = [
        ...baseWhereParams,
        ...(cursorCondition?.params ?? []),
        ...pagination.params,
      ];
      const dataResult = await driver.query(sql, params);
      const rows = dataResult.rows.map((row) => {
        const formattedRow: Record<string, unknown> = {};
        dataResult.columns.forEach((columnName, index) => {
          const value = row[`__col_${index}`];
          const column = columnMetaByName.get(columnName);
          formattedRow[columnName] = column
            ? driver.formatOutputValue(value, column)
            : value;
        });
        return formattedRow;
      });

      if (rows.length === 0) {
        break;
      }

      yield { columns, rows };

      if (rows.length < chunkSize) {
        break;
      }

      cursor = this.createCursorFromRow(rows[rows.length - 1], order);
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
  ): string {
    const clauses = order.map(
      (entry) =>
        `${driver.quoteIdentifier(entry.column.name)} ${entry.direction === "desc" ? "DESC" : "ASC"}`,
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
        const prefixValue = this.toCursorFilterValue(cursor[prefixColumn.name]);
        const equality = driver.buildFilterCondition(
          prefixColumn,
          "eq",
          prefixValue,
          paramIndex + params.length,
        );
        if (!equality) {
          continue;
        }
        prefixParts.push(`(${equality.sql})`);
        params.push(...equality.params);
      }

      const current = order[index];
      const currentValue = this.toCursorFilterValue(
        cursor[current.column.name],
      );
      const operator = current.direction === "desc" ? "lt" : "gt";
      const comparison = driver.buildFilterCondition(
        current.column,
        operator,
        currentValue,
        paramIndex + params.length,
      );
      if (!comparison) {
        continue;
      }

      params.push(...comparison.params);
      const segment = [...prefixParts, `(${comparison.sql})`].join(" AND ");
      disjunctionParts.push(`(${segment})`);
    }

    return {
      clause: disjunctionParts.join(" OR "),
      params,
    };
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
    return `${connectionId}::${database}::${schema}::${table}`;
  }

  private getConnectionDriver(connectionId: string): {
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>;
  } {
    const connection = this.connectionManager.getConnection(connectionId);
    const driver = this.connectionManager.getDriver(connectionId);
    if (!connection || !driver) {
      throw new Error(`[RapiDB] Not connected: ${connectionId}`);
    }

    return { driver };
  }
}
