import type { ConnectionManager } from "../connectionManager";
import type { ColumnTypeMeta, FilterExpression } from "../dbDrivers/types";
import { buildWhere } from "./filterSql";
import type { SortConfig, TablePage } from "./tableDataContracts";

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

    const orderByClause = sort
      ? `ORDER BY ${driver.quoteIdentifier(sort.column)} ${sort.direction === "desc" ? "DESC" : "ASC"}`
      : driver.buildOrderByDefault(columns);

    let totalCount = 0;
    if (!skipCount) {
      try {
        const countSql = `SELECT COUNT(*) AS cnt FROM ${qualifiedTableName} ${whereClause}`;
        const countResult = await driver.query(countSql, whereParams);
        const countRow = countResult.rows[0] as
          | Record<string, unknown>
          | undefined;
        totalCount = Number(
          countRow?.__col_0 ??
            countRow?.cnt ??
            countRow?.CNT ??
            countRow?.count ??
            0,
        );
      } catch (error: unknown) {
        console.error(
          "[RapiDB] COUNT query failed, totalCount will be 0:",
          error instanceof Error ? error.message : error,
        );
      }
    }

    const pagination = driver.buildPagination(
      offset,
      pageSize,
      whereParams.length + 1,
    );
    const effectiveOrderBy =
      orderByClause || driver.buildOrderByDefault(columns);
    const dataSql = `SELECT * FROM ${qualifiedTableName} ${whereClause} ${effectiveOrderBy} ${pagination.sql}`;
    const dataResult = await driver.query(dataSql, [
      ...whereParams,
      ...pagination.params,
    ]);
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );

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

    return { columns, rows, totalCount };
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
