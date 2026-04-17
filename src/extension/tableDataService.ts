import type { ConnectionManager } from "./connectionManager";
import { isoToLocalDateStr } from "./dbDrivers/BaseDBDriver";
import type {
  ColumnTypeMeta,
  IDBDriver,
  TransactionOperation,
} from "./dbDrivers/types";
import {
  DATE_ONLY_RE,
  DATETIME_SQL_RE,
  ISO_DATETIME_RE,
  NULL_SENTINEL,
} from "./dbDrivers/types";

// Re-export formatDatetimeForDisplay for consumers that imported it from here
export { formatDatetimeForDisplay } from "./dbDrivers/BaseDBDriver";

// Re-export legacy constants so existing consumers keep working
export { DATETIME_SQL_RE, ISO_DATETIME_RE } from "./dbDrivers/types";

// ─── Public types ───

export type ColumnDef = ColumnTypeMeta;

export interface Filter {
  column: string;
  value: string;
}

export interface SortConfig {
  column: string;
  direction: "asc" | "desc";
}

export interface TablePage {
  columns: ColumnDef[];
  rows: Record<string, unknown>[];
  totalCount: number;
}

export interface RowUpdate {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
}

export interface ApplyResult {
  success: boolean;
  error?: string;
  failedRows?: number[];
}

// ─── Service ───

export class TableDataService {
  private static readonly _colCache = new Map<string, ColumnDef[]>();

  constructor(private readonly cm: ConnectionManager) {}

  private colCacheKey(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): string {
    return `${connectionId}::${database}::${schema}::${table}`;
  }

  clearForConnection(connectionId: string): void {
    for (const key of TableDataService._colCache.keys()) {
      if (key.startsWith(`${connectionId}::`)) {
        TableDataService._colCache.delete(key);
      }
    }
  }

  private conn(id: string) {
    const cfg = this.cm.getConnection(id);
    const drv = this.cm.getDriver(id);
    if (!cfg || !drv) {
      throw new Error(`[RapiDB] Not connected: ${id}`);
    }
    return { cfg, drv };
  }

  async getColumns(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnDef[]> {
    const key = this.colCacheKey(connectionId, database, schema, table);
    const cached = TableDataService._colCache.get(key);
    if (cached) return cached;

    const { drv } = this.conn(connectionId);
    const cols = await drv.describeColumns(database, schema, table);
    TableDataService._colCache.set(key, cols);
    return cols;
  }

  async getPage(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    page: number,
    pageSize: number,
    filters: Filter[],
    sort: SortConfig | null = null,
    skipCount = false,
  ): Promise<TablePage> {
    const { drv } = this.conn(connectionId);
    const qt = drv.qualifiedTableName(database, schema, table);
    const cols = await this.getColumns(connectionId, database, schema, table);

    const { clause: where, params: whereParams } = buildWhere(
      drv,
      filters,
      cols,
    );
    const offset = (page - 1) * pageSize;

    const orderBy = sort
      ? `ORDER BY ${drv.quoteIdentifier(sort.column)} ${sort.direction === "desc" ? "DESC" : "ASC"}`
      : drv.buildOrderByDefault(cols);

    let totalCount = 0;
    if (!skipCount) {
      try {
        const countSql = `SELECT COUNT(*) AS cnt FROM ${qt} ${where}`;
        const countRes = await drv.query(countSql, whereParams);
        const countRow = countRes.rows[0] as
          | Record<string, unknown>
          | undefined;
        totalCount = Number(
          countRow?.__col_0 ??
            countRow?.cnt ??
            countRow?.CNT ??
            countRow?.count ??
            0,
        );
      } catch (err: any) {
        console.error(
          "[RapiDB] COUNT query failed, totalCount will be 0:",
          err?.message ?? err,
        );
      }
    }

    const paramIndex = whereParams.length + 1;
    const pag = drv.buildPagination(offset, pageSize, paramIndex);
    const effectiveOrderBy = orderBy || drv.buildOrderByDefault(cols);

    const dataSql = `SELECT * FROM ${qt} ${where} ${effectiveOrderBy} ${pag.sql}`;
    const dataParams = [...whereParams, ...pag.params];

    const dataRes = await drv.query(dataSql, dataParams);
    const dataColumns = dataRes.columns;

    const colMetaMap = new Map(cols.map((c) => [c.name, c]));

    const formattedRows = dataRes.rows.map((row) => {
      const newRow: Record<string, unknown> = {};
      dataColumns.forEach((colName, i) => {
        const val = row[`__col_${i}`];
        const meta = colMetaMap.get(colName);
        newRow[colName] = meta ? drv.formatOutputValue(val, meta) : val;
      });
      return newRow;
    });

    return { columns: cols, rows: formattedRows, totalCount };
  }

  async updateRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    pkValues: Record<string, unknown>,
    changes: Record<string, unknown>,
  ): Promise<void> {
    const { drv } = this.conn(connectionId);
    const cols = await this.getColumns(connectionId, database, schema, table);

    const op = buildUpdateRowSql(
      drv,
      database,
      schema,
      table,
      pkValues,
      changes,
      cols,
    );
    if (!op) return;

    const result = await drv.query(op.sql, op.params);
    const affectedRows = result.affectedRows ?? result.rowCount;
    if (affectedRows === 0) {
      throw new Error(
        "Row not found — the row may have been modified or deleted by another user",
      );
    }
  }

  async insertRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    values: Record<string, unknown>,
  ): Promise<void> {
    const { drv } = this.conn(connectionId);
    const qt = drv.qualifiedTableName(database, schema, table);
    const cols = await this.getColumns(connectionId, database, schema, table);
    const colMap = new Map(cols.map((c) => [c.name, c]));

    const entries = writableEntries(values, colMap);
    if (entries.length === 0) {
      throw new Error(
        "Insert failed: no values provided. Fill in at least one field or explicitly set a field to NULL.",
      );
    }

    const colNames: string[] = [];
    const valExprs: string[] = [];
    const params: unknown[] = [];

    for (let i = 0; i < entries.length; i++) {
      const [colName, rawVal] = entries[i];
      const meta = colMap.get(colName);
      if (!meta) continue;
      colNames.push(drv.quoteIdentifier(colName));
      valExprs.push(drv.buildInsertValueExpr(meta, params.length + 1));
      params.push(drv.coerceInputValue(rawVal, meta));
    }

    const result = await drv.query(
      `INSERT INTO ${qt} (${colNames.join(", ")}) VALUES (${valExprs.join(", ")})`,
      params,
    );

    const affected = result.affectedRows ?? result.rowCount;
    if (affected !== undefined && affected === 0) {
      throw new Error(
        "Insert failed: the database reported 0 rows affected. " +
          "The row may have been rejected by a trigger or constraint.",
      );
    }
  }

  async deleteRows(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    pkValuesList: Record<string, unknown>[],
  ): Promise<void> {
    if (pkValuesList.length === 0) return;
    const { drv } = this.conn(connectionId);
    const qt = drv.qualifiedTableName(database, schema, table);
    const cols = await this.getColumns(connectionId, database, schema, table);
    const colMap = new Map(cols.map((c) => [c.name, c]));

    const coercedList = pkValuesList
      .map((raw) => coerceRecord(drv, raw, colMap))
      .filter((r) => Object.keys(r).length > 0);

    if (coercedList.length === 0) return;

    const firstPkCols = Object.keys(coercedList[0]);
    const isSinglePk =
      firstPkCols.length === 1 &&
      coercedList.every(
        (r) =>
          Object.keys(r).length === 1 && Object.keys(r)[0] === firstPkCols[0],
      );

    if (isSinglePk) {
      const pkCol = firstPkCols[0];
      const values = coercedList.map((r) => r[pkCol]);
      const CHUNK = 1000;

      for (let i = 0; i < values.length; i += CHUNK) {
        const chunk = values.slice(i, i + CHUNK);
        const placeholders = chunk
          .map((_, j) => {
            const meta = colMap.get(pkCol);
            return meta ? drv.buildInsertValueExpr(meta, j + 1) : "?";
          })
          .join(", ");
        await drv.query(
          `DELETE FROM ${qt} WHERE ${drv.quoteIdentifier(pkCol)} IN (${placeholders})`,
          chunk,
        );
      }
      return;
    }

    const operations: TransactionOperation[] = [];

    for (const pkValues of coercedList) {
      const pkCols = Object.keys(pkValues);
      const params: unknown[] = [];
      const whereParts = pkCols.map((c) => {
        params.push(pkValues[c]);
        const meta = colMap.get(c);
        const placeholder = meta
          ? drv.buildInsertValueExpr(meta, params.length)
          : "?";
        return `${drv.quoteIdentifier(c)} = ${placeholder}`;
      });

      operations.push({
        sql: `DELETE FROM ${qt} WHERE ${whereParts.join(" AND ")}`,
        params,
      });
    }

    await drv.runTransaction(operations);
  }

  async *exportAll(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    chunkSize = 500,
    sort: SortConfig | null = null,
    filters: Filter[] = [],
    signal?: AbortSignal,
  ): AsyncGenerator<{ columns: ColumnDef[]; rows: Record<string, unknown>[] }> {
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
      if (result.rows.length === 0) break;
      yield { columns: result.columns, rows: result.rows };
      if (result.rows.length < chunkSize) break;
      page++;
    }
  }
}

// ─── Internal helpers ───

function buildWhere(
  drv: IDBDriver,
  filters: Filter[],
  cols: ColumnDef[],
): { clause: string; params: unknown[] } {
  if (filters.length === 0) return { clause: "", params: [] };

  const colMap = new Map(cols.map((c) => [c.name, c]));
  const params: unknown[] = [];
  const conditions: string[] = [];

  for (const f of filters) {
    const val = f.value.trim();
    if (val === "") continue;
    const meta = colMap.get(f.column);
    if (!meta) continue;

    const result = inferFilterCondition(drv, meta, val, params.length + 1);
    if (result) {
      conditions.push(result.sql);
      params.push(...result.params);
    }
  }

  if (conditions.length === 0) return { clause: "", params: [] };
  return { clause: `WHERE ${conditions.join(" AND ")}`, params };
}

function coerceRecord(
  drv: IDBDriver,
  record: Record<string, unknown>,
  colMap: Map<string, ColumnDef>,
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(record).map(([k, v]) => {
      const meta = colMap.get(k);
      return [k, meta ? drv.coerceInputValue(v, meta) : v];
    }),
  );
}

function writableEntries(
  values: Record<string, unknown>,
  colMap: Map<string, ColumnDef>,
): Array<[string, unknown]> {
  return Object.entries(values).filter(([columnName, value]) => {
    if (value === undefined) return false;
    return isWritableColumn(colMap.get(columnName));
  });
}

function filterWritableRecord(
  record: Record<string, unknown>,
  colMap: Map<string, ColumnDef>,
): Record<string, unknown> {
  return Object.fromEntries(writableEntries(record, colMap));
}

function isWritableColumn(column: ColumnDef | undefined): column is ColumnDef {
  return !!column && column.editable && !column.isAutoIncrement;
}

function inferFilterCondition(
  drv: IDBDriver,
  column: ColumnDef,
  rawValue: string,
  paramIndex: number,
) {
  if (!column.filterable) return null;

  const value = rawValue.trim();
  if (value === "") return null;
  if (value === NULL_SENTINEL) {
    return drv.buildFilterCondition(column, "is_null", value, paramIndex);
  }

  if (column.isBoolean) {
    const normalized = normalizeBooleanFilterValue(value);
    if (!normalized) {
      throw invalidFilterInputError(column.name, "true or false");
    }
    return drv.buildFilterCondition(column, "eq", normalized, paramIndex);
  }

  if (isNumericCategory(column.category)) {
    const numericValue = Number(value);
    if (Number.isNaN(numericValue) || !Number.isFinite(numericValue)) {
      throw invalidFilterInputError(column.name, "a number");
    }
    return drv.buildFilterCondition(column, "eq", value, paramIndex);
  }

  if (column.category === "date") {
    const normalizedDate = normalizeDateFilterValue(value);
    if (normalizedDate) {
      return drv.buildFilterCondition(column, "eq", normalizedDate, paramIndex);
    }
    if (looksLikeDateInput(value)) {
      throw invalidFilterInputError(column.name, "a valid date");
    }
    return drv.buildFilterCondition(column, "like", value, paramIndex);
  }

  if (column.category === "time" || column.category === "datetime") {
    return drv.buildFilterCondition(column, "like", value, paramIndex);
  }

  return drv.buildFilterCondition(column, "like", value, paramIndex);
}

function normalizeBooleanFilterValue(value: string): "true" | "false" | null {
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return "true";
  if (normalized === "false" || normalized === "0") return "false";
  return null;
}

function isNumericCategory(category: ColumnDef["category"]): boolean {
  return (
    category === "integer" || category === "float" || category === "decimal"
  );
}

function normalizeDateFilterValue(value: string): string | null {
  const normalized = value.trim();
  const normalizedSql = normalizeSqlDatetimeOffsetSpacing(normalized);
  if (DATE_ONLY_RE.test(normalized)) {
    return isValidDateOnly(normalized) ? normalized : null;
  }
  if (ISO_DATETIME_RE.test(normalized)) {
    if (!hasValidDateTimeParts(normalized)) {
      return null;
    }
    if (!hasExplicitTimezone(normalized)) {
      const dateOnly = normalized.slice(0, 10);
      return isValidDateOnly(dateOnly) ? dateOnly : null;
    }
    return isoToLocalDateStr(normalized);
  }
  if (DATETIME_SQL_RE.test(normalizedSql)) {
    if (!hasValidDateTimeParts(normalizedSql)) {
      return null;
    }
    if (hasExplicitTimezone(normalizedSql)) {
      return isoToLocalDateStr(normalizedSql.replace(" ", "T"));
    }
    const dateOnly = normalizedSql.slice(0, 10);
    return isValidDateOnly(dateOnly) ? dateOnly : null;
  }
  return null;
}

function looksLikeDateInput(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(value.trim());
}

function hasExplicitTimezone(value: string): boolean {
  return /[zZ]|[+-]\d{2}:\d{2}$/.test(value);
}

function normalizeSqlDatetimeOffsetSpacing(value: string): string {
  return value.replace(/ ([+-]\d{2}:\d{2})$/, "$1");
}

function isValidDateOnly(value: string): boolean {
  if (!DATE_ONLY_RE.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return false;

  const [year, month, day] = value.split("-").map(Number);
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() + 1 === month &&
    parsed.getUTCDate() === day
  );
}

function hasValidDateTimeParts(value: string): boolean {
  const match =
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?: ?(?:Z|[+-]\d{2}:\d{2}))?$/i.exec(
      value,
    );
  if (!match) return false;

  const [, date, rawHours, rawMinutes, rawSeconds] = match;
  const hours = Number(rawHours);
  const minutes = Number(rawMinutes);
  const seconds = Number(rawSeconds);

  return isValidDateOnly(date) && hours < 24 && minutes < 60 && seconds < 60;
}

function invalidFilterInputError(columnName: string, expected: string): Error {
  return new Error(`[RapiDB Filter] Column ${columnName} expects ${expected}.`);
}

function buildUpdateRowSql(
  drv: IDBDriver,
  database: string,
  schema: string,
  table: string,
  pkValues: Record<string, unknown>,
  changes: Record<string, unknown>,
  cols: ColumnDef[],
): { sql: string; params: unknown[] } | null {
  const qt = drv.qualifiedTableName(database, schema, table);
  const colMap = new Map(cols.map((c) => [c.name, c]));

  const coercedChanges = coerceRecord(
    drv,
    filterWritableRecord(changes, colMap),
    colMap,
  );
  const coercedPk = coerceRecord(drv, pkValues, colMap);

  const setCols = Object.keys(coercedChanges);
  const pkCols = Object.keys(coercedPk);
  if (setCols.length === 0 || pkCols.length === 0) return null;

  const params: unknown[] = [];

  const setParts = setCols.map((c) => {
    params.push(coercedChanges[c]);
    const meta = colMap.get(c);
    return meta
      ? drv.buildSetExpr(meta, params.length)
      : `${drv.quoteIdentifier(c)} = ?`;
  });

  const whereParts = pkCols.map((c) => {
    params.push(coercedPk[c]);
    const meta = colMap.get(c);
    const placeholder = meta
      ? drv.buildInsertValueExpr(meta, params.length)
      : "?";
    return `${drv.quoteIdentifier(c)} = ${placeholder}`;
  });

  return {
    sql: `UPDATE ${qt} SET ${setParts.join(", ")} WHERE ${whereParts.join(" AND ")}`,
    params,
  };
}

export async function applyChangesTransactional(
  cm: ConnectionManager,
  connectionId: string,
  database: string,
  schema: string,
  table: string,
  updates: RowUpdate[],
  cols: ColumnDef[],
): Promise<ApplyResult> {
  if (updates.length === 0) return { success: true };

  const driver = cm.getDriver(connectionId);
  if (!driver) return { success: false, error: "Not connected" };

  const operations: TransactionOperation[] = [];

  for (const { primaryKeys, changes } of updates) {
    const op = buildUpdateRowSql(
      driver,
      database,
      schema,
      table,
      primaryKeys,
      changes,
      cols,
    );
    if (op) {
      operations.push({
        sql: op.sql,
        params: op.params,
        checkAffectedRows: true,
      });
    }
  }

  if (operations.length === 0) return { success: true };

  try {
    await driver.runTransaction(operations);
    return { success: true };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
