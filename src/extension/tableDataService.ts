import type {
  ApplyResultPayload,
  ApplyRowOutcome,
} from "../shared/webviewContracts";
import type { ConnectionManager } from "./connectionManager";
import type {
  ColumnTypeMeta,
  FilterExpression,
  TransactionOperation,
} from "./dbDrivers/types";
import { buildWhere } from "./table/filterSql";
import { buildInsertRowOperation } from "./table/insertSql";
import { buildUpdateRowSql, coerceRecord } from "./table/updateSql";

// Re-export formatDatetimeForDisplay for consumers that imported it from here
export { formatDatetimeForDisplay } from "./dbDrivers/BaseDBDriver";

// ─── Public types ───

export type ColumnDef = ColumnTypeMeta;
export type Filter = FilterExpression;

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

export type ApplyResult = ApplyResultPayload;

export interface PreparedInsertPlan {
  connectionId: string;
  operation: TransactionOperation;
  previewStatements: string[];
}

export interface PreparedApplyPlan {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  cols: ColumnDef[];
  updates: RowUpdate[];
  operations: TransactionOperation[];
  previewStatements: string[];
  skippedRows: number[];
  verificationTargets: VerificationTarget[];
}

export type PreparedApplyPlanResult =
  | {
      executable: false;
      result: ApplyResult;
    }
  | {
      executable: true;
      plan: PreparedApplyPlan;
    };

interface VerificationTarget {
  rowIndex: number;
  primaryKeys: Record<string, unknown>;
  values: Array<{
    column: ColumnDef;
    expectedValue: unknown;
  }>;
}

interface VerificationFailure {
  rowIndex: number;
  columns: string[];
  message: string;
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
      } catch (err: unknown) {
        console.error(
          "[RapiDB] COUNT query failed, totalCount will be 0:",
          err instanceof Error ? err.message : err,
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
    const plan = await this.prepareInsertRow(
      connectionId,
      database,
      schema,
      table,
      values,
    );
    await this.executePreparedInsertPlan(plan);
  }

  async prepareInsertRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    values: Record<string, unknown>,
  ): Promise<PreparedInsertPlan> {
    const { drv } = this.conn(connectionId);
    const cols = await this.getColumns(connectionId, database, schema, table);
    const operation = buildInsertRowOperation(
      drv,
      database,
      schema,
      table,
      values,
      cols,
    );

    return {
      connectionId,
      operation,
      previewStatements: [
        drv.materializePreviewSql(operation.sql, operation.params),
      ],
    };
  }

  async executePreparedInsertPlan(plan: PreparedInsertPlan): Promise<void> {
    const { drv } = this.conn(plan.connectionId);

    const result = await drv.query(plan.operation.sql, plan.operation.params);

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

function summarizeOutcomeMessages(
  prefix: string,
  outcomes: ApplyRowOutcome[],
): string {
  const details = outcomes
    .slice(0, 2)
    .map(
      (outcome) =>
        `Row ${outcome.rowIndex + 1}: ${outcome.message ?? "Unknown issue"}`,
    )
    .join(" ");
  const suffix =
    outcomes.length > 2
      ? ` ${outcomes.length - 2} more row(s) had the same issue.`
      : "";
  return `${prefix} ${details}${suffix}`.trim();
}

function buildSkippedOutcome(
  rowIndex: number,
  message: string,
  success = true,
): ApplyRowOutcome {
  return {
    rowIndex,
    success,
    status: "skipped",
    message,
  };
}

async function verifyExactNumericUpdates(
  driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
  database: string,
  schema: string,
  table: string,
  cols: ColumnDef[],
  targets: VerificationTarget[],
): Promise<VerificationFailure[]> {
  const qt = driver.qualifiedTableName(database, schema, table);
  const colMap = new Map(cols.map((col) => [col.name, col]));
  const failures: VerificationFailure[] = [];

  for (const target of targets) {
    if (target.values.length === 0) {
      continue;
    }

    try {
      const params: unknown[] = [];
      const whereParts = Object.entries(target.primaryKeys).map(
        ([columnName, rawValue]) => {
          const meta = colMap.get(columnName);
          params.push(
            meta ? driver.coerceInputValue(rawValue, meta) : rawValue,
          );
          const placeholder = meta
            ? driver.buildInsertValueExpr(meta, params.length)
            : "?";
          return `${driver.quoteIdentifier(columnName)} = ${placeholder}`;
        },
      );

      const sql = `SELECT ${target.values
        .map(
          ({ column }, index) =>
            `${driver.quoteIdentifier(column.name)} AS ${driver.quoteIdentifier(`__col_${index}`)}`,
        )
        .join(", ")} FROM ${qt} WHERE ${whereParts.join(" AND ")}`;
      const result = await driver.query(sql, params);
      const row = result.rows[0];
      if (!row) {
        failures.push({
          rowIndex: target.rowIndex,
          columns: target.values.map(({ column }) => column.name),
          message: "The updated row could not be read back for verification.",
        });
        continue;
      }

      const mismatchColumns: string[] = [];
      const mismatchMessages: string[] = [];

      target.values.forEach(({ column, expectedValue }, index) => {
        const check = driver.checkPersistedEdit(column, expectedValue, {
          persistedValue: row[`__col_${index}`],
        });

        if (check && !check.ok) {
          mismatchColumns.push(column.name);
          mismatchMessages.push(
            check.message ??
              `${column.name} could not be confirmed against the persisted value.`,
          );
        }
      });

      if (mismatchColumns.length > 0) {
        failures.push({
          rowIndex: target.rowIndex,
          columns: mismatchColumns,
          message: mismatchMessages.join("; "),
        });
      }
    } catch (err: unknown) {
      failures.push({
        rowIndex: target.rowIndex,
        columns: target.values.map(({ column }) => column.name),
        message:
          err instanceof Error
            ? `Verification query failed: ${err.message}`
            : `Verification query failed: ${String(err)}`,
      });
    }
  }

  return failures;
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
  const prepared = prepareApplyChangesPlan(
    cm,
    connectionId,
    database,
    schema,
    table,
    updates,
    cols,
  );

  if (!prepared.executable) {
    return prepared.result;
  }

  return executePreparedApplyPlan(cm, prepared.plan);
}

export function prepareApplyChangesPlan(
  cm: ConnectionManager,
  connectionId: string,
  database: string,
  schema: string,
  table: string,
  updates: RowUpdate[],
  cols: ColumnDef[],
): PreparedApplyPlanResult {
  if (updates.length === 0) {
    return { executable: false, result: { success: true, rowOutcomes: [] } };
  }

  const driver = cm.getDriver(connectionId);
  if (!driver) {
    return {
      executable: false,
      result: { success: false, error: "Not connected" },
    };
  }

  const colMap = new Map(cols.map((col) => [col.name, col]));
  const operations: TransactionOperation[] = [];
  const previewStatements: string[] = [];
  const validationFailures = new Map<number, ApplyRowOutcome>();
  const verificationTargets: VerificationTarget[] = [];
  const skippedRows = new Set<number>();

  for (const [rowIndex, { primaryKeys, changes }] of updates.entries()) {
    const columnMessages: string[] = [];
    const invalidColumns: string[] = [];
    const verificationValues: VerificationTarget["values"] = [];
    const verificationPrimaryKeys = { ...primaryKeys };

    for (const [columnName, nextValue] of Object.entries(changes)) {
      const column = colMap.get(columnName);
      if (!column) {
        continue;
      }

      const check = driver.checkPersistedEdit(column, nextValue);
      if (!check) {
        continue;
      }

      if (!check.ok) {
        columnMessages.push(
          check.message ??
            `Column "${columnName}" failed persisted-value validation.`,
        );
        invalidColumns.push(columnName);
        continue;
      }

      if (check.shouldVerify) {
        verificationValues.push({
          column,
          expectedValue: nextValue,
        });
      }

      if (column.isPrimaryKey) {
        verificationPrimaryKeys[columnName] = nextValue;
      }
    }

    if (columnMessages.length > 0) {
      validationFailures.set(rowIndex, {
        rowIndex,
        success: false,
        status: "prevalidation_failed",
        message: columnMessages.join(" "),
        columns: invalidColumns,
      });
      continue;
    }

    const op = buildUpdateRowSql(
      driver,
      database,
      schema,
      table,
      primaryKeys,
      changes,
      cols,
    );
    if (!op) {
      skippedRows.add(rowIndex);
      continue;
    }

    operations.push({
      sql: op.sql,
      params: op.params,
      checkAffectedRows: true,
    });
    previewStatements.push(driver.materializePreviewSql(op.sql, op.params));
    verificationTargets.push({
      rowIndex,
      primaryKeys: verificationPrimaryKeys,
      values: verificationValues,
    });
  }

  if (validationFailures.size > 0) {
    const rowOutcomes = updates.map((_, rowIndex) => {
      const failure = validationFailures.get(rowIndex);
      if (failure) {
        return failure;
      }

      if (skippedRows.has(rowIndex)) {
        return buildSkippedOutcome(rowIndex, "No editable changes to apply.");
      }

      return buildSkippedOutcome(
        rowIndex,
        "Not applied because another row failed validation.",
        false,
      );
    });

    return {
      executable: false,
      result: {
        success: false,
        error: summarizeOutcomeMessages(
          "One or more edits were rejected before writing.",
          rowOutcomes.filter(
            (outcome) => outcome.status === "prevalidation_failed",
          ),
        ),
        failedRows: [...validationFailures.keys()],
        rowOutcomes,
      },
    };
  }

  if (operations.length === 0) {
    return {
      executable: false,
      result: {
        success: true,
        rowOutcomes: updates.map((_, rowIndex) =>
          buildSkippedOutcome(rowIndex, "No editable changes to apply."),
        ),
      },
    };
  }

  return {
    executable: true,
    plan: {
      connectionId,
      database,
      schema,
      table,
      cols,
      updates,
      operations,
      previewStatements,
      skippedRows: [...skippedRows],
      verificationTargets,
    },
  };
}

export async function executePreparedApplyPlan(
  cm: ConnectionManager,
  plan: PreparedApplyPlan,
): Promise<ApplyResult> {
  const driver = cm.getDriver(plan.connectionId);
  if (!driver) {
    return { success: false, error: "Not connected" };
  }

  const skippedRows = new Set(plan.skippedRows);

  try {
    await driver.runTransaction(plan.operations);

    const verificationFailures = await verifyExactNumericUpdates(
      driver,
      plan.database,
      plan.schema,
      plan.table,
      plan.cols,
      plan.verificationTargets,
    );
    const verificationFailureMap = new Map(
      verificationFailures.map((failure) => [failure.rowIndex, failure]),
    );

    const rowOutcomes = plan.updates.map((_, rowIndex) => {
      if (skippedRows.has(rowIndex)) {
        return buildSkippedOutcome(rowIndex, "No editable changes to apply.");
      }

      const verificationFailure = verificationFailureMap.get(rowIndex);
      if (verificationFailure) {
        return {
          rowIndex,
          success: false,
          status: "verification_failed",
          message: verificationFailure.message,
          columns: verificationFailure.columns,
        } satisfies ApplyRowOutcome;
      }

      return {
        rowIndex,
        success: true,
        status: "applied",
      } satisfies ApplyRowOutcome;
    });

    if (verificationFailures.length > 0) {
      const warning = summarizeOutcomeMessages(
        "Some edits were written but could not be confirmed exactly.",
        rowOutcomes.filter(
          (outcome) => outcome.status === "verification_failed",
        ),
      );

      return {
        success: true,
        warning,
        failedRows: verificationFailures.map((failure) => failure.rowIndex),
        rowOutcomes,
      };
    }

    return { success: true, rowOutcomes };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);

    return {
      success: false,
      error: message,
      rowOutcomes: plan.updates.map((_, rowIndex) =>
        skippedRows.has(rowIndex)
          ? buildSkippedOutcome(rowIndex, "No editable changes to apply.")
          : buildSkippedOutcome(
              rowIndex,
              `The transaction was rolled back: ${message}`,
              false,
            ),
      ),
    };
  }
}
