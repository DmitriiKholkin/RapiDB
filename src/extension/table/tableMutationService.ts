import type { ConnectionManager } from "../connectionManager";
import type { ColumnTypeMeta } from "../dbDrivers/types";
import { buildInsertRowOperation } from "./insertSql";
import type {
  PreparedDeletePlan,
  PreparedInsertPlan,
  TableColumnsProvider,
} from "./tableDataContracts";
import { buildUpdateRowSql, coerceRecord, writableEntries } from "./updateSql";

export class TableMutationService {
  constructor(
    private readonly connectionManager: ConnectionManager,
    private readonly columnsProvider: TableColumnsProvider,
  ) {}

  /** @deprecated Use prepareApplyChangesPlan + executePreparedApplyPlan for preview-first flow. */
  async updateRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    primaryKeyValues: Record<string, unknown>,
    changes: Record<string, unknown>,
  ): Promise<void> {
    const { driver } = this.getConnectionDriver(connectionId);
    const columns = await this.columnsProvider.getColumns(
      connectionId,
      database,
      schema,
      table,
    );

    const operation = buildUpdateRowSql(
      driver,
      database,
      schema,
      table,
      primaryKeyValues,
      changes,
      columns,
    );
    if (!operation) {
      return;
    }

    const result = await driver.query(operation.sql, operation.params);
    const affectedRows = result.affectedRows ?? result.rowCount;
    if (affectedRows === 0) {
      throw new Error(
        "Row not found — the row may have been modified or deleted by another user",
      );
    }
  }

  /** @deprecated Use prepareInsertRow + executePreparedInsertPlan for preview-first flow. */
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
    const { driver } = this.getConnectionDriver(connectionId);
    const columns = await this.columnsProvider.getColumns(
      connectionId,
      database,
      schema,
      table,
    );
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const writableValues = Object.fromEntries(
      writableEntries(values, columnMetaByName),
    );
    const coercedWritableValues = coerceRecord(
      driver,
      writableValues,
      columnMetaByName,
    );
    const primaryKeyColumns = columns
      .filter((column) => column.isPrimaryKey)
      .map((column) => column.name);
    const hasFullPrimaryKeyCriteria =
      primaryKeyColumns.length > 0 &&
      primaryKeyColumns.every(
        (columnName) => coercedWritableValues[columnName] !== undefined,
      );
    const verificationCriteria = hasFullPrimaryKeyCriteria
      ? Object.fromEntries(
          primaryKeyColumns.map((columnName) => [
            columnName,
            coercedWritableValues[columnName],
          ]),
        )
      : null;
    const operation = buildInsertRowOperation(
      driver,
      database,
      schema,
      table,
      values,
      columns,
    );

    const insertPreviewColumns = writableEntries(values, columnMetaByName)
      .map(([columnName]) => columnMetaByName.get(columnName))
      .filter((column): column is ColumnTypeMeta => column !== undefined);

    const oracleLikeDriver = driver as {
      materializePreviewInsertSql?: (
        sql: string,
        params: readonly unknown[] | undefined,
        columns: readonly ColumnTypeMeta[],
      ) => string;
    };

    const previewSql =
      typeof oracleLikeDriver.materializePreviewInsertSql === "function"
        ? oracleLikeDriver.materializePreviewInsertSql(
            operation.sql,
            operation.params,
            insertPreviewColumns,
          )
        : driver.materializePreviewSql(operation.sql, operation.params);

    return {
      connectionId,
      database,
      schema,
      table,
      operation,
      previewStatements: [previewSql],
      verificationCriteria,
    };
  }

  async executePreparedInsertPlan(plan: PreparedInsertPlan): Promise<void> {
    const { driver } = this.getConnectionDriver(plan.connectionId);
    const result = await driver.query(
      plan.operation.sql,
      plan.operation.params,
    );
    const affectedRows = result.affectedRows ?? result.rowCount;

    if (affectedRows !== undefined && affectedRows === 0) {
      throw new Error(
        "Insert failed: the database reported 0 rows affected. The row may have been rejected by a trigger or constraint.",
      );
    }

    if (!plan.verificationCriteria) {
      return;
    }

    const columns = await this.columnsProvider.getColumns(
      plan.connectionId,
      plan.database,
      plan.schema,
      plan.table,
    );
    const exists = await this.rowExistsByCriteria(
      driver,
      plan.database,
      plan.schema,
      plan.table,
      columns,
      plan.verificationCriteria,
    );

    if (!exists) {
      throw new Error(
        "Insert verification failed: the inserted row could not be read back by primary key.",
      );
    }
  }

  async deleteRows(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    primaryKeyValuesList: Record<string, unknown>[],
  ): Promise<void> {
    const plan = await this.prepareDeleteRowsPlan(
      connectionId,
      database,
      schema,
      table,
      primaryKeyValuesList,
    );
    if (!plan) {
      return;
    }

    await this.executePreparedDeletePlan(plan);
  }

  async prepareDeleteRowsPlan(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    primaryKeyValuesList: Record<string, unknown>[],
  ): Promise<PreparedDeletePlan | null> {
    if (primaryKeyValuesList.length === 0) {
      return null;
    }

    const { driver } = this.getConnectionDriver(connectionId);
    const qualifiedTableName = driver.qualifiedTableName(
      database,
      schema,
      table,
    );
    const columns = await this.columnsProvider.getColumns(
      connectionId,
      database,
      schema,
      table,
    );
    const primaryKeyColumns = columns.filter((column) => column.isPrimaryKey);

    if (primaryKeyColumns.length === 0) {
      throw new Error(
        "Delete requires a primary key so the affected rows can be targeted safely.",
      );
    }

    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const primaryKeyColumnNames = primaryKeyColumns.map(
      (column) => column.name,
    );
    const primaryKeyColumnSet = new Set(primaryKeyColumnNames);

    const coercedPrimaryKeys = primaryKeyValuesList.map((row) => {
      const providedColumnNames = Object.keys(row);
      const hasExactPrimaryKeyShape =
        providedColumnNames.length === primaryKeyColumnNames.length &&
        providedColumnNames.every((columnName) =>
          primaryKeyColumnSet.has(columnName),
        ) &&
        primaryKeyColumnNames.every(
          (columnName) => row[columnName] !== undefined,
        );

      if (!hasExactPrimaryKeyShape) {
        throw new Error(
          "Delete requires the full primary key for every selected row.",
        );
      }

      const normalizedPrimaryKeys = Object.fromEntries(
        primaryKeyColumnNames.map((columnName) => [
          columnName,
          row[columnName],
        ]),
      );

      return coerceRecord(driver, normalizedPrimaryKeys, columnMetaByName);
    });

    if (coercedPrimaryKeys.length === 0) {
      return null;
    }

    const isSinglePrimaryKey = primaryKeyColumnNames.length === 1;

    const operations = isSinglePrimaryKey
      ? this.buildDeleteSinglePrimaryKeyOperations(
          driver,
          qualifiedTableName,
          columnMetaByName,
          primaryKeyColumnNames[0],
          coercedPrimaryKeys,
        )
      : this.buildDeleteCompositePrimaryKeyOperations(
          driver,
          qualifiedTableName,
          columnMetaByName,
          coercedPrimaryKeys,
        );

    return {
      connectionId,
      database,
      schema,
      table,
      executionMode: isSinglePrimaryKey ? "sequential" : "transaction",
      operations,
      previewStatements: operations.map((operation) =>
        driver.materializePreviewSql(operation.sql, operation.params),
      ),
      verificationCriteriaList: coercedPrimaryKeys,
    };
  }

  async executePreparedDeletePlan(plan: PreparedDeletePlan): Promise<void> {
    const { driver } = this.getConnectionDriver(plan.connectionId);

    if (plan.executionMode === "sequential") {
      for (const operation of plan.operations) {
        await driver.query(operation.sql, operation.params);
      }
    } else {
      await driver.runTransaction(plan.operations);
    }

    const columns = await this.columnsProvider.getColumns(
      plan.connectionId,
      plan.database,
      plan.schema,
      plan.table,
    );

    await this.verifyRowsDeleted(
      driver,
      plan.database,
      plan.schema,
      plan.table,
      columns,
      plan.verificationCriteriaList,
    );
  }

  private async verifyRowsDeleted(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    database: string,
    schema: string,
    table: string,
    columns: ColumnTypeMeta[],
    criteriaList: Record<string, unknown>[],
  ): Promise<void> {
    for (const criteria of criteriaList) {
      const exists = await this.rowExistsByCriteria(
        driver,
        database,
        schema,
        table,
        columns,
        criteria,
      );
      if (exists) {
        throw new Error(
          "Delete verification failed: at least one row is still visible after delete.",
        );
      }
    }
  }

  private async rowExistsByCriteria(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    database: string,
    schema: string,
    table: string,
    columns: ColumnTypeMeta[],
    criteria: Record<string, unknown>,
  ): Promise<boolean> {
    const criteriaEntries = Object.entries(criteria);
    if (criteriaEntries.length === 0) {
      return false;
    }

    const qualifiedTableName = driver.qualifiedTableName(
      database,
      schema,
      table,
    );
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const parameters: unknown[] = [];
    const whereParts = criteriaEntries.map(([columnName, value]) => {
      parameters.push(value);
      const column = columnMetaByName.get(columnName);
      const placeholder = column
        ? driver.buildInsertValueExpr(column, parameters.length)
        : "?";
      return `${driver.quoteIdentifier(columnName)} = ${placeholder}`;
    });

    const result = await driver.query(
      `SELECT 1 FROM ${qualifiedTableName} WHERE ${whereParts.join(" AND ")}`,
      parameters,
    );
    return result.rows.length > 0;
  }

  private buildDeleteSinglePrimaryKeyOperations(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    qualifiedTableName: string,
    columnMetaByName: Map<string, ColumnTypeMeta>,
    primaryKeyColumn: string,
    rows: Record<string, unknown>[],
  ): Array<{ sql: string; params: unknown[] }> {
    const values = rows.map((row) => row[primaryKeyColumn]);
    const chunkSize = 1000;
    const operations: Array<{ sql: string; params: unknown[] }> = [];

    for (let index = 0; index < values.length; index += chunkSize) {
      const chunk = values.slice(index, index + chunkSize);
      const placeholders = chunk
        .map((_, placeholderIndex) => {
          const column = columnMetaByName.get(primaryKeyColumn);
          return column
            ? driver.buildInsertValueExpr(column, placeholderIndex + 1)
            : "?";
        })
        .join(", ");

      operations.push({
        sql: `DELETE FROM ${qualifiedTableName} WHERE ${driver.quoteIdentifier(primaryKeyColumn)} IN (${placeholders})`,
        params: chunk,
      });
    }

    return operations;
  }

  private buildDeleteCompositePrimaryKeyOperations(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    qualifiedTableName: string,
    columnMetaByName: Map<string, ColumnTypeMeta>,
    rows: Record<string, unknown>[],
  ): Array<{ sql: string; params: unknown[] }> {
    return rows.map((row) => {
      const parameters: unknown[] = [];
      const whereParts = Object.keys(row).map((columnName) => {
        parameters.push(row[columnName]);
        const column = columnMetaByName.get(columnName);
        const placeholder = column
          ? driver.buildInsertValueExpr(column, parameters.length)
          : "?";
        return `${driver.quoteIdentifier(columnName)} = ${placeholder}`;
      });

      return {
        sql: `DELETE FROM ${qualifiedTableName} WHERE ${whereParts.join(" AND ")}`,
        params: parameters,
      };
    });
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
