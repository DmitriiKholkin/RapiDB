import type { ConnectionManager } from "../connectionManager";
import type { ColumnTypeMeta } from "../dbDrivers/types";
import { buildInsertRowOperation } from "./insertSql";
import type {
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

    return {
      connectionId,
      database,
      schema,
      table,
      operation,
      previewStatements: [
        driver.materializePreviewSql(operation.sql, operation.params),
      ],
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
    if (primaryKeyValuesList.length === 0) {
      return;
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
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );

    const coercedPrimaryKeys = primaryKeyValuesList
      .map((row) => coerceRecord(driver, row, columnMetaByName))
      .filter((row) => Object.keys(row).length > 0);

    if (coercedPrimaryKeys.length === 0) {
      return;
    }

    const firstPrimaryKeys = Object.keys(coercedPrimaryKeys[0]);
    const isSinglePrimaryKey =
      firstPrimaryKeys.length === 1 &&
      coercedPrimaryKeys.every(
        (row) =>
          Object.keys(row).length === 1 &&
          Object.keys(row)[0] === firstPrimaryKeys[0],
      );

    if (isSinglePrimaryKey) {
      await this.deleteSinglePrimaryKeyRows(
        driver,
        qualifiedTableName,
        columnMetaByName,
        firstPrimaryKeys[0],
        coercedPrimaryKeys,
      );
      await this.verifyRowsDeleted(
        driver,
        database,
        schema,
        table,
        columns,
        coercedPrimaryKeys,
      );
      return;
    }

    await this.deleteCompositePrimaryKeyRows(
      driver,
      qualifiedTableName,
      columnMetaByName,
      coercedPrimaryKeys,
    );
    await this.verifyRowsDeleted(
      driver,
      database,
      schema,
      table,
      columns,
      coercedPrimaryKeys,
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

  private async deleteSinglePrimaryKeyRows(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    qualifiedTableName: string,
    columnMetaByName: Map<string, ColumnTypeMeta>,
    primaryKeyColumn: string,
    rows: Record<string, unknown>[],
  ): Promise<void> {
    const values = rows.map((row) => row[primaryKeyColumn]);
    const chunkSize = 1000;

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

      await driver.query(
        `DELETE FROM ${qualifiedTableName} WHERE ${driver.quoteIdentifier(primaryKeyColumn)} IN (${placeholders})`,
        chunk,
      );
    }
  }

  private async deleteCompositePrimaryKeyRows(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    qualifiedTableName: string,
    columnMetaByName: Map<string, ColumnTypeMeta>,
    rows: Record<string, unknown>[],
  ): Promise<void> {
    const operations = rows.map((row) => {
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

    await driver.runTransaction(operations);
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
