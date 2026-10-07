import type { ConnectionManager } from "../connectionManager";
import {
  buildDeleteResult,
  DeleteExecutionError,
  getDeleteEvidence,
  unattemptedDeleteResult,
} from "../dbDrivers/deleteOutcomes";
import {
  isMutationNotExecutedError,
  MutationNotExecutedError,
} from "../dbDrivers/mutationExecutionState";
import type { ColumnTypeMeta, TransactionOperation } from "../dbDrivers/types";
import { pMapWithLimit } from "../utils/concurrency";
import { assertConnectionWritable } from "../utils/readOnlyGuards";
import { buildInsertRowOperation } from "./insertSql";
import { prepareInsertVerification } from "./insertVerification";
import {
  PersistedEditValidationError,
  validateInsertColumnNames,
  validatePersistedEditRecord,
} from "./persistedEditValidation";
import type {
  PreparedDeletePlan,
  PreparedInsertPlan,
  TableColumnsProvider,
} from "./tableDataContracts";
import {
  buildUpdateRowSql,
  coercePrimaryKeyValues,
  coerceRecord,
  filterWritableRecord,
  writableEntries,
} from "./updateSql";

const DELETE_VERIFICATION_CONCURRENCY_LIMIT = 8;
export class TableMutationService {
  constructor(
    private readonly connectionManager: ConnectionManager,
    private readonly columnsProvider: TableColumnsProvider,
  ) {}
  async updateRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    primaryKeyValues: Record<string, unknown>,
    changes: Record<string, unknown>,
  ): Promise<void> {
    assertConnectionWritable(
      this.connectionManager,
      connectionId,
      "update data",
    );
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
    const failure = validatePersistedEditRecord(
      driver,
      changes,
      columnMetaByName,
    );
    if (failure) throw new PersistedEditValidationError(failure);
    if (driver.updateRows) {
      const writableChanges = filterWritableRecord(changes, columnMetaByName);
      if (Object.keys(writableChanges).length === 0) {
        return;
      }
      const primaryKeys = coercePrimaryKeyValues(
        driver,
        primaryKeyValues,
        columns,
      );
      const coercedChanges = coerceRecord(
        driver,
        writableChanges,
        columnMetaByName,
      );
      try {
        assertConnectionWritable(
          this.connectionManager,
          connectionId,
          "update data",
        );
        if (this.connectionManager.getDriver(connectionId) !== driver) {
          throw new Error(
            "Connection changed during mutation preflight. Prepare the changes again before retrying.",
          );
        }
      } catch (error) {
        throw new MutationNotExecutedError(
          error instanceof Error ? error.message : String(error),
          error,
        );
      }
      const result = await driver.updateRows({
        database,
        schema,
        table,
        updates: [
          {
            primaryKeys,
            changes: coercedChanges,
          },
        ],
      });
      const rowOutcome = result.updateRowOutcomes?.find(
        ({ rowIndex }) => rowIndex === 0,
      );
      if (rowOutcome?.status === "applied") {
        return;
      }
      if (rowOutcome?.status === "unknown") {
        throw new Error(
          "Update may have been applied. Refresh and verify the row before retrying.",
        );
      }
      if (rowOutcome?.status === "not_applied") {
        throw new Error(
          "Row not found — the row may have been modified or deleted by another user",
        );
      }
      if (result.affectedRows === 0) {
        throw new Error(
          "Row not found — the row may have been modified or deleted by another user",
        );
      }
      if (result.affectedRows !== 1) {
        throw new Error(
          "Update outcome is unknown. Refresh and verify the row before retrying.",
        );
      }
      return;
    }

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
    try {
      assertConnectionWritable(
        this.connectionManager,
        connectionId,
        "update data",
      );
      if (this.connectionManager.getDriver(connectionId) !== driver) {
        throw new Error(
          "Connection changed during mutation preflight. Prepare the changes again before retrying.",
        );
      }
    } catch (error) {
      throw new MutationNotExecutedError(
        error instanceof Error ? error.message : String(error),
        error,
      );
    }
    const result = await driver.query(operation.sql, operation.params, {
      database,
    });
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
    assertConnectionWritable(
      this.connectionManager,
      connectionId,
      "insert data",
    );
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
    const unknownColumnFailure = validateInsertColumnNames(
      driver,
      values,
      columnMetaByName,
    );
    if (unknownColumnFailure) {
      throw new PersistedEditValidationError(unknownColumnFailure);
    }
    const failure = validatePersistedEditRecord(
      driver,
      values,
      columnMetaByName,
      "Insert",
    );
    if (failure) throw new PersistedEditValidationError(failure);
    if (driver.insertRow) {
      const writableValues = Object.fromEntries(
        writableEntries(values, columnMetaByName),
      );
      const coercedValues = coerceRecord(
        driver,
        writableValues,
        columnMetaByName,
      );
      const previewStatements = driver.buildMutationPreviewStatements
        ? await driver.buildMutationPreviewStatements(
            "insert",
            database,
            schema,
            table,
            { values: coercedValues },
          )
        : [
            driver.buildMutationPreviewStatement
              ? driver.buildMutationPreviewStatement(
                  "insert",
                  database,
                  schema,
                  table,
                  { values: coercedValues },
                )
              : `INSERT ${driver.qualifiedTableName(database, schema, table)} ${JSON.stringify(coercedValues)}`,
          ];
      return {
        connectionId,
        database,
        schema,
        table,
        mode: "driver",
        values: coercedValues,
        operation: {
          sql: "-- driver-hook-insert",
          params: [],
        },
        previewStatements,
        verificationCriteria: null,
      };
    }

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
    const verification = prepareInsertVerification(
      driver,
      this.connectionManager.getConnection(connectionId)?.type,
      database,
      schema,
      table,
      writableValues,
      columns,
      operation,
    );
    return {
      connectionId,
      database,
      schema,
      table,
      operation,
      previewStatements: [previewSql],
      verificationCriteria,
      verification,
    };
  }
  async executePreparedInsertPlan(plan: PreparedInsertPlan): Promise<void> {
    let driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>;
    try {
      assertConnectionWritable(
        this.connectionManager,
        plan.connectionId,
        "insert data",
      );
      driver = this.getConnectionDriver(plan.connectionId).driver;
    } catch (error) {
      throw new MutationNotExecutedError(
        error instanceof Error ? error.message : String(error),
        error,
      );
    }
    if (plan.mode === "driver") {
      if (!driver.insertRow) {
        throw new MutationNotExecutedError(
          "Insert is not supported by this driver.",
        );
      }
      const result = await driver.insertRow({
        database: plan.database,
        schema: plan.schema,
        table: plan.table,
        values: plan.values ?? {},
      });
      if (result.affectedRows === 0) {
        throw new Error(
          "Insert failed: the database reported 0 rows affected.",
        );
      }
      return;
    }

    if (plan.verification) {
      let risk: string | null | undefined;
      try {
        risk = await driver.getMutationAtomicityRisk?.(
          plan.database,
          plan.schema,
          plan.table,
        );
      } catch (error) {
        throw new MutationNotExecutedError(
          `Could not verify INSERT rollback support before execution. ${error instanceof Error ? error.message : String(error)}`,
          error,
        );
      }
      if (risk)
        throw new MutationNotExecutedError(
          `INSERT verification requires rollback support. ${risk}`,
        );
      try {
        assertConnectionWritable(
          this.connectionManager,
          plan.connectionId,
          "insert data",
        );
        if (this.connectionManager.getDriver(plan.connectionId) !== driver) {
          throw new Error(
            "Connection changed during mutation preflight. Prepare the changes again before retrying.",
          );
        }
      } catch (error) {
        throw new MutationNotExecutedError(
          error instanceof Error ? error.message : String(error),
          error,
        );
      }
      await driver.runTransaction(
        [{ ...plan.operation, checkAffectedRows: true }],
        undefined,
        { database: plan.database, verifications: [plan.verification] },
      );
      return;
    }
    const result = await driver.query(
      plan.operation.sql,
      plan.operation.params,
      { database: plan.database },
    );
    const affectedRows = result.affectedRows ?? result.rowCount;
    if (affectedRows !== undefined && affectedRows === 0) {
      throw new Error(
        "Insert failed: the database reported 0 rows affected. The row may have been rejected by a trigger or constraint.",
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
    assertConnectionWritable(
      this.connectionManager,
      connectionId,
      "delete data",
    );
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
    if (driver.deleteRows) {
      const coercedPrimaryKeyValuesList = primaryKeyValuesList.map(
        (criteria) => {
          return coercePrimaryKeyValues(driver, criteria, columns, "delete");
        },
      );
      const previewStatements = driver.buildMutationPreviewStatements
        ? await driver.buildMutationPreviewStatements(
            "delete",
            database,
            schema,
            table,
            {
              primaryKeyValuesList: coercedPrimaryKeyValuesList,
            },
          )
        : driver.buildMutationPreviewStatement
          ? [
              driver.buildMutationPreviewStatement(
                "delete",
                database,
                schema,
                table,
                {
                  primaryKeyValuesList: coercedPrimaryKeyValuesList,
                },
              ),
            ]
          : coercedPrimaryKeyValuesList.map(
              (criteria) =>
                `DELETE ${driver.qualifiedTableName(database, schema, table)} ${JSON.stringify(criteria)}`,
            );
      return {
        connectionId,
        database,
        schema,
        table,
        mode: "driver",
        executionMode: "sequential",
        primaryKeyValuesList: coercedPrimaryKeyValuesList,
        rowIdentities: primaryKeyValuesList,
        operations: [],
        previewStatements,
        verificationCriteriaList: coercedPrimaryKeyValuesList,
      };
    }

    const qualifiedTableName = driver.qualifiedTableName(
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
    const primaryKeyColumnNames = primaryKeyColumns.map(
      (column) => column.name,
    );
    const coercedPrimaryKeys = primaryKeyValuesList.map((row) => {
      const coerced = coercePrimaryKeyValues(driver, row, columns, "delete");
      const normalizedPrimaryKeys = Object.fromEntries(
        primaryKeyColumnNames.map((columnName) => [
          columnName,
          coerced[columnName],
        ]),
      );
      return normalizedPrimaryKeys;
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
    if (operations.length > 1) {
      const atomicityRisk = await driver.getMutationAtomicityRisk?.(
        database,
        schema,
        table,
      );
      if (atomicityRisk) {
        throw new Error(atomicityRisk);
      }
    }
    return {
      connectionId,
      database,
      schema,
      table,
      executionMode: "transaction",
      rowIdentities: primaryKeyValuesList,
      operations,
      previewStatements: operations.map((operation) =>
        driver.materializePreviewSql(operation.sql, operation.params),
      ),
      verificationCriteriaList: coercedPrimaryKeys,
    };
  }
  async executePreparedDeletePlan(plan: PreparedDeletePlan): Promise<void> {
    const identities = plan.rowIdentities ?? plan.verificationCriteriaList;
    let driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>;
    try {
      assertConnectionWritable(
        this.connectionManager,
        plan.connectionId,
        "delete data",
      );
      driver = this.getConnectionDriver(plan.connectionId).driver;
    } catch (error) {
      throw new DeleteExecutionError(
        unattemptedDeleteResult(
          identities,
          error instanceof Error ? error.message : String(error),
        ),
      );
    }
    if (plan.mode === "driver") {
      if (!driver.deleteRows) {
        throw new DeleteExecutionError(
          unattemptedDeleteResult(
            identities,
            "Delete is not supported by this driver.",
          ),
        );
      }
      try {
        const result = await driver.deleteRows({
          database: plan.database,
          schema: plan.schema,
          table: plan.table,
          primaryKeyValuesList: plan.primaryKeyValuesList ?? [],
        });
        const outcome = buildDeleteResult(identities, result);
        if (!outcome.success) throw new DeleteExecutionError(outcome);
      } catch (error) {
        if (error instanceof DeleteExecutionError) throw error;
        if (isMutationNotExecutedError(error)) {
          throw new DeleteExecutionError(
            unattemptedDeleteResult(
              identities,
              error instanceof Error ? error.message : String(error),
            ),
          );
        }
        throw new DeleteExecutionError(
          buildDeleteResult(
            identities,
            getDeleteEvidence(error) ?? { affectedRows: 0 },
            error instanceof Error ? error.message : String(error),
          ),
        );
      }
      return;
    }

    let committed = false;
    try {
      await driver.runTransaction(plan.operations, undefined, {
        database: plan.database,
      });
      committed = true;
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
    } catch (error) {
      if (!committed && isMutationNotExecutedError(error)) {
        throw new DeleteExecutionError(
          unattemptedDeleteResult(
            identities,
            error instanceof Error ? error.message : String(error),
          ),
        );
      }
      throw new DeleteExecutionError(
        buildDeleteResult(
          plan.rowIdentities ?? plan.verificationCriteriaList,
          {
            affectedRows: committed ? plan.verificationCriteriaList.length : 0,
          },
          error instanceof Error ? error.message : String(error),
          committed ? { committed: true } : undefined,
        ),
      );
    }
  }
  private async verifyRowsDeleted(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    database: string,
    schema: string,
    table: string,
    columns: ColumnTypeMeta[],
    criteriaList: Record<string, unknown>[],
  ): Promise<void> {
    const existenceChecks = await pMapWithLimit(
      criteriaList,
      DELETE_VERIFICATION_CONCURRENCY_LIMIT,
      async (criteria) =>
        await this.rowExistsByCriteria(
          driver,
          database,
          schema,
          table,
          columns,
          criteria,
        ),
    );
    if (existenceChecks.some((exists) => exists)) {
      throw new Error(
        "Delete verification failed: at least one row is still visible after delete.",
      );
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
      { database },
    );
    return result.rows.length > 0;
  }
  private buildDeleteSinglePrimaryKeyOperations(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    qualifiedTableName: string,
    columnMetaByName: Map<string, ColumnTypeMeta>,
    primaryKeyColumn: string,
    rows: Record<string, unknown>[],
  ): TransactionOperation[] {
    const values = rows.map((row) => row[primaryKeyColumn]);
    const chunkSize = 1000;
    const operations: TransactionOperation[] = [];
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
        expectedAffectedRows: chunk.length,
      });
    }
    return operations;
  }
  private buildDeleteCompositePrimaryKeyOperations(
    driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
    qualifiedTableName: string,
    columnMetaByName: Map<string, ColumnTypeMeta>,
    rows: Record<string, unknown>[],
  ): TransactionOperation[] {
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
        expectedAffectedRows: 1,
      };
    });
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
