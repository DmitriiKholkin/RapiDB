import type {
  ApplyResultPayload,
  ApplyRowOutcome,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { isMutationNotExecutedError } from "../dbDrivers/mutationExecutionState";
import { isPostgresStatementRejectedAndRolledBack } from "../dbDrivers/postgresStatementRejection";
import { sqliteTableSchema } from "../dbDrivers/sqliteAttachmentTarget";
import { TransactionVerificationError } from "../dbDrivers/transactionVerification";
import type {
  ColumnTypeMeta,
  DriverMutationResult,
  DriverUpdateRowOutcome,
  IDBDriver,
  MutationSqlExpression,
  TransactionOperation,
  TransactionOptions,
  TransactionVerification,
} from "../dbDrivers/types";
import { DriverUpdateRowsError } from "../dbDrivers/updateRowOutcomes";
import { assertConnectionWritable } from "../utils/readOnlyGuards";
import {
  oracleCapturedIdentityComparison,
  oracleInsertIdentity,
} from "./insertIdentitySql";
import {
  applyRowTypeHints,
  buildPrevalidationFailedResult,
  validatePersistedEditRecord,
} from "./persistedEditValidation";
import type {
  ApplyResult,
  PreparedApplyPlan,
  PreparedApplyPlanResult,
  PreparedInsertPlan,
  RowUpdate,
  VerificationTarget,
} from "./tableDataContracts";
import {
  appendPrimaryKeyComparison,
  buildUpdateRowSql,
  coercePrimaryKeyValues,
  coerceRecord,
  filterWritableRecord,
} from "./updateSql";

export async function applyChangesTransactional(
  connectionManager: ConnectionManager,
  connectionId: string,
  database: string,
  schema: string,
  table: string,
  updates: RowUpdate[],
  columns: ColumnTypeMeta[],
): Promise<ApplyResult> {
  const prepared = prepareApplyChangesPlan(
    connectionManager,
    connectionId,
    database,
    schema,
    table,
    updates,
    columns,
  );
  if (!prepared.executable) {
    return prepared.result;
  }
  return executePreparedApplyPlan(connectionManager, prepared.plan);
}

export function getAtomicApplyRefusal(
  driver: Pick<IDBDriver, "supportsAtomicUpdateRows"> | undefined,
  apply: PreparedApplyPlan | null,
  inserts: readonly PreparedInsertPlan[],
): string | null {
  if (
    apply?.mode !== "driver" &&
    !inserts.some((plan) => plan.mode === "driver")
  ) {
    return null;
  }
  if (
    apply?.mode === "driver" &&
    inserts.length === 0 &&
    driver?.supportsAtomicUpdateRows === true
  ) {
    return null;
  }
  return "This backend cannot guarantee an atomic multi-operation apply. Apply one row at a time.";
}

export async function executeAtomicSqlApplyPlan(
  connectionManager: ConnectionManager,
  apply: PreparedApplyPlan | null,
  inserts: readonly PreparedInsertPlan[],
): Promise<ApplyResultPayload> {
  const connectionId = apply?.connectionId ?? inserts[0]?.connectionId;
  if (!connectionId) {
    return { success: true, rowOutcomes: [] };
  }
  const atomicApplyRefusal = getAtomicApplyRefusal(
    connectionManager.getDriver(connectionId),
    apply,
    inserts,
  );
  if (atomicApplyRefusal) {
    return { success: false, error: atomicApplyRefusal };
  }
  if (apply?.mode === "driver") {
    return executePreparedApplyPlan(connectionManager, apply);
  }
  let executionStarted = false;
  try {
    assertConnectionWritable(connectionManager, connectionId, "apply changes");
    const driver = connectionManager.getDriver(connectionId);
    if (!driver) {
      return { success: false, error: "Not connected" };
    }
    const database = apply?.database ?? inserts[0]?.database;
    const sqliteAttachmentTarget =
      apply?.sqliteAttachmentTarget ?? inserts[0]?.sqliteAttachmentTarget;
    const sameAttachment = (plan: PreparedApplyPlan | PreparedInsertPlan) =>
      plan.sqliteAttachmentTarget?.schema === sqliteAttachmentTarget?.schema &&
      plan.sqliteAttachmentTarget?.generation ===
        sqliteAttachmentTarget?.generation;
    if (
      (apply &&
        (apply.connectionId !== connectionId || !sameAttachment(apply))) ||
      inserts.some(
        (plan) =>
          plan.connectionId !== connectionId ||
          plan.database !== database ||
          !sameAttachment(plan),
      )
    ) {
      throw new Error(
        "An atomic apply must target one connection and database.",
      );
    }
    const options = apply
      ? buildTransactionOptions(driver, apply)
      : { database };
    const insertVerifications = inserts.flatMap((plan, index) =>
      plan.verification
        ? [
            {
              ...plan.verification,
              rowIndex: -1 - index,
              ...(plan.verification.identity
                ? {
                    identity: {
                      ...plan.verification.identity,
                      operationIndex: index,
                    },
                  }
                : {}),
            },
          ]
        : [],
    );
    const verifications = [
      ...insertVerifications,
      ...(options.verifications ?? []).map((verification) =>
        verification.identity
          ? {
              ...verification,
              identity: {
                ...verification.identity,
                operationIndex:
                  verification.identity.operationIndex + inserts.length,
              },
            }
          : verification,
      ),
    ];
    const operations = [
      ...inserts.map((plan) => ({
        ...plan.operation,
        checkAffectedRows: true,
      })),
      ...(apply?.operations ?? []),
    ];
    await assertMutationRollbackSupport(
      driver,
      [
        ...(apply && apply.operations.length > 0
          ? [
              {
                database: apply.database,
                schema: apply.schema,
                table: apply.table,
                verification: "UPDATE" as const,
              },
            ]
          : []),
        ...inserts.map((plan) => ({
          database: plan.database,
          schema: plan.schema,
          table: plan.table,
          ...(plan.verification ? { verification: "INSERT" as const } : {}),
        })),
      ],
      operations.length,
    );
    assertConnectionWritable(connectionManager, connectionId, "apply changes");
    if (connectionManager.getDriver(connectionId) !== driver) {
      throw new Error(
        "Connection changed during mutation preflight. Prepare the changes again before retrying.",
      );
    }
    executionStarted = true;
    await driver.runTransaction(operations, undefined, {
      ...options,
      ...(sqliteAttachmentTarget ? { sqliteAttachmentTarget } : {}),
      ...(verifications.length ? { verifications } : {}),
    });
    return {
      ...(apply
        ? appliedPlanResult(apply)
        : { success: true, rowOutcomes: [] }),
      insertApplied: inserts.length > 0,
    };
  } catch (error: unknown) {
    if (error instanceof TransactionVerificationError && apply) {
      return verificationFailedResult(apply, error);
    }
    if (error instanceof TransactionVerificationError) {
      return {
        success: false,
        error: error.rollbackConfirmed
          ? error.message
          : `${error.message} Transaction outcome is unknown. Refresh and verify before retrying.`,
        changesPossible: !error.rollbackConfirmed,
        outcomeUnknown: !error.rollbackConfirmed,
        rowOutcomes: [],
      };
    }
    const postgresRejectedAndRolledBack =
      executionStarted &&
      isConfirmedPostgresStatementRejection(
        connectionManager,
        connectionId,
        error,
      );
    const notExecuted =
      isMutationNotExecutedError(error) || postgresRejectedAndRolledBack;
    const detail = error instanceof Error ? error.message : String(error);
    const message = !executionStarted
      ? detail
      : postgresRejectedAndRolledBack
        ? `PostgreSQL rejected the statement and rolled back the transaction; no changes were made. ${detail}`
        : notExecuted
          ? `Transaction was not executed; no changes were made. ${detail}`
          : `Transaction failed; its outcome may be unknown. Refresh and verify the data before retrying. ${detail}`;
    return {
      success: false,
      error: message,
      changesPossible: executionStarted && !notExecuted,
      outcomeUnknown: executionStarted && !notExecuted,
      rowOutcomes: (apply?.updates ?? []).map((_, rowIndex) =>
        buildSkippedOutcome(rowIndex, message, false),
      ),
    };
  }
}
export function prepareApplyChangesPlan(
  connectionManager: ConnectionManager,
  connectionId: string,
  database: string,
  schema: string,
  table: string,
  updates: RowUpdate[],
  columns: ColumnTypeMeta[],
): PreparedApplyPlanResult {
  if (updates.length === 0) {
    return { executable: false, result: { success: true, rowOutcomes: [] } };
  }
  assertConnectionWritable(connectionManager, connectionId, "apply changes");
  const driver = connectionManager.getDriver(connectionId);
  if (!driver) {
    return {
      executable: false,
      result: { success: false, error: "Not connected" },
    };
  }

  const columnMap = new Map(columns.map((column) => [column.name, column]));
  const failures = updates.map((update) =>
    validatePersistedEditRecord(
      driver,
      update.changes,
      applyRowTypeHints(driver, columnMap, update.originalTypes),
    ),
  );
  if (failures.some(Boolean)) {
    return {
      executable: false,
      result: buildPrevalidationFailedResult(failures),
    };
  }
  driver.validateTableUpdates?.(updates);
  const primaryKeysByRow = updates.map((update) =>
    Object.keys(filterWritableRecord(update.changes, columnMap)).length > 0
      ? coercePrimaryKeyValues(driver, update.primaryKeys, columns)
      : null,
  );

  if (driver.updateRows) {
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const skippedRows = new Set<number>();
    const coercedUpdates = updates.map((update, rowIndex) => {
      const rowColumnMetaByName = applyRowTypeHints(
        driver,
        columnMetaByName,
        update.originalTypes,
      );
      const writableChanges = filterWritableRecord(
        update.changes,
        rowColumnMetaByName,
      );
      if (Object.keys(writableChanges).length === 0) {
        skippedRows.add(rowIndex);
      }
      return {
        primaryKeys:
          primaryKeysByRow[rowIndex] ??
          coerceRecord(driver, update.primaryKeys, columnMetaByName),
        changes: Object.fromEntries(
          Object.entries(writableChanges).map(([name, value]) => {
            const column = rowColumnMetaByName.get(name);
            const rowTypeHint = update.originalTypes?.[name];
            const hasRowTypeHint =
              rowTypeHint !== undefined &&
              rowTypeHint.nativeType !== "null" &&
              rowTypeHint.nativeType !== "undefined" &&
              rowTypeHint.nativeType !== "missing";
            return [
              name,
              column
                ? driver.coerceTableEditValue
                  ? driver.coerceTableEditValue(value, column, {
                      preserveNativeType: hasRowTypeHint,
                    })
                  : driver.coerceInputValue(value, column)
                : value,
            ];
          }),
        ),
        ...(update.originalValues
          ? {
              originalValues: Object.fromEntries(
                Object.entries(update.originalValues).map(([name, value]) => {
                  const column = rowColumnMetaByName.get(name);
                  return [
                    name,
                    column
                      ? driver.coerceOriginalValue
                        ? driver.coerceOriginalValue(value, column)
                        : driver.coerceInputValue(value, column)
                      : value,
                  ];
                }),
              ),
            }
          : {}),
        ...(update.originalTypes
          ? { originalTypes: update.originalTypes }
          : {}),
      };
    });
    const executableUpdates = coercedUpdates.filter(
      (_update, rowIndex) => !skippedRows.has(rowIndex),
    );
    if (executableUpdates.length === 0) {
      return {
        executable: false,
        result: {
          success: true,
          rowOutcomes: updates.map((_, rowIndex) =>
            buildSkippedOutcome(rowIndex, "No changes to apply."),
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
        mode: "driver",
        cols: columns,
        updates: coercedUpdates,
        operations: [],
        previewStatements: executableUpdates.map(
          ({ primaryKeys, changes, originalValues, originalTypes }) =>
            driver.buildMutationPreviewStatement
              ? driver.buildMutationPreviewStatement(
                  "update",
                  database,
                  schema,
                  table,
                  {
                    primaryKeys,
                    changes,
                    originalValues,
                    originalTypes,
                  },
                )
              : `UPDATE ${driver.qualifiedTableName(database, schema, table)} ${JSON.stringify({ primaryKeys, changes, originalValues })}`,
        ),
        skippedRows: [...skippedRows],
        verificationTargets: [],
      },
    };
  }

  const columnMetaByName = new Map(
    columns.map((column) => [column.name, column]),
  );
  const previewDriver = driver as typeof driver & {
    materializePreviewColumnSql?: (
      sql: string,
      params: readonly unknown[] | undefined,
      columns: readonly (ColumnTypeMeta | undefined)[],
    ) => string;
  };
  const operations: PreparedApplyPlan["operations"] = [];
  const previewStatements: string[] = [];
  const verificationTargets: VerificationTarget[] = [];
  const skippedRows = new Set<number>();
  const keyColumns = columns.filter((column) => column.isPrimaryKey);
  const backend = connectionManager.getConnection(connectionId)?.type;
  const hasPostgresExactKey = keyColumns.some(
    (column) =>
      column.postgresArrayPrimaryKey || column.postgresIntervalPrimaryKey,
  );
  const hasOracleTemporalKey =
    backend === "oracle" &&
    keyColumns.some((column) => driver.buildTablePrimaryKeyExpr?.(column));
  const hasSQLiteKey =
    backend === "sqlite" &&
    Boolean(driver.buildPrimaryKeyComparison) &&
    keyColumns.some((column) => driver.buildTablePrimaryKeyExpr?.(column));
  for (const [
    rowIndex,
    { primaryKeys, changes, originalValues },
  ] of updates.entries()) {
    const verificationValues: VerificationTarget["values"] = [];
    const verificationPrimaryKeys = { ...primaryKeys };
    const sqliteNullKeyColumns: string[] = [];
    for (const [columnName, nextValue] of Object.entries(
      filterWritableRecord(changes, columnMetaByName),
    )) {
      const column = columnMetaByName.get(columnName);
      if (!column) {
        continue;
      }
      if (column.isPrimaryKey) {
        verificationPrimaryKeys[columnName] = nextValue;
        if (hasSQLiteKey && nextValue === null) {
          sqliteNullKeyColumns.push(columnName);
        }
      }
      const check = driver.checkPersistedEdit(column, nextValue);
      if (check?.shouldVerify) {
        verificationValues.push({
          column,
          expectedValue: nextValue,
          ...(originalValues && Object.hasOwn(originalValues, columnName)
            ? { originalValue: originalValues[columnName] }
            : {}),
        });
      }
    }
    let previewColumns: Array<ColumnTypeMeta | undefined> = [];
    const operation = buildUpdateRowSql(
      driver,
      database,
      schema,
      table,
      primaryKeys,
      changes,
      columns,
      originalValues,
      (columns) => {
        previewColumns = columns;
      },
    );
    if (!operation) {
      skippedRows.add(rowIndex);
      continue;
    }
    // Capture the stored key instead of guessing it from SET. Triggers can
    // change the key, and a temporal display value does not carry Oracle's era.
    const captureSQLiteRowid = sqliteNullKeyColumns.length > 0;
    const captureColumns =
      !captureSQLiteRowid &&
      verificationValues.length > 0 &&
      (hasPostgresExactKey ||
        (hasOracleTemporalKey &&
          keyColumns.some((column) => Object.hasOwn(changes, column.name))) ||
        (hasSQLiteKey &&
          keyColumns.some((column) => Object.hasOwn(changes, column.name))))
        ? keyColumns
        : undefined;
    const operationIndex = operations.length;
    const oracleReturning =
      hasOracleTemporalKey && captureColumns
        ? captureColumns.map((column) =>
            oracleInsertIdentity(column, driver.quoteIdentifier(column.name)),
          )
        : undefined;
    const sql = oracleReturning
      ? `${operation.sql} RETURNING ${oracleReturning.map(({ expression }) => expression).join(", ")} INTO ${oracleReturning.map((_, index) => `:${operation.params.length + index + 1}`).join(", ")}`
      : captureColumns
        ? `${operation.sql} RETURNING ${captureColumns
            .map((column, index) => {
              const expr =
                driver.buildTablePrimaryKeyExpr?.(column) ??
                driver.quoteIdentifier(column.name);
              return hasSQLiteKey
                ? expr
                : `${expr} AS ${driver.quoteIdentifier(`__col_${index}`)}`;
            })
            .join(", ")}`
        : operation.sql;
    const writtenKeyComparisons = Object.fromEntries(
      verificationValues.length > 0
        ? keyColumns.flatMap((column) => {
            if (!Object.hasOwn(changes, column.name)) return [];
            const comparison = driver.buildWrittenPrimaryKeyComparison?.(
              column,
              changes[column.name],
              1,
            );
            return comparison ? [[column.name, comparison]] : [];
          })
        : [],
    );
    const mysqlKeyGuard =
      backend === "mysql" && Object.keys(writtenKeyComparisons).length > 0
        ? buildMySQLUpdatedKeyGuard(
            driver,
            driver.qualifiedTableName(database, schema, table),
            columnMetaByName,
            primaryKeysByRow[rowIndex] ?? primaryKeys,
            verificationPrimaryKeys,
            writtenKeyComparisons,
          )
        : undefined;
    operations.push({
      sql,
      params: operation.params,
      checkAffectedRows: true,
      expectedAffectedRows: 1,
      ...(captureColumns || captureSQLiteRowid || mysqlKeyGuard
        ? {
            captureIdentity: oracleReturning
              ? {
                  oracleOutTypes: oracleReturning.map(({ outType }) => outType),
                }
              : mysqlKeyGuard
                ? { mysqlKeyGuard }
                : {},
          }
        : {}),
    });
    const previewSql = oracleReturning ? operation.sql : sql;
    previewStatements.push(
      typeof previewDriver.materializePreviewColumnSql === "function"
        ? previewDriver.materializePreviewColumnSql(
            previewSql,
            operation.params,
            previewColumns,
          )
        : driver.materializePreviewSql(previewSql, operation.params),
    );
    verificationTargets.push({
      rowIndex,
      primaryKeys: hasSQLiteKey ? primaryKeys : verificationPrimaryKeys,
      ...(captureColumns || captureSQLiteRowid
        ? {
            capturedIdentity: {
              columns: captureColumns ?? [],
              operationIndex,
              sqlite: hasSQLiteKey,
              ...(captureSQLiteRowid ? { sqliteNullKeyColumns } : {}),
              ...(oracleReturning ? { oracle: true } : {}),
            },
          }
        : {}),
      ...(Object.keys(writtenKeyComparisons).length > 0
        ? { writtenKeyComparisons }
        : {}),
      values: verificationValues,
    });
  }
  if (operations.length === 0) {
    return {
      executable: false,
      result: {
        success: true,
        rowOutcomes: updates.map((_, rowIndex) =>
          buildSkippedOutcome(rowIndex, "No changes to apply."),
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
      cols: columns,
      updates,
      operations,
      previewStatements,
      skippedRows: [...skippedRows],
      verificationTargets,
    },
  };
}

export async function executePreparedApplyPlan(
  connectionManager: ConnectionManager,
  plan: PreparedApplyPlan,
): Promise<ApplyResultPayload> {
  try {
    assertConnectionWritable(
      connectionManager,
      plan.connectionId,
      "apply changes",
    );
  } catch (error: unknown) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }

  const driver = connectionManager.getDriver(plan.connectionId);
  if (!driver) {
    return { success: false, error: "Not connected" };
  }
  const skippedRows = new Set(plan.skippedRows);
  if (plan.mode === "driver") {
    if (!driver.updateRows) {
      return { success: false, error: "Driver does not support updates." };
    }
    const executableUpdates = plan.updates.filter(
      (_update, rowIndex) => !skippedRows.has(rowIndex),
    );
    const executableRowIndexes = plan.updates.flatMap((_, rowIndex) =>
      skippedRows.has(rowIndex) ? [] : [rowIndex],
    );
    try {
      const result = await driver.updateRows({
        database: plan.database,
        schema: plan.schema,
        table: plan.table,
        updates: executableUpdates.map((update) => ({
          primaryKeys: update.primaryKeys,
          changes: update.changes,
          ...(update.originalValues
            ? { originalValues: update.originalValues }
            : {}),
          ...(update.originalTypes
            ? { originalTypes: update.originalTypes }
            : {}),
        })),
      });
      return buildDriverApplyResult(
        plan,
        skippedRows,
        executableRowIndexes,
        result,
      );
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      if (error instanceof DriverUpdateRowsError) {
        return buildDriverApplyResult(
          plan,
          skippedRows,
          executableRowIndexes,
          error.result,
          message,
        );
      }
      const notExecuted = isMutationNotExecutedError(error);
      return {
        success: false,
        error: message,
        changesPossible: !notExecuted,
        outcomeUnknown: !notExecuted,
        rowOutcomes: plan.updates.map((_, rowIndex) =>
          skippedRows.has(rowIndex)
            ? buildSkippedOutcome(rowIndex, "No changes to apply.")
            : buildDriverFailureOutcome(rowIndex, notExecuted, message),
        ),
      };
    }
  }

  let executionStarted = false;
  try {
    const options = buildTransactionOptions(driver, plan);
    await assertMutationRollbackSupport(
      driver,
      [
        {
          database: plan.database,
          schema: plan.schema,
          table: plan.table,
          verification: "UPDATE" as const,
        },
      ],
      plan.operations.length,
    );
    assertConnectionWritable(
      connectionManager,
      plan.connectionId,
      "apply changes",
    );
    if (connectionManager.getDriver(plan.connectionId) !== driver) {
      throw new Error(
        "Connection changed during mutation preflight. Prepare the changes again before retrying.",
      );
    }
    executionStarted = true;
    await driver.runTransaction(plan.operations, undefined, options);
    return appliedPlanResult(plan);
  } catch (error: unknown) {
    if (error instanceof TransactionVerificationError) {
      return verificationFailedResult(plan, error);
    }
    const postgresRejectedAndRolledBack =
      executionStarted &&
      isConfirmedPostgresStatementRejection(
        connectionManager,
        plan.connectionId,
        error,
      );
    const notExecuted =
      isMutationNotExecutedError(error) || postgresRejectedAndRolledBack;
    const detail = error instanceof Error ? error.message : String(error);
    const message = !executionStarted
      ? detail
      : postgresRejectedAndRolledBack
        ? `PostgreSQL rejected the statement and rolled back the transaction; no changes were made. ${detail}`
        : notExecuted
          ? `Transaction was not executed; no changes were made. ${detail}`
          : `Transaction failed; its outcome may be unknown. Refresh and verify the data before retrying. ${detail}`;
    return {
      success: false,
      error: message,
      changesPossible: executionStarted && !notExecuted,
      outcomeUnknown: executionStarted && !notExecuted,
      rowOutcomes: plan.updates.map((_, rowIndex) =>
        skippedRows.has(rowIndex)
          ? buildSkippedOutcome(rowIndex, "No changes to apply.")
          : buildSkippedOutcome(rowIndex, message, false),
      ),
    };
  }
}

function isConfirmedPostgresStatementRejection(
  connectionManager: ConnectionManager,
  connectionId: string,
  error: unknown,
): boolean {
  return (
    connectionManager.getConnection(connectionId)?.type === "pg" &&
    isPostgresStatementRejectedAndRolledBack(error)
  );
}

function buildDriverApplyResult(
  plan: PreparedApplyPlan,
  skippedRows: ReadonlySet<number>,
  executableRowIndexes: readonly number[],
  result: DriverMutationResult,
  error?: string,
): ApplyResultPayload {
  const resolvedOutcomes = resolveDriverUpdateOutcomes(
    executableRowIndexes.length,
    result,
  );
  const requestIndexByRow = new Map(
    executableRowIndexes.map((rowIndex, requestIndex) => [
      rowIndex,
      requestIndex,
    ]),
  );
  const appliedCount = resolvedOutcomes.filter(
    (status) => status === "applied",
  ).length;
  const notAppliedMessage =
    error !== undefined && appliedCount === 0
      ? `This row was not updated. The apply failed: ${error}`
      : "This row was not updated; it may have changed or been deleted since it was loaded.";
  const rowOutcomes = plan.updates.map((_, rowIndex) => {
    if (skippedRows.has(rowIndex)) {
      return buildSkippedOutcome(rowIndex, "No changes to apply.");
    }
    const requestIndex = requestIndexByRow.get(rowIndex);
    const status =
      requestIndex === undefined
        ? "unknown"
        : (resolvedOutcomes[requestIndex] ?? "unknown");
    if (status === "applied") {
      return { rowIndex, success: true, status } satisfies ApplyRowOutcome;
    }
    return {
      rowIndex,
      success: false,
      status,
      message:
        status === "unknown"
          ? "This row may have been updated; refresh and verify before retrying."
          : notAppliedMessage,
    } satisfies ApplyRowOutcome;
  });
  const failedRows = rowOutcomes.flatMap((outcome) =>
    outcome.status === "not_applied" || outcome.status === "unknown"
      ? [outcome.rowIndex]
      : [],
  );
  const unknownCount = resolvedOutcomes.filter(
    (status) => status === "unknown",
  ).length;
  if (failedRows.length === 0 && error === undefined) {
    return { success: true, rowOutcomes };
  }

  const conflictRows =
    appliedCount === 0 && unknownCount === 0
      ? (result.updateRowOutcomes ?? []).flatMap(
          ({ rowIndex, conflict, status }) =>
            conflict === true && status === "not_applied"
              ? [executableRowIndexes[rowIndex]]
              : [],
        )
      : [];
  const message =
    failedRows.length === 0
      ? `The update was applied to ${appliedCount} row(s), but the driver reported an error. Refresh the table and verify the data. ${error}`
      : (error ??
        (unknownCount > 0
          ? "One or more row outcomes are unknown. Refresh the table and verify before retrying."
          : appliedCount > 0
            ? `The update partially succeeded (${appliedCount} of ${executableRowIndexes.length} row(s)). Refresh the table and verify before retrying.`
            : "One or more rows changed after they were loaded. Refresh the table and retry."));
  return {
    success: false,
    error: message,
    failedRows,
    changesPossible: appliedCount > 0 || unknownCount > 0,
    outcomeUnknown: unknownCount > 0,
    rowOutcomes,
    ...(conflictRows.length > 0 && { conflictRows }),
  };
}

function resolveDriverUpdateOutcomes(
  updateCount: number,
  result: DriverMutationResult,
): DriverUpdateRowOutcome["status"][] {
  const supplied = result.updateRowOutcomes;
  if (Array.isArray(supplied) && supplied.length > 0) {
    const byIndex = new Map<number, DriverUpdateRowOutcome["status"]>();
    let malformed = false;
    for (const outcome of supplied) {
      if (
        !Number.isInteger(outcome.rowIndex) ||
        outcome.rowIndex < 0 ||
        outcome.rowIndex >= updateCount ||
        byIndex.has(outcome.rowIndex) ||
        !["applied", "not_applied", "unknown"].includes(outcome.status)
      ) {
        malformed = true;
        break;
      }
      byIndex.set(outcome.rowIndex, outcome.status);
    }
    if (!malformed) {
      const outcomes: DriverUpdateRowOutcome["status"][] = Array.from(
        { length: updateCount },
        (_, index) => byIndex.get(index) ?? "unknown",
      );
      if (result.affectedRows !== undefined) {
        const appliedCount = outcomes.filter(
          (status) => status === "applied",
        ).length;
        if (
          !Number.isInteger(result.affectedRows) ||
          result.affectedRows !== appliedCount
        ) {
          return Array.from({ length: updateCount }, () => "unknown");
        }
      }
      return outcomes;
    }
    return Array.from({ length: updateCount }, () => "unknown");
  }

  if (Number.isInteger(result.affectedRows)) {
    if (result.affectedRows === updateCount) {
      return Array.from({ length: updateCount }, () => "applied");
    }
    if (result.affectedRows === 0) {
      return Array.from({ length: updateCount }, () => "not_applied");
    }
  }
  return Array.from({ length: updateCount }, () => "unknown");
}

function buildDriverFailureOutcome(
  rowIndex: number,
  notExecuted: boolean,
  message: string,
): ApplyRowOutcome {
  return {
    rowIndex,
    success: false,
    status: notExecuted ? "not_applied" : "unknown",
    message: `The update operation ${notExecuted ? "was not executed" : "may have been applied"}: ${message}`,
  };
}

function appliedPlanResult(plan: PreparedApplyPlan): ApplyResultPayload {
  const skippedRows = new Set(plan.skippedRows);
  const rowOutcomes = plan.updates.map((_, rowIndex) => {
    if (skippedRows.has(rowIndex)) {
      return buildSkippedOutcome(rowIndex, "No changes to apply.");
    }
    return {
      rowIndex,
      success: true,
      status: "applied",
    } satisfies ApplyRowOutcome;
  });
  return { success: true, rowOutcomes };
}
function verificationFailedResult(
  plan: PreparedApplyPlan,
  error: TransactionVerificationError,
): ApplyResultPayload {
  const failure = error.verificationFailure;
  const skippedRows = new Set(plan.skippedRows);
  return {
    success: false,
    error: `${error.message}${error.rollbackConfirmed ? " The transaction was rolled back." : " Transaction outcome is unknown."} Refresh and verify the data before retrying.`,
    changesPossible: !error.rollbackConfirmed,
    outcomeUnknown: !error.rollbackConfirmed,
    ...(failure.mutation !== "insert"
      ? { failedRows: [failure.rowIndex] }
      : {}),
    rowOutcomes: plan.updates.map((_, rowIndex) =>
      !skippedRows.has(rowIndex) && !error.rollbackConfirmed
        ? buildDriverFailureOutcome(rowIndex, false, error.message)
        : failure.mutation !== "insert" && rowIndex === failure.rowIndex
          ? {
              rowIndex,
              success: false,
              status: "verification_failed",
              columns: failure.columns,
              message: failure.message,
            }
          : buildSkippedOutcome(
              rowIndex,
              skippedRows.has(rowIndex)
                ? "No changes to apply."
                : "The transaction was aborted because another row failed verification.",
              skippedRows.has(rowIndex),
            ),
    ),
  };
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
function buildTransactionOptions(
  driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
  plan: PreparedApplyPlan,
): TransactionOptions {
  const {
    database,
    schema,
    table,
    cols: columns,
    verificationTargets: targets,
  } = plan;
  const qualifiedTableName = driver.qualifiedTableName(database, schema, table);
  const columnMetaByName = new Map(
    columns.map((column) => [column.name, column]),
  );
  const verifications: TransactionVerification[] = [];
  for (const target of targets) {
    const captured = target.capturedIdentity;
    const sqliteNullKeyColumns = captured?.sqliteNullKeyColumns;
    if (target.values.length === 0 && !sqliteNullKeyColumns) {
      continue;
    }
    const parameters: unknown[] = [];
    const whereParts = target.capturedIdentity
      ? []
      : buildVerificationKeyWhereParts(
          driver,
          columnMetaByName,
          target.primaryKeys,
          parameters,
          target.writtenKeyComparisons,
        );
    if (captured && !captured.sqlite) {
      captured.columns.forEach((column) => {
        if (captured.oracle) {
          whereParts.push(
            oracleCapturedIdentityComparison(
              column,
              driver.quoteIdentifier(column.name),
              parameters.length + 1,
            ),
          );
          parameters.push(null);
          return;
        }
        whereParts.push(
          appendPrimaryKeyComparison(driver, column, null, parameters),
        );
      });
    }
    const projection = target.values
      .map(
        ({ column }, index) =>
          `${driver.buildVerificationValueExpr?.(column) ?? driver.quoteIdentifier(column.name)} AS ${driver.quoteIdentifier(`__col_${index}`)}`,
      )
      .join(", ");
    const sql = `SELECT ${projection || "1"} FROM ${qualifiedTableName}${captured?.sqlite ? "" : ` WHERE ${whereParts.join(" AND ")}`}`;
    const parameterIndexes = sqliteNullKeyColumns
      ? [0]
      : (captured?.columns.map((_, index) => index) ?? []);
    verifications.push({
      rowIndex: target.rowIndex,
      ...(captured?.sqlite ? { mutation: "update" as const } : {}),
      sql,
      params: captured?.sqlite ? parameterIndexes.map(() => null) : parameters,
      values: target.values,
      ...(captured
        ? {
            ...(sqliteNullKeyColumns
              ? {
                  sqliteNullableKeyUpdate: {
                    schema: sqliteTableSchema(database, schema),
                    table,
                    columns: sqliteNullKeyColumns,
                  },
                }
              : captured.sqlite
                ? {
                    sqlitePrimaryKeyColumns: captured.columns.map(
                      ({ name }) => name,
                    ),
                  }
                : {}),
            identity: {
              operationIndex: captured.operationIndex,
              parameterIndexes,
            },
          }
        : {}),
    });
  }
  return {
    database,
    ...(plan.sqliteAttachmentTarget
      ? { sqliteAttachmentTarget: plan.sqliteAttachmentTarget }
      : {}),
    ...(verifications.length ? { verifications } : {}),
  };
}

function buildVerificationKeyWhereParts(
  driver: IDBDriver,
  columns: Map<string, ColumnTypeMeta>,
  keys: Record<string, unknown>,
  params: unknown[],
  written?: Record<string, MutationSqlExpression>,
): string[] {
  return Object.entries(keys).map(([name, raw]) => {
    const comparison = written && Object.hasOwn(written, name) && written[name];
    if (comparison) {
      params.push(...comparison.params);
      return comparison.sql;
    }
    const column = columns.get(name);
    const value = column
      ? driver.coercePrimaryKeyValue
        ? driver.coercePrimaryKeyValue(raw, column)
        : driver.coerceInputValue(raw, column)
      : raw;
    return appendPrimaryKeyComparison(driver, column ?? name, value, params);
  });
}

function buildMySQLUpdatedKeyGuard(
  driver: IDBDriver,
  qualified: string,
  columns: Map<string, ColumnTypeMeta>,
  oldKey: Record<string, unknown>,
  newKey: Record<string, unknown>,
  written: Record<string, MutationSqlExpression>,
): NonNullable<
  NonNullable<TransactionOperation["captureIdentity"]>["mysqlKeyGuard"]
> {
  const params: unknown[] = [];
  const destination = buildVerificationKeyWhereParts(
    driver,
    columns,
    newKey,
    params,
    written,
  ).join(" AND ");
  const source = buildVerificationKeyWhereParts(
    driver,
    columns,
    oldKey,
    params,
  ).join(" AND ");
  // A trigger could redirect the UPDATE. Don't let verification then succeed
  // by reading a different row that was already at the expected instant.
  return {
    sql: `SELECT 1 FROM ${qualified} WHERE ${destination} AND NOT (${source}) LIMIT 1 FOR UPDATE`,
    params,
    mutation: "update",
  };
}

async function assertMutationRollbackSupport(
  driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
  targets: Array<{
    database: string;
    schema: string;
    table: string;
    verification?: "INSERT" | "UPDATE";
  }>,
  operationCount: number,
): Promise<void> {
  const uniqueTargets = new Map<
    string,
    {
      database: string;
      schema: string;
      table: string;
      verification?: "INSERT" | "UPDATE";
    }
  >();
  for (const target of targets) {
    const key = `${target.database}\u0000${target.schema}\u0000${target.table}`;
    const previous = uniqueTargets.get(key);
    if (previous) {
      previous.verification ??= target.verification;
    } else {
      uniqueTargets.set(key, { ...target });
    }
  }

  for (const target of uniqueTargets.values()) {
    if (operationCount < 2 && !target.verification) continue;
    const risk = await driver.getMutationAtomicityRisk?.(
      target.database,
      target.schema,
      target.table,
    );
    if (risk) {
      throw new Error(
        target.verification
          ? `${target.verification} verification requires rollback support. ${risk}`
          : risk,
      );
    }
  }
}
