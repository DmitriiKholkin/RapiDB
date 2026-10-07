import type {
  ApplyResultPayload,
  ApplyRowOutcome,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { isMutationNotExecutedError } from "../dbDrivers/mutationExecutionState";
import { TransactionVerificationError } from "../dbDrivers/transactionVerification";
import type {
  ColumnTypeMeta,
  DriverMutationResult,
  DriverUpdateRowOutcome,
  TransactionOptions,
  TransactionVerification,
} from "../dbDrivers/types";
import { DriverUpdateRowsError } from "../dbDrivers/updateRowOutcomes";
import { assertConnectionWritable } from "../utils/readOnlyGuards";
import {
  buildPrevalidationFailedResult,
  validatePersistedEditRecord,
} from "./persistedEditValidation";
import type {
  ApplyResult,
  PreparedApplyPlan,
  PreparedApplyPlanResult,
  RowUpdate,
  VerificationTarget,
} from "./tableDataContracts";
import {
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

export async function executeAtomicSqlApplyPlan(
  connectionManager: ConnectionManager,
  apply: PreparedApplyPlan | null,
  inserts: readonly import("./tableDataContracts").PreparedInsertPlan[],
): Promise<ApplyResultPayload> {
  const connectionId = apply?.connectionId ?? inserts[0]?.connectionId;
  if (!connectionId) {
    return { success: true, rowOutcomes: [] };
  }
  if (
    apply?.mode === "driver" ||
    inserts.some((plan) => plan.mode === "driver")
  ) {
    const driver = connectionManager.getDriver(connectionId);
    if (
      apply?.mode === "driver" &&
      inserts.length === 0 &&
      driver?.supportsAtomicUpdateRows === true
    ) {
      return executePreparedApplyPlan(connectionManager, apply);
    }
    return {
      success: false,
      error:
        "This backend cannot guarantee an atomic multi-operation apply. Apply one row at a time.",
    };
  }
  let executionStarted = false;
  try {
    assertConnectionWritable(connectionManager, connectionId, "apply changes");
    const driver = connectionManager.getDriver(connectionId);
    if (!driver) {
      return { success: false, error: "Not connected" };
    }
    const database = apply?.database ?? inserts[0]?.database;
    if (
      (apply && apply.connectionId !== connectionId) ||
      inserts.some(
        (plan) =>
          plan.connectionId !== connectionId || plan.database !== database,
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
              // Keep INSERT failures separate from UPDATE row indexes in mixed results.
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
      ...(options.verifications ?? []),
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
                ...(options.verifications?.length
                  ? { verification: "UPDATE" as const }
                  : {}),
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
      return { success: false, error: error.message, rowOutcomes: [] };
    }
    const notExecuted = isMutationNotExecutedError(error);
    const detail = error instanceof Error ? error.message : String(error);
    const message = !executionStarted
      ? detail
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

  // Validate the entire batch before generating any mutation preview.
  const columnMap = new Map(columns.map((column) => [column.name, column]));
  const failures = updates.map((update) =>
    validatePersistedEditRecord(driver, update.changes, columnMap),
  );
  if (failures.some(Boolean)) {
    return {
      executable: false,
      result: buildPrevalidationFailedResult(failures),
    };
  }
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
      const writableChanges = filterWritableRecord(
        update.changes,
        columnMetaByName,
      );
      if (Object.keys(writableChanges).length === 0) {
        skippedRows.add(rowIndex);
      }
      return {
        primaryKeys:
          primaryKeysByRow[rowIndex] ??
          coerceRecord(driver, update.primaryKeys, columnMetaByName),
        changes: coerceRecord(driver, writableChanges, columnMetaByName),
        ...(update.originalValues
          ? {
              originalValues: Object.fromEntries(
                Object.entries(update.originalValues).map(([name, value]) => {
                  const column = columnMetaByName.get(name);
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
          ({ primaryKeys, changes, originalValues }) =>
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
  for (const [
    rowIndex,
    { primaryKeys, changes, originalValues },
  ] of updates.entries()) {
    const verificationValues: VerificationTarget["values"] = [];
    const verificationPrimaryKeys = { ...primaryKeys };
    for (const [columnName, nextValue] of Object.entries(
      filterWritableRecord(changes, columnMetaByName),
    )) {
      const column = columnMetaByName.get(columnName);
      if (!column) {
        continue;
      }
      if (column.isPrimaryKey) {
        verificationPrimaryKeys[columnName] = nextValue;
      }
      if (shouldSkipTemporalOnUpdateVerification(column)) {
        continue;
      }
      const check = driver.checkPersistedEdit(column, nextValue);
      if (check?.shouldVerify) {
        verificationValues.push({
          column,
          expectedValue: nextValue,
        });
      }
    }
    const operation = buildUpdateRowSql(
      driver,
      database,
      schema,
      table,
      primaryKeys,
      changes,
      columns,
      originalValues,
    );
    if (!operation) {
      skippedRows.add(rowIndex);
      continue;
    }
    operations.push({
      sql: operation.sql,
      params: operation.params,
      checkAffectedRows: true,
    });
    const previewColumns = buildUpdatePreviewColumns(
      changes,
      primaryKeys,
      columnMetaByName,
      originalValues,
    );
    previewStatements.push(
      typeof previewDriver.materializePreviewColumnSql === "function"
        ? previewDriver.materializePreviewColumnSql(
            operation.sql,
            operation.params,
            previewColumns,
          )
        : driver.materializePreviewSql(operation.sql, operation.params),
    );
    verificationTargets.push({
      rowIndex,
      primaryKeys: verificationPrimaryKeys,
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

function buildUpdatePreviewColumns(
  changes: Record<string, unknown>,
  primaryKeys: Record<string, unknown>,
  columnMetaByName: ReadonlyMap<string, ColumnTypeMeta>,
  originalValues: Record<string, unknown> = {},
): Array<ColumnTypeMeta | undefined> {
  const setColumns = Object.entries(changes)
    .filter(
      ([columnName, value]) =>
        value !== undefined && columnMetaByName.has(columnName),
    )
    .map(([columnName]) => columnMetaByName.get(columnName));
  const whereColumns = Object.keys(primaryKeys).map((columnName) =>
    columnMetaByName.get(columnName),
  );
  const originalColumns = Object.entries(originalValues)
    .filter(
      ([name, value]) =>
        value !== undefined && value !== null && columnMetaByName.has(name),
    )
    .map(([name]) => columnMetaByName.get(name));
  return [...setColumns, ...whereColumns, ...originalColumns];
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
          ...(options.verifications?.length
            ? { verification: "UPDATE" as const }
            : {}),
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
    const notExecuted = isMutationNotExecutedError(error);
    const detail = error instanceof Error ? error.message : String(error);
    const message = !executionStarted
      ? detail
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
          : "This row was not updated; it may have changed or been deleted since it was loaded.",
    } satisfies ApplyRowOutcome;
  });
  const failedRows = rowOutcomes.flatMap((outcome) =>
    outcome.status === "not_applied" || outcome.status === "unknown"
      ? [outcome.rowIndex]
      : [],
  );
  const appliedCount = resolvedOutcomes.filter(
    (status) => status === "applied",
  ).length;
  const unknownCount = resolvedOutcomes.filter(
    (status) => status === "unknown",
  ).length;
  if (failedRows.length === 0) {
    return { success: true, rowOutcomes };
  }

  const message =
    error ??
    (unknownCount > 0
      ? "One or more row outcomes are unknown. Refresh the table and verify before retrying."
      : appliedCount > 0
        ? `The update partially succeeded (${appliedCount} of ${executableRowIndexes.length} row(s)). Refresh the table and verify before retrying.`
        : "One or more rows changed after they were loaded. Refresh the table and retry.");
  return {
    success: false,
    error: message,
    failedRows,
    changesPossible: appliedCount > 0 || unknownCount > 0,
    outcomeUnknown: unknownCount > 0,
    rowOutcomes,
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
  // A partial aggregate cannot identify which updates committed. Never assign
  // successes by position; keep every row eligible for identity-based refresh.
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
    error: `${error.message} Refresh and verify the data before retrying.`,
    ...(failure.mutation !== "insert"
      ? { failedRows: [failure.rowIndex] }
      : {}),
    rowOutcomes: plan.updates.map((_, rowIndex) =>
      failure.mutation !== "insert" && rowIndex === failure.rowIndex
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
    if (target.values.length === 0) {
      continue;
    }
    const parameters: unknown[] = [];
    const whereParts = Object.entries(target.primaryKeys).map(
      ([columnName, rawValue]) => {
        const column = columnMetaByName.get(columnName);
        parameters.push(
          column ? driver.coerceInputValue(rawValue, column) : rawValue,
        );
        const placeholder = column
          ? driver.buildInsertValueExpr(column, parameters.length)
          : "?";
        return `${driver.quoteIdentifier(columnName)} = ${placeholder}`;
      },
    );
    const sql = `SELECT ${target.values
      .map(
        ({ column }, index) =>
          `${driver.quoteIdentifier(column.name)} AS ${driver.quoteIdentifier(`__col_${index}`)}`,
      )
      .join(
        ", ",
      )} FROM ${qualifiedTableName} WHERE ${whereParts.join(" AND ")}`;
    verifications.push({
      rowIndex: target.rowIndex,
      sql,
      params: parameters,
      values: target.values,
    });
  }
  return { database, ...(verifications.length ? { verifications } : {}) };
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

function shouldSkipTemporalOnUpdateVerification(
  column: Pick<ColumnTypeMeta, "category">,
): boolean {
  return (
    column.category === "date" ||
    column.category === "time" ||
    column.category === "datetime"
  );
}
