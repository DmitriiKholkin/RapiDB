import type {
  ApplyResultPayload,
  ApplyRowOutcome,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type { ColumnTypeMeta } from "../dbDrivers/types";
import { assertConnectionWritable } from "../utils/readOnlyGuards";
import type {
  ApplyResult,
  PreparedApplyPlan,
  PreparedApplyPlanResult,
  RowUpdate,
  VerificationTarget,
} from "./tableDataContracts";
import {
  assertExactPrimaryKeyShape,
  buildUpdateRowSql,
  coerceRecord,
  filterWritableRecord,
} from "./updateSql";

interface VerificationFailure {
  rowIndex: number;
  columns: string[];
  message: string;
}
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
  try {
    assertConnectionWritable(connectionManager, connectionId, "apply changes");
    const driver = connectionManager.getDriver(connectionId);
    if (!driver) {
      return { success: false, error: "Not connected" };
    }
    await driver.runTransaction([
      ...inserts.map((plan) => ({
        ...plan.operation,
        checkAffectedRows: true,
      })),
      ...(apply?.operations ?? []),
    ]);
    return {
      ...(apply
        ? await verifyAppliedPlan(driver, apply)
        : { success: true, rowOutcomes: [] }),
      insertApplied: inserts.length > 0,
    };
  } catch (error: unknown) {
    const message = `Transaction failed; its outcome may be unknown. Refresh and verify the data before retrying. ${error instanceof Error ? error.message : String(error)}`;
    return {
      success: false,
      error: message,
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
      } else {
        assertExactPrimaryKeyShape(update.primaryKeys, columns);
      }
      return {
        primaryKeys: coerceRecord(driver, update.primaryKeys, columnMetaByName),
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
    for (const [columnName, nextValue] of Object.entries(changes)) {
      const column = columnMetaByName.get(columnName);
      if (!column) {
        continue;
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
      if (column.isPrimaryKey) {
        verificationPrimaryKeys[columnName] = nextValue;
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
      const rowOutcomes = plan.updates.map((_, rowIndex) =>
        skippedRows.has(rowIndex)
          ? buildSkippedOutcome(rowIndex, "No changes to apply.")
          : {
              rowIndex,
              success: true,
              status: "applied",
            },
      ) satisfies ApplyRowOutcome[];
      if (result.affectedRows < executableUpdates.length) {
        throw new Error(
          "One or more rows changed after they were loaded. Refresh the table and retry.",
        );
      }
      return { success: true, rowOutcomes };
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        error: message,
        rowOutcomes: plan.updates.map((_, rowIndex) =>
          buildSkippedOutcome(
            rowIndex,
            `The update operation failed: ${message}`,
            false,
          ),
        ),
      };
    }
  }

  try {
    await driver.runTransaction(plan.operations);
    return await verifyAppliedPlan(driver, plan);
  } catch (error: unknown) {
    const message = `Transaction failed; its outcome may be unknown. Refresh and verify the data before retrying. ${error instanceof Error ? error.message : String(error)}`;
    return {
      success: false,
      error: message,
      rowOutcomes: plan.updates.map((_, rowIndex) =>
        skippedRows.has(rowIndex)
          ? buildSkippedOutcome(rowIndex, "No changes to apply.")
          : buildSkippedOutcome(rowIndex, message, false),
      ),
    };
  }
}

async function verifyAppliedPlan(
  driver: NonNullable<ReturnType<ConnectionManager["getDriver"]>>,
  plan: PreparedApplyPlan,
): Promise<ApplyResultPayload> {
  const skippedRows = new Set(plan.skippedRows);
  const verificationFailures = await verifyExactNumericUpdates(
    driver,
    plan.database,
    plan.schema,
    plan.table,
    plan.cols,
    plan.verificationTargets,
  );
  const verificationFailuresByRow = new Map(
    verificationFailures.map((failure) => [failure.rowIndex, failure]),
  );
  const rowOutcomes = plan.updates.map((_, rowIndex) => {
    if (skippedRows.has(rowIndex)) {
      return buildSkippedOutcome(rowIndex, "No changes to apply.");
    }
    const verificationFailure = verificationFailuresByRow.get(rowIndex);
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
    return {
      success: true,
      warning: summarizeOutcomeMessages(
        "Some edits were written but could not be confirmed exactly.",
        rowOutcomes.filter(
          (outcome) => outcome.status === "verification_failed",
        ),
      ),
      failedRows: verificationFailures.map((failure) => failure.rowIndex),
      rowOutcomes,
    };
  }
  return { success: true, rowOutcomes };
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
  columns: ColumnTypeMeta[],
  targets: VerificationTarget[],
): Promise<VerificationFailure[]> {
  const qualifiedTableName = driver.qualifiedTableName(database, schema, table);
  const columnMetaByName = new Map(
    columns.map((column) => [column.name, column]),
  );
  const failures: VerificationFailure[] = [];
  for (const target of targets) {
    if (target.values.length === 0) {
      continue;
    }
    try {
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
      const result = await driver.query(sql, parameters);
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
        if (shouldSkipTemporalOnUpdateVerification(column)) {
          return;
        }
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
    } catch (error: unknown) {
      failures.push({
        rowIndex: target.rowIndex,
        columns: target.values.map(({ column }) => column.name),
        message:
          error instanceof Error
            ? `Verification query failed: ${error.message}`
            : `Verification query failed: ${String(error)}`,
      });
    }
  }
  return failures;
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
