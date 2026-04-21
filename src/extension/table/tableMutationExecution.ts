import type {
  ApplyResultPayload,
  ApplyRowOutcome,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type { ColumnTypeMeta } from "../dbDrivers/types";
import type {
  ApplyResult,
  PreparedApplyPlan,
  PreparedApplyPlanResult,
  RowUpdate,
  VerificationTarget,
} from "./tableDataContracts";
import { buildUpdateRowSql } from "./updateSql";

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

  const driver = connectionManager.getDriver(connectionId);
  if (!driver) {
    return {
      executable: false,
      result: { success: false, error: "Not connected" },
    };
  }

  const columnMetaByName = new Map(
    columns.map((column) => [column.name, column]),
  );
  const operations: PreparedApplyPlan["operations"] = [];
  const previewStatements: string[] = [];
  const validationFailures = new Map<number, ApplyRowOutcome>();
  const verificationTargets: VerificationTarget[] = [];
  const skippedRows = new Set<number>();

  for (const [rowIndex, { primaryKeys, changes }] of updates.entries()) {
    const messages: string[] = [];
    const invalidColumns: string[] = [];
    const verificationValues: VerificationTarget["values"] = [];
    const verificationPrimaryKeys = { ...primaryKeys };

    for (const [columnName, nextValue] of Object.entries(changes)) {
      const column = columnMetaByName.get(columnName);
      if (!column) {
        continue;
      }

      const check = driver.checkPersistedEdit(column, nextValue);
      if (!check) {
        continue;
      }

      if (!check.ok) {
        messages.push(
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

    if (messages.length > 0) {
      validationFailures.set(rowIndex, {
        rowIndex,
        success: false,
        status: "prevalidation_failed",
        message: messages.join(" "),
        columns: invalidColumns,
      });
      continue;
    }

    const operation = buildUpdateRowSql(
      driver,
      database,
      schema,
      table,
      primaryKeys,
      changes,
      columns,
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
    previewStatements.push(
      driver.materializePreviewSql(operation.sql, operation.params),
    );
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
  const driver = connectionManager.getDriver(plan.connectionId);
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
    const verificationFailuresByRow = new Map(
      verificationFailures.map((failure) => [failure.rowIndex, failure]),
    );

    const rowOutcomes = plan.updates.map((_, rowIndex) => {
      if (skippedRows.has(rowIndex)) {
        return buildSkippedOutcome(rowIndex, "No editable changes to apply.");
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
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
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
