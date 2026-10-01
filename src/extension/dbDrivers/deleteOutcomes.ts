import type {
  DeleteResultPayload,
  DeleteRowOutcome,
} from "../../shared/webviewContracts";
import { throwIfTransactionCancelled } from "./timeout";
import type { DriverMutationResult, DriverOperationContext } from "./types";

export class DeleteMutationError extends Error {
  constructor(
    message: string,
    readonly deleteResult: DriverMutationResult,
  ) {
    super(message);
    this.name = "DeleteMutationError";
  }
}

export class DeleteExecutionError extends Error {
  constructor(readonly deleteResult: DeleteResultPayload) {
    super(deleteResult.error);
    this.name = "DeleteExecutionError";
  }
}

export function getDeleteEvidence(
  error: unknown,
): DriverMutationResult | undefined {
  return error && typeof error === "object" && "deleteResult" in error
    ? (error.deleteResult as DriverMutationResult | undefined)
    : undefined;
}

export function buildDeleteResult(
  identities: readonly Record<string, unknown>[],
  evidence: DriverMutationResult,
  cause?: string,
): DeleteResultPayload {
  const rowOutcomes = identities.map(
    (primaryKeys, rowIndex): DeleteRowOutcome => {
      const known = evidence.rowOutcomes?.find(
        (row) => row.rowIndex === rowIndex,
      );
      return known
        ? { ...known, primaryKeys }
        : {
            rowIndex,
            primaryKeys,
            success: evidence.affectedRows === identities.length && !cause,
            status:
              evidence.affectedRows === identities.length && !cause
                ? "deleted"
                : "unknown",
          };
    },
  );
  const outcomeUnknown = rowOutcomes.some((row) => row.status === "unknown");
  const success =
    rowOutcomes.every((row) => row.status === "deleted") && !cause;
  const changesPossible =
    evidence.affectedRows > 0 ||
    outcomeUnknown ||
    rowOutcomes.some((row) => row.status === "notfound");
  return {
    success,
    affectedRows: evidence.affectedRows,
    rowOutcomes,
    changesPossible,
    outcomeUnknown,
    ...(!success
      ? {
          error: `Delete ${evidence.affectedRows > 0 ? "partially changed the data" : outcomeUnknown ? "may have changed the data" : "did not delete all selected rows"}: ${evidence.affectedRows} row(s) confirmed deleted. ${outcomeUnknown ? "An in-flight write may finish after this refresh; refresh again and verify before retrying." : "Refresh and verify before retrying."}${cause ? ` ${cause}` : ""}`,
        }
      : {}),
  };
}

export function unattemptedDeleteResult(
  identities: readonly Record<string, unknown>[],
  message: string,
): DeleteResultPayload {
  return {
    ...buildDeleteResult(
      identities,
      {
        affectedRows: 0,
        rowOutcomes: identities.map((primaryKeys, rowIndex) => ({
          primaryKeys,
          rowIndex,
          success: false,
          status: "skipped",
          message,
        })),
      },
      message,
    ),
    error: message,
  };
}

/** Setup/metadata failures cannot have written any row in this batch. */
export async function prepareDeleteBatch<T>(
  identities: readonly Record<string, unknown>[],
  context: DriverOperationContext | undefined,
  prepare: () => T | Promise<T>,
): Promise<T> {
  const evidence = unattemptedDeleteResult(
    identities,
    "Delete was not started.",
  );
  context?.onDeleteProgress?.(evidence);
  try {
    throwIfTransactionCancelled(context);
    const prepared = await prepare();
    throwIfTransactionCancelled(context);
    return prepared;
  } catch (error) {
    throw new DeleteMutationError(
      error instanceof Error ? error.message : String(error),
      evidence,
    );
  }
}

/** Each backend write is atomic for one identity, but the batch is not. */
export async function deleteRowsSequentially(
  identities: readonly Record<string, unknown>[],
  context: DriverOperationContext | undefined,
  prepare: (
    identity: Record<string, unknown>,
  ) => () => Promise<"deleted" | "notfound" | "unknown">,
): Promise<DriverMutationResult> {
  const rowOutcomes: DeleteRowOutcome[] = identities.map(
    (primaryKeys, rowIndex) => ({
      rowIndex,
      primaryKeys,
      success: false,
      status: "skipped",
    }),
  );
  let affectedRows = 0;
  const snapshot = (): DriverMutationResult => ({
    affectedRows,
    rowOutcomes: rowOutcomes.map((row) => ({ ...row })),
  });
  const report = () => context?.onDeleteProgress?.(snapshot());
  report();
  for (const row of rowOutcomes) {
    try {
      throwIfTransactionCancelled(context);
      // Local validation errors are known failures; sent requests are unknown
      // until the backend acknowledges them. Never infer no-op from a network error.
      row.status = "failed";
      const execute = prepare(row.primaryKeys);
      row.status = "unknown";
      report();
      row.status = await execute();
      row.success = row.status === "deleted";
      if (row.success) affectedRows++;
      report();
      if (row.status === "unknown") {
        throw new Error(
          "The backend did not acknowledge this delete. Refresh and verify before retrying.",
        );
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      row.message = message;
      report();
      throw new DeleteMutationError(message, snapshot());
    }
  }
  return snapshot();
}
