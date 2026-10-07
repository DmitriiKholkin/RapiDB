import { isMutationNotExecutedError } from "./mutationExecutionState";
import type { DriverMutationResult, DriverUpdateRowOutcome } from "./types";

export class DriverUpdateRowsError extends Error {
  constructor(
    message: string,
    readonly result: DriverMutationResult,
    cause?: unknown,
  ) {
    super(message, { cause });
    this.name = "DriverUpdateRowsError";
  }
}

export function completeDriverUpdateRowsResult(
  affectedRows: number,
  updateRowOutcomes: DriverUpdateRowOutcome[],
): DriverMutationResult {
  return {
    affectedRows,
    ...(updateRowOutcomes.some(({ status }) => status !== "applied")
      ? { updateRowOutcomes }
      : {}),
  };
}

export function failedDriverUpdateRows(
  error: unknown,
  updateCount: number,
  completed: readonly DriverUpdateRowOutcome[],
  currentRowIndex: number,
  affectedRows: number,
  currentWriteMayHaveApplied: boolean,
): DriverUpdateRowsError {
  const completedByIndex = new Map(
    completed.map((outcome) => [outcome.rowIndex, outcome]),
  );
  const currentStatus =
    currentWriteMayHaveApplied && !isMutationNotExecutedError(error)
      ? "unknown"
      : "not_applied";
  const updateRowOutcomes = Array.from(
    { length: updateCount },
    (_, rowIndex): DriverUpdateRowOutcome =>
      completedByIndex.get(rowIndex) ?? {
        rowIndex,
        status: rowIndex === currentRowIndex ? currentStatus : "not_applied",
      },
  );
  const message = error instanceof Error ? error.message : String(error);
  return new DriverUpdateRowsError(
    message,
    { affectedRows, updateRowOutcomes },
    error,
  );
}
