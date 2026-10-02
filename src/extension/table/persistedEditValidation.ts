import type {
  ApplyResultPayload,
  ApplyRowOutcome,
} from "../../shared/webviewContracts";
import type { ColumnTypeMeta, IDBDriver } from "../dbDrivers/types";
import { writableEntries } from "./updateSql";

export interface PersistedEditFailure {
  columns: string[];
  message: string;
}

/** Check raw, executable values before coercion can erase a lossy edit. */
export function validatePersistedEditRecord(
  driver: IDBDriver,
  values: Record<string, unknown>,
  columnMap: Map<string, ColumnTypeMeta>,
): PersistedEditFailure | null {
  const columns: string[] = [];
  const messages: string[] = [];
  for (const [name, value] of writableEntries(values, columnMap)) {
    const column = columnMap.get(name);
    if (!column) continue;
    const check = driver.checkPersistedEdit(column, value);
    if (check?.ok === false) {
      columns.push(name);
      messages.push(
        check.message ??
          `Column "${name}" cannot persist the requested value exactly.`,
      );
    }
  }
  return columns.length ? { columns, message: messages.join(" ") } : null;
}

export class PersistedEditValidationError extends Error {
  readonly status = "prevalidation_failed" as const;
  readonly columns: string[];
  constructor(failure: PersistedEditFailure) {
    super(failure.message);
    this.name = "PersistedEditValidationError";
    this.columns = failure.columns;
  }
}

export function buildPrevalidationFailedResult(
  updateFailures: readonly (PersistedEditFailure | null)[],
  insertFailures: readonly (PersistedEditFailure | null)[] = [],
): ApplyResultPayload {
  const error = [...updateFailures, ...insertFailures]
    .flatMap((failure) => (failure ? [failure.message] : []))
    .join(" ");
  const outcomes = (
    failures: readonly (PersistedEditFailure | null)[],
  ): ApplyRowOutcome[] =>
    failures.map((failure, rowIndex) => ({
      rowIndex,
      success: false,
      status: failure ? "prevalidation_failed" : "skipped",
      message:
        failure?.message ??
        "Not applied because another row failed prevalidation.",
      ...(failure ? { columns: failure.columns } : {}),
    }));
  return {
    success: false,
    error,
    failedRows: updateFailures.flatMap((failure, index) =>
      failure ? [index] : [],
    ),
    rowOutcomes: outcomes(updateFailures),
    ...(insertFailures.length
      ? { insertApplied: false, insertRowOutcomes: outcomes(insertFailures) }
      : {}),
  };
}
