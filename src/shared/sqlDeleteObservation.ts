/** Hidden evidence from the same SQL statement that supplied a Viewer row. */
export interface SqlDeleteObservation {
  readonly shape: string;
  readonly values: readonly string[];
}

export const MISSING_DELETE_OBSERVATION =
  "Delete requires the original row snapshot. Refresh the table and select the rows again. If the snapshot is still unavailable, this table or row contains values the Viewer cannot compare safely.";

export function isSqlDeleteObservation(
  value: unknown,
): value is SqlDeleteObservation {
  if (value === null || typeof value !== "object") return false;
  const { shape, values } = value as Partial<SqlDeleteObservation>;
  return (
    typeof shape === "string" &&
    /^[0-9a-f]{64}$/.test(shape) &&
    Array.isArray(values) &&
    values.length > 0 &&
    [...values].every(
      (value) => typeof value === "string" && value.length <= 64,
    )
  );
}
