/** True when a mutation is known not to have executed or committed. */
export function isMutationNotExecutedError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const state = error as { code?: unknown; executionState?: unknown };
  return (
    state.code === "NOT_EXECUTED" || state.executionState === "not-executed"
  );
}

/** A local preflight rejected a mutation before it could be sent to a driver. */
export class MutationNotExecutedError extends Error {
  readonly executionState = "not-executed" as const;

  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "MutationNotExecutedError";
  }
}
