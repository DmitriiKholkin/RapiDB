/**
 * Type guards and helpers that turn arbitrary thrown values into
 * `Error` instances with usable `.message` and `.stack`.
 *
 * The point of this module is to keep error normalization in one place
 * so log output stays consistent and tests can rely on a stable
 * contract:
 *
 *  - `normalizeUnknownError(value)` always returns an `Error` whose
 *    `.message` is a non-empty string.
 *  - If `value` is already an `Error` with a usable message, the same
 *    instance is returned (no wrapper, no stack mutation).
 *  - If `value` is a plain object, the helper walks a small set of
 *    well-known fields (`message`, `sqlMessage`, `detail`, `reason`,
 *    `cause`, `originalError`, `error`) and returns the first
 *    meaningful string. Cycle protection is built in.
 *  - For everything else (numbers, booleans, primitives), the value is
 *    `String()`-ified.
 */

function isMeaningfulString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

/** Walk an unknown value, returning the deepest meaningful string. */
function extractErrorMessage(
  error: unknown,
  seen: Set<unknown> = new Set<unknown>(),
): string | undefined {
  if (isMeaningfulString(error)) {
    return error.trim();
  }

  if (
    error === null ||
    error === undefined ||
    typeof error === "number" ||
    typeof error === "boolean" ||
    typeof error === "bigint"
  ) {
    return error === null || error === undefined ? undefined : String(error);
  }

  if (typeof error !== "object") {
    return undefined;
  }

  if (seen.has(error)) {
    return undefined;
  }
  seen.add(error);

  const record = error as Record<string, unknown>;
  for (const key of ["message", "sqlMessage", "detail", "reason"] as const) {
    const nested = extractErrorMessage(record[key], seen);
    if (nested) {
      return nested;
    }
  }

  for (const key of ["cause", "originalError", "error"] as const) {
    const nested = extractErrorMessage(record[key], seen);
    if (nested) {
      return nested;
    }
  }

  const parts: string[] = [];
  if (isMeaningfulString(record.name) && record.name.trim() !== "Error") {
    parts.push(record.name.trim());
  }
  if (isMeaningfulString(record.code)) {
    parts.push(record.code.trim());
  }
  if (typeof record.errno === "number" && Number.isFinite(record.errno)) {
    parts.push(String(record.errno));
  }

  return parts.length > 0 ? parts.join(" ") : undefined;
}

function formatUnknownErrorMessage(error: unknown): string {
  return extractErrorMessage(error) ?? "Unknown error";
}

/**
 * Coerce an arbitrary value into a usable `Error`.
 *
 * Behavior:
 *  - If `error` is already an `Error` whose message is meaningful, it is
 *    returned unchanged. This preserves the original `stack` and
 *    identity (callers can `instanceof`-check).
 *  - If `error` is an `Error` with an empty `message`, a new `Error` is
 *    built with the inferred text and the original name/stack copied.
 *  - Otherwise, a fresh `Error` is built from the inferred message.
 */
export function normalizeUnknownError(error: unknown): Error {
  if (error instanceof Error) {
    const message = formatUnknownErrorMessage(error);
    if (message === error.message && message.trim().length > 0) {
      return error;
    }

    const normalized = new Error(message);
    normalized.name = error.name;
    normalized.stack = error.stack;
    return normalized;
  }

  return new Error(formatUnknownErrorMessage(error));
}

/**
 * Normalize an error and write a `[RapiDB] <context>: <stack-or-message>`
 * line to `console.error`. Returns the normalized error so callers can
 * re-throw or inspect it.
 */
export function logErrorWithContext(context: string, error: unknown): Error {
  const normalized = normalizeUnknownError(error);
  console.error(`[RapiDB] ${context}:`, normalized.stack ?? normalized.message);
  return normalized;
}
