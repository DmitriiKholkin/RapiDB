function isMeaningfulString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function extractErrorMessage(
  error: unknown,
  seen = new Set<unknown>(),
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

export function logErrorWithContext(context: string, error: unknown): Error {
  const normalized = normalizeUnknownError(error);
  console.error(`[RapiDB] ${context}:`, normalized.stack ?? normalized.message);
  return normalized;
}
