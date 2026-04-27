export function normalizeUnknownError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }

  if (typeof error === "string") {
    return new Error(error);
  }

  return new Error(String(error));
}

export function logErrorWithContext(context: string, error: unknown): Error {
  const normalized = normalizeUnknownError(error);
  console.error(`[RapiDB] ${context}:`, normalized.stack ?? normalized.message);
  return normalized;
}
