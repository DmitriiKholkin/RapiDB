export type ObservedNoSqlDeleteBackend = "mongodb" | "redis" | "dynamodb";

export function observedNoSqlDeleteBackend(
  type: unknown,
): ObservedNoSqlDeleteBackend | undefined {
  return type === "mongodb" || type === "redis" || type === "dynamodb"
    ? type
    : undefined;
}

export const noSqlDeleteObservationError = {
  mongodb:
    "MongoDB Table Viewer delete requires an exact BSON observation from the loaded page. Refresh and retry; documents with unsupported BSON values or shapes cannot be deleted from the grid.",
  redis:
    "Redis Table Viewer delete requires an exact value and expiry observation from the loaded page (Redis 7+; strings up to 64 KiB or compact collections read atomically). Refresh and retry; values read page by page (large or non-compact collections, Redis before 7), incomplete values, or servers without the required commands cannot be used for grid deletes. Use the Query Editor to delete such keys deliberately.",
  dynamodb:
    "DynamoDB Table Viewer delete is unavailable: DynamoDB does not provide a row version or an atomic condition that compares the entire item, including newly added attributes.",
} satisfies Record<ObservedNoSqlDeleteBackend, string>;

export function isNoSqlDeleteObservationId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value)
  );
}
