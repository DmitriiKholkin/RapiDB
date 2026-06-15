/**
 * DynamoDB shared types and parser helpers.
 *
 * Two distinct concerns live here:
 *   1. Operation name resolution (legacy `*Command` strings, JSON inference).
 *   2. Native JSON request-body parsing for the DynamoDB driver.
 *
 * Both are pure functions: same input -> same output, no I/O.
 */

export type DynamoDbAttributeValueJson =
  | { S: string }
  | { N: string }
  | { B: string }
  | { BOOL: boolean }
  | { NULL: boolean }
  | { SS: string[] }
  | { NS: string[] }
  | { BS: string[] }
  | { L: DynamoDbAttributeValueJson[] }
  | { M: Record<string, DynamoDbAttributeValueJson> };

export type DynamoDbNativeOperationName =
  | "BatchGetItem"
  | "BatchWriteItem"
  | "DeleteItem"
  | "GetItem"
  | "PutItem"
  | "Query"
  | "Scan"
  | "TransactGetItems"
  | "TransactWriteItems"
  | "UpdateItem";

export type DynamoDbNativeQueryInput = Record<string, unknown>;

/** Type guard for plain JSON objects (no `null`, no arrays). */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
