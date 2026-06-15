/**
 * Operation-name resolution for DynamoDB.
 *
 * Two entry points:
 *   - normalizeDynamoDbNativeOperationName: text → name
 *   - inferDynamoDbNativeOperationName:     JSON request body → name
 *
 * The text-normalizer is data-driven via a lookup table, so adding
 * a new operation only requires extending the union + the table.
 */
import type { DynamoDbNativeOperationName } from "./types";
import { isRecord } from "./types";

/**
 * Normalized name → canonical operation name. Both legacy `*Command`
 * suffixes and the bare names are accepted in input and are mapped
 * to the same canonical value here.
 */
const OPERATION_NAME_BY_NORMALIZED: ReadonlyMap<
  string,
  DynamoDbNativeOperationName
> = new Map([
  ["batchgetitem", "BatchGetItem"],
  ["batchwriteitem", "BatchWriteItem"],
  ["deleteitem", "DeleteItem"],
  ["getitem", "GetItem"],
  ["putitem", "PutItem"],
  ["query", "Query"],
  ["scan", "Scan"],
  ["transactgetitems", "TransactGetItems"],
  ["transactwriteitems", "TransactWriteItems"],
  ["updateitem", "UpdateItem"],
]);

/**
 * Normalizes a free-form operation-name string (e.g. `"getItemCommand"`)
 * into a canonical DynamoDB operation name, or `null` if unknown.
 */
export function normalizeDynamoDbNativeOperationName(
  value: string,
): DynamoDbNativeOperationName | null {
  const normalized = value
    .replace(/command$/i, "")
    .trim()
    .toLowerCase();
  return OPERATION_NAME_BY_NORMALIZED.get(normalized) ?? null;
}

/** Maps a single `TransactItems[*]` op to its parent operation, if known. */
function classifyTransactSubOp(
  subOp: string,
): DynamoDbNativeOperationName | null {
  if (subOp === "Get") return "TransactGetItems";
  if (
    subOp === "Put" ||
    subOp === "Delete" ||
    subOp === "Update" ||
    subOp === "ConditionCheck"
  ) {
    return "TransactWriteItems";
  }
  return null;
}

/**
 * Walks a `TransactItems` array and returns the parent operation if
 * all entries are homogeneous (all reads or all writes). Returns
 * `null` on the first heterogeneous or unknown entry.
 */
function inferTransactOperation(
  transactItems: unknown[],
): DynamoDbNativeOperationName | null {
  let inferred: DynamoDbNativeOperationName | null = null;
  for (const transactItem of transactItems) {
    if (!isRecord(transactItem)) {
      return null;
    }
    const operation = Object.keys(transactItem)[0];
    const normalized = classifyTransactSubOp(operation);
    if (!normalized) {
      return null;
    }
    if (inferred && inferred !== normalized) {
      return null;
    }
    inferred = normalized;
  }
  return inferred;
}

/**
 * Walks a `RequestItems` map and returns the batch operation
 * (`BatchGetItem` or `BatchWriteItem`) only when every entry is
 * homogeneous. Mixed/invalid batches return `null`.
 */
function inferBatchOperation(
  requestItems: Record<string, unknown>,
): DynamoDbNativeOperationName | null {
  let inferred: DynamoDbNativeOperationName | null = null;

  for (const value of Object.values(requestItems)) {
    if (Array.isArray(value)) {
      const hasWriteShape = value.some(
        (entry) => isRecord(entry) && (entry.PutRequest || entry.DeleteRequest),
      );
      if (hasWriteShape) {
        if (inferred && inferred !== "BatchWriteItem") return null;
        inferred = "BatchWriteItem";
        continue;
      }
      // Arrays of write-shape entries are handled above; the legacy
      // implementation also returns null for any other array shape
      // mixed into a batch — preserved here.
      return null;
    }

    if (isRecord(value) && Array.isArray(value.Keys)) {
      if (inferred && inferred !== "BatchGetItem") return null;
      inferred = "BatchGetItem";
      continue;
    }

    return null;
  }

  return inferred;
}

/**
 * Heuristically infers the DynamoDB operation from a JSON request
 * body. Returns `null` when the shape doesn't match a known operation.
 */
export function inferDynamoDbNativeOperationName(
  value: unknown,
): DynamoDbNativeOperationName | null {
  if (!isRecord(value)) {
    return null;
  }

  if (Array.isArray(value.TransactItems)) {
    const transactOperation = inferTransactOperation(value.TransactItems);
    if (transactOperation) {
      return transactOperation;
    }
  }

  if (isRecord(value.RequestItems)) {
    return inferBatchOperation(value.RequestItems);
  }

  if (typeof value.UpdateExpression === "string") {
    return "UpdateItem";
  }
  if (value.Item !== undefined) {
    return "PutItem";
  }
  if (typeof value.KeyConditionExpression === "string") {
    return "Query";
  }
  if (typeof value.FilterExpression === "string") {
    return "Scan";
  }
  if (
    value.Key !== undefined &&
    (value.ReturnValues !== undefined ||
      value.ConditionExpression !== undefined ||
      value.Expected !== undefined ||
      value.ConditionalOperator !== undefined)
  ) {
    return "DeleteItem";
  }
  if (value.Key !== undefined) {
    return "GetItem";
  }
  if (typeof value.TableName === "string") {
    return "Scan";
  }

  return null;
}

/** True when the query text looks like the legacy PartiQL SELECT/INSERT/... */
export function looksLikeLegacyDynamoPartiql(queryText: string): boolean {
  return /^\s*(select|insert|update|delete)\b/i.test(queryText);
}
