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

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function normalizeDynamoDbNativeOperationName(
  value: string,
): DynamoDbNativeOperationName | null {
  const normalized = value
    .replace(/command$/i, "")
    .trim()
    .toLowerCase();
  switch (normalized) {
    case "batchgetitem":
      return "BatchGetItem";
    case "batchwriteitem":
      return "BatchWriteItem";
    case "deleteitem":
      return "DeleteItem";
    case "getitem":
      return "GetItem";
    case "putitem":
      return "PutItem";
    case "query":
      return "Query";
    case "scan":
      return "Scan";
    case "transactgetitems":
      return "TransactGetItems";
    case "transactwriteitems":
      return "TransactWriteItems";
    case "updateitem":
      return "UpdateItem";
    default:
      return null;
  }
}

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

function inferTransactOperation(
  transactItems: unknown[],
): DynamoDbNativeOperationName | null {
  let inferred: DynamoDbNativeOperationName | null = null;

  for (const transactItem of transactItems) {
    if (!isRecord(transactItem)) {
      return null;
    }

    const operation = Object.keys(transactItem)[0];
    const normalized =
      operation === "Get"
        ? "TransactGetItems"
        : operation === "Put" ||
            operation === "Delete" ||
            operation === "Update" ||
            operation === "ConditionCheck"
          ? "TransactWriteItems"
          : null;

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

function inferBatchOperation(
  requestItems: Record<string, unknown>,
): DynamoDbNativeOperationName | null {
  let inferred: DynamoDbNativeOperationName | null = null;

  for (const value of Object.values(requestItems)) {
    if (Array.isArray(value)) {
      if (
        value.some(
          (entry) =>
            isRecord(entry) && (entry.PutRequest || entry.DeleteRequest),
        )
      ) {
        if (inferred && inferred !== "BatchWriteItem") {
          return null;
        }
        inferred = "BatchWriteItem";
        continue;
      }
    }

    if (isRecord(value) && Array.isArray(value.Keys)) {
      if (inferred && inferred !== "BatchGetItem") {
        return null;
      }
      inferred = "BatchGetItem";
      continue;
    }

    return null;
  }

  return inferred;
}

export function looksLikeLegacyDynamoDbEnvelope(
  value: unknown,
): value is { operation: string; input?: Record<string, unknown> } {
  return isRecord(value) && typeof value.operation === "string";
}

export function unwrapLegacyDynamoDbEnvelope(queryText: string): string | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(queryText) as unknown;
  } catch {
    return null;
  }

  if (!looksLikeLegacyDynamoDbEnvelope(parsed) || !isRecord(parsed.input)) {
    return null;
  }

  const normalizedOperation = normalizeDynamoDbNativeOperationName(
    parsed.operation,
  );
  if (!normalizedOperation) {
    return null;
  }

  return JSON.stringify(parsed.input, null, 2);
}

export function looksLikeLegacyDynamoPartiql(queryText: string): boolean {
  return /^\s*(select|insert|update|delete)\b/i.test(queryText);
}

export function parseDynamoDbNativeQueryInput(
  queryText: string,
): DynamoDbNativeQueryInput {
  const parsedInputs = parseDynamoDbNativeQueryInputs(queryText);
  if (parsedInputs.length !== 1) {
    throw new Error(
      "DynamoDB native query text must be a single JSON object matching the AWS request syntax.",
    );
  }

  return parsedInputs[0] as DynamoDbNativeQueryInput;
}

export function parseDynamoDbNativeQueryInputs(
  queryText: string,
): DynamoDbNativeQueryInput[] {
  const trimmed = queryText.trim();
  if (trimmed.length === 0) {
    throw new Error("DynamoDB native query text cannot be empty.");
  }

  if (looksLikeLegacyDynamoPartiql(trimmed)) {
    throw new Error(
      'Saved DynamoDB PartiQL is no longer supported. Choose a DynamoDB action and use the official JSON request body, for example {"TableName":"Users","KeyConditionExpression":"PK = :pk","ExpressionAttributeValues":{":pk":{"S":"USER#1"}}}.',
    );
  }

  const parsedDocuments = tryParseConcatenatedJsonObjects(trimmed);
  if (parsedDocuments !== null) {
    return validateParsedNativeQueryInputs(parsedDocuments);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed) as unknown;
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `DynamoDB native query text must be valid JSON. ${message}`,
    );
  }

  return validateParsedNativeQueryInputs([parsed]);
}

function validateParsedNativeQueryInputs(
  parsedValues: unknown[],
): DynamoDbNativeQueryInput[] {
  return parsedValues.map((parsed) => {
    if (looksLikeLegacyDynamoDbEnvelope(parsed)) {
      throw new Error(
        "DynamoDB native query text must match the official AWS request body only. Remove the legacy operation wrapper and keep only the JSON input block.",
      );
    }

    if (!isRecord(parsed)) {
      throw new Error(
        "DynamoDB native query text must be a single JSON object matching the AWS request syntax.",
      );
    }

    return parsed;
  });
}

function tryParseConcatenatedJsonObjects(queryText: string): unknown[] | null {
  const segments = splitConcatenatedJsonObjects(queryText);
  if (segments === null || segments.length <= 1) {
    return null;
  }

  return segments.map((segment) => JSON.parse(segment) as unknown);
}

function splitConcatenatedJsonObjects(queryText: string): string[] | null {
  const segments: string[] = [];
  let index = 0;

  while (index < queryText.length) {
    while (index < queryText.length && /\s/.test(queryText[index] ?? "")) {
      index += 1;
    }

    if (index >= queryText.length) {
      break;
    }

    if (queryText[index] !== "{") {
      return null;
    }

    const start = index;
    let depth = 0;
    let inString = false;
    let isEscaped = false;

    for (; index < queryText.length; index += 1) {
      const char = queryText[index] ?? "";
      if (inString) {
        if (isEscaped) {
          isEscaped = false;
          continue;
        }
        if (char === "\\") {
          isEscaped = true;
          continue;
        }
        if (char === '"') {
          inString = false;
        }
        continue;
      }

      if (char === '"') {
        inString = true;
        continue;
      }

      if (char === "{") {
        depth += 1;
        continue;
      }

      if (char !== "}") {
        continue;
      }

      depth -= 1;
      if (depth !== 0) {
        continue;
      }

      segments.push(queryText.slice(start, index + 1));
      index += 1;
      break;
    }

    if (depth !== 0 || inString) {
      return null;
    }
  }

  return segments.length > 0 ? segments : null;
}
