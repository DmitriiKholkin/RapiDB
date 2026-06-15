/**
 * Native JSON request-body parsing for the DynamoDB driver.
 *
 * The input may be:
 *   - a single JSON object
 *   - multiple JSON objects concatenated (common when copy-pasting
 *     multiple AWS examples back-to-back)
 *
 * The tokenizer (`splitConcatenatedJsonObjects`) is a hand-rolled
 * brace-counting state machine that respects string boundaries and
 * escape sequences — the same guarantees as the legacy code.
 */

import { looksLikeLegacyDynamoPartiql } from "./operation";
import type { DynamoDbNativeQueryInput } from "./types";
import { isRecord } from "./types";

const WHITESPACE_RE = /\s/;

/**
 * Splits a string of concatenated JSON objects into their source
 * substrings, or returns `null` if the string is not a sequence of
 * top-level objects.
 */
export function splitConcatenatedJsonObjects(
  queryText: string,
): string[] | null {
  const segments: string[] = [];
  let index = 0;

  while (index < queryText.length) {
    index = skipWhitespaceFrom(queryText, index);
    if (index >= queryText.length) break;
    if (queryText[index] !== "{") return null;

    const segment = readOneJsonObject(queryText, index);
    if (segment === null) return null;
    segments.push(segment.text);
    index = segment.nextIndex;
  }

  return segments.length > 0 ? segments : null;
}

/** Advances `start` past any whitespace; returns the new offset. */
function skipWhitespaceFrom(text: string, start: number): number {
  let cursor = start;
  while (cursor < text.length && WHITESPACE_RE.test(text[cursor] ?? "")) {
    cursor += 1;
  }
  return cursor;
}

interface ReadSegment {
  text: string;
  nextIndex: number;
}

/**
 * Reads a single top-level JSON object starting at `start`. Returns
 * `null` if the input is not well-formed at this position.
 */
function readOneJsonObject(text: string, start: number): ReadSegment | null {
  let depth = 0;
  let inString = false;
  let isEscaped = false;
  let index = start;

  for (; index < text.length; index += 1) {
    const char = text[index] ?? "";

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
    if (char !== "}") continue;

    depth -= 1;
    if (depth !== 0) continue;

    return { text: text.slice(start, index + 1), nextIndex: index + 1 };
  }

  // EOF before the matching close-brace, or string never closed.
  if (depth !== 0 || inString) return null;
  return null;
}

function tryParseConcatenatedJsonObjects(queryText: string): unknown[] | null {
  const segments = splitConcatenatedJsonObjects(queryText);
  if (!segments || segments.length <= 1) return null;
  return segments.map((segment) => JSON.parse(segment) as unknown);
}

/** Throws with a consistent error shape used by the public parsers. */
function wrapJsonParseError(original: unknown): Error {
  const message =
    original instanceof Error ? original.message : String(original);
  return new Error(`DynamoDB native query text must be valid JSON. ${message}`);
}

/**
 * Validates a single parsed value: rejects the legacy `operation` wrapper
 * and ensures the value is a plain object.
 */
function validateParsedDocument(parsed: unknown): DynamoDbNativeQueryInput {
  if (isRecord(parsed) && typeof parsed.operation === "string") {
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
}

function validateParsedNativeQueryInputs(
  parsedValues: unknown[],
): DynamoDbNativeQueryInput[] {
  return parsedValues.map(validateParsedDocument);
}

/**
 * Parses a query text into a single native request body. Throws when
 * the input contains 0 or 2+ top-level objects.
 */
export function parseDynamoDbNativeQueryInput(
  queryText: string,
): DynamoDbNativeQueryInput {
  const parsedInputs = parseDynamoDbNativeQueryInputs(queryText);
  if (parsedInputs.length !== 1) {
    throw new Error(
      "DynamoDB native query text must be a single JSON object matching the AWS request syntax.",
    );
  }
  return parsedInputs[0];
}

/**
 * Parses a query text into one or more native request bodies.
 * Throws on empty input, on legacy PartiQL syntax, or on invalid JSON.
 */
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

  const concatenated = tryParseConcatenatedJsonObjects(trimmed);
  if (concatenated !== null) {
    return validateParsedNativeQueryInputs(concatenated);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed) as unknown;
  } catch (error: unknown) {
    throw wrapJsonParseError(error);
  }
  return validateParsedNativeQueryInputs([parsed]);
}
