import {
  ConditionalCheckFailedException,
  DynamoDBServiceException,
  InternalServerError,
  ItemCollectionSizeLimitExceededException,
  ProvisionedThroughputExceededException,
  RequestLimitExceeded,
  ResourceNotFoundException,
  TransactionCanceledException,
  TransactionConflictException,
  TransactionInProgressException,
} from "@aws-sdk/client-dynamodb";
import { errors } from "@elastic/elasticsearch";
import { describe, expect, it } from "vitest";
import {
  isServerReportedQueryError,
  markQueryNotSent,
} from "../../src/extension/dbDrivers/queryErrorOutcome";

const metadata = (httpStatusCode: number, attempts = 1) => ({
  $metadata: { httpStatusCode, attempts, requestId: "request" },
});

function clientFault(name: string, status = 400, attempts = 1) {
  return new DynamoDBServiceException({
    name,
    $fault: "client",
    message: name,
    ...metadata(status, attempts),
  });
}

function responseError(
  statusCode: number,
  path = "/index/_doc/1",
  meta: { attempts?: number; aborted?: boolean } = {},
) {
  return new errors.ResponseError({
    body: { error: { type: "error", reason: "rejected" }, status: statusCode },
    statusCode,
    headers: {},
    warnings: null,
    meta: {
      context: null,
      name: "elasticsearch-js",
      request: {
        params: { method: "PUT", path },
        options: {},
        id: 1,
      },
      connection: null,
      attempts: meta.attempts ?? 0,
      aborted: meta.aborted ?? false,
    },
  } as never);
}

describe("DynamoDB query error outcomes", () => {
  it.each([
    ["ValidationException", () => clientFault("ValidationException"), true],
    [
      "ConditionalCheckFailedException",
      () =>
        new ConditionalCheckFailedException({
          message: "failed",
          ...metadata(400),
        }),
      true,
    ],
    [
      "ResourceNotFoundException",
      () => new ResourceNotFoundException({ message: "no", ...metadata(400) }),
      true,
    ],
    ["AccessDeniedException", () => clientFault("AccessDeniedException"), true],
    [
      "ItemCollectionSizeLimitExceededException",
      () =>
        new ItemCollectionSizeLimitExceededException({
          message: "too large",
          ...metadata(400),
        }),
      true,
    ],
    [
      "a cancelled transaction with only condition failures",
      () =>
        new TransactionCanceledException({
          message: "cancelled",
          ...metadata(400),
          CancellationReasons: [
            { Code: "None" },
            { Code: "ConditionalCheckFailed" },
          ],
        }),
      true,
    ],
    [
      "a cancelled transaction with a conflict",
      () =>
        new TransactionCanceledException({
          message: "cancelled",
          ...metadata(400),
          CancellationReasons: [
            { Code: "None" },
            { Code: "TransactionConflict" },
          ],
        }),
      false,
    ],
    [
      "a cancelled transaction without reasons",
      () =>
        new TransactionCanceledException({
          message: "cancelled",
          ...metadata(400),
        }),
      false,
    ],
    [
      "ProvisionedThroughputExceededException",
      () =>
        new ProvisionedThroughputExceededException({
          message: "slow down",
          ...metadata(400),
        }),
      false,
    ],
    [
      "RequestLimitExceeded",
      () => new RequestLimitExceeded({ message: "limit", ...metadata(400) }),
      false,
    ],
    ["ThrottlingException", () => clientFault("ThrottlingException"), false],
    [
      "TransactionConflictException",
      () =>
        new TransactionConflictException({
          message: "conflict",
          ...metadata(400),
        }),
      false,
    ],
    [
      "TransactionInProgressException",
      () =>
        new TransactionInProgressException({
          message: "in progress",
          ...metadata(400),
        }),
      false,
    ],
    [
      "InternalServerError",
      () => new InternalServerError({ message: "internal", ...metadata(500) }),
      false,
    ],
    [
      "a client fault with a 5xx status",
      () => clientFault("ValidationException", 503),
      false,
    ],
    [
      "a client fault after a retry",
      () => clientFault("ConditionalCheckFailedException", 400, 2),
      false,
    ],
    [
      "a client fault without response metadata",
      () =>
        new DynamoDBServiceException({
          name: "ValidationException",
          $fault: "client",
          message: "no response",
          $metadata: {},
        }),
      false,
    ],
    [
      "a duck-typed error with the same name",
      () =>
        Object.assign(new Error("validation"), {
          name: "ValidationException",
          $fault: "client",
          ...metadata(400),
        }),
      false,
    ],
    [
      "an SDK TimeoutError",
      () => Object.assign(new Error("timed out"), { name: "TimeoutError" }),
      false,
    ],
    [
      "an AbortError",
      () => Object.assign(new Error("aborted"), { name: "AbortError" }),
      false,
    ],
    [
      "a network error",
      () =>
        Object.assign(new Error("socket hang up"), {
          code: "ECONNRESET",
          $metadata: {},
        }),
      false,
    ],
  ] as const)("classifies %s as known: %s", (_label, make, known) => {
    expect(isServerReportedQueryError(make())).toBe(known);
  });

  it("follows a wrapping cause", () => {
    expect(
      isServerReportedQueryError(
        new Error("request 1 failed", {
          cause: clientFault("ValidationException"),
        }),
      ),
    ).toBe(true);
  });
});

describe("Elasticsearch query error outcomes", () => {
  it.each([
    ["400", () => responseError(400), true],
    ["401", () => responseError(401), true],
    ["404", () => responseError(404), true],
    ["409 on a document", () => responseError(409), true],
    ["408", () => responseError(408), false],
    ["429", () => responseError(429), false],
    ["499", () => responseError(499), false],
    ["500", () => responseError(500), false],
    ["503", () => responseError(503), false],
    ["400 on _bulk", () => responseError(400, "/_bulk"), false],
    ["400 on index _bulk", () => responseError(400, "/logs/_bulk"), false],
    [
      "409 on _update_by_query",
      () => responseError(409, "/logs/_update_by_query"),
      false,
    ],
    [
      "409 on _delete_by_query",
      () => responseError(409, "/logs/_delete_by_query"),
      false,
    ],
    ["400 on _reindex", () => responseError(400, "/_reindex"), false],
    [
      "400 after a retry",
      () => responseError(400, "/index/_doc/1", { attempts: 1 }),
      false,
    ],
    [
      "400 on an aborted request",
      () => responseError(400, "/index/_doc/1", { aborted: true }),
      false,
    ],
    ["a timeout", () => new errors.TimeoutError("timeout"), false],
    ["a connection error", () => new errors.ConnectionError("reset"), false],
    [
      "no living connections",
      () => new errors.NoLivingConnectionsError("none", {} as never),
      false,
    ],
    ["an aborted request", () => new errors.RequestAbortedError("x"), false],
    [
      "a deserialization error",
      () => new errors.DeserializationError("bad json", "{"),
      false,
    ],
  ] as const)("classifies %s as known: %s", (_label, make, known) => {
    expect(isServerReportedQueryError(make())).toBe(known);
  });
});

describe("requests rejected before sending", () => {
  it("are finished only when explicitly marked", () => {
    expect(isServerReportedQueryError(new Error("bad JSON"))).toBe(false);
    expect(isServerReportedQueryError(markQueryNotSent(new Error("x")))).toBe(
      true,
    );
    const unknown = markQueryNotSent(
      Object.assign(new Error("x"), { executionState: "unknown" }),
    );
    expect(isServerReportedQueryError(unknown)).toBe(false);
  });
});
