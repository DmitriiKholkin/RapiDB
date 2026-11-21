import { DynamoDBServiceException } from "@aws-sdk/client-dynamodb";
import { errors } from "@elastic/elasticsearch";
import { MongoServerError, MongoWriteConcernError } from "mongodb";
import { DatabaseError } from "pg";
import { ErrorReply } from "redis";

// A query outcome is "known" when the database itself answered with an error
// for the request on a live session: the request finished (statements before
// the failing one may have taken effect, the failing one did not complete).
// Transport failures, timeouts, cancellation, session loss, fatal errors and
// errors that leave the effect of the failing statement open stay unknown.

function pgOutcomeKnown(error: DatabaseError): boolean {
  const code = typeof error.code === "string" ? error.code.toUpperCase() : "";
  if (!/^[0-9A-Z]{5}$/.test(code)) return false;
  return !(
    (
      code.startsWith("08") || // connection exception
      code.startsWith("57") || // operator intervention: cancel, shutdown
      code.startsWith("58") || // system / I/O error
      code === "40003" || // statement completion unknown
      code === "25P03" || // idle-in-transaction timeout terminates the session
      code === "XX001" ||
      code === "XX002"
    ) // data / index corruption
  );
}

interface MssqlLikeError {
  name?: unknown;
  code?: unknown;
  number?: unknown;
  class?: unknown;
}

function mssqlOutcomeKnown(error: MssqlLikeError): boolean {
  return (
    error.name === "RequestError" &&
    error.code === "EREQUEST" &&
    typeof error.number === "number" &&
    error.number > 0 &&
    // 3980: batch aborted by a client attention (cancel).
    error.number !== 3980 &&
    // Severity 20+ terminates the connection.
    !(typeof error.class === "number" && error.class >= 20)
  );
}

interface MysqlLikeError {
  code?: unknown;
  errno?: unknown;
  sqlState?: unknown;
  fatal?: unknown;
}

const MYSQL_UNKNOWN_OUTCOME_ERRNOS = new Set([
  1317, // ER_QUERY_INTERRUPTED (KILL QUERY / cancellation)
  1053, // ER_SERVER_SHUTDOWN
  3024, // ER_QUERY_TIMEOUT (max_execution_time)
  1927, // ER_CONNECTION_KILLED (MariaDB)
  1969, // ER_STATEMENT_TIMEOUT (MariaDB)
]);

function mysqlOutcomeKnown(error: MysqlLikeError): boolean {
  return (
    error.fatal !== true &&
    typeof error.errno === "number" &&
    error.errno >= 1000 &&
    // 2000-2999 are client library (CR_*) errors, not server replies.
    (error.errno < 2000 || error.errno >= 3000) &&
    !MYSQL_UNKNOWN_OUTCOME_ERRNOS.has(error.errno) &&
    (typeof error.sqlState === "string" ||
      (typeof error.code === "string" && error.code.startsWith("ER_")))
  );
}

const ORACLE_UNKNOWN_OUTCOME = new Set([
  1013, // user requested cancel
  1089, // immediate shutdown in progress
  3113, // end-of-file on communication channel
  3114, // not connected
  3135, // connection lost contact
  28, // session killed
  2396, // idle time exceeded
  4068, // existing state of packages discarded (package state reset)
]);

function oracleOutcomeKnown(error: {
  errorNum?: unknown;
  code?: unknown;
}): boolean {
  return (
    typeof error.errorNum === "number" &&
    typeof error.code === "string" &&
    error.code.startsWith("ORA-") &&
    error.errorNum > 0 &&
    !(error.errorNum >= 12_000 && error.errorNum < 13_000) && // Net errors
    !ORACLE_UNKNOWN_OUTCOME.has(error.errorNum)
  );
}

function mongoOutcomeKnown(error: MongoServerError): boolean {
  if (error instanceof MongoWriteConcernError) return false;
  if (
    error.hasErrorLabel?.("RetryableWriteError") ||
    error.hasErrorLabel?.("TransientTransactionError") ||
    error.hasErrorLabel?.("UnknownTransactionCommitResult")
  ) {
    return false;
  }
  const code = typeof error.code === "number" ? error.code : -1;
  // Interrupted, MaxTimeMSExpired, ExceededTimeLimit, shutdown / step-down.
  return ![11600, 11601, 11602, 50, 262, 91, 189, 10107, 13435].includes(code);
}

// Endpoints that execute item by item: an error response can follow writes
// that were already applied, and the body is not a complete per-item report.
const ELASTICSEARCH_PARTIAL_ENDPOINT =
  /(?:^|\/)_(?:bulk|update_by_query|delete_by_query|reindex)(?:[/?]|$)/;

function elasticsearchOutcomeKnown(error: errors.ResponseError): boolean {
  const status = error.statusCode;
  const meta = error.meta?.meta as
    | {
        attempts?: unknown;
        aborted?: unknown;
        request?: { params?: { path?: unknown } };
      }
    | undefined;
  const path = meta?.request?.params?.path;
  return (
    typeof status === "number" &&
    status >= 400 &&
    status < 500 &&
    // 408 request timeout, 425 too early, 429 rejected/throttled, 499 client
    // closed request (proxies): the request may have been or still be running.
    ![408, 425, 429, 499].includes(status) &&
    // A retried request may have been applied by an earlier attempt.
    (meta?.attempts === undefined || meta.attempts === 0) &&
    meta?.aborted !== true &&
    typeof path === "string" &&
    !ELASTICSEARCH_PARTIAL_ENDPOINT.test(path)
  );
}

// Client faults that DynamoDB reports without applying the request, except
// for errors the SDK treats as retryable (throttling, conflicts with other
// in-flight transactions) and errors that do not prove the request finished.
const DYNAMODB_UNKNOWN_OUTCOME = new Set([
  "ProvisionedThroughputExceededException",
  "RequestLimitExceeded",
  "ThrottlingException",
  "TransactionConflictException",
  "TransactionInProgressException",
  "ReplicatedWriteConflictException",
  "LimitExceededException",
]);
// TransactionCanceledException cancels the whole transaction; only these
// per-item reasons prove that no conflicting or throttled work is involved.
const DYNAMODB_KNOWN_CANCELLATION_REASONS = new Set([
  "None",
  "ConditionalCheckFailed",
  "ItemCollectionSizeLimitExceeded",
  "ValidationError",
]);

function dynamoOutcomeKnown(error: DynamoDBServiceException): boolean {
  const metadata = error.$metadata as
    | { httpStatusCode?: unknown; attempts?: unknown }
    | undefined;
  const status = metadata?.httpStatusCode;
  if (
    error.$fault !== "client" ||
    typeof status !== "number" ||
    status < 400 ||
    status >= 500 ||
    [408, 429].includes(status) ||
    (metadata?.attempts !== undefined && metadata.attempts !== 1) ||
    DYNAMODB_UNKNOWN_OUTCOME.has(error.name)
  ) {
    return false;
  }
  if (error.name !== "TransactionCanceledException") return true;
  const reasons = (error as { CancellationReasons?: unknown })
    .CancellationReasons;
  return (
    Array.isArray(reasons) &&
    reasons.length > 0 &&
    reasons.every(
      (reason) =>
        typeof reason?.Code === "string" &&
        DYNAMODB_KNOWN_CANCELLATION_REASONS.has(reason.Code),
    )
  );
}

const NOT_SENT = Symbol("rapidb.queryNotSent");

// Marks an error raised while validating a request before anything was sent
// to the database: the run finished without effects.
export function markQueryNotSent(error: unknown): unknown {
  if (error && typeof error === "object") {
    Object.defineProperty(error, NOT_SENT, { value: true });
  }
  return error;
}

function knownOutcome(error: unknown): boolean | undefined {
  if (!error || typeof error !== "object") return undefined;
  const candidate = error as Record<string, unknown>;
  if (candidate.executionState === "unknown") return false;
  if ((error as { [NOT_SENT]?: unknown })[NOT_SENT] === true) return true;
  if (error instanceof AggregateError) return false;
  if (error instanceof errors.ResponseError) {
    return elasticsearchOutcomeKnown(error);
  }
  if (error instanceof errors.ElasticsearchClientError) return false;
  if (error instanceof DynamoDBServiceException) {
    return dynamoOutcomeKnown(error);
  }
  if (error instanceof DatabaseError) return pgOutcomeKnown(error);
  if (error instanceof MongoServerError) return mongoOutcomeKnown(error);
  if (error instanceof ErrorReply) return true;
  if (candidate.name === "RequestError" && "number" in candidate) {
    return mssqlOutcomeKnown(candidate);
  }
  if ("errno" in candidate && "sqlState" in candidate) {
    return mysqlOutcomeKnown(candidate);
  }
  if ("errorNum" in candidate) return oracleOutcomeKnown(candidate);
  if (
    candidate.name === "SqliteError" &&
    typeof candidate.message === "string"
  ) {
    return !/interrupt/i.test(candidate.message);
  }
  return undefined;
}

export function isServerReportedQueryError(error: unknown): boolean {
  const seen = new Set<unknown>();
  let current: unknown = error;
  // Drivers may wrap the native error (line numbers, context) via `cause`.
  while (current && typeof current === "object" && !seen.has(current)) {
    seen.add(current);
    const known = knownOutcome(current);
    if (known !== undefined) return known;
    current = (current as { cause?: unknown }).cause;
  }
  return false;
}
