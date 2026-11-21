import type { ConnectionType } from "../connectionTypes";
import {
  type QueryDatabaseFields,
  queryDatabaseFields,
  readQueryDatabaseScope,
} from "../queryDatabaseTarget";
import type { QueryColumnMeta } from "../tableTypes";
import type { PanelRetentionState, WebviewMessageEnvelope } from "./shared";
import {
  isRecord,
  parseEnvelope,
  parseEnvelopeQueryPayload,
  parseOptionalPayloadRecord,
  parseRequiredPayloadRecord,
  readConnectionType,
  readOptionalBoolean,
  readOptionalString,
  readRequiredString,
} from "./shared";
import {
  type ClipboardReadPayload,
  type ClipboardWritePayload,
  parseClipboardWritePayload,
} from "./table";

export type QueryEditorLanguage = "sql" | "javascript" | "plaintext" | "json";

export type QueryEditorMode = "sql" | "text";

export type QueryEditorSqlDialect =
  | "postgresql"
  | "mysql"
  | "transactsql"
  | "sqlite"
  | "plsql"
  | "sql";

export interface QueryEditorPresentation {
  queryMode?: QueryEditorMode;
  formatOnOpen?: boolean;
  editorLanguage?: QueryEditorLanguage;
  sqlDialect?: QueryEditorSqlDialect;
  allowFormatting?: boolean;
}

export interface QueryTargetBinding {
  connectionId: string;
  sessionKey: string | null;
}

export interface QueryTargetChangeResult {
  requestId: string;
  accepted: boolean;
  binding: QueryTargetBinding;
}

function readQuerySessionKey(value: unknown): string | undefined {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value)
    ? value
    : undefined;
}

export function parseQueryTargetBinding(
  value: unknown,
): QueryTargetBinding | undefined {
  if (!isRecord(value)) return undefined;
  const connectionId = readRequiredString(value, "connectionId");
  const sessionKey =
    value.sessionKey === null ? null : readQuerySessionKey(value.sessionKey);
  return connectionId && sessionKey !== undefined
    ? { connectionId, sessionKey }
    : undefined;
}

export function parseQueryTargetChangeResult(
  input: unknown,
): QueryTargetChangeResult | undefined {
  if (!isRecord(input) || typeof input.accepted !== "boolean") return;
  const requestId = readRequiredString(input, "requestId");
  const binding = parseQueryTargetBinding(input.binding);
  return requestId && binding
    ? { requestId, accepted: input.accepted, binding }
    : undefined;
}

export interface QueryInitialState
  extends PanelRetentionState,
    QueryDatabaseFields {
  view: "query";
  panelId?: string;
  connectionId: string;
  connectionType?: ConnectionType | "";
  targetBinding?: QueryTargetBinding;
  queryRunStatus?: boolean;
  queryText?: string;
  initialSql?: string;
  formatOnOpen?: boolean;
  isBookmarked?: boolean;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
}

const QUERY_EDITOR_LANGUAGES: ReadonlySet<QueryEditorLanguage> = new Set([
  "sql",
  "javascript",
  "plaintext",
  "json",
]);

const QUERY_EDITOR_MODES: ReadonlySet<QueryEditorMode> = new Set([
  "sql",
  "text",
]);

const QUERY_EDITOR_SQL_DIALECTS: ReadonlySet<QueryEditorSqlDialect> = new Set([
  "postgresql",
  "mysql",
  "transactsql",
  "sqlite",
  "plsql",
  "sql",
]);

function readQueryEditorLanguage(
  value: unknown,
): QueryEditorLanguage | undefined {
  return typeof value === "string" &&
    QUERY_EDITOR_LANGUAGES.has(value as QueryEditorLanguage)
    ? (value as QueryEditorLanguage)
    : undefined;
}

function readQueryEditorMode(value: unknown): QueryEditorMode | undefined {
  return typeof value === "string" &&
    QUERY_EDITOR_MODES.has(value as QueryEditorMode)
    ? (value as QueryEditorMode)
    : undefined;
}

function readQueryEditorSqlDialect(
  value: unknown,
): QueryEditorSqlDialect | undefined {
  return typeof value === "string" &&
    QUERY_EDITOR_SQL_DIALECTS.has(value as QueryEditorSqlDialect)
    ? (value as QueryEditorSqlDialect)
    : undefined;
}

function isEmptyPresentation(presentation: QueryEditorPresentation): boolean {
  return (
    presentation.queryMode === undefined &&
    presentation.formatOnOpen === undefined &&
    presentation.editorLanguage === undefined &&
    presentation.sqlDialect === undefined &&
    presentation.allowFormatting === undefined
  );
}

function readQueryEditorPresentation(
  value: unknown,
): QueryEditorPresentation | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const presentation: QueryEditorPresentation = {
    queryMode: readQueryEditorMode(value.queryMode),
    formatOnOpen: readOptionalBoolean(value, "formatOnOpen"),
    editorLanguage: readQueryEditorLanguage(value.editorLanguage),
    sqlDialect: readQueryEditorSqlDialect(value.sqlDialect),
    allowFormatting: readOptionalBoolean(value, "allowFormatting"),
  };
  return isEmptyPresentation(presentation) ? undefined : presentation;
}

export function parseQueryInitialState(
  input: Record<string, unknown>,
): QueryInitialState | null {
  const connectionId = readRequiredString(input, "connectionId");
  const connectionType = readConnectionType(input.connectionType);
  const databaseScope = readQueryDatabaseScope(input);
  if (!connectionId || connectionType === undefined || databaseScope === null) {
    return null;
  }

  const editorPresentation = readQueryEditorPresentation(
    input.editorPresentation,
  );

  const queryMode =
    editorPresentation?.queryMode ?? readQueryEditorMode(input.queryMode);
  const formatOnOpen =
    editorPresentation?.formatOnOpen ??
    readOptionalBoolean(input, "formatOnOpen");
  const editorLanguage =
    editorPresentation?.editorLanguage ??
    readQueryEditorLanguage(input.editorLanguage);
  const sqlDialect =
    editorPresentation?.sqlDialect ??
    readQueryEditorSqlDialect(input.sqlDialect);
  const allowFormatting =
    editorPresentation?.allowFormatting ??
    readOptionalBoolean(input, "allowFormatting");

  const queryText =
    readOptionalString(input, "queryText") ??
    readOptionalString(input, "initialSql");

  const finalPresentation: QueryEditorPresentation | undefined = (() => {
    const candidate: QueryEditorPresentation = {
      queryMode,
      formatOnOpen,
      editorLanguage,
      sqlDialect,
      allowFormatting,
    };
    return isEmptyPresentation(candidate) ? undefined : candidate;
  })();

  return {
    view: "query",
    panelId: readOptionalString(input, "panelId"),
    connectionId,
    connectionType,
    ...(input.targetBinding !== undefined
      ? {
          targetBinding: parseQueryTargetBinding(input.targetBinding) ?? {
            connectionId,
            sessionKey: null,
          },
        }
      : {}),
    ...queryDatabaseFields(databaseScope),
    ...(input.queryRunStatus === true ? { queryRunStatus: true } : {}),
    queryText,
    initialSql: queryText,
    formatOnOpen,
    isBookmarked: readOptionalBoolean(input, "isBookmarked"),
    editorLanguage,
    editorPresentation: finalPresentation,
  };
}

export interface QueryResultExportPayload {
  columnOrder?: string[];
  resultSetIndex?: number;
  sort?: { id: string; desc: boolean }[];
}

export interface QuerySchemaPayload<TSchemaObject> {
  connectionId: string;
  schema: TSchemaObject[];
  error?: string;
}

export interface QueryResultCounts {
  rowCount: number;
  /** This driver always uses rowCount for returned rows, even without a rowset. */
  rowCountKind?: "returned";
  /** Confirmed count, including zero. On partial results this excludes failed requests. */
  affectedRows?: number;
  /** Batch APIs confirm processed writes, not whether every delete found an item. */
  affectedRowsKind?: "processed-writes";
  /** No exact affected count is available for the reported result. */
  affectedRowsUnknown?: boolean;
}

export interface QueryResultSet {
  id: string;
  columns: string[];
  columnMeta?: QueryColumnMeta[];
  rows: Record<string, unknown>[];
  rowCount: number;
  affectedRows?: number;
  truncated?: boolean;
  truncatedBySize?: boolean;
  truncatedAt?: number;
  partial?: boolean;
}

export interface QueryBookmarkPayload {
  queryText: string;
  sql?: string;
  connectionId?: string;
  targetSessionKey?: string;
  requestId?: string;
}

export interface BookmarkSavedPayload {
  ok: boolean;
  error?: string;
  requestId?: string;
}

export function parseBookmarkSavedPayload(
  input: unknown,
): BookmarkSavedPayload | null {
  if (!isRecord(input) || typeof input.ok !== "boolean") {
    return null;
  }
  const requestId = readOptionalString(input, "requestId");
  if (input.requestId !== undefined && !requestId?.trim()) {
    return null;
  }
  if (input.error !== undefined && typeof input.error !== "string") {
    return null;
  }
  return {
    ok: input.ok,
    ...(requestId !== undefined ? { requestId } : {}),
    ...(input.error !== undefined ? { error: input.error } : {}),
  };
}

function isQueryColumnId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^__col_(0|[1-9]\d*)$/.test(value) &&
    Number.isSafeInteger(Number(value.slice(6)))
  );
}

export interface QueryOperationIdentity {
  connectionId: string;
  operationId: string;
}

export interface QueryRunIdentity
  extends QueryOperationIdentity,
    QueryDatabaseFields {
  panelId: string;
  targetSessionKey: string | null;
}

export type QueryRunOutcome = "completed" | "not-run" | "unknown";
export type QueryRunStatus = "accepted" | "running" | QueryRunOutcome;

export interface QueryRunStatusRequest {
  run: QueryRunIdentity;
  requestId: string;
}

export interface QueryRunStatusReply extends QueryRunStatusRequest {
  status: QueryRunStatus;
}

function readRunId(
  input: Record<string, unknown>,
  key: string,
): string | undefined {
  const id = readRequiredString(input, key);
  return id && id.length <= 256 ? id : undefined;
}

export function parseQueryRunIdentity(
  input: unknown,
): QueryRunIdentity | undefined {
  if (!isRecord(input)) return;
  const panelId = readRunId(input, "panelId");
  const operationId = readRunId(input, "operationId");
  const connectionId = readRunId(input, "connectionId");
  const databaseScope = readQueryDatabaseScope(input);
  const targetSessionKey =
    input.targetSessionKey === null
      ? null
      : readQuerySessionKey(input.targetSessionKey);
  if (
    !panelId ||
    !operationId ||
    !connectionId ||
    databaseScope === null ||
    targetSessionKey === undefined
  )
    return;
  return {
    panelId,
    operationId,
    connectionId,
    targetSessionKey,
    ...queryDatabaseFields(databaseScope),
  };
}

export function sameQueryRun(
  left: QueryRunIdentity,
  right: QueryRunIdentity,
): boolean {
  return (
    left.panelId === right.panelId &&
    left.operationId === right.operationId &&
    left.connectionId === right.connectionId &&
    left.targetSessionKey === right.targetSessionKey &&
    left.postgresDatabase === right.postgresDatabase &&
    left.mssqlDatabase === right.mssqlDatabase
  );
}

function parseQueryRunStatusRequest(
  input: unknown,
): QueryRunStatusRequest | undefined {
  if (!isRecord(input)) return;
  const requestId = readRunId(input, "requestId");
  const run = parseQueryRunIdentity(input.run);
  if (requestId && run) return { requestId, run };
}

export function parseQueryRunOutcome(
  input: unknown,
): QueryRunOutcome | undefined {
  if (input === "completed" || input === "not-run" || input === "unknown")
    return input;
}

export function parseQueryRunStatusReply(
  input: unknown,
): QueryRunStatusReply | undefined {
  if (!isRecord(input)) return;
  const request = parseQueryRunStatusRequest(input);
  const status =
    input.status === "accepted" || input.status === "running"
      ? input.status
      : parseQueryRunOutcome(input.status);
  if (request && status) return { ...request, status };
}

export type QueryPanelMessage =
  | WebviewMessageEnvelope<
      "activeConnectionChanged",
      { connectionId: string; targetSessionKey?: string; requestId?: string }
    >
  | WebviewMessageEnvelope<
      "executeQuery",
      {
        queryText: string;
        sql?: string;
        connectionId?: string;
        targetSessionKey?: string;
        operationId?: string;
        run?: QueryRunIdentity;
      }
    >
  | WebviewMessageEnvelope<
      "cancelQuery",
      QueryOperationIdentity & { run?: QueryRunIdentity }
    >
  | WebviewMessageEnvelope<"getQueryRunStatus", QueryRunStatusRequest>
  | WebviewMessageEnvelope<"getConnections">
  | WebviewMessageEnvelope<"getSchema", { connectionId?: string }>
  | WebviewMessageEnvelope<"exportResultsCSV", QueryResultExportPayload>
  | WebviewMessageEnvelope<"exportResultsJSON", QueryResultExportPayload>
  | WebviewMessageEnvelope<"readClipboard", ClipboardReadPayload>
  | WebviewMessageEnvelope<"writeClipboard", ClipboardWritePayload>
  | WebviewMessageEnvelope<"addBookmark", QueryBookmarkPayload>;

export function parseQueryPanelMessage(
  input: unknown,
): QueryPanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "activeConnectionChanged": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const connectionId = readRequiredString(envelope.payload, "connectionId");
      const requestId = readRequiredString(envelope.payload, "requestId");
      if (envelope.payload.requestId !== undefined && !requestId) return null;
      const targetSessionKey = readQuerySessionKey(
        envelope.payload.targetSessionKey,
      );
      return connectionId
        ? {
            type: envelope.type,
            payload: {
              connectionId,
              ...(targetSessionKey !== undefined ? { targetSessionKey } : {}),
              ...(requestId ? { requestId } : {}),
            },
          }
        : null;
    }

    case "addBookmark": {
      const payload = parseEnvelopeQueryPayload(envelope);
      if (!payload || !isRecord(envelope.payload)) {
        return null;
      }
      const requestId = readOptionalString(envelope.payload, "requestId");
      const targetSessionKey = readQuerySessionKey(
        envelope.payload.targetSessionKey,
      );
      if (envelope.payload.requestId !== undefined && !requestId?.trim()) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          ...payload,
          ...(targetSessionKey !== undefined ? { targetSessionKey } : {}),
          ...(requestId !== undefined ? { requestId } : {}),
        },
      };
    }

    case "executeQuery": {
      const payload = parseEnvelopeQueryPayload(envelope);
      if (!payload || !isRecord(envelope.payload)) return null;
      const targetSessionKey = readQuerySessionKey(
        envelope.payload.targetSessionKey,
      );
      const operationId = readOptionalString(envelope.payload, "operationId");
      const run = parseQueryRunIdentity(envelope.payload.run);
      if (
        envelope.payload.run !== undefined &&
        (!run ||
          run.connectionId !== payload.connectionId ||
          run.operationId !== operationId ||
          (envelope.payload.targetSessionKey !== undefined &&
            targetSessionKey === undefined) ||
          run.targetSessionKey !== (targetSessionKey ?? null))
      )
        return null;
      return {
        type: envelope.type,
        payload: {
          ...payload,
          operationId,
          ...(targetSessionKey !== undefined ? { targetSessionKey } : {}),
          ...(run ? { run } : {}),
        },
      };
    }

    case "cancelQuery": {
      if (!isRecord(envelope.payload)) return null;
      const connectionId = readRequiredString(envelope.payload, "connectionId");
      const operationId = readRequiredString(envelope.payload, "operationId");
      const run = parseQueryRunIdentity(envelope.payload.run);
      if (
        envelope.payload.run !== undefined &&
        (!run ||
          run.connectionId !== connectionId ||
          run.operationId !== operationId)
      )
        return null;
      return connectionId && operationId
        ? {
            type: envelope.type,
            payload: { connectionId, operationId, ...(run ? { run } : {}) },
          }
        : null;
    }

    case "getQueryRunStatus": {
      const payload = parseQueryRunStatusRequest(envelope.payload);
      return payload ? { type: envelope.type, payload } : null;
    }

    case "getConnections":
      return { type: envelope.type };

    case "readClipboard": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const requestId = readRequiredString(payload, "requestId");
      const recipient = readRequiredString(payload, "recipient");
      return requestId && recipient
        ? { type: envelope.type, payload: { requestId, recipient } }
        : null;
    }

    case "exportResultsCSV":
    case "exportResultsJSON": {
      const payload = parseOptionalPayloadRecord(envelope);
      if (!payload) return null;
      const { columnOrder, resultSetIndex, sort } = payload;
      if (
        columnOrder !== undefined &&
        (!Array.isArray(columnOrder) ||
          !columnOrder.every(isQueryColumnId) ||
          new Set(columnOrder).size !== columnOrder.length)
      ) {
        return null;
      }
      if (
        resultSetIndex !== undefined &&
        (typeof resultSetIndex !== "number" ||
          !Number.isSafeInteger(resultSetIndex) ||
          resultSetIndex < 0)
      ) {
        return null;
      }
      if (
        sort !== undefined &&
        (!Array.isArray(sort) ||
          !sort.every(
            (rule) =>
              isRecord(rule) &&
              isQueryColumnId(rule.id) &&
              typeof rule.desc === "boolean",
          ) ||
          new Set(sort.map((rule) => rule.id)).size !== sort.length)
      ) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          ...(columnOrder !== undefined ? { columnOrder } : {}),
          ...(resultSetIndex !== undefined ? { resultSetIndex } : {}),
          ...(sort !== undefined
            ? { sort: sort.map(({ id, desc }) => ({ id, desc })) }
            : {}),
        },
      };
    }

    case "writeClipboard": {
      const payload = parseClipboardWritePayload(envelope.payload);
      return payload ? { type: envelope.type, payload } : null;
    }

    case "getSchema": {
      const payload = parseOptionalPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          connectionId: readOptionalString(payload, "connectionId"),
        },
      };
    }

    default:
      return null;
  }
}
