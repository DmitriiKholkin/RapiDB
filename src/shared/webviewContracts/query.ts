/**
 * Query panel types, initial state, and message parser.
 *
 * All parsers are pure; `null` indicates a parse failure (caller
 * should surface UX error).
 */
import type { ConnectionType } from "../connectionTypes";
import type { PanelRetentionState, WebviewMessageEnvelope } from "./shared";
import {
  isRecord,
  parseEnvelope,
  parseEnvelopeQueryPayload,
  parseEnvelopeTextPayload,
  parseOptionalPayloadRecord,
  parseRequiredPayloadRecord,
  readConnectionType,
  readOptionalBoolean,
  readOptionalString,
  readRequiredString,
} from "./shared";
import type { ClipboardReadPayload } from "./table";

// ─── Editor Types ───────────────────────────────────────────────────────────

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

// ─── Initial State ──────────────────────────────────────────────────────────

export interface QueryInitialState extends PanelRetentionState {
  view: "query";
  panelId?: string;
  connectionId: string;
  connectionType?: ConnectionType | "";
  queryText?: string;
  initialSql?: string;
  formatOnOpen?: boolean;
  isBookmarked?: boolean;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
}

// ─── Parser Helpers ─────────────────────────────────────────────────────────

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

/** True if every presentation field is absent (lets the caller drop the wrapper). */
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

// ─── Initial State Parser ───────────────────────────────────────────────────

export function parseQueryInitialState(
  input: Record<string, unknown>,
): QueryInitialState | null {
  const connectionId = readRequiredString(input, "connectionId");
  const connectionType = readConnectionType(input.connectionType);
  if (!connectionId || connectionType === undefined) {
    return null;
  }

  // Editor presentation may be embedded in `editorPresentation` or in
  // top-level fields. Prefer the embedded form when present.
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

  // Only keep `editorPresentation` when at least one field is set.
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
    queryText,
    initialSql: queryText,
    formatOnOpen,
    isBookmarked: readOptionalBoolean(input, "isBookmarked"),
    editorLanguage,
    editorPresentation: finalPresentation,
  };
}

// ─── Messages ───────────────────────────────────────────────────────────────

export interface QueryResultExportPayload {
  // IDs refer to original result positions (__col_0, __col_1), not display names.
  columnOrder?: string[];
  sort?: { id: string; desc: boolean }[];
}

export interface QueryBookmarkPayload {
  queryText: string;
  sql?: string;
  connectionId?: string;
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

export type QueryPanelMessage =
  | WebviewMessageEnvelope<"activeConnectionChanged", { connectionId: string }>
  | WebviewMessageEnvelope<
      "executeQuery",
      {
        queryText: string;
        sql?: string;
        connectionId?: string;
        operationId?: string;
      }
    >
  | WebviewMessageEnvelope<"getConnections">
  | WebviewMessageEnvelope<"getSchema", { connectionId?: string }>
  | WebviewMessageEnvelope<"exportResultsCSV", QueryResultExportPayload>
  | WebviewMessageEnvelope<"exportResultsJSON", QueryResultExportPayload>
  | WebviewMessageEnvelope<"readClipboard", ClipboardReadPayload>
  | WebviewMessageEnvelope<"writeClipboard", { text: string }>
  | WebviewMessageEnvelope<"addBookmark", QueryBookmarkPayload>;

// ─── Message Parser ─────────────────────────────────────────────────────────

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
      return connectionId
        ? { type: envelope.type, payload: { connectionId } }
        : null;
    }

    case "addBookmark": {
      const payload = parseEnvelopeQueryPayload(envelope);
      if (!payload || !isRecord(envelope.payload)) {
        return null;
      }
      const requestId = readOptionalString(envelope.payload, "requestId");
      if (envelope.payload.requestId !== undefined && !requestId?.trim()) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          ...payload,
          ...(requestId !== undefined ? { requestId } : {}),
        },
      };
    }

    case "executeQuery": {
      const payload = parseEnvelopeQueryPayload(envelope);
      return payload
        ? {
            type: envelope.type,
            payload: {
              ...payload,
              ...(envelope.type === "executeQuery" && isRecord(envelope.payload)
                ? {
                    operationId: readOptionalString(
                      envelope.payload,
                      "operationId",
                    ),
                  }
                : {}),
            },
          }
        : null;
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
      const { columnOrder, sort } = payload;
      if (
        columnOrder !== undefined &&
        (!Array.isArray(columnOrder) ||
          !columnOrder.every(isQueryColumnId) ||
          new Set(columnOrder).size !== columnOrder.length)
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
          ...(sort !== undefined
            ? { sort: sort.map(({ id, desc }) => ({ id, desc })) }
            : {}),
        },
      };
    }

    case "writeClipboard": {
      const payload = parseEnvelopeTextPayload(envelope);
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
