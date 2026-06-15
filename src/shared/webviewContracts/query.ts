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
  readConnectionType,
  readOptionalBoolean,
  readOptionalString,
  readRequiredString,
} from "./shared";

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

export type QueryPanelMessage =
  | WebviewMessageEnvelope<"activeConnectionChanged", { connectionId: string }>
  | WebviewMessageEnvelope<
      "executeQuery",
      { queryText: string; sql?: string; connectionId?: string }
    >
  | WebviewMessageEnvelope<"getConnections">
  | WebviewMessageEnvelope<"getSchema", { connectionId?: string }>
  | WebviewMessageEnvelope<
      "exportResultsCSV",
      { columnOrder?: string[]; sort?: { column: string; desc: boolean }[] }
    >
  | WebviewMessageEnvelope<
      "exportResultsJSON",
      { columnOrder?: string[]; sort?: { column: string; desc: boolean }[] }
    >
  | WebviewMessageEnvelope<"readClipboard">
  | WebviewMessageEnvelope<"writeClipboard", { text: string }>
  | WebviewMessageEnvelope<
      "addBookmark",
      { queryText: string; sql?: string; connectionId?: string }
    >;

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

    case "executeQuery":
    case "addBookmark": {
      const payload = parseEnvelopeQueryPayload(envelope);
      return payload ? { type: envelope.type, payload } : null;
    }

    case "getConnections":
    case "readClipboard":
      return { type: envelope.type };

    case "exportResultsCSV":
    case "exportResultsJSON": {
      const payload = parseOptionalPayloadRecord(envelope);
      return { type: envelope.type, payload: payload ?? undefined };
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
