import type { ConnectionConfig } from "./connectionConfig";
import { CONNECTION_TYPES, type ConnectionType } from "./connectionTypes";

export interface WebviewMessageEnvelope<
  TType extends string = string,
  TPayload = unknown,
> {
  type: TType;
  payload?: TPayload;
}

export type ApplyRowStatus =
  | "applied"
  | "skipped"
  | "prevalidation_failed"
  | "verification_failed";

export interface ApplyRowOutcome {
  rowIndex: number;
  success: boolean;
  status: ApplyRowStatus;
  message?: string;
  columns?: string[];
}

export interface ApplyResultPayload {
  success: boolean;
  error?: string;
  warning?: string;
  failedRows?: number[];
  rowOutcomes?: ApplyRowOutcome[];
  insertApplied?: boolean;
}

export interface QueryInitialState {
  view: "query";
  connectionId: string;
  connectionType?: ConnectionType | "";
  initialSql?: string;
  formatOnOpen?: boolean;
  isBookmarked?: boolean;
}

export interface TableInitialState {
  view: "table";
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  isView?: boolean;
  defaultPageSize?: number;
}

export interface SchemaInitialState {
  view: "schema";
  connectionId: string;
  database: string;
  schema: string;
  table: string;
}

export type SanitizedConnectionConfig = Omit<ConnectionConfig, "password">;

export interface ConnectionFormExistingState extends SanitizedConnectionConfig {
  hasStoredSecret?: boolean;
}

export interface ConnectionFormSubmission extends SanitizedConnectionConfig {
  password?: string;
  hasStoredSecret?: boolean;
}

export interface ConnectionFormInitialState {
  view: "connection";
  existing: ConnectionFormExistingState | null;
}

export type WebviewInitialState =
  | QueryInitialState
  | TableInitialState
  | SchemaInitialState
  | ConnectionFormInitialState;

export type QueryPanelMessage =
  | WebviewMessageEnvelope<"activeConnectionChanged", { connectionId: string }>
  | WebviewMessageEnvelope<
      "executeQuery",
      { sql: string; connectionId?: string }
    >
  | WebviewMessageEnvelope<"getConnections">
  | WebviewMessageEnvelope<"getSchema", { connectionId?: string }>
  | WebviewMessageEnvelope<"exportResultsCSV">
  | WebviewMessageEnvelope<"exportResultsJSON">
  | WebviewMessageEnvelope<"readClipboard">
  | WebviewMessageEnvelope<
      "addBookmark",
      { sql: string; connectionId?: string }
    >;

export interface RowUpdateMessagePayload {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
}

export type TableMutationPreviewKind = "applyChanges" | "insertRow";

export interface TableMutationPreviewPayload {
  previewToken: string;
  kind: TableMutationPreviewKind;
  title: string;
  sql: string;
  statementCount: number;
}

export interface TableMutationPreviewDecisionPayload {
  previewToken: string;
}

export type TablePanelMessage =
  | WebviewMessageEnvelope<"ready">
  | WebviewMessageEnvelope<
      "fetchPage",
      {
        fetchId?: number;
        page?: number | string;
        pageSize?: number | string;
        filters?: unknown;
        sort?: unknown;
      }
    >
  | WebviewMessageEnvelope<
      "applyChanges",
      {
        updates?: RowUpdateMessagePayload[];
        insertValues?: Record<string, unknown>;
      }
    >
  | WebviewMessageEnvelope<"insertRow", { values?: Record<string, unknown> }>
  | WebviewMessageEnvelope<
      "deleteRows",
      { primaryKeysList?: Array<Record<string, unknown>> }
    >
  | WebviewMessageEnvelope<
      "exportCSV",
      {
        sort?: unknown;
        filters?: unknown[];
        limitToPage?: { page: number; pageSize: number };
      }
    >
  | WebviewMessageEnvelope<
      "exportJSON",
      {
        sort?: unknown;
        filters?: unknown[];
        limitToPage?: { page: number; pageSize: number };
      }
    >
  | WebviewMessageEnvelope<"confirmDelete", { count: number }>
  | WebviewMessageEnvelope<
      "confirmMutationPreview",
      TableMutationPreviewDecisionPayload
    >
  | WebviewMessageEnvelope<
      "cancelMutationPreview",
      TableMutationPreviewDecisionPayload
    >;

export type SchemaPanelMessage =
  | WebviewMessageEnvelope<"ready">
  | WebviewMessageEnvelope<
      "openRelatedSchema",
      { table: string; schema?: string; database?: string }
    >;

export type ConnectionFormPanelMessage =
  | WebviewMessageEnvelope<"saveConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"testConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"cancel">;

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function readRequiredString(record: UnknownRecord, key: string): string | null {
  const value = record[key];
  return typeof value === "string" ? value : null;
}

function readOptionalString(
  record: UnknownRecord,
  key: string,
): string | undefined {
  const value = record[key];
  return typeof value === "string" ? value : undefined;
}

function readOptionalBoolean(
  record: UnknownRecord,
  key: string,
): boolean | undefined {
  const value = record[key];
  return typeof value === "boolean" ? value : undefined;
}

function readOptionalNumber(
  record: UnknownRecord,
  key: string,
): number | undefined {
  const value = record[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function readPositiveInteger(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isInteger(value) && value > 0 ? value : undefined;
  }

  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
  }

  return undefined;
}

function readConnectionType(value: unknown): ConnectionType | "" | undefined {
  if (value === "") {
    return "";
  }
  return typeof value === "string" &&
    CONNECTION_TYPES.includes(value as ConnectionType)
    ? (value as ConnectionType)
    : undefined;
}

function parseEnvelope(input: unknown): WebviewMessageEnvelope | null {
  if (!isRecord(input)) {
    return null;
  }
  const type = readRequiredString(input, "type");
  if (!type) {
    return null;
  }
  return {
    type,
    payload: input.payload,
  };
}

function isRowUpdateMessagePayload(
  value: unknown,
): value is RowUpdateMessagePayload {
  return (
    isRecord(value) && isRecord(value.primaryKeys) && isRecord(value.changes)
  );
}

function parseConnectionBase(input: unknown): SanitizedConnectionConfig | null {
  if (!isRecord(input)) {
    return null;
  }

  const id = readRequiredString(input, "id");
  const name = readOptionalString(input, "name");
  const type = readConnectionType(input.type);
  if (!id || name === undefined || type === undefined || type === "") {
    return null;
  }

  return {
    id,
    name,
    type,
    host: readOptionalString(input, "host"),
    port: readOptionalNumber(input, "port"),
    database: readOptionalString(input, "database"),
    username: readOptionalString(input, "username"),
    filePath: readOptionalString(input, "filePath"),
    ssl: readOptionalBoolean(input, "ssl"),
    rejectUnauthorized: readOptionalBoolean(input, "rejectUnauthorized"),
    folder: readOptionalString(input, "folder"),
    serviceName: readOptionalString(input, "serviceName"),
    thickMode: readOptionalBoolean(input, "thickMode"),
    clientPath: readOptionalString(input, "clientPath"),
    useSecretStorage: readOptionalBoolean(input, "useSecretStorage"),
  };
}

export function parseConnectionFormExistingState(
  input: unknown,
): ConnectionFormExistingState | null {
  const base = parseConnectionBase(input);
  if (!base || !isRecord(input)) {
    return null;
  }
  return {
    ...base,
    hasStoredSecret: readOptionalBoolean(input, "hasStoredSecret"),
  };
}

export function parseConnectionFormSubmission(
  input: unknown,
): ConnectionFormSubmission | null {
  const base = parseConnectionBase(input);
  if (!base || !isRecord(input)) {
    return null;
  }
  const password = readOptionalString(input, "password");
  return {
    ...base,
    password,
    hasStoredSecret: readOptionalBoolean(input, "hasStoredSecret"),
  };
}

export function parseWebviewInitialState(
  input: unknown,
): WebviewInitialState | null {
  if (!isRecord(input)) {
    return null;
  }

  switch (input.view) {
    case "query": {
      const connectionId = readRequiredString(input, "connectionId");
      const connectionType = readConnectionType(input.connectionType);
      if (!connectionId || connectionType === undefined) {
        return null;
      }
      return {
        view: "query",
        connectionId,
        connectionType,
        initialSql: readOptionalString(input, "initialSql"),
        formatOnOpen: readOptionalBoolean(input, "formatOnOpen"),
        isBookmarked: readOptionalBoolean(input, "isBookmarked"),
      };
    }

    case "table": {
      const connectionId = readRequiredString(input, "connectionId");
      const database = readOptionalString(input, "database");
      const schema = readOptionalString(input, "schema");
      const table = readRequiredString(input, "table");
      if (
        !connectionId ||
        database === undefined ||
        schema === undefined ||
        !table
      ) {
        return null;
      }
      return {
        view: "table",
        connectionId,
        database,
        schema,
        table,
        isView: readOptionalBoolean(input, "isView"),
        defaultPageSize: readOptionalNumber(input, "defaultPageSize"),
      };
    }

    case "schema": {
      const connectionId = readRequiredString(input, "connectionId");
      const database = readOptionalString(input, "database");
      const schema = readOptionalString(input, "schema");
      const table = readRequiredString(input, "table");
      if (
        !connectionId ||
        database === undefined ||
        schema === undefined ||
        !table
      ) {
        return null;
      }
      return {
        view: "schema",
        connectionId,
        database,
        schema,
        table,
      };
    }

    case "connection": {
      const existing = input.existing;
      if (existing !== null && existing !== undefined) {
        const parsed = parseConnectionFormExistingState(existing);
        if (!parsed) {
          return null;
        }
        return { view: "connection", existing: parsed };
      }
      return { view: "connection", existing: null };
    }

    default:
      return null;
  }
}

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

    case "executeQuery": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const sql = readRequiredString(envelope.payload, "sql");
      if (!sql) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          sql,
          connectionId: readOptionalString(envelope.payload, "connectionId"),
        },
      };
    }

    case "getConnections":
    case "exportResultsCSV":
    case "exportResultsJSON":
    case "readClipboard":
      return { type: envelope.type };

    case "getSchema": {
      if (
        envelope.payload !== undefined &&
        envelope.payload !== null &&
        !isRecord(envelope.payload)
      ) {
        return null;
      }
      return {
        type: envelope.type,
        payload: isRecord(envelope.payload)
          ? {
              connectionId: readOptionalString(
                envelope.payload,
                "connectionId",
              ),
            }
          : {},
      };
    }

    case "addBookmark": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const sql = readRequiredString(envelope.payload, "sql");
      if (!sql) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          sql,
          connectionId: readOptionalString(envelope.payload, "connectionId"),
        },
      };
    }

    default:
      return null;
  }
}

export function parseTablePanelMessage(
  input: unknown,
): TablePanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "ready":
      return { type: envelope.type };

    case "fetchPage": {
      if (
        envelope.payload !== undefined &&
        envelope.payload !== null &&
        !isRecord(envelope.payload)
      ) {
        return null;
      }
      const payload = isRecord(envelope.payload) ? envelope.payload : {};
      return {
        type: envelope.type,
        payload: {
          fetchId: readOptionalNumber(payload, "fetchId"),
          page: payload.page as number | string | undefined,
          pageSize: payload.pageSize as number | string | undefined,
          filters: payload.filters,
          sort: payload.sort,
        },
      };
    }

    case "applyChanges": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const updates = envelope.payload.updates;
      const insertValues = envelope.payload.insertValues;
      if (
        updates !== undefined &&
        (!Array.isArray(updates) ||
          updates.some((item) => !isRowUpdateMessagePayload(item)))
      ) {
        return null;
      }
      if (insertValues !== undefined && !isRecord(insertValues)) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          updates: updates as RowUpdateMessagePayload[] | undefined,
          insertValues: insertValues as Record<string, unknown> | undefined,
        },
      };
    }

    case "insertRow": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const values = envelope.payload.values;
      if (values !== undefined && !isRecord(values)) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          values: (values as Record<string, unknown> | undefined) ?? {},
        },
      };
    }

    case "deleteRows": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const primaryKeysList = envelope.payload.primaryKeysList;
      if (
        primaryKeysList !== undefined &&
        (!Array.isArray(primaryKeysList) ||
          primaryKeysList.some((item) => !isRecord(item)))
      ) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          primaryKeysList: primaryKeysList as
            | Array<Record<string, unknown>>
            | undefined,
        },
      };
    }

    case "exportCSV":
    case "exportJSON": {
      if (
        envelope.payload !== undefined &&
        envelope.payload !== null &&
        !isRecord(envelope.payload)
      ) {
        return null;
      }
      const payload = isRecord(envelope.payload) ? envelope.payload : {};
      if (payload.filters !== undefined && !Array.isArray(payload.filters)) {
        return null;
      }
      let limitToPage: { page: number; pageSize: number } | undefined;
      if (isRecord(payload.limitToPage)) {
        const page = readPositiveInteger(payload.limitToPage.page);
        const pageSize = readPositiveInteger(payload.limitToPage.pageSize);
        if (page !== undefined && pageSize !== undefined) {
          limitToPage = { page, pageSize };
        }
      }
      return {
        type: envelope.type,
        payload: {
          sort: payload.sort,
          filters: payload.filters as unknown[] | undefined,
          limitToPage,
        },
      };
    }

    case "confirmDelete": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const count = readOptionalNumber(envelope.payload, "count");
      return count === undefined
        ? null
        : { type: envelope.type, payload: { count } };
    }

    case "confirmMutationPreview":
    case "cancelMutationPreview": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const previewToken = readRequiredString(envelope.payload, "previewToken");
      return previewToken
        ? { type: envelope.type, payload: { previewToken } }
        : null;
    }

    default:
      return null;
  }
}

export function parseSchemaPanelMessage(
  input: unknown,
): SchemaPanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "ready":
      return { type: envelope.type };

    case "openRelatedSchema": {
      if (!isRecord(envelope.payload)) {
        return null;
      }
      const table = readRequiredString(envelope.payload, "table");
      if (!table) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          table,
          schema: readOptionalString(envelope.payload, "schema"),
          database: readOptionalString(envelope.payload, "database"),
        },
      };
    }

    default:
      return null;
  }
}

export function parseConnectionFormPanelMessage(
  input: unknown,
): ConnectionFormPanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "cancel":
      return { type: envelope.type };

    case "saveConnection":
    case "testConnection": {
      const payload = parseConnectionFormSubmission(envelope.payload);
      return payload ? { type: envelope.type, payload } : null;
    }

    default:
      return null;
  }
}
