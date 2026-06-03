import {
  CONNECTION_TLS_MODES,
  type ConnectionConfig,
  type ConnectionSshConfig,
  type ConnectionTlsMode,
} from "./connectionConfig";
import { CONNECTION_TYPES, type ConnectionType } from "./connectionTypes";
import type { PrimaryKeyRole } from "./tableTypes";

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

export type PanelRetentionMode = "retain" | "rehydrate";

interface PanelRetentionState {
  panelRetentionMode?: PanelRetentionMode;
}

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

export interface TableInitialState extends PanelRetentionState {
  view: "table";
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  isView?: boolean;
  connectionReadOnly?: boolean;
  defaultPageSize?: number;
}

export interface ErdInitialState extends PanelRetentionState {
  view: "erd";
  connectionId: string;
  database?: string;
  schema?: string;
}

export interface ErdNodeColumn {
  name: string;
  type: string;
  isPrimaryKey: boolean;
  primaryKeyRole?: PrimaryKeyRole;
  isForeignKey: boolean;
  nullable: boolean;
}

export interface ErdNodePosition {
  x: number;
  y: number;
}

export interface ErdTableNode {
  id: string;
  database: string;
  schema: string;
  table: string;
  isView: boolean;
  columns: ErdNodeColumn[];
  position: ErdNodePosition;
}

export interface ErdRelationshipEdge {
  id: string;
  fromTableId: string;
  toTableId: string;
  fromColumn: string;
  toColumn: string;
  constraintName: string;
  cardinality: "one-to-one" | "many-to-one" | "unknown";
  sourceNullable: boolean;
}

export interface ErdGraphScope {
  database?: string;
  schema?: string;
}

export interface ErdGraph {
  nodes: ErdTableNode[];
  edges: ErdRelationshipEdge[];
  scope: ErdGraphScope;
}

export type SanitizedSshConfig = Omit<
  ConnectionSshConfig,
  "password" | "privateKey" | "passphrase"
>;

export type SanitizedConnectionConfig = Omit<
  ConnectionConfig,
  "password" | "ssh"
> & {
  ssh?: SanitizedSshConfig;
};

export interface ConnectionFormExistingState extends SanitizedConnectionConfig {
  hasStoredSecret?: boolean;
  hasStoredApiKey?: boolean;
  hasStoredSshPassword?: boolean;
  hasStoredSshPrivateKey?: boolean;
  hasStoredSshPassphrase?: boolean;
  hasStoredTlsKeyPassphrase?: boolean;
}

export interface ConnectionFormSubmission extends SanitizedConnectionConfig {
  password?: string;
  hasStoredApiKey?: boolean;
  ssh?: ConnectionSshConfig;
  hasStoredSecret?: boolean;
  hasStoredSshPassword?: boolean;
  hasStoredSshPrivateKey?: boolean;
  hasStoredSshPassphrase?: boolean;
  hasStoredTlsKeyPassphrase?: boolean;
}

export type ConnectionFormBrowseTarget =
  | "filePath"
  | "tlsCaFile"
  | "tlsCertFile"
  | "tlsKeyFile";

export interface ConnectionFormInitialState extends PanelRetentionState {
  view: "connection";
  existing: ConnectionFormExistingState | null;
}

export type WebviewInitialState =
  | QueryInitialState
  | TableInitialState
  | ErdInitialState
  | ConnectionFormInitialState;

export type QueryPanelMessage =
  | WebviewMessageEnvelope<"activeConnectionChanged", { connectionId: string }>
  | WebviewMessageEnvelope<
      "executeQuery",
      { queryText: string; sql?: string; connectionId?: string }
    >
  | WebviewMessageEnvelope<"getConnections">
  | WebviewMessageEnvelope<"getSchema", { connectionId?: string }>
  | WebviewMessageEnvelope<"exportResultsCSV">
  | WebviewMessageEnvelope<"exportResultsJSON">
  | WebviewMessageEnvelope<"readClipboard">
  | WebviewMessageEnvelope<"writeClipboard", { text: string }>
  | WebviewMessageEnvelope<
      "addBookmark",
      { queryText: string; sql?: string; connectionId?: string }
    >;

export interface RowUpdateMessagePayload {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
}

export type TableMutationPreviewKind =
  | "applyChanges"
  | "insertRow"
  | "deleteRows";

export interface TableMutationPreviewPayload {
  previewToken: string;
  kind: TableMutationPreviewKind;
  title: string;
  text: string;
  contentType: "application/sql" | "application/json" | "text/plain";
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
  | WebviewMessageEnvelope<
      "confirmMutationPreview",
      TableMutationPreviewDecisionPayload
    >
  | WebviewMessageEnvelope<
      "cancelMutationPreview",
      TableMutationPreviewDecisionPayload
    >
  | WebviewMessageEnvelope<"readClipboard">
  | WebviewMessageEnvelope<"writeClipboard", { text: string }>;

export type ErdPanelMessage =
  | WebviewMessageEnvelope<"ready">
  | WebviewMessageEnvelope<"reload">
  | WebviewMessageEnvelope<
      "openTableData",
      { table: string; schema?: string; database?: string; isView?: boolean }
    >;

export type ConnectionFormPanelMessage =
  | WebviewMessageEnvelope<"saveConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"testConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"cancel">
  | WebviewMessageEnvelope<
      "browseFile",
      { target: ConnectionFormBrowseTarget }
    >;

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

function readConnectionSshAuthMethod(
  value: unknown,
): ConnectionSshConfig["authMethod"] | undefined {
  return value === "password" || value === "privateKey" ? value : undefined;
}

function readConnectionSshHostVerificationMode(
  value: unknown,
): ConnectionSshConfig["hostVerificationMode"] | undefined {
  return value === "manual" || value === "trustOnFirstUse" ? value : undefined;
}

function readSqliteWalMode(
  value: unknown,
): ConnectionConfig["sqliteWalMode"] | undefined {
  return value === "auto" || value === "off" ? value : undefined;
}

function readConnectionTlsMode(value: unknown): ConnectionTlsMode | undefined {
  return typeof value === "string" &&
    CONNECTION_TLS_MODES.includes(value as ConnectionTlsMode)
    ? (value as ConnectionTlsMode)
    : undefined;
}

function readConnectionTlsConfig(
  value: unknown,
): ConnectionConfig["tls"] | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const mode = readConnectionTlsMode(value.mode);
  if (mode === undefined) {
    return undefined;
  }

  return {
    mode,
    caFilePath: readOptionalString(value, "caFilePath"),
    certFilePath: readOptionalString(value, "certFilePath"),
    keyFilePath: readOptionalString(value, "keyFilePath"),
    keyPassphrase: readOptionalString(value, "keyPassphrase"),
    serverNameOverride: readOptionalString(value, "serverNameOverride"),
  };
}

function readConnectionFormBrowseTarget(
  value: unknown,
): ConnectionFormBrowseTarget | undefined {
  return value === "filePath" ||
    value === "tlsCaFile" ||
    value === "tlsCertFile" ||
    value === "tlsKeyFile"
    ? value
    : undefined;
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

function readQueryEditorLanguage(
  value: unknown,
): QueryEditorLanguage | undefined {
  return value === "sql" ||
    value === "javascript" ||
    value === "plaintext" ||
    value === "json"
    ? value
    : undefined;
}

function readQueryEditorMode(value: unknown): QueryEditorMode | undefined {
  return value === "sql" || value === "text" ? value : undefined;
}

function readQueryEditorSqlDialect(
  value: unknown,
): QueryEditorSqlDialect | undefined {
  return value === "postgresql" ||
    value === "mysql" ||
    value === "transactsql" ||
    value === "sqlite" ||
    value === "plsql" ||
    value === "sql"
    ? value
    : undefined;
}

function readQueryEditorPresentation(
  value: unknown,
): QueryEditorPresentation | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const formatOnOpen = readOptionalBoolean(value, "formatOnOpen");
  const queryMode = readQueryEditorMode(value.queryMode);
  const editorLanguage = readQueryEditorLanguage(value.editorLanguage);
  const sqlDialect = readQueryEditorSqlDialect(value.sqlDialect);
  const allowFormatting = readOptionalBoolean(value, "allowFormatting");

  if (
    queryMode === undefined &&
    formatOnOpen === undefined &&
    editorLanguage === undefined &&
    sqlDialect === undefined &&
    allowFormatting === undefined
  ) {
    return undefined;
  }

  return {
    queryMode,
    formatOnOpen,
    editorLanguage,
    sqlDialect,
    allowFormatting,
  };
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

function parseRequiredPayloadRecord(
  envelope: WebviewMessageEnvelope,
): UnknownRecord | null {
  return isRecord(envelope.payload) ? envelope.payload : null;
}

function parseOptionalPayloadRecord(
  envelope: WebviewMessageEnvelope,
): UnknownRecord | null {
  if (envelope.payload === undefined || envelope.payload === null) {
    return {};
  }
  return isRecord(envelope.payload) ? envelope.payload : null;
}

function parseEnvelopeTextPayload(
  envelope: WebviewMessageEnvelope,
): { text: string } | null {
  const payload = parseRequiredPayloadRecord(envelope);
  if (!payload) {
    return null;
  }

  const text = readRequiredString(payload, "text");
  return text !== null ? { text } : null;
}

function parseEnvelopeQueryPayload(
  envelope: WebviewMessageEnvelope,
): { queryText: string; sql: string; connectionId?: string } | null {
  const payload = parseRequiredPayloadRecord(envelope);
  if (!payload) {
    return null;
  }

  const queryText =
    readRequiredString(payload, "queryText") ??
    readRequiredString(payload, "sql");
  if (!queryText) {
    return null;
  }

  return {
    queryText,
    sql: queryText,
    connectionId: readOptionalString(payload, "connectionId"),
  };
}

function isRowUpdateMessagePayload(
  value: unknown,
): value is RowUpdateMessagePayload {
  return (
    isRecord(value) && isRecord(value.primaryKeys) && isRecord(value.changes)
  );
}

function parseConnectionSshConfig(
  value: unknown,
): ConnectionSshConfig | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  return {
    host: readOptionalString(value, "host"),
    port: readOptionalNumber(value, "port"),
    username: readOptionalString(value, "username"),
    authMethod: readConnectionSshAuthMethod(value.authMethod),
    hostVerificationMode: readConnectionSshHostVerificationMode(
      value.hostVerificationMode,
    ),
    password: readOptionalString(value, "password"),
    privateKey: readOptionalString(value, "privateKey"),
    passphrase: readOptionalString(value, "passphrase"),
    hostFingerprintSha256: readOptionalString(value, "hostFingerprintSha256"),
  };
}

function parseConnectionBase(input: unknown): SanitizedConnectionConfig | null {
  if (!isRecord(input)) {
    return null;
  }

  const id = readRequiredString(input, "id");
  const name = readOptionalString(input, "name");
  const type = readConnectionType(input.type);
  const sqliteWalMode = readSqliteWalMode(input.sqliteWalMode);
  if (!id || name === undefined || type === undefined || type === "") {
    return null;
  }

  const database = readOptionalString(input, "database");
  const serviceName = readOptionalString(input, "serviceName");
  const normalizedOracleServiceName =
    type === "oracle"
      ? serviceName?.trim() || database?.trim() || undefined
      : serviceName;

  return {
    id,
    name,
    type,
    readOnly: readOptionalBoolean(input, "readOnly"),
    host: readOptionalString(input, "host"),
    port: readOptionalNumber(input, "port"),
    database: type === "oracle" ? undefined : database,
    username: readOptionalString(input, "username"),
    filePath: readOptionalString(input, "filePath"),
    tls: readConnectionTlsConfig(input.tls),
    folder: readOptionalString(input, "folder"),
    serviceName: normalizedOracleServiceName,
    connectionUri:
      readOptionalString(input, "connectionUri") ??
      readOptionalString(input, "uri"),
    replicaSet: readOptionalString(input, "replicaSet"),
    directConnection: readOptionalBoolean(input, "directConnection"),
    ...(type === "sqlite" ? { sqliteWalMode: sqliteWalMode ?? "auto" } : {}),
    awsProfile: readOptionalString(input, "awsProfile"),
    endpoint:
      readOptionalString(input, "endpoint") ??
      readOptionalString(input, "awsEndpoint"),
    apiKey: readOptionalString(input, "apiKey"),
    cloudId: readOptionalString(input, "cloudId"),
    uri: readOptionalString(input, "uri"),
    authSource: readOptionalString(input, "authSource"),
    awsRegion: readOptionalString(input, "awsRegion"),
    awsAccessKeyId: readOptionalString(input, "awsAccessKeyId"),
    awsSecretAccessKey: readOptionalString(input, "awsSecretAccessKey"),
    awsSessionToken: readOptionalString(input, "awsSessionToken"),
    awsEndpoint: readOptionalString(input, "awsEndpoint"),
    ssh: parseConnectionSshConfig(input.ssh),
    useSecretStorage: readOptionalBoolean(input, "useSecretStorage"),
    color: readOptionalString(input, "color"),
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
    hasStoredApiKey: readOptionalBoolean(input, "hasStoredApiKey"),
    hasStoredSshPassword: readOptionalBoolean(input, "hasStoredSshPassword"),
    hasStoredSshPrivateKey: readOptionalBoolean(
      input,
      "hasStoredSshPrivateKey",
    ),
    hasStoredSshPassphrase: readOptionalBoolean(
      input,
      "hasStoredSshPassphrase",
    ),
    hasStoredTlsKeyPassphrase: readOptionalBoolean(
      input,
      "hasStoredTlsKeyPassphrase",
    ),
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
  const ssh = parseConnectionSshConfig(input.ssh);
  return {
    ...base,
    password,
    hasStoredApiKey: readOptionalBoolean(input, "hasStoredApiKey"),
    ssh,
    hasStoredSecret: readOptionalBoolean(input, "hasStoredSecret"),
    hasStoredSshPassword: readOptionalBoolean(input, "hasStoredSshPassword"),
    hasStoredSshPrivateKey: readOptionalBoolean(
      input,
      "hasStoredSshPrivateKey",
    ),
    hasStoredSshPassphrase: readOptionalBoolean(
      input,
      "hasStoredSshPassphrase",
    ),
    hasStoredTlsKeyPassphrase: readOptionalBoolean(
      input,
      "hasStoredTlsKeyPassphrase",
    ),
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

      return {
        view: "query",
        connectionId,
        connectionType,
        queryText,
        initialSql: queryText,
        formatOnOpen,
        isBookmarked: readOptionalBoolean(input, "isBookmarked"),
        editorLanguage,
        editorPresentation:
          queryMode === undefined &&
          formatOnOpen === undefined &&
          editorLanguage === undefined &&
          sqlDialect === undefined &&
          allowFormatting === undefined
            ? undefined
            : {
                queryMode,
                formatOnOpen,
                editorLanguage,
                sqlDialect,
                allowFormatting,
              },
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
        connectionReadOnly: readOptionalBoolean(input, "connectionReadOnly"),
        defaultPageSize: readOptionalNumber(input, "defaultPageSize"),
      };
    }

    case "erd": {
      const connectionId = readRequiredString(input, "connectionId");
      if (!connectionId) {
        return null;
      }

      return {
        view: "erd",
        connectionId,
        database: readOptionalString(input, "database"),
        schema: readOptionalString(input, "schema"),
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

    case "executeQuery":
    case "addBookmark": {
      const payload = parseEnvelopeQueryPayload(envelope);
      return payload ? { type: envelope.type, payload } : null;
    }

    case "getConnections":
    case "exportResultsCSV":
    case "exportResultsJSON":
    case "readClipboard":
      return { type: envelope.type };

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
      const payload = parseOptionalPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
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
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const updates = payload.updates;
      const insertValues = payload.insertValues;
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
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const values = payload.values;
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
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const primaryKeysList = payload.primaryKeysList;
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
      const payload = parseOptionalPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
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

    case "confirmMutationPreview":
    case "cancelMutationPreview": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const previewToken = readRequiredString(payload, "previewToken");
      return previewToken
        ? { type: envelope.type, payload: { previewToken } }
        : null;
    }

    case "readClipboard":
      return { type: envelope.type };

    case "writeClipboard": {
      const payload = parseEnvelopeTextPayload(envelope);
      return payload ? { type: envelope.type, payload } : null;
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

    case "browseFile": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }

      const target = readConnectionFormBrowseTarget(payload.target);
      return target ? { type: envelope.type, payload: { target } } : null;
    }

    default:
      return null;
  }
}

export function parseErdPanelMessage(input: unknown): ErdPanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "ready":
    case "reload":
      return { type: envelope.type };

    case "openTableData": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const table = readRequiredString(payload, "table");
      if (!table) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          table,
          schema: readOptionalString(payload, "schema"),
          database: readOptionalString(payload, "database"),
          isView: readOptionalBoolean(payload, "isView"),
        },
      };
    }

    default:
      return null;
  }
}
