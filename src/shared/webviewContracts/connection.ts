/**
 * Connection form panel types, initial state, and message parser.
 *
 * All parsers are pure; `null` indicates a parse failure.
 */
import {
  CONNECTION_TLS_MODES,
  type ConnectionConfig,
  type ConnectionSshConfig,
  type ConnectionTlsMode,
} from "../connectionConfig";
import type { ConnectionType } from "../connectionTypes";
import type { PanelRetentionState, WebviewMessageEnvelope } from "./shared";
import {
  isRecord,
  parseEnvelope,
  parseRequiredPayloadRecord,
  readConnectionType,
  readOptionalBoolean,
  readOptionalNumber,
  readOptionalString,
  readRequiredString,
} from "./shared";

// ─── Sanitized Config Types ─────────────────────────────────────────────────

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

// ─── Connection Form Types ──────────────────────────────────────────────────

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

// ─── Parser Helpers ─────────────────────────────────────────────────────────

const CONNECTION_FORM_BROWSE_TARGETS: ReadonlySet<ConnectionFormBrowseTarget> =
  new Set(["filePath", "tlsCaFile", "tlsCertFile", "tlsKeyFile"]);

const SQLITE_WAL_MODES: ReadonlySet<
  NonNullable<ConnectionConfig["sqliteWalMode"]>
> = new Set(["auto", "off"]);

const SSH_AUTH_METHODS: ReadonlySet<
  NonNullable<ConnectionSshConfig["authMethod"]>
> = new Set(["password", "privateKey"]);

const SSH_HOST_VERIFICATION_MODES: ReadonlySet<
  NonNullable<ConnectionSshConfig["hostVerificationMode"]>
> = new Set(["manual", "trustOnFirstUse"]);

function readConnectionSshAuthMethod(
  value: unknown,
): ConnectionSshConfig["authMethod"] | undefined {
  return typeof value === "string" &&
    SSH_AUTH_METHODS.has(
      value as NonNullable<ConnectionSshConfig["authMethod"]>,
    )
    ? (value as ConnectionSshConfig["authMethod"])
    : undefined;
}

function readConnectionSshHostVerificationMode(
  value: unknown,
): ConnectionSshConfig["hostVerificationMode"] | undefined {
  return typeof value === "string" &&
    SSH_HOST_VERIFICATION_MODES.has(
      value as NonNullable<ConnectionSshConfig["hostVerificationMode"]>,
    )
    ? (value as ConnectionSshConfig["hostVerificationMode"])
    : undefined;
}

function readSqliteWalMode(
  value: unknown,
): ConnectionConfig["sqliteWalMode"] | undefined {
  return typeof value === "string" &&
    SQLITE_WAL_MODES.has(
      value as NonNullable<ConnectionConfig["sqliteWalMode"]>,
    )
    ? (value as ConnectionConfig["sqliteWalMode"])
    : undefined;
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
  return typeof value === "string" &&
    CONNECTION_FORM_BROWSE_TARGETS.has(value as ConnectionFormBrowseTarget)
    ? (value as ConnectionFormBrowseTarget)
    : undefined;
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

// ─── Connection Base Parser ─────────────────────────────────────────────────

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

function readStoredSecretFlags(input: Record<string, unknown>) {
  return {
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

// ─── Connection Form Parsers ────────────────────────────────────────────────

export function parseConnectionFormExistingState(
  input: unknown,
): ConnectionFormExistingState | null {
  const base = parseConnectionBase(input);
  if (!base || !isRecord(input)) {
    return null;
  }
  return { ...base, ...readStoredSecretFlags(input) };
}

export function parseConnectionFormSubmission(
  input: unknown,
): ConnectionFormSubmission | null {
  const base = parseConnectionBase(input);
  if (!base || !isRecord(input)) {
    return null;
  }
  return {
    ...base,
    password: readOptionalString(input, "password"),
    ssh: parseConnectionSshConfig(input.ssh),
    ...readStoredSecretFlags(input),
  };
}

// ─── Initial State Parser ───────────────────────────────────────────────────

export function parseConnectionFormInitialState(
  input: Record<string, unknown>,
): ConnectionFormInitialState | null {
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

// ─── Messages ───────────────────────────────────────────────────────────────

export type ConnectionFormPanelMessage =
  | WebviewMessageEnvelope<"saveConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"testConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"cancel">
  | WebviewMessageEnvelope<
      "browseFile",
      { target: ConnectionFormBrowseTarget }
    >;

// ─── Message Parser ─────────────────────────────────────────────────────────

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
