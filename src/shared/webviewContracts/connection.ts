import {
  CONNECTION_TLS_MODES,
  type ConnectionConfig,
  type ConnectionSshConfig,
  type ConnectionTlsMode,
} from "../connectionConfig";
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
  hasStoredMongoUriCredentials?: boolean;
  hasStoredConnectionUriCredentials?: boolean;
  keepConnectionUriCredentials?: boolean;
  keepMongoUriCredentials?: boolean;
  clearPassword?: boolean;
  hasStoredApiKey?: boolean;
  hasStoredAwsCredentials?: boolean;
  hasStoredAwsKeyPair?: boolean;
  hasStoredSshPassword?: boolean;
  hasStoredSshPrivateKey?: boolean;
  hasStoredSshPassphrase?: boolean;
  hasStoredTlsKeyPassphrase?: boolean;
}

export interface ConnectionFormSubmission extends SanitizedConnectionConfig {
  password?: string;
  hasStoredMongoUriCredentials?: boolean;
  hasStoredConnectionUriCredentials?: boolean;
  keepConnectionUriCredentials?: boolean;
  keepMongoUriCredentials?: boolean;
  clearPassword?: boolean;
  clearApiKey?: boolean;
  clearAwsCredentials?: boolean;
  hasStoredApiKey?: boolean;
  hasStoredAwsCredentials?: boolean;
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
): ConnectionConfig["tls"] | null {
  if (value === undefined) {
    return undefined;
  }
  if (!isRecord(value)) {
    return null;
  }
  const mode = readConnectionTlsMode(value.mode);
  if (mode === undefined) {
    return null;
  }
  for (const field of [
    "caFilePath",
    "certFilePath",
    "keyFilePath",
    "keyPassphrase",
    "serverNameOverride",
  ] as const) {
    if (value[field] !== undefined && typeof value[field] !== "string") {
      return null;
    }
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

function parseConnectionBase(input: unknown): SanitizedConnectionConfig | null {
  if (!isRecord(input)) {
    return null;
  }

  const id = readRequiredString(input, "id");
  const name = readOptionalString(input, "name");
  const type = readConnectionType(input.type);
  const sqliteWalMode = readSqliteWalMode(input.sqliteWalMode);
  const tls = readConnectionTlsConfig(input.tls);
  if (tls === null) {
    return null;
  }
  if (!id || name === undefined || type === undefined || type === "") {
    return null;
  }

  for (const source of [input, ...(isRecord(input.ssh) ? [input.ssh] : [])]) {
    if (
      source.port !== undefined &&
      readOptionalNumber(source, "port") === undefined
    ) {
      return null;
    }
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
    tls,
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
    mongoUriHasCredentials: readOptionalBoolean(
      input,
      "mongoUriHasCredentials",
    ),
    mongoScheme:
      input.mongoScheme === "mongodb+srv"
        ? "mongodb+srv"
        : input.mongoScheme === "mongodb"
          ? "mongodb"
          : undefined,
    mongoConnectionMode:
      input.mongoConnectionMode === "uri" ||
      input.mongoConnectionMode === "fields"
        ? input.mongoConnectionMode
        : undefined,
    mongoAuthMethod:
      input.mongoAuthMethod === "none" || input.mongoAuthMethod === "password"
        ? input.mongoAuthMethod
        : undefined,
    authMechanism:
      input.authMechanism === "SCRAM-SHA-1" ||
      input.authMechanism === "SCRAM-SHA-256"
        ? input.authMechanism
        : undefined,
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
    hasStoredConnectionUriCredentials: readOptionalBoolean(
      input,
      "hasStoredConnectionUriCredentials",
    ),
    keepConnectionUriCredentials: readOptionalBoolean(
      input,
      "keepConnectionUriCredentials",
    ),
    hasStoredMongoUriCredentials: readOptionalBoolean(
      input,
      "hasStoredMongoUriCredentials",
    ),
    keepMongoUriCredentials: readOptionalBoolean(
      input,
      "keepMongoUriCredentials",
    ),
    clearPassword: readOptionalBoolean(input, "clearPassword"),
    clearApiKey: readOptionalBoolean(input, "clearApiKey"),
    clearAwsCredentials: readOptionalBoolean(input, "clearAwsCredentials"),
    hasStoredApiKey: readOptionalBoolean(input, "hasStoredApiKey"),
    hasStoredAwsCredentials: readOptionalBoolean(
      input,
      "hasStoredAwsCredentials",
    ),
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

export function parseConnectionFormExistingState(
  input: unknown,
): ConnectionFormExistingState | null {
  const base = parseConnectionBase(input);
  if (!base || !isRecord(input)) {
    return null;
  }
  return {
    ...base,
    ...readStoredSecretFlags(input),
    hasStoredAwsKeyPair: readOptionalBoolean(input, "hasStoredAwsKeyPair"),
  };
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

export type ConnectionFormPanelMessage =
  | WebviewMessageEnvelope<"saveConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"testConnection", ConnectionFormSubmission>
  | WebviewMessageEnvelope<"cancelTestConnection">
  | WebviewMessageEnvelope<"cancel">
  | WebviewMessageEnvelope<
      "browseFile",
      { target: ConnectionFormBrowseTarget }
    >;

export function parseConnectionFormPanelMessage(
  input: unknown,
): ConnectionFormPanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "cancel":
    case "cancelTestConnection":
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
