import type { ConnectionConfig } from "./connectionManagerModels";

export interface ConnectionSecretSnapshot {
  password?: string;
  apiKey?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  awsSessionToken?: string;
  connectionUri?: string;
  uri?: string;
  endpoint?: string;
  awsEndpoint?: string;
  sshPassword?: string;
  sshPrivateKey?: string;
  sshPassphrase?: string;
  tlsKeyPassphrase?: string;
}

const CREDENTIAL_BEARING_URI_FIELDS = [
  "connectionUri",
  "uri",
  "endpoint",
  "awsEndpoint",
] as const satisfies readonly (keyof ConnectionSecretSnapshot)[];

export function trimOptionalSecretValue(
  value: string | undefined,
): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function redactCredentialBearingUri(uri: string): string {
  return uri.replace(/^([a-z][a-z\d+.-]*:\/\/)([^@/?#\s]+)@/i, "$1");
}

export function sanitizeCredentialBearingUri(
  value: string | undefined,
): string | undefined {
  const normalized = trimOptionalSecretValue(value);
  if (!normalized) {
    return normalized;
  }

  return redactCredentialBearingUri(normalized);
}

export function extractCredentialBearingUriSecret(
  value: string | undefined,
): string | undefined {
  const normalized = trimOptionalSecretValue(value);
  if (!normalized) {
    return undefined;
  }

  return redactCredentialBearingUri(normalized) !== normalized
    ? normalized
    : undefined;
}

export function resolvePersistedCredentialBearingUriSecret(
  currentValue: string | undefined,
  previousSecret: string | undefined,
): string | undefined {
  const explicitSecret = extractCredentialBearingUriSecret(currentValue);
  if (explicitSecret) {
    return explicitSecret;
  }

  const normalizedCurrent = trimOptionalSecretValue(currentValue);
  if (!normalizedCurrent || !previousSecret) {
    return undefined;
  }

  const previousRedacted = sanitizeCredentialBearingUri(previousSecret);
  return previousRedacted === normalizedCurrent ? previousSecret : undefined;
}

function serializeConnectionSecrets(
  secrets: ConnectionSecretSnapshot,
): string | undefined {
  const filtered = Object.fromEntries(
    Object.entries(secrets).filter(([, value]) => typeof value === "string"),
  );

  return Object.keys(filtered).length > 0
    ? JSON.stringify(filtered)
    : undefined;
}

function extractConnectionSecrets(
  config: ConnectionConfig,
  previousSecrets?: ConnectionSecretSnapshot,
): ConnectionSecretSnapshot {
  const ssh = config.ssh;
  const sshPassword =
    ssh && ssh.authMethod === "password"
      ? (trimOptionalSecretValue(ssh.password) ?? previousSecrets?.sshPassword)
      : undefined;
  const sshPrivateKey =
    ssh && ssh.authMethod === "privateKey"
      ? (trimOptionalSecretValue(ssh.privateKey) ??
        previousSecrets?.sshPrivateKey)
      : undefined;
  const sshPassphrase =
    ssh && ssh.authMethod === "privateKey"
      ? (trimOptionalSecretValue(ssh.passphrase) ??
        previousSecrets?.sshPassphrase)
      : undefined;
  const tlsKeyPassphrase =
    config.tls?.mode === "mutualTls"
      ? (trimOptionalSecretValue(config.tls.keyPassphrase) ??
        previousSecrets?.tlsKeyPassphrase)
      : undefined;

  return {
    password:
      trimOptionalSecretValue(config.password) ?? previousSecrets?.password,
    apiKey:
      config.type === "elasticsearch"
        ? (trimOptionalSecretValue(config.apiKey) ?? previousSecrets?.apiKey)
        : undefined,
    awsAccessKeyId:
      config.type === "dynamodb"
        ? (trimOptionalSecretValue(config.awsAccessKeyId) ??
          previousSecrets?.awsAccessKeyId)
        : undefined,
    awsSecretAccessKey:
      config.type === "dynamodb"
        ? (trimOptionalSecretValue(config.awsSecretAccessKey) ??
          previousSecrets?.awsSecretAccessKey)
        : undefined,
    awsSessionToken:
      config.type === "dynamodb"
        ? (trimOptionalSecretValue(config.awsSessionToken) ??
          previousSecrets?.awsSessionToken)
        : undefined,
    connectionUri: resolvePersistedCredentialBearingUriSecret(
      config.connectionUri,
      previousSecrets?.connectionUri,
    ),
    uri: resolvePersistedCredentialBearingUriSecret(
      config.uri,
      previousSecrets?.uri,
    ),
    endpoint: resolvePersistedCredentialBearingUriSecret(
      config.endpoint,
      previousSecrets?.endpoint,
    ),
    awsEndpoint: resolvePersistedCredentialBearingUriSecret(
      config.awsEndpoint,
      previousSecrets?.awsEndpoint,
    ),
    sshPassword,
    sshPrivateKey,
    sshPassphrase,
    tlsKeyPassphrase,
  };
}

function hasCredentialBearingUriSecret(config: ConnectionConfig): boolean {
  return CREDENTIAL_BEARING_URI_FIELDS.some(
    (field) => extractCredentialBearingUriSecret(config[field]) !== undefined,
  );
}

export function shouldForceSecretStorage(config: ConnectionConfig): boolean {
  return (
    config.ssh !== undefined ||
    config.type === "dynamodb" ||
    config.type === "elasticsearch" ||
    hasCredentialBearingUriSecret(config) ||
    trimOptionalSecretValue(config.password) !== undefined ||
    trimOptionalSecretValue(config.tls?.keyPassphrase) !== undefined ||
    trimOptionalSecretValue(config.apiKey) !== undefined ||
    trimOptionalSecretValue(config.awsAccessKeyId) !== undefined ||
    trimOptionalSecretValue(config.awsSecretAccessKey) !== undefined ||
    trimOptionalSecretValue(config.awsSessionToken) !== undefined ||
    config.useSecretStorage === true
  );
}

export function sanitizePersistedConnectionConfig(
  config: ConnectionConfig,
): ConnectionConfig {
  if (!shouldForceSecretStorage(config)) {
    return { ...config };
  }

  const {
    password: _password,
    apiKey: _apiKey,
    awsAccessKeyId: _awsAccessKeyId,
    awsSecretAccessKey: _awsSecretAccessKey,
    awsSessionToken: _awsSessionToken,
    ssh: _ssh,
    connectionUri: rawConnectionUri,
    uri: rawUri,
    endpoint: rawEndpoint,
    awsEndpoint: rawAwsEndpoint,
    ...rest
  } = config;

  const tls =
    rest.tls !== undefined
      ? {
          ...rest.tls,
          keyPassphrase: undefined,
        }
      : undefined;

  const sanitizedSsh = _ssh
    ? {
        host: _ssh.host,
        port: _ssh.port,
        username: _ssh.username,
        authMethod: _ssh.authMethod,
        hostVerificationMode: _ssh.hostVerificationMode,
        hostFingerprintSha256: _ssh.hostFingerprintSha256,
      }
    : undefined;

  return {
    ...rest,
    ssh: sanitizedSsh,
    tls,
    connectionUri: sanitizeCredentialBearingUri(rawConnectionUri),
    uri: sanitizeCredentialBearingUri(rawUri),
    endpoint: sanitizeCredentialBearingUri(rawEndpoint),
    awsEndpoint: sanitizeCredentialBearingUri(rawAwsEndpoint),
    useSecretStorage: true,
  };
}

export function hasPersistedConnectionConfigChanges(
  persisted: ConnectionConfig,
  original: ConnectionConfig,
): boolean {
  return (
    persisted.useSecretStorage !== original.useSecretStorage ||
    persisted.color !== original.color ||
    persisted.password !== original.password ||
    persisted.apiKey !== original.apiKey ||
    persisted.awsAccessKeyId !== original.awsAccessKeyId ||
    persisted.awsSecretAccessKey !== original.awsSecretAccessKey ||
    persisted.awsSessionToken !== original.awsSessionToken ||
    persisted.ssh?.password !== original.ssh?.password ||
    persisted.ssh?.privateKey !== original.ssh?.privateKey ||
    persisted.ssh?.passphrase !== original.ssh?.passphrase ||
    persisted.tls?.keyPassphrase !== original.tls?.keyPassphrase ||
    persisted.connectionUri !== original.connectionUri ||
    persisted.uri !== original.uri ||
    persisted.endpoint !== original.endpoint ||
    persisted.awsEndpoint !== original.awsEndpoint
  );
}

export function serializeConnectionSecretsForConfig(
  config: ConnectionConfig,
  previousSecrets?: ConnectionSecretSnapshot,
): string | undefined {
  return serializeConnectionSecrets(
    extractConnectionSecrets(config, previousSecrets),
  );
}
