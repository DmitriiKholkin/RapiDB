import type { ConnectionConfig } from "./connectionManagerModels";

export interface ConnectionSecretSnapshot {
  password?: string;
  apiKey?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  awsSessionToken?: string;
  connectionUri?: string;
  uri?: string;
}

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

function sanitizeUriForPersistence(
  value: string | undefined,
): string | undefined {
  const normalized = trimOptionalSecretValue(value);
  if (!normalized) {
    return normalized;
  }

  return redactCredentialBearingUri(normalized);
}

function resolvePersistedUriSecret(
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

  const previousRedacted = sanitizeUriForPersistence(previousSecret);
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
    connectionUri: resolvePersistedUriSecret(
      config.connectionUri,
      previousSecrets?.connectionUri,
    ),
    uri: resolvePersistedUriSecret(config.uri, previousSecrets?.uri),
  };
}

export function shouldForceSecretStorage(config: ConnectionConfig): boolean {
  return (
    config.type === "dynamodb" ||
    config.type === "elasticsearch" ||
    extractCredentialBearingUriSecret(config.connectionUri) !== undefined ||
    extractCredentialBearingUriSecret(config.uri) !== undefined ||
    trimOptionalSecretValue(config.password) !== undefined ||
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
    connectionUri: rawConnectionUri,
    uri: rawUri,
    ...rest
  } = config;

  return {
    ...rest,
    connectionUri: sanitizeUriForPersistence(rawConnectionUri),
    uri: sanitizeUriForPersistence(rawUri),
    useSecretStorage: true,
  };
}

export function hasPersistedConnectionConfigChanges(
  persisted: ConnectionConfig,
  original: ConnectionConfig,
): boolean {
  return (
    persisted.useSecretStorage !== original.useSecretStorage ||
    persisted.password !== original.password ||
    persisted.apiKey !== original.apiKey ||
    persisted.awsAccessKeyId !== original.awsAccessKeyId ||
    persisted.awsSecretAccessKey !== original.awsSecretAccessKey ||
    persisted.awsSessionToken !== original.awsSessionToken ||
    persisted.connectionUri !== original.connectionUri ||
    persisted.uri !== original.uri
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
