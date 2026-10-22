import {
  AMBIGUOUS_URI_CREDENTIALS_MESSAGE,
  hasAmbiguousUriCredentials,
  splitUriAuthorityPrefix,
} from "../shared/connectionUriSecurity";
import type { ConnectionConfig } from "./connectionManagerModels";
import {
  CONNECTION_SECRET_KEYS,
  parseStoredConnectionSecretDocument,
} from "./connectionSecretsData";

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

export interface ConnectionSecretClearOptions {
  password?: boolean;
  apiKey?: boolean;
  awsCredentials?: boolean;
  credentialBearingUris?: boolean;
}

const CREDENTIAL_BEARING_URI_FIELDS = [
  "connectionUri",
  "uri",
  "endpoint",
  "awsEndpoint",
] as const satisfies readonly (keyof ConnectionSecretSnapshot)[];
const CREDENTIAL_QUERY_PARAMETER =
  /^(?:password|passwd|pwd|proxyPassword|passphrase|tlsCertificateKeyFilePassword|authMechanismProperties|auth|key|sig|api[_-]?key|access[_-]?token|auth[_-]?token|token|secret|client[_-]?secret|aws[_-]?(?:access[_-]?key[_-]?id|secret[_-]?access[_-]?key|session[_-]?token))$/i;

export function trimOptionalSecretValue(
  value: string | undefined,
): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  return value.length > 0 ? value : undefined;
}

export function trimOptionalUriValue(
  value: string | undefined,
): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function redactCredentialBearingUri(uri: string): string {
  if (hasAmbiguousUriCredentials(uri)) {
    throw new Error(AMBIGUOUS_URI_CREDENTIALS_MESSAGE);
  }
  const { prefix, remainder } = splitUriAuthorityPrefix(uri);
  const authorityRedacted = prefix + remainder.replace(/^[^/?#\s\\]*@/, "");
  const hashIndex = authorityRedacted.indexOf("#");
  const fragment = hashIndex >= 0 ? authorityRedacted.slice(hashIndex + 1) : "";
  const withoutFragment =
    hashIndex >= 0 ? authorityRedacted.slice(0, hashIndex) : authorityRedacted;
  const queryIndex = withoutFragment.indexOf("?");
  const query = queryIndex >= 0 ? withoutFragment.slice(queryIndex + 1) : "";
  const base =
    queryIndex >= 0 ? withoutFragment.slice(0, queryIndex) : withoutFragment;
  const filteredQuery = query
    .split("&")
    .filter((part) => {
      const rawKey = part.split("=", 1)[0];
      try {
        return !CREDENTIAL_QUERY_PARAMETER.test(decodeURIComponent(rawKey));
      } catch {
        return !CREDENTIAL_QUERY_PARAMETER.test(rawKey);
      }
    })
    .join("&");
  const fragmentHasCredential = fragment.split("&").some((part) => {
    const rawKey = part.split("=", 1)[0];
    try {
      return CREDENTIAL_QUERY_PARAMETER.test(decodeURIComponent(rawKey));
    } catch {
      return CREDENTIAL_QUERY_PARAMETER.test(rawKey);
    }
  });
  const keepFragment = fragment.length > 0 && !fragmentHasCredential;
  if (
    authorityRedacted === uri &&
    filteredQuery === query &&
    (fragment.length === 0 || keepFragment)
  ) {
    return uri;
  }
  return `${base}${filteredQuery ? `?${filteredQuery}` : ""}${keepFragment ? `#${fragment}` : ""}`;
}

export function sanitizeCredentialBearingUri(
  value: string | undefined,
): string | undefined {
  const normalized = trimOptionalUriValue(value);
  if (!normalized) {
    return normalized;
  }

  return redactCredentialBearingUri(normalized);
}

export function extractCredentialBearingUriSecret(
  value: string | undefined,
): string | undefined {
  const normalized = trimOptionalUriValue(value);
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

  const normalizedCurrent = trimOptionalUriValue(currentValue);
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
      config.type === "mongodb" &&
      config.mongoConnectionMode &&
      (config.mongoConnectionMode === "uri" ||
        config.mongoAuthMethod === "none" ||
        config.password === "")
        ? undefined
        : (trimOptionalSecretValue(config.password) ??
          previousSecrets?.password),
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
      config.type === "mongodb" && config.mongoUriHasCredentials === false
        ? undefined
        : previousSecrets?.connectionUri,
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

export function hasConnectionConfigSecrets(config: ConnectionConfig): boolean {
  return (
    [
      config.password,
      config.apiKey,
      config.awsAccessKeyId,
      config.awsSecretAccessKey,
      config.awsSessionToken,
      config.ssh?.password,
      config.ssh?.privateKey,
      config.ssh?.passphrase,
      config.tls?.keyPassphrase,
    ].some((value) => trimOptionalSecretValue(value) !== undefined) ||
    hasCredentialBearingUriSecret(config)
  );
}

export function shouldForceSecretStorage(config: ConnectionConfig): boolean {
  for (const field of CREDENTIAL_BEARING_URI_FIELDS) {
    if (hasAmbiguousUriCredentials(config[field])) {
      throw new Error(AMBIGUOUS_URI_CREDENTIALS_MESSAGE);
    }
  }
  if (config.useSecretStorage === false) {
    return false;
  }

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

  return sanitizeConnectionConfigForResponse({
    ...config,
    useSecretStorage: true,
  });
}

export function sanitizeConnectionConfigForResponse(
  config: ConnectionConfig,
): ConnectionConfig {
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

export function serializeConnectionSecretsForStoredConfig(
  config: ConnectionConfig,
  previousSerialized: string | undefined,
  clearOptions: ConnectionSecretClearOptions = {},
): string | undefined {
  const previous = parseStoredConnectionSecretDocument(previousSerialized);
  const nextSecrets = extractConnectionSecrets(config, previous.known);
  if (clearOptions.password && !trimOptionalSecretValue(config.password)) {
    nextSecrets.password = undefined;
  }
  if (clearOptions.apiKey && !trimOptionalSecretValue(config.apiKey)) {
    nextSecrets.apiKey = undefined;
  }
  if (clearOptions.awsCredentials) {
    for (const key of [
      "awsAccessKeyId",
      "awsSecretAccessKey",
      "awsSessionToken",
    ] as const) {
      if (!trimOptionalSecretValue(config[key])) {
        nextSecrets[key] = undefined;
      }
    }
  }
  if (clearOptions.credentialBearingUris) {
    for (const key of CREDENTIAL_BEARING_URI_FIELDS) {
      if (extractCredentialBearingUriSecret(config[key]) === undefined) {
        nextSecrets[key] = undefined;
      }
    }
  }
  const knownUnchanged = CONNECTION_SECRET_KEYS.every(
    (key) => nextSecrets[key] === previous.known[key],
  );
  if (knownUnchanged) {
    return previousSerialized;
  }

  const nextRecord = { ...previous.record };
  for (const key of CONNECTION_SECRET_KEYS) {
    delete nextRecord[key];
  }
  for (const [key, value] of Object.entries(nextSecrets)) {
    if (typeof value === "string") {
      nextRecord[key] = value;
    }
  }

  return Object.keys(nextRecord).length > 0
    ? JSON.stringify(nextRecord)
    : undefined;
}

/** Retain URI credentials only when an editor explicitly requests it. */
export function restoreCredentialBearingUri(
  value: string | undefined,
  previous: string | undefined,
  keep: boolean,
): string | undefined {
  const submitted = trimOptionalUriValue(value);
  if (!submitted || !previous || !keep) return submitted;
  const pattern = /^([a-z][a-z\d+.-]*:\/\/)([^/?#]+)(.*)$/i;
  const oldParts = pattern.exec(previous);
  const parts = pattern.exec(submitted);
  if (!oldParts || !parts) return submitted;

  const splitSuffix = (suffix: string) => {
    const hashIndex = suffix.indexOf("#");
    const beforeFragment = hashIndex < 0 ? suffix : suffix.slice(0, hashIndex);
    const fragment = hashIndex < 0 ? "" : suffix.slice(hashIndex);
    const queryIndex = beforeFragment.indexOf("?");
    return {
      path:
        queryIndex < 0 ? beforeFragment : beforeFragment.slice(0, queryIndex),
      query: new URLSearchParams(
        queryIndex < 0 ? "" : beforeFragment.slice(queryIndex + 1),
      ),
      fragment,
    };
  };

  const at = oldParts[2].lastIndexOf("@");
  const replacesCredentials = parts[2].includes("@");
  const credentials =
    at >= 0 && !replacesCredentials ? oldParts[2].slice(0, at + 1) : "";
  const oldSuffix = splitSuffix(oldParts[3]);
  const safeParts = pattern.exec(sanitizeCredentialBearingUri(previous) ?? "");
  const safeQuery = splitSuffix(safeParts?.[3] ?? "").query;
  const nextSuffix = splitSuffix(parts[3]);
  const query = nextSuffix.query;
  let added = false;
  for (const [key, secret] of oldSuffix.query) {
    const hasEquivalentKey = (params: URLSearchParams) =>
      [...params.keys()].some(
        (candidate) => candidate.toLowerCase() === key.toLowerCase(),
      );
    if (
      !replacesCredentials &&
      !hasEquivalentKey(safeQuery) &&
      !hasEquivalentKey(query)
    ) {
      query.append(key, secret);
      added = true;
    }
  }
  const suffix = added
    ? `${nextSuffix.path}${query.size ? `?${query}` : ""}${nextSuffix.fragment}`
    : parts[3];
  return `${parts[1]}${credentials}${parts[2]}${suffix}`;
}

/** Retain credentials only when the MongoDB URI editor explicitly requests it. */
export function restoreMongoUriCredentials(
  value: string | undefined,
  previous: string | undefined,
  keep: boolean,
): string | undefined {
  return restoreCredentialBearingUri(value, previous, keep);
}
