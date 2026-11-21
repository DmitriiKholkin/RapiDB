import { createHash } from "node:crypto";
import { DEFAULT_PORT_BY_CONNECTION_TYPE } from "../shared/connectionTypes";
import type { StoredConnectionConfig } from "./connectionManagerModels";
import {
  hasConnectionConfigSecrets,
  sanitizeCredentialBearingUri,
} from "./connectionSecrets";
import {
  parseStoredConnectionSecretDocument,
  type StoredConnectionSecrets,
} from "./connectionSecretsData";
import { resolvePostgresPort } from "./services/postgresPort";

const TARGET_FIELD = "rapidbSecretTargetV1";
const PROFILE_FIELD = "rapidbSecretProfile";
export class ConnectionSecretBindingError extends Error {
  constructor(config: StoredConnectionConfig) {
    super(
      `${
        config.type === "dynamodb" && config.awsCredentialMode === "static"
          ? "Saved static AWS credentials"
          : "Saved credentials"
      } are missing or do not match this connection profile. Refresh the connection settings and reopen Edit Connection.`,
    );
  }
}

// Hash only non-secret settings. URI credentials (including URI usernames)
// travel with the secret; changing that material rotates the profile link.
export function connectionSecretTarget(config: StoredConnectionConfig): string {
  const ssh = config.ssh;
  const tls = config.tls;
  const target = [
    config.type,
    config.host || undefined,
    config.type === "pg"
      ? resolvePostgresPort(config.port)
      : (config.port ?? DEFAULT_PORT_BY_CONNECTION_TYPE[config.type]),
    config.type === "oracle"
      ? config.serviceName?.trim() || config.database?.trim()
      : config.database || undefined,
    (config.username ?? config.user) || undefined,
    config.filePath,
    sanitizeCredentialBearingUri(config.connectionUri),
    sanitizeCredentialBearingUri(config.uri),
    sanitizeCredentialBearingUri(config.endpoint),
    sanitizeCredentialBearingUri(config.awsEndpoint),
    config.cloudId,
    config.awsRegion,
    config.awsCredentialMode,
    config.awsProfile,
    config.mongoConnectionMode,
    config.mongoScheme,
    config.mongoAuthMethod,
    config.authSource,
    config.authMechanism,
    config.replicaSet,
    config.directConnection,
    tls
      ? [
          tls.mode,
          tls.caFilePath,
          tls.certFilePath,
          tls.keyFilePath,
          tls.serverNameOverride,
        ]
      : undefined,
    ssh
      ? [
          ssh.host?.trim(),
          ssh.port ?? 22,
          ssh.username?.trim(),
          ssh.authMethod,
          ssh.hostVerificationMode ?? "manual",
          // TOFU can add a pin without editing credentials; the SSH layer
          // checks the pin on every connection.
          ssh.hostVerificationMode !== "trustOnFirstUse"
            ? ssh.hostFingerprintSha256?.trim()
            : undefined,
        ]
      : undefined,
    // Appended only when set, so profiles without it keep their target hash.
    // Account verification through another STS endpoint is another target.
    ...(config.awsStsEndpoint?.trim()
      ? [["awsStsEndpoint", config.awsStsEndpoint.trim()]]
      : []),
  ];
  return createHash("sha256").update(JSON.stringify(target)).digest("hex");
}

export function assertConnectionSecretBinding(
  config: StoredConnectionConfig,
  raw: string | undefined,
): void {
  const { record } = parseStoredConnectionSecretDocument(raw);
  if (
    record[PROFILE_FIELD] !== config.secretStorageProfile ||
    ((config.secretStorageProfile !== undefined || TARGET_FIELD in record) &&
      (record[TARGET_FIELD] !== connectionSecretTarget(config) ||
        hasConnectionConfigSecrets(config)))
  ) {
    throw new ConnectionSecretBindingError(config);
  }
}

/**
 * A document saved before secrets were bound to their target: no profile
 * link in settings or in the document, and no target hash. It is bound to the
 * target on first use (trust on first use); afterwards a different target is
 * refused like any other mismatch.
 */
export function isUnboundLegacyConnectionSecret(
  config: StoredConnectionConfig,
  raw: string | undefined,
): boolean {
  // Inline settings secrets are migrated first; a bound document would then
  // be refused while they remain.
  if (
    raw === undefined ||
    config.secretStorageProfile !== undefined ||
    hasConnectionConfigSecrets(config)
  ) {
    return false;
  }
  const { record } = parseStoredConnectionSecretDocument(raw);
  return record[PROFILE_FIELD] === undefined && !(TARGET_FIELD in record);
}

export function bindConnectionSecret(
  config: StoredConnectionConfig,
  raw: string | undefined,
): string {
  const { record, known, isLegacy } = parseStoredConnectionSecretDocument(raw);
  return JSON.stringify({
    ...(isLegacy ? known : record),
    [PROFILE_FIELD]: config.secretStorageProfile,
    [TARGET_FIELD]: connectionSecretTarget(config),
  });
}

export function credentialBearingUriChanged(
  previous: StoredConnectionSecrets,
  next: StoredConnectionSecrets,
): boolean {
  return (["connectionUri", "uri", "endpoint", "awsEndpoint"] as const).some(
    (key) => previous[key] !== next[key],
  );
}

// A pin learned by a connection is not included in the target hash because
// it can be persisted after the initial SSH handshake. Explicit edits to it
// still cannot inherit credentials or publish replacements under the old link.
export function sameConnectionTofuPin(
  before: StoredConnectionConfig,
  after: StoredConnectionConfig,
): boolean {
  return (
    before.ssh?.hostVerificationMode !== "trustOnFirstUse" ||
    before.ssh.hostFingerprintSha256?.trim() ===
      after.ssh?.hostFingerprintSha256?.trim()
  );
}
