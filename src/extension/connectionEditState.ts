import { isDeepStrictEqual } from "node:util";
import type {
  ConnectionConfig,
  StoredConnectionConfig,
} from "./connectionManagerModels";
import {
  omitAmbiguousCredentialBearingUris,
  sanitizePersistedConnectionConfig,
  serializeConnectionSecretsForStoredConfig,
} from "./connectionSecrets";
import {
  parseStoredConnectionSecrets,
  type StoredConnectionSecrets,
} from "./connectionSecretsData";

export type ConnectionSavePrecondition =
  | { kind: "create" }
  | { kind: "edit"; id: string; state: ConnectionEditState | undefined };

export interface ConnectionEditState {
  config: ConnectionConfig;
  secrets: StoredConnectionSecrets;
}

export function connectionEditState(
  config: StoredConnectionConfig,
  secrets: StoredConnectionSecrets,
): ConnectionEditState {
  const normalized = omitAmbiguousCredentialBearingUris({
    ...config,
    username: config.username ?? config.user,
  });
  delete normalized.user;
  if (normalized.type === "oracle") {
    normalized.serviceName =
      normalized.serviceName?.trim() ||
      normalized.database?.trim() ||
      undefined;
    delete normalized.database;
  }
  return JSON.parse(
    JSON.stringify({
      config: sanitizePersistedConnectionConfig(normalized),
      secrets: parseStoredConnectionSecrets(
        serializeConnectionSecretsForStoredConfig(
          normalized,
          JSON.stringify(
            config.useSecretStorage === false || config.type === "sqlite"
              ? {}
              : secrets,
          ),
        ),
      ),
    }),
  );
}

function addsTrustedSshFingerprint(
  expected: ConnectionConfig["ssh"],
  current: ConnectionConfig["ssh"],
): boolean {
  return (
    expected !== undefined &&
    current?.hostVerificationMode === "trustOnFirstUse" &&
    !expected.hostFingerprintSha256?.trim() &&
    Boolean(current.hostFingerprintSha256?.trim())
  );
}

export function connectionEditStatesMatch(
  expected: ConnectionEditState,
  current: ConnectionEditState,
): boolean {
  const config = { ...expected.config };
  if (
    config.useSecretStorage === undefined &&
    current.config.useSecretStorage === true
  ) {
    config.useSecretStorage = true;
  }
  if (config.ssh && addsTrustedSshFingerprint(config.ssh, current.config.ssh)) {
    config.ssh = {
      ...config.ssh,
      hostFingerprintSha256: current.config.ssh?.hostFingerprintSha256,
    };
  }
  return isDeepStrictEqual({ ...expected, config }, current);
}

export function preserveTrustedSshFingerprint(
  config: ConnectionConfig,
  expected: ConnectionEditState,
  previous: StoredConnectionConfig,
): ConnectionConfig {
  const ssh = config.ssh;
  const stored = previous.ssh;
  if (!ssh || !stored || ssh.hostVerificationMode !== "trustOnFirstUse") {
    return config;
  }
  const pin = ssh.hostFingerprintSha256?.trim();
  if (
    ssh.host?.trim() !== stored.host?.trim() ||
    (ssh.port ?? 22) !== (stored.port ?? 22)
  ) {
    if (!pin || pin !== stored.hostFingerprintSha256?.trim()) {
      return config;
    }
    const { hostFingerprintSha256: _stalePin, ...unpinned } = ssh;
    return { ...config, ssh: unpinned };
  }
  if (pin || !addsTrustedSshFingerprint(expected.config.ssh, stored)) {
    return config;
  }
  return {
    ...config,
    ssh: { ...ssh, hostFingerprintSha256: stored.hostFingerprintSha256 },
  };
}
