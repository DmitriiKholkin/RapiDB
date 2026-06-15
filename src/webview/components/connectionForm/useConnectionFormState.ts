/**
 * Connection form state management hook.
 *
 * Encapsulates all useState calls, derived state computation, and
 * the `buildPayload` function. This keeps the view component thin
 * and the business logic independently testable.
 */
import { useCallback, useMemo, useState } from "react";
import type {
  ConnectionSshAuthMethod,
  ConnectionSshHostVerificationMode,
  ConnectionTlsMode,
  SQLiteWalMode,
} from "../../../shared/connectionConfig";
import {
  getConnectionTlsSupport,
  isConnectionTlsEnabled,
  resolveConnectionTlsMode,
} from "../../../shared/connectionConfig";
import {
  type ConnectionType,
  DEFAULT_PORT_BY_CONNECTION_TYPE,
} from "../../../shared/connectionTypes";
import type {
  ConnectionFormExistingState,
  ConnectionFormSubmission,
} from "../../../shared/webviewContracts";

// ─── Form State Shape ───────────────────────────────────────────────────────

export interface ConnectionFormState {
  // Identity
  name: string;
  setName: (v: string) => void;
  nameError: string;
  setNameError: (v: string) => void;
  color: string;
  setColor: (v: string) => void;

  // Type & connection
  type: ConnectionType;
  handleTypeChange: (nextType: ConnectionType) => void;
  host: string;
  setHost: (v: string) => void;
  port: string;
  setPort: (v: string) => void;
  database: string;
  setDatabase: (v: string) => void;
  username: string;
  setUsername: (v: string) => void;
  password: string;
  setPassword: (v: string) => void;
  filePath: string;
  setFilePath: (v: string) => void;
  sqliteWalMode: SQLiteWalMode;
  setSqliteWalMode: (v: SQLiteWalMode) => void;
  folder: string;
  setFolder: (v: string) => void;
  connectionReadOnly: boolean;
  setConnectionReadOnly: (v: boolean) => void;

  // Oracle
  oracleServiceName: string;
  setOracleServiceName: (v: string) => void;

  // MongoDB
  mongoConnectionUri: string;
  setMongoConnectionUri: (v: string) => void;

  // Elasticsearch
  elasticsearchEndpoint: string;
  setElasticsearchEndpoint: (v: string) => void;
  elasticsearchApiKey: string;
  setElasticsearchApiKey: (v: string) => void;
  elasticsearchCloudId: string;
  setElasticsearchCloudId: (v: string) => void;

  // AWS / DynamoDB
  awsRegion: string;
  setAwsRegion: (v: string) => void;
  awsAccessKeyId: string;
  setAwsAccessKeyId: (v: string) => void;
  awsSecretAccessKey: string;
  setAwsSecretAccessKey: (v: string) => void;
  awsSessionToken: string;
  setAwsSessionToken: (v: string) => void;
  awsProfile: string;
  setAwsProfile: (v: string) => void;
  dynamoEndpoint: string;
  setDynamoEndpoint: (v: string) => void;

  // SSH
  sshEnabled: boolean;
  setSshEnabled: (v: boolean) => void;
  sshHost: string;
  setSshHost: (v: string) => void;
  sshPort: string;
  setSshPort: (v: string) => void;
  sshUsername: string;
  setSshUsername: (v: string) => void;
  sshAuthMethod: ConnectionSshAuthMethod;
  setSshAuthMethod: (v: ConnectionSshAuthMethod) => void;
  sshHostVerificationMode: ConnectionSshHostVerificationMode;
  setSshHostVerificationMode: (v: ConnectionSshHostVerificationMode) => void;
  sshPassword: string;
  setSshPassword: (v: string) => void;
  sshPrivateKey: string;
  setSshPrivateKey: (v: string) => void;
  sshPassphrase: string;
  setSshPassphrase: (v: string) => void;
  sshHostFingerprintSha256: string;
  setSshHostFingerprintSha256: (v: string) => void;

  // TLS
  tlsMode: ConnectionTlsMode;
  setTlsMode: (v: ConnectionTlsMode) => void;
  tlsCaFilePath: string;
  setTlsCaFilePath: (v: string) => void;
  tlsCertFilePath: string;
  setTlsCertFilePath: (v: string) => void;
  tlsKeyFilePath: string;
  setTlsKeyFilePath: (v: string) => void;
  tlsKeyPassphrase: string;
  setTlsKeyPassphrase: (v: string) => void;
  tlsServerNameOverride: string;
  setTlsServerNameOverride: (v: string) => void;

  // Secret storage
  useSecretStorage: boolean;
  setUseSecretStorage: (v: boolean) => void;

  // Test state
  testState: "idle" | "testing" | "ok" | "fail";
  setTestState: (v: "idle" | "testing" | "ok" | "fail") => void;
  testError: string;
  setTestError: (v: string) => void;
  saving: boolean;
  setSaving: (v: boolean) => void;
}

// ─── Derived State ──────────────────────────────────────────────────────────

export interface ConnectionFormDerived {
  isEdit: boolean;
  hasStoredSecret: boolean;
  hasStoredApiKey: boolean;
  hasStoredSshPassword: boolean;
  hasStoredSshPrivateKey: boolean;
  hasStoredSshPassphrase: boolean;
  hasStoredTlsKeyPassphrase: boolean;
  isSQLite: boolean;
  isOracle: boolean;
  isMongo: boolean;
  isRedis: boolean;
  isElasticsearch: boolean;
  isDynamo: boolean;
  tlsSupport: ReturnType<typeof getConnectionTlsSupport>;
  supportsTls: boolean;
  tlsEnabled: boolean;
  tlsUsesClientAuth: boolean;
  tlsUsesCaFile: boolean;
  tlsShowsServerNameOverride: boolean;
  tlsShouldShowConfigFields: boolean;
  effectiveSshEnabled: boolean;
  effectiveUseSecretStorage: boolean;
  secretStorageRequired: boolean;
  secretStorageLabel: string;
  secretStorageHint: string;
  elasticsearchApiKeyHint: string;
  sshPasswordHint: string;
  sshPrivateKeyHint: string;
  sshPassphraseHint: string;
  tlsKeyPassphraseHint: string;
  tlsServerNameOverrideHint: string;
  sshFingerprintHint: string;
  sqliteWalHint: string;
  tlsModeHint: string;
}

export function computeDerivedState(
  state: ConnectionFormState,
  existing: ConnectionFormExistingState | null,
): ConnectionFormDerived {
  const isEdit = !!existing;
  const hasStoredSecret = existing?.hasStoredSecret ?? false;
  const hasStoredApiKey = existing?.hasStoredApiKey ?? false;
  const hasStoredSshPassword = existing?.hasStoredSshPassword ?? false;
  const hasStoredSshPrivateKey = existing?.hasStoredSshPrivateKey ?? false;
  const hasStoredSshPassphrase = existing?.hasStoredSshPassphrase ?? false;
  const hasStoredTlsKeyPassphrase =
    existing?.hasStoredTlsKeyPassphrase ?? false;

  const isSQLite = state.type === "sqlite";
  const isOracle = state.type === "oracle";
  const isMongo = state.type === "mongodb";
  const isRedis = state.type === "redis";
  const isElasticsearch = state.type === "elasticsearch";
  const isDynamo = state.type === "dynamodb";

  const tlsSupport = getConnectionTlsSupport(state.type);
  const supportsTls = tlsSupport !== undefined;
  const tlsEnabled = supportsTls && isConnectionTlsEnabled(state.tlsMode);
  const tlsUsesClientAuth = tlsEnabled && state.tlsMode === "mutualTls";
  const tlsUsesCaFile =
    tlsEnabled &&
    tlsSupport?.supportsCaFile === true &&
    state.tlsMode !== "requireTrustServerCertificate";
  const tlsShowsServerNameOverride =
    tlsEnabled &&
    tlsSupport?.supportsServerNameOverride === true &&
    state.tlsMode !== "requireTrustServerCertificate";
  const tlsShouldShowConfigFields =
    tlsUsesCaFile || tlsUsesClientAuth || tlsShowsServerNameOverride;

  const effectiveSshEnabled = !isSQLite && state.sshEnabled;
  const secretStorageRequired =
    isElasticsearch ||
    isDynamo ||
    effectiveSshEnabled ||
    (tlsUsesClientAuth &&
      (state.tlsKeyPassphrase.trim().length > 0 || hasStoredTlsKeyPassphrase));
  const effectiveUseSecretStorage = secretStorageRequired
    ? true
    : state.useSecretStorage;

  const secretStorageLabel =
    isElasticsearch || effectiveSshEnabled || secretStorageRequired
      ? "Store secrets in VS Code Secret Storage"
      : "Store password in VS Code Secret Storage";
  const secretStorageHint = isElasticsearch
    ? "Elasticsearch credentials are always saved in your OS keychain and will not appear in settings.json."
    : effectiveSshEnabled
      ? "SSH is enabled, so database and SSH secrets are always saved in your OS keychain and will not appear in settings.json."
      : secretStorageRequired
        ? "TLS client key passphrases are always saved in your OS keychain and will not appear in settings.json."
        : effectiveUseSecretStorage
          ? "Password saved in your OS keychain — will NOT appear in settings.json."
          : "Password will be saved in plaintext in settings.json. Enable to store securely.";

  const elasticsearchApiKeyHint =
    hasStoredApiKey && state.elasticsearchApiKey.length === 0
      ? "Leave blank to keep the stored API key unchanged."
      : "Will be stored securely in VS Code Secret Storage (OS keychain)";
  const sshPasswordHint =
    hasStoredSshPassword && state.sshPassword.length === 0
      ? "Leave blank to keep the stored SSH password unchanged."
      : "Will be stored securely in VS Code Secret Storage (OS keychain).";
  const sshPrivateKeyHint =
    hasStoredSshPrivateKey && state.sshPrivateKey.length === 0
      ? "Leave blank to keep the stored SSH private key unchanged."
      : "Paste the OpenSSH or PEM private key. Stored securely in VS Code Secret Storage (OS keychain).";
  const sshPassphraseHint =
    hasStoredSshPassphrase && state.sshPassphrase.length === 0
      ? "Leave blank to keep the stored SSH passphrase unchanged."
      : "Optional. Stored securely in VS Code Secret Storage (OS keychain).";
  const tlsKeyPassphraseHint =
    hasStoredTlsKeyPassphrase && state.tlsKeyPassphrase.length === 0
      ? "Leave blank to keep the stored TLS client key passphrase unchanged."
      : "Optional. Stored securely in VS Code Secret Storage (OS keychain).";
  const tlsServerNameOverrideHint =
    state.tlsMode === "requireVerifyFull"
      ? "Optional hostname used for TLS SNI and certificate hostname validation."
      : state.tlsMode === "requireVerifyCa"
        ? "Optional hostname used for TLS SNI when certificate host matching is relaxed."
        : "Optional hostname used for TLS SNI.";
  const sshFingerprintHint =
    state.sshHostVerificationMode === "trustOnFirstUse"
      ? state.sshHostFingerprintSha256.trim().length > 0
        ? `The first accepted SSH fingerprint is pinned automatically. Current pinned fingerprint: ${state.sshHostFingerprintSha256.trim()}`
        : "The first successful SSH handshake will pin the discovered SHA256 fingerprint automatically and enforce it on future connections."
      : "Required. Use the OpenSSH SHA256 fingerprint format, for example SHA256:AbCdEf...";
  const sqliteWalHint =
    state.sqliteWalMode === "off"
      ? "Advanced. Automatic WAL handling is disabled for this SQLite connection."
      : state.connectionReadOnly
        ? "Advanced. WAL is enabled automatically for writable SQLite connections; this read-only session leaves the file untouched."
        : "Advanced. WAL is enabled automatically for writable SQLite connections unless you disable it here.";
  const tlsModeHint =
    state.tlsMode === "disabled"
      ? "Connection encryption is disabled."
      : state.tlsMode === "requireVerifyFull"
        ? "Encrypt the connection and verify both the certificate chain and server hostname."
        : state.tlsMode === "requireVerifyCa"
          ? "Encrypt the connection and verify the certificate chain without strict hostname verification."
          : state.tlsMode === "requireTrustServerCertificate"
            ? "Encrypt the connection but accept self-signed or otherwise untrusted server certificates."
            : "Encrypt the connection and present a client certificate and private key to the server.";

  return {
    isEdit,
    hasStoredSecret,
    hasStoredApiKey,
    hasStoredSshPassword,
    hasStoredSshPrivateKey,
    hasStoredSshPassphrase,
    hasStoredTlsKeyPassphrase,
    isSQLite,
    isOracle,
    isMongo,
    isRedis,
    isElasticsearch,
    isDynamo,
    tlsSupport,
    supportsTls,
    tlsEnabled,
    tlsUsesClientAuth,
    tlsUsesCaFile,
    tlsShowsServerNameOverride,
    tlsShouldShowConfigFields,
    effectiveSshEnabled,
    effectiveUseSecretStorage,
    secretStorageRequired,
    secretStorageLabel,
    secretStorageHint,
    elasticsearchApiKeyHint,
    sshPasswordHint,
    sshPrivateKeyHint,
    sshPassphraseHint,
    tlsKeyPassphraseHint,
    tlsServerNameOverrideHint,
    sshFingerprintHint,
    sqliteWalHint,
    tlsModeHint,
  };
}

// ─── Payload Builder ────────────────────────────────────────────────────────

/** Parse a port string, returning the default if empty/invalid, clamping to 1-65535. */
function parsePort(raw: string, defaultPort: number): number {
  const trimmed = raw.trim();
  if (!trimmed) return defaultPort;
  const n = Number(trimmed);
  if (!Number.isFinite(n) || n < 1) return defaultPort;
  return Math.min(Math.trunc(n), 65535);
}

export function buildConnectionPayload(
  state: ConnectionFormState,
  derived: ConnectionFormDerived,
  existing: ConnectionFormExistingState | null,
): ConnectionFormSubmission {
  const parsedSshPort = Number.parseInt(state.sshPort.trim(), 10);
  const tlsConfig = derived.supportsTls
    ? {
        mode: state.tlsMode,
        caFilePath:
          derived.tlsUsesCaFile && derived.tlsSupport?.supportsCaFile
            ? state.tlsCaFilePath.trim() || undefined
            : undefined,
        certFilePath:
          derived.tlsUsesClientAuth &&
          derived.tlsSupport?.supportsClientCertificate
            ? state.tlsCertFilePath.trim() || undefined
            : undefined,
        keyFilePath:
          derived.tlsUsesClientAuth && derived.tlsSupport?.supportsClientKey
            ? state.tlsKeyFilePath.trim() || undefined
            : undefined,
        keyPassphrase:
          derived.tlsUsesClientAuth &&
          derived.tlsSupport?.supportsClientKeyPassphrase
            ? state.tlsKeyPassphrase
            : undefined,
        serverNameOverride:
          derived.tlsEnabled && derived.tlsSupport?.supportsServerNameOverride
            ? state.tlsServerNameOverride.trim() || undefined
            : undefined,
      }
    : undefined;

  return {
    id: existing?.id ?? crypto.randomUUID(),
    name: state.name.trim(),
    type: state.type,
    readOnly: state.connectionReadOnly,
    folder: state.folder.trim() || undefined,
    color: state.color.trim() || undefined,
    useSecretStorage: derived.effectiveUseSecretStorage,
    hasStoredSecret: derived.hasStoredSecret || undefined,
    hasStoredApiKey: derived.hasStoredApiKey || undefined,
    ssh: derived.effectiveSshEnabled
      ? {
          host: state.sshHost.trim() || undefined,
          port:
            Number.isInteger(parsedSshPort) && parsedSshPort > 0
              ? parsedSshPort
              : undefined,
          username: state.sshUsername.trim() || undefined,
          authMethod: state.sshAuthMethod,
          hostVerificationMode: state.sshHostVerificationMode,
          password:
            state.sshAuthMethod === "password" ? state.sshPassword : undefined,
          privateKey:
            state.sshAuthMethod === "privateKey"
              ? state.sshPrivateKey
              : undefined,
          passphrase:
            state.sshAuthMethod === "privateKey"
              ? state.sshPassphrase
              : undefined,
          hostFingerprintSha256:
            state.sshHostFingerprintSha256.trim() || undefined,
        }
      : undefined,
    hasStoredSshPassword: derived.hasStoredSshPassword || undefined,
    hasStoredSshPrivateKey: derived.hasStoredSshPrivateKey || undefined,
    hasStoredSshPassphrase: derived.hasStoredSshPassphrase || undefined,
    hasStoredTlsKeyPassphrase: derived.hasStoredTlsKeyPassphrase || undefined,
    ...(derived.isSQLite
      ? {
          filePath: state.filePath.trim(),
          sqliteWalMode: state.sqliteWalMode,
        }
      : derived.isDynamo
        ? {
            awsRegion: state.awsRegion.trim() || undefined,
            awsAccessKeyId: state.awsAccessKeyId.trim() || undefined,
            awsSecretAccessKey: state.awsSecretAccessKey.trim() || undefined,
            awsSessionToken: state.awsSessionToken.trim() || undefined,
            awsProfile: state.awsProfile.trim() || undefined,
            endpoint: state.dynamoEndpoint.trim() || undefined,
          }
        : {
            host: state.host.trim(),
            port: parsePort(
              state.port,
              DEFAULT_PORT_BY_CONNECTION_TYPE[state.type],
            ),
            database: state.database.trim(),
            username: state.username.trim(),
            password: state.password,
            tls: tlsConfig,
            ...(derived.isMongo
              ? {
                  connectionUri: state.mongoConnectionUri.trim() || undefined,
                }
              : {}),
            ...(derived.isElasticsearch
              ? {
                  endpoint: state.elasticsearchEndpoint.trim() || undefined,
                  apiKey: state.elasticsearchApiKey.trim() || undefined,
                  cloudId: state.elasticsearchCloudId.trim() || undefined,
                }
              : {}),
            ...(derived.isOracle
              ? {
                  serviceName: state.oracleServiceName.trim() || undefined,
                }
              : {}),
          }),
  };
}

// ─── Hook ───────────────────────────────────────────────────────────────────

function getDefaultColor(): string {
  if (
    typeof document !== "undefined" &&
    document.body.classList.contains("vscode-light")
  ) {
    return "#000000";
  }
  return "#ffffff";
}

// ─── Grouped State Shapes ────────────────────────────────────────────────────

interface IdentityState {
  name: string;
  color: string;
}

interface ConnectionState {
  type: ConnectionType;
  host: string;
  port: string;
  database: string;
  username: string;
  password: string;
  filePath: string;
  sqliteWalMode: SQLiteWalMode;
  folder: string;
  connectionReadOnly: boolean;
}

interface TypeSpecificState {
  oracleServiceName: string;
  mongoConnectionUri: string;
  elasticsearchEndpoint: string;
  elasticsearchApiKey: string;
  elasticsearchCloudId: string;
  awsRegion: string;
  awsAccessKeyId: string;
  awsSecretAccessKey: string;
  awsSessionToken: string;
  awsProfile: string;
  dynamoEndpoint: string;
}

interface SshState {
  sshEnabled: boolean;
  sshHost: string;
  sshPort: string;
  sshUsername: string;
  sshAuthMethod: ConnectionSshAuthMethod;
  sshHostVerificationMode: ConnectionSshHostVerificationMode;
  sshPassword: string;
  sshPrivateKey: string;
  sshPassphrase: string;
  sshHostFingerprintSha256: string;
}

interface TlsState {
  tlsMode: ConnectionTlsMode;
  tlsCaFilePath: string;
  tlsCertFilePath: string;
  tlsKeyFilePath: string;
  tlsKeyPassphrase: string;
  tlsServerNameOverride: string;
}

interface UiState {
  useSecretStorage: boolean;
  testState: "idle" | "testing" | "ok" | "fail";
  testError: string;
  nameError: string;
  saving: boolean;
}

// ─── Hook ───────────────────────────────────────────────────────────────────

export function useConnectionFormState(
  existing: ConnectionFormExistingState | null,
) {
  const initialTlsMode = resolveConnectionTlsMode(existing ?? {});
  const isEdit = !!existing;

  const [identity, setIdentity] = useState<IdentityState>({
    name: existing?.name ?? "",
    color: existing?.color ?? getDefaultColor(),
  });

  const [conn, setConn] = useState<ConnectionState>({
    type: existing?.type ?? "pg",
    host: existing?.host ?? "localhost",
    port: String(
      existing?.port ?? DEFAULT_PORT_BY_CONNECTION_TYPE[existing?.type ?? "pg"],
    ),
    database: existing?.database ?? "",
    username: existing?.username ?? "",
    password: "",
    filePath: existing?.filePath ?? "",
    sqliteWalMode: existing?.sqliteWalMode === "off" ? "off" : "auto",
    folder: existing?.folder ?? "",
    connectionReadOnly: existing?.readOnly ?? false,
  });

  const [typeSpecific, setTypeSpecific] = useState<TypeSpecificState>({
    oracleServiceName: existing?.serviceName ?? existing?.database ?? "",
    mongoConnectionUri: existing?.connectionUri ?? existing?.uri ?? "",
    elasticsearchEndpoint: existing?.endpoint ?? existing?.connectionUri ?? "",
    elasticsearchApiKey: "",
    elasticsearchCloudId: existing?.cloudId ?? "",
    awsRegion: existing?.awsRegion ?? "",
    awsAccessKeyId: "",
    awsSecretAccessKey: "",
    awsSessionToken: "",
    awsProfile: existing?.awsProfile ?? "",
    dynamoEndpoint: existing?.endpoint ?? existing?.awsEndpoint ?? "",
  });

  const [ssh, setSsh] = useState<SshState>({
    sshEnabled: !!existing?.ssh,
    sshHost: existing?.ssh?.host ?? "",
    sshPort: String(existing?.ssh?.port ?? 22),
    sshUsername: existing?.ssh?.username ?? "",
    sshAuthMethod: existing?.ssh?.authMethod ?? "privateKey",
    sshHostVerificationMode: existing?.ssh?.hostVerificationMode ?? "manual",
    sshPassword: "",
    sshPrivateKey: "",
    sshPassphrase: "",
    sshHostFingerprintSha256: existing?.ssh?.hostFingerprintSha256 ?? "",
  });

  const [tls, setTls] = useState<TlsState>({
    tlsMode: initialTlsMode,
    tlsCaFilePath: existing?.tls?.caFilePath ?? "",
    tlsCertFilePath: existing?.tls?.certFilePath ?? "",
    tlsKeyFilePath: existing?.tls?.keyFilePath ?? "",
    tlsKeyPassphrase: "",
    tlsServerNameOverride: existing?.tls?.serverNameOverride ?? "",
  });

  const [ui, setUi] = useState<UiState>({
    useSecretStorage: isEdit ? (existing?.useSecretStorage ?? false) : true,
    testState: "idle",
    testError: "",
    nameError: "",
    saving: false,
  });

  // ─── Adapter Setters (preserve external API) ─────────────────────────────

  const setName = useCallback(
    (v: string) => setIdentity((s) => ({ ...s, name: v })),
    [],
  );
  const setColor = useCallback(
    (v: string) => setIdentity((s) => ({ ...s, color: v })),
    [],
  );

  const _setType = useCallback(
    (v: ConnectionType) => setConn((s) => ({ ...s, type: v })),
    [],
  );
  const setHost = useCallback(
    (v: string) => setConn((s) => ({ ...s, host: v })),
    [],
  );
  const setPort = useCallback(
    (v: string) => setConn((s) => ({ ...s, port: v })),
    [],
  );
  const setDatabase = useCallback(
    (v: string) => setConn((s) => ({ ...s, database: v })),
    [],
  );
  const setUsername = useCallback(
    (v: string) => setConn((s) => ({ ...s, username: v })),
    [],
  );
  const setPassword = useCallback(
    (v: string) => setConn((s) => ({ ...s, password: v })),
    [],
  );
  const setFilePath = useCallback(
    (v: string) => setConn((s) => ({ ...s, filePath: v })),
    [],
  );
  const setSqliteWalMode = useCallback(
    (v: SQLiteWalMode) => setConn((s) => ({ ...s, sqliteWalMode: v })),
    [],
  );
  const setFolder = useCallback(
    (v: string) => setConn((s) => ({ ...s, folder: v })),
    [],
  );
  const setConnectionReadOnly = useCallback(
    (v: boolean) => setConn((s) => ({ ...s, connectionReadOnly: v })),
    [],
  );

  const setOracleServiceName = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, oracleServiceName: v })),
    [],
  );
  const setMongoConnectionUri = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, mongoConnectionUri: v })),
    [],
  );
  const setElasticsearchEndpoint = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, elasticsearchEndpoint: v })),
    [],
  );
  const setElasticsearchApiKey = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, elasticsearchApiKey: v })),
    [],
  );
  const setElasticsearchCloudId = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, elasticsearchCloudId: v })),
    [],
  );
  const setAwsRegion = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, awsRegion: v })),
    [],
  );
  const setAwsAccessKeyId = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, awsAccessKeyId: v })),
    [],
  );
  const setAwsSecretAccessKey = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, awsSecretAccessKey: v })),
    [],
  );
  const setAwsSessionToken = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, awsSessionToken: v })),
    [],
  );
  const setAwsProfile = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, awsProfile: v })),
    [],
  );
  const setDynamoEndpoint = useCallback(
    (v: string) => setTypeSpecific((s) => ({ ...s, dynamoEndpoint: v })),
    [],
  );

  const setSshEnabled = useCallback(
    (v: boolean) => setSsh((s) => ({ ...s, sshEnabled: v })),
    [],
  );
  const setSshHost = useCallback(
    (v: string) => setSsh((s) => ({ ...s, sshHost: v })),
    [],
  );
  const setSshPort = useCallback(
    (v: string) => setSsh((s) => ({ ...s, sshPort: v })),
    [],
  );
  const setSshUsername = useCallback(
    (v: string) => setSsh((s) => ({ ...s, sshUsername: v })),
    [],
  );
  const setSshAuthMethod = useCallback(
    (v: ConnectionSshAuthMethod) => setSsh((s) => ({ ...s, sshAuthMethod: v })),
    [],
  );
  const setSshHostVerificationMode = useCallback(
    (v: ConnectionSshHostVerificationMode) =>
      setSsh((s) => ({ ...s, sshHostVerificationMode: v })),
    [],
  );
  const setSshPassword = useCallback(
    (v: string) => setSsh((s) => ({ ...s, sshPassword: v })),
    [],
  );
  const setSshPrivateKey = useCallback(
    (v: string) => setSsh((s) => ({ ...s, sshPrivateKey: v })),
    [],
  );
  const setSshPassphrase = useCallback(
    (v: string) => setSsh((s) => ({ ...s, sshPassphrase: v })),
    [],
  );
  const setSshHostFingerprintSha256 = useCallback(
    (v: string) => setSsh((s) => ({ ...s, sshHostFingerprintSha256: v })),
    [],
  );

  const setTlsMode = useCallback(
    (v: ConnectionTlsMode) => setTls((s) => ({ ...s, tlsMode: v })),
    [],
  );
  const setTlsCaFilePath = useCallback(
    (v: string) => setTls((s) => ({ ...s, tlsCaFilePath: v })),
    [],
  );
  const setTlsCertFilePath = useCallback(
    (v: string) => setTls((s) => ({ ...s, tlsCertFilePath: v })),
    [],
  );
  const setTlsKeyFilePath = useCallback(
    (v: string) => setTls((s) => ({ ...s, tlsKeyFilePath: v })),
    [],
  );
  const setTlsKeyPassphrase = useCallback(
    (v: string) => setTls((s) => ({ ...s, tlsKeyPassphrase: v })),
    [],
  );
  const setTlsServerNameOverride = useCallback(
    (v: string) => setTls((s) => ({ ...s, tlsServerNameOverride: v })),
    [],
  );

  const setUseSecretStorage = useCallback(
    (v: boolean) => setUi((s) => ({ ...s, useSecretStorage: v })),
    [],
  );
  const setTestState = useCallback(
    (v: UiState["testState"]) => setUi((s) => ({ ...s, testState: v })),
    [],
  );
  const setTestError = useCallback(
    (v: string) => setUi((s) => ({ ...s, testError: v })),
    [],
  );
  const setNameError = useCallback(
    (v: string) => setUi((s) => ({ ...s, nameError: v })),
    [],
  );
  const setSaving = useCallback(
    (v: boolean) => setUi((s) => ({ ...s, saving: v })),
    [],
  );

  const handleTypeChange = useCallback((nextType: ConnectionType) => {
    setConn((s) => ({
      ...s,
      type: nextType,
      port: String(DEFAULT_PORT_BY_CONNECTION_TYPE[nextType] || ""),
    }));
    setUi((s) => ({ ...s, testState: "idle" }));
    setTls({
      tlsMode: "disabled",
      tlsCaFilePath: "",
      tlsCertFilePath: "",
      tlsKeyFilePath: "",
      tlsKeyPassphrase: "",
      tlsServerNameOverride: "",
    });
  }, []);

  // ─── Assemble Flat State Object (same shape as ConnectionFormState) ────────

  const state: ConnectionFormState = {
    name: identity.name,
    setName,
    nameError: ui.nameError,
    setNameError,
    color: identity.color,
    setColor,
    type: conn.type,
    handleTypeChange,
    host: conn.host,
    setHost,
    port: conn.port,
    setPort,
    database: conn.database,
    setDatabase,
    username: conn.username,
    setUsername,
    password: conn.password,
    setPassword,
    filePath: conn.filePath,
    setFilePath,
    sqliteWalMode: conn.sqliteWalMode,
    setSqliteWalMode,
    folder: conn.folder,
    setFolder,
    connectionReadOnly: conn.connectionReadOnly,
    setConnectionReadOnly,
    oracleServiceName: typeSpecific.oracleServiceName,
    setOracleServiceName,
    mongoConnectionUri: typeSpecific.mongoConnectionUri,
    setMongoConnectionUri,
    elasticsearchEndpoint: typeSpecific.elasticsearchEndpoint,
    setElasticsearchEndpoint,
    elasticsearchApiKey: typeSpecific.elasticsearchApiKey,
    setElasticsearchApiKey,
    elasticsearchCloudId: typeSpecific.elasticsearchCloudId,
    setElasticsearchCloudId,
    awsRegion: typeSpecific.awsRegion,
    setAwsRegion,
    awsAccessKeyId: typeSpecific.awsAccessKeyId,
    setAwsAccessKeyId,
    awsSecretAccessKey: typeSpecific.awsSecretAccessKey,
    setAwsSecretAccessKey,
    awsSessionToken: typeSpecific.awsSessionToken,
    setAwsSessionToken,
    awsProfile: typeSpecific.awsProfile,
    setAwsProfile,
    dynamoEndpoint: typeSpecific.dynamoEndpoint,
    setDynamoEndpoint,
    sshEnabled: ssh.sshEnabled,
    setSshEnabled,
    sshHost: ssh.sshHost,
    setSshHost,
    sshPort: ssh.sshPort,
    setSshPort,
    sshUsername: ssh.sshUsername,
    setSshUsername,
    sshAuthMethod: ssh.sshAuthMethod,
    setSshAuthMethod,
    sshHostVerificationMode: ssh.sshHostVerificationMode,
    setSshHostVerificationMode,
    sshPassword: ssh.sshPassword,
    setSshPassword,
    sshPrivateKey: ssh.sshPrivateKey,
    setSshPrivateKey,
    sshPassphrase: ssh.sshPassphrase,
    setSshPassphrase,
    sshHostFingerprintSha256: ssh.sshHostFingerprintSha256,
    setSshHostFingerprintSha256,
    tlsMode: tls.tlsMode,
    setTlsMode,
    tlsCaFilePath: tls.tlsCaFilePath,
    setTlsCaFilePath,
    tlsCertFilePath: tls.tlsCertFilePath,
    setTlsCertFilePath,
    tlsKeyFilePath: tls.tlsKeyFilePath,
    setTlsKeyFilePath,
    tlsKeyPassphrase: tls.tlsKeyPassphrase,
    setTlsKeyPassphrase,
    tlsServerNameOverride: tls.tlsServerNameOverride,
    setTlsServerNameOverride,
    useSecretStorage: ui.useSecretStorage,
    setUseSecretStorage,
    testState: ui.testState,
    setTestState,
    testError: ui.testError,
    setTestError,
    saving: ui.saving,
    setSaving,
  };

  const derived = useMemo(
    () => computeDerivedState(state, existing),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [existing, state],
  );

  const buildPayload = useCallback(
    () => buildConnectionPayload(state, derived, existing),
    [state, derived, existing],
  );

  return { state, derived, buildPayload };
}
