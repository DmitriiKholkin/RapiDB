import * as path from "node:path";
import * as vscode from "vscode";
import { resolveAwsCredentialMode } from "../../shared/connectionConfig";
import {
  buildMongoFieldsUri,
  getMongoConnectionUri,
  getMongoFormHost,
  getMongoFormUri,
  normalizeMongoConnectionConfig,
  resolveMongoConnectionMode,
} from "../../shared/mongoConnection";
import {
  type ConnectionFormBrowseTarget,
  type ConnectionFormExistingState,
  type ConnectionFormSubmission,
  parseConnectionFormPanelMessage,
} from "../../shared/webviewContracts";
import {
  type ConnectionSavePrecondition,
  connectionEditState,
  connectionEditStatesMatch,
  preserveTrustedSshFingerprint,
} from "../connectionEditState";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import { readConnectionSecret } from "../connectionManagerStore";
import {
  assertConnectionSecretBinding,
  connectionSecretTarget,
  sameConnectionTofuPin,
} from "../connectionSecretBinding";
import {
  type ConnectionSecretClearOptions,
  connectionTypeUsesPassword,
  extractCredentialBearingUriSecret,
  getAmbiguousCredentialBearingUriFields,
  hasConnectionConfigSecrets,
  omitAmbiguousCredentialBearingUris,
  resolveStoredCredentialBearingUri,
  restoreCredentialBearingUri,
  restoreMongoUriCredentials,
  sanitizeConnectionConfigForResponse,
  sanitizeCredentialBearingUri,
  shouldKeepHandWrittenSecretsInSettings,
  trimOptionalSecretValue,
  trimOptionalUriValue,
} from "../connectionSecrets";
import {
  parseStoredConnectionSecrets,
  readConnectionCredentialRevision,
  type StoredConnectionSecrets,
} from "../connectionSecretsData";
import { ConnectionValidationService } from "../services/connectionValidationService";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { attachPanelMessageHandler } from "./panelLifecycle";
import { createPanelWebviewOptions } from "./panelRetentionPolicy";
import { createWebviewShell } from "./webviewShell";

const CONNECTION_FORM_RETENTION_MODE = "retain" as const;
const LAST_SQLITE_DIRECTORY_STATE_KEY = "rapidb.lastSqliteDirectory";

function restoreSubmittedUriValue(
  submittedValue: string | undefined,
  storedValue: string | undefined,
  existingValue: string | undefined,
): string | undefined {
  const normalizedSubmitted = trimOptionalUriValue(submittedValue);
  if (!normalizedSubmitted) {
    return normalizedSubmitted;
  }

  for (const candidate of [storedValue, existingValue]) {
    const resolved = resolveStoredCredentialBearingUri(
      normalizedSubmitted,
      trimOptionalUriValue(candidate),
    );
    if (resolved !== normalizedSubmitted) {
      return resolved;
    }
  }

  return normalizedSubmitted;
}

const CONNECTION_ID_MISMATCH_MESSAGE =
  "This editor belongs to a different connection. Reopen Edit Connection.";

const AMBIGUOUS_STORED_URI_MESSAGE =
  "The saved connection URL has ambiguous credentials and is not shown. Enter it again with percent-encoded credentials, or choose to remove it.";

const CREDENTIAL_BEARING_URI_ALIASES: Partial<
  Record<
    ConnectionConfig["type"],
    ReturnType<typeof getAmbiguousCredentialBearingUriFields>
  >
> = {
  mongodb: ["connectionUri", "uri"],
  elasticsearch: ["connectionUri", "endpoint"],
  dynamodb: ["endpoint", "awsEndpoint"],
};

function hasUnresolvedAmbiguousStoredUri(
  stored: ConnectionConfig | undefined,
  submitted: ConnectionConfig,
  clear: boolean | undefined,
): boolean {
  if (!stored || clear) return false;
  const aliases = CREDENTIAL_BEARING_URI_ALIASES[submitted.type] ?? [];
  return getAmbiguousCredentialBearingUriFields(stored).some((field) =>
    (aliases.includes(field) ? aliases : [field]).every(
      (key) => trimOptionalUriValue(submitted[key]) === undefined,
    ),
  );
}

function usesStoredSecrets(config: ConnectionConfig | undefined): boolean {
  return (
    config !== undefined &&
    config.useSecretStorage !== false &&
    config.type !== "sqlite" &&
    !shouldKeepHandWrittenSecretsInSettings(config)
  );
}

function readRawMessageIdentity(message: unknown): {
  type?: unknown;
  requestId?: string;
} {
  if (typeof message !== "object" || message === null) {
    return {};
  }
  const { type, payload } = message as { type?: unknown; payload?: unknown };
  const requestId =
    typeof payload === "object" && payload !== null
      ? (payload as { requestId?: unknown }).requestId
      : undefined;
  return {
    type,
    requestId: typeof requestId === "string" ? requestId : undefined,
  };
}

function getExistingMongoUri(
  existing: ConnectionConfig,
  secrets: StoredConnectionSecrets,
): string | undefined {
  const value = getMongoConnectionUri(existing);
  const usesCanonicalUri = Boolean(existing.connectionUri?.trim());
  return resolveStoredCredentialBearingUri(
    value,
    usesCanonicalUri ? secrets.connectionUri : secrets.uri,
  );
}

function getExistingRedisUri(
  existing: ConnectionConfig,
  secrets: StoredConnectionSecrets,
): string | undefined {
  const value = resolveStoredCredentialBearingUri(
    existing.connectionUri,
    secrets.connectionUri,
  );
  if (!value) return value;
  try {
    const parsed = new URL(value);
    if (!["redis:", "rediss:"].includes(parsed.protocol)) return value;
    if (!parsed.port && existing.port !== undefined) {
      parsed.port = String(existing.port);
    }
    const database = existing.database?.trim();
    if (database) parsed.pathname = `/${database.replace(/^db/, "")}`;
    if (parsed.username || parsed.password) return parsed.toString();
    const username = existing.username?.trim();
    const password = secrets.password ?? existing.password;
    if (!username && !password) return parsed.toString();
    parsed.username = encodeURIComponent(username ?? "");
    parsed.password = encodeURIComponent(password ?? "");
    return parsed.toString();
  } catch {
    return value;
  }
}

function getExistingElasticEndpoint(
  existing: ConnectionConfig,
  secrets: StoredConnectionSecrets,
): string | undefined {
  return existing.connectionUri !== undefined
    ? resolveStoredCredentialBearingUri(
        existing.connectionUri,
        secrets.connectionUri,
      )
    : resolveStoredCredentialBearingUri(existing.endpoint, secrets.endpoint);
}

function sanitizeExistingForForm(
  existing: ConnectionConfig,
  storedSecrets: StoredConnectionSecrets,
): ConnectionFormExistingState {
  const {
    password: _password,
    apiKey: _apiKey,
    awsAccessKeyId: _awsAccessKeyId,
    awsSecretAccessKey: _awsSecretAccessKey,
    awsSessionToken: _awsSessionToken,
    ssh: _ssh,
    connectionUri,
    uri,
    endpoint,
    awsEndpoint,
    ...rest
  } = existing;
  delete rest.secretStorageProfile;
  const existingRedisUri =
    existing.type === "redis"
      ? getExistingRedisUri(existing, storedSecrets)
      : undefined;
  const existingElasticEndpoint =
    existing.type === "elasticsearch"
      ? getExistingElasticEndpoint(existing, storedSecrets)
      : endpoint;
  const hasStoredAwsKeyPair =
    trimOptionalSecretValue(storedSecrets.awsAccessKeyId ?? _awsAccessKeyId) !==
      undefined &&
    trimOptionalSecretValue(
      storedSecrets.awsSecretAccessKey ?? _awsSecretAccessKey,
    ) !== undefined;
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
    ...(existing.type === "dynamodb"
      ? {
          awsCredentialMode: resolveAwsCredentialMode({
            ...existing,
            awsAccessKeyId: storedSecrets.awsAccessKeyId ?? _awsAccessKeyId,
            awsSecretAccessKey:
              storedSecrets.awsSecretAccessKey ?? _awsSecretAccessKey,
            awsSessionToken: storedSecrets.awsSessionToken ?? _awsSessionToken,
          }),
        }
      : {}),
    ssh: sanitizedSsh,
    tls,
    connectionUri: sanitizeCredentialBearingUri(
      existing.type === "mongodb"
        ? getMongoFormUri(
            existing,
            getExistingMongoUri(existing, storedSecrets),
          )
        : existing.type === "redis"
          ? existingRedisUri
          : connectionUri,
    ),
    hasStoredConnectionUriCredentials:
      (existing.type === "redis" &&
        extractCredentialBearingUriSecret(existingRedisUri) !== undefined) ||
      (existing.type === "elasticsearch" &&
        extractCredentialBearingUriSecret(existingElasticEndpoint) !==
          undefined),
    hasStoredMongoUriCredentials:
      existing.type === "mongodb" &&
      extractCredentialBearingUriSecret(
        getExistingMongoUri(existing, storedSecrets),
      ) !== undefined,
    uri: sanitizeCredentialBearingUri(uri),
    endpoint: sanitizeCredentialBearingUri(
      existing.type === "elasticsearch" ? existingElasticEndpoint : endpoint,
    ),
    awsEndpoint: sanitizeCredentialBearingUri(awsEndpoint),
    hasStoredSecret:
      storedSecrets.password !== undefined ||
      trimOptionalSecretValue(_password) !== undefined ||
      undefined,
    hasStoredApiKey:
      storedSecrets.apiKey !== undefined ||
      trimOptionalSecretValue(_apiKey) !== undefined ||
      undefined,
    hasStoredAwsCredentials:
      storedSecrets.awsAccessKeyId !== undefined ||
      storedSecrets.awsSecretAccessKey !== undefined ||
      storedSecrets.awsSessionToken !== undefined ||
      trimOptionalSecretValue(_awsAccessKeyId) !== undefined ||
      trimOptionalSecretValue(_awsSecretAccessKey) !== undefined ||
      trimOptionalSecretValue(_awsSessionToken) !== undefined ||
      undefined,
    hasStoredAwsKeyPair: hasStoredAwsKeyPair || undefined,
    hasStoredSshPassword:
      storedSecrets.sshPassword !== undefined ||
      trimOptionalSecretValue(_ssh?.password) !== undefined ||
      undefined,
    hasStoredSshPrivateKey:
      storedSecrets.sshPrivateKey !== undefined ||
      trimOptionalSecretValue(_ssh?.privateKey) !== undefined ||
      undefined,
    hasStoredSshPassphrase:
      storedSecrets.sshPassphrase !== undefined ||
      trimOptionalSecretValue(_ssh?.passphrase) !== undefined ||
      undefined,
    hasStoredTlsKeyPassphrase:
      storedSecrets.tlsKeyPassphrase !== undefined ||
      trimOptionalSecretValue(existing.tls?.keyPassphrase) !== undefined ||
      undefined,
  };
}

function formCredentialTarget(
  config: ConnectionConfig,
  fromSettings = false,
): string {
  if (config.type === "redis" && config.connectionUri) {
    return connectionSecretTarget(
      fromSettings
        ? {
            ...config,
            host: undefined,
            port: undefined,
            database: undefined,
            username: undefined,
          }
        : config,
    );
  }
  if (config.type === "mongodb") {
    const mode = resolveMongoConnectionMode(config);
    // Compare the form's canonical URI/field representations, not legacy
    // alias placement. The database override is not an auth DB in URI mode.
    const uri =
      mode === "uri"
        ? fromSettings
          ? getMongoConnectionUri(config)
          : getMongoFormUri(config, getMongoConnectionUri(config))
        : buildMongoFieldsUri({
            ...config,
            host: getMongoFormHost(config),
            username: undefined,
            password: undefined,
          });
    return connectionSecretTarget({
      ...config,
      host: undefined,
      port: undefined,
      database: undefined,
      username: mode === "fields" ? config.username : undefined,
      connectionUri: uri,
      uri: undefined,
      mongoConnectionMode: mode,
      mongoScheme: undefined,
      mongoAuthMethod:
        mode === "fields"
          ? (config.mongoAuthMethod ?? (config.username ? "password" : "none"))
          : undefined,
      authSource: undefined,
      authMechanism: undefined,
      replicaSet: undefined,
      directConnection: undefined,
    });
  }
  if (
    config.type === "elasticsearch" &&
    (config.connectionUri || config.endpoint || config.cloudId)
  ) {
    return connectionSecretTarget({
      ...config,
      connectionUri: undefined,
      endpoint: config.connectionUri ?? config.endpoint,
      host: undefined,
      port: undefined,
    });
  }
  if (config.type === "dynamodb") {
    return connectionSecretTarget({
      ...config,
      endpoint: config.endpoint ?? config.awsEndpoint,
      awsEndpoint: undefined,
      // Legacy static profiles could also carry an inactive profile name.
      awsProfile:
        config.awsCredentialMode === "static" ? undefined : config.awsProfile,
    });
  }
  return connectionSecretTarget(config);
}

export class ConnectionFormPanel {
  private static readonly viewType = "rapidb.connectionForm";
  private readonly validationService = new ConnectionValidationService();

  private readonly panel: vscode.WebviewPanel;
  private readonly context: vscode.ExtensionContext;
  private readonly connectionManager: ConnectionManager;
  private resolveFn?: (result: ConnectionConfig | undefined) => void;
  private disposed = false;
  private saveInProgress = false;
  private testAbortController: AbortController | null = null;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    existing?: ConnectionConfig,
    private readonly savePrecondition: ConnectionSavePrecondition = {
      kind: "create",
    },
  ) {
    this.panel = panel;
    this.context = context;
    this.connectionManager = connectionManager;

    this.panel.webview.html = this.buildHtml(context, existing);
    attachPanelMessageHandler(
      this.panel,
      (message) => this.handleMessage(message),
      (error, message) => {
        if (this.disposed) return;
        const normalized = logErrorWithContext(
          "ConnectionFormPanel unhandled error",
          error,
        );
        const { type, requestId } = readRawMessageIdentity(message);
        if (type === "saveConnection") {
          this.panel.webview.postMessage({
            type: "saveResult",
            payload: { success: false, error: normalized.message },
          });
        } else if (type === "testConnection") {
          this.panel.webview.postMessage({
            type: "testResult",
            payload: { requestId, success: false, error: normalized.message },
          });
        } else {
          vscode.window.showErrorMessage(
            `[RapiDB] Unexpected error: ${normalized.message}`,
          );
        }
      },
    );
    this.panel.onDidDispose(() => {
      this.disposed = true;
      this.testAbortController?.abort();
      this.testAbortController = null;
      this.resolveFn?.(undefined);
      this.resolveFn = undefined;
    });
  }

  static async show(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    existing?: ConnectionConfig,
  ): Promise<ConnectionConfig | undefined> {
    const title = existing ? `Edit — ${existing.name}` : "New Connection";

    let existingForForm: ConnectionFormExistingState | undefined;
    let precondition: ConnectionSavePrecondition = { kind: "create" };
    if (existing) {
      existing = structuredClone(existing);
      let storedSecrets: StoredConnectionSecrets = {};
      let secretsRead = true;
      let revision: string | undefined;
      if (existing.id && usesStoredSecrets(existing)) {
        try {
          const raw = await readConnectionSecret(context.secrets, existing.id);
          assertConnectionSecretBinding(existing, raw);
          storedSecrets = parseStoredConnectionSecrets(raw);
          revision = readConnectionCredentialRevision(raw);
        } catch {
          secretsRead = false;
        }
      }

      precondition = {
        kind: "edit",
        id: existing.id,
        state: secretsRead
          ? connectionEditState(existing, storedSecrets, revision)
          : undefined,
      };
      existingForForm = {
        ...sanitizeExistingForForm(
          omitAmbiguousCredentialBearingUris(existing),
          storedSecrets,
        ),
        useSecretStorage: existing.useSecretStorage ?? true,
        hasAmbiguousStoredUri:
          getAmbiguousCredentialBearingUriFields(existing).length > 0 ||
          undefined,
      };
    }

    const panel = vscode.window.createWebviewPanel(
      ConnectionFormPanel.viewType,
      title,
      vscode.ViewColumn.One,
      createPanelWebviewOptions(CONNECTION_FORM_RETENTION_MODE),
    );

    const instance = new ConnectionFormPanel(
      panel,
      context,
      connectionManager,
      existingForForm,
      precondition,
    );

    return new Promise<ConnectionConfig | undefined>((resolve) => {
      instance.resolveFn = resolve;
    });
  }

  private resolveSubmittedPassword(
    payload: ConnectionFormSubmission,
    storedSecrets: StoredConnectionSecrets,
    existing?: ConnectionConfig,
  ): string {
    if (payload.password !== undefined && payload.password !== "") {
      return payload.password;
    }
    if (payload.clearPassword) {
      return "";
    }

    return (
      storedSecrets.password ?? existing?.password ?? payload.password ?? ""
    );
  }

  private getStoredConnection(id: string): ConnectionConfig | undefined {
    return this.savePrecondition.kind === "edit" &&
      id === this.savePrecondition.id
      ? this.connectionManager.getConnection(id, true)
      : undefined;
  }

  private changesStoredConnectionType(
    config: Pick<ConnectionConfig, "id" | "type">,
  ): boolean {
    const stored = this.getStoredConnection(config.id);
    return stored !== undefined && stored.type !== config.type;
  }

  private rejectsConnectionId(id: string): boolean {
    return (
      this.savePrecondition.kind === "edit" && id !== this.savePrecondition.id
    );
  }

  private resolveSubmittedSecret(
    submittedValue: string | undefined,
    storedValue: string | undefined,
    existingValue: string | undefined,
  ): string | undefined {
    return (
      trimOptionalSecretValue(submittedValue) ??
      storedValue ??
      trimOptionalSecretValue(existingValue)
    );
  }

  private resolveSubmittedConfig(
    payload: ConnectionFormSubmission,
    storedExisting: ConnectionConfig | undefined,
    storedSecrets: StoredConnectionSecrets,
  ): ConnectionConfig {
    if (
      storedExisting &&
      this.savePrecondition.kind === "edit" &&
      this.savePrecondition.state
    ) {
      const { ssh } = preserveTrustedSshFingerprint(
        payload,
        this.savePrecondition.state,
        storedExisting,
      );
      payload = { ...payload, ssh };
    }
    const safeExisting =
      storedExisting && omitAmbiguousCredentialBearingUris(storedExisting);
    const existingForForm =
      safeExisting && sanitizeExistingForForm(safeExisting, storedSecrets);
    const draftTarget =
      existingForForm &&
      formCredentialTarget({
        ...payload,
        ...(payload.type === "dynamodb" &&
        payload.awsCredentialMode === undefined &&
        existingForForm?.type === "dynamodb" &&
        payload.awsProfile === existingForForm.awsProfile
          ? {
              awsCredentialMode: payload.clearAwsCredentials
                ? payload.awsProfile
                  ? "profile"
                  : existingForForm.awsCredentialMode === undefined
                    ? undefined
                    : "default"
                : existingForForm.awsCredentialMode,
            }
          : {}),
      });
    const mayInherit =
      storedExisting !== undefined &&
      getAmbiguousCredentialBearingUriFields(storedExisting).length === 0 &&
      sameConnectionTofuPin(storedExisting, payload) &&
      existingForForm !== undefined &&
      draftTarget === formCredentialTarget(existingForForm, true);
    const existing = mayInherit ? safeExisting : undefined;
    const resolvedStoredSecrets = mayInherit ? storedSecrets : {};
    const useSecretStorage = payload.useSecretStorage !== false;
    const isRedisUriSubmission =
      payload.type === "redis" &&
      trimOptionalUriValue(payload.connectionUri) !== undefined;
    const password =
      isRedisUriSubmission || !connectionTypeUsesPassword(payload)
        ? undefined
        : this.resolveSubmittedPassword(
            payload,
            resolvedStoredSecrets,
            existing,
          );
    const {
      hasStoredSecret: _hasStoredSecret,
      hasStoredMongoUriCredentials: _hasStoredMongoUriCredentials,
      hasStoredConnectionUriCredentials: _hasStoredConnectionUriCredentials,
      hasStoredAwsCredentials: _hasStoredAwsCredentials,
      keepConnectionUriCredentials: _keepConnectionUriCredentials,
      keepMongoUriCredentials: _keepMongoUriCredentials,
      clearPassword: _clearPassword,
      clearApiKey: _clearApiKey,
      clearAwsCredentials: _clearAwsCredentials,
      clearSshPassphrase: _clearSshPassphrase,
      clearTlsKeyPassphrase: _clearTlsKeyPassphrase,
      clearAmbiguousStoredUri: _clearAmbiguousStoredUri,
      hasStoredApiKey: _hasStoredApiKey,
      hasStoredSshPassword: _hasStoredSshPassword,
      hasStoredSshPrivateKey: _hasStoredSshPrivateKey,
      hasStoredSshPassphrase: _hasStoredSshPassphrase,
      hasStoredTlsKeyPassphrase: _hasStoredTlsKeyPassphrase,
      connectionUri: submittedConnectionUri,
      uri: submittedUri,
      endpoint: submittedEndpoint,
      awsEndpoint: submittedAwsEndpoint,
      ssh: submittedSsh,
      ...rest
    } = payload;
    const ssh = submittedSsh
      ? {
          host: submittedSsh.host,
          port: submittedSsh.port,
          username: submittedSsh.username,
          authMethod: submittedSsh.authMethod,
          hostVerificationMode: submittedSsh.hostVerificationMode,
          hostFingerprintSha256: submittedSsh.hostFingerprintSha256,
          password:
            submittedSsh.authMethod === "password"
              ? this.resolveSubmittedSecret(
                  submittedSsh.password,
                  resolvedStoredSecrets.sshPassword,
                  existing?.ssh?.password,
                )
              : undefined,
          privateKey:
            submittedSsh.authMethod === "privateKey"
              ? this.resolveSubmittedSecret(
                  submittedSsh.privateKey,
                  resolvedStoredSecrets.sshPrivateKey,
                  existing?.ssh?.privateKey,
                )
              : undefined,
          passphrase:
            submittedSsh.authMethod === "privateKey"
              ? this.resolveSubmittedSecret(
                  submittedSsh.passphrase,
                  payload.clearSshPassphrase
                    ? undefined
                    : resolvedStoredSecrets.sshPassphrase,
                  payload.clearSshPassphrase
                    ? undefined
                    : existing?.ssh?.passphrase,
                )
              : undefined,
        }
      : undefined;
    const tls =
      rest.tls !== undefined
        ? {
            ...rest.tls,
            keyPassphrase:
              rest.tls.mode === "mutualTls"
                ? this.resolveSubmittedSecret(
                    rest.tls.keyPassphrase,
                    payload.clearTlsKeyPassphrase
                      ? undefined
                      : resolvedStoredSecrets.tlsKeyPassphrase,
                    payload.clearTlsKeyPassphrase
                      ? undefined
                      : existing?.tls?.keyPassphrase,
                  )
                : undefined,
          }
        : rest.tls;
    const prefersElasticsearchBasicAuth =
      payload.type === "elasticsearch" &&
      Boolean(payload.username?.trim()) &&
      trimOptionalSecretValue(payload.password) !== undefined;
    const hasAwsProfile =
      payload.type === "dynamodb" && Boolean(payload.awsProfile?.trim());
    const hasSubmittedAwsKeyMaterial =
      payload.type === "dynamodb" &&
      (trimOptionalSecretValue(payload.awsAccessKeyId) !== undefined ||
        trimOptionalSecretValue(payload.awsSecretAccessKey) !== undefined);
    const hasSubmittedAwsCredentialMaterial =
      hasSubmittedAwsKeyMaterial ||
      (payload.type === "dynamodb" &&
        trimOptionalSecretValue(payload.awsSessionToken) !== undefined);
    const useAwsProfile = hasAwsProfile && !hasSubmittedAwsCredentialMaterial;
    const hasExistingAwsKeyPair =
      trimOptionalSecretValue(
        resolvedStoredSecrets.awsAccessKeyId ?? existing?.awsAccessKeyId,
      ) !== undefined &&
      trimOptionalSecretValue(
        resolvedStoredSecrets.awsSecretAccessKey ??
          existing?.awsSecretAccessKey,
      ) !== undefined;
    const hasChangedAwsProfile =
      (payload.awsProfile?.trim() ?? "") !==
      (existing?.awsProfile?.trim() ?? "");
    const ignoreStaticAwsCredentials =
      payload.type === "dynamodb" &&
      (payload.awsCredentialMode !== undefined
        ? payload.awsCredentialMode !== "static"
        : useAwsProfile &&
          (payload.clearAwsCredentials === true ||
            !hasExistingAwsKeyPair ||
            hasChangedAwsProfile));
    const existingRedisUri =
      existing?.type === "redis"
        ? getExistingRedisUri(existing, resolvedStoredSecrets)
        : undefined;
    const resolvedConnectionUri =
      payload.type === "mongodb" && payload.mongoConnectionMode === "uri"
        ? restoreMongoUriCredentials(
            submittedConnectionUri,
            getMongoFormUri(
              existing ?? {},
              existing
                ? getExistingMongoUri(existing, resolvedStoredSecrets)
                : undefined,
            ),
            payload.keepMongoUriCredentials === true,
          )
        : payload.type === "redis"
          ? restoreCredentialBearingUri(
              submittedConnectionUri,
              existingRedisUri,
              payload.keepConnectionUriCredentials ??
                (Boolean(submittedConnectionUri) &&
                  sanitizeCredentialBearingUri(submittedConnectionUri) ===
                    sanitizeCredentialBearingUri(existingRedisUri)),
            )
          : restoreSubmittedUriValue(
              submittedConnectionUri,
              resolvedStoredSecrets.connectionUri,
              existing?.connectionUri,
            );
    const existingElasticEndpoint =
      existing?.type === "elasticsearch"
        ? getExistingElasticEndpoint(existing, resolvedStoredSecrets)
        : undefined;
    const usesLegacyAwsEndpoint =
      payload.type === "dynamodb" &&
      existing?.type === "dynamodb" &&
      trimOptionalUriValue(existing.endpoint) === undefined;
    const resolvedEndpoint =
      payload.type === "elasticsearch"
        ? restoreCredentialBearingUri(
            submittedEndpoint,
            existingElasticEndpoint,
            payload.keepConnectionUriCredentials ??
              (Boolean(submittedEndpoint) &&
                sanitizeCredentialBearingUri(submittedEndpoint) ===
                  sanitizeCredentialBearingUri(existingElasticEndpoint)),
          )
        : restoreSubmittedUriValue(
            submittedEndpoint,
            usesLegacyAwsEndpoint
              ? resolvedStoredSecrets.awsEndpoint
              : resolvedStoredSecrets.endpoint,
            usesLegacyAwsEndpoint ? existing?.awsEndpoint : existing?.endpoint,
          );
    const resolved = normalizeMongoConnectionConfig({
      ...rest,
      tls,
      useSecretStorage,
      connectionUri: resolvedConnectionUri,
      ...(payload.type === "mongodb" && payload.mongoConnectionMode === "uri"
        ? {
            mongoUriHasCredentials:
              extractCredentialBearingUriSecret(resolvedConnectionUri) !==
              undefined,
          }
        : {}),
      uri: restoreSubmittedUriValue(
        submittedUri,
        resolvedStoredSecrets.uri,
        existing?.uri,
      ),
      endpoint: resolvedEndpoint,
      awsEndpoint: restoreSubmittedUriValue(
        submittedAwsEndpoint,
        resolvedStoredSecrets.awsEndpoint,
        existing?.awsEndpoint,
      ),
      password,
      apiKey:
        trimOptionalSecretValue(payload.apiKey) ??
        (!payload.clearApiKey && !prefersElasticsearchBasicAuth
          ? resolvedStoredSecrets.apiKey
          : undefined) ??
        (!payload.clearApiKey && !prefersElasticsearchBasicAuth
          ? existing?.apiKey
          : undefined),
      awsAccessKeyId: ignoreStaticAwsCredentials
        ? undefined
        : (trimOptionalSecretValue(payload.awsAccessKeyId) ??
          (!payload.clearAwsCredentials && !hasSubmittedAwsKeyMaterial
            ? (resolvedStoredSecrets.awsAccessKeyId ?? existing?.awsAccessKeyId)
            : undefined)),
      awsSecretAccessKey: ignoreStaticAwsCredentials
        ? undefined
        : (trimOptionalSecretValue(payload.awsSecretAccessKey) ??
          (!payload.clearAwsCredentials && !hasSubmittedAwsKeyMaterial
            ? (resolvedStoredSecrets.awsSecretAccessKey ??
              existing?.awsSecretAccessKey)
            : undefined)),
      awsSessionToken: ignoreStaticAwsCredentials
        ? undefined
        : (trimOptionalSecretValue(payload.awsSessionToken) ??
          (!payload.clearAwsCredentials && !hasSubmittedAwsKeyMaterial
            ? (resolvedStoredSecrets.awsSessionToken ??
              existing?.awsSessionToken)
            : undefined)),
      awsSessionTokenRequired:
        !ignoreStaticAwsCredentials &&
        !hasSubmittedAwsKeyMaterial &&
        !payload.clearAwsCredentials &&
        existing?.awsSessionTokenRequired === true
          ? true
          : undefined,
      ssh,
    });
    if (
      resolved.type === "dynamodb" &&
      resolved.awsCredentialMode === undefined
    ) {
      // Older form messages can still make an explicit switch via the former
      // removal control or by entering a different profile name.
      resolved.awsCredentialMode =
        (ignoreStaticAwsCredentials && hasChangedAwsProfile
          ? "profile"
          : undefined) ??
        (payload.clearAwsCredentials
          ? hasAwsProfile
            ? "profile"
            : "default"
          : undefined) ??
        resolveAwsCredentialMode(resolved);
    }
    return resolved;
  }

  private async handleMessage(msg: unknown): Promise<void> {
    if (this.disposed) return;
    const parsed = parseConnectionFormPanelMessage(msg);
    if (!parsed) {
      const { type, requestId } = readRawMessageIdentity(msg);
      if (type === "testConnection") {
        this.panel.webview.postMessage({
          type: "testResult",
          payload: {
            requestId,
            success: false,
            error: "Connection settings are invalid.",
          },
        });
      }
      return;
    }

    switch (parsed.type) {
      case "saveConnection": {
        const payload = parsed.payload;
        if (!payload || this.saveInProgress) {
          return;
        }
        if (this.rejectsConnectionId(payload.id)) {
          this.panel.webview.postMessage({
            type: "saveResult",
            payload: { success: false, error: CONNECTION_ID_MISMATCH_MESSAGE },
          });
          return;
        }
        this.saveInProgress = true;
        try {
          const existing = this.getStoredConnection(payload.id);
          let storedSecrets: StoredConnectionSecrets = {};
          let revision: string | undefined;
          try {
            if (existing && usesStoredSecrets(existing)) {
              const raw = await readConnectionSecret(
                this.context.secrets,
                payload.id,
              );
              assertConnectionSecretBinding(existing, raw);
              storedSecrets = parseStoredConnectionSecrets(raw);
              revision = readConnectionCredentialRevision(raw);
            }
          } catch (error) {
            if (this.disposed) return;
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error: `SecretStorage unavailable: ${normalizeUnknownError(error).message}. Existing connection data was not changed.`,
              },
            });
            return;
          }
          if (this.disposed) return;
          if (
            existing &&
            this.savePrecondition.kind === "edit" &&
            this.savePrecondition.state &&
            !connectionEditStatesMatch(
              this.savePrecondition.state,
              connectionEditState(existing, storedSecrets, revision),
            )
          ) {
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error:
                  "This connection changed since the editor was opened. Reopen Edit Connection before saving.",
              },
            });
            return;
          }
          const raw = this.resolveSubmittedConfig(
            payload,
            existing,
            storedSecrets,
          );
          if (
            hasUnresolvedAmbiguousStoredUri(
              existing,
              raw,
              payload.clearAmbiguousStoredUri,
            )
          ) {
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: { success: false, error: AMBIGUOUS_STORED_URI_MESSAGE },
            });
            return;
          }
          const validation = this.validationService.validate(raw);
          if (!validation.valid) {
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error: validation.message ?? "Connection settings are invalid.",
                validation,
              },
            });
            return;
          }

          const inlinePassword = trimOptionalSecretValue(raw.password);
          const changesConnectionType = this.changesStoredConnectionType(raw);
          const usesSeparatePassword = !(
            (raw.type === "redis" && trimOptionalUriValue(raw.connectionUri)) ||
            (raw.type === "mongodb" &&
              (raw.mongoConnectionMode === "uri" ||
                raw.mongoAuthMethod === "none"))
          );
          const requiresStoredPasswordRecovery =
            usesSeparatePassword &&
            !changesConnectionType &&
            connectionTypeUsesPassword(raw) &&
            !raw.useSecretStorage &&
            payload.hasStoredSecret === true &&
            payload.clearPassword !== true;
          if (
            requiresStoredPasswordRecovery &&
            inlinePassword === undefined &&
            storedSecrets.password === undefined
          ) {
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error:
                  "Existing stored password could not be loaded. Existing credentials were preserved; re-enter password to continue.",
              },
            });
            return;
          }

          try {
            if (
              raw.useSecretStorage === false &&
              hasConnectionConfigSecrets(raw)
            ) {
              const consent = await vscode.window.showWarningMessage(
                "Save credentials in plaintext? Passwords, API/AWS keys, SSH secrets, TLS key passphrases and credentials in connection URLs will be saved in settings.json without encryption. They may be exposed by sharing or syncing settings.",
                { modal: true },
                "Save in Plaintext",
              );
              if (this.disposed) return;
              if (consent !== "Save in Plaintext") {
                this.panel.webview.postMessage({
                  type: "saveResult",
                  payload: {
                    success: false,
                    error:
                      "Saving plaintext credentials was cancelled. Connection data was not changed.",
                  },
                });
                return;
              }
            }
            const hasSubmittedAwsKeyMaterial =
              raw.type === "dynamodb" &&
              (trimOptionalSecretValue(payload.awsAccessKeyId) !== undefined ||
                trimOptionalSecretValue(payload.awsSecretAccessKey) !==
                  undefined);
            const clearStoredSecrets: ConnectionSecretClearOptions = {
              ...(payload.clearSshPassphrase ? { sshPassphrase: true } : {}),
              ...(payload.clearTlsKeyPassphrase
                ? { tlsKeyPassphrase: true }
                : {}),
              password:
                payload.clearPassword === true ||
                changesConnectionType ||
                (raw.type === "redis" &&
                  trimOptionalUriValue(payload.connectionUri) !== undefined),
              apiKey: payload.clearApiKey === true,
              credentialBearingUris:
                (payload.type === "redis" ||
                  payload.type === "elasticsearch") &&
                payload.keepConnectionUriCredentials === false,
              awsCredentials:
                payload.clearAwsCredentials === true ||
                (raw.type === "dynamodb" &&
                  (hasSubmittedAwsKeyMaterial ||
                    raw.awsCredentialMode === "profile" ||
                    raw.awsCredentialMode === "default" ||
                    (Boolean(raw.awsProfile?.trim()) &&
                      !(
                        trimOptionalSecretValue(raw.awsAccessKeyId) &&
                        trimOptionalSecretValue(raw.awsSecretAccessKey)
                      )))),
            };
            if (this.disposed) return;
            const saved =
              (await this.connectionManager.saveConnection(
                raw,
                clearStoredSecrets,
                this.savePrecondition,
              )) ?? raw;
            if (this.disposed) return;
            this.resolveFn?.(sanitizeConnectionConfigForResponse(saved));
          } catch (err: unknown) {
            if (this.disposed) return;
            const error = normalizeUnknownError(err);
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error: `Could not save connection: ${error.message}`,
              },
            });
            return;
          }

          this.resolveFn = undefined;
          this.panel.dispose();
        } finally {
          this.saveInProgress = false;
        }
        break;
      }
      case "cancelTestConnection": {
        this.testAbortController?.abort();
        this.testAbortController = null;
        break;
      }
      case "testConnection": {
        if (!parsed.payload) {
          return;
        }
        const { requestId, ...payload } = parsed.payload;
        if (this.rejectsConnectionId(payload.id)) {
          this.panel.webview.postMessage({
            type: "testResult",
            payload: {
              requestId,
              success: false,
              error: CONNECTION_ID_MISMATCH_MESSAGE,
            },
          });
          return;
        }
        this.testAbortController?.abort();
        const controller = new AbortController();
        this.testAbortController = controller;
        const { signal } = controller;
        try {
          const initial = this.getStoredConnection(payload.id);
          const serialized =
            initial && usesStoredSecrets(initial)
              ? await readConnectionSecret(this.context.secrets, payload.id)
              : undefined;
          if (signal.aborted) break;
          const current = this.getStoredConnection(payload.id);
          const storedSecrets = parseStoredConnectionSecrets(serialized);
          let raw: ConnectionConfig;
          if (this.savePrecondition.kind === "edit") {
            const expected = this.savePrecondition.state;
            if (!expected || !initial || !current) {
              throw new Error(
                "Could not verify the saved connection. Reopen Edit Connection before testing.",
              );
            }
            if (usesStoredSecrets(initial)) {
              assertConnectionSecretBinding(initial, serialized);
            }
            if (usesStoredSecrets(current)) {
              assertConnectionSecretBinding(current, serialized);
            }
            const revision = readConnectionCredentialRevision(serialized);
            if (
              !connectionEditStatesMatch(
                expected,
                connectionEditState(initial, storedSecrets, revision),
              ) ||
              !connectionEditStatesMatch(
                expected,
                connectionEditState(current, storedSecrets, revision),
              )
            ) {
              throw new Error(
                "This connection changed since the editor was opened. Reopen Edit Connection before testing.",
              );
            }
            raw = this.resolveSubmittedConfig(payload, current, storedSecrets);
          } else {
            raw = this.resolveSubmittedConfig(payload, undefined, {});
          }
          if (
            hasUnresolvedAmbiguousStoredUri(
              current,
              raw,
              payload.clearAmbiguousStoredUri,
            )
          ) {
            this.panel.webview.postMessage({
              type: "testResult",
              payload: {
                requestId,
                success: false,
                error: AMBIGUOUS_STORED_URI_MESSAGE,
              },
            });
            return;
          }
          const validation = this.validationService.validate(raw);
          if (!validation.valid) {
            this.panel.webview.postMessage({
              type: "testResult",
              payload: {
                requestId,
                success: false,
                error: validation.message ?? "Connection settings are invalid.",
                validation,
              },
            });
            return;
          }
          if (signal.aborted) break;
          const result = await this.connectionManager.testConnection(
            raw,
            signal,
          );
          if (signal.aborted) break;
          this.panel.webview.postMessage({
            type: "testResult",
            payload: { ...result, requestId },
          });
        } catch (error: unknown) {
          if (
            this.disposed ||
            signal.aborted ||
            this.testAbortController !== controller
          ) {
            return;
          }
          const normalized = logErrorWithContext(
            "ConnectionFormPanel test error",
            error,
          );
          this.panel.webview.postMessage({
            type: "testResult",
            payload: { requestId, success: false, error: normalized.message },
          });
        } finally {
          if (this.testAbortController === controller) {
            this.testAbortController = null;
          }
        }
        break;
      }
      case "cancel": {
        if (this.saveInProgress) return;
        this.resolveFn?.(undefined);
        this.resolveFn = undefined;
        this.panel.dispose();
        break;
      }
      case "browseFile": {
        const target = parsed.payload?.target ?? "filePath";
        const lastSqliteDirectory = this.context.globalState.get<string>(
          LAST_SQLITE_DIRECTORY_STATE_KEY,
        );
        const isSqliteFile = target === "filePath";
        const filters: { [name: string]: string[] } = isSqliteFile
          ? {
              "SQLite databases": ["db", "sqlite", "sqlite3", "db3"],
              "All files": ["*"],
            }
          : {
              "TLS files": ["pem", "crt", "cer", "key"],
              "All files": ["*"],
            };
        const titleByTarget: Record<ConnectionFormBrowseTarget, string> = {
          filePath: "Select SQLite database file",
          tlsCaFile: "Select CA certificate file",
          tlsCertFile: "Select client certificate file",
          tlsKeyFile: "Select client key file",
        };
        const uris = await vscode.window.showOpenDialog({
          canSelectFiles: true,
          canSelectFolders: false,
          canSelectMany: false,
          defaultUri:
            isSqliteFile &&
            lastSqliteDirectory &&
            path.isAbsolute(lastSqliteDirectory)
              ? vscode.Uri.file(lastSqliteDirectory)
              : undefined,
          filters,
          title: titleByTarget[target],
        });
        const selected = uris?.[0];
        if (isSqliteFile && selected) {
          const selectedDirectory = path.dirname(selected.fsPath);
          if (path.isAbsolute(selectedDirectory)) {
            await this.context.globalState.update(
              LAST_SQLITE_DIRECTORY_STATE_KEY,
              selectedDirectory,
            );
          }
        }
        this.panel.webview.postMessage({
          type: "browseFileResult",
          payload: {
            target,
            filePath: selected ? selected.fsPath : null,
          },
        });
        break;
      }
    }
  }

  private buildHtml(
    context: vscode.ExtensionContext,
    existing?: ConnectionConfig,
  ): string {
    return createWebviewShell({
      context,
      webview: this.panel.webview,
      title: "RapiDB - Connection",
      initialState: {
        view: "connection",
        existing: existing ?? null,
        panelRetentionMode: CONNECTION_FORM_RETENTION_MODE,
      },
      includeMediaRoot: true,
    });
  }
}
