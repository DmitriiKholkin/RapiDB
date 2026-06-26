import * as path from "node:path";
import * as vscode from "vscode";
import type { ConnectionSecretUpdateTransaction } from "../../shared/safetyContracts";
import {
  type ConnectionFormBrowseTarget,
  type ConnectionFormExistingState,
  type ConnectionFormSubmission,
  parseConnectionFormPanelMessage,
} from "../../shared/webviewContracts";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import {
  extractCredentialBearingUriSecret,
  resolvePersistedCredentialBearingUriSecret,
  sanitizeCredentialBearingUri,
  sanitizePersistedConnectionConfig,
  trimOptionalSecretValue,
} from "../connectionSecrets";
import { ConnectionValidationService } from "../services/connectionValidationService";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { attachPanelMessageHandler } from "./panelLifecycle";
import { createPanelWebviewOptions } from "./panelRetentionPolicy";
import { createWebviewShell } from "./webviewShell";

type StoredConnectionSecrets = {
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
};

const CONNECTION_FORM_RETENTION_MODE = "retain" as const;
const LAST_SQLITE_DIRECTORY_STATE_KEY = "rapidb.lastSqliteDirectory";

function restoreSubmittedUriValue(
  submittedValue: string | undefined,
  storedValue: string | undefined,
  existingValue: string | undefined,
): string | undefined {
  const normalizedSubmitted = trimOptionalSecretValue(submittedValue);
  if (!normalizedSubmitted) {
    return normalizedSubmitted;
  }

  for (const candidate of [storedValue, existingValue]) {
    const normalizedCandidate = trimOptionalSecretValue(candidate);
    if (!normalizedCandidate) {
      continue;
    }

    if (
      extractCredentialBearingUriSecret(normalizedCandidate) !== undefined &&
      sanitizeCredentialBearingUri(normalizedCandidate) === normalizedSubmitted
    ) {
      return normalizedCandidate;
    }
  }

  return normalizedSubmitted;
}

function parseStoredConnectionSecrets(
  value: string | undefined,
): StoredConnectionSecrets {
  if (!value) {
    return {};
  }

  try {
    const parsed = JSON.parse(value) as Record<string, unknown>;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return {
        password:
          typeof parsed.password === "string" ? parsed.password : undefined,
        apiKey: typeof parsed.apiKey === "string" ? parsed.apiKey : undefined,
        awsAccessKeyId:
          typeof parsed.awsAccessKeyId === "string"
            ? parsed.awsAccessKeyId
            : undefined,
        awsSecretAccessKey:
          typeof parsed.awsSecretAccessKey === "string"
            ? parsed.awsSecretAccessKey
            : undefined,
        awsSessionToken:
          typeof parsed.awsSessionToken === "string"
            ? parsed.awsSessionToken
            : undefined,
        connectionUri:
          typeof parsed.connectionUri === "string"
            ? parsed.connectionUri
            : undefined,
        uri: typeof parsed.uri === "string" ? parsed.uri : undefined,
        endpoint:
          typeof parsed.endpoint === "string" ? parsed.endpoint : undefined,
        awsEndpoint:
          typeof parsed.awsEndpoint === "string"
            ? parsed.awsEndpoint
            : undefined,
        sshPassword:
          typeof parsed.sshPassword === "string"
            ? parsed.sshPassword
            : undefined,
        sshPrivateKey:
          typeof parsed.sshPrivateKey === "string"
            ? parsed.sshPrivateKey
            : undefined,
        sshPassphrase:
          typeof parsed.sshPassphrase === "string"
            ? parsed.sshPassphrase
            : undefined,
        tlsKeyPassphrase:
          typeof parsed.tlsKeyPassphrase === "string"
            ? parsed.tlsKeyPassphrase
            : undefined,
      };
    }
  } catch {}

  return { password: value };
}

function serializeStoredConnectionSecrets(
  secrets: StoredConnectionSecrets,
): string | undefined {
  const filtered = Object.fromEntries(
    Object.entries(secrets).filter(([, value]) => typeof value === "string"),
  );

  return Object.keys(filtered).length > 0
    ? JSON.stringify(filtered)
    : undefined;
}

function shouldUseSecretStorage(payload: ConnectionFormSubmission): boolean {
  return (
    payload.ssh !== undefined ||
    payload.type === "dynamodb" ||
    payload.type === "elasticsearch" ||
    payload.useSecretStorage === true ||
    trimOptionalSecretValue(payload.password) !== undefined ||
    trimOptionalSecretValue(payload.apiKey) !== undefined ||
    trimOptionalSecretValue(payload.awsAccessKeyId) !== undefined ||
    trimOptionalSecretValue(payload.awsSecretAccessKey) !== undefined ||
    trimOptionalSecretValue(payload.awsSessionToken) !== undefined ||
    trimOptionalSecretValue(payload.tls?.keyPassphrase) !== undefined ||
    extractCredentialBearingUriSecret(payload.connectionUri) !== undefined ||
    extractCredentialBearingUriSecret(payload.uri) !== undefined ||
    extractCredentialBearingUriSecret(payload.endpoint) !== undefined ||
    extractCredentialBearingUriSecret(payload.awsEndpoint) !== undefined
  );
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
    connectionUri: sanitizeCredentialBearingUri(connectionUri),
    uri: sanitizeCredentialBearingUri(uri),
    endpoint: sanitizeCredentialBearingUri(endpoint),
    awsEndpoint: sanitizeCredentialBearingUri(awsEndpoint),
    hasStoredSecret: storedSecrets.password !== undefined || undefined,
    hasStoredApiKey: storedSecrets.apiKey !== undefined || undefined,
    hasStoredSshPassword: storedSecrets.sshPassword !== undefined || undefined,
    hasStoredSshPrivateKey:
      storedSecrets.sshPrivateKey !== undefined || undefined,
    hasStoredSshPassphrase:
      storedSecrets.sshPassphrase !== undefined || undefined,
    hasStoredTlsKeyPassphrase:
      storedSecrets.tlsKeyPassphrase !== undefined || undefined,
  };
}

export class ConnectionFormPanel {
  private static readonly viewType = "rapidb.connectionForm";
  private readonly validationService = new ConnectionValidationService();

  private readonly panel: vscode.WebviewPanel;
  private readonly context: vscode.ExtensionContext;
  private readonly connectionManager: ConnectionManager;
  private resolveFn?: (result: ConnectionConfig | undefined) => void;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    existing?: ConnectionConfig,
  ) {
    this.panel = panel;
    this.context = context;
    this.connectionManager = connectionManager;

    this.panel.webview.html = this.buildHtml(context, existing);
    attachPanelMessageHandler(
      this.panel,
      (message) => this.handleMessage(message),
      (error, message) => {
        const normalized = logErrorWithContext(
          "ConnectionFormPanel unhandled error",
          error,
        );
        const isSaveConnectionMessage =
          typeof message === "object" &&
          message !== null &&
          "type" in message &&
          (message as { type?: unknown }).type === "saveConnection";
        this.panel.webview.postMessage({
          type: isSaveConnectionMessage ? "saveResult" : "testResult",
          payload: { success: false, error: normalized.message },
        });
      },
    );
    this.panel.onDidDispose(() => {
      this.resolveFn?.(undefined);
    });
  }

  static async show(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    existing?: ConnectionConfig,
  ): Promise<ConnectionConfig | undefined> {
    const title = existing ? `Edit — ${existing.name}` : "New Connection";

    let existingForForm: ConnectionFormExistingState | undefined;
    if (existing) {
      let storedSecrets: StoredConnectionSecrets = {};
      if (existing.id) {
        try {
          storedSecrets = parseStoredConnectionSecrets(
            await context.secrets.get(existing.id),
          );
        } catch {}
      }

      existingForForm = sanitizeExistingForForm(existing, storedSecrets);
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
    );

    return new Promise<ConnectionConfig | undefined>((resolve) => {
      instance.resolveFn = resolve;
    });
  }

  private async resolveSubmittedPassword(
    payload: ConnectionFormSubmission,
    storedSecrets: StoredConnectionSecrets,
    useSecretStorage: boolean,
  ): Promise<string> {
    if (payload.password !== undefined && payload.password !== "") {
      return payload.password;
    }

    if (useSecretStorage && storedSecrets.password !== undefined) {
      return storedSecrets.password;
    }

    if (payload.hasStoredSecret && storedSecrets.password !== undefined) {
      return storedSecrets.password;
    }

    const existing = this.connectionManager.getConnection(payload.id);
    return existing?.password ?? payload.password ?? "";
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

  private async loadStoredSecrets(
    id: string,
  ): Promise<StoredConnectionSecrets> {
    try {
      return parseStoredConnectionSecrets(await this.context.secrets.get(id));
    } catch {
      return {};
    }
  }

  private async readStoredSecretsSnapshot(id: string): Promise<{
    serialized: string | undefined;
    parsed: StoredConnectionSecrets;
    readError?: string;
  }> {
    try {
      const serialized = await this.context.secrets.get(id);
      return {
        serialized,
        parsed: parseStoredConnectionSecrets(serialized),
      };
    } catch (err: unknown) {
      return {
        serialized: undefined,
        parsed: {},
        readError: normalizeUnknownError(err).message,
      };
    }
  }

  private async restoreSecretsSnapshot(
    snapshot: string | undefined,
    id: string,
  ): Promise<void> {
    if (snapshot === undefined) {
      await this.context.secrets.delete(id);
      return;
    }

    await this.context.secrets.store(id, snapshot);
  }

  private async commitSecretUpdateTransaction(
    transaction: ConnectionSecretUpdateTransaction,
    saveConfig: () => Promise<void>,
  ): Promise<void> {
    const nextSecretSnapshot = transaction.nextSecretSnapshot;
    const shouldStoreNextSecret =
      nextSecretSnapshot !== undefined &&
      nextSecretSnapshot !== transaction.previousSecretSnapshot;
    const shouldDeleteSecret =
      transaction.nextSecretSnapshot === undefined &&
      transaction.previousSecretSnapshot !== undefined;
    const secretMutationNeeded = shouldStoreNextSecret || shouldDeleteSecret;

    if (shouldStoreNextSecret) {
      await this.context.secrets.store(
        transaction.connectionId,
        nextSecretSnapshot,
      );
    } else if (shouldDeleteSecret) {
      await this.context.secrets.delete(transaction.connectionId);
    }

    try {
      await saveConfig();
    } catch (err: unknown) {
      if (secretMutationNeeded) {
        try {
          await this.restoreSecretsSnapshot(
            transaction.previousSecretSnapshot,
            transaction.connectionId,
          );
        } catch {}
      }

      throw err;
    }
  }

  private async resolveSubmittedConfig(
    payload: ConnectionFormSubmission,
  ): Promise<ConnectionConfig> {
    const storedSecrets = await this.loadStoredSecrets(payload.id);
    const existing = this.connectionManager.getConnection(payload.id);
    const useSecretStorage =
      shouldUseSecretStorage(payload) ||
      storedSecrets.connectionUri !== undefined ||
      storedSecrets.uri !== undefined ||
      storedSecrets.endpoint !== undefined ||
      storedSecrets.awsEndpoint !== undefined ||
      extractCredentialBearingUriSecret(existing?.connectionUri) !==
        undefined ||
      extractCredentialBearingUriSecret(existing?.uri) !== undefined ||
      extractCredentialBearingUriSecret(existing?.endpoint) !== undefined ||
      extractCredentialBearingUriSecret(existing?.awsEndpoint) !== undefined;
    const password = await this.resolveSubmittedPassword(
      payload,
      storedSecrets,
      useSecretStorage,
    );
    const {
      hasStoredSecret: _hasStoredSecret,
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
                  useSecretStorage ? storedSecrets.sshPassword : undefined,
                  existing?.ssh?.password,
                )
              : undefined,
          privateKey:
            submittedSsh.authMethod === "privateKey"
              ? this.resolveSubmittedSecret(
                  submittedSsh.privateKey,
                  useSecretStorage ? storedSecrets.sshPrivateKey : undefined,
                  existing?.ssh?.privateKey,
                )
              : undefined,
          passphrase:
            submittedSsh.authMethod === "privateKey"
              ? this.resolveSubmittedSecret(
                  submittedSsh.passphrase,
                  useSecretStorage ? storedSecrets.sshPassphrase : undefined,
                  existing?.ssh?.passphrase,
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
                    useSecretStorage
                      ? storedSecrets.tlsKeyPassphrase
                      : undefined,
                    existing?.tls?.keyPassphrase,
                  )
                : undefined,
          }
        : rest.tls;
    const prefersElasticsearchBasicAuth =
      payload.type === "elasticsearch" &&
      trimOptionalSecretValue(payload.username) !== undefined &&
      trimOptionalSecretValue(payload.password) !== undefined;
    return {
      ...rest,
      tls,
      useSecretStorage,
      connectionUri: restoreSubmittedUriValue(
        submittedConnectionUri,
        storedSecrets.connectionUri,
        existing?.connectionUri,
      ),
      uri: restoreSubmittedUriValue(
        submittedUri,
        storedSecrets.uri,
        existing?.uri,
      ),
      endpoint: restoreSubmittedUriValue(
        submittedEndpoint,
        storedSecrets.endpoint,
        existing?.endpoint,
      ),
      awsEndpoint: restoreSubmittedUriValue(
        submittedAwsEndpoint,
        storedSecrets.awsEndpoint,
        existing?.awsEndpoint,
      ),
      password,
      apiKey:
        trimOptionalSecretValue(payload.apiKey) ??
        (useSecretStorage && !prefersElasticsearchBasicAuth
          ? storedSecrets.apiKey
          : undefined) ??
        existing?.apiKey,
      awsAccessKeyId:
        trimOptionalSecretValue(payload.awsAccessKeyId) ??
        (useSecretStorage ? storedSecrets.awsAccessKeyId : undefined) ??
        existing?.awsAccessKeyId,
      awsSecretAccessKey:
        trimOptionalSecretValue(payload.awsSecretAccessKey) ??
        (useSecretStorage ? storedSecrets.awsSecretAccessKey : undefined) ??
        existing?.awsSecretAccessKey,
      awsSessionToken:
        trimOptionalSecretValue(payload.awsSessionToken) ??
        (useSecretStorage ? storedSecrets.awsSessionToken : undefined) ??
        existing?.awsSessionToken,
      ssh,
    };
  }

  private async handleMessage(msg: unknown): Promise<void> {
    const parsed = parseConnectionFormPanelMessage(msg);
    if (!parsed) {
      return;
    }

    switch (parsed.type) {
      case "saveConnection": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        const raw = await this.resolveSubmittedConfig(payload);
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

        const previousSecretSnapshot = await this.readStoredSecretsSnapshot(
          raw.id,
        );
        const inlinePassword = trimOptionalSecretValue(raw.password);
        const requiresStoredPasswordRecovery =
          !raw.useSecretStorage && payload.hasStoredSecret === true;
        if (
          requiresStoredPasswordRecovery &&
          inlinePassword === undefined &&
          previousSecretSnapshot.parsed.password === undefined
        ) {
          this.panel.webview.postMessage({
            type: "saveResult",
            payload: {
              success: false,
              error: previousSecretSnapshot.readError
                ? `SecretStorage unavailable: ${previousSecretSnapshot.readError}. Existing credentials were preserved; re-enter password to continue.`
                : "Existing stored password could not be loaded. Existing credentials were preserved; re-enter password to continue.",
            },
          });
          return;
        }

        if (raw.useSecretStorage) {
          const normalizedPreviousSecretSnapshot =
            serializeStoredConnectionSecrets(previousSecretSnapshot.parsed);
          const nextSecrets = serializeStoredConnectionSecrets({
            password: inlinePassword,
            apiKey:
              raw.type === "elasticsearch"
                ? trimOptionalSecretValue(raw.apiKey)
                : undefined,
            awsAccessKeyId:
              raw.type === "dynamodb"
                ? trimOptionalSecretValue(raw.awsAccessKeyId)
                : undefined,
            awsSecretAccessKey:
              raw.type === "dynamodb"
                ? trimOptionalSecretValue(raw.awsSecretAccessKey)
                : undefined,
            awsSessionToken:
              raw.type === "dynamodb"
                ? trimOptionalSecretValue(raw.awsSessionToken)
                : undefined,
            connectionUri: resolvePersistedCredentialBearingUriSecret(
              raw.connectionUri,
              previousSecretSnapshot.parsed.connectionUri,
            ),
            uri: resolvePersistedCredentialBearingUriSecret(
              raw.uri,
              previousSecretSnapshot.parsed.uri,
            ),
            endpoint: resolvePersistedCredentialBearingUriSecret(
              raw.endpoint,
              previousSecretSnapshot.parsed.endpoint,
            ),
            awsEndpoint: resolvePersistedCredentialBearingUriSecret(
              raw.awsEndpoint,
              previousSecretSnapshot.parsed.awsEndpoint,
            ),
            sshPassword:
              raw.ssh?.authMethod === "password"
                ? trimOptionalSecretValue(raw.ssh.password)
                : undefined,
            sshPrivateKey:
              raw.ssh?.authMethod === "privateKey"
                ? trimOptionalSecretValue(raw.ssh.privateKey)
                : undefined,
            sshPassphrase:
              raw.ssh?.authMethod === "privateKey"
                ? trimOptionalSecretValue(raw.ssh.passphrase)
                : undefined,
            tlsKeyPassphrase:
              raw.tls?.mode === "mutualTls"
                ? trimOptionalSecretValue(raw.tls.keyPassphrase)
                : undefined,
          });
          const secretMutationNeeded =
            nextSecrets !== normalizedPreviousSecretSnapshot;
          if (
            secretMutationNeeded &&
            previousSecretSnapshot.readError &&
            normalizedPreviousSecretSnapshot === undefined
          ) {
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error: `SecretStorage unavailable: ${previousSecretSnapshot.readError}. Existing credentials were preserved; retry once Secret Storage is available.`,
              },
            });
            return;
          }

          const transaction: ConnectionSecretUpdateTransaction = {
            connectionId: raw.id,
            useSecretStorage: true,
            previousSecretSnapshot: normalizedPreviousSecretSnapshot,
            nextSecretSnapshot: nextSecrets,
          };
          try {
            await this.commitSecretUpdateTransaction(transaction, async () => {
              await this.connectionManager.saveConnection(raw);
            });
          } catch (err: unknown) {
            const error = normalizeUnknownError(err);
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error: `SecretStorage unavailable: ${error.message}. Credentials were not saved.`,
              },
            });
            return;
          }
          this.resolveFn?.(sanitizePersistedConnectionConfig(raw));
        } else {
          await this.connectionManager.saveConnection(raw);
          if (previousSecretSnapshot.serialized !== undefined) {
            try {
              await this.context.secrets.delete(raw.id);
            } catch {}
          }
          this.resolveFn?.(raw);
        }

        this.resolveFn = undefined;
        this.panel.dispose();
        break;
      }
      case "testConnection": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        const raw = await this.resolveSubmittedConfig(payload);
        const validation = this.validationService.validate(raw);
        if (!validation.valid) {
          this.panel.webview.postMessage({
            type: "testResult",
            payload: {
              success: false,
              error: validation.message ?? "Connection settings are invalid.",
              validation,
            },
          });
          return;
        }
        const result = await this.connectionManager.testConnection(raw);
        this.panel.webview.postMessage({ type: "testResult", payload: result });
        break;
      }
      case "cancel": {
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
