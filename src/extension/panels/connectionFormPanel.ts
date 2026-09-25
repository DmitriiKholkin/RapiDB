import * as path from "node:path";
import * as vscode from "vscode";
import {
  type ConnectionFormBrowseTarget,
  type ConnectionFormExistingState,
  type ConnectionFormSubmission,
  parseConnectionFormPanelMessage,
} from "../../shared/webviewContracts";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import {
  extractCredentialBearingUriSecret,
  sanitizeCredentialBearingUri,
  sanitizePersistedConnectionConfig,
  trimOptionalSecretValue,
  trimOptionalUriValue,
} from "../connectionSecrets";
import {
  parseStoredConnectionSecrets,
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
    const normalizedCandidate = trimOptionalUriValue(candidate);
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

function shouldUseSecretStorage(payload: ConnectionFormSubmission): boolean {
  if (payload.useSecretStorage === false) {
    return false;
  }
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
  private testAbortController: AbortController | null = null;

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
      this.testAbortController?.abort();
      this.testAbortController = null;
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

  private async readStoredSecretsSnapshot(id: string): Promise<{
    parsed: StoredConnectionSecrets;
    readError?: string;
  }> {
    try {
      const serialized = await this.context.secrets.get(id);
      return {
        parsed: parseStoredConnectionSecrets(serialized),
      };
    } catch (err: unknown) {
      return {
        parsed: {},
        readError: normalizeUnknownError(err).message,
      };
    }
  }

  private async resolveSubmittedConfig(
    payload: ConnectionFormSubmission,
    storedSecrets?: StoredConnectionSecrets,
  ): Promise<ConnectionConfig> {
    const resolvedStoredSecrets =
      storedSecrets ??
      parseStoredConnectionSecrets(await this.context.secrets.get(payload.id));
    const existing = this.connectionManager.getConnection(payload.id);
    const useSecretStorage =
      shouldUseSecretStorage(payload) ||
      resolvedStoredSecrets.connectionUri !== undefined ||
      resolvedStoredSecrets.uri !== undefined ||
      resolvedStoredSecrets.endpoint !== undefined ||
      resolvedStoredSecrets.awsEndpoint !== undefined ||
      extractCredentialBearingUriSecret(existing?.connectionUri) !==
        undefined ||
      extractCredentialBearingUriSecret(existing?.uri) !== undefined ||
      extractCredentialBearingUriSecret(existing?.endpoint) !== undefined ||
      extractCredentialBearingUriSecret(existing?.awsEndpoint) !== undefined;
    const password = await this.resolveSubmittedPassword(
      payload,
      resolvedStoredSecrets,
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
                  resolvedStoredSecrets.sshPassphrase,
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
                      ? resolvedStoredSecrets.tlsKeyPassphrase
                      : undefined,
                    existing?.tls?.keyPassphrase,
                  )
                : undefined,
          }
        : rest.tls;
    const prefersElasticsearchBasicAuth =
      payload.type === "elasticsearch" &&
      Boolean(payload.username?.trim()) &&
      trimOptionalSecretValue(payload.password) !== undefined;
    return {
      ...rest,
      tls,
      useSecretStorage,
      connectionUri: restoreSubmittedUriValue(
        submittedConnectionUri,
        resolvedStoredSecrets.connectionUri,
        existing?.connectionUri,
      ),
      uri: restoreSubmittedUriValue(
        submittedUri,
        resolvedStoredSecrets.uri,
        existing?.uri,
      ),
      endpoint: restoreSubmittedUriValue(
        submittedEndpoint,
        resolvedStoredSecrets.endpoint,
        existing?.endpoint,
      ),
      awsEndpoint: restoreSubmittedUriValue(
        submittedAwsEndpoint,
        resolvedStoredSecrets.awsEndpoint,
        existing?.awsEndpoint,
      ),
      password,
      apiKey:
        trimOptionalSecretValue(payload.apiKey) ??
        (useSecretStorage && !prefersElasticsearchBasicAuth
          ? resolvedStoredSecrets.apiKey
          : undefined) ??
        existing?.apiKey,
      awsAccessKeyId:
        trimOptionalSecretValue(payload.awsAccessKeyId) ??
        (useSecretStorage ? resolvedStoredSecrets.awsAccessKeyId : undefined) ??
        existing?.awsAccessKeyId,
      awsSecretAccessKey:
        trimOptionalSecretValue(payload.awsSecretAccessKey) ??
        (useSecretStorage
          ? resolvedStoredSecrets.awsSecretAccessKey
          : undefined) ??
        existing?.awsSecretAccessKey,
      awsSessionToken:
        trimOptionalSecretValue(payload.awsSessionToken) ??
        (useSecretStorage
          ? resolvedStoredSecrets.awsSessionToken
          : undefined) ??
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
        const previousSecretSnapshot = await this.readStoredSecretsSnapshot(
          payload.id,
        );
        const existing = this.connectionManager.getConnection(payload.id);
        if (previousSecretSnapshot.readError && existing?.useSecretStorage) {
          this.panel.webview.postMessage({
            type: "saveResult",
            payload: {
              success: false,
              error: `SecretStorage unavailable: ${previousSecretSnapshot.readError}. Existing connection data was not changed.`,
            },
          });
          return;
        }
        const raw = await this.resolveSubmittedConfig(
          payload,
          previousSecretSnapshot.parsed,
        );
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

        try {
          const saved =
            (await this.connectionManager.saveConnection(raw)) ?? raw;
          this.resolveFn?.(
            saved.useSecretStorage
              ? sanitizePersistedConnectionConfig(saved)
              : saved,
          );
        } catch (err: unknown) {
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
        break;
      }
      case "cancelTestConnection": {
        this.testAbortController?.abort();
        this.testAbortController = null;
        break;
      }
      case "testConnection": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        this.testAbortController?.abort();
        const controller = new AbortController();
        this.testAbortController = controller;
        const { signal } = controller;
        try {
          const raw = await this.resolveSubmittedConfig(payload);
          if (signal.aborted) break;
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
          if (signal.aborted) break;
          const result = await this.connectionManager.testConnection(
            raw,
            signal,
          );
          if (signal.aborted) break;
          this.panel.webview.postMessage({
            type: "testResult",
            payload: result,
          });
        } finally {
          if (this.testAbortController === controller) {
            this.testAbortController = null;
          }
        }
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
