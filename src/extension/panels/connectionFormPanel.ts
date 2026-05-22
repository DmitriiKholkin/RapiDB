import * as vscode from "vscode";
import {
  type ConnectionFormExistingState,
  type ConnectionFormSubmission,
  parseConnectionFormPanelMessage,
} from "../../shared/webviewContracts";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import { ConnectionValidationService } from "../services/connectionValidationService";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { createPanelWebviewOptions } from "./panelRetentionPolicy";
import { createWebviewShell } from "./webviewShell";

type StoredConnectionSecrets = {
  password?: string;
  apiKey?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  awsSessionToken?: string;
};

const CONNECTION_FORM_RETENTION_MODE = "rehydrate" as const;

function trimOptionalSecret(value: string | undefined): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
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
    payload.type === "dynamodb" ||
    payload.type === "elasticsearch" ||
    payload.useSecretStorage === true
  );
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
    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: unknown) {
        const error = logErrorWithContext(
          "ConnectionFormPanel unhandled error",
          err,
        );
        const isSaveConnectionMessage =
          typeof msg === "object" &&
          msg !== null &&
          "type" in msg &&
          (msg as { type?: unknown }).type === "saveConnection";
        this.panel.webview.postMessage({
          type: isSaveConnectionMessage ? "saveResult" : "testResult",
          payload: { success: false, error: error.message },
        });
      }
    });
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
      let hasStoredSecret = false;
      if (existing.useSecretStorage && existing.id) {
        try {
          hasStoredSecret =
            (await context.secrets.get(existing.id)) !== undefined;
        } catch {}
      }

      const { password: _password, ...rest } = existing;
      existingForForm = {
        ...rest,
        hasStoredSecret: hasStoredSecret || undefined,
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
    );

    return new Promise<ConnectionConfig | undefined>((resolve) => {
      instance.resolveFn = resolve;
    });
  }

  private async resolveSubmittedPassword(
    payload: ConnectionFormSubmission,
    storedSecrets: StoredConnectionSecrets,
  ): Promise<string> {
    if (payload.password !== undefined && payload.password !== "") {
      return payload.password;
    }

    if (
      shouldUseSecretStorage(payload) &&
      storedSecrets.password !== undefined
    ) {
      return storedSecrets.password;
    }

    const existing = this.connectionManager.getConnection(payload.id);
    return existing?.password ?? payload.password ?? "";
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

  private async resolveSubmittedConfig(
    payload: ConnectionFormSubmission,
  ): Promise<ConnectionConfig> {
    const storedSecrets = await this.loadStoredSecrets(payload.id);
    const password = await this.resolveSubmittedPassword(
      payload,
      storedSecrets,
    );
    const { hasStoredSecret: _hasStoredSecret, ...rest } = payload;
    const existing = this.connectionManager.getConnection(payload.id);
    const useSecretStorage = shouldUseSecretStorage(payload);
    return {
      ...rest,
      useSecretStorage,
      password,
      apiKey:
        trimOptionalSecret(payload.apiKey) ??
        (useSecretStorage ? storedSecrets.apiKey : undefined) ??
        existing?.apiKey,
      awsAccessKeyId:
        trimOptionalSecret(payload.awsAccessKeyId) ??
        (useSecretStorage ? storedSecrets.awsAccessKeyId : undefined) ??
        existing?.awsAccessKeyId,
      awsSecretAccessKey:
        trimOptionalSecret(payload.awsSecretAccessKey) ??
        (useSecretStorage ? storedSecrets.awsSecretAccessKey : undefined) ??
        existing?.awsSecretAccessKey,
      awsSessionToken:
        trimOptionalSecret(payload.awsSessionToken) ??
        (useSecretStorage ? storedSecrets.awsSessionToken : undefined) ??
        existing?.awsSessionToken,
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

        if (raw.useSecretStorage) {
          const nextSecrets = serializeStoredConnectionSecrets({
            password: trimOptionalSecret(raw.password),
            apiKey:
              raw.type === "elasticsearch"
                ? trimOptionalSecret(raw.apiKey)
                : undefined,
            awsAccessKeyId:
              raw.type === "dynamodb"
                ? trimOptionalSecret(raw.awsAccessKeyId)
                : undefined,
            awsSecretAccessKey:
              raw.type === "dynamodb"
                ? trimOptionalSecret(raw.awsSecretAccessKey)
                : undefined,
            awsSessionToken:
              raw.type === "dynamodb"
                ? trimOptionalSecret(raw.awsSessionToken)
                : undefined,
          });
          const currentSecrets = serializeStoredConnectionSecrets(
            await this.loadStoredSecrets(raw.id),
          );
          try {
            if (nextSecrets && nextSecrets !== currentSecrets) {
              await this.context.secrets.store(raw.id, nextSecrets);
            } else if (!nextSecrets) {
              await this.context.secrets.delete(raw.id);
            }
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
          const {
            password: _pw,
            apiKey: _apiKey,
            awsAccessKeyId: _awsAccessKeyId,
            awsSecretAccessKey: _awsSecretAccessKey,
            awsSessionToken: _awsSessionToken,
            ...configWithoutSecrets
          } = raw;
          const config = configWithoutSecrets as ConnectionConfig;
          await this.connectionManager.saveConnection(config);
          this.resolveFn?.(config);
        } else {
          try {
            await this.context.secrets.delete(raw.id);
          } catch {}
          await this.connectionManager.saveConnection(raw);
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
