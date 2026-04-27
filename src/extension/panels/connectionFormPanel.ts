import * as vscode from "vscode";
import {
  type ConnectionFormExistingState,
  type ConnectionFormSubmission,
  parseConnectionFormPanelMessage,
} from "../../shared/webviewContracts";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { createWebviewShell } from "./webviewShell";

export class ConnectionFormPanel {
  private static readonly viewType = "rapidb.connectionForm";

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
      { enableScripts: true, retainContextWhenHidden: true },
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
  ): Promise<string> {
    if (payload.password !== undefined && payload.password !== "") {
      return payload.password;
    }

    if (payload.useSecretStorage && payload.hasStoredSecret) {
      try {
        return (await this.context.secrets.get(payload.id)) ?? "";
      } catch {
        return "";
      }
    }

    const existing = this.connectionManager.getConnection(payload.id);
    if (payload.useSecretStorage && existing?.useSecretStorage) {
      try {
        return (await this.context.secrets.get(payload.id)) ?? "";
      } catch {
        return "";
      }
    }

    return existing?.password ?? payload.password ?? "";
  }

  private async resolveSubmittedConfig(
    payload: ConnectionFormSubmission,
  ): Promise<ConnectionConfig> {
    const password = await this.resolveSubmittedPassword(payload);
    const { hasStoredSecret: _hasStoredSecret, ...rest } = payload;
    return { ...rest, password };
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
        if (!payload.name.trim()) {
          this.panel.webview.postMessage({
            type: "saveResult",
            payload: { success: false, error: "Name is required." },
          });
          return;
        }
        const raw = await this.resolveSubmittedConfig(payload);

        if (raw.useSecretStorage) {
          const shouldReuseStored =
            payload.hasStoredSecret === true && (payload.password ?? "") === "";
          const password = raw.password ?? "";
          try {
            if (!shouldReuseStored) {
              await this.context.secrets.store(raw.id, password);
            }
          } catch (err: unknown) {
            const error = normalizeUnknownError(err);
            this.panel.webview.postMessage({
              type: "saveResult",
              payload: {
                success: false,
                error: `SecretStorage unavailable: ${error.message}. Password was not saved.`,
              },
            });
            return;
          }
          const { password: _pw, ...configWithoutPassword } = raw;
          const config = configWithoutPassword as ConnectionConfig;
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
      },
      includeMediaRoot: true,
    });
  }
}
