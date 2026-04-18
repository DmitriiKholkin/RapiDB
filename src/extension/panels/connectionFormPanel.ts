import * as vscode from "vscode";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { createWebviewShell } from "./webviewShell";

interface PanelMessage {
  type: string;
  payload?: unknown;
}

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
        this.panel.webview.postMessage({
          type: msg.type === "saveConnection" ? "saveResult" : "testResult",
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

    let existingForForm = existing;
    if (existing?.useSecretStorage && existing.id) {
      try {
        const stored = await context.secrets.get(existing.id);
        if (stored !== undefined) {
          existingForForm = { ...existing, password: stored };
        }
      } catch {}
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

  private async handleMessage(msg: PanelMessage): Promise<void> {
    switch (msg.type) {
      case "saveConnection": {
        const raw = msg.payload as ConnectionConfig;

        if (raw.useSecretStorage) {
          const password = raw.password ?? "";
          try {
            await this.context.secrets.store(raw.id, password);
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
        const result = await this.connectionManager.testConnection(msg.payload);
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
