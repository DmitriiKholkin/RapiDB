import * as vscode from "vscode";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";

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

    this.panel.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(context.extensionUri, "dist"),
        vscode.Uri.joinPath(context.extensionUri, "media"),
      ],
    };

    this.panel.webview.html = this.buildHtml(context, existing);
    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: any) {
        console.error(
          "[RapiDB] ConnectionFormPanel unhandled error:",
          err?.message ?? err,
        );
        this.panel.webview.postMessage({
          type: "testResult",
          payload: { success: false, error: err?.message ?? String(err) },
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
      existing,
    );

    return new Promise<ConnectionConfig | undefined>((resolve) => {
      instance.resolveFn = resolve;
    });
  }

  private async handleMessage(msg: {
    type: string;
    payload?: any;
  }): Promise<void> {
    switch (msg.type) {
      case "saveConnection": {
        const config: ConnectionConfig = msg.payload;
        await this.connectionManager.saveConnection(config);
        this.resolveFn?.(config);
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
    const webview = this.panel.webview;

    const webviewJs = webview.asWebviewUri(
      vscode.Uri.joinPath(context.extensionUri, "dist", "webview.js"),
    );
    const webviewCss = webview.asWebviewUri(
      vscode.Uri.joinPath(context.extensionUri, "dist", "webview.css"),
    );

    const nonce = crypto.randomUUID();
    const initialState = JSON.stringify({
      view: "connection",
      existing: existing ?? null,
    });

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none';
             script-src 'nonce-${nonce}' ${webview.cspSource};
             style-src ${webview.cspSource} 'nonce-${nonce}';
             font-src ${webview.cspSource} data:;
             img-src ${webview.cspSource} https: data:;" />
  <title>RapiDB — Connection</title>
  <link rel="stylesheet" href="${webviewCss}" />
  <style nonce="${nonce}">
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--vscode-editor-background); color: var(--vscode-foreground);
           font-family: var(--vscode-font-family, system-ui, sans-serif);
           font-size: var(--vscode-font-size, 13px); }
    #root { height: 100vh; overflow: auto; }
  </style>
</head>
<body>
  <div id="root"></div>
  <script nonce="${nonce}">
    window.__HAPPYDB_INITIAL_STATE__ = ${initialState};
  </script>
  <script nonce="${nonce}" src="${webviewJs}"></script>
</body>
</html>`;
  }
}
