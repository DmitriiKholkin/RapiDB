import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";

export class SchemaPanel {
  private static readonly viewType = "rapidb.schemaPanel";
  private static panels = new Map<string, SchemaPanel>();

  private readonly panel: vscode.WebviewPanel;
  private readonly cm: ConnectionManager;
  private readonly context: vscode.ExtensionContext;
  private readonly connectionId: string;
  private readonly database: string;
  private readonly schema: string;
  private readonly table: string;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    cm: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ) {
    this.panel = panel;
    this.context = context;
    this.cm = cm;
    this.connectionId = connectionId;
    this.database = database;
    this.schema = schema;
    this.table = table;

    this.panel.webview.options = {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, "dist")],
    };
    this.panel.webview.html = this.buildHtml(context);

    const key = SchemaPanel.key(connectionId, database, schema, table);
    this.panel.onDidDispose(() => SchemaPanel.panels.delete(key));
    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: any) {
        console.error(
          "[RapiDB] SchemaPanel unhandled error:",
          err?.message ?? err,
        );
        this.panel.webview.postMessage({
          type: "schemaError",
          payload: { error: err?.message ?? String(err) },
        });
      }
    });
  }

  private static key(c: string, d: string, s: string, t: string) {
    return `${c}::${d}::${s}::${t}`;
  }

  static disposeAll(): void {
    for (const panel of SchemaPanel.panels.values()) {
      try {
        panel.panel.dispose();
      } catch {}
    }
    SchemaPanel.panels.clear();
  }

  static createOrShow(
    context: vscode.ExtensionContext,
    cm: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): void {
    const key = SchemaPanel.key(connectionId, database, schema, table);
    const existing = SchemaPanel.panels.get(key);
    if (existing) {
      existing.panel.reveal(vscode.ViewColumn.One);
      return;
    }

    const buildTitle = () => {
      const connName = cm.getConnection(connectionId)?.name ?? connectionId;
      const schemaPrefix = schema ? `${schema}.` : "";
      return `${schemaPrefix}${table} (schema) [${connName}]`;
    };
    const panel = vscode.window.createWebviewPanel(
      SchemaPanel.viewType,
      buildTitle(),
      vscode.ViewColumn.One,
      { enableScripts: true, retainContextWhenHidden: true },
    );
    const instance = new SchemaPanel(
      panel,
      context,
      cm,
      connectionId,
      database,
      schema,
      table,
    );
    SchemaPanel.panels.set(key, instance);

    const confSub = vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration("rapidb.connections")) {
        panel.title = buildTitle();
      }
    });
    const disconnSub = cm.onDidDisconnect((id) => {
      if (id === connectionId) {
        panel.dispose();
      }
    });
    panel.onDidDispose(() => {
      confSub.dispose();
      disconnSub.dispose();
    });
  }

  private async handleMessage(msg: {
    type: string;
    payload?: any;
  }): Promise<void> {
    const send = (type: string, payload: unknown) =>
      this.panel.webview.postMessage({ type, payload });

    switch (msg.type) {
      case "ready": {
        const driver = this.cm.getDriver(this.connectionId);
        if (!driver) {
          send("schemaError", { error: "Not connected" });
          return;
        }
        try {
          const [columns, indexes, foreignKeys] = await Promise.all([
            driver.describeTable(this.database, this.schema, this.table),
            driver
              .getIndexes(this.database, this.schema, this.table)
              .catch(() => []),
            driver
              .getForeignKeys(this.database, this.schema, this.table)
              .catch(() => []),
          ]);
          send("schemaData", { columns, indexes, foreignKeys });
        } catch (err: any) {
          send("schemaError", { error: err?.message ?? String(err) });
        }
        break;
      }

      case "openRelatedSchema": {
        const { table, schema, database } = msg.payload ?? {};
        SchemaPanel.createOrShow(
          this.context,
          this.cm,
          this.connectionId,
          database ?? this.database,
          schema ?? this.schema,
          table,
        );
        break;
      }
    }
  }

  private buildHtml(context: vscode.ExtensionContext): string {
    const webview = this.panel.webview;

    const webviewJs = webview.asWebviewUri(
      vscode.Uri.joinPath(context.extensionUri, "dist", "webview.js"),
    );
    const webviewCss = webview.asWebviewUri(
      vscode.Uri.joinPath(context.extensionUri, "dist", "webview.css"),
    );

    function escapeHtml(str: string): string {
      return str
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    const nonce = crypto.randomUUID();

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none';
             script-src 'nonce-${nonce}' ${webview.cspSource};
             style-src ${webview.cspSource} 'unsafe-inline';
             font-src ${webview.cspSource} data:;
             img-src ${webview.cspSource} https: data:;" />
  <title>Schema — ${escapeHtml(this.table)}</title>
  <link rel="stylesheet" href="${webviewCss}" />
  <style nonce="${nonce}">
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; overflow: hidden; }
    body {
      background: var(--vscode-editor-background);
      color: var(--vscode-foreground);
      font-family: var(--vscode-font-family, system-ui, sans-serif);
      font-size: var(--vscode-font-size, 13px);
    }
    #root { height: 100vh; overflow: auto; }
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--vscode-scrollbarSlider-background); border-radius: 4px; }
  </style>
</head>
<body>
  <div id="root"></div>
  <script nonce="${nonce}">
    window.__HAPPYDB_INITIAL_STATE__ = {
      view:         'schema',
      connectionId: ${JSON.stringify(this.connectionId)},
      database:     ${JSON.stringify(this.database)},
      schema:       ${JSON.stringify(this.schema)},
      table:        ${JSON.stringify(this.table)},
    };
  </script>
  <script nonce="${nonce}" src="${webviewJs}"></script>
</body>
</html>`;
  }
}
