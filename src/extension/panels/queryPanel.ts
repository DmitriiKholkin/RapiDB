import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";
import { formatDatetimeForDisplay } from "../tableDataService";

export class QueryPanel {
  private static readonly viewType = "rapidb.queryPanel";
  private static panels = new Map<string, QueryPanel>();
  private static _seq = 0;

  private readonly panel: vscode.WebviewPanel;
  private readonly context: vscode.ExtensionContext;
  private readonly connectionManager: ConnectionManager;
  readonly originalConnectionId: string;
  private formatOnOpen = false;
  private isBookmarked = false;

  private lastQueryResult: {
    columns: string[];
    rows: Record<string, unknown>[];
  } | null = null;

  private activeConnectionId: string;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    initialSql?: string,
    formatOnOpen?: boolean,
    isBookmarked?: boolean,
  ) {
    this.panel = panel;
    this.context = context;
    this.connectionManager = connectionManager;
    this.originalConnectionId = connectionId;
    this.activeConnectionId = connectionId;

    this.panel.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(context.extensionUri, "dist"),
        vscode.Uri.joinPath(context.extensionUri, "media"),
      ],
    };

    this.formatOnOpen = formatOnOpen ?? false;
    this.isBookmarked = isBookmarked ?? false;
    this.panel.webview.html = this.buildHtml(context, initialSql);
    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: any) {
        console.error(
          "[RapiDB] QueryPanel unhandled error:",
          err?.message ?? err,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${err?.message ?? String(err)}`,
        );
      }
    });

    const cfgWatcher = vscode.workspace.onDidChangeConfiguration((e) => {
      if (!e.affectsConfiguration("rapidb.connections")) {
        return;
      }
      this.pushConnections();
      this.syncTitle();
    });

    const schemaWatcher = connectionManager.onDidSchemaLoad((cid) => {
      if (
        cid === this.activeConnectionId ||
        cid === this.originalConnectionId
      ) {
        this._pushSchemaAsync(cid);
      }
    });
    this.panel.onDidDispose(() => {
      cfgWatcher.dispose();
      schemaWatcher.dispose();
    });
  }

  static disposeAll(): void {
    for (const panel of QueryPanel.panels.values()) {
      try {
        panel.panel.dispose();
      } catch {}
    }
    QueryPanel.panels.clear();
  }

  static createOrShow(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    initialSql?: string,
    forceNew = false,
    formatOnOpen = false,
    isBookmarked = false,
  ): QueryPanel {
    const conn = connectionManager.getConnection(connectionId);
    const title = `SQL [${conn?.name ?? connectionId}]`;

    if (!initialSql && !forceNew) {
      for (const p of QueryPanel.panels.values()) {
        if (p.originalConnectionId === connectionId) {
          p.panel.reveal(vscode.ViewColumn.One);
          return p;
        }
      }
    }

    const pid = `qp_${++QueryPanel._seq}`;
    const panel = vscode.window.createWebviewPanel(
      QueryPanel.viewType,
      title,
      vscode.ViewColumn.One,
      { enableScripts: true, retainContextWhenHidden: true },
    );

    const instance = new QueryPanel(
      panel,
      context,
      connectionManager,
      connectionId,
      initialSql,
      formatOnOpen,
      isBookmarked,
    );
    QueryPanel.panels.set(pid, instance);
    panel.onDidDispose(() => QueryPanel.panels.delete(pid));
    return instance;
  }

  private syncTitle(): void {
    const conn = this.connectionManager.getConnection(this.activeConnectionId);
    this.panel.title = `Query — ${conn?.name ?? this.activeConnectionId}`;
  }

  private _pushSchemaAsync(connectionId: string): void {
    const tables = this.connectionManager.getSchema(connectionId);
    this.panel.webview.postMessage({
      type: "schema",
      payload: { connectionId, tables },
    });
  }

  private pushConnections(): void {
    const conns = this.connectionManager.getConnections().map((c) => ({
      id: c.id,
      name: c.name,
      type: c.type,
    }));
    this.panel.webview.postMessage({ type: "connections", payload: conns });
  }

  private async handleMessage(msg: {
    type: string;
    payload?: any;
  }): Promise<void> {
    switch (msg.type) {
      case "activeConnectionChanged": {
        const newId: string = msg.payload?.connectionId;
        if (!newId) {
          break;
        }
        this.activeConnectionId = newId;
        this.syncTitle();
        break;
      }

      case "executeQuery": {
        const sql: string = msg.payload?.sql ?? "";
        if (!sql.trim()) {
          return;
        }

        const connectionId: string =
          msg.payload?.connectionId || this.originalConnectionId;

        if (!this.connectionManager.isConnected(connectionId)) {
          try {
            await this.connectionManager.connectTo(connectionId);
          } catch (err: any) {
            this.panel.webview.postMessage({
              type: "queryResult",
              payload: {
                columns: [],
                rows: [],
                rowCount: 0,
                executionTimeMs: 0,
                error: `Cannot connect: ${err?.message ?? String(err)}`,
              },
            });
            return;
          }
        }

        await this.connectionManager.addToHistory(connectionId, sql);

        const driver = this.connectionManager.getDriver(connectionId);
        if (!driver) {
          break;
        }

        try {
          const result = await driver.query(sql);
          const limit = this.connectionManager.getQueryRowLimit();
          const truncated = result.rows.length > limit;
          const rawRows = truncated ? result.rows.slice(0, limit) : result.rows;

          const rows = rawRows.map((row) => {
            const normalised: Record<string, unknown> = {};
            for (const [k, v] of Object.entries(row)) {
              if (Buffer.isBuffer(v)) {
                normalised[k] =
                  v.length === 0
                    ? 0
                    : v.length <= 6
                      ? v.readUIntBE(0, v.length)
                      : v.toString("hex");
              } else if (
                v !== null &&
                typeof v === "object" &&
                !(v instanceof Date) &&
                !Buffer.isBuffer(v)
              ) {
                normalised[k] = JSON.stringify(v);
              } else {
                const fmt = formatDatetimeForDisplay(v);
                normalised[k] = fmt !== null ? fmt : v;
              }
            }
            return normalised;
          });

          this.lastQueryResult = { columns: result.columns, rows };
          this.panel.webview.postMessage({
            type: "queryResult",
            payload: { ...result, rows, truncated, truncatedAt: limit },
          });
        } catch (err: any) {
          this.panel.webview.postMessage({
            type: "queryResult",
            payload: {
              columns: [],
              rows: [],
              rowCount: 0,
              executionTimeMs: 0,
              error: err?.message ?? String(err),
            },
          });
        }
        break;
      }

      case "getConnections": {
        this.pushConnections();
        break;
      }

      case "getSchema": {
        const connectionId: string =
          msg.payload?.connectionId || this.activeConnectionId;

        if (!this.connectionManager.isConnected(connectionId)) {
          this.panel.webview.postMessage({
            type: "schema",
            payload: { connectionId, tables: [] },
          });
          break;
        }

        this._pushSchemaAsync(connectionId);
        break;
      }

      case "refreshSchema": {
        const connectionId: string =
          msg.payload?.connectionId || this.activeConnectionId;

        await this.connectionManager.reloadSchema(connectionId);

        for (const p of QueryPanel.panels.values()) {
          if (
            p.activeConnectionId === connectionId ||
            p.originalConnectionId === connectionId
          ) {
            p._pushSchemaAsync(connectionId);
          }
        }
        break;
      }

      case "exportResultsCSV": {
        const cached = this.lastQueryResult;
        if (!cached || cached.columns.length === 0) {
          vscode.window.showWarningMessage(
            "[RapiDB] No query results to export.",
          );
          break;
        }
        const saveUri = await vscode.window.showSaveDialog({
          defaultUri: vscode.Uri.file(
            path.join(os.homedir(), "Downloads", "query_results.csv"),
          ),
          filters: { "CSV files": ["csv"], "All files": ["*"] },
        });
        if (!saveUri) {
          break;
        }
        const { columns, rows } = cached;

        const writeStream = fs.createWriteStream(saveUri.fsPath, {
          encoding: "utf8",
        });
        try {
          const csvQuote = (v: unknown): string => {
            if (v == null) {
              return "";
            }
            const s = String(v);
            return s.includes(",") ||
              s.includes('"') ||
              s.includes("\n") ||
              s.includes("\r")
              ? `"${s.replace(/"/g, '""')}"`
              : s;
          };

          writeStream.write(columns.map(csvQuote).join(",") + "\n");

          for (const row of rows) {
            writeStream.write(
              columns.map((_, i) => csvQuote(row[`__col_${i}`])).join(",") +
                "\n",
            );
          }
          await new Promise<void>((res, rej) => {
            writeStream.end((err?: Error | null) => (err ? rej(err) : res()));
          });
        } catch (err) {
          writeStream.destroy();
          throw err;
        }
        vscode.window.showInformationMessage(
          `[RapiDB] Exported → ${path.basename(saveUri.fsPath)}`,
        );
        break;
      }

      case "exportResultsJSON": {
        const cached = this.lastQueryResult;
        if (!cached || cached.columns.length === 0) {
          vscode.window.showWarningMessage(
            "[RapiDB] No query results to export.",
          );
          break;
        }
        const saveUri = await vscode.window.showSaveDialog({
          defaultUri: vscode.Uri.file(
            path.join(os.homedir(), "Downloads", "query_results.json"),
          ),
          filters: { "JSON files": ["json"], "All files": ["*"] },
        });
        if (!saveUri) {
          break;
        }

        const writeStream = fs.createWriteStream(saveUri.fsPath, {
          encoding: "utf8",
        });
        try {
          writeStream.write("[\n");
          const { columns, rows } = cached;
          for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const seen = new Map<string, number>();
            const obj: Record<string, unknown> = {};
            for (let c = 0; c < columns.length; c++) {
              const displayName = columns[c];
              const count = seen.get(displayName) ?? 0;
              seen.set(displayName, count + 1);
              const key =
                count === 0 ? displayName : `${displayName}_${count + 1}`;
              obj[key] = row[`__col_${c}`];
            }
            writeStream.write((i === 0 ? "" : ",\n") + JSON.stringify(obj));
          }
          writeStream.write("\n]\n");
          await new Promise<void>((res, rej) => {
            writeStream.end((err?: Error | null) => (err ? rej(err) : res()));
          });
        } catch (err) {
          writeStream.destroy();
          throw err;
        }
        vscode.window.showInformationMessage(
          `[RapiDB] Exported → ${path.basename(saveUri.fsPath)}`,
        );
        break;
      }

      case "readClipboard": {
        try {
          const text = await vscode.env.clipboard.readText();
          this.panel.webview.postMessage({
            type: "clipboardText",
            payload: text,
          });
        } catch {
          this.panel.webview.postMessage({
            type: "clipboardText",
            payload: "",
          });
        }
        break;
      }

      case "addBookmark": {
        const { sql, connectionId: bmConnId } = (msg.payload ?? {}) as {
          sql: string;
          connectionId: string;
        };
        if (!sql?.trim()) {
          break;
        }
        const connId =
          bmConnId || this.activeConnectionId || this.originalConnectionId;
        try {
          await this.connectionManager.addBookmark(connId, sql);
          this.panel.webview.postMessage({
            type: "bookmarkSaved",
            payload: { ok: true },
          });
        } catch (err: any) {
          this.panel.webview.postMessage({
            type: "bookmarkSaved",
            payload: { ok: false, error: err?.message ?? String(err) },
          });
        }
        break;
      }
    }
  }

  private buildHtml(
    context: vscode.ExtensionContext,
    initialSql?: string,
  ): string {
    const webview = this.panel.webview;

    const webviewJs = webview.asWebviewUri(
      vscode.Uri.joinPath(context.extensionUri, "dist", "webview.js"),
    );
    const webviewCss = webview.asWebviewUri(
      vscode.Uri.joinPath(context.extensionUri, "dist", "webview.css"),
    );

    const nonce = crypto.randomUUID();

    const conn = this.connectionManager.getConnection(
      this.originalConnectionId,
    );
    const connType = conn?.type ?? "";

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none';
             worker-src blob:;
             script-src 'nonce-${nonce}' ${webview.cspSource};
             style-src ${webview.cspSource} 'unsafe-inline';
             font-src ${webview.cspSource} data:;
             img-src ${webview.cspSource} https: data:;" /> 
  <title>RapiDB Query</title>
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
    #root { height: 100vh; }
    .monaco-editor .scrollbar .slider { background: var(--vscode-scrollbarSlider-background) !important; border-radius: 4px; }
    .monaco-editor .scrollbar .slider:hover { background: var(--vscode-scrollbarSlider-hoverBackground) !important; }
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--vscode-scrollbarSlider-background); border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--vscode-scrollbarSlider-hoverBackground); }
  </style>
</head>
<body>
  <div id="root"></div>
  <script nonce="${nonce}">
    window.__HAPPYDB_INITIAL_STATE__ = {
      view:           'query',
      connectionId:   ${JSON.stringify(this.originalConnectionId)},
      connectionType: ${JSON.stringify(connType)},
      initialSql:     ${JSON.stringify(initialSql ?? "")},
      formatOnOpen:   ${JSON.stringify(this.formatOnOpen ?? false)},
      isBookmarked:   ${JSON.stringify(this.isBookmarked ?? false)},
    };
  </script>
  <script nonce="${nonce}" src="${webviewJs}"></script>
</body>
</html>`;
  }
}
