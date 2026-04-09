import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";
import {
  applyChangesTransactional,
  type Filter,
  type RowUpdate,
  type SortConfig,
  TableDataService,
} from "../tableDataService";

export class TablePanel {
  private static readonly viewType = "rapidb.tablePanel";

  private static panels = new Map<string, TablePanel>();

  private readonly panel: vscode.WebviewPanel;
  private readonly svc: TableDataService;
  private readonly cm: ConnectionManager;
  private readonly connectionId: string;
  private readonly database: string;
  private readonly schema: string;
  private readonly table: string;
  private readonly isView: boolean;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    isView = false,
  ) {
    this.panel = panel;
    this.svc = new TableDataService(connectionManager);
    this.cm = connectionManager;
    this.connectionId = connectionId;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.isView = isView;

    this.panel.webview.options = {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, "dist")],
    };

    this.panel.webview.html = this.buildHtml(context);

    const key = TablePanel.panelKey(connectionId, database, schema, table);
    this.panel.onDidDispose(() => {
      TablePanel.panels.delete(key);

      this.svc.clearForConnection(connectionId);
    });

    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: any) {
        console.error(
          "[RapiDB] TablePanel unhandled error:",
          err?.message ?? err,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${err?.message ?? String(err)}`,
        );
      }
    });
  }

  private static panelKey(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): string {
    return `${connectionId}::${database}::${schema}::${table}`;
  }

  static disposeAll(): void {
    for (const panel of TablePanel.panels.values()) {
      try {
        panel.panel.dispose();
      } catch {}
    }
    TablePanel.panels.clear();
  }

  static createOrShow(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    isView = false,
  ): void {
    const key = TablePanel.panelKey(connectionId, database, schema, table);
    const existing = TablePanel.panels.get(key);
    if (existing) {
      existing.panel.reveal(vscode.ViewColumn.One);
      return;
    }

    const buildTitle = () => {
      const connName =
        connectionManager.getConnection(connectionId)?.name ?? connectionId;
      const objType = isView ? "view" : "table";
      const schemaPrefix = schema ? `${schema}.` : "";
      return `${schemaPrefix}${table} (${objType}) [${connName}]`;
    };
    const panel = vscode.window.createWebviewPanel(
      TablePanel.viewType,
      buildTitle(),
      vscode.ViewColumn.One,
      { enableScripts: true, retainContextWhenHidden: true },
    );

    const instance = new TablePanel(
      panel,
      context,
      connectionManager,
      connectionId,
      database,
      schema,
      table,
      isView,
    );
    TablePanel.panels.set(key, instance);

    const confSub = vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration("rapidb.connections")) {
        panel.title = buildTitle();
      }
    });
    const disconnSub = connectionManager.onDidDisconnect((id) => {
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
        try {
          const cols = await this.svc.getColumns(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
          );
          const pkCols = cols.filter((c) => c.isPrimaryKey).map((c) => c.name);
          send("tableInit", {
            columns: cols,
            primaryKeyColumns: pkCols,
            isView: this.isView,
          });
        } catch (err: any) {
          send("tableError", { error: err?.message ?? String(err) });
        }
        break;
      }

      case "fetchPage": {
        const raw = msg.payload ?? {};
        const page = Math.max(1, Math.floor(Number(raw.page) || 1));
        const pageSize = Math.min(
          10000,
          Math.max(1, Math.floor(Number(raw.pageSize) || 50)),
        );
        const filters = Array.isArray(raw.filters) ? raw.filters : [];
        const sort = raw.sort ?? null;
        try {
          const result = await this.svc.getPage(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            page,
            pageSize,
            filters as Filter[],
            sort as SortConfig | null,
          );
          send("tableData", {
            rows: result.rows,
            totalCount: result.totalCount,
          });
        } catch (err: any) {
          send("tableError", { error: err?.message ?? String(err) });
        }
        break;
      }

      case "applyChanges": {
        const { updates } = msg.payload ?? {};
        try {
          const result = await applyChangesTransactional(
            this.cm,
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            (updates ?? []) as RowUpdate[],
          );
          send("applyResult", result);
        } catch (err: any) {
          send("applyResult", {
            success: false,
            error: err?.message ?? String(err),
          });
        }
        break;
      }

      case "insertRow": {
        const { values } = msg.payload ?? {};
        try {
          await this.svc.insertRow(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            values,
          );
          send("insertResult", { success: true });
        } catch (err: any) {
          send("insertResult", {
            success: false,
            error: err?.message ?? String(err),
          });
        }
        break;
      }

      case "deleteRows": {
        const { primaryKeysList } = msg.payload ?? {};
        try {
          await this.svc.deleteRows(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            primaryKeysList,
          );
          send("deleteResult", { success: true });
        } catch (err: any) {
          send("deleteResult", {
            success: false,
            error: err?.message ?? String(err),
          });
        }
        break;
      }

      case "exportCSV": {
        const saveUri = await vscode.window.showSaveDialog({
          defaultUri: vscode.Uri.file(
            path.join(os.homedir(), "Downloads", `${this.table}.csv`),
          ),
          filters: { "CSV files": ["csv"], "All files": ["*"] },
        });
        if (!saveUri) {
          break;
        }

        try {
          await vscode.window.withProgress(
            {
              location: vscode.ProgressLocation.Notification,
              title: `RapiDB: Exporting ${this.table}…`,
              cancellable: false,
            },
            async () => {
              const writeStream = fs.createWriteStream(saveUri.fsPath, {
                encoding: "utf8",
              });
              let headerWritten = false;
              const { sort: csvSort = null, filters: csvFilters = [] } =
                msg.payload ?? {};
              try {
                for await (const chunk of this.svc.exportAll(
                  this.connectionId,
                  this.database,
                  this.schema,
                  this.table,
                  500,
                  csvSort as SortConfig | null,
                  (csvFilters as { column: string; value: string }[]).map(
                    (f) => ({
                      column: f.column,
                      value: f.value,
                    }),
                  ),
                )) {
                  if (!headerWritten) {
                    writeStream.write(
                      chunk.columns.map((c) => csvCell(c.name)).join(",") +
                        "\n",
                    );
                    headerWritten = true;
                  }
                  for (const row of chunk.rows) {
                    writeStream.write(
                      chunk.columns.map((c) => csvCell(row[c.name])).join(",") +
                        "\n",
                    );
                  }
                }
                await new Promise<void>((res, rej) => {
                  writeStream.end((err?: Error | null) =>
                    err ? rej(err) : res(),
                  );
                });
              } catch (err) {
                writeStream.destroy();
                throw err;
              }
            },
          );

          vscode.window.showInformationMessage(
            `[RapiDB] Exported ${this.table} → ${path.basename(saveUri.fsPath)}`,
          );
        } catch (err: any) {
          vscode.window.showErrorMessage(
            `[RapiDB] CSV export failed: ${err?.message ?? String(err)}`,
          );
        }
        break;
      }

      case "exportJSON": {
        const { sort = null, filters: jsonFilters = [] } = msg.payload ?? {};
        const saveUri = await vscode.window.showSaveDialog({
          defaultUri: vscode.Uri.file(
            path.join(os.homedir(), "Downloads", `${this.table}.json`),
          ),
          filters: { "JSON files": ["json"], "All files": ["*"] },
        });
        if (!saveUri) {
          break;
        }

        try {
          await vscode.window.withProgress(
            {
              location: vscode.ProgressLocation.Notification,
              title: `RapiDB: Exporting ${this.table} as JSON…`,
              cancellable: false,
            },
            async () => {
              const writeStream = fs.createWriteStream(saveUri.fsPath, {
                encoding: "utf8",
              });
              writeStream.write("[\n");
              let first = true;
              try {
                for await (const chunk of this.svc.exportAll(
                  this.connectionId,
                  this.database,
                  this.schema,
                  this.table,
                  500,
                  sort as SortConfig | null,
                  (jsonFilters as { column: string; value: string }[]).map(
                    (f) => ({
                      column: f.column,
                      value: f.value,
                    }),
                  ),
                )) {
                  for (const row of chunk.rows) {
                    const serialisable = Object.fromEntries(
                      Object.entries(row).map(([k, v]) => [
                        k,
                        v instanceof Date
                          ? isNaN(v.getTime())
                            ? null
                            : formatCellValue(v)
                          : (v ?? null),
                      ]),
                    );
                    writeStream.write(
                      (first ? "" : ",\n") + JSON.stringify(serialisable),
                    );
                    first = false;
                  }
                }
                writeStream.write("\n]\n");
                await new Promise<void>((res, rej) => {
                  writeStream.end((err?: Error | null) =>
                    err ? rej(err) : res(),
                  );
                });
              } catch (err) {
                writeStream.destroy();
                throw err;
              }
            },
          );

          vscode.window.showInformationMessage(
            `[RapiDB] Exported ${this.table} → ${path.basename(saveUri.fsPath)}`,
          );
        } catch (err: any) {
          vscode.window.showErrorMessage(
            `[RapiDB] JSON export failed: ${err?.message ?? String(err)}`,
          );
        }
        break;
      }

      case "confirmDelete": {
        const { count } = msg.payload ?? {};
        const answer = await vscode.window.showWarningMessage(
          `Delete ${count} row${count !== 1 ? "s" : ""} from "${this.table}"? This cannot be undone.`,
          { modal: true },
          "Delete",
        );
        send("deleteConfirmed", { confirmed: answer === "Delete" });
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
             style-src ${webview.cspSource} 'nonce-${nonce}';
             font-src ${webview.cspSource} data:;
             img-src ${webview.cspSource} https: data:;" />
  <title>${this.isView ? "View" : "Table"} — ${escapeHtml(this.table)}</title>
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
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--vscode-scrollbarSlider-background); border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--vscode-scrollbarSlider-hoverBackground); }
    
    .pk-key-icon {
      display: inline-flex; align-items: center; justify-content: center;
      vertical-align: middle;
    }
  </style>
</head>
<body>
  <div id="root"></div>
  <script nonce="${nonce}">
    window.__HAPPYDB_INITIAL_STATE__ = {
      view:            'table',
      connectionId:    ${JSON.stringify(this.connectionId)},
      database:        ${JSON.stringify(this.database)},
      schema:          ${JSON.stringify(this.schema)},
      table:           ${JSON.stringify(this.table)},
      isView:          ${JSON.stringify(this.isView)},
      defaultPageSize: ${JSON.stringify(this.cm.getDefaultPageSize())},
    };
  </script>
  <script nonce="${nonce}" src="${webviewJs}"></script>
</body>
</html>`;
  }
}

function formatCellValue(value: unknown): string {
  if (value == null) {
    return "";
  }

  if (value instanceof Date) {
    if (isNaN(value.getTime())) {
      return "";
    }

    const pad = (n: number) => String(n).padStart(2, "0");
    return (
      `${value.getUTCFullYear()}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())} ` +
      `${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}`
    );
  }
  return String(value);
}

function csvCell(value: unknown): string {
  const s = formatCellValue(value);
  if (s === "") {
    return "";
  }
  return s.includes(",") ||
    s.includes('"') ||
    s.includes("\n") ||
    s.includes("\r")
    ? `"${s.replace(/"/g, '""')}"`
    : s;
}
