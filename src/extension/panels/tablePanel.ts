import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as vscode from "vscode";
import { coerceFilterExpressions } from "../../shared/tableTypes";
import type { WebviewMessageEnvelope } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import {
  applyChangesTransactional,
  type Filter,
  type RowUpdate,
  type SortConfig,
  TableDataService,
} from "../tableDataService";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { createWebviewShell } from "./webviewShell";

type PanelMessage = WebviewMessageEnvelope;

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

  private cachedColumns: import("../tableDataService").ColumnDef[] = [];

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

    this.panel.webview.html = this.buildHtml(context);

    const key = TablePanel.panelKey(connectionId, database, schema, table);
    this.panel.onDidDispose(() => {
      TablePanel.panels.delete(key);

      this.svc.clearForConnection(connectionId);
    });

    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: unknown) {
        const error = logErrorWithContext("TablePanel unhandled error", err);
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${error.message}`,
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

  private async handleMessage(msg: PanelMessage): Promise<void> {
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
          this.cachedColumns = cols;
          const pkCols = cols.filter((c) => c.isPrimaryKey).map((c) => c.name);
          send("tableInit", {
            columns: cols,
            primaryKeyColumns: pkCols,
            isView: this.isView,
          });
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          send("tableError", { error: error.message });
        }
        break;
      }

      case "fetchPage": {
        const raw = (msg.payload ?? {}) as {
          fetchId?: number;
          page?: number | string;
          pageSize?: number | string;
          filters?: unknown;
          sort?: unknown;
        };
        const fetchId = raw.fetchId;
        const page = Math.max(1, Math.floor(Number(raw.page) || 1));
        const pageSize = Math.min(
          10000,
          Math.max(1, Math.floor(Number(raw.pageSize) || 50)),
        );
        const filters = coerceFilterExpressions(raw.filters);
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
            fetchId,
            rows: result.rows,
            totalCount: result.totalCount,
          });
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          const errMsg = error.message;
          const isFilterError =
            /^\[RapiDB Filter\]/.test(errMsg) ||
            /invalid input syntax|invalid cidr|malformed array|not a valid (binary|hex|uuid)|syntax error in input|invalid value for type|invalid number|operator does not exist|conversion failed|arithmetic overflow|ORA-0(1841|1843|1858|1861|6502)|ORA-01722|incorrect (date|datetime|time)|Incorrect integer value|Truncated incorrect|data truncat/i.test(
              errMsg,
            );
          send("tableError", { fetchId, error: errMsg, isFilterError });
        }
        break;
      }

      case "applyChanges": {
        const { updates } = (msg.payload ?? {}) as {
          updates?: RowUpdate[];
        };
        try {
          const result = await applyChangesTransactional(
            this.cm,
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            (updates ?? []) as RowUpdate[],
            this.cachedColumns,
          );
          send("applyResult", result);
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          send("applyResult", {
            success: false,
            error: error.message,
          });
        }
        break;
      }

      case "insertRow": {
        const { values = {} } = (msg.payload ?? {}) as {
          values?: Record<string, unknown>;
        };
        try {
          await this.svc.insertRow(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            values,
          );
          send("insertResult", { success: true });
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          send("insertResult", {
            success: false,
            error: error.message,
          });
        }
        break;
      }

      case "deleteRows": {
        const { primaryKeysList = [] } = (msg.payload ?? {}) as {
          primaryKeysList?: Record<string, unknown>[];
        };
        try {
          await this.svc.deleteRows(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            primaryKeysList,
          );
          send("deleteResult", { success: true });
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          send("deleteResult", {
            success: false,
            error: error.message,
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
              cancellable: true,
            },
            async (_progress, token) => {
              const abortCtrl = new AbortController();
              const cancelSub = token.onCancellationRequested(() =>
                abortCtrl.abort(),
              );
              const writeStream = fs.createWriteStream(saveUri.fsPath, {
                encoding: "utf8",
              });
              let headerWritten = false;
              const { sort: csvSort = null, filters: csvFilters = [] } =
                (msg.payload ?? {}) as {
                  sort?: SortConfig | null;
                  filters?: unknown[];
                };
              try {
                for await (const chunk of this.svc.exportAll(
                  this.connectionId,
                  this.database,
                  this.schema,
                  this.table,
                  500,
                  csvSort as SortConfig | null,
                  coerceFilterExpressions(csvFilters),
                  abortCtrl.signal,
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
              } finally {
                cancelSub.dispose();
              }
            },
          );

          vscode.window.showInformationMessage(
            `[RapiDB] Exported ${this.table} → ${path.basename(saveUri.fsPath)}`,
          );
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          if (error.name !== "AbortError") {
            vscode.window.showErrorMessage(
              `[RapiDB] CSV export failed: ${error.message}`,
            );
          }
        }
        break;
      }

      case "exportJSON": {
        const { sort = null, filters: jsonFilters = [] } = (msg.payload ??
          {}) as {
          sort?: SortConfig | null;
          filters?: unknown[];
        };
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
              cancellable: true,
            },
            async (_progress, token) => {
              const abortCtrl = new AbortController();
              const cancelSub = token.onCancellationRequested(() =>
                abortCtrl.abort(),
              );
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
                  coerceFilterExpressions(jsonFilters),
                  abortCtrl.signal,
                )) {
                  for (const row of chunk.rows) {
                    const serialisable = Object.fromEntries(
                      Object.entries(row).map(([k, v]) => [
                        k,
                        v instanceof Date
                          ? Number.isNaN(v.getTime())
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
              } finally {
                cancelSub.dispose();
              }
            },
          );

          vscode.window.showInformationMessage(
            `[RapiDB] Exported ${this.table} → ${path.basename(saveUri.fsPath)}`,
          );
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          if (error.name !== "AbortError") {
            vscode.window.showErrorMessage(
              `[RapiDB] JSON export failed: ${error.message}`,
            );
          }
        }
        break;
      }

      case "confirmDelete": {
        const { count } = (msg.payload ?? {}) as { count?: number };
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
    return createWebviewShell({
      context,
      webview: this.panel.webview,
      title: `${this.isView ? "View" : "Table"} - ${this.table}`,
      initialState: {
        view: "table",
        connectionId: this.connectionId,
        database: this.database,
        schema: this.schema,
        table: this.table,
        isView: this.isView,
        defaultPageSize: this.cm.getDefaultPageSize(),
      },
      htmlStyles: "height: 100%; overflow: hidden;",
      bodyStyles: "height: 100%; overflow: hidden;",
      rootStyles: "height: 100vh;",
      extraStyles: `
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--vscode-scrollbarSlider-background); border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--vscode-scrollbarSlider-hoverBackground); }

        .pk-key-icon {
          display: inline-flex; align-items: center; justify-content: center;
          vertical-align: middle;
        }
      `,
    });
  }
}

function formatCellValue(value: unknown): string {
  if (value == null) {
    return "";
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
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
