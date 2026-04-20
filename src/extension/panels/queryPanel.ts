import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as vscode from "vscode";
import { parseQueryPanelMessage } from "../../shared/webviewContracts";
import { inferQueryColumnCategory, type QueryColumnMeta } from "../../shared/tableTypes";
import type { ConnectionManager } from "../connectionManager";
import { formatDatetimeForDisplay } from "../dbDrivers/BaseDBDriver";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { createWebviewShell } from "./webviewShell";

export class QueryPanel {
  private static readonly viewType = "rapidb.queryPanel";
  private static panels = new Map<string, QueryPanel>();
  private static _seq = 0;

  private readonly panel: vscode.WebviewPanel;
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
    this.connectionManager = connectionManager;
    this.originalConnectionId = connectionId;
    this.activeConnectionId = connectionId;

    this.formatOnOpen = formatOnOpen ?? false;
    this.isBookmarked = isBookmarked ?? false;
    this.panel.webview.html = this.buildHtml(context, initialSql);
    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: unknown) {
        const error = logErrorWithContext("QueryPanel unhandled error", err);
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${error.message}`,
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

  private postQueryError(error: string): void {
    this.panel.webview.postMessage({
      type: "queryResult",
      payload: {
        columns: [],
        columnMeta: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
        error,
      },
    });
  }

  private async confirmQueryExecution(sql: string): Promise<boolean> {
    if (!isLikelyUnboundedResultQuery(sql)) {
      return true;
    }

    const answer = await vscode.window.showWarningMessage(
      "[RapiDB] This query looks unbounded and the extension currently fetches the full result set before truncating it. Continue anyway?",
      { modal: true },
      "Run Anyway",
    );
    return answer === "Run Anyway";
  }

  private async handleMessage(msg: unknown): Promise<void> {
    const parsed = parseQueryPanelMessage(msg);
    if (!parsed) {
      return;
    }

    switch (parsed.type) {
      case "activeConnectionChanged": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        this.activeConnectionId = payload.connectionId;
        this.syncTitle();
        break;
      }

      case "executeQuery": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        const sql = payload.sql;
        if (!sql.trim()) {
          return;
        }

        if (!(await this.confirmQueryExecution(sql))) {
          this.postQueryError("Query execution cancelled.");
          return;
        }

        const connectionId = payload.connectionId || this.originalConnectionId;

        if (!this.connectionManager.isConnected(connectionId)) {
          try {
            await this.connectionManager.connectTo(connectionId);
          } catch (err: unknown) {
            const error = normalizeUnknownError(err);
            this.postQueryError(`Cannot connect: ${error.message}`);
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
          const columnMeta = resolveQueryColumnMeta(result.columns, result.columnMeta, result.rows);

          const rows = rawRows.map((row) => {
            const normalised: Record<string, unknown> = {};
            for (const [k, v] of Object.entries(row)) {
              if (typeof v === "bigint") {
                normalised[k] = v.toString();
              } else if (Buffer.isBuffer(v)) {
                normalised[k] = v.length === 0 ? "" : "\\x" + v.toString("hex");
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
            payload: {
              ...result,
              columnMeta,
              rows,
              truncated,
              truncatedAt: limit,
            },
          });
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          this.postQueryError(error.message);
        }
        break;
      }

      case "getConnections": {
        this.pushConnections();
        break;
      }

      case "getSchema": {
        const connectionId =
          parsed.payload?.connectionId || this.activeConnectionId;

        if (!this.connectionManager.isConnected(connectionId)) {
          this.panel.webview.postMessage({
            type: "schema",
            payload: { connectionId, tables: [] },
          });
          break;
        }

        const tables =
          await this.connectionManager.getSchemaAsync(connectionId);
        this.panel.webview.postMessage({
          type: "schema",
          payload: { connectionId, tables },
        });
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
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        const { sql, connectionId: bmConnId } = payload;
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
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          this.panel.webview.postMessage({
            type: "bookmarkSaved",
            payload: { ok: false, error: error.message },
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
    const conn = this.connectionManager.getConnection(
      this.originalConnectionId,
    );
    const connType = conn?.type ?? "";

    return createWebviewShell({
      context,
      webview: this.panel.webview,
      title: "RapiDB Query",
      initialState: {
        view: "query",
        connectionId: this.originalConnectionId,
        connectionType: connType,
        initialSql: initialSql ?? "",
        formatOnOpen: this.formatOnOpen ?? false,
        isBookmarked: this.isBookmarked ?? false,
      },
      includeMediaRoot: true,
      extraCspDirectives: ["worker-src blob:"],
      htmlStyles: "height: 100%; overflow: hidden;",
      bodyStyles: "height: 100%; overflow: hidden;",
      rootStyles: "height: 100vh;",
      extraStyles: `
        .monaco-editor .scrollbar .slider { background: var(--vscode-scrollbarSlider-background) !important; border-radius: 4px; }
        .monaco-editor .scrollbar .slider:hover { background: var(--vscode-scrollbarSlider-hoverBackground) !important; }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--vscode-scrollbarSlider-background); border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--vscode-scrollbarSlider-hoverBackground); }
      `,
    });
  }
}

function resolveQueryColumnMeta(
  columns: string[],
  rawMeta: QueryColumnMeta[] | undefined,
  rows: Record<string, unknown>[],
): QueryColumnMeta[] {
  const samples = rows.slice(0, 50);
  return columns.map((_, index) => ({
    category:
      rawMeta?.[index]?.category ??
      inferQueryColumnCategory(samples.map((row) => row[`__col_${index}`])),
  }));
}

function normaliseSqlForGuardrail(sql: string): string {
  return sql
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--.*$/gm, " ")
    .replace(/'([^']|'')*'/g, "''")
    .replace(/"([^"]|"")*"/g, '""')
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

export function isLikelyUnboundedResultQuery(sql: string): boolean {
  const normalized = normaliseSqlForGuardrail(sql);
  if (!/^(select|with)\b/.test(normalized)) {
    return false;
  }

  if (/\bcount\s*\(/.test(normalized)) {
    return false;
  }

  if (
    /\blimit\s+\d+\b/.test(normalized) ||
    /\btop\s*(?:\(\s*\d+\s*\)|\d+)\b/.test(normalized) ||
    /\bfetch\s+(?:first|next)\s+\d+\s+rows?\s+only\b/.test(normalized) ||
    /\boffset\s+\d+\s+rows?\s+fetch\s+(?:first|next)\s+\d+\s+rows?\s+only\b/.test(
      normalized,
    ) ||
    /\brownum\s*(?:<=|<)\s*\d+\b/.test(normalized)
  ) {
    return false;
  }

  return true;
}
