import * as vscode from "vscode";
import { parseQueryPanelMessage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { normalizeUnknownError } from "../utils/errorHandling";
import {
  exportQueryResultsAsCsv,
  exportQueryResultsAsJson,
} from "../utils/exportService";
import { formatQueryResult } from "../utils/queryResultFormatting";

export interface QueryPanelCachedResult {
  columns: string[];
  rows: Record<string, unknown>[];
}

interface QueryPanelView {
  getActiveConnectionId(): string;
  getInitialConnectionId(): string;
  getLastQueryResult(): QueryPanelCachedResult | null;
  postMessage(message: unknown): void;
  setActiveConnectionId(connectionId: string): void;
  setLastQueryResult(result: QueryPanelCachedResult | null): void;
  syncTitle(): void;
}

export class QueryPanelController {
  constructor(
    private readonly connectionManager: ConnectionManager,
    private readonly view: QueryPanelView,
  ) {}

  async handleMessage(message: unknown): Promise<void> {
    const parsed = parseQueryPanelMessage(message);
    if (!parsed) {
      return;
    }

    switch (parsed.type) {
      case "activeConnectionChanged":
        if (parsed.payload) {
          this.handleActiveConnectionChanged(parsed.payload.connectionId);
        }
        break;
      case "executeQuery":
        if (parsed.payload) {
          await this.handleExecuteQuery(
            parsed.payload.sql,
            parsed.payload.connectionId,
          );
        }
        break;
      case "getConnections":
        this.pushConnections();
        break;
      case "getSchema":
        await this.pushSchema(parsed.payload?.connectionId);
        break;
      case "exportResultsCSV":
        await this.handleExportResultsCsv();
        break;
      case "exportResultsJSON":
        await this.handleExportResultsJson();
        break;
      case "readClipboard":
        await this.handleReadClipboard();
        break;
      case "addBookmark":
        if (parsed.payload) {
          await this.handleAddBookmark(
            parsed.payload.sql,
            parsed.payload.connectionId,
          );
        }
        break;
    }
  }

  handleConnectionsChanged(): void {
    this.pushConnections();
    this.view.syncTitle();
  }

  async handleSchemaLoaded(connectionId: string): Promise<void> {
    const activeConnectionId = this.view.getActiveConnectionId();
    const initialConnectionId = this.view.getInitialConnectionId();
    if (
      connectionId !== activeConnectionId &&
      connectionId !== initialConnectionId
    ) {
      return;
    }

    const tables = this.connectionManager.getSchema(connectionId);
    this.view.postMessage({
      type: "schema",
      payload: { connectionId, tables },
    });
  }

  private handleActiveConnectionChanged(connectionId: string): void {
    this.view.setActiveConnectionId(connectionId);
    this.view.syncTitle();
  }

  private async handleExecuteQuery(
    sql: string,
    connectionIdOverride?: string,
  ): Promise<void> {
    if (!sql.trim()) {
      return;
    }

    if (!(await this.confirmQueryExecution(sql))) {
      this.postQueryError("Query execution cancelled.");
      return;
    }

    const connectionId =
      connectionIdOverride || this.view.getInitialConnectionId();

    if (!this.connectionManager.isConnected(connectionId)) {
      try {
        await this.connectionManager.connectTo(connectionId);
      } catch (error: unknown) {
        const normalized = normalizeUnknownError(error);
        this.postQueryError(`Cannot connect: ${normalized.message}`);
        return;
      }
    }

    await this.connectionManager.addToHistory(connectionId, sql);

    const driver = this.connectionManager.getDriver(connectionId);
    if (!driver) {
      return;
    }

    try {
      const result = await driver.query(sql);
      const formattedResult = formatQueryResult(
        result,
        this.connectionManager.getQueryRowLimit(),
      );

      this.view.setLastQueryResult({
        columns: formattedResult.columns,
        rows: formattedResult.rows,
      });
      this.view.postMessage({
        type: "queryResult",
        payload: formattedResult,
      });
    } catch (error: unknown) {
      const normalized = normalizeUnknownError(error);
      this.postQueryError(normalized.message);
    }
  }

  private pushConnections(): void {
    const connections = this.connectionManager
      .getConnections()
      .map((connection) => ({
        id: connection.id,
        name: connection.name,
        type: connection.type,
      }));
    this.view.postMessage({ type: "connections", payload: connections });
  }

  private async pushSchema(connectionIdOverride?: string): Promise<void> {
    const connectionId =
      connectionIdOverride || this.view.getActiveConnectionId();

    if (!this.connectionManager.isConnected(connectionId)) {
      this.view.postMessage({
        type: "schema",
        payload: { connectionId, tables: [] },
      });
      return;
    }

    const tables = await this.connectionManager.getSchemaAsync(connectionId);
    this.view.postMessage({
      type: "schema",
      payload: { connectionId, tables },
    });
  }

  private postQueryError(error: string): void {
    this.view.postMessage({
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

  private async handleExportResultsCsv(): Promise<void> {
    const cached = this.view.getLastQueryResult();
    if (!cached || cached.columns.length === 0) {
      vscode.window.showWarningMessage("[RapiDB] No query results to export.");
      return;
    }

    await exportQueryResultsAsCsv(cached);
  }

  private async handleExportResultsJson(): Promise<void> {
    const cached = this.view.getLastQueryResult();
    if (!cached || cached.columns.length === 0) {
      vscode.window.showWarningMessage("[RapiDB] No query results to export.");
      return;
    }

    await exportQueryResultsAsJson(cached);
  }

  private async handleReadClipboard(): Promise<void> {
    try {
      const text = await vscode.env.clipboard.readText();
      this.view.postMessage({ type: "clipboardText", payload: text });
    } catch {
      this.view.postMessage({ type: "clipboardText", payload: "" });
    }
  }

  private async handleAddBookmark(
    sql: string,
    connectionIdOverride?: string,
  ): Promise<void> {
    if (!sql?.trim()) {
      return;
    }

    const connectionId =
      connectionIdOverride ||
      this.view.getActiveConnectionId() ||
      this.view.getInitialConnectionId();

    try {
      await this.connectionManager.addBookmark(connectionId, sql);
      this.view.postMessage({
        type: "bookmarkSaved",
        payload: { ok: true },
      });
    } catch (error: unknown) {
      const normalized = normalizeUnknownError(error);
      this.view.postMessage({
        type: "bookmarkSaved",
        payload: { ok: false, error: normalized.message },
      });
    }
  }
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
