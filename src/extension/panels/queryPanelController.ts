import * as vscode from "vscode";
import type { ConnectionType } from "../../shared/connectionTypes";
import {
  type OperationCancellationContext,
  QUERY_LIMIT_POLICY,
  type QueryExecutionCancellationHandle,
  type SqlHardCapRewriteDecision,
} from "../../shared/safetyContracts";
import { parseQueryPanelMessage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { colKey, type QueryColumnMeta } from "../dbDrivers/types";
import { readClipboardTextSafe, writeClipboardText } from "../utils/clipboard";
import { normalizeUnknownError } from "../utils/errorHandling";
import {
  exportQueryResultsAsCsv,
  exportQueryResultsAsJson,
} from "../utils/exportService";
import { logger } from "../utils/logger";
import { formatQueryResult } from "../utils/queryResultFormatting";
import { decideReadOnlyQueryExecution } from "../utils/readOnlyGuards";

const SQL_CONNECTION_TYPES = new Set<ConnectionType>([
  "pg",
  "mysql",
  "sqlite",
  "mssql",
  "oracle",
]);

const LIMITABLE_QUERY_PREFIX = /^\s*(with|select|values)\b/i;
const WITH_QUERY_PREFIX = /^\s*with\b/i;
const SUPERSEDED_QUERY_REJECTED_MESSAGE =
  "[RapiDB] Cannot execute query while a previous query is still running for this connection.";
const SUPERSEDED_QUERY_CANCEL_TIMEOUT_MS = 1_500;

function stripTrailingBlockComment(queryText: string): string {
  if (!queryText.endsWith("*/")) {
    return queryText;
  }

  const blockStart = queryText.lastIndexOf("/*");
  if (blockStart < 0) {
    return queryText;
  }

  return queryText.slice(0, blockStart);
}

function trimTrailingSemicolonsAndComments(queryText: string): string {
  let normalized = queryText;

  while (normalized.length > 0) {
    const withoutTrailingWhitespace = normalized.replace(/\s+$/g, "");
    if (withoutTrailingWhitespace !== normalized) {
      normalized = withoutTrailingWhitespace;
      continue;
    }

    const withoutTrailingLineComment = normalized.replace(/--[^\r\n]*$/g, "");
    if (withoutTrailingLineComment !== normalized) {
      normalized = withoutTrailingLineComment;
      continue;
    }

    const withoutTrailingBlockComment = stripTrailingBlockComment(normalized);
    if (withoutTrailingBlockComment !== normalized) {
      normalized = withoutTrailingBlockComment;
      continue;
    }

    const withoutTrailingSemicolons = normalized.replace(/;+$/g, "");
    if (withoutTrailingSemicolons !== normalized) {
      normalized = withoutTrailingSemicolons;
      continue;
    }

    break;
  }

  return normalized;
}

function stripLeadingSqlComments(queryText: string): string {
  let cursor = queryText;

  while (cursor.length > 0) {
    const trimmed = cursor.trimStart();
    if (trimmed.startsWith("--")) {
      const nextLineBreak = trimmed.indexOf("\n");
      cursor = nextLineBreak >= 0 ? trimmed.slice(nextLineBreak + 1) : "";
      continue;
    }

    if (trimmed.startsWith("/*")) {
      const blockEnd = trimmed.indexOf("*/", 2);
      if (blockEnd < 0) {
        return "";
      }
      cursor = trimmed.slice(blockEnd + 2);
      continue;
    }

    return trimmed;
  }

  return "";
}

function findFirstSqlTokenIndex(queryText: string): number {
  let index = 0;
  while (index < queryText.length) {
    const rest = queryText.slice(index);
    const whitespace = /^\s+/.exec(rest);
    if (whitespace) {
      index += whitespace[0].length;
      continue;
    }

    if (rest.startsWith("--")) {
      const nextLineBreak = rest.indexOf("\n");
      index = nextLineBreak >= 0 ? index + nextLineBreak + 1 : queryText.length;
      continue;
    }

    if (rest.startsWith("/*")) {
      const blockEnd = rest.indexOf("*/", 2);
      if (blockEnd < 0) {
        return -1;
      }
      index += blockEnd + 2;
      continue;
    }

    return index;
  }

  return -1;
}

function applyMssqlTopHardCap(
  queryText: string,
  hardCap: number,
): string | null {
  const tokenIndex = findFirstSqlTokenIndex(queryText);
  if (tokenIndex < 0) {
    return null;
  }

  const head = queryText.slice(0, tokenIndex);
  const tail = queryText.slice(tokenIndex);
  const selectPrefix = /^select\s+(distinct\s+|all\s+)?/i.exec(tail);
  if (!selectPrefix) {
    return null;
  }
  if (/^select\s+(?:distinct\s+|all\s+)?top\b/i.test(tail)) {
    return null;
  }

  const matched = selectPrefix[0];
  const topSelect = `${matched}TOP (${hardCap}) `;
  return `${head}${topSelect}${tail.slice(matched.length)}`;
}

function applyHardCapToSqlQuery(
  queryText: string,
  connectionType: ConnectionType | undefined,
  hardCap: number,
): { queryText: string; decision: SqlHardCapRewriteDecision } {
  if (!connectionType || !SQL_CONNECTION_TYPES.has(connectionType)) {
    return {
      queryText,
      decision: { applied: false, reason: "unsupported_connection" },
    };
  }

  const normalizedQuery = trimTrailingSemicolonsAndComments(queryText);
  if (!normalizedQuery) {
    return {
      queryText,
      decision: { applied: false, reason: "non_limitable_statement" },
    };
  }

  const classificationQuery = stripLeadingSqlComments(normalizedQuery);
  if (!classificationQuery) {
    return {
      queryText,
      decision: { applied: false, reason: "non_limitable_statement" },
    };
  }

  if (WITH_QUERY_PREFIX.test(classificationQuery)) {
    return {
      queryText,
      decision: { applied: false, reason: "unsafe_with_clause" },
    };
  }

  if (!LIMITABLE_QUERY_PREFIX.test(classificationQuery)) {
    return {
      queryText,
      decision: { applied: false, reason: "non_limitable_statement" },
    };
  }

  switch (connectionType) {
    case "mssql":
      {
        const topRewritten = applyMssqlTopHardCap(normalizedQuery, hardCap);
        if (topRewritten) {
          return {
            queryText: topRewritten,
            decision: { applied: true },
          };
        }
      }
      return {
        queryText: `SELECT TOP (${hardCap}) * FROM (${normalizedQuery}) AS [rapidb_query_cap]`,
        decision: { applied: true },
      };
    case "oracle":
      return {
        queryText: `SELECT * FROM (${normalizedQuery}) rapidb_query_cap FETCH FIRST ${hardCap} ROWS ONLY`,
        decision: { applied: true },
      };
    default:
      return {
        queryText: `SELECT * FROM (${normalizedQuery}) AS rapidb_query_cap LIMIT ${hardCap}`,
        decision: { applied: true },
      };
  }
}

export interface QueryPanelCachedResult {
  columns: string[];
  columnMeta?: QueryColumnMeta[];
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
  private schemaRequestToken = 0;

  private queryRequestToken = 0;

  private readonly activeQueryExecutions = new Map<
    string,
    QueryExecutionCancellationHandle
  >();

  constructor(
    private readonly connectionManager: ConnectionManager,
    private readonly view: QueryPanelView,
    private readonly context?: vscode.ExtensionContext,
  ) {}

  private resolveConnectionId(connectionIdOverride?: string): string {
    return (
      connectionIdOverride ||
      this.view.getActiveConnectionId() ||
      this.view.getInitialConnectionId()
    );
  }

  private getCachedResultForExport(): QueryPanelCachedResult | null {
    const cached = this.view.getLastQueryResult();
    if (cached && cached.columns.length > 0) {
      return cached;
    }

    vscode.window.showWarningMessage("[RapiDB] No query results to export.");
    return null;
  }

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
            parsed.payload.queryText,
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
        await this.handleExportResults(
          "csv",
          parsed.payload?.columnOrder,
          parsed.payload?.sort,
        );
        break;
      case "exportResultsJSON":
        await this.handleExportResults(
          "json",
          parsed.payload?.columnOrder,
          parsed.payload?.sort,
        );
        break;
      case "readClipboard":
        await this.handleReadClipboard();
        break;
      case "writeClipboard":
        if (parsed.payload) {
          await this.handleWriteClipboard(parsed.payload.text);
        }
        break;
      case "addBookmark":
        if (parsed.payload) {
          await this.handleAddBookmark(
            parsed.payload.queryText,
            parsed.payload.connectionId,
          );
        }
        break;
    }
  }

  handleConnectionsChanged(): void {
    this.pushConnections();
    this.view.syncTitle();
    void this.pushSchema();
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

    const schema = this.connectionManager.getSchema(connectionId);
    this.view.postMessage({
      type: "schema",
      payload: { connectionId, schema },
    });
  }

  private handleActiveConnectionChanged(connectionId: string): void {
    this.view.setActiveConnectionId(connectionId);
    this.view.syncTitle();
    void this.pushSchema(connectionId);
  }

  private async handleExecuteQuery(
    queryText: string,
    connectionIdOverride?: string,
  ): Promise<void> {
    if (!queryText.trim()) {
      return;
    }

    const connectionId = this.resolveConnectionId(connectionIdOverride);
    const requestToken = ++this.queryRequestToken;
    const canProceed = await this.cancelSupersededQueryExecution(
      connectionId,
      requestToken,
    );
    if (!canProceed) {
      this.postQueryError(SUPERSEDED_QUERY_REJECTED_MESSAGE, requestToken);
      return;
    }
    const readOnlyDecision = decideReadOnlyQueryExecution(
      this.connectionManager,
      connectionId,
      queryText,
    );
    if (!readOnlyDecision.allowed) {
      this.postQueryError(readOnlyDecision.reason, requestToken);
      return;
    }

    if (!this.isCurrentQueryRequest(requestToken)) {
      return;
    }

    const connectionType =
      this.connectionManager.getConnection(connectionId)?.type;
    const effectiveRowLimit = Math.min(
      this.connectionManager.getQueryRowLimit(),
      QUERY_LIMIT_POLICY.hardCap,
    );
    const hardCapProbeLimit = effectiveRowLimit + 1;
    const rewrite = applyHardCapToSqlQuery(
      queryText,
      connectionType,
      hardCapProbeLimit,
    );
    const cappedQueryText = rewrite.queryText;

    if (!this.connectionManager.isConnected(connectionId)) {
      try {
        await this.connectionManager.connectTo(connectionId);
      } catch (error: unknown) {
        const normalized = normalizeUnknownError(error);
        this.postQueryError(
          `Cannot connect: ${normalized.message}`,
          requestToken,
        );
        return;
      }
    }

    if (!this.isCurrentQueryRequest(requestToken)) {
      return;
    }

    const driver = this.connectionManager.getDriver(connectionId);
    if (!driver) {
      this.postQueryError(
        `[RapiDB] Cannot execute query: driver is unavailable for ${connectionId}.`,
        requestToken,
      );
      return;
    }

    this.activeQueryExecutions.set(
      connectionId,
      this.createQueryExecutionHandle(connectionId, requestToken, driver),
    );

    if (!this.isCurrentQueryRequest(requestToken)) {
      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === requestToken) {
        this.activeQueryExecutions.delete(connectionId);
      }
      return;
    }

    // History persistence must not block query execution.
    void this.connectionManager
      .addToHistory(connectionId, queryText)
      .catch((error) => {
        console.error("[RapiDB] Failed to save query history:", error);
      });

    try {
      const result = await driver.query(cappedQueryText);
      if (!this.isCurrentQueryRequest(requestToken)) {
        return;
      }
      const formattedResult = formatQueryResult(result, effectiveRowLimit);

      this.view.setLastQueryResult({
        columns: formattedResult.columns,
        columnMeta: formattedResult.columnMeta,
        rows: formattedResult.rows,
      });
      this.view.postMessage({
        type: "queryResult",
        payload: formattedResult,
      });
    } catch (error: unknown) {
      const normalized = normalizeUnknownError(error);
      this.postQueryError(normalized.message, requestToken);
    } finally {
      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === requestToken) {
        this.activeQueryExecutions.delete(connectionId);
      }
    }
  }

  private createQueryExecutionHandle(
    connectionId: string,
    requestToken: number,
    driver: {
      query: (queryText: string) => Promise<unknown>;
      cancelCurrentOperation?: (
        context?: OperationCancellationContext,
      ) => void | Promise<void>;
    },
  ): QueryExecutionCancellationHandle {
    const cancelCurrentOperation = driver.cancelCurrentOperation;
    const supportsCancellation = typeof cancelCurrentOperation === "function";

    return {
      requestToken,
      connectionId,
      operationName: "query",
      supportsCancellation,
      cancel: async (context: OperationCancellationContext) => {
        if (!supportsCancellation) {
          return;
        }

        await cancelCurrentOperation({
          ...context,
          operationName: "query",
          connectionId,
          requestToken,
        });
      },
    };
  }

  private async cancelSupersededQueryExecution(
    connectionId: string,
    supersededByRequestToken: number,
  ): Promise<boolean> {
    const previous = this.activeQueryExecutions.get(connectionId);
    if (!previous) {
      return true;
    }

    if (!previous.supportsCancellation) {
      logger.warn(
        `Query cancellation is not supported for connection ${previous.connectionId}; superseded request ${previous.requestToken} may continue executing in the backend.`,
      );
      return false;
    }

    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    try {
      const cancellationResult = await Promise.race([
        previous
          .cancel({
            reason: "superseded",
            operationName: previous.operationName,
            connectionId: previous.connectionId,
            requestToken: previous.requestToken,
            supersededByRequestToken,
          })
          .then(() => "cancelled" as const),
        new Promise<"timed-out">((resolve) => {
          timeoutHandle = setTimeout(() => {
            resolve("timed-out");
          }, SUPERSEDED_QUERY_CANCEL_TIMEOUT_MS);
        }),
      ]);

      if (cancellationResult !== "cancelled") {
        // 1-arg console.error preserves the original log shape that
        // downstream log scrapers and tests key on.
        console.error(
          `[RapiDB] Superseded query cancellation timed out for connection ${previous.connectionId}.`,
        );
        return false;
      }

      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === previous.requestToken) {
        this.activeQueryExecutions.delete(connectionId);
      }
      return true;
    } catch (error: unknown) {
      logger.error("Failed to cancel superseded query execution", error);
      return false;
    } finally {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    }
  }

  private pushConnections(): void {
    const connections = this.connectionManager
      .getConnections()
      .map((connection) => ({
        id: connection.id,
        name: connection.name,
        type: connection.type,
        editorPresentation: this.connectionManager.getQueryEditorPresentation(
          connection.id,
        ),
      }));
    this.view.postMessage({ type: "connections", payload: connections });
  }

  private async pushSchema(connectionIdOverride?: string): Promise<void> {
    const requestToken = ++this.schemaRequestToken;
    const connectionId = this.resolveConnectionId(connectionIdOverride);

    if (!this.connectionManager.isConnected(connectionId)) {
      if (!this.isCurrentSchemaRequest(requestToken)) {
        return;
      }
      this.view.postMessage({
        type: "schema",
        payload: { connectionId, schema: [] },
      });
      return;
    }

    try {
      const schema = await this.connectionManager.getSchemaAsync(connectionId);
      if (!this.isCurrentSchemaRequest(requestToken)) {
        return;
      }
      this.view.postMessage({
        type: "schema",
        payload: { connectionId, schema },
      });
    } catch (error: unknown) {
      if (!this.isCurrentSchemaRequest(requestToken)) {
        return;
      }
      console.error("[RapiDB] Failed to load schema:", error);
      this.view.postMessage({
        type: "schema",
        payload: { connectionId, schema: [] },
      });
    }
  }

  private isCurrentSchemaRequest(requestToken: number): boolean {
    return requestToken === this.schemaRequestToken;
  }

  private isCurrentQueryRequest(requestToken: number): boolean {
    return requestToken === this.queryRequestToken;
  }

  private postQueryError(error: string, requestToken?: number): void {
    if (
      requestToken !== undefined &&
      !this.isCurrentQueryRequest(requestToken)
    ) {
      return;
    }
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

  private async handleExportResults(
    format: "csv" | "json",
    columnOrder?: string[],
    sort?: { column: string; desc: boolean }[],
  ): Promise<void> {
    const cached = this.getCachedResultForExport();
    if (!cached) {
      return;
    }

    const sortedResult =
      sort && sort.length > 0 ? this.sortResultRows(cached, sort) : cached;

    const orderedResult = columnOrder
      ? this.reorderResultColumns(sortedResult, columnOrder)
      : sortedResult;

    if (format === "csv") {
      await exportQueryResultsAsCsv(orderedResult, { context: this.context });
      return;
    }

    await exportQueryResultsAsJson(orderedResult, { context: this.context });
  }

  private reorderResultColumns(
    result: QueryPanelCachedResult,
    columnOrder: string[],
  ): QueryPanelCachedResult {
    const indexMap = new Map(result.columns.map((col, i) => [col, i]));
    const reorderedColumns: string[] = [];
    const reorderedMeta: NonNullable<typeof result.columnMeta> = [];

    for (const colId of columnOrder) {
      const origIndex = indexMap.get(colId);
      if (origIndex !== undefined) {
        reorderedColumns.push(colId);
        if (result.columnMeta) {
          reorderedMeta.push(result.columnMeta[origIndex]);
        }
      }
    }

    if (reorderedColumns.length === 0) return result;

    const reorderedRows = result.rows.map((row) => {
      const newRow: Record<string, unknown> = {};
      for (let i = 0; i < reorderedColumns.length; i++) {
        const origIndex = indexMap.get(reorderedColumns[i]);
        if (origIndex !== undefined) {
          newRow[colKey(i)] = row[colKey(origIndex)];
        }
      }
      return newRow;
    });

    return {
      ...result,
      columns: reorderedColumns,
      ...(result.columnMeta ? { columnMeta: reorderedMeta } : {}),
      rows: reorderedRows,
    };
  }

  private sortResultRows(
    result: QueryPanelCachedResult,
    sort: { column: string; desc: boolean }[],
  ): QueryPanelCachedResult {
    const nameToKey = new Map(result.columns.map((col, i) => [col, colKey(i)]));
    const sortedRows = [...result.rows].sort((a, b) => {
      for (const { column, desc } of sort) {
        const key = nameToKey.get(column);
        if (key === undefined) continue;
        const aVal = a[key];
        const bVal = b[key];
        let cmp = 0;
        if (aVal == null && bVal == null) cmp = 0;
        else if (aVal == null) cmp = -1;
        else if (bVal == null) cmp = 1;
        else if (typeof aVal === "number" && typeof bVal === "number")
          cmp = aVal - bVal;
        else if (typeof aVal === "string" && typeof bVal === "string")
          cmp = aVal.localeCompare(bVal);
        else cmp = String(aVal).localeCompare(String(bVal));
        if (cmp !== 0) return desc ? -cmp : cmp;
      }
      return 0;
    });
    return { ...result, rows: sortedRows };
  }

  private async handleReadClipboard(): Promise<void> {
    const text = await readClipboardTextSafe();
    this.view.postMessage({ type: "clipboardText", payload: text });
  }

  private async handleWriteClipboard(text: string): Promise<void> {
    await writeClipboardText(text);
  }

  private async handleAddBookmark(
    queryText: string,
    connectionIdOverride?: string,
  ): Promise<void> {
    if (!queryText?.trim()) {
      return;
    }

    const connectionId = this.resolveConnectionId(connectionIdOverride);

    try {
      await this.connectionManager.addBookmark(connectionId, queryText);
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
