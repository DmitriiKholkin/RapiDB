import * as vscode from "vscode";
import type { ConnectionType } from "../../shared/connectionTypes";
import {
  type OperationCancellationContext,
  QUERY_LIMIT_POLICY,
  type QueryExecutionCancellationHandle,
} from "../../shared/safetyContracts";
import {
  type BookmarkSavedPayload,
  parseQueryPanelMessage,
  type QueryBookmarkPayload,
  type QueryEditorSqlDialect,
  type QueryResultExportPayload,
} from "../../shared/webviewContracts";
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
import {
  decideReadOnlyQueryExecution,
  mayChangeDatabaseSchema,
} from "../utils/readOnlyGuards";
import { applyHardCapToSqlQuery } from "../utils/sqlHardCap";

const SUPERSEDED_QUERY_REJECTED_MESSAGE =
  "[RapiDB] Cannot execute query while a previous query is still running for this connection.";
const SUPERSEDED_QUERY_CANCEL_TIMEOUT_MS = 1_500;
const MSSQL_READ_ONLY_ENFORCEMENT_MESSAGE =
  "[RapiDB] Read-only MSSQL queries require database-enforced read permissions; client-side SQL classification is not sufficient.";
const CONNECTION_TYPE_TO_SCHEMA_DIALECT: Record<
  ConnectionType,
  QueryEditorSqlDialect
> = {
  pg: "postgresql",
  mysql: "mysql",
  sqlite: "sqlite",
  mssql: "transactsql",
  oracle: "plsql",
  mongodb: "sql",
  redis: "sql",
  elasticsearch: "sql",
  dynamodb: "sql",
};
let nextQueryRequestToken = 0;

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
          await this.handleActiveConnectionChanged(parsed.payload.connectionId);
        }
        break;
      case "executeQuery":
        if (parsed.payload) {
          await this.handleExecuteQuery(
            parsed.payload.queryText,
            parsed.payload.connectionId,
            parsed.payload.operationId,
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
        if (parsed.payload) await this.handleReadClipboard(parsed.payload);
        break;
      case "writeClipboard":
        if (parsed.payload) {
          await this.handleWriteClipboard(parsed.payload.text);
        }
        break;
      case "addBookmark":
        if (parsed.payload) {
          await this.handleAddBookmark(parsed.payload);
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

  private async handleActiveConnectionChanged(
    connectionId: string,
  ): Promise<void> {
    if (connectionId === this.view.getActiveConnectionId()) {
      return;
    }
    this.queryRequestToken = 0;
    await this.cancelOwnedQueryExecutions("superseded");
    this.view.setActiveConnectionId(connectionId);
    this.view.syncTitle();
    void this.pushSchema(connectionId);
  }

  private async handleExecuteQuery(
    queryText: string,
    connectionIdOverride?: string,
    operationId?: string,
  ): Promise<void> {
    if (!queryText.trim()) {
      return;
    }

    const connectionId = this.resolveConnectionId(connectionIdOverride);
    const requestToken = ++nextQueryRequestToken;
    this.queryRequestToken = requestToken;
    const resultIdentity = operationId
      ? { connectionId, operationId, requestToken }
      : undefined;
    const canProceed = await this.cancelSupersededQueryExecution(
      connectionId,
      requestToken,
    );
    if (!canProceed) {
      this.postQueryError(
        SUPERSEDED_QUERY_REJECTED_MESSAGE,
        requestToken,
        resultIdentity,
      );
      return;
    }
    const reconnectBlock =
      this.connectionManager.getAutomaticReconnectBlockReason?.(connectionId);
    if (reconnectBlock) {
      this.postQueryError(reconnectBlock, requestToken, resultIdentity);
      return;
    }
    const readOnlyDecision = decideReadOnlyQueryExecution(
      this.connectionManager,
      connectionId,
      queryText,
    );
    if (!readOnlyDecision.allowed) {
      this.postQueryError(
        readOnlyDecision.reason,
        requestToken,
        resultIdentity,
      );
      return;
    }
    const connection = this.connectionManager.getConnection(connectionId);
    if (connection?.readOnly === true && connection.type === "mssql") {
      this.postQueryError(
        MSSQL_READ_ONLY_ENFORCEMENT_MESSAGE,
        requestToken,
        resultIdentity,
      );
      return;
    }

    if (!this.isCurrentQueryRequest(requestToken)) {
      return;
    }

    const connectionType = connection?.type;
    const effectiveRowLimit = Math.min(
      this.connectionManager.getQueryRowLimit(),
      QUERY_LIMIT_POLICY.hardCap,
    );
    const hardCapProbeLimit = effectiveRowLimit + 1;
    const driverBounded =
      this.connectionManager.getDriverCapabilities?.(connectionId)
        ?.boundedQueryResults === true;
    const rewrite = driverBounded
      ? { queryText }
      : applyHardCapToSqlQuery(queryText, connectionType, hardCapProbeLimit);
    if ("error" in rewrite && rewrite.error) {
      this.postQueryError(rewrite.error, requestToken, resultIdentity);
      return;
    }
    const cappedQueryText = rewrite.queryText;

    if (!this.connectionManager.isConnected(connectionId)) {
      try {
        await this.connectionManager.connectTo(connectionId, "automatic");
      } catch (error: unknown) {
        const normalized = normalizeUnknownError(error, connection);
        this.postQueryError(
          `Cannot connect: ${normalized.message}`,
          requestToken,
          resultIdentity,
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
        resultIdentity,
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
        logger.error("Failed to save query history", error);
      });

    try {
      const result = await driver.query(cappedQueryText, undefined, {
        requestToken,
        ...(driverBounded ? { hardCap: hardCapProbeLimit } : {}),
        ...(connection?.readOnly === true ? { readOnly: true } : {}),
      });
      if (!this.isCurrentQueryRequest(requestToken)) {
        return;
      }
      const formattedResult = formatQueryResult(result, effectiveRowLimit);
      const schemaDialect: QueryEditorSqlDialect =
        this.connectionManager.getDriverCapabilities?.(connectionId)
          ?.editorPresentation?.sqlDialect ??
        (connectionType
          ? CONNECTION_TYPE_TO_SCHEMA_DIALECT[connectionType]
          : "sql");
      if (mayChangeDatabaseSchema(cappedQueryText, schemaDialect)) {
        this.connectionManager.refreshSchemaCache(connectionId);
      }

      this.view.setLastQueryResult({
        columns: formattedResult.columns,
        columnMeta: formattedResult.columnMeta,
        rows: formattedResult.rows,
      });
      this.view.postMessage({
        type: "queryResult",
        payload: { ...formattedResult, ...resultIdentity },
      });
    } catch (error: unknown) {
      const normalized = normalizeUnknownError(error, connection);
      this.postQueryError(normalized.message, requestToken, resultIdentity);
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
    const supportsCancellation =
      typeof driver.cancelCurrentOperation === "function";

    return {
      requestToken,
      connectionId,
      operationName: "query",
      supportsCancellation,
      cancel: async (context: OperationCancellationContext) => {
        if (!supportsCancellation) {
          return;
        }

        await driver.cancelCurrentOperation?.({
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

  private async cancelOwnedQueryExecutions(
    reason: OperationCancellationContext["reason"],
  ): Promise<void> {
    const executions = [...this.activeQueryExecutions.values()];
    this.activeQueryExecutions.clear();
    await Promise.all(
      executions.map(async (execution) => {
        if (!execution.supportsCancellation) {
          return;
        }
        let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
        try {
          await Promise.race([
            execution.cancel({
              reason,
              operationName: execution.operationName,
              connectionId: execution.connectionId,
              requestToken: execution.requestToken,
            }),
            new Promise<void>((resolve) => {
              timeoutHandle = setTimeout(
                resolve,
                SUPERSEDED_QUERY_CANCEL_TIMEOUT_MS,
              );
            }),
          ]);
        } catch (error: unknown) {
          logger.error("Failed to cancel owned query execution", error);
        } finally {
          if (timeoutHandle) {
            clearTimeout(timeoutHandle);
          }
        }
      }),
    );
  }

  async dispose(): Promise<void> {
    this.queryRequestToken = 0;
    await this.cancelOwnedQueryExecutions("lifecycle_shutdown");
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
      logger.error("Failed to load schema", error);
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

  private postQueryError(
    error: string,
    requestToken?: number,
    identity?: {
      connectionId: string;
      operationId?: string;
      requestToken: number;
    },
  ): void {
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
        ...identity,
      },
    });
  }

  private async handleExportResults(
    format: "csv" | "json",
    columnOrder?: string[],
    sort?: QueryResultExportPayload["sort"],
  ): Promise<void> {
    const cached = this.getCachedResultForExport();
    if (!cached) {
      return;
    }

    const columnIds = new Set(cached.columns.map((_, i) => colKey(i)));
    if (
      columnOrder?.some((id) => !columnIds.has(id)) ||
      sort?.some(({ id }) => !columnIds.has(id))
    ) {
      vscode.window.showWarningMessage(
        "[RapiDB] Invalid query result column ID.",
      );
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
    if (columnOrder.length === 0) return result;
    const indices = columnOrder.map((id) => Number(id.slice(6)));

    const reorderedRows = result.rows.map((row) => {
      const newRow: Record<string, unknown> = {};
      for (let i = 0; i < columnOrder.length; i++) {
        newRow[colKey(i)] = row[columnOrder[i]];
      }
      return newRow;
    });

    return {
      ...result,
      columns: indices.map((i) => result.columns[i]),
      ...(result.columnMeta
        ? { columnMeta: indices.map((i) => result.columnMeta![i]) }
        : {}),
      rows: reorderedRows,
    };
  }

  private sortResultRows(
    result: QueryPanelCachedResult,
    sort: NonNullable<QueryResultExportPayload["sort"]>,
  ): QueryPanelCachedResult {
    const sortedRows = [...result.rows].sort((a, b) => {
      for (const { id, desc } of sort) {
        const aVal = a[id];
        const bVal = b[id];
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

  private async handleReadClipboard(
    payload: import("../../shared/webviewContracts").ClipboardReadPayload,
  ): Promise<void> {
    const text = await readClipboardTextSafe();
    this.view.postMessage({
      type: "clipboardText",
      payload: { ...payload, text },
    });
  }

  private async handleWriteClipboard(text: string): Promise<void> {
    await writeClipboardText(text);
  }

  private async handleAddBookmark({
    queryText,
    connectionId: connectionIdOverride,
    requestId,
  }: QueryBookmarkPayload): Promise<void> {
    if (!queryText?.trim()) {
      return;
    }

    const connectionId = this.resolveConnectionId(connectionIdOverride);

    try {
      await this.connectionManager.addBookmark(connectionId, queryText);
      this.view.postMessage({
        type: "bookmarkSaved",
        payload: {
          ok: true,
          ...(requestId !== undefined ? { requestId } : {}),
        } satisfies BookmarkSavedPayload,
      });
    } catch (error: unknown) {
      const normalized = normalizeUnknownError(error);
      this.view.postMessage({
        type: "bookmarkSaved",
        payload: {
          ok: false,
          error: normalized.message,
          ...(requestId !== undefined ? { requestId } : {}),
        } satisfies BookmarkSavedPayload,
      });
    }
  }
}
