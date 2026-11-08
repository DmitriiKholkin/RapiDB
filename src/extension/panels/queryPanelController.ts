import {
  createTable,
  getCoreRowModel,
  getSortedRowModel,
  type SortingFn,
} from "@tanstack/react-table";
import * as vscode from "vscode";
import type { ConnectionType } from "../../shared/connectionTypes";
import { compareExactNumericValues } from "../../shared/exactNumericComparison";
import {
  type OperationCancellationContext,
  QUERY_LIMIT_POLICY,
  type QueryExecutionCancellationHandle,
} from "../../shared/safetyContracts";
import type { QueryMissingCell } from "../../shared/tableTypes";
import {
  type BookmarkSavedPayload,
  parseQueryPanelMessage,
  type QueryBookmarkPayload,
  type QueryEditorSqlDialect,
  type QueryResultExportPayload,
  type QuerySchemaPayload,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type { SchemaObjectEntry } from "../connectionManagerModels";
import { colKey, type QueryColumnMeta } from "../dbDrivers/types";
import { readClipboardTextSafe, writeClipboardText } from "../utils/clipboard";
import { normalizeUnknownError } from "../utils/errorHandling";
import {
  exportQueryResultsAsCsv,
  exportQueryResultsAsJson,
} from "../utils/exportService";
import { logger } from "../utils/logger";
import {
  type FormattedQueryResultSet,
  formatQueryResult,
} from "../utils/queryResultFormatting";
import {
  decideReadOnlyQueryExecution,
  mayChangeDatabaseSchema,
} from "../utils/readOnlyGuards";
import { applyHardCapToSqlQuery } from "../utils/sqlHardCap";
import { hasExecutableMysqlComment } from "../utils/sqlStatementScan";

const SUPERSEDED_QUERY_REJECTED_MESSAGE =
  "[RapiDB] Cannot execute query while a previous query is still running for this connection.";
const SUPERSEDED_QUERY_CANCEL_TIMEOUT_MS = 1_500;
const MSSQL_READ_ONLY_ENFORCEMENT_MESSAGE =
  "[RapiDB] Read-only MSSQL queries require database-enforced read permissions; client-side SQL classification is not sufficient.";
const MYSQL_EXECUTABLE_COMMENT_MESSAGE =
  "[RapiDB] MySQL/MariaDB executable comments are blocked because the server executes their contents. Remove the `/*!...*/` or `/*M!...*/` wrapper and run the intended statement explicitly.";
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
  exportRows?: Record<string, unknown>[];
  partial?: boolean;
  resultSets?: FormattedQueryResultSet[];
  resultSetsTruncated?: boolean;
  missingCells?: QueryMissingCell[];
  truncated?: boolean;
  truncatedAt?: number;
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

interface ActiveQueryExecution extends QueryExecutionCancellationHandle {
  operationId?: string;
  cancelRequested?: boolean;
  cancellationPending?: Promise<void>;
  queryStarted: boolean;
  completion?: Promise<void>;
  finish?: () => void;
}

export class QueryPanelController {
  private schemaRequestToken = 0;

  private connectionChangeToken = 0;
  private requestedConnectionId: string | undefined;

  private queryRequestToken = 0;
  private acceptedQueryRequestToken = 0;

  private readonly activeQueryExecutions = new Map<
    string,
    ActiveQueryExecution
  >();

  constructor(
    private readonly connectionManager: ConnectionManager,
    private readonly view: QueryPanelView,
    private readonly context?: vscode.ExtensionContext,
  ) {}

  private resolveConnectionId(connectionIdOverride?: string): string {
    return (
      connectionIdOverride ||
      this.requestedConnectionId ||
      this.view.getActiveConnectionId() ||
      this.view.getInitialConnectionId()
    );
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
      case "cancelQuery":
        if (parsed.payload) {
          await this.handleCancelQuery(parsed.payload.operationId);
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
          parsed.payload?.resultSetIndex,
        );
        break;
      case "exportResultsJSON":
        await this.handleExportResults(
          "json",
          parsed.payload?.columnOrder,
          parsed.payload?.sort,
          parsed.payload?.resultSetIndex,
        );
        break;
      case "readClipboard":
        if (parsed.payload) await this.handleReadClipboard(parsed.payload);
        break;
      case "writeClipboard":
        if (parsed.payload) {
          await this.handleWriteClipboard(parsed.payload);
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
      payload: this.buildSchemaPayload(connectionId, schema),
    });
  }

  private async handleActiveConnectionChanged(
    connectionId: string,
  ): Promise<void> {
    if (
      connectionId ===
      (this.requestedConnectionId ?? this.view.getActiveConnectionId())
    ) {
      return;
    }
    const changeToken = ++this.connectionChangeToken;
    this.requestedConnectionId = connectionId;
    this.queryRequestToken = 0;
    this.acceptedQueryRequestToken = 0;
    await this.cancelOwnedQueryExecutions("superseded");
    if (changeToken !== this.connectionChangeToken) {
      return;
    }
    this.requestedConnectionId = undefined;
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
      if (this.queryRequestToken === requestToken) {
        this.queryRequestToken = this.acceptedQueryRequestToken;
      }
      return;
    }
    this.acceptedQueryRequestToken =
      this.queryRequestToken === requestToken ? requestToken : 0;
    const reconnectBlock =
      this.connectionManager.getAutomaticReconnectBlockReason?.(connectionId);
    if (reconnectBlock) {
      this.postQueryError(reconnectBlock, requestToken, resultIdentity);
      return;
    }
    const connection = this.connectionManager.getConnection(connectionId);
    if (connection?.type === "mysql" && hasExecutableMysqlComment(queryText)) {
      this.postQueryError(
        MYSQL_EXECUTABLE_COMMENT_MESSAGE,
        requestToken,
        resultIdentity,
      );
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

    const pendingExecution: ActiveQueryExecution = {
      requestToken,
      connectionId,
      operationName: "query",
      operationId,
      supportsCancellation: false,
      queryStarted: false,
      cancel: async () => undefined,
    };
    this.activeQueryExecutions.set(connectionId, pendingExecution);

    if (!this.connectionManager.isConnected(connectionId)) {
      try {
        await this.connectionManager.connectTo(connectionId, "automatic");
      } catch (error: unknown) {
        const active = this.activeQueryExecutions.get(connectionId);
        if (active?.requestToken === requestToken) {
          this.activeQueryExecutions.delete(connectionId);
        }
        if (!this.isCurrentQueryRequest(requestToken)) return;
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
      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === requestToken) {
        this.activeQueryExecutions.delete(connectionId);
      }
      return;
    }

    const driver = this.connectionManager.getDriver(connectionId);
    if (!driver) {
      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === requestToken) {
        this.activeQueryExecutions.delete(connectionId);
      }
      this.postQueryError(
        `[RapiDB] Cannot execute query: driver is unavailable for ${connectionId}.`,
        requestToken,
        resultIdentity,
      );
      return;
    }

    const executionConnection =
      this.connectionManager.getConnection(connectionId);
    const executionDecision = decideReadOnlyQueryExecution(
      this.connectionManager,
      connectionId,
      queryText,
    );
    const executionBlock =
      executionConnection?.type !== connectionType
        ? "[RapiDB] Connection type changed while preparing the query. Execute it again."
        : (this.connectionManager.getAutomaticReconnectBlockReason?.(
            connectionId,
          ) ??
          (!executionDecision.allowed
            ? executionDecision.reason
            : executionConnection?.readOnly === true &&
                executionConnection.type === "mssql"
              ? MSSQL_READ_ONLY_ENFORCEMENT_MESSAGE
              : undefined));
    if (executionBlock) {
      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === requestToken) {
        this.activeQueryExecutions.delete(connectionId);
      }
      this.postQueryError(executionBlock, requestToken, resultIdentity);
      return;
    }

    this.activeQueryExecutions.set(
      connectionId,
      this.createQueryExecutionHandle(
        connectionId,
        requestToken,
        driver,
        operationId,
      ),
    );

    if (!this.isCurrentQueryRequest(requestToken)) {
      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === requestToken) {
        this.activeQueryExecutions.delete(connectionId);
      }
      return;
    }

    void this.connectionManager
      .addToHistory(connectionId, queryText)
      .catch((error) => {
        logger.error("Failed to save query history", error);
      });

    const driverCapabilities =
      this.connectionManager.getDriverCapabilities?.(connectionId);
    const schemaDialect: QueryEditorSqlDialect =
      driverCapabilities?.editorPresentation?.sqlDialect ??
      (connectionType
        ? CONNECTION_TYPE_TO_SCHEMA_DIALECT[connectionType]
        : "sql");
    const changesDiscoverableSchema =
      driverCapabilities?.schemaMutationGuard?.(cappedQueryText) ??
      mayChangeDatabaseSchema(cappedQueryText, schemaDialect);
    let queryStarted = false;

    try {
      queryStarted = true;
      const result = await driver.query(cappedQueryText, undefined, {
        requestToken,
        ...(driverBounded ? { hardCap: hardCapProbeLimit } : {}),
        ...(executionConnection?.readOnly === true ? { readOnly: true } : {}),
      });
      const activeExecution = this.activeQueryExecutions.get(connectionId);
      if (activeExecution?.requestToken === requestToken) {
        await activeExecution.cancellationPending;
      }
      if (!this.isCurrentQueryRequest(requestToken)) {
        return;
      }
      const formattedResult = formatQueryResult(result, effectiveRowLimit);

      this.view.setLastQueryResult({
        columns: formattedResult.columns,
        columnMeta: formattedResult.columnMeta,
        rows: formattedResult.rows,
        ...(formattedResult.exportRows
          ? { exportRows: formattedResult.exportRows }
          : {}),
        ...(formattedResult.resultSets
          ? { resultSets: formattedResult.resultSets }
          : {}),
        ...(formattedResult.resultSetsTruncated
          ? { resultSetsTruncated: true }
          : {}),
        missingCells: formattedResult.missingCells,
        ...(formattedResult.error ? { partial: true } : {}),
        truncated: formattedResult.truncated,
        truncatedAt: formattedResult.truncatedAt,
      });
      const webviewResult = { ...formattedResult };
      delete webviewResult.exportRows;
      this.view.postMessage({
        type: "queryResult",
        payload: { ...webviewResult, ...resultIdentity },
      });
    } catch (error: unknown) {
      const activeExecution = this.activeQueryExecutions.get(connectionId);
      if (activeExecution?.requestToken === requestToken) {
        await activeExecution.cancellationPending;
      }
      const normalized = normalizeUnknownError(error, connection);
      if (
        activeExecution?.requestToken === requestToken &&
        activeExecution.cancelRequested
      ) {
        const isRedisClientDisconnect =
          connection?.type === "redis" &&
          /^disconnects client\.?$/i.test(normalized.message.trim());
        const cancellationMessage = isRedisClientDisconnect
          ? "[RapiDB] Query cancellation was requested. The Redis query client was disconnected to stop the active request. The operation outcome may be unknown; cancellation does not confirm rollback."
          : `[RapiDB] Query failed after a cancellation request. ${normalized.message} The operation outcome may be unknown; cancellation does not confirm rollback.`;
        this.postQueryError(cancellationMessage, requestToken, resultIdentity);
        return;
      }
      this.postQueryError(normalized.message, requestToken, resultIdentity);
    } finally {
      if (
        queryStarted &&
        driver.isConnected?.() === false &&
        driver.getAutomaticReconnectBlockReason?.() &&
        this.connectionManager.getDriver(connectionId) === driver
      ) {
        try {
          await this.connectionManager.disconnectFrom(connectionId);
        } catch (error) {
          logger.error("Failed to retire a lost database session", error);
        }
      }
      if (queryStarted && changesDiscoverableSchema) {
        this.connectionManager.refreshSchemaCache(connectionId);
      }
      const active = this.activeQueryExecutions.get(connectionId);
      if (active?.requestToken === requestToken) {
        active.finish?.();
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
    operationId?: string,
  ): ActiveQueryExecution {
    const supportsCancellation =
      typeof driver.cancelCurrentOperation === "function";
    let finish!: () => void;
    const completion = new Promise<void>((resolve) => {
      finish = resolve;
    });

    return {
      requestToken,
      connectionId,
      operationName: "query",
      operationId,
      queryStarted: true,
      completion,
      finish,
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

  private async handleCancelQuery(operationId: string): Promise<void> {
    const execution = [...this.activeQueryExecutions.values()].find(
      (candidate) => candidate.operationId === operationId,
    );
    if (!execution) return;
    if (!execution.queryStarted) {
      execution.cancelRequested = true;
      this.postQueryError("[RapiDB] Query cancelled.", execution.requestToken, {
        connectionId: execution.connectionId,
        operationId: execution.operationId,
        requestToken: execution.requestToken,
      });
      if (this.queryRequestToken === execution.requestToken) {
        this.queryRequestToken = ++nextQueryRequestToken;
      }
      if (this.acceptedQueryRequestToken === execution.requestToken) {
        this.acceptedQueryRequestToken = 0;
      }
      const current = this.activeQueryExecutions.get(execution.connectionId);
      if (current?.requestToken === execution.requestToken) {
        this.activeQueryExecutions.delete(execution.connectionId);
      }
      return;
    }
    if (!execution.supportsCancellation) {
      void vscode.window.showWarningMessage(
        "[RapiDB] This database driver does not support cancelling the running query. It may continue until completion or timeout.",
      );
      return;
    }

    if (execution.cancelRequested) return;
    execution.cancelRequested = true;
    let finishCancellation!: () => void;
    execution.cancellationPending = new Promise<void>((resolve) => {
      finishCancellation = resolve;
    });
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    try {
      const result = await Promise.race([
        execution
          .cancel({
            reason: "manual",
            operationName: execution.operationName,
            connectionId: execution.connectionId,
            requestToken: execution.requestToken,
          })
          .then(() => "cancelled" as const),
        new Promise<"timed-out">((resolve) => {
          timeoutHandle = setTimeout(
            () => resolve("timed-out"),
            SUPERSEDED_QUERY_CANCEL_TIMEOUT_MS,
          );
        }),
      ]);
      if (result !== "cancelled") {
        execution.cancelRequested = false;
        void vscode.window.showWarningMessage(
          "[RapiDB] Query cancellation did not complete in time; the query may still be running.",
        );
        return;
      }
    } catch (error: unknown) {
      execution.cancelRequested = false;
      logger.error("Failed to cancel query execution", error);
      void vscode.window.showWarningMessage(
        "[RapiDB] Query cancellation failed; the query may still be running.",
      );
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      execution.cancellationPending = undefined;
      finishCancellation();
    }
  }

  private async cancelSupersededQueryExecution(
    connectionId: string,
    supersededByRequestToken: number,
  ): Promise<boolean> {
    const previous = this.activeQueryExecutions.get(connectionId);
    if (!previous) {
      return true;
    }

    if (!previous.queryStarted) {
      previous.cancelRequested = true;
      this.activeQueryExecutions.delete(connectionId);
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
          .then(async () => {
            await previous.completion;
            return "cancelled" as const;
          }),
        new Promise<"timed-out">((resolve) => {
          timeoutHandle = setTimeout(() => {
            resolve("timed-out");
          }, SUPERSEDED_QUERY_CANCEL_TIMEOUT_MS);
        }),
      ]);

      if (cancellationResult !== "cancelled") {
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
    await Promise.all(
      executions.map(async (execution) => {
        if (!execution.queryStarted) {
          execution.cancelRequested = true;
          if (
            this.activeQueryExecutions.get(execution.connectionId) === execution
          )
            this.activeQueryExecutions.delete(execution.connectionId);
          return;
        }
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
    ++this.connectionChangeToken;
    this.queryRequestToken = 0;
    this.acceptedQueryRequestToken = 0;
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
        payload: {
          connectionId,
          schema: [],
          error: "Not connected.",
        } satisfies QuerySchemaPayload<SchemaObjectEntry>,
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
        payload: this.buildSchemaPayload(connectionId, schema),
      });
    } catch (error: unknown) {
      if (!this.isCurrentSchemaRequest(requestToken)) {
        return;
      }
      logger.error("Failed to load schema", error);
      this.view.postMessage({
        type: "schema",
        payload: {
          connectionId,
          schema: [],
          error: normalizeUnknownError(error).message,
        } satisfies QuerySchemaPayload<SchemaObjectEntry>,
      });
    }
  }

  private buildSchemaPayload(
    connectionId: string,
    schema: SchemaObjectEntry[],
  ): QuerySchemaPayload<SchemaObjectEntry> {
    const schemaState =
      this.connectionManager.getSchemaSnapshotState(connectionId);
    return {
      connectionId,
      schema,
      ...(schemaState.status === "error"
        ? { error: schemaState.error ?? "Schema loading failed." }
        : {}),
    };
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
    resultSetIndex?: number,
  ): Promise<void> {
    const cached = this.getCachedResultForExport(resultSetIndex);
    if (!cached) {
      return;
    }

    if (columnOrder?.length === 0 && cached.columns.length > 0) {
      vscode.window.showWarningMessage(
        "[RapiDB] All result columns are collapsed. Expand at least one column to export.",
      );
      return;
    }

    if (cached.partial || cached.truncated) {
      const warnings: string[] = [];
      if (cached.partial) {
        warnings.push(
          "This result set may be incomplete because execution stopped after an error. The export will contain only rows received before the error.",
        );
      }
      if (cached.truncated) {
        const displayedRowCount = cached.rows.length;
        warnings.push(
          `This query result was truncated. The export will contain only the first ${displayedRowCount.toLocaleString()} displayed ${displayedRowCount === 1 ? "row" : "rows"}, not the full result.`,
        );
      }
      const exportLabel =
        cached.partial && !cached.truncated
          ? "Export partial result"
          : "Export displayed rows";
      const confirmation = await vscode.window.showWarningMessage(
        `[RapiDB] ${warnings.join(" ")}`,
        { modal: true },
        exportLabel,
      );
      if (confirmation !== exportLabel) return;
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

  private getCachedResultForExport(
    resultSetIndex?: number,
  ): QueryPanelCachedResult | null {
    const cached = this.view.getLastQueryResult();
    if (cached?.resultSets?.length) {
      const selectedIndex = resultSetIndex ?? 0;
      const selected = cached.resultSets[selectedIndex];
      if (!selected) {
        vscode.window.showWarningMessage(
          "[RapiDB] The selected query result set is no longer available.",
        );
        return null;
      }
      return cached.partial ? { ...selected, partial: true } : selected;
    }
    if (cached && cached.columns.length > 0) return cached;

    vscode.window.showWarningMessage("[RapiDB] No query results to export.");
    return null;
  }

  private reorderResultColumns(
    result: QueryPanelCachedResult,
    columnOrder: string[],
  ): QueryPanelCachedResult {
    if (columnOrder.length === 0) return result;
    const indices = columnOrder.map((id) => Number(id.slice(6)));
    const columnMeta = result.columnMeta;

    const reorderedRows = result.rows.map((row) => {
      const newRow: Record<string, unknown> = {};
      for (let i = 0; i < columnOrder.length; i++) {
        newRow[colKey(i)] = row[columnOrder[i]];
      }
      return newRow;
    });
    const reorderedExportRows = result.exportRows?.map((row) => {
      const newRow: Record<string, unknown> = {};
      for (let index = 0; index < indices.length; index++) {
        const sourceKey = colKey(indices[index]);
        if (Object.hasOwn(row, sourceKey)) {
          newRow[colKey(index)] = row[sourceKey];
        }
      }
      return newRow;
    });
    const reorderedMissingCells = result.missingCells?.flatMap(
      ({ rowIndex, columnIndex }) => {
        const nextColumnIndex = indices.indexOf(columnIndex);
        return nextColumnIndex < 0
          ? []
          : [{ rowIndex, columnIndex: nextColumnIndex }];
      },
    );

    return {
      ...result,
      columns: indices.map((i) => result.columns[i]),
      ...(columnMeta ? { columnMeta: indices.map((i) => columnMeta[i]) } : {}),
      rows: reorderedRows,
      ...(reorderedExportRows ? { exportRows: reorderedExportRows } : {}),
      ...(reorderedMissingCells ? { missingCells: reorderedMissingCells } : {}),
    };
  }

  private sortResultRows(
    result: QueryPanelCachedResult,
    sort: NonNullable<QueryResultExportPayload["sort"]>,
  ): QueryPanelCachedResult {
    const numericSort: SortingFn<Record<string, unknown>> = (a, b, id) =>
      compareExactNumericValues(a.getValue(id), b.getValue(id));
    const table = createTable({
      data: result.rows,
      columns: result.columns.map((_name, index) => {
        const key = colKey(index);
        const category = result.columnMeta?.[index]?.category;
        return {
          id: key,
          accessorKey: key,
          ...(category === "integer" ||
          category === "decimal" ||
          category === "float"
            ? {
                sortingFn: numericSort,
                sortUndefined: false as const,
              }
            : {}),
        };
      }),
      state: { sorting: sort },
      onStateChange: () => {},
      renderFallbackValue: null,
      getCoreRowModel: getCoreRowModel(),
      getSortedRowModel: getSortedRowModel(),
    });
    const sortedRows = table.getRowModel().rows.map(({ original, index }) => ({
      row: original,
      rowIndex: index,
    }));
    const nextRowIndexByPrevious = new Map(
      sortedRows.map(({ rowIndex }, nextRowIndex) => [rowIndex, nextRowIndex]),
    );
    const missingCells = result.missingCells?.flatMap(
      ({ rowIndex, columnIndex }) => {
        const nextRowIndex = nextRowIndexByPrevious.get(rowIndex);
        return nextRowIndex === undefined
          ? []
          : [{ rowIndex: nextRowIndex, columnIndex }];
      },
    );
    return {
      ...result,
      rows: sortedRows.map(({ row }) => row),
      ...(result.exportRows
        ? {
            exportRows: sortedRows.map(
              ({ rowIndex }) => result.exportRows?.[rowIndex] ?? {},
            ),
          }
        : {}),
      ...(missingCells ? { missingCells } : {}),
    };
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

  private async handleWriteClipboard({
    text,
    requestId,
    recipient,
  }: import("../../shared/webviewContracts/table").ClipboardWritePayload): Promise<void> {
    let success = false;
    try {
      await writeClipboardText(text);
      success = true;
    } finally {
      if (requestId && recipient) {
        this.view.postMessage({
          type: "clipboardWritten",
          payload: { requestId, recipient, success },
        });
      }
    }
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
