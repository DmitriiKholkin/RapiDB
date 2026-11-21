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
  type QueryDatabaseScope,
  queryDatabaseTarget,
} from "../../shared/queryDatabaseTarget";
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
  type QueryOperationIdentity,
  type QueryResultExportPayload,
  type QueryRunIdentity,
  type QueryRunOutcome,
  type QueryRunStatus,
  type QueryRunStatusReply,
  type QueryRunStatusRequest,
  type QuerySchemaPayload,
  type QueryTargetBinding,
  type QueryTargetChangeResult,
  sameQueryRun,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type { SchemaObjectEntry } from "../connectionManagerModels";
import { connectionSessionKey } from "../connectionManagerStore";
import { isServerReportedQueryError } from "../dbDrivers/queryErrorOutcome";
import {
  colKey,
  type QueryColumnMeta,
  type QueryJsonExportOverrides,
  type QueryResult,
} from "../dbDrivers/types";
import { readClipboardTextSafe, writeClipboardText } from "../utils/clipboard";
import { redactDiagnosticText } from "../utils/diagnosticRedaction";
import { normalizeUnknownError } from "../utils/errorHandling";
import {
  exportQueryResultsAsCsv,
  exportQueryResultsAsJson,
  type QueryResultExport,
} from "../utils/exportService";
import { logger } from "../utils/logger";
import {
  type FormattedQueryResultSet,
  formatQueryResult,
} from "../utils/queryResultFormatting";
import {
  POSTGRES_QUERY_RESULT_IPC_BYTES,
  queryResultJsonFits,
} from "../utils/queryResultSize";
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
const QUERY_TARGET_CHANGED_MESSAGE =
  "[RapiDB] The query target changed or could not be verified. Review and confirm the target before running again.";
const QUERY_TARGET_CHANGE_PENDING_MESSAGE =
  "[RapiDB] Wait for the selected query target to be verified.";
const QUERY_SESSION_ENDED_MESSAGE =
  "[RapiDB] The connection attempt was interrupted. Run the query again after reconnecting.";
const MAX_TRACKED_QUERY_RUNS = 32;
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

export function currentQueryTargetSessionKey(
  manager: ConnectionManager,
  connectionId: string,
): string | null {
  if (manager.getQueryTargetSessionKey) {
    return manager.getQueryTargetSessionKey(connectionId);
  }
  return (
    connectionSessionKey(
      manager.getConnection(connectionId),
      manager.getConnectionSessionNonce?.(connectionId),
    ) ?? null
  );
}

export interface QueryPanelCachedResult extends QueryJsonExportOverrides {
  columns: string[];
  columnMeta?: QueryColumnMeta[];
  rows: Record<string, unknown>[];
  exportRows?: QueryResultExport["exportRows"];
  serializeJsonExportRow?: QueryResultExport["serializeJsonExportRow"];
  partial?: boolean;
  resultSets?: FormattedQueryResultSet[];
  resultSetsTruncated?: boolean;
  missingCells?: QueryMissingCell[];
  truncated?: boolean;
  truncatedBySize?: boolean;
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
  secretContext?: unknown;
  targetSessionKey?: string;
  operationId?: string;
  cancelRequested?: boolean;
  cancellationPending?: Promise<void>;
  queryStarted: boolean;
  tracked?: TrackedQueryRun;
  completion?: Promise<void>;
  finish?: () => void;
}

interface QueryResultIdentity extends QueryOperationIdentity {
  requestToken: number;
  tracked?: TrackedQueryRun;
}

interface TrackedQueryRun {
  run: QueryRunIdentity;
  status: QueryRunStatus;
}

export class QueryPanelController {
  private schemaRequestToken = 0;

  private connectionChangeToken = 0;
  private requestedConnectionId: string | undefined;
  private targetChangePending = false;

  private queryRequestToken = 0;
  private acceptedQueryRequestToken = 0;
  private readonly pinnedTarget;
  private disposed = false;
  // Keep only metadata, not query text, errors, result rows, or export data.
  private readonly queryRuns = new Map<string, TrackedQueryRun>();

  private readonly activeQueryExecutions = new Map<
    string,
    ActiveQueryExecution
  >();

  constructor(
    private readonly connectionManager: ConnectionManager,
    private readonly view: QueryPanelView,
    private readonly context?: vscode.ExtensionContext,
    private readonly databaseScope?: QueryDatabaseScope,
    private targetBinding?: QueryTargetBinding,
    private readonly panelId?: string,
  ) {
    this.pinnedTarget = queryDatabaseTarget(databaseScope);
  }

  private get pinnedTargetMessage(): string {
    return `[RapiDB] This tab is pinned to its original ${this.pinnedTarget?.label} connection and database. Open a new query to use another target.`;
  }

  private get pinnedUnavailableMessage(): string {
    return `[RapiDB] The ${this.pinnedTarget?.label} connection pinned to this tab is unavailable.`;
  }

  private isPinnedProfileUnavailable(connectionId: string): boolean {
    return (
      this.pinnedTarget !== undefined &&
      this.connectionManager.getConnection(connectionId)?.type !==
        this.pinnedTarget.type
    );
  }

  private resolveConnectionId(connectionIdOverride?: string): string {
    if (this.pinnedTarget) {
      return this.view.getInitialConnectionId();
    }
    if (this.targetBinding) {
      return connectionIdOverride ?? this.targetBinding.connectionId;
    }
    return (
      connectionIdOverride ||
      this.requestedConnectionId ||
      this.view.getActiveConnectionId() ||
      this.view.getInitialConnectionId()
    );
  }

  private rejectsTarget(connectionIdOverride?: string): boolean {
    return (
      this.pinnedTarget !== undefined &&
      !!connectionIdOverride &&
      connectionIdOverride !== this.view.getInitialConnectionId()
    );
  }

  private isValidTarget(
    connectionId: string,
    sessionKey?: string | null,
  ): boolean {
    if (!this.targetBinding) return true;
    return (
      !!sessionKey &&
      this.targetBinding.connectionId === connectionId &&
      this.targetBinding.sessionKey === sessionKey &&
      currentQueryTargetSessionKey(this.connectionManager, connectionId) ===
        sessionKey
    );
  }

  private rejectUnverifiedTarget(
    connectionId: string,
    sessionKey: string | undefined,
    resultIdentity: QueryResultIdentity | undefined,
  ): boolean {
    if (this.isValidTarget(connectionId, sessionKey)) return false;
    this.pushConnections();
    this.postQueryError(
      this.isPinnedProfileUnavailable(connectionId)
        ? this.pinnedUnavailableMessage
        : QUERY_TARGET_CHANGED_MESSAGE,
      undefined,
      resultIdentity,
    );
    return true;
  }

  private publishTargetChange(): void {
    const binding = this.targetBinding;
    if (!binding) return;
    this.view.postMessage({ type: "queryTargetBinding", payload: binding });
  }

  private removeExecution(connectionId: string, requestToken: number): void {
    if (
      this.activeQueryExecutions.get(connectionId)?.requestToken ===
      requestToken
    ) {
      this.activeQueryExecutions.delete(connectionId);
    }
  }

  private retirePendingExecution(execution: ActiveQueryExecution): void {
    execution.cancelRequested = true;
    if (execution.tracked?.status === "accepted")
      execution.tracked.status = "not-run";
    this.removeExecution(execution.connectionId, execution.requestToken);
  }

  async handleMessage(message: unknown): Promise<void> {
    if (this.disposed) return;
    const parsed = parseQueryPanelMessage(message);
    if (!parsed) {
      return;
    }

    switch (parsed.type) {
      case "activeConnectionChanged":
        if (parsed.payload) {
          await this.handleActiveConnectionChanged(
            parsed.payload.connectionId,
            parsed.payload.targetSessionKey,
            parsed.payload.requestId,
          );
        }
        break;
      case "executeQuery":
        if (parsed.payload) {
          await this.handleExecuteQuery(
            parsed.payload.queryText,
            parsed.payload.connectionId,
            parsed.payload.operationId,
            parsed.payload.targetSessionKey,
            parsed.payload.run,
          );
        }
        break;
      case "getQueryRunStatus":
        if (parsed.payload) this.postQueryRunStatus(parsed.payload);
        break;
      case "cancelQuery":
        if (parsed.payload) {
          await this.handleCancelQuery(parsed.payload);
        }
        break;
      case "getConnections": {
        if (this.targetBinding) {
          const initializing = this.connectionManager
            .getConnections()
            .filter(
              ({ id }) =>
                !this.pinnedTarget || id === this.view.getInitialConnectionId(),
            )
            .flatMap(({ id }) => {
              const pending =
                this.connectionManager.initializeConnectionCredentialRevision?.(
                  id,
                );
              return pending ? [pending] : [];
            });
          if (initializing.length > 0) await Promise.all(initializing);
        }
        this.publishTargetChange();
        this.pushConnections();
        break;
      }
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
    // A pinned panel loads and awaits its own database scope. An aggregate
    // Explorer event may be for any database under this connection.
    if (this.pinnedTarget) return;
    if (!this.isValidTarget(connectionId, this.targetBinding?.sessionKey))
      return;
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
    sessionKey?: string,
    requestId?: string,
  ): Promise<void> {
    if (this.targetBinding) {
      const changeToken = ++this.connectionChangeToken;
      // Message handlers overlap while credentials are being read.
      this.targetChangePending = true;
      let verified = false;
      try {
        if (sessionKey && !this.rejectsTarget(connectionId)) {
          const initializing =
            this.connectionManager.initializeConnectionCredentialRevision?.(
              connectionId,
            );
          verified = !initializing || (await initializing);
          if (
            verified &&
            this.connectionManager.getConnection(connectionId)?.type ===
              "dynamodb"
          ) {
            verified = !!(await this.connectionManager.getDriverForQuery(
              connectionId,
              sessionKey,
            ));
          }
        }
      } catch (error: unknown) {
        logger.error(
          "Failed to verify query target",
          error,
          this.connectionManager.getConnectionDiagnosticContext?.(connectionId),
        );
      }
      if (changeToken !== this.connectionChangeToken) return;

      const nextBinding =
        verified &&
        sessionKey &&
        !this.isPinnedProfileUnavailable(connectionId) &&
        currentQueryTargetSessionKey(this.connectionManager, connectionId) ===
          sessionKey
          ? { connectionId, sessionKey }
          : undefined;
      const changed =
        nextBinding &&
        (this.targetBinding.connectionId !== nextBinding.connectionId ||
          this.targetBinding.sessionKey !== nextBinding.sessionKey);
      if (changed) {
        // Install the verified revision before acknowledging it or awaiting cancellation.
        this.targetBinding = nextBinding;
        ++this.schemaRequestToken;
        this.queryRequestToken = 0;
        this.acceptedQueryRequestToken = 0;
        this.view.setActiveConnectionId(connectionId);
        this.view.setLastQueryResult(null);
        this.view.syncTitle();
      }
      this.targetChangePending = false;
      this.pushConnections();
      if (requestId) {
        this.view.postMessage({
          type: "queryTargetChangeResult",
          payload: {
            requestId,
            accepted: !!nextBinding,
            binding: this.targetBinding,
          } satisfies QueryTargetChangeResult,
        });
      }
      if (changed) {
        // A document reloaded during verification won't know the request ID.
        this.publishTargetChange();
        void this.pushSchema(connectionId);
        await this.cancelOwnedQueryExecutions("superseded");
      }
      return;
    }
    if (this.pinnedTarget) return;
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

  private matchesRunPanel(run: QueryRunIdentity): boolean {
    const target = queryDatabaseTarget(this.databaseScope);
    return (
      run.panelId === this.panelId &&
      run.postgresDatabase ===
        (target?.type === "pg" ? target.database : undefined) &&
      run.mssqlDatabase ===
        (target?.type === "mssql" ? target.database : undefined)
    );
  }

  private postQueryRunStatus(request: QueryRunStatusRequest): void {
    const tracked = this.queryRuns.get(request.run.operationId);
    let status: QueryRunStatus = "unknown";
    if (tracked && sameQueryRun(tracked.run, request.run)) {
      const { connectionId, targetSessionKey } = tracked.run;
      // Terminal summaries belong to the old target; a live operation for a
      // retired target is no longer controllable from this panel.
      if (
        (tracked.status !== "accepted" && tracked.status !== "running") ||
        this.isValidTarget(connectionId, targetSessionKey)
      )
        status = tracked.status;
    }
    this.view.postMessage({
      type: "queryRunStatus",
      payload: {
        ...request,
        status,
      } satisfies QueryRunStatusReply,
    });
  }

  private async handleExecuteQuery(
    queryText: string,
    connectionId?: string,
    operationId?: string,
    targetSessionKey?: string,
    run?: QueryRunIdentity,
  ): Promise<void> {
    if (operationId && this.queryRuns.has(operationId)) return;
    if (!run) {
      // Older documents do not send or recover a tracked identity.
      await this.executeQuery(
        queryText,
        connectionId,
        operationId,
        targetSessionKey,
      );
      return;
    }
    if (!this.matchesRunPanel(run)) return;

    const tracked: TrackedQueryRun = { run, status: "accepted" };
    if (this.queryRuns.size >= MAX_TRACKED_QUERY_RUNS) {
      for (const [id, entry] of this.queryRuns) {
        if (entry.status === "accepted" || entry.status === "running") continue;
        this.queryRuns.delete(id);
        break;
      }
    }
    if (this.queryRuns.size >= MAX_TRACKED_QUERY_RUNS) {
      this.postQueryError(
        "[RapiDB] Too many unresolved queries in this panel.",
        undefined,
        {
          connectionId: run.connectionId,
          operationId: run.operationId,
          requestToken: ++nextQueryRequestToken,
          tracked,
        },
      );
      return;
    }
    this.queryRuns.set(run.operationId, tracked);
    try {
      await this.executeQuery(
        queryText,
        connectionId,
        operationId,
        targetSessionKey,
        tracked,
      );
    } finally {
      // Supersede/retirement can suppress a reply; never leave a stale "running".
      if (tracked.status === "accepted") tracked.status = "not-run";
      else if (tracked.status === "running") tracked.status = "unknown";
    }
  }

  private async executeQuery(
    queryText: string,
    connectionIdOverride?: string,
    operationId?: string,
    targetSessionKey?: string,
    tracked?: TrackedQueryRun,
  ): Promise<void> {
    if (!queryText.trim()) {
      return;
    }

    const connectionId = this.resolveConnectionId(connectionIdOverride);
    if (this.rejectsTarget(connectionIdOverride)) {
      this.postQueryError(
        this.pinnedTargetMessage,
        undefined,
        operationId
          ? {
              connectionId: connectionIdOverride || connectionId,
              operationId,
              requestToken: ++nextQueryRequestToken,
              tracked,
            }
          : undefined,
      );
      return;
    }
    const requestToken = ++nextQueryRequestToken;
    const resultIdentity = operationId
      ? { connectionId, operationId, requestToken, tracked }
      : undefined;
    const rejectIfTargetChanged = () =>
      this.rejectUnverifiedTarget(
        connectionId,
        targetSessionKey,
        resultIdentity,
      );
    const rejectIfTargetChangePending = () => {
      if (!this.targetChangePending) return false;
      this.postQueryError(
        QUERY_TARGET_CHANGE_PENDING_MESSAGE,
        undefined,
        resultIdentity,
      );
      return true;
    };
    if (rejectIfTargetChangePending() || rejectIfTargetChanged()) return;
    this.queryRequestToken = requestToken;
    const canProceed = await this.cancelSupersededQueryExecution(
      connectionId,
      requestToken,
    );
    if (!this.isCurrentQueryRequest(requestToken)) return;
    if (rejectIfTargetChangePending()) {
      this.queryRequestToken = this.acceptedQueryRequestToken;
      return;
    }
    if (rejectIfTargetChanged()) return;
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
    if (this.isPinnedProfileUnavailable(connectionId)) {
      this.postQueryError(
        this.pinnedUnavailableMessage,
        requestToken,
        resultIdentity,
      );
      return;
    }
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
      targetSessionKey,
      tracked,
      supportsCancellation: false,
      queryStarted: false,
      cancel: async () => undefined,
    };
    this.activeQueryExecutions.set(connectionId, pendingExecution);

    let driver: ReturnType<ConnectionManager["getDriver"]>;
    try {
      if (this.targetBinding && targetSessionKey) {
        driver = await this.connectionManager.getDriverForQuery(
          connectionId,
          targetSessionKey,
        );
      } else {
        if (!this.connectionManager.isConnected(connectionId)) {
          await this.connectionManager.connectTo(connectionId, "automatic");
        }
        driver = this.connectionManager.getDriver(connectionId);
      }
    } catch (error: unknown) {
      this.removeExecution(connectionId, requestToken);
      if (!this.isCurrentQueryRequest(requestToken)) return;
      if (rejectIfTargetChanged() || pendingExecution.cancelRequested) return;
      const normalized = normalizeUnknownError(
        error,
        this.connectionManager.getConnectionDiagnosticContext?.(connectionId) ??
          connection,
      );
      this.postQueryError(
        `Cannot connect: ${normalized.message}`,
        requestToken,
        resultIdentity,
      );
      return;
    }

    if (!this.isCurrentQueryRequest(requestToken)) {
      this.removeExecution(connectionId, requestToken);
      return;
    }

    if (
      rejectIfTargetChangePending() ||
      rejectIfTargetChanged() ||
      pendingExecution.cancelRequested
    ) {
      this.removeExecution(connectionId, requestToken);
      return;
    }

    if (
      !driver ||
      (this.targetBinding &&
        this.connectionManager.getDriver(connectionId) !== driver)
    ) {
      this.removeExecution(connectionId, requestToken);
      this.postQueryError(
        this.targetBinding
          ? QUERY_SESSION_ENDED_MESSAGE
          : `[RapiDB] Cannot execute query: driver is unavailable for ${connectionId}.`,
        requestToken,
        resultIdentity,
      );
      return;
    }

    const executionConnection =
      this.connectionManager.getConnection(connectionId);
    const secretContext =
      this.connectionManager.getConnectionDiagnosticContext?.(
        connectionId,
        driver,
      ) ?? executionConnection;
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
      this.removeExecution(connectionId, requestToken);
      this.postQueryError(executionBlock, requestToken, resultIdentity);
      return;
    }

    const execution = this.createQueryExecutionHandle(
      connectionId,
      requestToken,
      driver,
      operationId,
      secretContext,
      targetSessionKey,
    );
    this.activeQueryExecutions.set(connectionId, execution);

    if (!this.isCurrentQueryRequest(requestToken)) {
      execution.finish?.();
      this.removeExecution(connectionId, requestToken);
      return;
    }

    void this.connectionManager
      .addToHistory(
        connectionId,
        queryText,
        this.databaseScope,
        this.targetBinding?.sessionKey ?? undefined,
      )
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
      if (tracked) tracked.status = "running";
      const result = await driver.query(cappedQueryText, undefined, {
        requestToken,
        ...(this.pinnedTarget ? { database: this.pinnedTarget.database } : {}),
        ...(driverBounded ? { hardCap: hardCapProbeLimit } : {}),
        ...(executionConnection?.readOnly === true ? { readOnly: true } : {}),
      });
      const activeExecution = this.activeQueryExecutions.get(connectionId);
      if (activeExecution?.requestToken === requestToken) {
        await activeExecution.cancellationPending;
      }
      // Even if the view superseded this run, a driver response without an
      // error is enough to record completion, without retaining its rows. An
      // error the database itself reported on a live session (a typo, a
      // constraint violation after earlier statements) is also a known,
      // finished outcome; transport, timeout and cancellation stay unknown.
      if (tracked)
        tracked.status =
          result.error === undefined ||
          (result.serverReportedError === true &&
            !activeExecution?.cancelRequested &&
            driver.isConnected?.() !== false)
            ? "completed"
            : "unknown";
      if (!this.isCurrentQueryRequest(requestToken)) {
        return;
      }
      if (rejectIfTargetChanged()) return;
      const formattedResult = formatQueryResult(result, effectiveRowLimit);

      this.view.setLastQueryResult({
        columns: formattedResult.columns,
        columnMeta: formattedResult.columnMeta,
        rows: formattedResult.rows,
        ...(formattedResult.exportRows
          ? { exportRows: formattedResult.exportRows }
          : {}),
        ...(formattedResult.jsonExportCellOverrides
          ? { jsonExportCellOverrides: formattedResult.jsonExportCellOverrides }
          : {}),
        ...(formattedResult.serializeJsonExportRow
          ? { serializeJsonExportRow: formattedResult.serializeJsonExportRow }
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
        ...(formattedResult.truncatedBySize ? { truncatedBySize: true } : {}),
        truncatedAt: formattedResult.truncatedAt,
      });
      const webviewResult = { ...formattedResult };
      delete webviewResult.serverReportedError;
      delete webviewResult.exportRows;
      delete webviewResult.serializeJsonExportRow;
      delete webviewResult.jsonExportCellOverrides;
      webviewResult.resultSets = webviewResult.resultSets?.map((resultSet) => {
        if (!resultSet.jsonExportCellOverrides) return resultSet;
        const displayResultSet = { ...resultSet };
        delete displayResultSet.jsonExportCellOverrides;
        return displayResultSet;
      });
      if (connectionType === "pg" && webviewResult.resultSets?.length) {
        // The grid reads these from the selected result set. Keep the required
        // top-level keys, but don't serialize a second copy of the first set.
        webviewResult.columns = [];
        webviewResult.columnMeta = [];
        webviewResult.rows = [];
      }
      this.postQueryResult(
        {
          ...webviewResult,
          ...(webviewResult.error !== undefined && {
            error: redactDiagnosticText(webviewResult.error, secretContext),
          }),
          ...(webviewResult.warnings !== undefined && {
            warnings: webviewResult.warnings.map((warning) =>
              redactDiagnosticText(warning, secretContext),
            ),
          }),
        },
        resultIdentity,
        tracked?.status === "unknown" ? "unknown" : "completed",
        connectionType === "pg",
      );
    } catch (error: unknown) {
      const activeExecution = this.activeQueryExecutions.get(connectionId);
      if (activeExecution?.requestToken === requestToken) {
        await activeExecution.cancellationPending;
      }
      if (!this.isCurrentQueryRequest(requestToken)) return;
      if (rejectIfTargetChanged()) return;
      const normalized = normalizeUnknownError(error, secretContext);
      if (
        activeExecution?.requestToken === requestToken &&
        activeExecution.cancelRequested
      ) {
        const isRedisClientDisconnect =
          connection?.type === "redis" &&
          /^disconnects client\.?$/i.test(
            normalizeUnknownError(error, connection).message.trim(),
          );
        const cancellationMessage = isRedisClientDisconnect
          ? "[RapiDB] Query cancellation was requested. The Redis query client was disconnected to stop the active request. The operation outcome may be unknown; cancellation does not confirm rollback."
          : `[RapiDB] Query failed after a cancellation request. ${normalized.message} The operation outcome may be unknown; cancellation does not confirm rollback.`;
        this.postQueryError(
          cancellationMessage,
          requestToken,
          resultIdentity,
          connectionType === "pg",
        );
        return;
      }
      this.postQueryError(
        normalized.message,
        requestToken,
        resultIdentity,
        connectionType === "pg",
        isServerReportedQueryError(error) && driver.isConnected?.() !== false,
      );
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
          logger.error(
            "Failed to retire a lost database session",
            error,
            secretContext,
          );
        }
      }
      if (
        queryStarted &&
        changesDiscoverableSchema &&
        (!this.targetBinding ||
          (currentQueryTargetSessionKey(
            this.connectionManager,
            connectionId,
          ) === targetSessionKey &&
            this.connectionManager.getDriver(connectionId) === driver))
      ) {
        this.connectionManager.refreshSchemaCache(connectionId);
      }
      execution.finish?.();
      this.removeExecution(connectionId, requestToken);
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
    secretContext?: unknown,
    targetSessionKey?: string,
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
      secretContext,
      targetSessionKey,
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

  private async handleCancelQuery(
    identity: QueryOperationIdentity & { run?: QueryRunIdentity },
  ): Promise<void> {
    const { connectionId, operationId, run } = identity;
    const execution = this.activeQueryExecutions.get(connectionId);
    const tracked = this.queryRuns.get(operationId);
    if (
      !execution ||
      execution.operationId !== operationId ||
      (run ? !tracked || !sameQueryRun(run, tracked.run) : !!tracked) ||
      !this.isCurrentQueryRequest(execution.requestToken) ||
      !this.isValidTarget(connectionId, execution.targetSessionKey)
    ) {
      return;
    }
    if (!execution.queryStarted) {
      this.retirePendingExecution(execution);
      this.postQueryError("[RapiDB] Query cancelled.", execution.requestToken, {
        connectionId,
        operationId,
        requestToken: execution.requestToken,
        tracked,
      });
      if (this.queryRequestToken === execution.requestToken) {
        this.queryRequestToken = ++nextQueryRequestToken;
      }
      if (this.acceptedQueryRequestToken === execution.requestToken) {
        this.acceptedQueryRequestToken = 0;
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
      logger.error(
        "Failed to cancel query execution",
        error,
        execution.secretContext,
      );
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
      this.retirePendingExecution(previous);
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
      logger.error(
        "Failed to cancel superseded query execution",
        error,
        previous.secretContext,
      );
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
          this.retirePendingExecution(execution);
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
          logger.error(
            "Failed to cancel owned query execution",
            error,
            execution.secretContext,
          );
        } finally {
          if (timeoutHandle) {
            clearTimeout(timeoutHandle);
          }
        }
      }),
    );
  }

  async dispose(): Promise<void> {
    this.disposed = true;
    this.queryRuns.clear();
    ++this.connectionChangeToken;
    this.queryRequestToken = 0;
    this.acceptedQueryRequestToken = 0;
    await this.cancelOwnedQueryExecutions("lifecycle_shutdown");
  }

  private pushConnections(): void {
    const connections = this.connectionManager
      .getConnections()
      .filter(
        (connection) =>
          !this.pinnedTarget ||
          (connection.id === this.view.getInitialConnectionId() &&
            connection.type === this.pinnedTarget.type),
      )
      .map((connection) => ({
        id: connection.id,
        name: connection.name,
        type: connection.type,
        ...(this.targetBinding
          ? {
              targetSessionKey: currentQueryTargetSessionKey(
                this.connectionManager,
                connection.id,
              ),
            }
          : {}),
        editorPresentation: this.connectionManager.getQueryEditorPresentation(
          connection.id,
        ),
      }));
    if (this.targetBinding) {
      // Retire operations for replaced targets without making the new target
      // wait for the old driver. Any late callback still has its own token.
      for (const [id, execution] of this.activeQueryExecutions) {
        if (
          currentQueryTargetSessionKey(this.connectionManager, id) !==
          execution.targetSessionKey
        ) {
          if (execution.queryStarted) this.activeQueryExecutions.delete(id);
          else this.retirePendingExecution(execution);
        }
      }
      if (
        !this.isValidTarget(
          this.targetBinding.connectionId,
          this.targetBinding.sessionKey,
        )
      ) {
        this.view.setLastQueryResult(null);
      }
    }
    this.view.postMessage({ type: "connections", payload: connections });
  }

  private async pushSchema(connectionIdOverride?: string): Promise<void> {
    if (this.rejectsTarget(connectionIdOverride)) return;
    const requestToken = ++this.schemaRequestToken;
    const connectionId = this.resolveConnectionId(connectionIdOverride);
    const targetSessionKey = this.targetBinding?.sessionKey;

    const unavailable = this.isPinnedProfileUnavailable(connectionId)
      ? this.pinnedUnavailableMessage
      : !this.isValidTarget(connectionId, targetSessionKey)
        ? QUERY_TARGET_CHANGED_MESSAGE
        : !this.connectionManager.isConnected(connectionId)
          ? "Not connected."
          : undefined;
    if (unavailable) {
      if (!this.isCurrentSchemaRequest(requestToken)) {
        return;
      }
      this.view.postMessage({
        type: "schema",
        payload: {
          connectionId,
          schema: [],
          error: unavailable,
        } satisfies QuerySchemaPayload<SchemaObjectEntry>,
      });
      return;
    }

    const secretContext =
      this.connectionManager.getConnectionDiagnosticContext?.(connectionId);
    try {
      const schema = !this.pinnedTarget
        ? await this.connectionManager.getSchemaAsync(connectionId)
        : await this.connectionManager.getSchemaAsync(
            connectionId,
            this.pinnedTarget.database,
          );
      if (
        !this.isCurrentSchemaRequest(requestToken) ||
        !this.isValidTarget(connectionId, targetSessionKey)
      ) {
        return;
      }
      this.view.postMessage({
        type: "schema",
        payload: this.buildSchemaPayload(connectionId, schema),
      });
    } catch (error: unknown) {
      if (
        !this.isCurrentSchemaRequest(requestToken) ||
        !this.isValidTarget(connectionId, targetSessionKey)
      ) {
        return;
      }
      const normalized = logger.error(
        "Failed to load schema",
        error,
        secretContext,
      );
      this.view.postMessage({
        type: "schema",
        payload: {
          connectionId,
          schema: [],
          error: normalized.message,
        } satisfies QuerySchemaPayload<SchemaObjectEntry>,
      });
    }
  }

  private buildSchemaPayload(
    connectionId: string,
    schema: SchemaObjectEntry[],
  ): QuerySchemaPayload<SchemaObjectEntry> {
    const schemaState = !this.pinnedTarget
      ? this.connectionManager.getSchemaSnapshotState(connectionId)
      : this.connectionManager.getSchemaSnapshotState(connectionId, {
          kind: "database",
          database: this.pinnedTarget.database,
        });
    return {
      connectionId,
      schema,
      ...(schemaState.status === "error"
        ? {
            error: redactDiagnosticText(
              schemaState.error ?? "Schema loading failed.",
              this.connectionManager.getConnectionDiagnosticContext?.(
                connectionId,
              ),
            ),
          }
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
    identity?: QueryResultIdentity,
    limitPostgres = false,
    reportedByServer = false,
  ): void {
    if (
      requestToken !== undefined &&
      !this.isCurrentQueryRequest(requestToken)
    ) {
      return;
    }
    const status = identity?.tracked?.status;
    this.postQueryResult(
      {
        columns: [],
        columnMeta: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
        error,
      },
      identity,
      status === "completed" || (status === "running" && reportedByServer)
        ? "completed"
        : status === "running" || status === "unknown"
          ? "unknown"
          : "not-run",
      limitPostgres,
    );
  }

  private postQueryResult(
    payload: QueryResult,
    identity: QueryResultIdentity | undefined,
    outcome: QueryRunOutcome,
    limitPostgres = false,
  ): void {
    if (this.disposed) return;
    const { tracked, ...operation } = identity ?? {};
    if (tracked) tracked.status = outcome;
    const envelope = (data: QueryResult) => ({
      type: "queryResult",
      payload: {
        ...data,
        ...operation,
        ...(tracked ? { run: tracked.run, runOutcome: outcome } : {}),
      },
    });
    const message = envelope(payload);
    if (
      !limitPostgres ||
      queryResultJsonFits(message, POSTGRES_QUERY_RESULT_IPC_BYTES)
    ) {
      this.view.postMessage(message);
      return;
    }
    // Last-resort guard includes the full message (metadata, diagnostics and
    // identity). A presentation failure cannot change a completed SQL run.
    this.view.setLastQueryResult(null);
    const summary = envelope({
      columns: [],
      columnMeta: [],
      rows: [],
      executionTimeMs: payload.executionTimeMs,
      rowCount: payload.rowCount,
      affectedRows: payload.affectedRows,
      rowCountKind: payload.rowCountKind,
      ...(payload.error !== undefined
        ? {
            error:
              "PostgreSQL reported an error, but the result or diagnostic exceeded the display transfer limit. Statements may have executed; verify before retrying.",
          }
        : {
            resultSetsTruncated: true,
            warnings: [
              "PostgreSQL finished executing, but the result exceeded the display transfer limit. No rows are available to export from this run.",
            ],
          }),
    });
    if (queryResultJsonFits(summary, POSTGRES_QUERY_RESULT_IPC_BYTES))
      this.view.postMessage(summary);
  }

  private async handleExportResults(
    format: "csv" | "json",
    columnOrder?: string[],
    sort?: QueryResultExportPayload["sort"],
    resultSetIndex?: number,
  ): Promise<void> {
    const binding = this.targetBinding;
    if (
      binding &&
      !this.isValidTarget(binding.connectionId, binding.sessionKey)
    )
      return;
    const originalCache = this.view.getLastQueryResult();
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
          `This query result was truncated${cached.truncatedBySize ? " by the PostgreSQL display size limit" : ""}. The export will contain only the first ${displayedRowCount.toLocaleString()} displayed ${displayedRowCount === 1 ? "row" : "rows"}, not the full result.`,
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
      if (
        binding &&
        (this.targetBinding !== binding ||
          !this.isValidTarget(binding.connectionId, binding.sessionKey) ||
          this.view.getLastQueryResult() !== originalCache)
      ) {
        return;
      }
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
    const reorderExportRows = <T>(
      rows?: readonly (Readonly<Record<string, T>> | undefined)[],
    ) =>
      rows?.map((row) => {
        if (row === undefined) return undefined;
        const newRow: Record<string, T> = {};
        for (let index = 0; index < indices.length; index++) {
          const sourceKey = colKey(indices[index]);
          if (Object.hasOwn(row, sourceKey)) {
            newRow[colKey(index)] = row[sourceKey];
          }
        }
        return newRow;
      });
    const reorderedExportRows = reorderExportRows(result.exportRows);
    const reorderedJsonOverrides = reorderExportRows(
      result.jsonExportCellOverrides,
    );
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
      ...(reorderedJsonOverrides
        ? { jsonExportCellOverrides: reorderedJsonOverrides }
        : {}),
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
              ({ rowIndex }) => result.exportRows?.[rowIndex],
            ),
          }
        : {}),
      ...(result.jsonExportCellOverrides
        ? {
            jsonExportCellOverrides: sortedRows.map(
              ({ rowIndex }) => result.jsonExportCellOverrides?.[rowIndex],
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
    targetSessionKey,
    requestId,
  }: QueryBookmarkPayload): Promise<void> {
    if (!queryText?.trim()) {
      return;
    }

    const connectionId = this.resolveConnectionId(connectionIdOverride);

    try {
      if (this.targetChangePending) {
        throw new Error(QUERY_TARGET_CHANGE_PENDING_MESSAGE);
      }
      if (this.rejectsTarget(connectionIdOverride)) {
        throw new Error(this.pinnedTargetMessage);
      }
      if (this.isPinnedProfileUnavailable(connectionId)) {
        throw new Error(this.pinnedUnavailableMessage);
      }
      if (!this.isValidTarget(connectionId, targetSessionKey)) {
        this.pushConnections();
        throw new Error(QUERY_TARGET_CHANGED_MESSAGE);
      }
      if (
        targetSessionKey &&
        this.connectionManager.getConnection(connectionId)?.type ===
          "dynamodb" &&
        (!(await this.connectionManager.getDriverForQuery(
          connectionId,
          targetSessionKey,
        )) ||
          this.targetChangePending ||
          !this.isValidTarget(connectionId, targetSessionKey))
      ) {
        this.pushConnections();
        throw new Error(QUERY_TARGET_CHANGED_MESSAGE);
      }
      await this.connectionManager.addBookmark(
        connectionId,
        queryText,
        this.databaseScope,
        this.targetBinding?.sessionKey ?? undefined,
      );
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
