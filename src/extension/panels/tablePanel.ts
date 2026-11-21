import { randomUUID } from "node:crypto";
import * as vscode from "vscode";
import { getDbObjectKindDisplayLabel } from "../../shared/dbObjectKinds";
import { observedNoSqlDeleteBackend } from "../../shared/noSqlDeleteObservation";
import type { SqlDeleteObservation } from "../../shared/sqlDeleteObservation";
import { isSQLitePrimaryKeyValue } from "../../shared/sqliteRowIdentity";
import { coerceFilterExpressions } from "../../shared/tableTypes";
import {
  type ApplyResultPayload,
  type DeleteResultPayload,
  parseTablePanelMessage,
  type TableInitPayload,
  type TableMutationPreviewPayload,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { connectionSessionKey } from "../connectionManagerStore";
import {
  buildDeleteResult,
  unattemptedDeleteResult,
} from "../dbDrivers/deleteOutcomes";
import {
  redisKeyspaceDisplayName,
  redisKeyspaceFileName,
} from "../dbDrivers/redisKeyspace";
import { isSqlDeleteBackend } from "../dbDrivers/sqlDeleteSnapshot";
import {
  SQLITE_ATTACHMENT_CHANGED,
  type SQLiteAttachmentTarget,
  sqliteAttachmentSchema,
} from "../dbDrivers/sqliteAttachmentTarget";
import type {
  ColumnTypeMeta,
  DriverTableExportChunk,
  DriverTableExportFormat,
  FilterExpression,
  NoSqlDeleteObservation,
} from "../dbDrivers/types";
import {
  applyRowTypeHints,
  buildPrevalidationFailedResult,
  PersistedEditValidationError,
  validatePersistedEditRecord,
} from "../table/persistedEditValidation";
import type { TablePage } from "../table/tableDataContracts";
import { getAtomicApplyRefusal } from "../table/tableMutationExecution";
import {
  prepareApplyChangesPlan,
  type SortConfig,
  TableDataService,
} from "../tableDataService";
import { readClipboardTextSafe, writeClipboardText } from "../utils/clipboard";
import { redactDiagnosticText } from "../utils/diagnosticRedaction";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import {
  type ChunkedExportData,
  exportTableDataAsCsv,
  exportTableDataAsJson,
} from "../utils/exportService";
import {
  attachConnectionScopedPanelLifecycle,
  attachPanelMessageHandler,
  disposePanelInstances,
} from "./panelLifecycle";
import { createPanelWebviewOptions } from "./panelRetentionPolicy";
import { TableMutationPreviewController } from "./tableMutationPreviewController";
import {
  APP_WEBVIEW_SHELL_LAYOUT,
  createWebviewShell,
  WEBVIEW_SCROLLBAR_STYLES,
} from "./webviewShell";

const EXPORT_CHUNK_SIZE = 500;
const TABLE_PANEL_RETENTION_MODE = "retain" as const;
const MUTATION_SESSION_CONFLICT =
  "Connection session changed or is not connected. The previous mutation preview cannot be applied; create a new preview before retrying.";
const MUTATION_SCHEMA_CONFLICT =
  "Schema metadata changed. The previous mutation preview cannot be applied; pending work is retained. Review the schema conflict before retrying.";

function assertSQLitePanelIdentities(keys: Record<string, unknown>[]): void {
  if (keys.some((row) => !Object.values(row).every(isSQLitePrimaryKeyValue))) {
    throw new Error(
      "SQLite row identity is missing or invalid. Refresh the table and retry.",
    );
  }
}

function redactMutationDiagnostics(
  payload: ApplyResultPayload | DeleteResultPayload,
  secretContext: unknown,
) {
  let changed = false;
  const redact = (text: string): string => {
    const safe = redactDiagnosticText(text, secretContext);
    changed ||= safe !== text;
    return safe;
  };
  const redactOutcome = <T extends { message?: string }>(outcome: T): T => {
    if (outcome.message === undefined) return outcome;
    const message = redact(outcome.message);
    return message === outcome.message ? outcome : { ...outcome, message };
  };
  // Do not redact the full payload: row identities, columns, and status
  // fields are used for reconciliation and are not diagnostic text.
  const safe = {
    ...payload,
    ...(payload.error !== undefined && {
      error: redact(payload.error),
    }),
    ...("warning" in payload &&
      payload.warning !== undefined && {
        warning: redact(payload.warning),
      }),
    ...(payload.rowOutcomes && {
      rowOutcomes: payload.rowOutcomes.map(redactOutcome),
    }),
    ...("insertRowOutcomes" in payload &&
      payload.insertRowOutcomes && {
        insertRowOutcomes: payload.insertRowOutcomes.map(redactOutcome),
      }),
  };
  return changed ? safe : payload;
}

type NotExecutedOutcome = { changesPossible: false; outcomeUnknown: false };

type NotExecutedMutationResult =
  | {
      type: "applyResult" | "insertResult";
      payload: ApplyResultPayload & NotExecutedOutcome;
    }
  | {
      type: "deleteResult";
      payload: DeleteResultPayload & NotExecutedOutcome;
    };

function notExecutedMutationResult(
  operationId: string,
  kind: TableMutationPreviewPayload["kind"],
  error: string,
): NotExecutedMutationResult {
  const payload = {
    operationId,
    success: false,
    error,
    changesPossible: false,
    outcomeUnknown: false,
  } as const;
  return kind === "deleteRows"
    ? {
        type: "deleteResult",
        payload: { ...payload, affectedRows: 0, rowOutcomes: [] },
      }
    : {
        type: kind === "applyChanges" ? "applyResult" : "insertResult",
        payload,
      };
}

type TablePanelObjectKind = "table" | "view" | "materializedView";

interface TablePanelSerializedState {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  isView?: boolean;
  objectKind?: TablePanelObjectKind;
  connectionSessionKey?: string;
}

function isTablePanelSerialization(
  value: unknown,
): value is TablePanelSerializedState {
  if (!value || typeof value !== "object") return false;
  const state = value as Record<string, unknown>;
  return (
    typeof state.connectionId === "string" &&
    typeof state.database === "string" &&
    typeof state.schema === "string" &&
    typeof state.table === "string" &&
    (state.isView === undefined || typeof state.isView === "boolean") &&
    (state.objectKind === undefined ||
      state.objectKind === "table" ||
      state.objectKind === "view" ||
      state.objectKind === "materializedView") &&
    (state.connectionSessionKey === undefined ||
      typeof state.connectionSessionKey === "string")
  );
}

function currentConnectionSessionKey(
  connectionManager: ConnectionManager,
  connectionId: string,
): string | null | undefined {
  try {
    if (
      connectionManager.getConnection(connectionId)?.type === "dynamodb" &&
      connectionManager.getQueryTargetSessionKey
    ) {
      return connectionManager.getQueryTargetSessionKey(connectionId);
    }
    return connectionSessionKey(
      connectionManager.getConnection(connectionId),
      connectionManager.getConnectionSessionNonce?.(connectionId),
    );
  } catch {
    return null;
  }
}

function titleObjectKindLabel(
  connectionType: string | undefined,
  objectKind: TablePanelObjectKind,
): string {
  return getDbObjectKindDisplayLabel(connectionType, objectKind);
}

function shouldShowSchemaPrefix(connectionType: string | undefined): boolean {
  return (
    connectionType !== "mongodb" &&
    connectionType !== "dynamodb" &&
    connectionType !== "redis"
  );
}

function tableDisplayName(
  connectionType: string | undefined,
  table: string,
): string {
  return connectionType === "redis" ? redisKeyspaceDisplayName(table) : table;
}

function tableFileName(
  connectionType: string | undefined,
  table: string,
): string {
  return connectionType === "redis" ? redisKeyspaceFileName(table) : table;
}

type ExportPayload = {
  sort?: SortConfig | null;
  filters?: unknown[];
  limitToPage?: { page: number; pageSize: number };
  columnOrder?: string[];
};

interface TableSchemaSnapshot {
  readonly generation: number;
  readonly connectionGeneration: number;
  readonly columns: ColumnTypeMeta[];
  readonly signature: string;
}

export class TablePanel {
  private static readonly viewType = "rapidb.tablePanel";

  private static panels = new Map<string, TablePanel>();
  private static readonly instances = new Set<TablePanel>();

  private readonly panel: vscode.WebviewPanel;
  private readonly key: string;
  private readonly context: vscode.ExtensionContext;
  private readonly svc: TableDataService;
  private readonly connectionManager: ConnectionManager;
  private readonly connectionId: string;
  private readonly database: string;
  private readonly schema: string;
  private readonly table: string;
  private readonly isView: boolean;
  private readonly objectKind: TablePanelObjectKind | undefined;
  private sessionKey: string | null | undefined;
  private readonly initiallyBoundDynamoTarget: boolean;
  private sessionGeneration: number;
  private readonly previewController: TableMutationPreviewController;
  private disposed = false;
  private hasUnsavedChanges: boolean;
  private webviewHasDeleteRecovery = false;
  private readonly deleteOperations = new Map<
    string,
    "executing" | "replying" | "recovery"
  >();
  private messageEpoch = 0;
  private webviewReady = false;
  private initialized = false;
  private initializationPromise: Promise<void> | undefined;
  private initializationAttempt: { waitingForConnect: boolean } | undefined;
  private connectionGeneration = 0;
  private metadataInvalidated = false;
  private pageReadAbortController = new AbortController();
  private browsingSnapshot?: { key: string; controller: AbortController };
  private readonly inFlightPageRequests = new Map<string, Promise<TablePage>>();
  // Only opaque handles cross the webview boundary. Keep the current and
  // preceding page replies so a click racing a new reply keeps its own proof.
  private readonly noSqlDeletePages: Array<
    Map<string, NoSqlDeleteObservation>
  > = [];
  // Export may encounter a selected sparse attribute only in a later chunk.
  // Keep its display descriptor separate from the schema used for mutations.
  private readonly dynamoExportColumns = new Map<string, ColumnTypeMeta>();
  private sqliteAttachmentTarget?: SQLiteAttachmentTarget;

  private schemaSnapshot: TableSchemaSnapshot = {
    generation: 0,
    connectionGeneration: 0,
    columns: [],
    signature: "[]",
  };
  private schemaRefreshGeneration = 0;
  private schemaRefreshPromise: Promise<void> = Promise.resolve();
  private schemaRefreshError: string | null = null;
  private readonly previewSchemas = new Map<
    string,
    {
      snapshot: TableSchemaSnapshot;
      operationId: string;
      kind: TableMutationPreviewPayload["kind"];
    }
  >();
  private readonly previewOperations = new Map<
    string,
    { kind: TableMutationPreviewPayload["kind"]; executed: boolean }
  >();
  private readonly confirmingPreviewOperationIds = new Set<string>();

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    isView = false,
    objectKind?: TablePanelObjectKind,
    restoredDraft = false,
  ) {
    this.panel = panel;
    this.key = TablePanel.panelKey(connectionId, database, schema, table);
    this.context = context;
    this.svc = new TableDataService(connectionManager);
    this.connectionManager = connectionManager;
    this.connectionId = connectionId;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.isView = isView;
    this.objectKind = objectKind;
    this.hasUnsavedChanges = restoredDraft;
    this.sessionKey = currentConnectionSessionKey(
      connectionManager,
      connectionId,
    );
    this.initiallyBoundDynamoTarget =
      typeof this.sessionKey === "string" &&
      connectionManager.getConnection(connectionId)?.type === "dynamodb";
    this.sessionGeneration = this.currentSessionGeneration();
    this.previewController = new TableMutationPreviewController({
      connectionId,
      tableName: this.getDisplayTableName(),
      connectionManager,
      tableDataService: this.svc,
      notifyWarning: (message) => {
        void vscode.window.showWarningMessage(
          `[RapiDB] ${redactDiagnosticText(message, this.diagnosticContext())}`,
        );
      },
    });

    this.panel.webview.html = this.buildHtml(context);

    const connectSubscription = connectionManager.onDidConnect?.(() => {
      void this.retryInitializationAfterConnect();
    });
    const metadataInvalidationSubscription =
      connectionManager.onDidInvalidateConnectionMetadata?.((id) => {
        if (this.disposed || id !== connectionId) return;
        ++this.connectionGeneration;
        this.dynamoExportColumns.clear();
        this.previewController.clear();
        this.pageReadAbortController.abort();
        this.noSqlDeletePages.length = 0;
        this.resetBrowsingSnapshot();
        this.pageReadAbortController = new AbortController();
        this.inFlightPageRequests.clear();
        if (!this.initializationAttempt?.waitingForConnect) {
          this.initializationAttempt = undefined;
          this.initializationPromise = undefined;
        }
        if (this.initialized) {
          void this.postMessage("tableConnectionInvalidated", {});
        }
        this.metadataInvalidated = true;
        this.svc.clearForConnection(connectionId);
        ++this.schemaRefreshGeneration;
        this.schemaRefreshPromise = Promise.resolve();
        this.schemaRefreshError =
          "Connection changed. Waiting for metadata from the new session.";
      });
    const schemaRefreshSubscription = connectionManager.onDidRefreshSchemas?.(
      (refreshedConnectionId) => {
        if (this.disposed) return;
        if (refreshedConnectionId && refreshedConnectionId !== connectionId) {
          return;
        }
        this.refreshMetadata();
      },
    );
    const sessionSubscription =
      connectionManager.onDidChangeConnectionSession?.(
        (changedConnectionId) => {
          if (changedConnectionId === connectionId)
            this.closeIfSessionChanged();
        },
      );
    this.panel.onDidDispose(() => {
      if (this.disposed) return;
      this.disposed = true;
      this.messageEpoch++;
      this.pageReadAbortController.abort();
      this.noSqlDeletePages.length = 0;
      this.resetBrowsingSnapshot();
      connectSubscription?.dispose();
      metadataInvalidationSubscription?.dispose();
      schemaRefreshSubscription?.dispose();
      sessionSubscription?.dispose();
      this.previewController.clear();
      this.previewSchemas.clear();
      this.deleteOperations.clear();
      TablePanel.instances.delete(this);
      if (TablePanel.panels.get(this.key) === this) {
        TablePanel.panels.delete(this.key);
        for (const instance of TablePanel.instances) {
          if (!instance.disposed && instance.key === this.key) {
            TablePanel.panels.set(this.key, instance);
            break;
          }
        }
      }

      this.svc.clearForConnection(connectionId);
    });

    attachPanelMessageHandler(
      this.panel,
      (message) => this.handleMessage(message),
      (error) => {
        const normalized = logErrorWithContext(
          "TablePanel unhandled error",
          error,
          this.diagnosticContext(),
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${normalized.message}`,
        );
      },
    );
  }

  private diagnosticContext(): unknown {
    return this.connectionManager.getConnectionDiagnosticContext?.(
      this.connectionId,
    );
  }

  private static panelKey(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): string {
    return JSON.stringify([connectionId, database, schema, table]);
  }

  static registerSerializer(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
  ): vscode.Disposable {
    if (typeof vscode.window.registerWebviewPanelSerializer !== "function") {
      return { dispose: () => undefined };
    }
    return vscode.window.registerWebviewPanelSerializer(TablePanel.viewType, {
      async deserializeWebviewPanel(panel, state) {
        const saved =
          state !== null && typeof state === "object"
            ? (state as { initialState?: unknown }).initialState
            : undefined;
        if (!isTablePanelSerialization(saved)) {
          panel.dispose();
          return;
        }
        const initializing =
          connectionManager.initializeConnectionCredentialRevision?.(
            saved.connectionId,
          );
        const credentialsVerified = !initializing || (await initializing);
        const currentSessionKey = currentConnectionSessionKey(
          connectionManager,
          saved.connectionId,
        );
        const hasDraft =
          (state as { tableDraft?: unknown }).tableDraft !== undefined;
        const restoredAttachmentDraft =
          hasDraft &&
          connectionManager.getConnection(saved.connectionId)?.type ===
            "sqlite" &&
          sqliteAttachmentSchema(saved.database, saved.schema) !== undefined;
        const restoredDynamoTargetMismatch =
          connectionManager.getConnection(saved.connectionId)?.type ===
            "dynamodb" &&
          (!credentialsVerified ||
            !saved.connectionSessionKey ||
            saved.connectionSessionKey !== currentSessionKey);
        if (
          currentSessionKey === undefined ||
          restoredAttachmentDraft ||
          restoredDynamoTargetMismatch ||
          (hasDraft &&
            (!credentialsVerified ||
              (saved.connectionSessionKey === undefined
                ? connectionManager.getConnectionSessionNonce?.(
                    saved.connectionId,
                  ) !== undefined
                : saved.connectionSessionKey !== currentSessionKey)))
        ) {
          if (hasDraft) {
            void vscode.window.showWarningMessage(
              currentSessionKey === undefined
                ? `[RapiDB] A restored table panel for "${saved.table}" was closed because its connection no longer exists. Its unsaved changes were discarded.`
                : restoredAttachmentDraft
                  ? `[RapiDB] A restored SQLite table panel for "${saved.table}" was closed because attachment identity cannot be verified across restarts. Its unsaved changes were discarded.`
                  : `[RapiDB] A restored table panel for "${saved.table}" was closed because its connection settings changed since its unsaved changes were made. The changes were discarded so they cannot be applied to a different database.`,
            );
          }
          panel.dispose();
          return;
        }
        const title = TablePanel.buildPanelTitle(
          connectionManager,
          saved.connectionId,
          saved.schema,
          saved.table,
          saved.isView ?? false,
          saved.objectKind,
        );
        panel.title = title;
        TablePanel.trackPanel(
          panel,
          context,
          connectionManager,
          saved.connectionId,
          saved.database,
          saved.schema,
          saved.table,
          saved.isView ?? false,
          saved.objectKind,
          hasDraft,
        );
      },
    });
  }

  private static buildPanelTitle(
    connectionManager: ConnectionManager,
    connectionId: string,
    schema: string,
    table: string,
    isView: boolean,
    objectKind?: TablePanelObjectKind,
  ): string {
    const connection = connectionManager.getConnection(connectionId);
    const connName = connection?.name ?? connectionId;
    const connectionType = connection?.type;
    const effectiveObjectKind = objectKind ?? (isView ? "view" : "table");
    const objType = titleObjectKindLabel(connectionType, effectiveObjectKind);
    const displayTable = tableDisplayName(connectionType, table);
    const schemaPrefix =
      schema && shouldShowSchemaPrefix(connectionType) ? `${schema}.` : "";
    return `${schemaPrefix}${displayTable} (${objType}) [${connName}]`;
  }

  private static trackPanel(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    isView: boolean,
    objectKind?: TablePanelObjectKind,
    restoredDraft = false,
  ): TablePanel {
    const instance = new TablePanel(
      panel,
      context,
      connectionManager,
      connectionId,
      database,
      schema,
      table,
      isView,
      objectKind,
      restoredDraft,
    );
    TablePanel.instances.add(instance);
    if (!TablePanel.panels.has(instance.key)) {
      TablePanel.panels.set(instance.key, instance);
    }
    attachConnectionScopedPanelLifecycle(
      panel,
      connectionManager,
      connectionId,
      () => {
        if (instance.closeIfSessionChanged()) return;
        panel.title = TablePanel.buildPanelTitle(
          connectionManager,
          connectionId,
          schema,
          table,
          isView,
          objectKind,
        );
      },
      () => instance.keepOpenAfterDisconnect(),
    );
    return instance;
  }

  private keepOpenAfterDisconnect(): boolean {
    const hasDeleteRecovery = this.hasDeleteRecovery();
    if (this.disposed || (!this.hasUnsavedChanges && !hasDeleteRecovery))
      return false;
    const sessionChange = this.sessionChangeWarning();
    void vscode.window.showWarningMessage(
      sessionChange ??
        (hasDeleteRecovery
          ? this.sqliteAttachmentTarget
            ? `[RapiDB] "${this.panel.title}" was kept open with an unresolved delete. Reconnecting cannot restore this SQLite attachment or transfer the delete recovery.`
            : `[RapiDB] "${this.panel.title}" was kept open with a delete in progress or awaiting verification. Reconnect and verify the data before retrying.`
          : this.sqliteAttachmentTarget
            ? `[RapiDB] "${this.panel.title}" was kept open with unsaved changes. Reconnecting cannot restore this SQLite attachment or transfer the changes.`
            : `[RapiDB] "${this.panel.title}" has unsaved changes and was kept open. Reconnect to apply them, or use Revert All to discard them.`),
    );
    return sessionChange === null;
  }

  private hasDeleteRecovery(): boolean {
    return this.webviewHasDeleteRecovery || this.deleteOperations.size > 0;
  }

  private currentSessionGeneration(): number {
    return (
      this.connectionManager.getConnectionSessionGeneration?.(
        this.connectionId,
      ) ?? 0
    );
  }

  private sessionChangeWarning(): string | null {
    const key = currentConnectionSessionKey(
      this.connectionManager,
      this.connectionId,
    );
    if (
      typeof key === "string" &&
      key === this.sessionKey &&
      this.currentSessionGeneration() === this.sessionGeneration
    ) {
      return null;
    }
    const warning =
      key === undefined
        ? `[RapiDB] "${this.panel.title}" was closed because its connection was removed. Its unsaved changes were discarded.`
        : `[RapiDB] "${this.panel.title}" was closed because its connection settings changed. Its unsaved changes were discarded so they cannot be applied to a different database.`;
    return this.hasDeleteRecovery()
      ? `${warning} A previous delete may have affected the original target; verify it there before retrying.`
      : warning;
  }

  private closeIfSessionChanged(): boolean {
    if (this.disposed) return true;
    const warning = this.sessionChangeWarning();
    if (warning === null) return false;
    const hasRetainedWork = this.hasUnsavedChanges || this.hasDeleteRecovery();
    if (
      !hasRetainedWork &&
      !this.initialized &&
      !this.initiallyBoundDynamoTarget
    ) {
      const key = currentConnectionSessionKey(
        this.connectionManager,
        this.connectionId,
      );
      if (key !== undefined) {
        this.sessionKey = key;
        this.sessionGeneration = this.currentSessionGeneration();
        return false;
      }
    }
    if (hasRetainedWork) void vscode.window.showWarningMessage(warning);
    this.panel.dispose();
    return true;
  }

  private async postMessage(type: string, payload: unknown): Promise<boolean> {
    const epoch = this.messageEpoch;
    if (this.disposed) return false;

    let delivered = false;
    let deliveryError: unknown;
    try {
      delivered = await this.panel.webview.postMessage({ type, payload });
    } catch (error: unknown) {
      deliveryError = error;
    }

    if (delivered || !this.isCurrentMessageEpoch(epoch)) return delivered;

    const fallback = this.createUndeliveredMutationFallback(type, payload);
    if (!fallback) {
      if (deliveryError !== undefined) {
        logErrorWithContext(
          "TablePanel webview message delivery failed",
          deliveryError,
          this.diagnosticContext(),
        );
      }
      return false;
    }

    if (type === "tableMutationPreview") {
      this.discardUndeliveredPreview(payload);
    }

    try {
      const fallbackDelivered = await this.panel.webview.postMessage(fallback);
      if (!fallbackDelivered && this.isCurrentMessageEpoch(epoch)) {
        this.reportUndeliveredMutationFallback();
      }
    } catch (error: unknown) {
      if (this.isCurrentMessageEpoch(epoch)) {
        this.reportUndeliveredMutationFallback(error);
      }
    }
    return false;
  }

  private isCurrentMessageEpoch(epoch: number): boolean {
    return !this.disposed && epoch === this.messageEpoch;
  }

  private createUndeliveredMutationFallback(
    type: string,
    payload: unknown,
  ): { type: string; payload: unknown } | null {
    if (payload === null || typeof payload !== "object") return null;
    const message = payload as Record<string, unknown>;
    if (typeof message.operationId !== "string") return null;

    if (type === "tableMutationPreview") {
      const resultType =
        message.kind === "applyChanges"
          ? "applyResult"
          : message.kind === "deleteRows"
            ? "deleteResult"
            : message.kind === "insertRow"
              ? "insertResult"
              : null;
      if (!resultType) return null;
      const error =
        "The mutation preview could not be delivered. No changes were applied; retry when the table panel is responsive.";
      return {
        type: resultType,
        payload:
          resultType === "deleteResult"
            ? {
                operationId: message.operationId,
                success: false,
                error,
                affectedRows: 0,
                rowOutcomes: [],
                changesPossible: false,
                outcomeUnknown: false,
              }
            : { operationId: message.operationId, success: false, error },
      };
    }

    if (
      type !== "applyResult" &&
      type !== "deleteResult" &&
      type !== "insertResult"
    ) {
      return null;
    }

    return { type, payload };
  }

  private discardUndeliveredPreview(payload: unknown): void {
    if (payload === null || typeof payload !== "object") return;
    const preview = payload as Partial<TableMutationPreviewPayload>;
    if (!preview.previewToken || !preview.operationId) return;
    if (
      this.previewSchemas.get(preview.previewToken)?.operationId !==
      preview.operationId
    ) {
      return;
    }
    this.previewSchemas.delete(preview.previewToken);
    this.previewController.cancel(preview.previewToken, preview.operationId);
  }

  private reportUndeliveredMutationFallback(error?: unknown): void {
    if (this.disposed) return;
    const warning =
      "The table mutation response could not be delivered to the panel. Refresh the table and verify before retrying.";
    logErrorWithContext(
      "TablePanel mutation fallback delivery failed",
      error ?? new Error(warning),
      this.diagnosticContext(),
    );
    void vscode.window.showWarningMessage(`[RapiDB] ${warning}`);
  }

  private shouldSkipTableMutationPreview(): boolean {
    return this.connectionManager.getSkipTableMutationPreview() === true;
  }

  private async presentOrExecuteMutationPreview(
    preview: TableMutationPreviewPayload,
    preparedSnapshot: TableSchemaSnapshot,
  ): Promise<void> {
    if (!this.isCurrentMutationSession(preparedSnapshot.connectionGeneration)) {
      this.previewController.cancel(preview.previewToken, preview.operationId);
      await this.postConnectionConflict(preview.operationId, preview.kind);
      return;
    }
    const snapshot =
      await this.resolveUnchangedSchemaSnapshot(preparedSnapshot);
    if (!this.isCurrentMutationSession(preparedSnapshot.connectionGeneration)) {
      this.previewController.cancel(preview.previewToken, preview.operationId);
      await this.postConnectionConflict(preview.operationId, preview.kind);
      return;
    }
    if (!snapshot) {
      this.previewController.cancel(preview.previewToken, preview.operationId);
      await this.postSchemaConflict(preview.operationId, preview.kind);
      return;
    }
    if (this.previewSchemas.size >= 50) {
      const oldest = this.previewSchemas.keys().next().value;
      if (oldest !== undefined) this.previewSchemas.delete(oldest);
    }
    this.previewSchemas.set(preview.previewToken, {
      snapshot,
      operationId: preview.operationId,
      kind: preview.kind,
    });
    const executed =
      this.previewOperations.get(preview.operationId)?.executed === true;
    this.previewOperations.delete(preview.operationId);
    if (this.previewOperations.size >= 200) {
      const oldest = this.previewOperations.keys().next().value;
      if (oldest !== undefined) this.previewOperations.delete(oldest);
    }
    this.previewOperations.set(preview.operationId, {
      kind: preview.kind,
      executed,
    });
    if (!this.shouldSkipTableMutationPreview()) {
      await this.postMessage("tableMutationPreview", preview);
      return;
    }
    if (preview.confirmationWarning) {
      await this.postMessage("tableMutationPreview", {
        ...preview,
        text: "",
        sql: "",
        confirmationOnly: true,
      } satisfies TableMutationPreviewPayload);
      return;
    }

    await this._handleConfirmMutationPreview(preview);
  }

  private async awaitSchemaRefresh(): Promise<TableSchemaSnapshot> {
    let current: Promise<void>;
    do {
      current = this.schemaRefreshPromise;
      await current;
    } while (current !== this.schemaRefreshPromise);
    if (this.schemaRefreshError)
      throw new Error(
        `Schema metadata refresh failed: ${this.schemaRefreshError}`,
      );
    const verification = this.verifyDynamoDBTarget();
    if (verification) await verification;
    return this.schemaSnapshot;
  }

  private isCurrentSchemaSnapshot(snapshot: TableSchemaSnapshot): boolean {
    return (
      snapshot === this.schemaSnapshot &&
      snapshot.connectionGeneration === this.connectionGeneration &&
      snapshot.generation === this.schemaRefreshGeneration &&
      this.schemaRefreshError === null
    );
  }

  private async resolveUnchangedSchemaSnapshot(
    snapshot: TableSchemaSnapshot,
  ): Promise<TableSchemaSnapshot | null> {
    if (this.isCurrentSchemaSnapshot(snapshot)) return snapshot;
    let current: TableSchemaSnapshot;
    try {
      current = await this.awaitSchemaRefresh();
    } catch {
      return null;
    }
    return this.isCurrentSchemaSnapshot(current) &&
      current.connectionGeneration === snapshot.connectionGeneration &&
      current.signature === snapshot.signature
      ? current
      : null;
  }

  private isCurrentMutationSession(generation: number): boolean {
    return (
      !this.disposed &&
      generation === this.connectionGeneration &&
      (!this.initiallyBoundDynamoTarget ||
        this.sessionChangeWarning() === null) &&
      this.connectionManager.isConnected?.(this.connectionId) !== false &&
      this.connectionManager.isConnecting?.(this.connectionId) !== true
    );
  }

  private assertMutationSession(generation: number): void {
    if (!this.isCurrentMutationSession(generation)) {
      throw new Error(MUTATION_SESSION_CONFLICT);
    }
  }

  private async postConnectionConflict(
    operationId: string,
    kind: TableMutationPreviewPayload["kind"],
  ): Promise<void> {
    const { type, payload } = notExecutedMutationResult(
      operationId,
      kind,
      MUTATION_SESSION_CONFLICT,
    );
    await this.postMessage(type, payload);
  }

  private async postPreviewUnavailable(operationId: string): Promise<void> {
    const operation = this.previewOperations.get(operationId);
    if (!operation) {
      logErrorWithContext(
        "TablePanel mutation preview confirmation rejected",
        new Error(`Unknown mutation preview operation ${operationId}.`),
      );
      return;
    }
    const { kind, executed } = operation;
    await this.postMessage(
      kind === "applyChanges"
        ? "applyResult"
        : kind === "deleteRows"
          ? "deleteResult"
          : "insertResult",
      {
        operationId,
        success: false,
        error: executed
          ? "This mutation preview was already confirmed and executed. Refresh the table and verify the data before retrying."
          : "The mutation preview expired and was not executed. No changes were applied; review the changes and retry.",
        changesPossible: executed,
        outcomeUnknown: executed,
        ...(kind === "deleteRows" ? { affectedRows: 0, rowOutcomes: [] } : {}),
      },
    );
  }

  private async postSchemaConflict(
    operationId: string,
    kind: TableMutationPreviewPayload["kind"],
  ): Promise<void> {
    const { type, payload } = notExecutedMutationResult(
      operationId,
      kind,
      MUTATION_SCHEMA_CONFLICT,
    );
    await this.postMessage(type, payload);
  }

  private isConnectionReadOnly(): boolean {
    return (
      this.connectionManager.getConnection(this.connectionId)?.readOnly === true
    );
  }

  private normalizePageRequest(
    page: number | string | undefined,
    pageSize: number | string | undefined,
  ): { page: number; pageSize: number } {
    return {
      page: Math.max(1, Math.floor(Number(page) || 1)),
      pageSize: Math.min(
        10000,
        Math.max(1, Math.floor(Number(pageSize) || 50)),
      ),
    };
  }

  private buildPageRequestKey(
    page: number,
    pageSize: number,
    filters: FilterExpression[],
    sort: SortConfig | null,
  ): string {
    return JSON.stringify({
      schemaGeneration: this.schemaRefreshGeneration,
      page,
      pageSize,
      sort,
      filters,
    });
  }

  private resetBrowsingSnapshot(): void {
    if (!this.browsingSnapshot) return;
    this.browsingSnapshot.controller.abort();
    this.browsingSnapshot = undefined;
    this.inFlightPageRequests.clear();
  }

  private getBrowsingSnapshot(
    pageSize: number,
    filters: FilterExpression[],
    sort: SortConfig | null,
  ): AbortSignal | undefined {
    if (
      this.connectionManager.getConnection(this.connectionId)?.type !==
      "elasticsearch"
    )
      return undefined;
    const key = this.buildPageRequestKey(0, pageSize, filters, sort);
    if (this.browsingSnapshot?.key !== key) {
      this.resetBrowsingSnapshot();
      this.browsingSnapshot = { key, controller: new AbortController() };
    }
    return this.browsingSnapshot.controller.signal;
  }

  static disposeAll(): void {
    disposePanelInstances([...TablePanel.instances], (panel) => {
      panel.panel.dispose();
    });
    TablePanel.instances.clear();
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
    objectKind?: TablePanelObjectKind,
  ): void {
    const key = TablePanel.panelKey(connectionId, database, schema, table);
    const existing = TablePanel.panels.get(key);
    if (existing) {
      existing.panel.reveal(vscode.ViewColumn.One);
      return;
    }

    const panel = vscode.window.createWebviewPanel(
      TablePanel.viewType,
      TablePanel.buildPanelTitle(
        connectionManager,
        connectionId,
        schema,
        table,
        isView,
        objectKind,
      ),
      vscode.ViewColumn.One,
      createPanelWebviewOptions(TABLE_PANEL_RETENTION_MODE),
    );

    TablePanel.trackPanel(
      panel,
      context,
      connectionManager,
      connectionId,
      database,
      schema,
      table,
      isView,
      objectKind,
    );
  }

  private async handleMessage(msg: unknown): Promise<void> {
    const parsed = parseTablePanelMessage(msg);
    if (!parsed) {
      return;
    }

    switch (parsed.type) {
      case "ready":
        await this._handleReady();
        break;
      case "fetchPage":
        if (parsed.payload) await this._handleFetchPage(parsed.payload);
        break;
      case "applyChanges":
        if (parsed.payload) await this._handleApplyChanges(parsed.payload);
        break;
      case "insertRow":
        if (parsed.payload) await this._handleInsertRow(parsed.payload);
        break;
      case "deleteRows":
        if (parsed.payload) await this._handleDeleteRows(parsed.payload);
        break;
      case "exportCSV":
        await this._handleExport("csv", parsed.payload);
        break;
      case "exportJSON":
        await this._handleExport("json", parsed.payload);
        break;
      case "confirmMutationPreview":
        if (parsed.payload)
          await this._handleConfirmMutationPreview(parsed.payload);
        break;
      case "cancelMutationPreview":
        if (parsed.payload) this._handleCancelMutationPreview(parsed.payload);
        break;
      case "readClipboard":
        if (parsed.payload) await this._handleReadClipboard(parsed.payload);
        break;
      case "writeClipboard":
        if (parsed.payload) {
          await this._handleWriteClipboard(parsed.payload);
        }
        break;
      case "unsavedChangesState":
        if (parsed.payload) {
          this.hasUnsavedChanges = parsed.payload.hasUnsavedChanges;
          this.webviewHasDeleteRecovery =
            parsed.payload.hasDeleteRecovery === true;
        }
        break;
      case "deleteRecoveryResolved":
        if (
          parsed.payload &&
          this.deleteOperations.get(parsed.payload.operationId) === "recovery"
        ) {
          this.deleteOperations.delete(parsed.payload.operationId);
        }
        break;
    }
  }

  private async _handleReadClipboard(
    payload: import("../../shared/webviewContracts").ClipboardReadPayload,
  ): Promise<void> {
    const text = await readClipboardTextSafe();
    await this.postMessage("clipboardText", { ...payload, text });
  }

  private async _handleWriteClipboard({
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
        await this.postMessage("clipboardWritten", {
          requestId,
          recipient,
          success,
        });
      }
    }
  }

  private async _handleReady(): Promise<void> {
    if (this.disposed) return;
    this.webviewReady = true;
    if (this.initializationPromise) return this.initializationPromise;
    const attempt = { waitingForConnect: false };
    this.initializationAttempt = attempt;
    this.initializationPromise = this.initializeTable(attempt).finally(() => {
      if (this.initializationAttempt === attempt) {
        this.initializationAttempt = undefined;
        this.initializationPromise = undefined;
      }
    });
    return this.initializationPromise;
  }

  private async retryInitializationAfterConnect(): Promise<void> {
    await Promise.resolve();
    await this.initializationPromise;
    if (
      !this.disposed &&
      this.webviewReady &&
      this.connectionManager.isConnected(this.connectionId)
    ) {
      if (!this.initialized) {
        this.svc.clearForConnection(this.connectionId);
        await this._handleReady();
      } else if (this.metadataInvalidated) {
        this.refreshMetadata();
      }
    }
  }

  private refreshMetadata(): void {
    this.resetBrowsingSnapshot();
    this.svc.clearForConnection(this.connectionId);
    const generation = ++this.schemaRefreshGeneration;
    this.schemaRefreshPromise = this.loadTableColumns()
      .then((columns) => {
        if (!this.disposed && generation === this.schemaRefreshGeneration) {
          this.schemaRefreshError = null;
          this.postTableInit(
            columns,
            this.initialized
              ? this.metadataInvalidated
                ? "connectionRefresh"
                : "metadataRefresh"
              : "initialize",
          );
        }
      })
      .catch((error: unknown) => {
        if (this.disposed || generation !== this.schemaRefreshGeneration)
          return;
        this.schemaRefreshError = normalizeUnknownError(error).message;
        void this.postMessage("tableError", {
          error: `Schema metadata refresh failed: ${redactDiagnosticText(this.schemaRefreshError, this.diagnosticContext())}`,
        });
      });
  }

  private getSQLiteAttachmentSchema(): string | undefined {
    return this.connectionManager.getConnection(this.connectionId)?.type ===
      "sqlite"
      ? sqliteAttachmentSchema(this.database, this.schema)
      : undefined;
  }

  private async loadTableColumns(): Promise<ColumnTypeMeta[]> {
    const verification = this.verifyDynamoDBTarget();
    if (verification) await verification;
    const schema = this.getSQLiteAttachmentSchema();
    if (schema && !this.sqliteAttachmentTarget) {
      const target = await this.connectionManager
        .getDriver(this.connectionId)
        ?.getSQLiteAttachmentTarget?.(schema);
      if (!target) throw new Error(SQLITE_ATTACHMENT_CHANGED);
      // Pin the viewer, not just the latest metadata: old rows, queued edits,
      // undo history and previews may still refer to this attachment.
      this.sqliteAttachmentTarget ??= target;
      this.svc.bindSQLiteAttachmentTarget(this.sqliteAttachmentTarget);
    }
    return this.svc.getColumns(
      this.connectionId,
      this.database,
      this.schema,
      this.table,
    );
  }

  private verifyDynamoDBTarget(): Promise<void> | undefined {
    if (
      !this.connectionManager.verifyLiveDynamoDBTarget ||
      this.connectionManager.getConnection(this.connectionId)?.type !==
        "dynamodb"
    )
      return;
    const driver = this.connectionManager.getDriver(this.connectionId);
    if (!driver?.verifyDynamoDBTarget) return;
    if (this.closeIfSessionChanged())
      throw new Error(MUTATION_SESSION_CONFLICT);
    return this.connectionManager
      .verifyLiveDynamoDBTarget(this.connectionId, driver)
      .then(() => {
        if (
          this.connectionManager.getDriver(this.connectionId) !== driver ||
          this.closeIfSessionChanged()
        )
          throw new Error(MUTATION_SESSION_CONFLICT);
      });
  }

  private getSQLiteAttachmentScope() {
    if (this.getSQLiteAttachmentSchema() && !this.sqliteAttachmentTarget) {
      throw new Error(SQLITE_ATTACHMENT_CHANGED);
    }
    return this.sqliteAttachmentTarget
      ? { sqliteAttachmentTarget: this.sqliteAttachmentTarget }
      : {};
  }

  private async initializeTable(attempt: {
    waitingForConnect: boolean;
  }): Promise<void> {
    const epoch = this.messageEpoch;
    const isCurrent = () =>
      this.isCurrentMessageEpoch(epoch) &&
      this.initializationAttempt === attempt;
    let generation: number | undefined;
    try {
      const initializing =
        this.connectionManager.initializeConnectionCredentialRevision?.(
          this.connectionId,
        );
      if (initializing) {
        if (!(await initializing)) {
          throw new Error(
            "Could not verify the connection's saved credentials. Unlock Secret Storage and retry.",
          );
        }
        if (!isCurrent() || this.closeIfSessionChanged()) return;
      }
      const reconnectBlock =
        this.connectionManager.getAutomaticReconnectBlockReason?.(
          this.connectionId,
        );
      if (reconnectBlock) throw new Error(reconnectBlock);
      if (this.connectionManager.isConnected?.(this.connectionId) === false) {
        attempt.waitingForConnect = true;
        try {
          await this.connectionManager.connectTo(
            this.connectionId,
            "automatic",
          );
        } finally {
          attempt.waitingForConnect = false;
        }
        if (!isCurrent()) return;
        if (!this.connectionManager.isConnected(this.connectionId)) {
          throw new Error(`[RapiDB] Not connected: ${this.connectionId}`);
        }
      }
      if (!isCurrent()) return;
      generation = this.schemaRefreshGeneration;
      const cols = await this.loadTableColumns();
      if (!isCurrent() || generation !== this.schemaRefreshGeneration) {
        return;
      }
      this.schemaRefreshError = null;
      this.postTableInit(
        cols,
        this.initialized && this.metadataInvalidated
          ? "connectionRefresh"
          : "initialize",
      );
    } catch (err: unknown) {
      if (
        !isCurrent() ||
        (generation !== undefined &&
          generation !== this.schemaRefreshGeneration)
      ) {
        return;
      }
      const error = normalizeUnknownError(err, this.diagnosticContext());
      await this.postMessage("tableError", { error: error.message });
    }
  }

  private postTableInit(
    columns: ColumnTypeMeta[],
    intent: TableInitPayload["intent"] = "initialize",
  ): void {
    if (this.closeIfSessionChanged()) return;
    // Preserve the driver's catalog scope attached to this metadata array.
    const reference = this.connectionManager
      .getDriver(this.connectionId)
      ?.getTableDataReference?.(
        this.database,
        this.schema,
        this.table,
        columns,
      );
    this.initialized = true;
    this.metadataInvalidated = false;
    const snapshotColumns =
      reference === undefined ? structuredClone(columns) : columns;
    this.schemaSnapshot = {
      generation: this.schemaRefreshGeneration,
      connectionGeneration: this.connectionGeneration,
      columns: snapshotColumns,
      signature: JSON.stringify(
        reference === undefined
          ? snapshotColumns
          : [snapshotColumns, reference],
      ),
    };
    const primaryKeyColumns = columns
      .filter((column) => column.isPrimaryKey)
      .map((column) => column.name);
    const noSqlDeleteBackend = this.noSqlDeleteBackend();
    const connectionReadOnly = this.isConnectionReadOnly();
    const payload: TableInitPayload = {
      intent,
      columns,
      primaryKeyColumns,
      isView: this.isView,
      connectionReadOnly,
      connectionSessionKey: this.sessionKey ?? undefined,
      ...(this.canObserveSqlDelete()
        ? { requiresSqlDeleteObservation: true }
        : {}),
      ...(noSqlDeleteBackend && !connectionReadOnly && !this.isView
        ? { noSqlDeleteBackend }
        : {}),
    };
    void this.postMessage("tableInit", payload);
  }

  private async _handleFetchPage(
    raw: NonNullable<
      Extract<
        import("../../shared/webviewContracts").TablePanelMessage,
        { type: "fetchPage" }
      >["payload"]
    >,
  ): Promise<void> {
    const fetchId = raw.fetchId;
    const connectionGeneration = this.connectionGeneration;
    const isCurrentSession = () =>
      !this.disposed && connectionGeneration === this.connectionGeneration;
    const { page, pageSize } = this.normalizePageRequest(
      raw.page,
      raw.pageSize,
    );
    const filters = coerceFilterExpressions(raw.filters);
    const sort = raw.sort ?? null;
    const normalizedFilters = filters as FilterExpression[];
    let browsingSnapshot: AbortSignal | undefined;
    try {
      if (raw.refreshMetadata) this.refreshMetadata();
      if (raw.conflictRefresh || raw.refreshData) this.resetBrowsingSnapshot();
      browsingSnapshot = this.getBrowsingSnapshot(
        pageSize,
        normalizedFilters,
        sort,
      );
      await this.awaitSchemaRefresh();
      if (!isCurrentSession()) return;
      browsingSnapshot?.throwIfAborted();
      this.getSQLiteAttachmentScope();
      // A reloaded webview can request verification before a late delete has
      // settled. Do not let that read lift its retry fence.
      if ([...this.deleteOperations.values()].includes("executing")) {
        throw new Error(
          "A delete is still in progress. Refresh again after it finishes.",
        );
      }
      const normalizedSort: SortConfig | null = sort;
      const requestKey = this.buildPageRequestKey(
        page,
        pageSize,
        normalizedFilters,
        normalizedSort,
      );
      // Conflict review must observe a new read, never a pre-conflict read
      // already in flight for the same page. The panel's target stays pinned.
      const freshRead =
        raw.refreshMetadata || raw.conflictRefresh || raw.refreshData;
      let inFlightRequest = freshRead
        ? undefined
        : this.inFlightPageRequests.get(requestKey);
      if (!inFlightRequest) {
        const observeSqlDelete = this.canObserveSqlDelete();
        const readOptions: [
          observeSqlDelete?: boolean,
          conflictRefresh?: boolean,
          browsingSnapshot?: AbortSignal,
        ] = browsingSnapshot
          ? [observeSqlDelete, raw.conflictRefresh, browsingSnapshot]
          : raw.conflictRefresh
            ? [observeSqlDelete, true]
            : observeSqlDelete
              ? [true]
              : [];
        const pageRequest = this.svc
          .getPage(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            page,
            pageSize,
            normalizedFilters,
            normalizedSort,
            false,
            this.pageReadAbortController.signal,
            ...readOptions,
          )
          .finally(() => {
            if (this.inFlightPageRequests.get(requestKey) === pageRequest) {
              this.inFlightPageRequests.delete(requestKey);
            }
          });
        if (!freshRead) {
          this.inFlightPageRequests.set(requestKey, pageRequest);
        }
        inFlightRequest = pageRequest;
      }

      const result = await inFlightRequest;
      if (!isCurrentSession()) return;
      browsingSnapshot?.throwIfAborted();
      const snapshotColumnNames = new Set(
        this.schemaSnapshot.columns.map((column) => column.name),
      );
      const connectionType = this.connectionManager.getConnection(
        this.connectionId,
      )?.type;
      const pageScopedExtraColumns = connectionType === "dynamodb";
      let extraColumns: ColumnTypeMeta[] = [];
      if (pageScopedExtraColumns) {
        // A client-side sort or filter may materialize more than this page.
        const pageAttributeNames = new Set(
          result.rows.flatMap((row) => Object.keys(row)),
        );
        extraColumns = result.columns
          .filter(
            ({ name }) =>
              pageAttributeNames.has(name) && !snapshotColumnNames.has(name),
          )
          .map(
            ({ name, type, nativeType, category, valueSemantics }) =>
              ({
                name,
                type,
                nativeType,
                category,
                valueSemantics,
                nullable: true,
                isReadOnly: true,
                isPrimaryKey: false,
                isForeignKey: false,
                filterable: false,
                filterOperators: [],
              }) satisfies ColumnTypeMeta,
          );
        for (const column of extraColumns) {
          this.dynamoExportColumns.set(column.name, column);
        }
      } else if (connectionType === "mongodb") {
        extraColumns = result.columns.filter(
          (column) => !snapshotColumnNames.has(column.name),
        );
      }
      let noSqlDeleteObservationIds: Array<string | null> | undefined;
      if (result.noSqlDeleteObservations) {
        const pageObservations = new Map<string, NoSqlDeleteObservation>();
        noSqlDeleteObservationIds = result.noSqlDeleteObservations.map(
          (observation) => {
            if (!observation) return null;
            const id = randomUUID();
            pageObservations.set(id, observation);
            return id;
          },
        );
        this.noSqlDeletePages.unshift(pageObservations);
        this.noSqlDeletePages.length = Math.min(
          this.noSqlDeletePages.length,
          2,
        );
      }
      this.postMessage("tableData", {
        fetchId,
        ...(extraColumns.length > 0 ? { extraColumns } : {}),
        ...(pageScopedExtraColumns ? { pageScopedExtraColumns } : {}),
        rows: result.rows,
        ...(result.rowUpdateBlockReasons
          ? { rowUpdateBlockReasons: result.rowUpdateBlockReasons }
          : {}),
        mongoIdTypes: result.mongoIdTypes,
        ...(result.mongoRowMutationBlockReason
          ? { mongoRowMutationBlockReason: result.mongoRowMutationBlockReason }
          : {}),
        mongoValueTypes: result.mongoValueTypes,
        rowValueTypes: result.rowValueTypes ?? result.mongoValueTypes,
        ...(result.sqliteRowIdentities
          ? { sqliteRowIdentities: result.sqliteRowIdentities }
          : {}),
        ...(result.postgresArrayRowIdentities
          ? { postgresArrayRowIdentities: result.postgresArrayRowIdentities }
          : {}),
        ...(result.postgresIntervalRowIdentities
          ? {
              postgresIntervalRowIdentities:
                result.postgresIntervalRowIdentities,
            }
          : {}),
        ...(result.mysqlTimestampRowIdentities
          ? { mysqlTimestampRowIdentities: result.mysqlTimestampRowIdentities }
          : {}),
        ...(result.mssqlSqlVariantRowIdentities
          ? {
              mssqlSqlVariantRowIdentities: result.mssqlSqlVariantRowIdentities,
            }
          : {}),
        ...(result.oracleTemporalRowIdentities
          ? { oracleTemporalRowIdentities: result.oracleTemporalRowIdentities }
          : {}),
        ...(result.sqlDeleteObservations
          ? { sqlDeleteObservations: result.sqlDeleteObservations }
          : {}),
        ...(noSqlDeleteObservationIds ? { noSqlDeleteObservationIds } : {}),
        totalCount: result.totalCount,
        executionTimeMs: result.executionTimeMs,
      });
    } catch (err: unknown) {
      if (!isCurrentSession()) return;
      const isFilterError =
        filters.length > 0 &&
        Boolean(
          this.connectionManager
            .getDriverCapabilities(this.connectionId)
            ?.isTableFilterError?.(normalizeUnknownError(err).message),
        );
      this.postMessage("tableError", {
        fetchId,
        error: normalizeUnknownError(err, this.diagnosticContext()).message,
        isFilterError,
      });
    }
  }

  private async _handleApplyChanges(payload: {
    operationId: string;
    updates?: import("../../shared/webviewContracts").RowUpdateMessagePayload[];
    insertValues?: Record<string, unknown>[];
  }): Promise<void> {
    const { operationId, updates, insertValues } = payload;
    const connectionGeneration = this.connectionGeneration;
    const secretContext = this.diagnosticContext();
    try {
      this.assertMutationSession(connectionGeneration);
      const snapshot = await this.awaitSchemaRefresh();
      this.assertMutationSession(connectionGeneration);
      if (!this.isCurrentSchemaSnapshot(snapshot)) {
        await this.postSchemaConflict(operationId, "applyChanges");
        return;
      }
      const driver = this.connectionManager.getDriver(this.connectionId);
      if (updates?.length && driver?.assertTableRowMutationIdentity) {
        await driver.assertTableRowMutationIdentity(
          this.database,
          this.schema,
          this.table,
        );
        this.assertMutationSession(connectionGeneration);
      }
      if (
        this.connectionManager.getConnection(this.connectionId)?.type ===
        "sqlite"
      ) {
        assertSQLitePanelIdentities(
          (updates ?? []).map((row) => row.primaryKeys),
        );
      }
      const attachmentScope = this.getSQLiteAttachmentScope();
      if (driver?.checkPersistedEdit) {
        const columnMap = new Map(
          snapshot.columns.map((column) => [column.name, column]),
        );
        const updateFailures = (updates ?? []).map((update) =>
          validatePersistedEditRecord(
            driver,
            update.changes,
            applyRowTypeHints(driver, columnMap, update.originalTypes),
          ),
        );
        const insertFailures = (insertValues ?? []).map((values) =>
          validatePersistedEditRecord(driver, values, columnMap, "Insert"),
        );
        if ([...updateFailures, ...insertFailures].some(Boolean)) {
          await this.postMessage("applyResult", {
            ...redactMutationDiagnostics(
              buildPrevalidationFailedResult(updateFailures, insertFailures),
              secretContext,
            ),
            operationId,
          });
          return;
        }
      }
      const prepared = prepareApplyChangesPlan(
        this.connectionManager,
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        updates ?? [],
        snapshot.columns,
      );
      if (!prepared.executable && !prepared.result.success) {
        await this.postMessage("applyResult", {
          ...redactMutationDiagnostics(prepared.result, secretContext),
          operationId,
        });
        return;
      }
      const previewBuilder =
        driver?.buildMutationPreviewStatements?.bind(driver);
      const applyPlan =
        prepared.executable && previewBuilder
          ? {
              ...prepared.plan,
              previewStatements: (
                await Promise.all(
                  prepared.plan.updates
                    .filter(
                      (_update, rowIndex) =>
                        !prepared.plan.skippedRows.includes(rowIndex),
                    )
                    .map(
                      ({
                        primaryKeys,
                        changes,
                        originalValues,
                        originalTypes,
                      }) =>
                        previewBuilder(
                          "update",
                          this.database,
                          this.schema,
                          this.table,
                          {
                            primaryKeys,
                            changes,
                            originalValues,
                            originalTypes,
                          },
                        ),
                    ),
                )
              ).flat(),
            }
          : prepared.executable
            ? prepared.plan
            : null;

      this.assertMutationSession(connectionGeneration);
      const insertPlans =
        insertValues !== undefined && insertValues.length > 0
          ? await Promise.all(
              insertValues.map((values) =>
                this.svc.prepareInsertRow(
                  this.connectionId,
                  this.database,
                  this.schema,
                  this.table,
                  values,
                ),
              ),
            )
          : [];

      this.assertMutationSession(connectionGeneration);
      const insertCount = insertPlans?.length ?? 0;
      const preparedSnapshot =
        await this.resolveUnchangedSchemaSnapshot(snapshot);
      this.assertMutationSession(connectionGeneration);
      if (!preparedSnapshot) {
        await this.postSchemaConflict(operationId, "applyChanges");
        return;
      }
      const mutationStatementCount =
        insertCount +
        (prepared.executable
          ? prepared.plan.updates.length - prepared.plan.skippedRows.length
          : 0);
      if (mutationStatementCount > 1) {
        const driver = this.connectionManager.getDriver(this.connectionId);
        const risk =
          getAtomicApplyRefusal(driver, applyPlan, insertPlans) ??
          (await driver?.getMutationAtomicityRisk?.(
            this.database,
            this.schema,
            this.table,
          ));

        if (risk) {
          this.postMessage("applyResult", {
            operationId,
            success: false,
            error: redactDiagnosticText(risk, secretContext),
          });
          return;
        }
      }

      if (!prepared.executable && insertCount === 0) {
        if (prepared.result.warning) {
          void vscode.window.showWarningMessage(
            `[RapiDB] ${redactDiagnosticText(prepared.result.warning, secretContext)}`,
          );
        }
        this.postMessage("applyResult", {
          ...redactMutationDiagnostics(prepared.result, secretContext),
          operationId,
        });
        return;
      }

      await this.presentOrExecuteMutationPreview(
        this.previewController.createApplyChangesPreview(operationId, {
          apply: applyPlan ? { ...applyPlan, ...attachmentScope } : null,
          applyResultWhenEmpty: prepared.executable ? null : prepared.result,
          inserts: insertPlans.map((plan) => ({ ...plan, ...attachmentScope })),
        }),
        preparedSnapshot,
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err, secretContext);
      this.postMessage("applyResult", {
        operationId,
        success: false,
        error: error.message,
        ...(err instanceof PersistedEditValidationError
          ? {
              rowOutcomes: (updates ?? []).map((_, rowIndex) => ({
                rowIndex,
                success: false,
                status: "skipped" as const,
                message: "Not applied because an insert failed prevalidation.",
              })),
              insertApplied: false,
            }
          : {}),
      });
    }
  }

  private async _handleInsertRow(payload: {
    operationId: string;
    values?: Record<string, unknown>;
  }): Promise<void> {
    const { operationId, values = {} } = payload;
    const connectionGeneration = this.connectionGeneration;
    const secretContext = this.diagnosticContext();
    try {
      this.assertMutationSession(connectionGeneration);
      const snapshot = await this.awaitSchemaRefresh();
      this.assertMutationSession(connectionGeneration);
      if (!this.isCurrentSchemaSnapshot(snapshot)) {
        await this.postSchemaConflict(operationId, "insertRow");
        return;
      }
      const attachmentScope = this.getSQLiteAttachmentScope();
      const plan = await this.svc.prepareInsertRow(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        values,
      );
      await this.presentOrExecuteMutationPreview(
        this.previewController.createInsertPreview(operationId, {
          ...plan,
          ...attachmentScope,
        }),
        snapshot,
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err, secretContext);
      this.postMessage("insertResult", {
        operationId,
        success: false,
        error: error.message,
        ...(err instanceof PersistedEditValidationError
          ? {
              status: err.status,
              columns: err.columns,
            }
          : {}),
      });
    }
  }

  private requiresSqlDeleteObservation(): boolean {
    return isSqlDeleteBackend(
      this.connectionManager.getConnection(this.connectionId)?.type,
    );
  }

  private noSqlDeleteBackend() {
    return observedNoSqlDeleteBackend(
      this.connectionManager.getConnection(this.connectionId)?.type,
    );
  }

  private canObserveSqlDelete(): boolean {
    return (
      this.requiresSqlDeleteObservation() &&
      !this.isView &&
      !this.isConnectionReadOnly()
    );
  }

  private async _handleDeleteRows(payload: {
    operationId: string;
    primaryKeysList?: Array<Record<string, unknown>>;
    sqlDeleteObservations?: Array<SqlDeleteObservation | null>;
    noSqlDeleteObservationIds?: Array<string | null>;
  }): Promise<void> {
    const { operationId, primaryKeysList = [] } = payload;
    const connectionGeneration = this.connectionGeneration;
    const secretContext = this.diagnosticContext();
    try {
      this.assertMutationSession(connectionGeneration);
      const snapshot = await this.awaitSchemaRefresh();
      this.assertMutationSession(connectionGeneration);
      if (!this.isCurrentSchemaSnapshot(snapshot)) {
        await this.postSchemaConflict(operationId, "deleteRows");
        return;
      }
      if (
        this.connectionManager.getConnection(this.connectionId)?.type ===
        "sqlite"
      ) {
        assertSQLitePanelIdentities(primaryKeysList);
      }
      const attachmentScope = this.getSQLiteAttachmentScope();
      const noSqlBackend = this.noSqlDeleteBackend();
      const noSqlObservations = noSqlBackend
        ? (payload.noSqlDeleteObservationIds ?? []).map((id) =>
            id
              ? (this.noSqlDeletePages.find((page) => page.has(id))?.get(id) ??
                null)
              : null,
          )
        : undefined;
      const observations: [
        sql?: readonly (SqlDeleteObservation | null)[],
        noSql?: readonly (NoSqlDeleteObservation | null)[],
      ] = noSqlObservations
        ? [undefined, noSqlObservations]
        : this.requiresSqlDeleteObservation()
          ? [payload.sqlDeleteObservations ?? []]
          : [];
      const plan = await this.svc.prepareDeleteRowsPlan(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        primaryKeysList,
        ...observations,
      );

      this.assertMutationSession(connectionGeneration);
      const preparedSnapshot =
        await this.resolveUnchangedSchemaSnapshot(snapshot);
      this.assertMutationSession(connectionGeneration);
      if (!preparedSnapshot) {
        await this.postSchemaConflict(operationId, "deleteRows");
        return;
      }
      if (!plan) {
        this.postMessage("deleteResult", {
          operationId,
          ...buildDeleteResult([], { affectedRows: 0 }),
        });
        return;
      }

      await this.presentOrExecuteMutationPreview(
        this.previewController.createDeleteRowsPreview(operationId, {
          ...plan,
          ...attachmentScope,
        }),
        preparedSnapshot,
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err, secretContext);
      this.postMessage("deleteResult", {
        operationId,
        ...unattemptedDeleteResult(primaryKeysList, error.message),
      });
    }
  }

  private async _handleExport(
    format: DriverTableExportFormat,
    payload: ExportPayload | undefined,
  ): Promise<void> {
    const connectionGeneration = this.connectionGeneration;
    const {
      sort = null,
      filters = [],
      limitToPage,
      columnOrder,
    } = payload ?? {};
    const normalizedLimitToPage = limitToPage
      ? this.normalizePageRequest(limitToPage.page, limitToPage.pageSize)
      : undefined;
    const connectionType = this.connectionManager.getConnection(
      this.connectionId,
    )?.type;
    const observedColumns =
      connectionType === "dynamodb"
        ? columnOrder?.flatMap((name) => {
            const column = this.dynamoExportColumns.get(name);
            return column ? [column] : [];
          })
        : undefined;
    const pageMayLackSnapshot =
      connectionType === "mysql" ||
      connectionType === "mssql" ||
      connectionType === "mongodb" ||
      connectionType === "redis" ||
      connectionType === "dynamodb";
    const exportDriver = this.connectionManager.getDriver(this.connectionId);
    const secretContext = this.diagnosticContext();
    const usesElasticsearchPit =
      connectionType === "elasticsearch" &&
      (normalizedLimitToPage
        ? !!exportDriver?.readTablePage && !exportDriver.readTablePageChunks
        : !!exportDriver?.exportTableChunks);
    if (
      !usesElasticsearchPit &&
      (!normalizedLimitToPage ||
        pageMayLackSnapshot ||
        connectionType === "elasticsearch")
    ) {
      const choice = await vscode.window.showWarningMessage(
        "This export does not guarantee a consistent snapshot. Concurrent inserts, deletes, or updates may cause missing, duplicated, or inconsistent rows, even with sorting. For a consistent export, stop writes or use a database-native snapshot export.",
        { modal: true },
        "Export Anyway",
      );
      if (
        choice !== "Export Anyway" ||
        !this.isCurrentMutationSession(connectionGeneration)
      ) {
        return;
      }
    }
    const displayTable = tableFileName(connectionType, this.table);
    const fileName = this.schema
      ? `${this.schema}_${displayTable}`
      : displayTable;
    const filterExpressions = coerceFilterExpressions(filters);
    const loadBoundChunks = (signal: AbortSignal) => {
      this.getSQLiteAttachmentScope();
      const assertSession = () => {
        signal.throwIfAborted();
        if (!this.isCurrentMutationSession(connectionGeneration)) {
          throw new Error(
            "Export connection changed or the table panel was closed. Retry the export from the current connection.",
          );
        }
      };
      return this.guardExportSession(
        this.reorderChunks(
          normalizedLimitToPage
            ? this._pageAsChunks(
                normalizedLimitToPage.page,
                normalizedLimitToPage.pageSize,
                sort,
                filterExpressions,
                signal,
                columnOrder,
                assertSession,
                format,
              )
            : this.svc.exportAll(
                this.connectionId,
                this.database,
                this.schema,
                this.table,
                EXPORT_CHUNK_SIZE,
                sort,
                filterExpressions,
                signal,
                columnOrder,
                format,
              ),
          columnOrder,
          assertSession,
          observedColumns,
        ),
        assertSession,
      );
    };
    const verifyTarget = () => this.verifyDynamoDBTarget();
    const loadChunks =
      connectionType === "dynamodb"
        ? async function* (signal: AbortSignal) {
            const verification = verifyTarget();
            if (verification) await verification;
            yield* loadBoundChunks(signal);
          }
        : loadBoundChunks;

    if (format === "csv") {
      await exportTableDataAsCsv({
        fileName,
        loadChunks,
        context: this.context,
        secretContext,
      });
      return;
    }

    await exportTableDataAsJson({
      fileName,
      loadChunks,
      context: this.context,
      secretContext,
    });
  }

  private async *guardExportSession(
    chunks: AsyncIterable<ChunkedExportData>,
    assertSession: () => void,
  ): AsyncIterable<ChunkedExportData> {
    assertSession();
    for await (const chunk of chunks) {
      assertSession();
      yield chunk;
      assertSession();
    }
    assertSession();
  }

  private async *reorderChunks(
    chunks: AsyncIterable<ChunkedExportData>,
    columnOrder: string[] | undefined,
    assertSession: () => void,
    observedColumns: readonly ColumnTypeMeta[] = [],
  ): AsyncIterable<ChunkedExportData> {
    if (!columnOrder) {
      yield* chunks;
      return;
    }

    if (
      columnOrder.length === 0 ||
      new Set(columnOrder).size !== columnOrder.length
    ) {
      throw new Error(
        "Export column selection must be non-empty and contain no duplicates.",
      );
    }
    await this.schemaRefreshPromise;
    assertSession();
    if (this.schemaRefreshError) {
      throw new Error(
        `Export schema metadata unavailable: ${this.schemaRefreshError}`,
      );
    }
    const schemaColumns = this.schemaSnapshot.columns;
    const knownColumns = new Map<string, ChunkedExportData["columns"][number]>(
      schemaColumns.map((column) => [column.name, column]),
    );
    const sparseColumns = new Map(
      observedColumns
        .filter((column) => !knownColumns.has(column.name))
        .map((column) => [column.name, column]),
    );

    for await (const chunk of chunks) {
      for (const column of chunk.columns) {
        knownColumns.set(column.name, column);
      }
      const sourceColumnIndexByName = new Map(
        chunk.columns.map((column, index) => [column.name, index]),
      );
      const sourceMissingCells = new Set(
        chunk.missingCells?.map(
          ({ rowIndex, columnIndex }) => `${rowIndex}:${columnIndex}`,
        ),
      );
      const reorderedColumns = columnOrder.map((name) => {
        // DynamoDB exports native AttributeValue envelopes even when a sparse
        // field is absent or has a different inferred type in another chunk.
        const column = sparseColumns.get(name) ?? knownColumns.get(name);
        if (!column) {
          throw new Error(
            `Unknown export column "${name}". Refresh table metadata and retry.`,
          );
        }
        return column;
      });

      const missingCells: Array<
        NonNullable<ChunkedExportData["missingCells"]>[number]
      > = [];
      const reorderedRows = chunk.rows.map((row, rowIndex) => {
        const entries: Array<[string, unknown]> = [];
        reorderedColumns.forEach((column, columnIndex) => {
          const sourceColumnIndex = sourceColumnIndexByName.get(column.name);
          const isMissing =
            !Object.hasOwn(row, column.name) ||
            (sourceColumnIndex !== undefined &&
              sourceMissingCells.has(`${rowIndex}:${sourceColumnIndex}`));
          if (isMissing) {
            missingCells.push({ rowIndex, columnIndex });
          } else {
            entries.push([column.name, row[column.name]]);
          }
        });
        return Object.fromEntries(entries);
      });

      yield {
        columns: reorderedColumns,
        rows: reorderedRows,
        ...(missingCells.length > 0 ? { missingCells } : {}),
      };
    }
  }

  private async *_pageAsChunks(
    page: number,
    pageSize: number,
    sort: SortConfig | null,
    filters: FilterExpression[],
    signal: AbortSignal,
    selectedColumns?: readonly string[],
    assertSession?: () => void,
    format?: DriverTableExportFormat,
  ): AsyncGenerator<DriverTableExportChunk> {
    assertSession?.();
    signal.throwIfAborted();
    const driver = this.connectionManager.getDriver(this.connectionId);
    for await (const chunk of this.svc.getPageChunks(
      this.connectionId,
      this.database,
      this.schema,
      this.table,
      page,
      pageSize,
      filters,
      sort,
      signal,
      assertSession,
    )) {
      assertSession?.();
      signal.throwIfAborted();
      yield driver?.formatTableExportChunk?.(chunk, selectedColumns, format) ??
        chunk;
    }
  }

  private async _handleConfirmMutationPreview(payload: {
    operationId: string;
    previewToken: string;
  }): Promise<void> {
    if (this.confirmingPreviewOperationIds.has(payload.operationId)) return;
    const schema = this.previewSchemas.get(payload.previewToken);
    if (!schema || schema.operationId !== payload.operationId) {
      await this.postPreviewUnavailable(payload.operationId);
      return;
    }
    this.confirmingPreviewOperationIds.add(payload.operationId);
    try {
      await this.confirmMutationPreview(payload, schema);
    } finally {
      this.confirmingPreviewOperationIds.delete(payload.operationId);
    }
  }

  private async confirmMutationPreview(
    payload: { operationId: string; previewToken: string },
    schema: {
      snapshot: TableSchemaSnapshot;
      operationId: string;
      kind: TableMutationPreviewPayload["kind"];
    },
  ): Promise<void> {
    if (!this.isCurrentMutationSession(schema.snapshot.connectionGeneration)) {
      this.previewSchemas.delete(payload.previewToken);
      this.previewController.cancel(payload.previewToken, payload.operationId);
      await this.postConnectionConflict(payload.operationId, schema.kind);
      return;
    }
    let currentSnapshot: TableSchemaSnapshot;
    try {
      currentSnapshot = await this.awaitSchemaRefresh();
    } catch {
      this.previewSchemas.delete(payload.previewToken);
      this.previewController.cancel(payload.previewToken, payload.operationId);
      if (this.schemaRefreshError === SQLITE_ATTACHMENT_CHANGED) {
        const { type, payload: result } = notExecutedMutationResult(
          payload.operationId,
          schema.kind,
          SQLITE_ATTACHMENT_CHANGED,
        );
        await this.postMessage(type, result);
      } else {
        await this.postSchemaConflict(payload.operationId, schema.kind);
      }
      return;
    }
    if (!this.isCurrentMutationSession(schema.snapshot.connectionGeneration)) {
      this.previewSchemas.delete(payload.previewToken);
      this.previewController.cancel(payload.previewToken, payload.operationId);
      await this.postConnectionConflict(payload.operationId, schema.kind);
      return;
    }
    if (
      !this.isCurrentSchemaSnapshot(currentSnapshot) ||
      schema.snapshot.signature !== currentSnapshot.signature
    ) {
      this.previewSchemas.delete(payload.previewToken);
      this.previewController.cancel(payload.previewToken, payload.operationId);
      await this.postSchemaConflict(payload.operationId, schema.kind);
      return;
    }
    this.previewSchemas.delete(payload.previewToken);
    const { operationId } = payload;
    const secretContext = this.diagnosticContext();
    let result:
      | Awaited<ReturnType<TableMutationPreviewController["confirm"]>>
      | undefined;
    try {
      result = await this.previewController.confirm(
        payload.previewToken,
        operationId,
        () => this.deleteOperations.set(operationId, "executing"),
      );
    } catch (error: unknown) {
      if (this.deleteOperations.get(operationId) !== "executing") throw error;
      result = {
        type: "deleteResult",
        payload: {
          operationId,
          success: false,
          affectedRows: 0,
          rowOutcomes: [],
          changesPossible: true,
          outcomeUnknown: true,
          error: normalizeUnknownError(error, secretContext).message,
        },
      };
    } finally {
      const operation = this.previewOperations.get(operationId);
      if (operation && result !== null) operation.executed = true;
    }
    if (!result) {
      await this.postPreviewUnavailable(operationId);
      return;
    }

    // Post-mutation reads (including verification of an uncertain write) must
    // not read from a PIT opened before the write.
    if (
      result.payload.success ||
      result.payload.changesPossible ||
      result.payload.outcomeUnknown ||
      (result.type === "applyResult" && result.payload.insertApplied)
    ) {
      this.resetBrowsingSnapshot();
    }
    if (result.type === "deleteResult" && !this.disposed) {
      // A verification request must not join a page read started before the
      // delete settled.
      this.inFlightPageRequests.clear();
      this.deleteOperations.set(
        operationId,
        result.payload.changesPossible || result.payload.outcomeUnknown
          ? "recovery"
          : "replying",
      );
    }
    try {
      await this.postMessage(
        result.type,
        redactMutationDiagnostics(result.payload, secretContext),
      );
    } finally {
      if (this.deleteOperations.get(operationId) === "replying") {
        this.deleteOperations.delete(operationId);
      }
    }
  }

  private _handleCancelMutationPreview(payload: {
    operationId: string;
    previewToken: string;
  }): void {
    if (
      this.previewSchemas.get(payload.previewToken)?.operationId ===
      payload.operationId
    )
      this.previewSchemas.delete(payload.previewToken);
    this.previewController.cancel(payload.previewToken, payload.operationId);
  }

  private buildHtml(context: vscode.ExtensionContext): string {
    return createWebviewShell({
      context,
      webview: this.panel.webview,
      title: `${this.isView ? "View" : "Table"} - ${this.getDisplayTableName()}`,
      initialState: {
        view: "table",
        connectionId: this.connectionId,
        database: this.database,
        schema: this.schema,
        table: this.table,
        displayTableName: this.getDisplayTableName(),
        isView: this.isView,
        objectKind: this.objectKind,
        connectionReadOnly: this.isConnectionReadOnly(),
        mongoRowIdentity:
          this.connectionManager.getConnection(this.connectionId)?.type ===
          "mongodb",
        defaultPageSize: this.connectionManager.getDefaultPageSize(),
        panelRetentionMode: TABLE_PANEL_RETENTION_MODE,
        connectionSessionKey: this.sessionKey ?? undefined,
      },
      extraCspDirectives: ["worker-src blob:"],
      ...APP_WEBVIEW_SHELL_LAYOUT,
      extraStyles: `
        ${WEBVIEW_SCROLLBAR_STYLES}

        .pk-key-icon {
          display: inline-flex; align-items: center; justify-content: center;
          vertical-align: middle;
        }
      `,
    });
  }

  private getDisplayTableName(): string {
    return tableDisplayName(
      this.connectionManager.getConnection(this.connectionId)?.type,
      this.table,
    );
  }
}
