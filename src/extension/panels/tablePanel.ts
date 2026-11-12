import * as vscode from "vscode";
import { getDbObjectKindDisplayLabel } from "../../shared/dbObjectKinds";
import {
  coerceFilterExpressions,
  type MongoValueTypeHint,
} from "../../shared/tableTypes";
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
import type {
  ColumnTypeMeta,
  DriverTableExportChunk,
  FilterExpression,
} from "../dbDrivers/types";
import {
  applyRowTypeHints,
  buildPrevalidationFailedResult,
  PersistedEditValidationError,
  validatePersistedEditRecord,
} from "../table/persistedEditValidation";
import { getAtomicApplyRefusal } from "../table/tableMutationExecution";
import {
  prepareApplyChangesPlan,
  type SortConfig,
  TableDataService,
} from "../tableDataService";
import { readClipboardTextSafe, writeClipboardText } from "../utils/clipboard";
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
  private sessionGeneration: number;
  private readonly previewController: TableMutationPreviewController;
  private disposed = false;
  private hasUnsavedChanges: boolean;
  private messageEpoch = 0;
  private webviewReady = false;
  private initialized = false;
  private initializationPromise: Promise<void> | undefined;
  private initializationAttempt: { waitingForConnect: boolean } | undefined;
  private connectionGeneration = 0;
  private metadataInvalidated = false;
  private pageReadAbortController = new AbortController();
  private readonly inFlightPageRequests = new Map<
    string,
    Promise<{
      columns: ColumnTypeMeta[];
      rows: Record<string, unknown>[];
      mongoIdTypes?: Array<"objectId" | "string" | null>;
      mongoValueTypes?: Array<Record<string, MongoValueTypeHint>>;
      rowValueTypes?: Array<Record<string, MongoValueTypeHint>>;
      totalCount: number;
      executionTimeMs?: number;
    }>
  >();

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
    this.sessionGeneration = this.currentSessionGeneration();
    this.previewController = new TableMutationPreviewController({
      connectionId,
      tableName: this.getDisplayTableName(),
      connectionManager,
      tableDataService: this.svc,
      notifyWarning: (message) => {
        void vscode.window.showWarningMessage(`[RapiDB] ${message}`);
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
        this.previewController.clear();
        this.pageReadAbortController.abort();
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
      connectSubscription?.dispose();
      metadataInvalidationSubscription?.dispose();
      schemaRefreshSubscription?.dispose();
      sessionSubscription?.dispose();
      this.previewController.clear();
      this.previewSchemas.clear();
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
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${normalized.message}`,
        );
      },
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
        const currentSessionKey = currentConnectionSessionKey(
          connectionManager,
          saved.connectionId,
        );
        const hasDraft =
          (state as { tableDraft?: unknown }).tableDraft !== undefined;
        if (
          currentSessionKey === undefined ||
          (hasDraft &&
            saved.connectionSessionKey !== undefined &&
            saved.connectionSessionKey !== currentSessionKey)
        ) {
          if (hasDraft) {
            void vscode.window.showWarningMessage(
              currentSessionKey === undefined
                ? `[RapiDB] A restored table panel for "${saved.table}" was closed because its connection no longer exists. Its unsaved changes were discarded.`
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
    if (this.disposed || !this.hasUnsavedChanges) return false;
    const sessionChange = this.sessionChangeWarning();
    void vscode.window.showWarningMessage(
      sessionChange ??
        `[RapiDB] "${this.panel.title}" has unsaved changes and was kept open. Reconnect to apply them, or use Revert All to discard them.`,
    );
    return sessionChange === null;
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
    return key === undefined
      ? `[RapiDB] "${this.panel.title}" was closed because its connection was removed. Its unsaved changes were discarded.`
      : `[RapiDB] "${this.panel.title}" was closed because its connection settings changed. Its unsaved changes were discarded so they cannot be applied to a different database.`;
  }

  private closeIfSessionChanged(): boolean {
    if (this.disposed) return true;
    const warning = this.sessionChangeWarning();
    if (warning === null) return false;
    if (!this.hasUnsavedChanges && !this.initialized) {
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
    if (this.hasUnsavedChanges) void vscode.window.showWarningMessage(warning);
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
        if (parsed.payload)
          this.hasUnsavedChanges = parsed.payload.hasUnsavedChanges;
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
    this.svc.clearForConnection(this.connectionId);
    const generation = ++this.schemaRefreshGeneration;
    this.schemaRefreshPromise = this.svc
      .getColumns(this.connectionId, this.database, this.schema, this.table)
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
          error: `Schema metadata refresh failed: ${this.schemaRefreshError}`,
        });
      });
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
      const cols = await this.svc.getColumns(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
      );
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
      const error = normalizeUnknownError(err);
      await this.postMessage("tableError", { error: error.message });
    }
  }

  private postTableInit(
    columns: ColumnTypeMeta[],
    intent: TableInitPayload["intent"] = "initialize",
  ): void {
    if (this.closeIfSessionChanged()) return;
    this.initialized = true;
    this.metadataInvalidated = false;
    const snapshotColumns = structuredClone(columns);
    this.schemaSnapshot = {
      generation: this.schemaRefreshGeneration,
      connectionGeneration: this.connectionGeneration,
      columns: snapshotColumns,
      signature: JSON.stringify(snapshotColumns),
    };
    const primaryKeyColumns = columns
      .filter((column) => column.isPrimaryKey)
      .map((column) => column.name);
    const payload: TableInitPayload = {
      intent,
      columns,
      primaryKeyColumns,
      isView: this.isView,
      connectionReadOnly: this.isConnectionReadOnly(),
      connectionSessionKey: this.sessionKey ?? undefined,
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
    try {
      if (raw.refreshMetadata) this.refreshMetadata();
      await this.awaitSchemaRefresh();
      if (!isCurrentSession()) return;
      const normalizedFilters = filters as FilterExpression[];
      const normalizedSort: SortConfig | null = sort;
      const requestKey = this.buildPageRequestKey(
        page,
        pageSize,
        normalizedFilters,
        normalizedSort,
      );
      let inFlightRequest = raw.refreshMetadata
        ? undefined
        : this.inFlightPageRequests.get(requestKey);
      if (!inFlightRequest) {
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
          )
          .finally(() => {
            if (this.inFlightPageRequests.get(requestKey) === pageRequest) {
              this.inFlightPageRequests.delete(requestKey);
            }
          });
        if (!raw.refreshMetadata) {
          this.inFlightPageRequests.set(requestKey, pageRequest);
        }
        inFlightRequest = pageRequest;
      }

      const result = await inFlightRequest;
      if (!isCurrentSession()) return;
      const snapshotColumnNames = new Set(
        this.schemaSnapshot.columns.map((column) => column.name),
      );
      const extraColumns =
        this.connectionManager.getConnection(this.connectionId)?.type ===
        "mongodb"
          ? result.columns.filter(
              (column) => !snapshotColumnNames.has(column.name),
            )
          : [];
      this.postMessage("tableData", {
        fetchId,
        ...(extraColumns.length > 0 ? { extraColumns } : {}),
        rows: result.rows,
        mongoIdTypes: result.mongoIdTypes,
        mongoValueTypes: result.mongoValueTypes,
        rowValueTypes: result.rowValueTypes ?? result.mongoValueTypes,
        totalCount: result.totalCount,
        executionTimeMs: result.executionTimeMs,
      });
    } catch (err: unknown) {
      if (!isCurrentSession()) return;
      const error = normalizeUnknownError(err);
      const errMsg = error.message;
      const isFilterError =
        filters.length > 0 &&
        Boolean(
          this.connectionManager
            .getDriverCapabilities(this.connectionId)
            ?.isTableFilterError?.(errMsg),
        );
      this.postMessage("tableError", { fetchId, error: errMsg, isFilterError });
    }
  }

  private async _handleApplyChanges(payload: {
    operationId: string;
    updates?: import("../../shared/webviewContracts").RowUpdateMessagePayload[];
    insertValues?: Record<string, unknown>[];
  }): Promise<void> {
    const { operationId, updates, insertValues } = payload;
    const connectionGeneration = this.connectionGeneration;
    try {
      this.assertMutationSession(connectionGeneration);
      const snapshot = await this.awaitSchemaRefresh();
      this.assertMutationSession(connectionGeneration);
      if (!this.isCurrentSchemaSnapshot(snapshot)) {
        await this.postSchemaConflict(operationId, "applyChanges");
        return;
      }
      const driver = this.connectionManager.getDriver(this.connectionId);
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
            ...buildPrevalidationFailedResult(updateFailures, insertFailures),
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
          ...prepared.result,
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
            error: risk,
          });
          return;
        }
      }

      if (!prepared.executable && insertCount === 0) {
        if (prepared.result.warning) {
          void vscode.window.showWarningMessage(
            `[RapiDB] ${prepared.result.warning}`,
          );
        }
        this.postMessage("applyResult", { ...prepared.result, operationId });
        return;
      }

      await this.presentOrExecuteMutationPreview(
        this.previewController.createApplyChangesPreview(operationId, {
          apply: applyPlan,
          applyResultWhenEmpty: prepared.executable ? null : prepared.result,
          inserts: insertPlans,
        }),
        preparedSnapshot,
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
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
    try {
      this.assertMutationSession(connectionGeneration);
      const snapshot = await this.awaitSchemaRefresh();
      this.assertMutationSession(connectionGeneration);
      if (!this.isCurrentSchemaSnapshot(snapshot)) {
        await this.postSchemaConflict(operationId, "insertRow");
        return;
      }
      const plan = await this.svc.prepareInsertRow(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        values,
      );
      await this.presentOrExecuteMutationPreview(
        this.previewController.createInsertPreview(operationId, plan),
        snapshot,
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
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

  private async _handleDeleteRows(payload: {
    operationId: string;
    primaryKeysList?: Array<Record<string, unknown>>;
  }): Promise<void> {
    const { operationId, primaryKeysList = [] } = payload;
    const connectionGeneration = this.connectionGeneration;
    let executionPossible = false;
    try {
      this.assertMutationSession(connectionGeneration);
      const snapshot = await this.awaitSchemaRefresh();
      this.assertMutationSession(connectionGeneration);
      if (!this.isCurrentSchemaSnapshot(snapshot)) {
        await this.postSchemaConflict(operationId, "deleteRows");
        return;
      }
      const plan = await this.svc.prepareDeleteRowsPlan(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        primaryKeysList,
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

      executionPossible = true;
      await this.presentOrExecuteMutationPreview(
        this.previewController.createDeleteRowsPreview(operationId, plan),
        preparedSnapshot,
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      this.postMessage("deleteResult", {
        operationId,
        ...(executionPossible
          ? buildDeleteResult(
              primaryKeysList,
              { affectedRows: 0 },
              error.message,
            )
          : unattemptedDeleteResult(primaryKeysList, error.message)),
      });
    }
  }

  private async _handleExport(
    format: "csv" | "json",
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
    const pageMayLackSnapshot =
      connectionType === "mysql" ||
      connectionType === "mssql" ||
      connectionType === "mongodb" ||
      connectionType === "redis" ||
      connectionType === "dynamodb";
    const exportDriver = this.connectionManager.getDriver(this.connectionId);
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
    const loadChunks = (signal: AbortSignal) => {
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
              ),
          columnOrder,
          assertSession,
        ),
        assertSession,
      );
    };

    if (format === "csv") {
      await exportTableDataAsCsv({
        fileName,
        loadChunks,
        context: this.context,
      });
      return;
    }

    await exportTableDataAsJson({
      fileName,
      loadChunks,
      context: this.context,
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
    const knownColumns = new Map<string, ChunkedExportData["columns"][number]>(
      this.schemaSnapshot.columns.map((column) => [column.name, column]),
    );

    for await (const chunk of chunks) {
      for (const column of chunk.columns) knownColumns.set(column.name, column);
      const sourceColumnIndexByName = new Map(
        chunk.columns.map((column, index) => [column.name, index]),
      );
      const sourceMissingCells = new Set(
        chunk.missingCells?.map(
          ({ rowIndex, columnIndex }) => `${rowIndex}:${columnIndex}`,
        ),
      );
      const reorderedColumns = columnOrder.map((name) => {
        const column = knownColumns.get(name);
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
      yield driver?.formatTableExportChunk?.(chunk, selectedColumns) ?? chunk;
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
      await this.postSchemaConflict(payload.operationId, schema.kind);
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
    let result:
      | Awaited<ReturnType<TableMutationPreviewController["confirm"]>>
      | undefined;
    try {
      result = await this.previewController.confirm(
        payload.previewToken,
        payload.operationId,
      );
    } finally {
      const operation = this.previewOperations.get(payload.operationId);
      if (operation && result !== null) operation.executed = true;
    }
    if (!result) {
      await this.postPreviewUnavailable(payload.operationId);
      return;
    }

    await this.postMessage(result.type, result.payload);
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
