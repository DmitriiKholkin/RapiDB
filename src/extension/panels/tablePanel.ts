import * as vscode from "vscode";
import { getDbObjectKindDisplayLabel } from "../../shared/dbObjectKinds";
import { coerceFilterExpressions } from "../../shared/tableTypes";
import {
  parseTablePanelMessage,
  type TableInitPayload,
  type TableMutationPreviewPayload,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import {
  buildDeleteResult,
  unattemptedDeleteResult,
} from "../dbDrivers/deleteOutcomes";
import type { ColumnTypeMeta, FilterExpression } from "../dbDrivers/types";
import {
  buildPrevalidationFailedResult,
  PersistedEditValidationError,
  validatePersistedEditRecord,
} from "../table/persistedEditValidation";
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

type TablePanelObjectKind = "table" | "view" | "materializedView";

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

type ExportPayload = {
  sort?: unknown;
  filters?: unknown[];
  limitToPage?: { page: number; pageSize: number };
  columnOrder?: string[];
};

interface TableSchemaSnapshot {
  readonly generation: number;
  readonly columns: ColumnTypeMeta[];
  readonly signature: string;
}

export class TablePanel {
  private static readonly viewType = "rapidb.tablePanel";

  private static panels = new Map<string, TablePanel>();

  private readonly panel: vscode.WebviewPanel;
  private readonly context: vscode.ExtensionContext;
  private readonly svc: TableDataService;
  private readonly connectionManager: ConnectionManager;
  private readonly connectionId: string;
  private readonly database: string;
  private readonly schema: string;
  private readonly table: string;
  private readonly isView: boolean;
  private readonly previewController: TableMutationPreviewController;
  private readonly inFlightPageRequests = new Map<
    string,
    Promise<{
      rows: Record<string, unknown>[];
      mongoIdTypes?: Array<"objectId" | "string" | null>;
      totalCount: number;
      executionTimeMs?: number;
    }>
  >();

  private schemaSnapshot: TableSchemaSnapshot = {
    generation: 0,
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
    this.context = context;
    this.svc = new TableDataService(connectionManager);
    this.connectionManager = connectionManager;
    this.connectionId = connectionId;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.isView = isView;
    this.previewController = new TableMutationPreviewController({
      connectionId,
      tableName: table,
      connectionManager,
      tableDataService: this.svc,
      notifyWarning: (message) => {
        void vscode.window.showWarningMessage(`[RapiDB] ${message}`);
      },
    });

    this.panel.webview.html = this.buildHtml(context);

    const key = TablePanel.panelKey(connectionId, database, schema, table);
    const schemaRefreshSubscription = connectionManager.onDidRefreshSchemas?.(
      (refreshedConnectionId) => {
        if (refreshedConnectionId && refreshedConnectionId !== connectionId) {
          return;
        }
        this.svc.clearForConnection(connectionId);
        const generation = ++this.schemaRefreshGeneration;
        this.schemaRefreshPromise = this.svc
          .getColumns(connectionId, database, schema, table)
          .then((columns) => {
            if (generation === this.schemaRefreshGeneration) {
              this.schemaRefreshError = null;
              this.postTableInit(columns, "metadataRefresh");
            }
          })
          .catch((error: unknown) => {
            if (generation !== this.schemaRefreshGeneration) return;
            this.schemaRefreshError = normalizeUnknownError(error).message;
            void this.postMessage("tableError", {
              error: `Schema metadata refresh failed: ${this.schemaRefreshError}`,
            });
          });
      },
    );
    this.panel.onDidDispose(() => {
      schemaRefreshSubscription?.dispose();
      this.previewController.clear();
      this.previewSchemas.clear();
      TablePanel.panels.delete(key);

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

  private postMessage(type: string, payload: unknown): Thenable<boolean> {
    return this.panel.webview.postMessage({ type, payload });
  }

  private shouldSkipTableMutationPreview(): boolean {
    return this.connectionManager.getSkipTableMutationPreview() === true;
  }

  private async presentOrExecuteMutationPreview(
    preview: TableMutationPreviewPayload,
    snapshot: TableSchemaSnapshot,
  ): Promise<void> {
    if (!this.isCurrentSchemaSnapshot(snapshot)) {
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
    // A refresh advances the requested generation before replacing the committed
    // snapshot. Check again in the caller after every await, never pairing that
    // new generation with old columns while its metadata request is pending.
    return (
      snapshot === this.schemaSnapshot &&
      snapshot.generation === this.schemaRefreshGeneration &&
      this.schemaRefreshError === null
    );
  }

  private async postSchemaConflict(
    operationId: string,
    kind: TableMutationPreviewPayload["kind"],
  ): Promise<void> {
    await this.postMessage(
      kind === "applyChanges"
        ? "applyResult"
        : kind === "deleteRows"
          ? "deleteResult"
          : "insertResult",
      {
        operationId,
        success: false,
        error:
          "Schema metadata changed. The previous mutation preview cannot be applied; pending work is retained. Review the schema conflict before retrying.",
      },
    );
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
    disposePanelInstances(TablePanel.panels.values(), (panel) => {
      panel.panel.dispose();
    });
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

    const buildTitle = () => {
      const connection = connectionManager.getConnection(connectionId);
      const connName = connection?.name ?? connectionId;
      const connectionType = connection?.type;
      const effectiveObjectKind = objectKind ?? (isView ? "view" : "table");
      const objType = titleObjectKindLabel(connectionType, effectiveObjectKind);
      const schemaPrefix =
        schema && shouldShowSchemaPrefix(connectionType) ? `${schema}.` : "";
      return `${schemaPrefix}${table} (${objType}) [${connName}]`;
    };
    const panel = vscode.window.createWebviewPanel(
      TablePanel.viewType,
      buildTitle(),
      vscode.ViewColumn.One,
      createPanelWebviewOptions(TABLE_PANEL_RETENTION_MODE),
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

    attachConnectionScopedPanelLifecycle(
      panel,
      connectionManager,
      connectionId,
      () => {
        panel.title = buildTitle();
      },
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
          await this._handleWriteClipboard(parsed.payload.text);
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

  private async _handleWriteClipboard(text: string): Promise<void> {
    await writeClipboardText(text);
  }

  private async _handleReady(): Promise<void> {
    try {
      const generation = this.schemaRefreshGeneration;
      const cols = await this.svc.getColumns(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
      );
      if (generation !== this.schemaRefreshGeneration) {
        return;
      }
      this.postTableInit(cols);
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      this.postMessage("tableError", { error: error.message });
    }
  }

  private postTableInit(
    columns: ColumnTypeMeta[],
    intent: TableInitPayload["intent"] = "initialize",
  ): void {
    const snapshotColumns = structuredClone(columns);
    this.schemaSnapshot = {
      generation: this.schemaRefreshGeneration,
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
    const { page, pageSize } = this.normalizePageRequest(
      raw.page,
      raw.pageSize,
    );
    const filters = coerceFilterExpressions(raw.filters);
    const sort = raw.sort ?? null;
    try {
      await this.awaitSchemaRefresh();
      const normalizedFilters = filters as FilterExpression[];
      const normalizedSort = sort as SortConfig | null;
      const requestKey = this.buildPageRequestKey(
        page,
        pageSize,
        normalizedFilters,
        normalizedSort,
      );
      let inFlightRequest = this.inFlightPageRequests.get(requestKey);
      if (!inFlightRequest) {
        inFlightRequest = this.svc
          .getPage(
            this.connectionId,
            this.database,
            this.schema,
            this.table,
            page,
            pageSize,
            normalizedFilters,
            normalizedSort,
          )
          .finally(() => {
            this.inFlightPageRequests.delete(requestKey);
          });
        this.inFlightPageRequests.set(requestKey, inFlightRequest);
      }

      const result = await inFlightRequest;
      this.postMessage("tableData", {
        fetchId,
        rows: result.rows,
        mongoIdTypes: result.mongoIdTypes,
        totalCount: result.totalCount,
        executionTimeMs: result.executionTimeMs,
      });
    } catch (err: unknown) {
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
    try {
      const snapshot = await this.awaitSchemaRefresh();
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
          validatePersistedEditRecord(driver, update.changes, columnMap),
        );
        const insertFailures = (insertValues ?? []).map((values) =>
          validatePersistedEditRecord(driver, values, columnMap),
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
                    .map(({ primaryKeys, changes, originalValues }) =>
                      previewBuilder(
                        "update",
                        this.database,
                        this.schema,
                        this.table,
                        {
                          primaryKeys,
                          changes,
                          originalValues,
                        },
                      ),
                    ),
                )
              ).flat(),
            }
          : prepared.executable
            ? prepared.plan
            : null;

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

      const insertCount = insertPlans?.length ?? 0;
      if (!this.isCurrentSchemaSnapshot(snapshot)) {
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
        const risk = await driver?.getMutationAtomicityRisk?.(
          this.database,
          this.schema,
          this.table,
        );

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
        snapshot,
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
    try {
      const snapshot = await this.awaitSchemaRefresh();
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
    let executionPossible = false;
    try {
      const snapshot = await this.awaitSchemaRefresh();
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

      if (!this.isCurrentSchemaSnapshot(snapshot)) {
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
        snapshot,
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
    const {
      sort = null,
      filters = [],
      limitToPage,
      columnOrder,
    } = payload ?? {};
    const normalizedLimitToPage = limitToPage
      ? this.normalizePageRequest(limitToPage.page, limitToPage.pageSize)
      : undefined;
    const fileName = this.schema ? `${this.schema}_${this.table}` : this.table;
    const filterExpressions = coerceFilterExpressions(filters);
    const loadChunks = (signal: AbortSignal) =>
      this.reorderChunks(
        normalizedLimitToPage
          ? this._pageAsChunks(
              normalizedLimitToPage.page,
              normalizedLimitToPage.pageSize,
              sort as SortConfig | null,
              filterExpressions,
              signal,
            )
          : this.svc.exportAll(
              this.connectionId,
              this.database,
              this.schema,
              this.table,
              EXPORT_CHUNK_SIZE,
              sort as SortConfig | null,
              filterExpressions,
              signal,
            ),
        columnOrder,
      );

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

  private async *reorderChunks(
    chunks: AsyncIterable<ChunkedExportData>,
    columnOrder?: string[],
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
      const reorderedColumns = columnOrder.map((name) => {
        const column = knownColumns.get(name);
        if (!column) {
          throw new Error(
            `Unknown export column "${name}". Refresh table metadata and retry.`,
          );
        }
        return column;
      });

      const reorderedRows = chunk.rows.map((row) => {
        const newRow: Record<string, unknown> = {};
        for (const col of reorderedColumns) {
          newRow[col.name] = row[col.name];
        }
        return newRow;
      });

      yield { columns: reorderedColumns, rows: reorderedRows };
    }
  }

  private async *_pageAsChunks(
    page: number,
    pageSize: number,
    sort: SortConfig | null,
    filters: FilterExpression[],
    signal: AbortSignal,
  ): AsyncGenerator<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
  }> {
    signal.throwIfAborted();
    const result = await this.svc.getPage(
      this.connectionId,
      this.database,
      this.schema,
      this.table,
      page,
      pageSize,
      filters,
      sort,
      true,
      signal,
    );
    signal.throwIfAborted();
    yield { columns: result.columns, rows: result.rows };
  }

  private async _handleConfirmMutationPreview(payload: {
    operationId: string;
    previewToken: string;
  }): Promise<void> {
    const schema = this.previewSchemas.get(payload.previewToken);
    if (!schema || schema.operationId !== payload.operationId) return;
    let currentSnapshot: TableSchemaSnapshot;
    // Refresh errors are reported as a mutation result with the original ID.
    try {
      currentSnapshot = await this.awaitSchemaRefresh();
    } catch {
      this.previewSchemas.delete(payload.previewToken);
      this.previewController.cancel(payload.previewToken, payload.operationId);
      await this.postSchemaConflict(payload.operationId, schema.kind);
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
    // No await between the final current-snapshot check and starting execution.
    const result = await this.previewController.confirm(
      payload.previewToken,
      payload.operationId,
    );
    if (!result) {
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
      title: `${this.isView ? "View" : "Table"} - ${this.table}`,
      initialState: {
        view: "table",
        connectionId: this.connectionId,
        database: this.database,
        schema: this.schema,
        table: this.table,
        isView: this.isView,
        connectionReadOnly: this.isConnectionReadOnly(),
        mongoRowIdentity:
          this.connectionManager.getConnection(this.connectionId)?.type ===
          "mongodb",
        defaultPageSize: this.connectionManager.getDefaultPageSize(),
        panelRetentionMode: TABLE_PANEL_RETENTION_MODE,
      },
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
}
