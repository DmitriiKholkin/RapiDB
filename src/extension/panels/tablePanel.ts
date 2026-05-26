import * as vscode from "vscode";
import { getDbObjectKindDisplayLabel } from "../../shared/dbObjectKinds";
import { coerceFilterExpressions } from "../../shared/tableTypes";
import {
  parseTablePanelMessage,
  type TableMutationPreviewPayload,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type { ColumnTypeMeta, FilterExpression } from "../dbDrivers/types";
import {
  prepareApplyChangesPlan,
  type SortConfig,
  TableDataService,
} from "../tableDataService";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import {
  exportTableDataAsCsv,
  exportTableDataAsJson,
} from "../utils/exportService";
import {
  attachPanelDisposables,
  disposePanelInstances,
} from "./panelLifecycle";
import { createPanelWebviewOptions } from "./panelRetentionPolicy";
import { TableMutationPreviewController } from "./tableMutationPreviewController";
import { createWebviewShell } from "./webviewShell";

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
};

export class TablePanel {
  private static readonly viewType = "rapidb.tablePanel";

  private static panels = new Map<string, TablePanel>();

  private readonly panel: vscode.WebviewPanel;
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
      totalCount: number;
    }>
  >();

  private cachedColumns: import("../dbDrivers/types").ColumnTypeMeta[] = [];

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
    this.panel.onDidDispose(() => {
      this.previewController.clear();
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

  private postMessage(type: string, payload: unknown): Thenable<boolean> {
    return this.panel.webview.postMessage({ type, payload });
  }

  private shouldSkipTableMutationPreview(): boolean {
    const managerWithPreviewSetting = this
      .connectionManager as ConnectionManager & {
      getSkipTableMutationPreview?: () => boolean;
    };
    return managerWithPreviewSetting.getSkipTableMutationPreview?.() === true;
  }

  private async presentOrExecuteMutationPreview(
    preview: TableMutationPreviewPayload,
  ): Promise<void> {
    if (!this.shouldSkipTableMutationPreview()) {
      await this.postMessage("tableMutationPreview", preview);
      return;
    }

    const result = await this.previewController.confirm(preview.previewToken);
    if (!result) {
      return;
    }

    await this.postMessage(result.type, result.payload);
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
    attachPanelDisposables(panel, confSub, disconnSub);
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
        await this._handleExportCSV(parsed.payload);
        break;
      case "exportJSON":
        await this._handleExportJSON(parsed.payload);
        break;
      case "confirmMutationPreview":
        if (parsed.payload)
          await this._handleConfirmMutationPreview(parsed.payload);
        break;
      case "cancelMutationPreview":
        if (parsed.payload) this._handleCancelMutationPreview(parsed.payload);
        break;
    }
  }

  private async _handleReady(): Promise<void> {
    try {
      const cols = await this.svc.getColumns(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
      );
      this.cachedColumns = cols;
      const pkCols = cols.filter((c) => c.isPrimaryKey).map((c) => c.name);
      this.postMessage("tableInit", {
        columns: cols,
        primaryKeyColumns: pkCols,
        isView: this.isView,
        connectionReadOnly: this.isConnectionReadOnly(),
      });
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      this.postMessage("tableError", { error: error.message });
    }
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
        totalCount: result.totalCount,
      });
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      const errMsg = error.message;
      const managerWithCapabilities = this
        .connectionManager as ConnectionManager & {
        getDriverCapabilities?: (
          connectionId: string,
        ) => { isTableFilterError?: (message: string) => boolean } | undefined;
      };
      const isFilterError =
        filters.length > 0 &&
        Boolean(
          managerWithCapabilities
            .getDriverCapabilities?.(this.connectionId)
            ?.isTableFilterError?.(errMsg),
        );
      this.postMessage("tableError", { fetchId, error: errMsg, isFilterError });
    }
  }

  private async _handleApplyChanges(payload: {
    updates?: import("../../shared/webviewContracts").RowUpdateMessagePayload[];
    insertValues?: Record<string, unknown>;
  }): Promise<void> {
    const { updates, insertValues } = payload;
    try {
      const prepared = prepareApplyChangesPlan(
        this.connectionManager,
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        updates ?? [],
        this.cachedColumns,
      );
      const driver = this.connectionManager.getDriver(this.connectionId);
      const previewBuilder = driver?.buildMutationPreviewStatements;
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
                    .map(({ primaryKeys, changes }) =>
                      previewBuilder(
                        "update",
                        this.database,
                        this.schema,
                        this.table,
                        {
                          primaryKeys,
                          changes,
                        },
                      ),
                    ),
                )
              ).flat(),
            }
          : prepared.executable
            ? prepared.plan
            : null;

      const insertPlan =
        insertValues !== undefined
          ? await this.svc.prepareInsertRow(
              this.connectionId,
              this.database,
              this.schema,
              this.table,
              insertValues,
            )
          : null;

      const mutationStatementCount =
        (insertPlan ? 1 : 0) +
        (prepared.executable ? prepared.plan.operations.length : 0);
      if (mutationStatementCount > 1) {
        const driver = this.connectionManager.getDriver(this.connectionId);
        const risk = await driver?.getMutationAtomicityRisk?.(
          this.database,
          this.schema,
          this.table,
        );

        if (risk) {
          this.postMessage("applyResult", {
            success: false,
            error: risk,
          });
          return;
        }
      }

      if (!prepared.executable && !insertPlan) {
        if (prepared.result.warning) {
          void vscode.window.showWarningMessage(
            `[RapiDB] ${prepared.result.warning}`,
          );
        }
        this.postMessage("applyResult", prepared.result);
        return;
      }

      await this.presentOrExecuteMutationPreview(
        this.previewController.createApplyChangesPreview({
          apply: applyPlan,
          applyResultWhenEmpty: prepared.executable ? null : prepared.result,
          insert: insertPlan,
        }),
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      this.postMessage("applyResult", { success: false, error: error.message });
    }
  }

  private async _handleInsertRow(payload: {
    values?: Record<string, unknown>;
  }): Promise<void> {
    const { values = {} } = payload;
    try {
      const plan = await this.svc.prepareInsertRow(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        values,
      );
      await this.presentOrExecuteMutationPreview(
        this.previewController.createInsertPreview(plan),
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      this.postMessage("insertResult", {
        success: false,
        error: error.message,
      });
    }
  }

  private async _handleDeleteRows(payload: {
    primaryKeysList?: Array<Record<string, unknown>>;
  }): Promise<void> {
    const { primaryKeysList = [] } = payload;
    try {
      const plan = await this.svc.prepareDeleteRowsPlan(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        primaryKeysList,
      );

      if (!plan) {
        this.postMessage("deleteResult", { success: true });
        return;
      }

      await this.presentOrExecuteMutationPreview(
        this.previewController.createDeleteRowsPreview(plan),
      );
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      this.postMessage("deleteResult", {
        success: false,
        error: error.message,
      });
    }
  }

  private async _handleExportCSV(
    payload: ExportPayload | undefined,
  ): Promise<void> {
    await this._handleExport("csv", payload);
  }

  private async _handleExportJSON(
    payload: ExportPayload | undefined,
  ): Promise<void> {
    await this._handleExport("json", payload);
  }

  private async _handleExport(
    format: "csv" | "json",
    payload: ExportPayload | undefined,
  ): Promise<void> {
    const { sort = null, filters = [], limitToPage } = payload ?? {};
    const normalizedLimitToPage = limitToPage
      ? this.normalizePageRequest(limitToPage.page, limitToPage.pageSize)
      : undefined;
    const fileName = this.schema ? `${this.schema}_${this.table}` : this.table;
    const filterExpressions = coerceFilterExpressions(filters);
    const loadChunks = (signal: AbortSignal) =>
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
          );

    if (format === "csv") {
      await exportTableDataAsCsv({ fileName, loadChunks });
      return;
    }

    await exportTableDataAsJson({ fileName, loadChunks });
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
    if (signal.aborted) return;
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
    );
    yield { columns: result.columns, rows: result.rows };
  }

  private async _handleConfirmMutationPreview(payload: {
    previewToken: string;
  }): Promise<void> {
    const result = await this.previewController.confirm(payload.previewToken);
    if (!result) {
      return;
    }

    await this.postMessage(result.type, result.payload);
  }

  private _handleCancelMutationPreview(payload: {
    previewToken: string;
  }): void {
    this.previewController.cancel(payload.previewToken);
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
        defaultPageSize: this.connectionManager.getDefaultPageSize(),
        panelRetentionMode: TABLE_PANEL_RETENTION_MODE,
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
