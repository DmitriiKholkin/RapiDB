import * as vscode from "vscode";
import { coerceFilterExpressions } from "../../shared/tableTypes";
import { parseTablePanelMessage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type { FilterExpression } from "../dbDrivers/types";
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
import { TableMutationPreviewController } from "./tableMutationPreviewController";
import { createWebviewShell } from "./webviewShell";

const EXPORT_CHUNK_SIZE = 500;

type ExportPayload = {
  sort?: unknown;
  filters?: unknown[];
  limitToPage?: { page: number; pageSize: number };
};
const FILTER_ERROR_RE =
  /^\[RapiDB Filter\]|invalid input syntax|invalid cidr|malformed array|not a valid (binary|hex|uuid)|syntax error in input|invalid value for type|invalid number|operator does not exist|conversion failed|arithmetic overflow|ORA-0(1841|1843|1858|1861|6502)|ORA-01722|incorrect (date|datetime|time)|Incorrect integer value|Truncated incorrect|data truncat/i;

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

  static disposeAll(): void {
    for (const panel of TablePanel.panels.values()) {
      try {
        panel.panel.dispose();
      } catch {}
    }
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
  ): void {
    const key = TablePanel.panelKey(connectionId, database, schema, table);
    const existing = TablePanel.panels.get(key);
    if (existing) {
      existing.panel.reveal(vscode.ViewColumn.One);
      return;
    }

    const buildTitle = () => {
      const connName =
        connectionManager.getConnection(connectionId)?.name ?? connectionId;
      const objType = isView ? "view" : "table";
      const schemaPrefix = schema ? `${schema}.` : "";
      return `${schemaPrefix}${table} (${objType}) [${connName}]`;
    };
    const panel = vscode.window.createWebviewPanel(
      TablePanel.viewType,
      buildTitle(),
      vscode.ViewColumn.One,
      { enableScripts: true, retainContextWhenHidden: true },
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
    panel.onDidDispose(() => {
      confSub.dispose();
      disconnSub.dispose();
    });
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
      const result = await this.svc.getPage(
        this.connectionId,
        this.database,
        this.schema,
        this.table,
        page,
        pageSize,
        filters as FilterExpression[],
        sort as SortConfig | null,
      );
      this.postMessage("tableData", {
        fetchId,
        rows: result.rows,
        totalCount: result.totalCount,
      });
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      const errMsg = error.message;
      const isFilterError =
        filters.length > 0 &&
        FILTER_ERROR_RE.test(errMsg) &&
        !/arithmetic overflow/i.test(errMsg);
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

      await this.postMessage(
        "tableMutationPreview",
        this.previewController.createApplyChangesPreview({
          apply: prepared.executable ? prepared.plan : null,
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
      await this.postMessage(
        "tableMutationPreview",
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

      await this.postMessage(
        "tableMutationPreview",
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
    columns: { name: string }[];
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
        defaultPageSize: this.connectionManager.getDefaultPageSize(),
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
