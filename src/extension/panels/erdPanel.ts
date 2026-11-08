import * as vscode from "vscode";
import { parseErdPanelMessage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { ErdGraphService } from "../services/erdGraphService";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import {
  attachConnectionScopedPanelLifecycle,
  attachPanelDisposables,
  attachPanelMessageHandler,
  disposePanelInstances,
} from "./panelLifecycle";
import { createPanelWebviewOptions } from "./panelRetentionPolicy";
import { TablePanel } from "./tablePanel";
import {
  createWebviewShell,
  FULLSCREEN_WEBVIEW_SHELL_LAYOUT,
} from "./webviewShell";

interface ErdPanelScope {
  connectionId: string;
  database?: string;
  schema?: string;
}

const ERD_PANEL_RETENTION_MODE = "rehydrate" as const;

function isErdPanelSerialization(value: unknown): value is ErdPanelScope {
  if (!value || typeof value !== "object") return false;
  const state = value as Record<string, unknown>;
  return (
    typeof state.connectionId === "string" &&
    state.connectionId.trim().length > 0 &&
    (state.database === undefined || typeof state.database === "string") &&
    (state.schema === undefined || typeof state.schema === "string")
  );
}

export class ErdPanel {
  private static readonly viewType = "rapidb.erdPanel";
  private static panels = new Map<string, ErdPanel>();
  private static graphService: ErdGraphService | null = null;

  private readonly panel: vscode.WebviewPanel;
  private readonly context: vscode.ExtensionContext;
  private readonly connectionManager: ConnectionManager;
  private readonly scope: ErdPanelScope;
  private loadGeneration = 0;
  private loadAbortController: AbortController | undefined;
  private disposed = false;
  private partialWarned = false;
  private lastLoadFailed = false;
  private awaitingConnect = false;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    scope: ErdPanelScope,
  ) {
    this.panel = panel;
    this.context = context;
    this.connectionManager = connectionManager;
    this.scope = scope;

    this.panel.webview.html = this.buildHtml();

    const key = ErdPanel.key(scope);
    this.panel.onDidDispose(() => {
      this.disposed = true;
      ++this.loadGeneration;
      this.loadAbortController?.abort();
      this.loadAbortController = undefined;
      if (ErdPanel.panels.get(key) === this) {
        ErdPanel.panels.delete(key);
      }
    });

    const connectSubscription = connectionManager.onDidConnect?.(() => {
      void this.retryAfterConnect();
    });
    if (connectSubscription) {
      attachPanelDisposables(this.panel, connectSubscription);
    }

    attachPanelMessageHandler(
      this.panel,
      (message) => this.handleMessage(message),
      (error) => {
        const normalized = logErrorWithContext(
          "ErdPanel unhandled error",
          error,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${normalized.message}`,
        );
      },
    );
  }

  static disposeAll(): void {
    disposePanelInstances(ErdPanel.panels.values(), (panel) => {
      panel.panel.dispose();
    });
    ErdPanel.panels.clear();

    ErdPanel.graphService?.dispose();
    ErdPanel.graphService = null;
  }

  static registerSerializer(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
  ): vscode.Disposable {
    if (typeof vscode.window.registerWebviewPanelSerializer !== "function") {
      return { dispose: () => undefined };
    }
    return vscode.window.registerWebviewPanelSerializer(ErdPanel.viewType, {
      async deserializeWebviewPanel(panel, state) {
        const saved =
          state !== null && typeof state === "object"
            ? (state as { initialState?: unknown }).initialState
            : undefined;
        if (!isErdPanelSerialization(saved)) {
          panel.dispose();
          return;
        }
        const scope: ErdPanelScope = {
          connectionId: saved.connectionId.trim(),
          database: saved.database,
          schema: saved.schema,
        };
        if (ErdPanel.panels.has(ErdPanel.key(scope))) {
          panel.dispose();
          return;
        }
        panel.title = ErdPanel.buildPanelTitle(connectionManager, scope);
        ErdPanel.trackPanel(panel, context, connectionManager, scope);
      },
    });
  }

  static createOrShow(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    scope: ErdPanelScope,
  ): void {
    const key = ErdPanel.key(scope);
    const existing = ErdPanel.panels.get(key);
    if (existing) {
      existing.panel.reveal(vscode.ViewColumn.One);
      if (existing.lastLoadFailed) {
        void existing.loadGraph(false);
      }
      return;
    }

    const panel = vscode.window.createWebviewPanel(
      ErdPanel.viewType,
      ErdPanel.buildPanelTitle(connectionManager, scope),
      vscode.ViewColumn.One,
      createPanelWebviewOptions(ERD_PANEL_RETENTION_MODE),
    );

    ErdPanel.trackPanel(panel, context, connectionManager, scope);
  }

  private static trackPanel(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    scope: ErdPanelScope,
  ): void {
    if (!ErdPanel.graphService) {
      ErdPanel.graphService = new ErdGraphService(connectionManager);
    }

    const instance = new ErdPanel(panel, context, connectionManager, scope);
    ErdPanel.panels.set(ErdPanel.key(scope), instance);

    attachConnectionScopedPanelLifecycle(
      panel,
      connectionManager,
      scope.connectionId,
      () => {
        panel.title = ErdPanel.buildPanelTitle(connectionManager, scope);
      },
    );
  }

  private static buildPanelTitle(
    connectionManager: ConnectionManager,
    scope: ErdPanelScope,
  ): string {
    const connectionName =
      connectionManager.getConnection(scope.connectionId)?.name ??
      scope.connectionId;

    if (scope.database && scope.schema) {
      return `${scope.database}.${scope.schema} (ERD) [${connectionName}]`;
    }

    if (scope.database) {
      return `${scope.database} (ERD) [${connectionName}]`;
    }

    if (scope.schema) {
      return `${scope.schema} (ERD) [${connectionName}]`;
    }

    return `ERD [${connectionName}]`;
  }

  private static key(scope: ErdPanelScope): string {
    return JSON.stringify([
      scope.connectionId.trim(),
      scope.database || null,
      scope.schema || null,
    ]);
  }

  private async handleMessage(message: unknown): Promise<void> {
    const parsed = parseErdPanelMessage(message);
    if (!parsed) {
      return;
    }

    switch (parsed.type) {
      case "ready":
        await this.loadGraph(false);
        break;

      case "reload":
        await this.loadGraph(true);
        break;

      case "openTableData": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        TablePanel.createOrShow(
          this.context,
          this.connectionManager,
          this.scope.connectionId,
          payload.database ?? this.scope.database ?? "",
          payload.schema ?? this.scope.schema ?? "",
          payload.table,
          payload.isView ?? false,
        );
        break;
      }
    }
  }

  private isConnectionReady(): boolean {
    const { connectionId } = this.scope;
    return (
      !this.connectionManager.isConnecting?.(connectionId) &&
      this.connectionManager.isConnected?.(connectionId) !== false
    );
  }

  private async retryAfterConnect(): Promise<void> {
    // onDidConnect fires before the connect attempt is finalized.
    await Promise.resolve();
    if (
      this.disposed ||
      !this.lastLoadFailed ||
      !this.awaitingConnect ||
      !this.isConnectionReady()
    ) {
      return;
    }
    await this.loadGraph(false);
  }

  private async loadGraph(forceReload: boolean): Promise<void> {
    const graphService = ErdPanel.graphService;
    if (!graphService || this.disposed) {
      return;
    }
    this.lastLoadFailed = false;
    this.awaitingConnect = false;
    const generation = ++this.loadGeneration;
    this.loadAbortController?.abort();
    const abortController = new AbortController();
    this.loadAbortController = abortController;

    this.post("erdLoading", {
      forceReload,
    });

    try {
      const result = await graphService.getGraph(
        {
          connectionId: this.scope.connectionId,
          database: this.scope.database,
          schema: this.scope.schema,
        },
        forceReload,
        abortController.signal,
      );
      if (generation !== this.loadGeneration) return;

      this.post("erdGraph", {
        graph: result.graph,
        fromCache: result.fromCache,
        partial: result.partial,
        loadedAt: new Date().toISOString(),
      });
      const metadataIncomplete =
        result.graph.metadataIncomplete === true ||
        (result.partial && !result.graph.omittedTables?.length);
      if (!metadataIncomplete) {
        this.partialWarned = false;
      } else if (!result.fromCache || !this.partialWarned) {
        this.partialWarned = true;
        void vscode.window.showWarningMessage(
          "[RapiDB] Some schema metadata could not be loaded, so this ERD may be incomplete. Reload the diagram to try again.",
        );
      }
    } catch (error) {
      if (generation !== this.loadGeneration) return;
      this.lastLoadFailed = true;
      this.awaitingConnect = !this.isConnectionReady();
      const normalized = normalizeUnknownError(error);
      this.post("erdError", { error: normalized.message });
    } finally {
      if (this.loadAbortController === abortController) {
        this.loadAbortController = undefined;
      }
    }
  }

  private buildHtml(): string {
    return createWebviewShell({
      context: this.context,
      webview: this.panel.webview,
      title: "ERD",
      initialState: {
        view: "erd",
        connectionId: this.scope.connectionId,
        database: this.scope.database,
        schema: this.scope.schema,
        panelRetentionMode: ERD_PANEL_RETENTION_MODE,
      },
      ...FULLSCREEN_WEBVIEW_SHELL_LAYOUT,
    });
  }

  private post(type: string, payload: unknown): void {
    if (this.disposed) return;
    void this.panel.webview.postMessage({ type, payload });
  }
}
