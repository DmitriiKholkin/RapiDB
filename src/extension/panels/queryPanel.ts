import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";
import { logErrorWithContext } from "../utils/errorHandling";
import {
  type QueryPanelCachedResult,
  QueryPanelController,
} from "./queryPanelController";
import { createWebviewShell } from "./webviewShell";

export class QueryPanel {
  private static readonly viewType = "rapidb.queryPanel";
  private static panels = new Map<string, QueryPanel>();
  private static sequence = 0;

  private readonly panel: vscode.WebviewPanel;
  private readonly connectionManager: ConnectionManager;
  private readonly controller: QueryPanelController;
  readonly initialConnectionId: string;
  private formatOnOpen = false;
  private isBookmarked = false;
  private lastQueryResult: QueryPanelCachedResult | null = null;
  private activeConnectionId: string;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    initialSql?: string,
    formatOnOpen?: boolean,
    isBookmarked?: boolean,
  ) {
    this.panel = panel;
    this.connectionManager = connectionManager;
    this.initialConnectionId = connectionId;
    this.activeConnectionId = connectionId;
    this.formatOnOpen = formatOnOpen ?? false;
    this.isBookmarked = isBookmarked ?? false;
    this.controller = new QueryPanelController(connectionManager, {
      getActiveConnectionId: () => this.activeConnectionId,
      getInitialConnectionId: () => this.initialConnectionId,
      getLastQueryResult: () => this.lastQueryResult,
      postMessage: (message) => {
        this.panel.webview.postMessage(message);
      },
      setActiveConnectionId: (nextConnectionId) => {
        this.activeConnectionId = nextConnectionId;
      },
      setLastQueryResult: (result) => {
        this.lastQueryResult = result;
      },
      syncTitle: () => {
        this.syncTitle();
      },
    });
    this.panel.webview.html = this.buildHtml(context, initialSql);
    this.panel.webview.onDidReceiveMessage(async (message) => {
      try {
        await this.controller.handleMessage(message);
      } catch (error: unknown) {
        const normalized = logErrorWithContext(
          "QueryPanel unhandled error",
          error,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${normalized.message}`,
        );
      }
    });

    const configWatcher = vscode.workspace.onDidChangeConfiguration((event) => {
      if (!event.affectsConfiguration("rapidb.connections")) {
        return;
      }
      this.controller.handleConnectionsChanged();
    });

    const schemaWatcher = connectionManager.onDidSchemaLoad(
      (loadedConnectionId) => {
        void this.controller.handleSchemaLoaded(loadedConnectionId);
      },
    );

    this.panel.onDidDispose(() => {
      configWatcher.dispose();
      schemaWatcher.dispose();
    });
  }

  static disposeAll(): void {
    for (const panel of QueryPanel.panels.values()) {
      try {
        panel.panel.dispose();
      } catch {}
    }
    QueryPanel.panels.clear();
  }

  static createOrShow(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    initialSql?: string,
    forceNew = false,
    formatOnOpen = false,
    isBookmarked = false,
  ): QueryPanel {
    const connection = connectionManager.getConnection(connectionId);
    const title = `SQL [${connection?.name ?? connectionId}]`;

    if (!initialSql && !forceNew) {
      for (const panel of QueryPanel.panels.values()) {
        if (panel.initialConnectionId === connectionId) {
          panel.panel.reveal(vscode.ViewColumn.One);
          return panel;
        }
      }
    }

    const panelId = `qp_${++QueryPanel.sequence}`;
    const webviewPanel = vscode.window.createWebviewPanel(
      QueryPanel.viewType,
      title,
      vscode.ViewColumn.One,
      { enableScripts: true, retainContextWhenHidden: true },
    );

    const instance = new QueryPanel(
      webviewPanel,
      context,
      connectionManager,
      connectionId,
      initialSql,
      formatOnOpen,
      isBookmarked,
    );
    QueryPanel.panels.set(panelId, instance);
    webviewPanel.onDidDispose(() => QueryPanel.panels.delete(panelId));
    return instance;
  }

  private syncTitle(): void {
    const connection = this.connectionManager.getConnection(
      this.activeConnectionId,
    );
    this.panel.title = `Query — ${connection?.name ?? this.activeConnectionId}`;
  }

  private buildHtml(
    context: vscode.ExtensionContext,
    initialSql?: string,
  ): string {
    const connection = this.connectionManager.getConnection(
      this.initialConnectionId,
    );
    const connectionType = connection?.type ?? "";

    return createWebviewShell({
      context,
      webview: this.panel.webview,
      title: "RapiDB Query",
      initialState: {
        view: "query",
        connectionId: this.initialConnectionId,
        connectionType,
        initialSql: initialSql ?? "",
        formatOnOpen: this.formatOnOpen ?? false,
        isBookmarked: this.isBookmarked ?? false,
      },
      includeMediaRoot: true,
      extraCspDirectives: ["worker-src blob:"],
      htmlStyles: "height: 100%; overflow: hidden;",
      bodyStyles: "height: 100%; overflow: hidden;",
      rootStyles: "height: 100vh;",
      extraStyles: `
        .monaco-editor .scrollbar .slider { background: var(--vscode-scrollbarSlider-background) !important; border-radius: 4px; }
        .monaco-editor .scrollbar .slider:hover { background: var(--vscode-scrollbarSlider-hoverBackground) !important; }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--vscode-scrollbarSlider-background); border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--vscode-scrollbarSlider-hoverBackground); }
      `,
    });
  }
}
