import * as vscode from "vscode";
import type { QueryEditorLanguage } from "../../shared/webviewContracts";
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
  private readonly formatOnOpen: boolean | undefined;
  private isBookmarked = false;
  private editorLanguage: QueryEditorLanguage | undefined;
  private lastQueryResult: QueryPanelCachedResult | null = null;
  private activeConnectionId: string;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    initialQueryText?: string,
    formatOnOpen?: boolean,
    isBookmarked?: boolean,
    editorLanguage?: QueryEditorLanguage,
  ) {
    this.panel = panel;
    this.connectionManager = connectionManager;
    this.initialConnectionId = connectionId;
    this.activeConnectionId = connectionId;
    this.formatOnOpen = formatOnOpen;
    this.isBookmarked = isBookmarked ?? false;
    this.editorLanguage = editorLanguage;
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
    this.panel.webview.html = this.buildHtml(context, initialQueryText);
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
    const connectWatcher = connectionManager.onDidConnect(() => {
      this.controller.handleConnectionsChanged();
    });
    const disconnectWatcher = connectionManager.onDidDisconnect(() => {
      this.controller.handleConnectionsChanged();
    });
    const schemaRefreshWatcher = connectionManager.onDidRefreshSchemas(() => {
      this.controller.handleConnectionsChanged();
    });

    this.panel.onDidDispose(() => {
      configWatcher.dispose();
      schemaWatcher.dispose();
      connectWatcher.dispose();
      disconnectWatcher.dispose();
      schemaRefreshWatcher.dispose();
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
    initialQueryText?: string,
    forceNew = false,
    formatOnOpen?: boolean,
    isBookmarked = false,
    editorLanguage?: QueryEditorLanguage,
  ): QueryPanel {
    const connection = connectionManager.getConnection(connectionId);
    const title = `Query [${connection?.name ?? connectionId}]`;

    if (!initialQueryText && !forceNew) {
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
      initialQueryText,
      formatOnOpen,
      isBookmarked,
      editorLanguage,
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
    initialQueryText?: string,
  ): string {
    const connection = this.connectionManager.getConnection(
      this.initialConnectionId,
    );
    const connectionType = connection?.type ?? "";
    const resolvedQueryText = initialQueryText;
    const managerWithPresentation = this
      .connectionManager as ConnectionManager & {
      getQueryEditorPresentation?: (
        connectionId: string,
      ) =>
        | import("../../shared/webviewContracts").QueryEditorPresentation
        | undefined;
    };
    const driverEditorPresentation =
      managerWithPresentation.getQueryEditorPresentation?.(
        this.initialConnectionId,
      );
    const editorPresentation = driverEditorPresentation
      ? {
          ...driverEditorPresentation,
          ...(this.formatOnOpen !== undefined
            ? { formatOnOpen: this.formatOnOpen }
            : {}),
          ...(this.editorLanguage !== undefined
            ? { editorLanguage: this.editorLanguage }
            : {}),
        }
      : this.editorLanguage !== undefined || this.formatOnOpen !== undefined
        ? {
            formatOnOpen: this.formatOnOpen,
            editorLanguage: this.editorLanguage,
          }
        : undefined;

    return createWebviewShell({
      context,
      webview: this.panel.webview,
      title: "RapiDB Query",
      initialState: {
        view: "query",
        connectionId: this.initialConnectionId,
        connectionType,
        queryText: resolvedQueryText ?? "",
        initialSql: resolvedQueryText ?? "",
        formatOnOpen: this.formatOnOpen,
        isBookmarked: this.isBookmarked ?? false,
        editorLanguage: this.editorLanguage,
        editorPresentation,
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
