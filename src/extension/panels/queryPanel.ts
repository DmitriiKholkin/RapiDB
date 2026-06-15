import * as vscode from "vscode";
import type { QueryEditorLanguage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { logErrorWithContext } from "../utils/errorHandling";
import {
  attachPanelDisposables,
  attachPanelMessageHandler,
  disposePanelInstances,
} from "./panelLifecycle";
import { createPanelWebviewOptions } from "./panelRetentionPolicy";
import {
  type QueryPanelCachedResult,
  QueryPanelController,
} from "./queryPanelController";
import {
  APP_WEBVIEW_SHELL_LAYOUT,
  createWebviewShell,
  MONACO_SCROLLBAR_STYLES,
  WEBVIEW_SCROLLBAR_STYLES,
} from "./webviewShell";

const QUERY_PANEL_RETENTION_MODE = "retain" as const;

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
    this.controller = new QueryPanelController(
      connectionManager,
      {
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
      },
      context,
    );
    this.panel.webview.html = this.buildHtml(context, initialQueryText);
    attachPanelMessageHandler(
      this.panel,
      (message) => this.controller.handleMessage(message),
      (error) => {
        const normalized = logErrorWithContext(
          "QueryPanel unhandled error",
          error,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Unexpected error: ${normalized.message}`,
        );
      },
    );

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

    attachPanelDisposables(
      this.panel,
      configWatcher,
      schemaWatcher,
      connectWatcher,
      disconnectWatcher,
      schemaRefreshWatcher,
    );
  }

  static disposeAll(): void {
    disposePanelInstances(QueryPanel.panels.values(), (panel) => {
      panel.panel.dispose();
    });
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
      createPanelWebviewOptions(QUERY_PANEL_RETENTION_MODE),
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
    const driverEditorPresentation =
      this.connectionManager.getQueryEditorPresentation(
        this.initialConnectionId,
      );
    const initialSql = resolvedQueryText ?? "";
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
        queryText: initialSql,
        initialSql,
        formatOnOpen: this.formatOnOpen,
        isBookmarked: this.isBookmarked ?? false,
        editorLanguage: this.editorLanguage,
        editorPresentation,
        panelRetentionMode: QUERY_PANEL_RETENTION_MODE,
      },
      includeMediaRoot: true,
      extraCspDirectives: ["worker-src blob:"],
      ...APP_WEBVIEW_SHELL_LAYOUT,
      extraStyles: `
        ${MONACO_SCROLLBAR_STYLES}
        ${WEBVIEW_SCROLLBAR_STYLES}
      `,
    });
  }
}
