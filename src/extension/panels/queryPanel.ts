import { randomUUID } from "node:crypto";
import * as vscode from "vscode";
import type { QueryEditorLanguage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { logErrorWithContext } from "../utils/errorHandling";
import { logger } from "../utils/logger";
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

interface QueryPanelSerializedState {
  panelId: string;
  connectionId: string;
  activeConnectionId?: string;
  initialQueryText?: string;
  formatOnOpen?: boolean;
  isBookmarked?: boolean;
  editorLanguage?: QueryEditorLanguage;
}

function isQueryPanelSerialization(
  value: unknown,
): value is QueryPanelSerializedState {
  if (!value || typeof value !== "object") return false;
  const state = value as Record<string, unknown>;
  return (
    typeof state.panelId === "string" &&
    /^qp_(?:\d+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i.test(
      state.panelId,
    ) &&
    typeof state.connectionId === "string" &&
    (state.activeConnectionId === undefined ||
      typeof state.activeConnectionId === "string") &&
    (state.initialQueryText === undefined ||
      typeof state.initialQueryText === "string") &&
    (state.formatOnOpen === undefined ||
      typeof state.formatOnOpen === "boolean") &&
    (state.isBookmarked === undefined ||
      typeof state.isBookmarked === "boolean") &&
    (state.editorLanguage === undefined ||
      state.editorLanguage === "sql" ||
      state.editorLanguage === "javascript" ||
      state.editorLanguage === "plaintext" ||
      state.editorLanguage === "json")
  );
}

export class QueryPanel {
  private static readonly viewType = "rapidb.queryPanel";
  private static panels = new Map<string, QueryPanel>();

  private readonly panel: vscode.WebviewPanel;
  private readonly connectionManager: ConnectionManager;
  private readonly controller: QueryPanelController;
  readonly initialConnectionId: string;
  private readonly formatOnOpen: boolean | undefined;
  private isBookmarked = false;
  private editorLanguage: QueryEditorLanguage | undefined;
  private lastQueryResult: QueryPanelCachedResult | null = null;
  private activeConnectionId: string;
  private readonly initialQueryText: string | undefined;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionId: string,
    initialQueryText?: string,
    formatOnOpen?: boolean,
    isBookmarked?: boolean,
    editorLanguage?: QueryEditorLanguage,
    private readonly panelId?: string,
  ) {
    this.panel = panel;
    this.connectionManager = connectionManager;
    this.initialQueryText = initialQueryText;
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
          // webview.postMessage returns Thenable<boolean>: false when the
          // webview is gone, rejection on dispose races. Swallowing it caused
          // unhandled rejections; log instead so diagnostics stay visible.
          try {
            const result = this.panel.webview.postMessage(message);
            void Promise.resolve(result).then(
              (delivered) => {
                if (delivered === false) {
                  logger.warn("QueryPanel postMessage dropped (webview gone)");
                }
              },
              (error: unknown) => {
                logger.warn(
                  `QueryPanel postMessage failed: ${error instanceof Error ? error.message : String(error)}`,
                );
              },
            );
          } catch (error: unknown) {
            logger.warn(
              `QueryPanel postMessage threw: ${error instanceof Error ? error.message : String(error)}`,
            );
          }
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
    this.panel.onDidDispose(() => {
      void this.controller.dispose().catch((error: unknown) => {
        logger.warn(
          `QueryPanel controller dispose failed: ${error instanceof Error ? error.message : String(error)}`,
        );
      });
    });
  }

  static disposeAll(): void {
    disposePanelInstances(QueryPanel.panels.values(), (panel) => {
      panel.panel.dispose();
    });
    QueryPanel.panels.clear();
  }

  private static trackPanel(panelId: string, instance: QueryPanel): void {
    QueryPanel.panels.set(panelId, instance);
    instance.panel.onDidDispose(() => {
      if (QueryPanel.panels.get(panelId) === instance) {
        QueryPanel.panels.delete(panelId);
      }
    });
  }

  static registerSerializer(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
  ): vscode.Disposable {
    if (typeof vscode.window.registerWebviewPanelSerializer !== "function") {
      return { dispose: () => undefined };
    }
    return vscode.window.registerWebviewPanelSerializer(QueryPanel.viewType, {
      async deserializeWebviewPanel(panel, state) {
        const saved =
          state !== null && typeof state === "object"
            ? (state as { initialState?: unknown }).initialState
            : undefined;
        if (!saved || typeof saved !== "object") {
          panel.dispose();
          return;
        }
        const initialState = saved as Record<string, unknown>;
        const persistedDraft =
          state !== null && typeof state === "object"
            ? (state as { queryDraft?: unknown }).queryDraft
            : undefined;
        const activeConnectionId =
          persistedDraft !== null && typeof persistedDraft === "object"
            ? (
                persistedDraft as {
                  panelId?: unknown;
                  activeConnectionId?: unknown;
                }
              ).panelId === initialState.panelId &&
              typeof (persistedDraft as { activeConnectionId?: unknown })
                .activeConnectionId === "string"
              ? (persistedDraft as { activeConnectionId: string })
                  .activeConnectionId
              : undefined
            : undefined;
        const restored = {
          panelId: initialState.panelId,
          connectionId: initialState.connectionId,
          activeConnectionId,
          initialQueryText: initialState.queryText ?? initialState.initialSql,
          formatOnOpen: initialState.formatOnOpen,
          isBookmarked: initialState.isBookmarked,
          editorLanguage: initialState.editorLanguage,
        };
        if (!isQueryPanelSerialization(restored)) {
          panel.dispose();
          return;
        }
        const instance = new QueryPanel(
          panel,
          context,
          connectionManager,
          restored.connectionId,
          restored.initialQueryText,
          restored.formatOnOpen,
          restored.isBookmarked,
          restored.editorLanguage,
          restored.panelId,
        );
        instance.activeConnectionId =
          restored.activeConnectionId ?? restored.connectionId;
        QueryPanel.trackPanel(restored.panelId, instance);
      },
    });
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

    // Hidden saved panels are restored lazily. A local counter cannot reserve
    // their IDs before deserialization runs, including after an extension restart.
    const panelId = `qp_${randomUUID()}`;
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
      panelId,
    );
    QueryPanel.trackPanel(panelId, instance);
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
        panelId: this.panelId,
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
