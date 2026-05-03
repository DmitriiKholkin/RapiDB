import * as vscode from "vscode";
import { parseErdPanelMessage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { ErdGraphService } from "../services/erdGraphService";
import { normalizeUnknownError } from "../utils/errorHandling";
import { SchemaPanel } from "./schemaPanel";
import { TablePanel } from "./tablePanel";
import { createWebviewShell } from "./webviewShell";

interface ErdPanelScope {
  connectionId: string;
  database?: string;
  schema?: string;
}

export class ErdPanel {
  private static readonly viewType = "rapidb.erdPanel";
  private static panels = new Map<string, ErdPanel>();
  private static graphService: ErdGraphService | null = null;

  private readonly panel: vscode.WebviewPanel;
  private readonly context: vscode.ExtensionContext;
  private readonly connectionManager: ConnectionManager;
  private readonly scope: ErdPanelScope;

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
      ErdPanel.panels.delete(key);
    });

    this.panel.webview.onDidReceiveMessage(async (message) => {
      await this.handleMessage(message);
    });
  }

  static disposeAll(): void {
    for (const panel of ErdPanel.panels.values()) {
      try {
        panel.panel.dispose();
      } catch {}
    }
    ErdPanel.panels.clear();

    ErdPanel.graphService?.dispose();
    ErdPanel.graphService = null;
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
      return;
    }

    if (!ErdPanel.graphService) {
      ErdPanel.graphService = new ErdGraphService(connectionManager);
    }

    const buildTitle = () => {
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
    };

    const panel = vscode.window.createWebviewPanel(
      ErdPanel.viewType,
      buildTitle(),
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
      },
    );

    const instance = new ErdPanel(panel, context, connectionManager, scope);
    ErdPanel.panels.set(key, instance);

    const confSub = vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("rapidb.connections")) {
        panel.title = buildTitle();
      }
    });

    const disconnectSub = connectionManager.onDidDisconnect((connectionId) => {
      if (connectionId === scope.connectionId) {
        panel.dispose();
      }
    });

    panel.onDidDispose(() => {
      confSub.dispose();
      disconnectSub.dispose();
    });
  }

  private static key(scope: ErdPanelScope): string {
    return [scope.connectionId, scope.database ?? "*", scope.schema ?? "*"]
      .map((part) => part.trim())
      .join("::");
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

      case "openSchema": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        SchemaPanel.createOrShow(
          this.context,
          this.connectionManager,
          this.scope.connectionId,
          payload.database ?? this.scope.database ?? "",
          payload.schema ?? this.scope.schema ?? "",
          payload.table,
        );
        break;
      }
    }
  }

  private async loadGraph(forceReload: boolean): Promise<void> {
    const graphService = ErdPanel.graphService;
    if (!graphService) {
      return;
    }

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
      );

      this.post("erdGraph", {
        graph: result.graph,
        fromCache: result.fromCache,
        loadedAt: new Date().toISOString(),
      });
    } catch (error) {
      const normalized = normalizeUnknownError(error);
      this.post("erdError", { error: normalized.message });
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
      },
      htmlStyles: "height: 100%; overflow: hidden;",
      bodyStyles: "height: 100%; overflow: hidden;",
      rootStyles: "height: 100vh; overflow: hidden;",
    });
  }

  private post(type: string, payload: unknown): void {
    void this.panel.webview.postMessage({ type, payload });
  }
}
