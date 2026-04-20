import * as vscode from "vscode";
import { parseSchemaPanelMessage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import { createWebviewShell } from "./webviewShell";

export class SchemaPanel {
  private static readonly viewType = "rapidb.schemaPanel";
  private static panels = new Map<string, SchemaPanel>();

  private readonly panel: vscode.WebviewPanel;
  private readonly cm: ConnectionManager;
  private readonly context: vscode.ExtensionContext;
  private readonly connectionId: string;
  private readonly database: string;
  private readonly schema: string;
  private readonly table: string;

  private constructor(
    panel: vscode.WebviewPanel,
    context: vscode.ExtensionContext,
    cm: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ) {
    this.panel = panel;
    this.context = context;
    this.cm = cm;
    this.connectionId = connectionId;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.panel.webview.html = this.buildHtml(context);

    const key = SchemaPanel.key(connectionId, database, schema, table);
    this.panel.onDidDispose(() => SchemaPanel.panels.delete(key));
    this.panel.webview.onDidReceiveMessage(async (msg) => {
      try {
        await this.handleMessage(msg);
      } catch (err: unknown) {
        const error = logErrorWithContext("SchemaPanel unhandled error", err);
        this.panel.webview.postMessage({
          type: "schemaError",
          payload: { error: error.message },
        });
      }
    });
  }

  private static key(c: string, d: string, s: string, t: string) {
    return `${c}::${d}::${s}::${t}`;
  }

  static disposeAll(): void {
    for (const panel of SchemaPanel.panels.values()) {
      try {
        panel.panel.dispose();
      } catch {}
    }
    SchemaPanel.panels.clear();
  }

  static createOrShow(
    context: vscode.ExtensionContext,
    cm: ConnectionManager,
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): void {
    const key = SchemaPanel.key(connectionId, database, schema, table);
    const existing = SchemaPanel.panels.get(key);
    if (existing) {
      existing.panel.reveal(vscode.ViewColumn.One);
      return;
    }

    const buildTitle = () => {
      const connName = cm.getConnection(connectionId)?.name ?? connectionId;
      const schemaPrefix = schema ? `${schema}.` : "";
      return `${schemaPrefix}${table} (schema) [${connName}]`;
    };
    const panel = vscode.window.createWebviewPanel(
      SchemaPanel.viewType,
      buildTitle(),
      vscode.ViewColumn.One,
      { enableScripts: true, retainContextWhenHidden: true },
    );
    const instance = new SchemaPanel(
      panel,
      context,
      cm,
      connectionId,
      database,
      schema,
      table,
    );
    SchemaPanel.panels.set(key, instance);

    const confSub = vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration("rapidb.connections")) {
        panel.title = buildTitle();
      }
    });
    const disconnSub = cm.onDidDisconnect((id) => {
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
    const send = (type: string, payload: unknown) =>
      this.panel.webview.postMessage({ type, payload });

    const parsed = parseSchemaPanelMessage(msg);
    if (!parsed) {
      return;
    }

    switch (parsed.type) {
      case "ready": {
        const driver = this.cm.getDriver(this.connectionId);
        if (!driver) {
          send("schemaError", { error: "Not connected" });
          return;
        }
        try {
          const [columns, indexes, foreignKeys] = await Promise.all([
            driver.describeColumns(this.database, this.schema, this.table),
            driver
              .getIndexes(this.database, this.schema, this.table)
              .catch(() => []),
            driver
              .getForeignKeys(this.database, this.schema, this.table)
              .catch(() => []),
          ]);
          send("schemaData", { columns, indexes, foreignKeys });
        } catch (err: unknown) {
          const error = normalizeUnknownError(err);
          send("schemaError", { error: error.message });
        }
        break;
      }

      case "openRelatedSchema": {
        const payload = parsed.payload;
        if (!payload) {
          return;
        }
        const { table, schema, database } = payload;
        SchemaPanel.createOrShow(
          this.context,
          this.cm,
          this.connectionId,
          database ?? this.database,
          schema ?? this.schema,
          table,
        );
        break;
      }
    }
  }

  private buildHtml(context: vscode.ExtensionContext): string {
    return createWebviewShell({
      context,
      webview: this.panel.webview,
      title: `Schema - ${this.table}`,
      initialState: {
        view: "schema",
        connectionId: this.connectionId,
        database: this.database,
        schema: this.schema,
        table: this.table,
      },
      htmlStyles: "height: 100%; overflow: hidden;",
      bodyStyles: "height: 100%; overflow: hidden;",
      rootStyles: "height: 100vh; overflow: auto;",
      extraStyles: `
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--vscode-scrollbarSlider-background); border-radius: 4px; }
      `,
    });
  }
}
