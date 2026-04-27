import * as vscode from "vscode";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import type { TableInfo } from "../dbDrivers/types";
import { normalizeUnknownError } from "../utils/errorHandling";

export type NodeKind =
  | "connectionNode_disconnected"
  | "connectionNode_connecting"
  | "connectionNode_connected"
  | "folder"
  | "database"
  | "schema"
  | "category_tables"
  | "category_views"
  | "category_functions"
  | "category_procedures"
  | "table"
  | "view"
  | "function"
  | "procedure";

const ICON_MAP: Record<NodeKind, string> = {
  connectionNode_disconnected: "server",
  connectionNode_connecting: "sync~spin",
  connectionNode_connected: "server",
  folder: "folder",
  database: "database",
  schema: "symbol-namespace",
  category_tables: "list-flat",
  category_views: "eye",
  category_functions: "symbol-method",
  category_procedures: "symbol-event",
  table: "list-flat",
  view: "eye",
  function: "symbol-method",
  procedure: "symbol-event",
};

export class RapiDBNode extends vscode.TreeItem {
  constructor(
    label: string,
    public readonly kind: NodeKind,
    collapsibleState: vscode.TreeItemCollapsibleState,
    public readonly connectionId: string,
    public readonly database?: string,
    public readonly schema?: string,
    public readonly objectName?: string,
  ) {
    super(label, collapsibleState);

    this.id = [kind, connectionId, database, schema, objectName]
      .filter(Boolean)
      .join(":");

    this.contextValue = kind;
    if (kind === "connectionNode_connected") {
      this.iconPath = new vscode.ThemeIcon(
        "server",
        new vscode.ThemeColor("testing.iconPassed"),
      );
    } else {
      this.iconPath = new vscode.ThemeIcon(ICON_MAP[kind] ?? "circle-outline");
    }
    this.tooltip = label;
  }
}

const CATEGORY_TYPES: Record<string, TableInfo["type"][]> = {
  category_tables: ["table"],
  category_views: ["view"],
  category_functions: ["function"],
  category_procedures: ["procedure"],
};

const CATEGORY_NODE_KIND: Record<string, NodeKind> = {
  category_tables: "table",
  category_views: "view",
  category_functions: "function",
  category_procedures: "procedure",
};

export class ConnectionProvider implements vscode.TreeDataProvider<RapiDBNode> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<
    RapiDBNode | undefined | null
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private readonly _objectsCache = new Map<string, TableInfo[]>();

  private readonly _subscriptions: vscode.Disposable[] = [];
  private _refreshTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(private readonly connectionManager: ConnectionManager) {
    const scheduleRefresh = (clearConnectionId?: string) => {
      if (this._refreshTimer !== null) {
        clearTimeout(this._refreshTimer);
      }
      this._refreshTimer = setTimeout(() => {
        this._refreshTimer = null;
        this.refresh(undefined, clearConnectionId);
      }, 50);
    };
    this._subscriptions.push(
      connectionManager.onDidConnect(() => scheduleRefresh()),
      connectionManager.onDidDisconnect((id) => scheduleRefresh(id)),
      connectionManager.onDidChangeConnections(() => scheduleRefresh()),
    );
  }

  get disposable(): vscode.Disposable {
    return {
      dispose: () => {
        if (this._refreshTimer !== null) {
          clearTimeout(this._refreshTimer);
          this._refreshTimer = null;
        }
        for (const s of this._subscriptions) {
          s.dispose();
        }
        this._subscriptions.length = 0;
      },
    };
  }

  refresh(node?: RapiDBNode, clearConnectionId?: string): void {
    if (clearConnectionId) {
      for (const key of this._objectsCache.keys()) {
        if (key.startsWith(`${clearConnectionId}::`)) {
          this._objectsCache.delete(key);
        }
      }
    } else {
      this._objectsCache.clear();
    }
    this._onDidChangeTreeData.fire(node ?? undefined);
  }

  getTreeItem(element: RapiDBNode): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: RapiDBNode): Promise<RapiDBNode[]> {
    if (!element) {
      const conns = this.connectionManager.getConnections();

      const grouped = conns.filter((c) => c.folder?.trim());
      const ungrouped = conns.filter((c) => !c.folder?.trim());

      const folderNames = [
        ...new Set(
          grouped
            .map((c) => c.folder?.trim())
            .filter((name): name is string => !!name),
        ),
      ].sort((a, b) => a.localeCompare(b));

      const folderNodes = folderNames.map((name) => this.makeFolderNode(name));

      const connNodes = ungrouped
        .slice()
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((c) => this.makeConnectionNode(c));

      return [...folderNodes, ...connNodes];
    }

    if (element.kind === "folder") {
      const folderName = element.objectName;
      if (!folderName) {
        return [];
      }
      const conns = this.connectionManager
        .getConnections()
        .filter((c) => c.folder?.trim() === folderName)
        .sort((a, b) => a.name.localeCompare(b.name));
      return conns.map((c) => this.makeConnectionNode(c));
    }

    if (
      element.kind === "connectionNode_connected" ||
      element.kind === "connectionNode_disconnected"
    ) {
      if (!this.connectionManager.isConnected(element.connectionId)) {
        return [];
      }
      const driver = this.connectionManager.getDriver(element.connectionId);
      if (!driver) {
        return [];
      }
      try {
        const dbs = await driver.listDatabases();
        return dbs.map((db) => {
          const node = new RapiDBNode(
            db.name,
            "database",
            vscode.TreeItemCollapsibleState.Collapsed,
            element.connectionId,
            db.name,
          );
          node.tooltip = `Database: ${db.name}`;
          return node;
        });
      } catch (err: unknown) {
        return [
          this.makeError(
            element.connectionId,
            normalizeUnknownError(err).message,
          ),
        ];
      }
    }

    if (element.kind === "database") {
      const databaseName = element.database;
      if (!databaseName) {
        return [];
      }
      const driver = this.connectionManager.getDriver(element.connectionId);
      if (!driver) {
        return [];
      }
      try {
        const schemas = await driver.listSchemas(databaseName);
        if (schemas.length <= 1) {
          return this.categoryNodes(
            element.connectionId,
            databaseName,
            schemas[0]?.name ?? "",
            driver,
          );
        }
        return schemas.map((s) => {
          const node = new RapiDBNode(
            s.name,
            "schema",
            vscode.TreeItemCollapsibleState.Collapsed,
            element.connectionId,
            databaseName,
            s.name,
          );
          node.tooltip = `Schema: ${s.name}`;
          return node;
        });
      } catch (err: unknown) {
        return [
          this.makeError(
            element.connectionId,
            normalizeUnknownError(err).message,
          ),
        ];
      }
    }

    if (element.kind === "schema") {
      const databaseName = element.database;
      const schemaName = element.schema;
      if (!databaseName || !schemaName) {
        return [];
      }
      const driver = this.connectionManager.getDriver(element.connectionId);
      if (!driver) {
        return [];
      }
      return this.categoryNodes(
        element.connectionId,
        databaseName,
        schemaName,
        driver,
      );
    }

    if (element.kind in CATEGORY_TYPES) {
      const databaseName = element.database;
      const schemaName = element.schema;
      if (!databaseName || !schemaName) {
        return [];
      }
      const driver = this.connectionManager.getDriver(element.connectionId);
      if (!driver) {
        return [];
      }
      try {
        const cacheKey = this.objectsCacheKey(
          element.connectionId,
          databaseName,
          schemaName,
        );
        let all = this._objectsCache.get(cacheKey);
        if (!all) {
          all = await driver.listObjects(databaseName, schemaName);
          this._objectsCache.set(cacheKey, all);
        }

        const wantedTypes = CATEGORY_TYPES[element.kind];
        const childKind = CATEGORY_NODE_KIND[element.kind];
        const filtered = all.filter((o) => wantedTypes.includes(o.type));

        element.description = `(${filtered.length})`;

        if (filtered.length === 0) {
          return [];
        }

        return filtered.map((o) => {
          const node = new RapiDBNode(
            o.name,
            childKind,
            vscode.TreeItemCollapsibleState.None,
            element.connectionId,
            databaseName,
            schemaName,
            o.name,
          );
          node.tooltip = `${childKind}: ${o.name}\nSchema: ${schemaName}\nDatabase: ${databaseName}`;
          if (childKind === "table" || childKind === "view") {
            node.command = {
              command: "rapidb.openTableData",
              title: "Open Data",
              arguments: [node],
            };
          } else if (childKind === "function" || childKind === "procedure") {
            node.command = {
              command: "rapidb.openRoutine",
              title: "Open Definition",
              arguments: [node],
            };
          }
          return node;
        });
      } catch (err: unknown) {
        return [
          this.makeError(
            element.connectionId,
            normalizeUnknownError(err).message,
          ),
        ];
      }
    }

    return [];
  }

  private makeFolderNode(folderName: string): RapiDBNode {
    const conns = this.connectionManager
      .getConnections()
      .filter((c) => c.folder?.trim() === folderName);
    const node = new RapiDBNode(
      folderName,
      "folder",
      vscode.TreeItemCollapsibleState.Collapsed,
      "",
      undefined,
      undefined,
      folderName,
    );
    node.description = `${conns.length} connection${conns.length !== 1 ? "s" : ""}`;
    node.tooltip = `Folder: ${folderName} (${conns.length} connection${conns.length !== 1 ? "s" : ""})`;

    node.iconPath = new vscode.ThemeIcon("folder");
    return node;
  }

  private makeConnectionNode(config: ConnectionConfig): RapiDBNode {
    const connected = this.connectionManager.isConnected(config.id);
    const connecting =
      !connected && this.connectionManager.isConnecting(config.id);
    const kind: NodeKind = connected
      ? "connectionNode_connected"
      : connecting
        ? "connectionNode_connecting"
        : "connectionNode_disconnected";

    const node = new RapiDBNode(
      config.name,
      kind,

      connected
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None,
      config.id,
    );

    node.description = config.type;

    if (config.type === "sqlite") {
      const tooltipLines = [
        `**${config.name}**`,
        ``,
        `Type: \`sqlite\``,
        `File: \`${config.filePath ?? "—"}\``,
      ];
      node.tooltip = new vscode.MarkdownString(tooltipLines.join("\n\n"));
    } else {
      const sslStatus = config.ssl
        ? config.rejectUnauthorized !== false
          ? "enabled"
          : "enabled (allow self-signed)"
        : "disabled";
      const tooltipLines = [
        `**${config.name}**`,
        ``,
        `Type: \`${config.type}\``,
        `Host: \`${config.host ?? "—"}\``,
        `Port: \`${config.port ?? "—"}\``,
        `Database: \`${config.database ?? "—"}\``,
        `User: \`${config.username ?? "—"}\``,
        `SSL: \`${sslStatus}\``,
      ];
      node.tooltip = new vscode.MarkdownString(tooltipLines.join("\n\n"));
    }

    if (!connected && !connecting) {
      node.command = {
        command: "rapidb.connect",
        title: "Connect",
        arguments: [node],
      };
    }

    return node;
  }

  private objectsCacheKey(
    connectionId: string,
    database: string,
    schema: string,
  ): string {
    return `${connectionId}::${database}::${schema}`;
  }

  private async categoryNodes(
    connectionId: string,
    database: string,
    schema: string,
    driver: import("../dbDrivers/types").IDBDriver,
  ): Promise<RapiDBNode[]> {
    const cacheKey = this.objectsCacheKey(connectionId, database, schema);
    let all = this._objectsCache.get(cacheKey);
    if (!all) {
      try {
        all = await driver.listObjects(database, schema);
      } catch {
        all = [];
      }
      this._objectsCache.set(cacheKey, all);
    }

    const counts: Record<string, number> = {
      category_tables: 0,
      category_views: 0,
      category_functions: 0,
      category_procedures: 0,
    };
    for (const obj of all) {
      if (obj.type === "table") {
        counts.category_tables++;
      } else if (obj.type === "view") {
        counts.category_views++;
      } else if (obj.type === "function") {
        counts.category_functions++;
      } else if (obj.type === "procedure") {
        counts.category_procedures++;
      }
    }

    const cats: { kind: NodeKind; label: string }[] = [
      { kind: "category_tables", label: "Tables" },
      { kind: "category_views", label: "Views" },
      { kind: "category_functions", label: "Functions" },
      { kind: "category_procedures", label: "Procedures" },
    ];

    return cats.map((c) => {
      const count = counts[c.kind];
      const hasItems = count > 0;
      const node = new RapiDBNode(
        c.label,
        c.kind,

        hasItems
          ? vscode.TreeItemCollapsibleState.Collapsed
          : vscode.TreeItemCollapsibleState.None,
        connectionId,
        database,
        schema,
      );
      node.description = `(${count})`;
      node.tooltip = `${c.label} in ${schema ? `${schema}.` : ""}${database} — ${count} item${count !== 1 ? "s" : ""}`;
      return node;
    });
  }

  private makeError(connectionId: string, message: string): RapiDBNode {
    const node = new RapiDBNode(
      message,
      "connectionNode_disconnected",
      vscode.TreeItemCollapsibleState.None,
      connectionId,
    );
    node.iconPath = new vscode.ThemeIcon(
      "error",
      new vscode.ThemeColor("notificationsErrorIcon.foreground"),
    );
    node.contextValue = "_error";
    node.tooltip = `Error: ${message}`;
    return node;
  }
}
