import * as vscode from "vscode";
import type { ConnectionConfig, ConnectionManager } from "../connectionManager";
import type { SchemaSnapshotSchemaEntry } from "../connectionManagerModels";
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

type CategoryKind =
  | "category_tables"
  | "category_views"
  | "category_functions"
  | "category_procedures";

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

const CATEGORY_TYPES: Record<CategoryKind, NodeKind[]> = {
  category_tables: ["table"],
  category_views: ["view"],
  category_functions: ["function"],
  category_procedures: ["procedure"],
};

const CATEGORY_NODE_KIND: Record<CategoryKind, NodeKind> = {
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

  private readonly _subscriptions: vscode.Disposable[] = [];
  private _refreshTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(private readonly connectionManager: ConnectionManager) {
    const scheduleRefresh = () => {
      if (this._refreshTimer !== null) {
        clearTimeout(this._refreshTimer);
      }
      this._refreshTimer = setTimeout(() => {
        this._refreshTimer = null;
        this.refresh();
      }, 50);
    };
    this._subscriptions.push(
      connectionManager.onDidConnect(() => scheduleRefresh()),
      connectionManager.onDidDisconnect(() => scheduleRefresh()),
      connectionManager.onDidChangeConnections(() => scheduleRefresh()),
      connectionManager.onDidSchemaLoad(() => scheduleRefresh()),
      connectionManager.onDidRefreshSchemas(() => scheduleRefresh()),
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

  refresh(node?: RapiDBNode): void {
    this._onDidChangeTreeData.fire(node ?? undefined);
  }

  getTreeItem(element: RapiDBNode): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: RapiDBNode): Promise<RapiDBNode[]> {
    if (!element) {
      return this.getRootChildren();
    }

    if (element.kind === "folder") {
      return this.getFolderChildren(element);
    }

    if (
      element.kind === "connectionNode_connected" ||
      element.kind === "connectionNode_disconnected"
    ) {
      return this.getConnectionChildren(element);
    }

    if (element.kind === "database") {
      return this.getDatabaseChildren(element);
    }

    if (element.kind === "schema") {
      return this.getSchemaChildren(element);
    }

    if (this.isCategoryKind(element.kind)) {
      return this.getCategoryChildren(element, element.kind);
    }

    return [];
  }

  private getRootChildren(): RapiDBNode[] {
    const connections = this.connectionManager.getConnections();
    const groupedConnections = connections.filter((connection) =>
      connection.folder?.trim(),
    );
    const ungroupedConnections = connections.filter(
      (connection) => !connection.folder?.trim(),
    );
    const folderNodes = [
      ...new Set(
        groupedConnections
          .map((connection) => connection.folder?.trim())
          .filter((folderName): folderName is string => !!folderName),
      ),
    ]
      .sort((left, right) => left.localeCompare(right))
      .map((folderName) => this.makeFolderNode(folderName));

    return [
      ...folderNodes,
      ...this.sortConnectionsByName(ungroupedConnections).map((connection) =>
        this.makeConnectionNode(connection),
      ),
    ];
  }

  private getFolderChildren(element: RapiDBNode): RapiDBNode[] {
    const folderName = element.objectName;
    if (!folderName) {
      return [];
    }

    return this.sortConnectionsByName(
      this.connectionManager
        .getConnections()
        .filter((connection) => connection.folder?.trim() === folderName),
    ).map((connection) => this.makeConnectionNode(connection));
  }

  private async getConnectionChildren(
    element: RapiDBNode,
  ): Promise<RapiDBNode[]> {
    if (!this.connectionManager.isConnected(element.connectionId)) {
      return [];
    }

    try {
      const snapshot = await this.connectionManager.getSchemaSnapshotAsync(
        element.connectionId,
      );
      return snapshot.databases.map((database) =>
        this.makeDatabaseNode(element.connectionId, database.name),
      );
    } catch (err: unknown) {
      return this.makeErrorNodes(element.connectionId, err);
    }
  }

  private async getDatabaseChildren(
    element: RapiDBNode,
  ): Promise<RapiDBNode[]> {
    const databaseName = element.database;
    if (!databaseName) {
      return [];
    }

    try {
      const snapshot = await this.connectionManager.getSchemaSnapshotAsync(
        element.connectionId,
      );
      const database = snapshot.databases.find(
        (entry) => entry.name === databaseName,
      );
      const schemas = database?.schemas ?? [];

      if (schemas.length <= 1) {
        return schemas[0]
          ? this.categoryNodes(element.connectionId, databaseName, schemas[0])
          : [];
      }

      return schemas.map((schema) =>
        this.makeSchemaNode(element.connectionId, databaseName, schema.name),
      );
    } catch (err: unknown) {
      return this.makeErrorNodes(element.connectionId, err);
    }
  }

  private async getSchemaChildren(element: RapiDBNode): Promise<RapiDBNode[]> {
    const databaseName = element.database;
    const schemaName = element.schema;
    if (!databaseName || !schemaName) {
      return [];
    }

    const snapshot = await this.connectionManager.getSchemaSnapshotAsync(
      element.connectionId,
    );
    const schema = snapshot.databases
      .find((entry) => entry.name === databaseName)
      ?.schemas.find((entry) => entry.name === schemaName);
    return schema
      ? this.categoryNodes(element.connectionId, databaseName, schema)
      : [];
  }

  private async getCategoryChildren(
    element: RapiDBNode,
    kind: CategoryKind,
  ): Promise<RapiDBNode[]> {
    const databaseName = element.database;
    const schemaName = element.schema;
    if (!databaseName || !schemaName) {
      return [];
    }

    try {
      const snapshot = await this.connectionManager.getSchemaSnapshotAsync(
        element.connectionId,
      );
      const schema = snapshot.databases
        .find((entry) => entry.name === databaseName)
        ?.schemas.find((entry) => entry.name === schemaName);
      const filteredObjects = (schema?.objects ?? []).filter((object) =>
        CATEGORY_TYPES[kind].includes(object.type),
      );
      const childKind = CATEGORY_NODE_KIND[kind];

      element.description = `(${filteredObjects.length})`;

      if (filteredObjects.length === 0) {
        return [];
      }

      return filteredObjects.map((object) =>
        this.makeObjectNode(
          childKind,
          element.connectionId,
          databaseName,
          schemaName,
          object.name,
        ),
      );
    } catch (err: unknown) {
      return this.makeErrorNodes(element.connectionId, err);
    }
  }

  private isCategoryKind(kind: NodeKind): kind is CategoryKind {
    return kind in CATEGORY_TYPES;
  }

  private sortConnectionsByName(
    connections: ConnectionConfig[],
  ): ConnectionConfig[] {
    return connections
      .slice()
      .sort((left, right) => left.name.localeCompare(right.name));
  }

  private makeDatabaseNode(
    connectionId: string,
    databaseName: string,
  ): RapiDBNode {
    const node = new RapiDBNode(
      databaseName,
      "database",
      vscode.TreeItemCollapsibleState.Collapsed,
      connectionId,
      databaseName,
    );
    node.tooltip = `Database: ${databaseName}`;
    return node;
  }

  private makeSchemaNode(
    connectionId: string,
    databaseName: string,
    schemaName: string,
  ): RapiDBNode {
    const node = new RapiDBNode(
      schemaName,
      "schema",
      vscode.TreeItemCollapsibleState.Collapsed,
      connectionId,
      databaseName,
      schemaName,
    );
    node.tooltip = `Schema: ${schemaName}`;
    return node;
  }

  private makeObjectNode(
    kind: NodeKind,
    connectionId: string,
    databaseName: string,
    schemaName: string,
    objectName: string,
  ): RapiDBNode {
    const node = new RapiDBNode(
      objectName,
      kind,
      vscode.TreeItemCollapsibleState.None,
      connectionId,
      databaseName,
      schemaName,
      objectName,
    );
    node.tooltip = `${kind}: ${objectName}\nSchema: ${schemaName}\nDatabase: ${databaseName}`;

    if (kind === "table" || kind === "view") {
      node.command = {
        command: "rapidb.openTableData",
        title: "Open Data",
        arguments: [node],
      };
    } else if (kind === "function" || kind === "procedure") {
      node.command = {
        command: "rapidb.openRoutine",
        title: "Open Definition",
        arguments: [node],
      };
    }

    return node;
  }

  private makeErrorNodes(connectionId: string, error: unknown): RapiDBNode[] {
    return [this.makeError(connectionId, normalizeUnknownError(error).message)];
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

  private categoryNodes(
    connectionId: string,
    database: string,
    schema: SchemaSnapshotSchemaEntry,
  ): RapiDBNode[] {
    const all = schema.objects;

    const counts: Record<CategoryKind, number> = {
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

    const cats: { kind: CategoryKind; label: string }[] = [
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
        schema.name,
      );
      node.description = `(${count})`;
      node.tooltip = `${c.label} in ${schema.name ? `${schema.name}.` : ""}${database} — ${count} item${count !== 1 ? "s" : ""}`;
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
