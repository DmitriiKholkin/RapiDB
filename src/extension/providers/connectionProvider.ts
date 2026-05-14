import * as vscode from "vscode";
import {
  type DbObjectKind,
  EXPLORER_CATEGORY_CONFIG,
  EXPLORER_CATEGORY_ORDER,
  type ExplorerCategoryId,
  isDataDbObjectKind,
} from "../../shared/dbObjectKinds";
import {
  formatColumnDetailDescription,
  formatColumnDetailTooltip,
  formatPrimaryKeyRoleLabel,
  type IndexDdlSupport,
} from "../../shared/tableTypes";
import type {
  ConnectionConfig,
  ConnectionManager,
  ExplorerSchemaScope,
} from "../connectionManager";
import type {
  SchemaSnapshotSchemaEntry,
  SchemaSnapshotState,
  TableDetailRequest,
  TableDetailSectionKind,
  TableDetailState,
} from "../connectionManagerModels";
import type {
  ColumnTypeMeta,
  DriverEntityManifest,
  IndexMeta,
  TableConstraintMeta,
  TriggerMeta,
} from "../dbDrivers/types";
import { DEFAULT_DRIVER_ENTITY_MANIFEST as DEFAULT_ENTITY_MANIFEST } from "../dbDrivers/types";
import {
  composeCreateAwareConnectionContextValue,
  composeCreateAwareDatabaseContextValue,
} from "../utils/createAction";
import {
  composeOpenDdlAwareContextValue,
  type OpenDdlNodeKind,
  type OpenDdlSupportHints,
} from "../utils/openDdlEligibility";

export type NodeKind =
  | "connectionNode_disconnected"
  | "connectionNode_connecting"
  | "connectionNode_connected"
  | "status_loading"
  | "status_error"
  | "folder"
  | "database"
  | "schema"
  | "category_tables"
  | "category_views"
  | "category_materializedViews"
  | "category_functions"
  | "category_procedures"
  | "category_sequences"
  | "category_types"
  | "table"
  | "view"
  | "materializedView"
  | "function"
  | "procedure"
  | "sequence"
  | "type"
  | "table_section_columns"
  | "table_section_constraints"
  | "table_section_indexes"
  | "table_section_triggers"
  | "table_detail_column"
  | "table_detail_constraint"
  | "table_detail_index"
  | "table_detail_trigger"
  | "status_info";

type CategoryKind =
  | "category_tables"
  | "category_views"
  | "category_materializedViews"
  | "category_functions"
  | "category_procedures"
  | "category_sequences"
  | "category_types";

const ICON_MAP: Record<NodeKind, string> = {
  connectionNode_disconnected: "server",
  connectionNode_connecting: "sync~spin",
  connectionNode_connected: "server",
  status_loading: "sync~spin",
  status_error: "error",
  folder: "folder",
  database: "database",
  schema: "symbol-namespace",
  category_tables: "list-flat",
  category_views: "eye",
  category_materializedViews: "eye",
  category_functions: "symbol-method",
  category_procedures: "symbol-event",
  category_sequences: "symbol-number",
  category_types: "symbol-structure",
  table: "list-flat",
  view: "eye",
  materializedView: "eye",
  function: "symbol-method",
  procedure: "symbol-event",
  sequence: "symbol-number",
  type: "symbol-structure",
  table_section_columns: "symbol-field",
  table_section_constraints: "lock",
  table_section_indexes: "list-ordered",
  table_section_triggers: "zap",
  table_detail_column: "symbol-field",
  table_detail_constraint: "lock",
  table_detail_index: "list-ordered",
  table_detail_trigger: "zap",
  status_info: "info",
};

const TABLE_SECTION_KIND_TO_NODE_KIND: Record<
  TableDetailSectionKind,
  NodeKind
> = {
  columns: "table_section_columns",
  constraints: "table_section_constraints",
  indexes: "table_section_indexes",
  triggers: "table_section_triggers",
};

const TABLE_SECTION_LABELS: Record<TableDetailSectionKind, string> = {
  columns: "Columns",
  constraints: "Constraints",
  indexes: "Indexes",
  triggers: "Triggers",
};

const PRIMARY_KEY_ICON_COLOR = new vscode.ThemeColor("charts.yellow");
const SORT_KEY_ICON_COLOR = new vscode.ThemeColor("textLink.foreground");

export class RapiDBNode extends vscode.TreeItem {
  constructor(
    label: string,
    public readonly kind: NodeKind,
    collapsibleState: vscode.TreeItemCollapsibleState,
    public readonly connectionId: string,
    public readonly database?: string,
    public readonly schema?: string,
    public readonly objectName?: string,
    public readonly parentTable?: string,
    public readonly section?: TableDetailSectionKind,
    public readonly detailKey?: string,
    public readonly ddlSupport?: IndexDdlSupport,
  ) {
    super(label, collapsibleState);

    this.id = [
      kind,
      connectionId,
      database,
      schema,
      objectName,
      parentTable,
      section,
      detailKey,
    ]
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

const CATEGORY_TYPES: Record<CategoryKind, DbObjectKind[]> = {
  category_tables: ["table"],
  category_views: ["view"],
  category_materializedViews: ["materializedView"],
  category_functions: ["function"],
  category_procedures: ["procedure"],
  category_sequences: ["sequence"],
  category_types: ["type"],
};

const CATEGORY_NODE_KIND: Record<CategoryKind, DbObjectKind> = {
  category_tables: "table",
  category_views: "view",
  category_materializedViews: "materializedView",
  category_functions: "function",
  category_procedures: "procedure",
  category_sequences: "sequence",
  category_types: "type",
};

const CATEGORY_NODE_KIND_BY_ID: Record<ExplorerCategoryId, CategoryKind> = {
  tables: "category_tables",
  views: "category_views",
  materializedViews: "category_materializedViews",
  functions: "category_functions",
  procedures: "category_procedures",
  sequences: "category_sequences",
  types: "category_types",
};

export class ConnectionProvider implements vscode.TreeDataProvider<RapiDBNode> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<
    RapiDBNode | undefined | null
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private readonly _subscriptions: vscode.Disposable[] = [];
  private readonly _connectionNodes = new Map<string, RapiDBNode>();
  private readonly _pendingConnectionRefreshIds = new Set<string>();
  private _refreshTimer: ReturnType<typeof setTimeout> | null = null;
  private _refreshAllPending = false;

  constructor(private readonly connectionManager: ConnectionManager) {
    const scheduleRefresh = (connectionId?: string) => {
      if (connectionId) {
        this._pendingConnectionRefreshIds.add(connectionId);
      } else {
        this._refreshAllPending = true;
      }
      if (this._refreshTimer !== null) {
        clearTimeout(this._refreshTimer);
      }
      this._refreshTimer = setTimeout(() => {
        this._refreshTimer = null;
        const pendingConnectionIds = [...this._pendingConnectionRefreshIds];
        const refreshAll =
          this._refreshAllPending || pendingConnectionIds.length === 0;

        this._pendingConnectionRefreshIds.clear();
        this._refreshAllPending = false;

        if (refreshAll) {
          this.refresh();
          return;
        }

        for (const pendingConnectionId of pendingConnectionIds) {
          this.refreshConnectionTree(pendingConnectionId);
        }
      }, 50);
    };
    this._subscriptions.push(
      connectionManager.onDidConnect(() => scheduleRefresh()),
      connectionManager.onDidDisconnect((connectionId) =>
        scheduleRefresh(connectionId),
      ),
      connectionManager.onDidChangeConnections(() => scheduleRefresh()),
      connectionManager.onDidChangeSchemaState((connectionId) =>
        scheduleRefresh(connectionId),
      ),
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

  refreshConnectionTree(connectionId?: string): void {
    if (!connectionId) {
      this.refresh();
      return;
    }

    this.refresh(this._connectionNodes.get(connectionId));
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

    if (element.kind === "table") {
      return this.getTableChildren(element);
    }

    if (this.isTableSectionKind(element.kind)) {
      return this.getTableSectionChildren(element);
    }

    return [];
  }

  private getRootChildren(): RapiDBNode[] {
    const connections = this.connectionManager.getConnections();
    this.syncConnectionNodeCache(connections);
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

    const state = this.getSchemaState(element.connectionId, {
      kind: "connectionRoot",
    });
    const databaseNodes = state.snapshot.databases.map((database) =>
      this.makeDatabaseNode(element.connectionId, database.name),
    );
    return this.appendStateNodes(
      element.connectionId,
      databaseNodes,
      state,
      "Loading schema…",
    );
  }

  private async getDatabaseChildren(
    element: RapiDBNode,
  ): Promise<RapiDBNode[]> {
    const databaseName = element.database;
    if (!databaseName) {
      return [];
    }

    const state = this.getSchemaState(element.connectionId, {
      kind: "database",
      database: databaseName,
    });
    const database = state.snapshot.databases.find(
      (entry) => entry.name === databaseName,
    );
    if (!database) {
      return this.pendingStateNodes(
        element.connectionId,
        state,
        `Loading ${databaseName}…`,
      );
    }

    const schemas = database.schemas;
    if (this.shouldFlattenSchemaLevel(element.connectionId, schemas)) {
      return this.categoryNodes(
        element.connectionId,
        databaseName,
        schemas[0],
        this.getEntityManifest(element.connectionId),
      );
    }

    return schemas.map((schema) =>
      this.makeSchemaNode(element.connectionId, databaseName, schema.name),
    );
  }

  private async getSchemaChildren(element: RapiDBNode): Promise<RapiDBNode[]> {
    const databaseName = element.database;
    const schemaName = element.schema;
    if (!databaseName || !schemaName) {
      return [];
    }

    const state = this.getSchemaState(element.connectionId, {
      kind: "schema",
      database: databaseName,
      schema: schemaName,
    });
    const schema = state.snapshot.databases
      .find((entry) => entry.name === databaseName)
      ?.schemas.find((entry) => entry.name === schemaName);
    if (!schema) {
      return this.pendingStateNodes(
        element.connectionId,
        state,
        `Loading ${schemaName}…`,
      );
    }

    return this.categoryNodes(
      element.connectionId,
      databaseName,
      schema,
      this.getEntityManifest(element.connectionId),
    );
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

    const state = this.getSchemaState(element.connectionId, {
      kind: "schema",
      database: databaseName,
      schema: schemaName,
    });
    const schema = state.snapshot.databases
      .find((entry) => entry.name === databaseName)
      ?.schemas.find((entry) => entry.name === schemaName);
    if (!schema) {
      return this.pendingStateNodes(
        element.connectionId,
        state,
        `Loading ${schemaName}…`,
      );
    }

    const filteredObjects = schema.objects.filter((object) =>
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
  }

  private async getTableChildren(element: RapiDBNode): Promise<RapiDBNode[]> {
    const request = this.toTableDetailRequest(element);
    if (!request) {
      return [];
    }

    this.connectionManager.ensureTableDetailLoading(request);
    const state = this.connectionManager.getTableDetailState(request);

    if (state.status === "loading") {
      return [
        this.makeLoadingNode(request.connectionId, `Loading ${request.table}…`),
      ];
    }

    if (state.status === "error") {
      return this.makeErrorNodes(
        request.connectionId,
        state.error ?? `Failed to load ${request.table}`,
      );
    }

    return this.makeTableSectionNodes(
      request,
      state,
      this.getEntityManifest(request.connectionId),
    );
  }

  private async getTableSectionChildren(
    element: RapiDBNode,
  ): Promise<RapiDBNode[]> {
    const request = this.toTableDetailRequest(element);
    if (!request || !element.section) {
      return [];
    }

    const manifest = this.getEntityManifest(request.connectionId);
    const availability = manifest.tableSections[element.section];
    if (availability === "not_applicable") {
      return [];
    }

    this.connectionManager.ensureTableDetailLoading(request);
    const state = this.connectionManager.getTableDetailState(request);

    if (state.status === "loading") {
      return [
        this.makeLoadingNode(request.connectionId, `Loading ${request.table}…`),
      ];
    }

    if (state.status === "error") {
      return this.makeErrorNodes(
        request.connectionId,
        state.error ?? `Failed to load ${request.table}`,
      );
    }

    const sectionState = state.snapshot[element.section];

    if (sectionState.status === "loading") {
      return [
        this.makeLoadingNode(
          request.connectionId,
          `Loading ${TABLE_SECTION_LABELS[element.section]}…`,
        ),
      ];
    }

    if (sectionState.status === "error") {
      return this.makeErrorNodes(
        request.connectionId,
        sectionState.error ??
          `Failed to load ${TABLE_SECTION_LABELS[element.section]}`,
      );
    }

    switch (element.section) {
      case "columns":
        return state.snapshot.columns.items.map((column) =>
          this.makeColumnDetailNode(request, column),
        );
      case "constraints":
        return state.snapshot.constraints.items.map((constraint) =>
          this.makeConstraintDetailNode(request, constraint),
        );
      case "indexes":
        return state.snapshot.indexes.items.map((index) =>
          this.makeIndexDetailNode(request, index),
        );
      case "triggers":
        return state.snapshot.triggers.items.map((trigger) =>
          this.makeTriggerDetailNode(request, trigger),
        );
    }
  }

  private getSchemaState(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): SchemaSnapshotState {
    this.connectionManager.ensureSchemaScopeLoading(connectionId, scope);
    return this.connectionManager.getSchemaSnapshotState(connectionId, scope);
  }

  private appendStateNodes(
    connectionId: string,
    nodes: RapiDBNode[],
    state: SchemaSnapshotState,
    loadingLabel: string,
  ): RapiDBNode[] {
    if (state.status === "loading") {
      return nodes.length > 0
        ? nodes
        : [this.makeLoadingNode(connectionId, loadingLabel)];
    }

    if (state.status === "error") {
      return [...nodes, ...this.makeErrorNodes(connectionId, state.error)];
    }

    return nodes;
  }

  private pendingStateNodes(
    connectionId: string,
    state: SchemaSnapshotState,
    loadingLabel: string,
  ): RapiDBNode[] {
    if (state.status === "error") {
      return this.makeErrorNodes(connectionId, state.error);
    }

    if (state.status === "loading") {
      return [this.makeLoadingNode(connectionId, loadingLabel)];
    }

    return [];
  }

  private isCategoryKind(kind: NodeKind): kind is CategoryKind {
    return kind in CATEGORY_TYPES;
  }

  private isTableSectionKind(kind: NodeKind): boolean {
    return (
      kind === "table_section_columns" ||
      kind === "table_section_constraints" ||
      kind === "table_section_indexes" ||
      kind === "table_section_triggers"
    );
  }

  private sortConnectionsByName(
    connections: ConnectionConfig[],
  ): ConnectionConfig[] {
    return connections
      .slice()
      .sort((left, right) => left.name.localeCompare(right.name));
  }

  private syncConnectionNodeCache(connections: ConnectionConfig[]): void {
    const liveConnectionIds = new Set(
      connections.map((connection) => connection.id),
    );
    for (const connectionId of this._connectionNodes.keys()) {
      if (!liveConnectionIds.has(connectionId)) {
        this._connectionNodes.delete(connectionId);
      }
    }
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
    node.contextValue = composeCreateAwareDatabaseContextValue(
      this.getConnectionType(connectionId),
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
    node.tooltip = this.hasSchemaConcept(connectionId)
      ? `Schema: ${schemaName}`
      : `Database: ${databaseName}`;
    return node;
  }

  private makeObjectNode(
    kind: DbObjectKind,
    connectionId: string,
    databaseName: string,
    schemaName: string,
    objectName: string,
  ): RapiDBNode {
    const node = new RapiDBNode(
      objectName,
      kind,
      kind === "table"
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None,
      connectionId,
      databaseName,
      schemaName,
      objectName,
    );
    const includeSchema = this.hasSchemaConcept(connectionId);
    node.tooltip = includeSchema
      ? `${kind}: ${objectName}\nSchema: ${schemaName}\nDatabase: ${databaseName}`
      : `${kind}: ${objectName}\nDatabase: ${databaseName}`;
    node.contextValue = this.composeContextValue(kind, connectionId);

    if (isDataDbObjectKind(kind)) {
      node.command = {
        command: "rapidb.openTableData",
        title: "Open Data",
        arguments: [node],
      };
    }

    return node;
  }

  private toTableDetailRequest(
    node: RapiDBNode,
  ): TableDetailRequest | undefined {
    const table = node.kind === "table" ? node.objectName : node.parentTable;
    if (!node.connectionId || !node.database || !node.schema || !table) {
      return undefined;
    }

    return {
      connectionId: node.connectionId,
      database: node.database,
      schema: node.schema,
      table,
    };
  }

  private makeTableSectionNodes(
    request: TableDetailRequest,
    state: TableDetailState,
    manifest: DriverEntityManifest,
  ): RapiDBNode[] {
    const sections: TableDetailSectionKind[] = [
      "columns",
      "constraints",
      "indexes",
      "triggers",
    ];

    return sections
      .filter((section) => manifest.tableSections[section] === "supported")
      .map((section) => {
        const sectionState = state.snapshot[section];
        const hasChildren =
          sectionState.status !== "loaded" ||
          sectionState.items.length > 0 ||
          Boolean(sectionState.error);
        const node = new RapiDBNode(
          TABLE_SECTION_LABELS[section],
          TABLE_SECTION_KIND_TO_NODE_KIND[section],
          hasChildren
            ? vscode.TreeItemCollapsibleState.Collapsed
            : vscode.TreeItemCollapsibleState.None,
          request.connectionId,
          request.database,
          request.schema,
          request.table,
          request.table,
          section,
        );
        node.description = this.describeTableSection(section, sectionState);
        node.tooltip = this.hasSchemaConcept(request.connectionId)
          ? `${TABLE_SECTION_LABELS[section]} for ${request.schema}.${request.table}`
          : `${TABLE_SECTION_LABELS[section]} for ${request.table}`;
        return node;
      });
  }

  private describeTableSection(
    section: TableDetailSectionKind,
    state: TableDetailState["snapshot"][TableDetailSectionKind],
  ): string {
    if (state.status === "error") {
      return "error";
    }

    if (section === "triggers") {
      return `(${state.items.length})`;
    }

    return `(${state.items.length})`;
  }

  private makeColumnDetailNode(
    request: TableDetailRequest,
    column: ColumnTypeMeta,
  ): RapiDBNode {
    const description = formatColumnDetailDescription(column);
    const keyRoleDescription = column.isPrimaryKey
      ? formatPrimaryKeyRoleLabel(column.primaryKeyRole)
      : undefined;
    const node = new RapiDBNode(
      column.name,
      "table_detail_column",
      vscode.TreeItemCollapsibleState.None,
      request.connectionId,
      request.database,
      request.schema,
      column.name,
      request.table,
      "columns",
      column.name,
    );
    if (column.isPrimaryKey) {
      node.iconPath = new vscode.ThemeIcon(
        "key",
        column.primaryKeyRole === "sort"
          ? SORT_KEY_ICON_COLOR
          : PRIMARY_KEY_ICON_COLOR,
      );
    } else if (column.isForeignKey) {
      node.iconPath = new vscode.ThemeIcon("key");
    }
    node.description = keyRoleDescription
      ? `${description} - ${keyRoleDescription.toLowerCase()}`
      : description;
    node.tooltip = formatColumnDetailTooltip(column);
    return node;
  }

  private makeConstraintDetailNode(
    request: TableDetailRequest,
    constraint: TableConstraintMeta,
  ): RapiDBNode {
    const parts = [
      this.formatConstraintKind(constraint.kind),
      constraint.columns.length > 0 ? constraint.columns.join(", ") : undefined,
      constraint.referencedTable
        ? `references ${(constraint.referencedSchema ? `${constraint.referencedSchema}.` : "") + constraint.referencedTable}${constraint.referencedColumns?.length ? ` ${constraint.referencedColumns.join(", ")}` : ""}`
        : undefined,
      constraint.checkExpression
        ? `check: ${constraint.checkExpression}`
        : undefined,
    ].filter((value): value is string => Boolean(value));
    const node = new RapiDBNode(
      constraint.name,
      "table_detail_constraint",
      vscode.TreeItemCollapsibleState.None,
      request.connectionId,
      request.database,
      request.schema,
      constraint.name,
      request.table,
      "constraints",
      constraint.name,
    );
    node.contextValue = this.composeContextValue(
      "table_detail_constraint",
      request.connectionId,
    );
    node.description = parts.join(" - ");
    node.tooltip = this.makeDetailTooltip(constraint.name, node.description);
    return node;
  }

  private makeIndexDetailNode(
    request: TableDetailRequest,
    index: IndexMeta,
  ): RapiDBNode {
    const flags = [
      index.primary ? "primary" : undefined,
      index.unique ? "unique" : undefined,
    ].filter((value): value is string => Boolean(value));
    const descriptionParts = [
      flags.length > 0 ? flags.join(", ") : undefined,
      index.columns.join(", "),
    ].filter((value): value is string => Boolean(value));
    const node = new RapiDBNode(
      index.name,
      "table_detail_index",
      vscode.TreeItemCollapsibleState.None,
      request.connectionId,
      request.database,
      request.schema,
      index.name,
      request.table,
      "indexes",
      index.name,
      index.ddlSupport,
    );
    node.contextValue = this.composeContextValue(
      "table_detail_index",
      request.connectionId,
      { indexDdlSupport: index.ddlSupport },
    );
    node.description = descriptionParts.join(" - ");
    node.tooltip = this.makeDetailTooltip(index.name, node.description);
    return node;
  }

  private makeTriggerDetailNode(
    request: TableDetailRequest,
    trigger: TriggerMeta,
  ): RapiDBNode {
    const detail = [
      `${trigger.timing} ${trigger.events.join(", ")}`,
      trigger.enabled === false ? "disabled" : undefined,
    ].filter((value): value is string => Boolean(value));
    const node = new RapiDBNode(
      trigger.name,
      "table_detail_trigger",
      vscode.TreeItemCollapsibleState.None,
      request.connectionId,
      request.database,
      request.schema,
      trigger.name,
      request.table,
      "triggers",
      trigger.name,
    );
    node.contextValue = this.composeContextValue(
      "table_detail_trigger",
      request.connectionId,
    );
    node.description = detail.join(", ");
    node.tooltip = this.makeDetailTooltip(trigger.name, node.description);
    return node;
  }

  private composeContextValue(
    kind: OpenDdlNodeKind,
    connectionId: string,
    hints?: OpenDdlSupportHints,
  ): string {
    const connectionType = this.getConnectionType(connectionId);
    return composeOpenDdlAwareContextValue(
      kind,
      connectionType,
      this.getEntityManifest(connectionId),
      hints,
    );
  }

  private getConnectionType(
    connectionId: string,
  ): ConnectionConfig["type"] | undefined {
    const managerWithConnectionLookup = this
      .connectionManager as ConnectionManager & {
      getConnection?: (id: string) => ConnectionConfig | undefined;
    };
    const directMatch =
      managerWithConnectionLookup.getConnection?.(connectionId);
    if (directMatch?.type) {
      return directMatch.type;
    }

    return this.connectionManager
      .getConnections()
      .find((connection) => connection.id === connectionId)?.type;
  }

  private makeDetailTooltip(name: string, description?: string): string {
    return description ? `${name} ${description}` : name;
  }

  private formatConstraintKind(kind: TableConstraintMeta["kind"]): string {
    return kind.replace(/_/g, " ");
  }

  private makeErrorNodes(
    connectionId: string,
    message = "Failed to load schema",
  ): RapiDBNode[] {
    return [this.makeError(connectionId, message)];
  }

  private makeLoadingNode(connectionId: string, label: string): RapiDBNode {
    const node = new RapiDBNode(
      label,
      "status_loading",
      vscode.TreeItemCollapsibleState.None,
      connectionId,
    );
    node.contextValue = "_status";
    node.tooltip = label;
    return node;
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

    this._connectionNodes.set(config.id, node);

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

    if (
      kind === "connectionNode_connected" ||
      kind === "connectionNode_disconnected"
    ) {
      node.contextValue = composeCreateAwareConnectionContextValue(
        kind,
        config.type,
      );
    }

    return node;
  }

  private categoryNodes(
    connectionId: string,
    database: string,
    schema: SchemaSnapshotSchemaEntry,
    manifest: DriverEntityManifest,
  ): RapiDBNode[] {
    const connectionType = this.getConnectionType(connectionId);
    const supportedKinds = new Set(manifest.dbObjectKinds);
    const visibleCategoryIds = EXPLORER_CATEGORY_ORDER.filter((categoryId) =>
      EXPLORER_CATEGORY_CONFIG[categoryId].objectKinds.some((kind) =>
        supportedKinds.has(kind),
      ),
    );
    const counts = new Map<CategoryKind, number>();

    for (const categoryId of visibleCategoryIds) {
      const categoryKind = CATEGORY_NODE_KIND_BY_ID[categoryId];
      const categoryConfig = EXPLORER_CATEGORY_CONFIG[categoryId];
      counts.set(
        categoryKind,
        schema.objects.filter((object) =>
          categoryConfig.objectKinds.includes(object.type),
        ).length,
      );
    }

    return visibleCategoryIds.map((categoryId) => {
      const categoryKind = CATEGORY_NODE_KIND_BY_ID[categoryId];
      const categoryConfig = EXPLORER_CATEGORY_CONFIG[categoryId];
      const categoryLabel =
        connectionType === "mongodb" && categoryId === "tables"
          ? "Collections"
          : categoryConfig.label;
      const count = counts.get(categoryKind) ?? 0;
      const hasItems = count > 0;
      const node = new RapiDBNode(
        categoryLabel,
        categoryKind,

        hasItems
          ? vscode.TreeItemCollapsibleState.Collapsed
          : vscode.TreeItemCollapsibleState.None,
        connectionId,
        database,
        schema.name,
      );
      node.description = `(${count})`;
      const scopeLabel = this.hasSchemaConcept(connectionId)
        ? `${schema.name ? `${schema.name}.` : ""}${database}`
        : database;
      node.tooltip = `${categoryLabel} in ${scopeLabel} - ${count} item${count !== 1 ? "s" : ""}`;
      return node;
    });
  }

  private shouldFlattenSchemaLevel(
    connectionId: string,
    schemas: readonly SchemaSnapshotSchemaEntry[],
  ): boolean {
    const connectionType = this.getConnectionType(connectionId);
    return (
      (connectionType === "mongodb" || connectionType === "dynamodb") &&
      schemas.length === 1
    );
  }

  private hasSchemaConcept(connectionId: string): boolean {
    const connectionType = this.getConnectionType(connectionId);
    return connectionType !== "mongodb" && connectionType !== "dynamodb";
  }

  private getEntityManifest(connectionId: string): DriverEntityManifest {
    const managerWithManifest = this.connectionManager as ConnectionManager & {
      getDriverEntityManifest?: (id: string) => DriverEntityManifest;
    };
    return (
      managerWithManifest.getDriverEntityManifest?.(connectionId) ??
      DEFAULT_ENTITY_MANIFEST
    );
  }

  private makeError(connectionId: string, message: string): RapiDBNode {
    const node = new RapiDBNode(
      message,
      "status_error",
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
