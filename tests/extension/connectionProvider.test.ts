import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class EventEmitter<T> {
    private readonly listeners = new Set<(value: T) => void>();

    readonly event = (listener: (value: T) => void) => {
      this.listeners.add(listener);
      return {
        dispose: () => {
          this.listeners.delete(listener);
        },
      };
    };

    fire(value: T): void {
      for (const listener of this.listeners) {
        listener(value);
      }
    }
  }

  return {
    EventEmitter,
    ThemeIcon: class ThemeIcon {
      constructor(
        readonly id: string,
        readonly color?: { id: string },
      ) {}
    },
    ThemeColor: class ThemeColor {
      constructor(readonly id: string) {}
    },
    TreeItem: class TreeItem {
      label: string;
      collapsibleState?: number;

      constructor(label: string, collapsibleState?: number) {
        this.label = label;
        this.collapsibleState = collapsibleState;
      }
    },
    TreeItemCollapsibleState: {
      None: 0,
      Collapsed: 1,
      Expanded: 2,
    },
    MarkdownString: class MarkdownString {
      constructor(readonly value: string) {}
    },
  };
});

describe("ConnectionProvider", () => {
  function createEventSource<T>() {
    const listeners = new Set<(value: T) => void>();

    return {
      event: vi.fn((listener: (value: T) => void) => {
        listeners.add(listener);
        return {
          dispose: () => {
            listeners.delete(listener);
          },
        };
      }),
      fire(value: T) {
        for (const listener of listeners) {
          listener(value);
        }
      },
    };
  }

  function loadedState(snapshot: { databases: unknown[] }) {
    return {
      snapshot,
      status: "loaded",
      isPartial: false,
    };
  }

  function loadingState(snapshot: { databases: unknown[] }, isPartial = false) {
    return {
      snapshot,
      status: "loading",
      isPartial,
    };
  }

  function errorState(message: string) {
    return {
      snapshot: { databases: [] },
      status: "error",
      isPartial: false,
      error: message,
    };
  }

  function loadedTableDetailState() {
    return {
      request: {
        connectionId: "conn-1",
        database: "app_db",
        schema: "app_db",
        table: "users",
      },
      status: "loaded",
      isPartial: false,
      snapshot: {
        columns: {
          status: "loaded",
          items: [
            {
              name: "id",
              type: "integer",
              nativeType: "integer",
              nullable: false,
              isPrimaryKey: true,
              primaryKeyOrdinal: 1,
              isForeignKey: false,
              identityGeneration: "always",
              filterable: true,
              filterOperators: [],
              category: "integer",
              valueSemantics: "plain",
            },
            {
              name: "uid",
              type: "uuid",
              nativeType: "uuid",
              nullable: true,
              defaultValue: "gen_random_uuid()",
              isPrimaryKey: false,
              isForeignKey: true,
              filterable: true,
              filterOperators: [],
              category: "uuid",
              valueSemantics: "plain",
            },
          ],
        },
        constraints: {
          status: "loaded",
          items: [
            {
              name: "pk_users",
              kind: "primary_key",
              columns: ["id"],
              source: "catalog",
            },
          ],
        },
        indexes: {
          status: "loaded",
          items: [
            {
              name: "users_uid_key",
              columns: ["uid"],
              unique: true,
              primary: false,
            },
          ],
        },
        triggers: {
          status: "loaded",
          items: [
            {
              name: "users_audit_trigger",
              timing: "after",
              events: ["insert", "update"],
              orientation: "row",
              enabled: true,
            },
          ],
        },
      },
    };
  }

  function loadingTableDetailState() {
    return {
      request: {
        connectionId: "conn-1",
        database: "app_db",
        schema: "app_db",
        table: "users",
      },
      status: "loading",
      isPartial: false,
      snapshot: {
        columns: {
          status: "loading",
          items: [],
        },
        constraints: {
          status: "loading",
          items: [],
        },
        indexes: {
          status: "loading",
          items: [],
        },
        triggers: {
          status: "loading",
          items: [],
        },
      },
    };
  }

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("refreshes only the affected connection subtree when schema state changes", async () => {
    vi.useFakeTimers();

    const schemaState = createEventSource<string>();
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
        { id: "conn-2", name: "Audit", type: "mysql" },
      ]),
      isConnected: vi.fn(() => true),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() => loadedState({ databases: [] })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: schemaState.event,
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);
    const roots = await provider.getChildren();
    const auditNode = roots.find((node) => node.connectionId === "conn-2");
    const treeChangeSpy = vi.fn();
    provider.onDidChangeTreeData(treeChangeSpy);

    schemaState.fire("conn-2");
    await vi.advanceTimersByTimeAsync(60);

    expect(treeChangeSpy).toHaveBeenCalledTimes(1);
    expect(auditNode).toBeDefined();
    expect(treeChangeSpy).toHaveBeenCalledWith(auditNode);

    vi.useRealTimers();
  });

  it("groups folder connections ahead of ungrouped roots and preserves folder metadata", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-b", name: "Zeta", type: "pg", folder: "Team" },
        { id: "conn-c", name: "Solo", type: "sqlite" },
        { id: "conn-a", name: "Alpha", type: "mysql", folder: "Team" },
      ]),
      isConnected: vi.fn(() => false),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() => loadedState({ databases: [] })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    expect(roots.map((node) => node.label)).toEqual(["Team", "Solo"]);
    expect(roots[0]).toMatchObject({
      id: "folder:Team",
      contextValue: "folder",
      description: "2 connections",
      tooltip: "Folder: Team (2 connections)",
    });

    const folderChildren = await provider.getChildren(roots[0]);
    expect(folderChildren.map((node) => node.label)).toEqual(["Alpha", "Zeta"]);
    expect(folderChildren.map((node) => node.description)).toEqual([
      "mysql",
      "pg",
    ]);
  });

  it("composes create-aware context values for connected and disconnected connection nodes", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-can", name: "Connected PG", type: "pg" },
        { id: "conn-no", name: "Disconnected Redis", type: "redis" },
        { id: "conn-limited", name: "Disconnected SQLite", type: "sqlite" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-can"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() => loadedState({ databases: [] })),
      getConnection: vi.fn((id: string) =>
        [
          { id: "conn-can", name: "Connected PG", type: "pg" },
          { id: "conn-no", name: "Disconnected Redis", type: "redis" },
          {
            id: "conn-limited",
            name: "Disconnected SQLite",
            type: "sqlite",
          },
        ].find((connection) => connection.id === id),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);
    const roots = await provider.getChildren();

    const connectedPg = roots.find((node) => node.connectionId === "conn-can");
    const disconnectedRedis = roots.find(
      (node) => node.connectionId === "conn-no",
    );
    const disconnectedSqlite = roots.find(
      (node) => node.connectionId === "conn-limited",
    );

    expect(connectedPg?.contextValue).toBe(
      "connectionNode_connected_canCreateDatabase",
    );
    expect(disconnectedRedis?.contextValue).toBe(
      "connectionNode_disconnected_noCreateDatabase",
    );
    expect(disconnectedSqlite?.contextValue).toBe(
      "connectionNode_disconnected_canCreateDatabase",
    );
  });

  it("composes create-aware database context values for can/no schema support", async () => {
    const connections = [
      { id: "conn-pg", name: "PG", type: "pg" },
      { id: "conn-oracle", name: "Oracle", type: "oracle" },
      { id: "conn-mysql", name: "MySQL", type: "mysql" },
      { id: "conn-redis", name: "Redis", type: "redis" },
    ];
    const connectionManager = {
      getConnections: vi.fn(() => connections),
      isConnected: vi.fn(() => true),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [{ name: "app_db", schemas: [] }],
        }),
      ),
      getConnection: vi.fn((id: string) =>
        connections.find((connection) => connection.id === id),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);
    const roots = await provider.getChildren();

    const byConnectionId = new Map<string, string | undefined>();
    for (const root of roots) {
      const databaseNodes = await provider.getChildren(root);
      byConnectionId.set(root.connectionId, databaseNodes[0]?.contextValue);
    }

    expect(byConnectionId.get("conn-pg")).toBe("database_canCreateSchema");
    expect(byConnectionId.get("conn-oracle")).toBe("database_canCreateSchema");
    expect(byConnectionId.get("conn-mysql")).toBe("database_noCreateSchema");
    expect(byConnectionId.get("conn-redis")).toBe("database_noCreateSchema");
  });

  it("renders multi-schema databases from the shared schema snapshot", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "public",
                  objects: [
                    { name: "users", type: "table", columns: [] },
                    { name: "active_users", type: "view", columns: [] },
                  ],
                },
                {
                  name: "audit",
                  objects: [
                    { name: "sync_events", type: "procedure", columns: [] },
                  ],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    expect(roots).toHaveLength(1);
    expect(roots[0]?.label).toBe("Primary");

    const databases = await provider.getChildren(roots[0]);
    expect(databases).toHaveLength(1);
    expect(databases[0]?.label).toBe("app_db");

    const schemas = await provider.getChildren(databases[0]);
    expect(schemas.map((node) => node.label)).toEqual(["public", "audit"]);

    const categories = await provider.getChildren(schemas[0]);
    expect(
      categories.map((node) => ({
        label: node.label,
        description: node.description,
      })),
    ).toEqual([
      { label: "Tables", description: "(1)" },
      { label: "Views", description: "(1)" },
      { label: "Materialized Views", description: "(0)" },
      { label: "Functions", description: "(0)" },
      { label: "Procedures", description: "(0)" },
      { label: "Sequences", description: "(0)" },
      { label: "Types", description: "(0)" },
    ]);

    const tableNodes = await provider.getChildren(categories[0]);
    expect(tableNodes.map((node) => node.label)).toEqual(["users"]);
    expect(connectionManager.getDriver).not.toHaveBeenCalled();
  });

  it("flattens MongoDB schema level and shows Collections category", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-mongo", name: "Mongo", type: "mongodb" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-mongo"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "app_db",
                  objects: [
                    { name: "users", type: "table", columns: [] },
                    { name: "orders", type: "table", columns: [] },
                    { name: "active_users", type: "view", columns: [] },
                  ],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);

    expect(databases.map((node) => node.label)).toEqual(["app_db"]);

    const databaseChildren = await provider.getChildren(databases[0]);
    expect(databaseChildren.map((node) => node.label)).toContain("Collections");
    expect(databaseChildren.map((node) => node.label)).toContain("Views");
    expect(databaseChildren.map((node) => node.label)).not.toContain("app_db");

    const collectionsCategory = databaseChildren.find(
      (node) => node.label === "Collections",
    );
    if (!collectionsCategory) {
      throw new Error("Expected Collections category for MongoDB");
    }

    const collections = await provider.getChildren(collectionsCategory);
    expect(collections.map((node) => node.label)).toEqual(["users", "orders"]);
    expect(collectionsCategory.tooltip).not.toContain("Schema:");
    expect(collectionsCategory.tooltip).toContain("Collections in app_db");
    expect(collections[0]?.tooltip).toContain("Database: app_db");
    expect(collections[0]?.tooltip).not.toContain("Schema:");

    const viewsCategory = databaseChildren.find(
      (node) => node.label === "Views",
    );
    if (!viewsCategory) {
      throw new Error("Expected Views category for MongoDB");
    }

    const views = await provider.getChildren(viewsCategory);
    expect(views.map((node) => node.label)).toEqual(["active_users"]);
    expect(viewsCategory.tooltip).toContain("Views in app_db");
    expect(views[0]?.tooltip).toContain("view: active_users");
  });

  it("keeps schema level for Redis and Elasticsearch", async () => {
    const baseDatabases = {
      redis: {
        name: "db0",
        schemas: [{ name: "default", objects: [] }],
      },
      elasticsearch: {
        name: "default",
        schemas: [{ name: "indices", objects: [] }],
      },
    } as const;

    for (const [type, database] of Object.entries(baseDatabases)) {
      const connectionId = `conn-${type}`;
      const connectionManager = {
        getConnections: vi.fn(() => [
          { id: connectionId, name: `${type}-conn`, type },
        ]),
        isConnected: vi.fn((id: string) => id === connectionId),
        isConnecting: vi.fn(() => false),
        ensureSchemaScopeLoading: vi.fn(),
        getSchemaSnapshotState: vi.fn(() =>
          loadedState({
            databases: [database],
          }),
        ),
        getDriver: vi.fn(() => {
          throw new Error(
            "ConnectionProvider should not query drivers directly",
          );
        }),
        onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
        onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
        onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
        onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
        onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      };

      const { ConnectionProvider } = await import(
        "../../src/extension/providers/connectionProvider"
      );

      const provider = new ConnectionProvider(connectionManager as never);
      const roots = await provider.getChildren();
      const databases = await provider.getChildren(roots[0]);
      const schemas = await provider.getChildren(databases[0]);

      expect(schemas.map((node) => node.label)).toEqual([
        database.schemas[0].name,
      ]);
    }
  });

  it("flattens DynamoDB schema level and hides schema wording", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-ddb", name: "Dynamo", type: "dynamodb" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-ddb"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "us-east-1",
              schemas: [
                {
                  name: "us-east-1",
                  objects: [
                    { name: "users", type: "table", columns: [] },
                    { name: "orders", type: "table", columns: [] },
                  ],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);
    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);

    expect(databases.map((node) => node.label)).toEqual(["us-east-1"]);

    const databaseChildren = await provider.getChildren(databases[0]);
    expect(databaseChildren.map((node) => node.label)).toContain("Tables");
    expect(databaseChildren.map((node) => node.label)).not.toContain(
      "us-east-1",
    );

    const tablesCategory = databaseChildren.find(
      (node) => node.label === "Tables",
    );
    if (!tablesCategory) {
      throw new Error("Expected Tables category for DynamoDB");
    }

    const tables = await provider.getChildren(tablesCategory);
    expect(tables.map((node) => node.label)).toEqual(["users", "orders"]);
    expect(tablesCategory.tooltip).toContain("Tables in us-east-1");
    expect(tablesCategory.tooltip).not.toContain("Schema:");
    expect(tables[0]?.tooltip).toContain("Database: us-east-1");
    expect(tables[0]?.tooltip).not.toContain("Schema:");
  });

  it("keeps single-schema databases visible in the tree", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "mysql" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "app_db",
                  objects: [
                    { name: "users", type: "table", columns: [] },
                    {
                      name: "latest_users",
                      type: "materializedView",
                      columns: [],
                    },
                    { name: "users_total", type: "function", columns: [] },
                    { name: "refresh_users", type: "procedure", columns: [] },
                    { name: "users_seq", type: "sequence", columns: [] },
                    { name: "user_status", type: "type", columns: [] },
                  ],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);

    expect(schemas.map((node) => node.label)).toEqual(["app_db"]);

    const categories = await provider.getChildren(schemas[0]);

    expect(categories.map((node) => node.label)).toEqual([
      "Tables",
      "Views",
      "Materialized Views",
      "Functions",
      "Procedures",
      "Sequences",
      "Types",
    ]);

    const procedureCategory = categories.find(
      (node) => node.label === "Procedures",
    );
    const materializedViewCategory = categories.find(
      (node) => node.label === "Materialized Views",
    );
    const sequenceCategory = categories.find(
      (node) => node.label === "Sequences",
    );
    const typeCategory = categories.find((node) => node.label === "Types");

    if (
      !procedureCategory ||
      !materializedViewCategory ||
      !sequenceCategory ||
      !typeCategory
    ) {
      throw new Error("Expected all schema categories to be present");
    }

    const procedureNodes = await provider.getChildren(procedureCategory);
    const materializedViewNodes = await provider.getChildren(
      materializedViewCategory,
    );
    const sequenceNodes = await provider.getChildren(sequenceCategory);
    const typeNodes = await provider.getChildren(typeCategory);

    expect(procedureNodes.map((node) => node.label)).toEqual(["refresh_users"]);
    expect(materializedViewNodes.map((node) => node.label)).toEqual([
      "latest_users",
    ]);
    expect(sequenceNodes.map((node) => node.label)).toEqual(["users_seq"]);
    expect(typeNodes.map((node) => node.label)).toEqual(["user_status"]);
    expect(connectionManager.getDriver).not.toHaveBeenCalled();
  });

  it("preserves object node ids, tooltips, and command wiring", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "mysql" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "app_db",
                  objects: [
                    { name: "users", type: "table", columns: [] },
                    {
                      name: "latest_users",
                      type: "materializedView",
                      columns: [],
                    },
                    { name: "users_total", type: "function", columns: [] },
                    { name: "refresh_users", type: "procedure", columns: [] },
                    { name: "users_seq", type: "sequence", columns: [] },
                    { name: "user_status", type: "type", columns: [] },
                  ],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);
    const categories = await provider.getChildren(schemas[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const materializedViewCategory = categories.find(
      (node) => node.label === "Materialized Views",
    );
    const functionCategory = categories.find(
      (node) => node.label === "Functions",
    );
    const procedureCategory = categories.find(
      (node) => node.label === "Procedures",
    );
    const sequenceCategory = categories.find(
      (node) => node.label === "Sequences",
    );
    const typeCategory = categories.find((node) => node.label === "Types");

    if (
      !materializedViewCategory ||
      !functionCategory ||
      !procedureCategory ||
      !sequenceCategory ||
      !typeCategory
    ) {
      throw new Error("Expected all object categories to be present");
    }

    const materializedViewNode = (
      await provider.getChildren(materializedViewCategory)
    )[0];
    const functionNode = (await provider.getChildren(functionCategory))[0];
    const procedureNode = (await provider.getChildren(procedureCategory))[0];
    const sequenceNode = (await provider.getChildren(sequenceCategory))[0];
    const typeNode = (await provider.getChildren(typeCategory))[0];

    expect(tableNode).toMatchObject({
      id: "table:conn-1:app_db:app_db:users",
      contextValue: "table",
      tooltip: "table: users\nSchema: app_db\nDatabase: app_db",
    });
    expect(tableNode?.command).toEqual({
      command: "rapidb.openTableData",
      title: "Open Data",
      arguments: [tableNode],
    });
    expect(materializedViewNode).toMatchObject({
      id: "materializedView:conn-1:app_db:app_db:latest_users",
      contextValue: "materializedView",
      tooltip:
        "materializedView: latest_users\nSchema: app_db\nDatabase: app_db",
    });
    expect(materializedViewNode?.command).toEqual({
      command: "rapidb.openTableData",
      title: "Open Data",
      arguments: [materializedViewNode],
    });
    expect(functionNode).toMatchObject({
      id: "function:conn-1:app_db:app_db:users_total",
      contextValue: "function",
      tooltip: "function: users_total\nSchema: app_db\nDatabase: app_db",
    });
    expect(functionNode?.command).toBeUndefined();
    expect(procedureNode).toMatchObject({
      id: "procedure:conn-1:app_db:app_db:refresh_users",
      contextValue: "procedure",
      tooltip: "procedure: refresh_users\nSchema: app_db\nDatabase: app_db",
    });
    expect(procedureNode?.command).toBeUndefined();
    expect(sequenceNode?.command).toBeUndefined();
    expect(typeNode?.command).toBeUndefined();
  });

  it("returns a loading status node immediately instead of awaiting the full schema load", async () => {
    const getSchemaSnapshotAsync = vi.fn(() => new Promise(() => {}));
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() => loadingState({ databases: [] })),
      getSchemaSnapshotAsync,
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const children = await provider.getChildren(roots[0]);

    expect(children).toHaveLength(1);
    expect(children[0]).toMatchObject({
      id: "status_loading:conn-1",
      label: "Loading schema…",
      contextValue: "_status",
      tooltip: "Loading schema…",
    });
    expect(connectionManager.ensureSchemaScopeLoading).toHaveBeenCalledWith(
      "conn-1",
      { kind: "connectionRoot" },
    );
    expect(getSchemaSnapshotAsync).not.toHaveBeenCalled();
  });

  it("keeps MongoDB collection and index nodes Open DDL enabled", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "NoSQL", type: "mongodb" },
      ]),
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "NoSQL",
        type: "mongodb",
      })),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "rapidb",
              schemas: [
                {
                  name: "rapidb",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
              ],
            },
          ],
        }),
      ),
      getTableDetailState: vi.fn(() => loadedTableDetailState()),
      getDriverEntityManifest: vi.fn(() => ({
        dbObjectKinds: ["table", "view"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "supported",
          triggers: "not_applicable",
        },
      })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const categories = await provider.getChildren(databases[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];

    const tableSections = await provider.getChildren(tableNode);
    const indexesSection = tableSections.find(
      (node) => node.label === "Indexes",
    );

    if (!indexesSection) {
      throw new Error("Expected indexes section to be present");
    }

    const indexNode = (await provider.getChildren(indexesSection))[0];

    expect(tableNode?.contextValue).toBe("table");
    expect(indexNode?.contextValue).toBe("table_detail_index");
  });

  it("marks unsupported DynamoDB index detail nodes with noDdl context values", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "NoSQL", type: "dynamodb" },
      ]),
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "NoSQL",
        type: "dynamodb",
      })),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "us-east-1",
              schemas: [
                {
                  name: "us-east-1",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
              ],
            },
          ],
        }),
      ),
      getTableDetailState: vi.fn(() => ({
        ...loadedTableDetailState(),
        snapshot: {
          ...loadedTableDetailState().snapshot,
          indexes: {
            status: "loaded",
            items: [
              {
                name: "users_by_email",
                columns: ["email"],
                unique: false,
                primary: false,
                ddlSupport: "unsupported",
              },
            ],
          },
        },
      })),
      getDriverEntityManifest: vi.fn(() => ({
        dbObjectKinds: ["table"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "supported",
          triggers: "not_applicable",
        },
      })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const categories = await provider.getChildren(databases[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const tableSections = await provider.getChildren(tableNode);
    const indexesSection = tableSections.find(
      (node) => node.label === "Indexes",
    );

    if (!indexesSection) {
      throw new Error("Expected indexes section to be present");
    }

    const indexNode = (await provider.getChildren(indexesSection))[0];

    expect(tableNode?.contextValue).toBe("table");
    expect(indexNode?.contextValue).toBe("table_detail_index_noDdl");
  });

  it("marks Elasticsearch index detail nodes with noDdl context values via connection-type override", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Search", type: "elasticsearch" },
      ]),
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Search",
        type: "elasticsearch",
      })),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "default",
              schemas: [
                {
                  name: "indices",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
              ],
            },
          ],
        }),
      ),
      getTableDetailState: vi.fn(() => ({
        ...loadedTableDetailState(),
        snapshot: {
          ...loadedTableDetailState().snapshot,
          indexes: {
            status: "loaded",
            items: [
              {
                name: "users_id_idx",
                columns: ["_id"],
                unique: true,
                primary: true,
              },
            ],
          },
        },
      })),
      getDriverEntityManifest: vi.fn(() => ({
        dbObjectKinds: ["table"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "supported",
          triggers: "not_applicable",
        },
      })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);
    const categories = await provider.getChildren(schemas[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const tableSections = await provider.getChildren(tableNode);
    const indexesSection = tableSections.find(
      (node) => node.label === "Indexes",
    );

    if (!indexesSection) {
      throw new Error("Expected indexes section to be present");
    }

    const indexNode = (await provider.getChildren(indexesSection))[0];

    expect(tableNode?.contextValue).toBe("table");
    expect(indexNode?.contextValue).toBe("table_detail_index_noDdl");
  });

  it("loads database scopes lazily and shows a database-level placeholder while that scope is pending", async () => {
    const ensureSchemaScopeLoading = vi.fn();
    const getSchemaSnapshotState = vi.fn(
      (_connectionId: string, scope?: { kind: string; database?: string }) => {
        if (scope?.kind === "connectionRoot") {
          return loadedState({
            databases: [{ name: "app_db", schemas: [] }],
          });
        }

        if (scope?.kind === "database" && scope.database === "app_db") {
          return loadingState({ databases: [] });
        }

        return loadedState({ databases: [] });
      },
    );
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading,
      getSchemaSnapshotState,
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const children = await provider.getChildren(databases[0]);

    expect(children).toHaveLength(1);
    expect(children[0]).toMatchObject({
      id: "status_loading:conn-1",
      label: "Loading app_db…",
      contextValue: "_status",
      tooltip: "Loading app_db…",
    });
    expect(ensureSchemaScopeLoading).toHaveBeenNthCalledWith(1, "conn-1", {
      kind: "connectionRoot",
    });
    expect(ensureSchemaScopeLoading).toHaveBeenNthCalledWith(2, "conn-1", {
      kind: "database",
      database: "app_db",
    });
  });

  it("loads schema scopes lazily and shows a schema-level placeholder while that scope is pending", async () => {
    const ensureSchemaScopeLoading = vi.fn();
    const getSchemaSnapshotState = vi.fn(
      (
        _connectionId: string,
        scope?: { kind: string; database?: string; schema?: string },
      ) => {
        if (scope?.kind === "connectionRoot") {
          return loadedState({
            databases: [{ name: "app_db", schemas: [] }],
          });
        }

        if (scope?.kind === "database" && scope.database === "app_db") {
          return loadedState({
            databases: [
              {
                name: "app_db",
                schemas: [
                  { name: "public", objects: [] },
                  { name: "audit", objects: [] },
                ],
              },
            ],
          });
        }

        if (
          scope?.kind === "schema" &&
          scope.database === "app_db" &&
          scope.schema === "public"
        ) {
          return loadingState({ databases: [] });
        }

        return loadedState({ databases: [] });
      },
    );
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading,
      getSchemaSnapshotState,
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);
    const children = await provider.getChildren(schemas[0]);

    expect(schemas.map((node) => node.label)).toEqual(["public", "audit"]);
    expect(children).toHaveLength(1);
    expect(children[0]).toMatchObject({
      id: "status_loading:conn-1",
      label: "Loading public…",
      contextValue: "_status",
      tooltip: "Loading public…",
    });
    expect(ensureSchemaScopeLoading).toHaveBeenNthCalledWith(1, "conn-1", {
      kind: "connectionRoot",
    });
    expect(ensureSchemaScopeLoading).toHaveBeenNthCalledWith(2, "conn-1", {
      kind: "database",
      database: "app_db",
    });
    expect(ensureSchemaScopeLoading).toHaveBeenNthCalledWith(3, "conn-1", {
      kind: "schema",
      database: "app_db",
      schema: "public",
    });
  });

  it("does not append a trailing loading node when a level already has visible children", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadingState(
          {
            databases: [
              {
                name: "app_db",
                schemas: [
                  {
                    name: "public",
                    objects: [{ name: "users", type: "table", columns: [] }],
                  },
                ],
              },
            ],
          },
          true,
        ),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const connectionChildren = await provider.getChildren(roots[0]);
    expect(connectionChildren.map((node) => node.label)).toEqual(["app_db"]);

    const schemas = await provider.getChildren(connectionChildren[0]);
    expect(schemas.map((node) => node.label)).toEqual(["public"]);

    const categories = await provider.getChildren(schemas[0]);
    expect(
      categories.map((node) => ({
        label: node.label,
        description: node.description,
      })),
    ).toEqual([
      { label: "Tables", description: "(1)" },
      { label: "Views", description: "(0)" },
      { label: "Materialized Views", description: "(0)" },
      { label: "Functions", description: "(0)" },
      { label: "Procedures", description: "(0)" },
      { label: "Sequences", description: "(0)" },
      { label: "Types", description: "(0)" },
    ]);

    const tables = await provider.getChildren(categories[0]);
    expect(tables.map((node) => node.label)).toEqual(["users"]);
  });

  it("returns an error node when loading connection children fails", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() => errorState("Snapshot failed")),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const children = await provider.getChildren(roots[0]);

    expect(children).toHaveLength(1);
    expect(children[0]).toMatchObject({
      id: "status_error:conn-1",
      label: "Snapshot failed",
      contextValue: "_error",
      tooltip: "Error: Snapshot failed",
    });
  });

  it("keeps table click wiring and exposes cached detail sections under expanded tables", async () => {
    const ensureTableDetailLoading = vi.fn();
    const getTableDetailState = vi.fn(() => loadedTableDetailState());
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "mysql" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading,
      getTableDetailState,
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "app_db",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);
    const categories = await provider.getChildren(schemas[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const sections = await provider.getChildren(tableNode);
    const columnRows = await provider.getChildren(sections[0]);
    const constraintRows = await provider.getChildren(sections[1]);
    const indexRows = await provider.getChildren(sections[2]);
    const triggerRows = await provider.getChildren(sections[3]);

    expect(tableNode?.command).toEqual({
      command: "rapidb.openTableData",
      title: "Open Data",
      arguments: [tableNode],
    });
    expect(tableNode?.collapsibleState).toBe(1);
    expect(sections.map((node) => node.label)).toEqual([
      "Columns",
      "Constraints",
      "Indexes",
      "Triggers",
    ]);
    expect(columnRows.map((node) => node.label)).toEqual(["id", "uid"]);
    expect(columnRows.map((node) => node.description)).toEqual([
      "integer, auto increment",
      "uuid?, default: gen_random_uuid()",
    ]);
    expect(columnRows[0]?.tooltip).toBe(
      "id integer, auto increment\nPrimary key",
    );
    expect(columnRows[0]?.iconPath).toMatchObject({
      id: "key",
      color: { id: "charts.yellow" },
    });
    expect(columnRows[1]?.iconPath).toMatchObject({
      id: "key",
      color: undefined,
    });
    expect(constraintRows[0]?.command).toBeUndefined();
    expect(indexRows[0]?.command).toBeUndefined();
    expect(triggerRows[0]?.command).toBeUndefined();
    expect(constraintRows[0]).toMatchObject({
      label: "pk_users",
      description: "primary key - id",
    });
    expect(indexRows[0]).toMatchObject({
      label: "users_uid_key",
      description: "unique - uid",
    });
    expect(triggerRows[0]).toMatchObject({
      label: "users_audit_trigger",
      description: "after insert, update",
      tooltip: "users_audit_trigger after insert, update",
    });
    expect(ensureTableDetailLoading).toHaveBeenCalledWith({
      connectionId: "conn-1",
      database: "app_db",
      schema: "app_db",
      table: "users",
    });
    expect(getTableDetailState).toHaveBeenCalledWith({
      connectionId: "conn-1",
      database: "app_db",
      schema: "app_db",
      table: "users",
    });
  });

  it("distinguishes DynamoDB partition and sort keys in column details", async () => {
    const ensureTableDetailLoading = vi.fn();
    const getTableDetailState = vi.fn(() => ({
      request: {
        connectionId: "conn-ddb",
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
      },
      status: "loaded",
      isPartial: false,
      snapshot: {
        columns: {
          status: "loaded",
          items: [
            {
              name: "tenant_id",
              type: "text",
              nativeType: "text",
              nullable: false,
              isPrimaryKey: true,
              primaryKeyOrdinal: 1,
              primaryKeyRole: "partition",
              isForeignKey: false,
              filterable: true,
              filterOperators: [],
              category: "text",
              valueSemantics: "plain",
            },
            {
              name: "user_id",
              type: "text",
              nativeType: "text",
              nullable: false,
              isPrimaryKey: true,
              primaryKeyOrdinal: 2,
              primaryKeyRole: "sort",
              isForeignKey: false,
              filterable: true,
              filterOperators: [],
              category: "text",
              valueSemantics: "plain",
            },
          ],
        },
        constraints: { status: "loaded", items: [] },
        indexes: { status: "loaded", items: [] },
        triggers: { status: "loaded", items: [] },
      },
    }));
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-ddb", name: "Dynamo", type: "dynamodb" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-ddb"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading,
      getTableDetailState,
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "us-east-1",
              schemas: [
                {
                  name: "us-east-1",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);
    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const categories = await provider.getChildren(databases[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const sections = await provider.getChildren(tableNode);
    const columnRows = await provider.getChildren(sections[0]);

    expect(columnRows.map((node) => node.description)).toEqual([
      "text - partition key",
      "text - sort key",
    ]);
    expect(columnRows[0]?.tooltip).toBe("tenant_id text\nPartition key");
    expect(columnRows[1]?.tooltip).toBe("user_id text\nSort key");
    expect(columnRows[0]?.iconPath).toMatchObject({
      id: "key",
      color: { id: "charts.yellow" },
    });
    expect(columnRows[1]?.iconPath).toMatchObject({
      id: "key",
      color: { id: "textLink.foreground" },
    });
  });

  it("shows a single table-level loader until all table detail sections are ready", async () => {
    const ensureTableDetailLoading = vi.fn();
    const getTableDetailState = vi.fn(() => loadingTableDetailState());
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "mysql" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading,
      getTableDetailState,
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "app_db",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
              ],
            },
          ],
        }),
      ),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);
    const categories = await provider.getChildren(schemas[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const children = await provider.getChildren(tableNode);

    expect(children).toHaveLength(1);
    expect(children[0]).toMatchObject({
      id: "status_loading:conn-1",
      label: "Loading users…",
      contextValue: "_status",
      tooltip: "Loading users…",
    });
    expect(ensureTableDetailLoading).toHaveBeenCalledWith({
      connectionId: "conn-1",
      database: "app_db",
      schema: "app_db",
      table: "users",
    });
    expect(getTableDetailState).toHaveBeenCalledWith({
      connectionId: "conn-1",
      database: "app_db",
      schema: "app_db",
      table: "users",
    });
  });

  it("filters schema category nodes using the driver entity manifest from manager", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "mysql" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "app_db",
                  objects: [
                    { name: "users", type: "table", columns: [] },
                    { name: "active_users", type: "view", columns: [] },
                    {
                      name: "daily_users",
                      type: "materializedView",
                      columns: [],
                    },
                    { name: "users_total", type: "function", columns: [] },
                    { name: "refresh_users", type: "procedure", columns: [] },
                    { name: "users_seq", type: "sequence", columns: [] },
                    { name: "user_status", type: "type", columns: [] },
                  ],
                },
              ],
            },
          ],
        }),
      ),
      getDriverEntityManifest: vi.fn(() => ({
        dbObjectKinds: ["table", "view"],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
      })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);
    const categories = await provider.getChildren(schemas[0]);

    expect(categories.map((node) => node.label)).toEqual(["Tables", "Views"]);
    expect(connectionManager.getDriverEntityManifest).toHaveBeenCalledWith(
      "conn-1",
    );
  });

  it("hides non-applicable table sections", async () => {
    const ensureTableDetailLoading = vi.fn();
    const getTableDetailState = vi.fn(() => loadedTableDetailState());
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "redis" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading,
      getTableDetailState,
      getSchemaSnapshotState: vi.fn(() =>
        loadedState({
          databases: [
            {
              name: "app_db",
              schemas: [
                {
                  name: "app_db",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
              ],
            },
          ],
        }),
      ),
      getDriverEntityManifest: vi.fn(() => ({
        dbObjectKinds: ["table"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "not_applicable",
          triggers: "not_applicable",
        },
      })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const schemas = await provider.getChildren(databases[0]);
    const categories = await provider.getChildren(schemas[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const sections = await provider.getChildren(tableNode);

    expect(sections.map((node) => node.label)).toEqual(["Columns"]);
    expect(ensureTableDetailLoading).toHaveBeenCalled();
    expect(getTableDetailState).toHaveBeenCalled();
  });
});
