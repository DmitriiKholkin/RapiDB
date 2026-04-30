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
      { label: "Functions", description: "(0)" },
      { label: "Procedures", description: "(0)" },
    ]);

    const tableNodes = await provider.getChildren(categories[0]);
    expect(tableNodes.map((node) => node.label)).toEqual(["users"]);
    expect(connectionManager.getDriver).not.toHaveBeenCalled();
  });

  it("renders single-schema databases without an extra schema level", async () => {
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
                    { name: "refresh_users", type: "procedure", columns: [] },
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
    const categories = await provider.getChildren(databases[0]);

    expect(categories.map((node) => node.label)).toEqual([
      "Tables",
      "Views",
      "Functions",
      "Procedures",
    ]);

    const procedureNodes = await provider.getChildren(categories[3]);
    expect(procedureNodes.map((node) => node.label)).toEqual(["refresh_users"]);
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
                    { name: "refresh_users", type: "procedure", columns: [] },
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
    const categories = await provider.getChildren(databases[0]);
    const tableNode = (await provider.getChildren(categories[0]))[0];
    const procedureNode = (await provider.getChildren(categories[3]))[0];

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
    expect(procedureNode).toMatchObject({
      id: "procedure:conn-1:app_db:app_db:refresh_users",
      contextValue: "procedure",
      tooltip: "procedure: refresh_users\nSchema: app_db\nDatabase: app_db",
    });
    expect(procedureNode?.command).toEqual({
      command: "rapidb.openRoutine",
      title: "Open Definition",
      arguments: [procedureNode],
    });
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

    const categories = await provider.getChildren(connectionChildren[0]);
    expect(
      categories.map((node) => ({
        label: node.label,
        description: node.description,
      })),
    ).toEqual([
      { label: "Tables", description: "(1)" },
      { label: "Views", description: "(0)" },
      { label: "Functions", description: "(0)" },
      { label: "Procedures", description: "(0)" },
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
});
