import { describe, expect, it, vi } from "vitest";
import type { SchemaSnapshot } from "../../src/extension/connectionManager";
import { ErdGraphService } from "../../src/extension/services/erdGraphService";

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

describe("ErdGraphService", () => {
  it("builds deterministic graph nodes and edges", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const describeColumns = vi.fn(async (_database, _schema, table) => [
      {
        name: "id",
        nativeType: "int4",
        isPrimaryKey: true,
        isForeignKey: false,
        nullable: false,
      },
      {
        name: table === "orders" ? "user_id" : `${table}_name`,
        nativeType: "text",
        isPrimaryKey: false,
        isForeignKey: table === "orders",
        nullable: false,
      },
    ]);
    const getForeignKeys = vi.fn(async (_database, _schema, table) => {
      if (table === "orders") {
        return [
          {
            column: "user_id",
            referencedSchema: "public",
            referencedTable: "users",
            referencedColumn: "id",
            constraintName: "orders_user_id_fkey",
          },
        ];
      }
      return [];
    });

    const connectionManager = {
      getDriver: vi.fn(() => ({
        describeColumns,
        getForeignKeys,
        getIndexes: vi.fn(async () => []),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: [
                  { name: "orders", type: "table", columns: [] },
                  { name: "users", type: "table", columns: [] },
                ],
              },
            ],
          },
        ],
      })),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    expect(result.fromCache).toBe(false);
    expect(result.graph.nodes.map((node) => node.table)).toEqual([
      "orders",
      "users",
    ]);
    expect(result.graph.edges).toEqual([
      {
        id: "app_db.public.orders::app_db.public.users::orders_user_id_fkey::user_id::id",
        fromTableId: "app_db.public.orders",
        toTableId: "app_db.public.users",
        fromColumn: "user_id",
        toColumn: "id",
        constraintName: "orders_user_id_fkey",
        cardinality: "many-to-one",
        sourceNullable: false,
      },
    ]);

    service.dispose();
  });

  it("deduplicates repeated foreign keys and ignores edges outside the scoped graph", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const connectionManager = {
      getDriver: vi.fn(() => ({
        describeColumns: vi.fn(async () => [
          {
            name: "id",
            nativeType: "int4",
            isPrimaryKey: true,
            isForeignKey: false,
            nullable: false,
          },
          {
            name: "user_id",
            nativeType: "int4",
            isPrimaryKey: false,
            isForeignKey: true,
            nullable: true,
          },
        ]),
        getIndexes: vi.fn(async () => []),
        getForeignKeys: vi.fn(async (_database, _schema, table) => {
          if (table !== "orders") {
            return [];
          }

          return [
            {
              column: "user_id",
              referencedSchema: "   ",
              referencedTable: "users",
              referencedColumn: "id",
              constraintName: "orders_user_id_fkey",
            },
            {
              column: "user_id",
              referencedSchema: "public",
              referencedTable: "users",
              referencedColumn: "id",
              constraintName: "orders_user_id_fkey",
            },
            {
              column: "audit_id",
              referencedSchema: "audit",
              referencedTable: "entries",
              referencedColumn: "id",
              constraintName: "orders_audit_id_fkey",
            },
          ];
        }),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: [
                  { name: "orders", type: "table", columns: [] },
                  { name: "users", type: "table", columns: [] },
                ],
              },
            ],
          },
        ],
      })),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    expect(result.graph.edges).toEqual([
      {
        id: "app_db.public.orders::app_db.public.users::orders_user_id_fkey::user_id::id",
        fromTableId: "app_db.public.orders",
        toTableId: "app_db.public.users",
        fromColumn: "user_id",
        toColumn: "id",
        constraintName: "orders_user_id_fkey",
        cardinality: "many-to-one",
        sourceNullable: true,
      },
    ]);

    service.dispose();
  });

  it("uses cache and invalidates on schema-state change", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const describeColumns = vi.fn(async () => [
      {
        name: "id",
        nativeType: "int4",
        isPrimaryKey: true,
        isForeignKey: false,
        nullable: false,
      },
    ]);

    const connectionManager = {
      getDriver: vi.fn(() => ({
        describeColumns,
        getForeignKeys: vi.fn(async () => []),
        getIndexes: vi.fn(async () => []),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
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
      })),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);
    const request = {
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    };

    const first = await service.getGraph(request);
    const second = await service.getGraph(request);

    expect(first.fromCache).toBe(false);
    expect(second.fromCache).toBe(true);
    expect(describeColumns).toHaveBeenCalledTimes(1);

    schemaState.fire("conn-1");

    const third = await service.getGraph(request);
    expect(third.fromCache).toBe(false);
    expect(describeColumns).toHaveBeenCalledTimes(2);

    service.dispose();
  });

  it("bypasses cache on force reload and clears cached graphs on disconnect and refresh", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const describeColumns = vi.fn(async () => [
      {
        name: "id",
        nativeType: "int4",
        isPrimaryKey: true,
        isForeignKey: false,
        nullable: false,
      },
    ]);

    const connectionManager = {
      getDriver: vi.fn(() => ({
        describeColumns,
        getForeignKeys: vi.fn(async () => []),
        getIndexes: vi.fn(async () => []),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
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
      })),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);
    const request = {
      connectionId: "conn-1",
      database: " app_db ",
      schema: " public ",
    };

    await service.getGraph(request);
    await service.getGraph(request, true);
    disconnect.fire("conn-1");
    await service.getGraph(request);
    refresh.fire();
    await service.getGraph(request);

    expect(describeColumns).toHaveBeenCalledTimes(4);

    service.dispose();
  });

  it("throws when the connection is not active", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const connectionManager = {
      getDriver: vi.fn(() => undefined),
      getSchemaSnapshotAsync: vi.fn(),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    await expect(
      service.getGraph({
        connectionId: "conn-1",
      }),
    ).rejects.toThrow("Not connected");

    service.dispose();
  });

  it("falls back to driver discovery when database snapshot has no loaded schemas", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const connectionManager = {
      getDriver: vi.fn(() => ({
        listSchemas: vi.fn(async () => [{ name: "dbo" }]),
        listObjects: vi.fn(async () => [
          { name: "users", type: "table" },
          { name: "v_users", type: "view" },
        ]),
        describeColumns: vi.fn(async () => [
          {
            name: "id",
            nativeType: "int",
            isPrimaryKey: true,
            isForeignKey: false,
            nullable: false,
          },
        ]),
        getForeignKeys: vi.fn(async () => []),
        getIndexes: vi.fn(async () => []),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [],
          },
        ],
      })),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
    });

    expect(result.graph.nodes).toHaveLength(1);
    expect(result.graph.nodes[0]).toEqual(
      expect.objectContaining({
        database: "app_db",
        schema: "dbo",
        table: "users",
      }),
    );

    service.dispose();
  });

  it("warms unopened database and schema scopes into the tree cache before building the graph", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    let snapshot: SchemaSnapshot = {
      databases: [
        {
          name: "archive_db",
          schemas: [],
        },
      ],
    };
    let databaseScopeLoaded = false;
    const loadedSchemaNames = new Set<string>();

    const ensureSchemaScopeLoading = vi.fn(
      (
        connectionId: string,
        scope:
          | { kind: "database"; database: string }
          | { kind: "schema"; database: string; schema: string },
      ) => {
        if (connectionId !== "conn-1" || scope.database !== "archive_db") {
          return;
        }

        if (scope.kind === "database") {
          databaseScopeLoaded = true;
          snapshot = {
            databases: [
              {
                name: "archive_db",
                schemas: [
                  { name: "public", objects: [] },
                  { name: "audit", objects: [] },
                ],
              },
            ],
          };
          schemaState.fire(connectionId);
          return;
        }

        snapshot = {
          databases: [
            {
              name: "archive_db",
              schemas: [
                {
                  name: "public",
                  objects: [
                    { name: "users", type: "table" as const, columns: [] },
                  ],
                },
                {
                  name: "audit",
                  objects: [
                    {
                      name: "audit_log",
                      type: "table" as const,
                      columns: [],
                    },
                  ],
                },
              ].filter((schemaEntry) =>
                schemaEntry.name === scope.schema
                  ? true
                  : loadedSchemaNames.has(schemaEntry.name) ||
                    snapshot.databases[0]?.schemas.some(
                      (loadedSchema) => loadedSchema.name === schemaEntry.name,
                    ),
              ),
            },
          ],
        };
        loadedSchemaNames.add(scope.schema);
        schemaState.fire(connectionId);
      },
    );

    const connectionManager = {
      getDriver: vi.fn(() => ({
        listSchemas: vi.fn(async () => [{ name: "public" }, { name: "audit" }]),
        describeColumns: vi.fn(async (_database, schema, table) => [
          {
            name: schema === "audit" ? "event_id" : "id",
            nativeType: table === "audit_log" ? "bigint" : "int4",
            isPrimaryKey: true,
            isForeignKey: false,
            nullable: false,
          },
        ]),
        listObjects: vi.fn(async () => []),
        getForeignKeys: vi.fn(async () => []),
        getIndexes: vi.fn(async () => []),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => snapshot),
      getSchemaSnapshot: vi.fn(() => snapshot),
      getSchemaSnapshotState: vi.fn(
        (
          _connectionId: string,
          scope:
            | { kind: "database"; database: string }
            | { kind: "schema"; database: string; schema: string },
        ) => ({
          snapshot: { databases: [] },
          status:
            scope.kind === "database"
              ? databaseScopeLoaded
                ? "loaded"
                : "idle"
              : loadedSchemaNames.has(scope.schema)
                ? "loaded"
                : "idle",
          isPartial: false,
        }),
      ),
      ensureSchemaScopeLoading,
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "archive_db",
    });

    expect(ensureSchemaScopeLoading).toHaveBeenCalledTimes(3);
    expect(ensureSchemaScopeLoading).toHaveBeenCalledWith("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    expect(ensureSchemaScopeLoading).toHaveBeenCalledWith("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "public",
    });
    expect(ensureSchemaScopeLoading).toHaveBeenCalledWith("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });
    expect(
      result.graph.nodes.map((node) => `${node.schema}.${node.table}`),
    ).toEqual(["audit.audit_log", "public.users"]);

    service.dispose();
  });
});
