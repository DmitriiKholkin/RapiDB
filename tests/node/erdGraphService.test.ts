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
  it("keeps exact database/schema scopes, metadata requests and cache entries distinct", async () => {
    const names = ["sales", " sales ", "   "];
    const driver = {
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: names.map((name) => ({
          name,
          schemas: names.map((name) => ({
            name,
            objects: [{ name: "orders", type: "table", columns: [] }],
          })),
        })),
      })),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
    };
    const service = new ErdGraphService(connectionManager as never);
    try {
      for (const database of names) {
        for (const schema of names) {
          const request = { connectionId: "conn-1", database, schema };
          const result = await service.getGraph(request);
          expect(result.fromCache).toBe(false);
          expect(result.graph.scope).toEqual({ database, schema });
          expect(result.graph.nodes).toEqual([
            expect.objectContaining({
              id: JSON.stringify([database, schema, "orders"]),
              database,
              schema,
            }),
          ]);
          for (const method of [
            driver.describeColumns,
            driver.getForeignKeys,
            driver.getIndexes,
          ]) {
            expect(method).toHaveBeenLastCalledWith(database, schema, "orders");
          }
          expect(await service.getGraph(request)).toEqual({
            graph: result.graph,
            fromCache: true,
            partial: false,
          });
        }
      }
      expect(driver.describeColumns).toHaveBeenCalledTimes(9);

      const unscoped = await service.getGraph({ connectionId: "conn-1" });
      expect(unscoped.fromCache).toBe(false);
      expect(unscoped.graph.nodes).toHaveLength(9);
      expect(
        await service.getGraph({
          connectionId: "conn-1",
          database: "",
          schema: "",
        }),
      ).toEqual({ graph: unscoped.graph, fromCache: true, partial: false });
      const databaseOnly = await service.getGraph({
        connectionId: "conn-1",
        database: "sales",
        schema: "",
      });
      expect(databaseOnly.fromCache).toBe(false);
      expect(databaseOnly.graph.nodes).toHaveLength(3);
    } finally {
      service.dispose();
    }
  });

  it.each([
    "discovery",
    "warmup-snapshot",
    "warmup-discovery",
  ])("preserves exact schemas during %s, excluding only empty names", async (mode) => {
    const names = ["sales", " sales ", "   "];
    const snapshot = {
      databases: [
        {
          name: " sales ",
          schemas:
            mode === "warmup-snapshot"
              ? [...names, ""].map((name) => ({ name, objects: [] }))
              : [],
        },
      ],
    };
    const driver = {
      listSchemas: vi.fn(async () =>
        [...names, "", " sales "].map((name) => ({ name })),
      ),
      listObjects: vi.fn(async () => [{ name: "orders", type: "table" }]),
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const warmSchemaScope = vi.fn();
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => snapshot),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
      ...(mode === "discovery"
        ? {}
        : {
            getSchemaSnapshot: vi.fn(() => snapshot),
            getSchemaSnapshotState: vi.fn(() => ({ status: "idle" })),
            warmSchemaScope,
            onDidChangeSchemaState: createEventSource<string>().event,
          }),
    };
    const service = new ErdGraphService(connectionManager as never);
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: " sales ",
      });
      expect(graph.nodes.map((node) => node.schema).sort()).toEqual(
        [...names].sort(),
      );
      expect(driver.listObjects).toHaveBeenCalledTimes(3);
      for (const schema of names) {
        expect(driver.listObjects).toHaveBeenCalledWith(" sales ", schema);
        expect(driver.describeColumns).toHaveBeenCalledWith(
          " sales ",
          schema,
          "orders",
        );
        if (mode !== "discovery") {
          expect(warmSchemaScope).toHaveBeenCalledWith("conn-1", {
            kind: "schema",
            database: " sales ",
            schema,
          });
        }
      }
      if (mode !== "discovery") {
        expect(warmSchemaScope).toHaveBeenCalledTimes(4);
        expect(warmSchemaScope).toHaveBeenCalledWith("conn-1", {
          kind: "database",
          database: " sales ",
        });
      }
    } finally {
      service.dispose();
    }
  });

  it("uses exact foreign-key schema names and falls back only for an empty schema", async () => {
    const schemas = ["source", "sales", " sales ", "   "];
    const connectionManager = {
      getDriver: vi.fn(() => ({
        describeColumns: vi.fn(async () => []),
        getIndexes: vi.fn(async () => []),
        getForeignKeys: vi.fn(async (_database, schema) =>
          schema === "source"
            ? ["", ...schemas.slice(1)].map((referencedSchema, index) => ({
                column: `fk_${index}`,
                referencedSchema,
                referencedTable: "users",
                referencedColumn: "id",
                constraintName: `fk_${index}`,
              }))
            : [],
        ),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: schemas.map((name) => ({
              name,
              objects: [{ name: "users", type: "table", columns: [] }],
            })),
          },
        ],
      })),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
    };
    const service = new ErdGraphService(connectionManager as never);
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: "app_db",
      });
      expect(graph.edges.map((edge) => edge.toTableId).sort()).toEqual(
        schemas
          .map((schema) => JSON.stringify(["app_db", schema, "users"]))
          .sort(),
      );
    } finally {
      service.dispose();
    }
  });

  it("preserves quoted case-sensitive and special column names in nodes and foreign keys", async () => {
    const names = ["Foo", "foo", "FOO", "%", "%25", "列/Имя 🐘"];
    const connectionManager = {
      getDriver: vi.fn(() => ({
        describeColumns: vi.fn(async () =>
          names.map((name, index) => ({
            name,
            nativeType: "text",
            isPrimaryKey: false,
            isForeignKey: true,
            nullable: index === 1,
          })),
        ),
        getForeignKeys: vi.fn(async () =>
          names.map((name, index) => ({
            column: name,
            referencedSchema: "public",
            referencedTable: "quoted",
            referencedColumn: names[(index + 1) % names.length],
            constraintName: `fk-${index}`,
          })),
        ),
        getIndexes: vi.fn(async () => [
          { name: "unique-Foo", unique: true, columns: ["Foo"] },
        ]),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: [{ name: "quoted", type: "table", columns: [] }],
              },
            ],
          },
        ],
      })),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
      onDidChangeSchemaState: createEventSource<string>().event,
    };
    const service = new ErdGraphService(connectionManager as never);
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      });
      expect(graph.nodes[0].id).toBe(
        JSON.stringify(["app_db", "public", "quoted"]),
      );
      expect(graph.nodes[0].columns.map((column) => column.name)).toEqual(
        names,
      );
      expect(graph.edges).toHaveLength(names.length);
      names.forEach((name, index) => {
        expect(
          graph.edges.find((edge) => edge.fromColumn === name),
        ).toMatchObject({
          fromColumn: name,
          toColumn: names[(index + 1) % names.length],
          sourceNullable: index === 1,
          cardinality: index === 0 ? "one-to-one" : "many-to-one",
        });
      });
    } finally {
      service.dispose();
    }
  });

  it("marks every edge of a composite foreign key nullable when any column is nullable", async () => {
    const connectionManager = {
      getDriver: vi.fn(() => ({
        describeColumns: vi.fn(async () => [
          {
            name: "tenant_id",
            nativeType: "int",
            isPrimaryKey: false,
            isForeignKey: true,
            nullable: false,
          },
          {
            name: "parent_id",
            nativeType: "int",
            isPrimaryKey: false,
            isForeignKey: true,
            nullable: true,
          },
        ]),
        getForeignKeys: vi.fn(async () =>
          ["tenant_id", "parent_id"].map((column) => ({
            column,
            referencedSchema: "public",
            referencedTable: "items",
            referencedColumn: column === "tenant_id" ? "tenant_id" : "id",
            constraintName: "fk_parent",
          })),
        ),
        getIndexes: vi.fn(async () => []),
      })),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: [{ name: "items", type: "table", columns: [] }],
              },
            ],
          },
        ],
      })),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
      onDidChangeSchemaState: createEventSource<string>().event,
    };
    const service = new ErdGraphService(connectionManager as never);
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      });
      expect(
        graph.edges.map((edge) => [edge.fromColumn, edge.sourceNullable]),
      ).toEqual([
        ["parent_id", true],
        ["tenant_id", true],
      ]);
    } finally {
      service.dispose();
    }
  });

  it("builds deterministic graph nodes and edges", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const describeColumns = vi.fn(async (_database, _schema, table) => [
      {
        name: "id",
        nativeType: "int4",
        isPrimaryKey: true,
        primaryKeyRole: "partition",
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
    expect(result.graph.nodes[0]?.columns[0]).toMatchObject({
      name: "id",
      isPrimaryKey: true,
      primaryKeyRole: "partition",
    });
    expect(result.graph.edges).toEqual([
      {
        id: JSON.stringify([
          JSON.stringify(["app_db", "public", "orders"]),
          JSON.stringify(["app_db", "public", "users"]),
          "orders_user_id_fkey",
          "user_id",
          "id",
        ]),
        fromTableId: JSON.stringify(["app_db", "public", "orders"]),
        toTableId: JSON.stringify(["app_db", "public", "users"]),
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
              referencedSchema: "",
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
        id: JSON.stringify([
          JSON.stringify(["app_db", "public", "orders"]),
          JSON.stringify(["app_db", "public", "users"]),
          "orders_user_id_fkey",
          "user_id",
          "id",
        ]),
        fromTableId: JSON.stringify(["app_db", "public", "orders"]),
        toTableId: JSON.stringify(["app_db", "public", "users"]),
        fromColumn: "user_id",
        toColumn: "id",
        constraintName: "orders_user_id_fkey",
        cardinality: "many-to-one",
        sourceNullable: true,
      },
    ]);

    service.dispose();
  });

  it("uses cache and invalidates on schema refresh, not transient state changes", async () => {
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

    const afterStateChange = await service.getGraph(request);
    expect(afterStateChange.fromCache).toBe(true);
    expect(describeColumns).toHaveBeenCalledTimes(1);

    refresh.fire();

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
      database: "app_db",
      schema: "public",
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

    const warmSchemaScope = vi.fn(
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
      warmSchemaScope,
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "archive_db",
    });

    expect(warmSchemaScope).toHaveBeenCalledTimes(3);
    expect(warmSchemaScope).toHaveBeenCalledWith("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    expect(warmSchemaScope).toHaveBeenCalledWith("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "public",
    });
    expect(warmSchemaScope).toHaveBeenCalledWith("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });
    expect(
      result.graph.nodes.map((node) => `${node.schema}.${node.table}`),
    ).toEqual(["audit.audit_log", "public.users"]);

    service.dispose();
  });

  it("bounds scope warmup waiting when schema-state events are never emitted", async () => {
    vi.useFakeTimers();

    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const connectionManager = {
      getDriver: vi.fn(() => ({
        listSchemas: vi.fn(async () => [{ name: "public" }]),
        listObjects: vi.fn(async () => [{ name: "users", type: "table" }]),
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
            name: "archive_db",
            schemas: [],
          },
        ],
      })),
      getSchemaSnapshot: vi.fn(() => ({
        databases: [
          {
            name: "archive_db",
            schemas: [],
          },
        ],
      })),
      getSchemaSnapshotState: vi.fn(() => ({
        snapshot: { databases: [] },
        status: "loading",
        isPartial: false,
      })),
      warmSchemaScope: vi.fn(),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    const graphPromise = service.getGraph({
      connectionId: "conn-1",
      database: "archive_db",
    });

    await vi.advanceTimersByTimeAsync(9000);

    await expect(graphPromise).resolves.toMatchObject({
      graph: {
        nodes: [expect.objectContaining({ table: "users", schema: "public" })],
      },
      fromCache: false,
    });

    service.dispose();
    vi.useRealTimers();
  });

  it("treats idle scope state as a terminal warmup condition", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const getSchemaSnapshotState = vi
      .fn()
      .mockReturnValueOnce({
        snapshot: { databases: [] },
        status: "idle",
        isPartial: false,
      })
      .mockReturnValueOnce({
        snapshot: { databases: [] },
        status: "idle",
        isPartial: false,
      });

    const connectionManager = {
      getDriver: vi.fn(() => ({
        isConnected: () => true,
        listSchemas: vi.fn(async () => [{ name: "public" }]),
        listObjects: vi.fn(async () => [{ name: "users", type: "table" }]),
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
        databases: [{ name: "archive_db", schemas: [] }],
      })),
      getSchemaSnapshot: vi.fn(() => ({
        databases: [{ name: "archive_db", schemas: [] }],
      })),
      getSchemaSnapshotState,
      warmSchemaScope: vi.fn(),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    await expect(
      service.getGraph({
        connectionId: "conn-1",
        database: "archive_db",
      }),
    ).resolves.toMatchObject({
      graph: {
        nodes: [expect.objectContaining({ table: "users", schema: "public" })],
      },
    });

    expect(connectionManager.warmSchemaScope).toHaveBeenCalledTimes(1);

    service.dispose();
  });

  it("stops warmup waiting when connection is no longer active", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();

    const liveDriver = {
      isConnected: vi.fn(() => false),
      listSchemas: vi.fn(async () => []),
      listObjects: vi.fn(async () => []),
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };

    const connectionManager = {
      getDriver: vi.fn(() => liveDriver),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [{ name: "archive_db", schemas: [] }],
      })),
      getSchemaSnapshot: vi.fn(() => ({
        databases: [{ name: "archive_db", schemas: [] }],
      })),
      getSchemaSnapshotState: vi.fn(() => ({
        snapshot: { databases: [] },
        status: "loading",
        isPartial: false,
      })),
      warmSchemaScope: vi.fn(),
      onDidDisconnect: disconnect.event,
      onDidRefreshSchemas: refresh.event,
      onDidChangeSchemaState: schemaState.event,
    };

    const service = new ErdGraphService(connectionManager as never);

    await expect(
      service.getGraph({
        connectionId: "conn-1",
        database: "archive_db",
      }),
    ).resolves.toMatchObject({
      graph: {
        nodes: [],
      },
      fromCache: false,
    });

    service.dispose();
  });

  it("does not cache a graph completed after disconnect", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();
    let resolveColumns!: (value: unknown[]) => void;
    const columns = new Promise<unknown[]>((resolve) => {
      resolveColumns = resolve;
    });
    const driver = {
      describeColumns: vi.fn(() => columns),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
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
    const pending = service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    disconnect.fire("conn-1");
    resolveColumns([]);

    await expect(pending).rejects.toThrow(
      "Connection changed while the ERD was loading",
    );
    service.dispose();
  });

  it("stops issuing metadata queries after the request signal aborts", async () => {
    let releaseColumns!: () => void;
    const columns = new Promise<unknown[]>((resolve) => {
      releaseColumns = () => resolve([]);
    });
    const driver = {
      describeColumns: vi.fn(() => columns),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const tables = Array.from({ length: 10 }, (_, index) => `t${index}`);
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: tables.map((name) => ({
                  name,
                  type: "table",
                  columns: [],
                })),
              },
            ],
          },
        ],
      })),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
      onDidChangeSchemaState: createEventSource<string>().event,
    };
    const service = new ErdGraphService(connectionManager as never);
    const request = {
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    };
    const controller = new AbortController();
    try {
      const pending = service.getGraph(request, false, controller.signal);
      await vi.waitFor(() => {
        expect(driver.describeColumns).toHaveBeenCalledTimes(3);
      });

      controller.abort();
      releaseColumns();

      await expect(pending).rejects.toMatchObject({ name: "AbortError" });
      expect(driver.describeColumns).toHaveBeenCalledTimes(3);

      const reloaded = await service.getGraph(request);
      expect(reloaded.fromCache).toBe(false);
      expect(reloaded.graph.nodes).toHaveLength(10);
    } finally {
      service.dispose();
    }
  });

  it("stops waiting for schema warmup when the request signal aborts", async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const driver = {
      isConnected: vi.fn(() => true),
      listSchemas: vi.fn(async () => []),
      listObjects: vi.fn(async () => []),
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const snapshot = {
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
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => snapshot),
      getSchemaSnapshot: vi.fn(() => snapshot),
      getSchemaSnapshotState: vi.fn(
        (_connectionId: string, scope: { kind: string }) => ({
          status: scope.kind === "schema" ? "loading" : "loaded",
        }),
      ),
      warmSchemaScope: vi.fn(() => {
        setTimeout(() => controller.abort(), 100);
      }),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
      onDidChangeSchemaState: createEventSource<string>().event,
    };
    const service = new ErdGraphService(connectionManager as never);
    try {
      const pending = service.getGraph(
        { connectionId: "conn-1", database: "app_db" },
        false,
        controller.signal,
      );
      const assertion = expect(pending).rejects.toMatchObject({
        name: "AbortError",
      });
      await vi.advanceTimersByTimeAsync(100);
      await assertion;

      expect(connectionManager.warmSchemaScope).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
      expect(driver.listObjects).not.toHaveBeenCalled();
      expect(driver.describeColumns).not.toHaveBeenCalled();
    } finally {
      service.dispose();
      vi.useRealTimers();
    }
  });

  it("rebuilds instead of caching a graph completed after schema refresh", async () => {
    const disconnect = createEventSource<string>();
    const refresh = createEventSource<void>();
    const schemaState = createEventSource<string>();
    let resolveColumns!: (value: unknown[]) => void;
    const columns = new Promise<unknown[]>((resolve) => {
      resolveColumns = resolve;
    });
    const driver = {
      describeColumns: vi.fn(() => columns),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
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
    const pending = service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    refresh.fire();
    resolveColumns([]);

    await expect(pending).resolves.toMatchObject({ fromCache: false });
    expect(connectionManager.getSchemaSnapshotAsync).toHaveBeenCalledTimes(2);
    service.dispose();
  });

  it("discovers schemas whose scope warmup times out and caches the complete graph", async () => {
    vi.useFakeTimers();
    try {
      const snapshot = {
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: [{ name: "users", type: "table", columns: [] }],
              },
              { name: "audit", objects: [] },
            ],
          },
        ],
      };
      const driver = {
        isConnected: () => true,
        listSchemas: vi.fn(async () => [{ name: "public" }, { name: "audit" }]),
        listObjects: vi.fn(async (_database: string, schema: string) =>
          schema === "audit" ? [{ name: "audit_log", type: "table" }] : [],
        ),
        describeColumns: vi.fn(async () => []),
        getForeignKeys: vi.fn(async () => []),
        getIndexes: vi.fn(async () => []),
      };
      const connectionManager = {
        getDriver: vi.fn(() => driver),
        getSchemaSnapshotAsync: vi.fn(async () => snapshot),
        getSchemaSnapshot: vi.fn(() => snapshot),
        getSchemaSnapshotState: vi.fn(
          (
            _connectionId: string,
            scope: { kind: string; schema?: string },
          ) => ({
            status:
              scope.kind === "schema" && scope.schema === "audit"
                ? "loading"
                : "loaded",
          }),
        ),
        warmSchemaScope: vi.fn(),
        onDidDisconnect: createEventSource<string>().event,
        onDidRefreshSchemas: createEventSource<void>().event,
        onDidChangeSchemaState: createEventSource<string>().event,
      };
      const service = new ErdGraphService(connectionManager as never);
      const request = { connectionId: "conn-1", database: "app_db" };

      const pending = service.getGraph(request);
      await vi.advanceTimersByTimeAsync(5000);
      const result = await pending;

      expect(driver.listObjects).toHaveBeenCalledTimes(1);
      expect(driver.listObjects).toHaveBeenCalledWith("app_db", "audit");
      expect(
        result.graph.nodes.map((node) => `${node.schema}.${node.table}`),
      ).toEqual(["audit.audit_log", "public.users"]);
      expect(result.partial).toBe(false);
      expect(await service.getGraph(request)).toMatchObject({
        fromCache: true,
      });
      service.dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    { tables: ["audit_log"], discoveryFails: false },
    { tables: [], discoveryFails: false },
    { tables: [], discoveryFails: true },
  ])("replaces stale tables only when schema discovery succeeds: %j", async ({
    tables,
    discoveryFails,
  }) => {
    vi.useFakeTimers();
    try {
      const snapshot = {
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "audit",
                objects: [
                  { name: "dropped_log", type: "table", columns: [] },
                  { name: "audit_log", type: "table", columns: [] },
                ],
              },
            ],
          },
        ],
      };
      const driver = {
        isConnected: () => true,
        listSchemas: vi.fn(async () => [{ name: "audit" }]),
        listObjects: vi.fn(async () => {
          if (discoveryFails) throw new Error("permission denied");
          return tables.map((name) => ({ name, type: "table" }));
        }),
        describeColumns: vi.fn(async () => []),
        getForeignKeys: vi.fn(async () => []),
        getIndexes: vi.fn(async () => []),
      };
      const connectionManager = {
        getDriver: vi.fn(() => driver),
        getSchemaSnapshotAsync: vi.fn(async () => snapshot),
        getSchemaSnapshot: vi.fn(() => snapshot),
        getSchemaSnapshotState: vi.fn(
          (_connectionId: string, scope: { kind: string }) => ({
            status: scope.kind === "schema" ? "loading" : "loaded",
          }),
        ),
        warmSchemaScope: vi.fn(),
        refreshSchemaCache: vi.fn(),
        onDidDisconnect: createEventSource<string>().event,
        onDidRefreshSchemas: createEventSource<void>().event,
        onDidChangeSchemaState: createEventSource<string>().event,
      };
      const service = new ErdGraphService(connectionManager as never);

      const pending = service.getGraph(
        { connectionId: "conn-1", database: "app_db" },
        true,
      );
      await vi.advanceTimersByTimeAsync(5000);
      const result = await pending;

      expect(
        result.graph.nodes.map((node) => `${node.schema}.${node.table}`),
      ).toEqual(
        discoveryFails
          ? ["audit.audit_log", "audit.dropped_log"]
          : tables.map((table) => `audit.${table}`),
      );
      if (!discoveryFails) {
        expect(driver.describeColumns).not.toHaveBeenCalledWith(
          "app_db",
          "audit",
          "dropped_log",
        );
      }
      expect(result.partial).toBe(discoveryFails);
      service.dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not cache a partial graph when an unloaded schema cannot be discovered", async () => {
    const snapshot = {
      databases: [
        {
          name: "app_db",
          schemas: [
            {
              name: "public",
              objects: [{ name: "users", type: "table", columns: [] }],
            },
            { name: "audit", objects: [] },
          ],
        },
      ],
    };
    const driver = {
      isConnected: () => true,
      listObjects: vi.fn(async () => {
        throw new Error("permission denied");
      }),
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => snapshot),
      getSchemaSnapshot: vi.fn(() => snapshot),
      getSchemaSnapshotState: vi.fn(
        (_connectionId: string, scope: { kind: string; schema?: string }) =>
          scope.kind === "schema" && scope.schema === "audit"
            ? { status: "error", error: "boom" }
            : { status: "loaded" },
      ),
      warmSchemaScope: vi.fn(),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
      onDidChangeSchemaState: createEventSource<string>().event,
    };
    const service = new ErdGraphService(connectionManager as never);
    const request = { connectionId: "conn-1", database: "app_db" };

    const first = await service.getGraph(request);
    expect(first.partial).toBe(true);
    expect(first.graph.nodes.map((node) => node.table)).toEqual(["users"]);
    expect(await service.getGraph(request)).toEqual({
      graph: first.graph,
      fromCache: false,
      partial: true,
    });
    expect(driver.listObjects).toHaveBeenCalledTimes(2);
    const reloaded = await service.getGraph(request, true);
    expect(reloaded).toMatchObject({ fromCache: false, partial: true });
    expect(driver.listObjects).toHaveBeenCalledTimes(3);
    service.dispose();
  });

  it.each([
    true,
    false,
  ])("tracks database scope failures through the driver schema list (listSchemas succeeds: %s)", async (listSchemasSucceeds) => {
    const snapshot = {
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
    };
    const driver = {
      isConnected: () => true,
      listSchemas: vi.fn(async () => {
        if (!listSchemasSucceeds) {
          throw new Error("permission denied");
        }
        return [{ name: "public" }, { name: "audit" }];
      }),
      listObjects: vi.fn(async () => [{ name: "audit_log", type: "table" }]),
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => snapshot),
      getSchemaSnapshot: vi.fn(() => snapshot),
      getSchemaSnapshotState: vi.fn(
        (_connectionId: string, scope: { kind: string; schema?: string }) =>
          scope.kind === "database"
            ? { status: "error", error: "boom" }
            : scope.schema === "public"
              ? { status: "loaded" }
              : { status: "idle" },
      ),
      warmSchemaScope: vi.fn(),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
      onDidChangeSchemaState: createEventSource<string>().event,
    };
    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
    });

    expect(result.partial).toBe(!listSchemasSucceeds);
    expect(
      result.graph.nodes.map((node) => `${node.schema}.${node.table}`),
    ).toEqual(
      listSchemasSucceeds
        ? ["audit.audit_log", "public.users"]
        : ["public.users"],
    );
    service.dispose();
  });

  it("skips driver discovery after the service is disposed", async () => {
    const driver = {
      listSchemas: vi.fn(async () => [{ name: "public" }]),
      listObjects: vi.fn(async () => [{ name: "users", type: "table" }]),
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async () => []),
      getIndexes: vi.fn(async () => []),
    };
    let service!: ErdGraphService;
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => {
        service.dispose();
        return { databases: [{ name: "app_db", schemas: [] }] };
      }),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
    };
    service = new ErdGraphService(connectionManager as never);

    await expect(
      service.getGraph({ connectionId: "conn-1", database: "app_db" }),
    ).rejects.toThrow("Connection changed while the ERD was loading");
    expect(driver.listSchemas).not.toHaveBeenCalled();
    expect(driver.listObjects).not.toHaveBeenCalled();
  });

  it("marks composite foreign keys covered by a unique index as one-to-one", async () => {
    const foreignKeys = [
      {
        column: "tenant_id",
        referencedSchema: "public",
        referencedTable: "accounts",
        referencedColumn: "tenant_id",
        constraintName: "profile_account_fk",
      },
      {
        column: "account_id",
        referencedSchema: "public",
        referencedTable: "accounts",
        referencedColumn: "id",
        constraintName: "profile_account_fk",
      },
      {
        column: "tenant_id",
        referencedSchema: "public",
        referencedTable: "tenants",
        referencedColumn: "id",
        constraintName: "profile_tenant_fk",
      },
    ];
    const indexes = [
      {
        name: "profiles_pkey",
        columns: ["tenant_id", "account_id"],
        unique: true,
        primary: true,
      },
    ];
    const driver = {
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async (_database, _schema, table) =>
        table === "profiles" ? foreignKeys : [],
      ),
      getIndexes: vi.fn(async (_database, _schema, table) =>
        table === "profiles" ? indexes : [],
      ),
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: ["accounts", "profiles", "tenants"].map((name) => ({
                  name,
                  type: "table",
                  columns: [],
                })),
              },
            ],
          },
        ],
      })),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
    };
    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    expect(
      result.graph.edges.map((edge) => [
        edge.constraintName,
        edge.fromColumn,
        edge.cardinality,
      ]),
    ).toEqual([
      ["profile_account_fk", "account_id", "one-to-one"],
      ["profile_account_fk", "tenant_id", "one-to-one"],
      ["profile_tenant_fk", "tenant_id", "many-to-one"],
    ]);
    service.dispose();
  });

  it("does not treat partial unique indexes as one-to-one", async () => {
    const driver = {
      describeColumns: vi.fn(async () => []),
      getForeignKeys: vi.fn(async (_database, _schema, table) =>
        table === "profiles"
          ? [
              {
                column: "account_id",
                referencedSchema: "public",
                referencedTable: "accounts",
                referencedColumn: "id",
                constraintName: "profile_account_fk",
              },
            ]
          : [],
      ),
      getIndexes: vi.fn(async (_database, _schema, table) =>
        table === "profiles"
          ? [
              {
                name: "profiles_active_account_key",
                columns: ["account_id"],
                unique: true,
                primary: false,
                partial: true,
              },
            ]
          : [],
      ),
    };
    const connectionManager = {
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: ["accounts", "profiles"].map((name) => ({
                  name,
                  type: "table",
                  columns: [],
                })),
              },
            ],
          },
        ],
      })),
      onDidDisconnect: createEventSource<string>().event,
      onDidRefreshSchemas: createEventSource<void>().event,
    };
    const service = new ErdGraphService(connectionManager as never);

    const result = await service.getGraph({
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    expect(result.graph.edges.map((edge) => edge.cardinality)).toEqual([
      "many-to-one",
    ]);
    service.dispose();
  });
});
