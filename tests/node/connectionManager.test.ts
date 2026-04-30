import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DriverTimeoutSettingsProvider } from "../../src/extension/dbDrivers/timeout";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  IDBDriver,
  SchemaInfo,
  TableInfo,
} from "../../src/extension/dbDrivers/types";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { MockEventEmitter } from "../support/mockVscode";

interface DriverBehavior {
  connectError?: unknown;
  listDatabases?: DatabaseInfo[];
  listDatabasesImpl?: () => DatabaseInfo[] | Promise<DatabaseInfo[]>;
  listSchemasByDatabase?: Record<string, SchemaInfo[]>;
  listSchemasImpl?: (database: string) => SchemaInfo[] | Promise<SchemaInfo[]>;
  listObjectsByScope?: Record<string, TableInfo[]>;
  listObjectsImpl?: (
    database: string,
    schema: string,
  ) => TableInfo[] | Promise<TableInfo[]>;
  describeTableByScope?: Record<string, ColumnMeta[]>;
  describeTableImpl?: (
    database: string,
    schema: string,
    table: string,
  ) => ColumnMeta[] | Promise<ColumnMeta[]>;
}

const driverBehaviors = new Map<string, DriverBehavior>();
const driverInstances: FakeDriver[] = [];

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>["resolve"];
  let reject!: Deferred<T>["reject"];
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

async function waitForSchemaCondition(
  manager: {
    onDidSchemaLoad(listener: (connectionId: string) => void): {
      dispose(): void;
    };
  },
  connectionId: string,
  predicate: () => boolean,
): Promise<void> {
  if (predicate()) {
    return;
  }

  await new Promise<void>((resolve) => {
    const subscription = manager.onDidSchemaLoad((loadedConnectionId) => {
      if (loadedConnectionId !== connectionId || !predicate()) {
        return;
      }

      subscription.dispose();
      resolve();
    });
  });
}

class FakeDriver implements IDBDriver {
  connectCalls = 0;
  disconnectCalls = 0;
  describeTableCalls: string[] = [];
  private connected = false;

  constructor(
    readonly config: { id: string },
    readonly timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
  ) {
    driverInstances.push(this);
  }

  async connect(): Promise<void> {
    this.connectCalls += 1;
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.connectError) {
      throw behavior.connectError;
    }
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.disconnectCalls += 1;
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  async listDatabases() {
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.listDatabasesImpl) {
      return behavior.listDatabasesImpl();
    }

    return behavior?.listDatabases ?? [{ name: "main", schemas: [] }];
  }

  async listSchemas(database = "") {
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.listSchemasImpl) {
      return behavior.listSchemasImpl(database);
    }

    return behavior?.listSchemasByDatabase?.[database] ?? [{ name: "public" }];
  }

  async listObjects(database = "", schema = "") {
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.listObjectsImpl) {
      return behavior.listObjectsImpl(database, schema);
    }

    return behavior?.listObjectsByScope?.[`${database}.${schema}`] ?? [];
  }

  async describeTable(
    database = "",
    schema = "",
    table = "",
  ): Promise<ColumnMeta[]> {
    this.describeTableCalls.push(`${database}.${schema}.${table}`);
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.describeTableImpl) {
      return behavior.describeTableImpl(database, schema, table);
    }

    return (
      behavior?.describeTableByScope?.[`${database}.${schema}.${table}`] ?? []
    );
  }

  async describeColumns(): Promise<ColumnTypeMeta[]> {
    return [];
  }

  async getIndexes() {
    return [];
  }

  async getForeignKeys() {
    return [];
  }

  async getCreateTableDDL(): Promise<string> {
    return "";
  }

  async getRoutineDefinition(): Promise<string> {
    return "";
  }

  async query() {
    return {
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    };
  }

  async runTransaction(): Promise<void> {}

  quoteIdentifier(name: string): string {
    return `"${name}"`;
  }

  qualifiedTableName(_database: string, schema: string, table: string): string {
    return `${schema}.${table}`;
  }

  buildPagination(offset: number, limit: number) {
    return {
      sql: `LIMIT ${limit} OFFSET ${offset}`,
      params: [],
    };
  }

  buildOrderByDefault(): string {
    return "ORDER BY 1";
  }

  coerceInputValue(value: unknown): unknown {
    return value;
  }

  formatOutputValue(value: unknown): unknown {
    return value;
  }

  checkPersistedEdit() {
    return null;
  }

  normalizeFilterValue(
    _column: ColumnTypeMeta,
    _operator: never,
    value: string | [string, string] | undefined,
  ) {
    return value;
  }

  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: never,
    value: string | [string, string] | undefined,
  ) {
    return {
      sql: `${column.name}:${String(operator)}`,
      params: value === undefined ? [] : Array.isArray(value) ? value : [value],
    };
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, paramIndex: number): string {
    return `$${paramIndex}`;
  }

  buildInsertDefaultValuesSql(qualifiedTableName: string): string {
    return `INSERT INTO ${qualifiedTableName} DEFAULT VALUES`;
  }

  buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = $${paramIndex}`;
  }

  materializePreviewSql(sql: string): string {
    return sql;
  }
}

function driverFactory() {
  return {
    MSSQLDriver: FakeDriver,
    MySQLDriver: FakeDriver,
    OracleDriver: FakeDriver,
    PostgresDriver: FakeDriver,
    SQLiteDriver: FakeDriver,
  };
}

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

vi.mock("../../src/extension/dbDrivers/mssql", driverFactory);
vi.mock("../../src/extension/dbDrivers/mysql", driverFactory);
vi.mock("../../src/extension/dbDrivers/oracle", driverFactory);
vi.mock("../../src/extension/dbDrivers/postgres", driverFactory);
vi.mock("../../src/extension/dbDrivers/sqlite", driverFactory);

beforeEach(() => {
  driverBehaviors.clear();
  driverInstances.splice(0, driverInstances.length);
  vi.resetModules();
});

describe("ConnectionManager", () => {
  it("deduplicates concurrent connect attempts for the same connection", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    const first = manager.beginConnect("conn-1");
    const second = manager.beginConnect("conn-1");

    expect(first.isNew).toBe(true);
    expect(second.isNew).toBe(false);

    await Promise.all([first.promise, second.promise]);

    expect(driverInstances).toHaveLength(1);
    expect(driverInstances[0]?.connectCalls).toBe(1);
    expect(manager.isConnected("conn-1")).toBe(true);
  });

  it("passes a live timeout settings provider to created drivers", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
      },
    ]);
    store.setTimeoutSettings({
      connectionTimeoutSeconds: 21,
      dbOperationTimeoutSeconds: 75,
    });

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.connectTo("conn-1");

    const timeoutSettingsProvider = driverInstances[0]?.timeoutSettingsProvider;
    expect(timeoutSettingsProvider).toBeTypeOf("function");
    expect(timeoutSettingsProvider?.().connectionTimeoutSeconds).toBe(21);
    expect(timeoutSettingsProvider?.().dbOperationTimeoutSeconds).toBe(75);

    store.setTimeoutSettings({
      connectionTimeoutSeconds: 8,
      dbOperationTimeoutSeconds: 12,
    });

    expect(timeoutSettingsProvider?.().connectionTimeoutSeconds).toBe(8);
    expect(timeoutSettingsProvider?.().dbOperationTimeoutSeconds).toBe(12);
  });

  it("disconnects an edited connection after saving updated settings", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");

    const connectedDriver = driverInstances[0];
    expect(connectedDriver?.isConnected()).toBe(true);

    await manager.saveConnection({
      id: "conn-1",
      name: "Primary Updated",
      type: "pg",
      database: "next_db",
    });

    expect(connectedDriver?.disconnectCalls).toBe(1);
    expect(manager.isConnected("conn-1")).toBe(false);
    expect(store.getConnections()[0]?.name).toBe("Primary Updated");
  });

  it("removes a connection and purges associated history, bookmarks, and secrets", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "sqlite",
        useSecretStorage: true,
      },
      {
        id: "conn-2",
        name: "Secondary",
        type: "sqlite",
      },
    ]);
    store.setHistory([
      {
        id: "h1",
        connectionId: "conn-1",
        sql: "select 1",
        executedAt: "2026-04-21T00:00:00.000Z",
      },
      {
        id: "h2",
        connectionId: "conn-2",
        sql: "select 2",
        executedAt: "2026-04-21T00:00:01.000Z",
      },
    ]);
    store.setBookmarks([
      {
        id: "b1",
        connectionId: "conn-1",
        sql: "select 1",
        savedAt: "2026-04-21T00:00:00.000Z",
      },
      {
        id: "b2",
        connectionId: "conn-2",
        sql: "select 2",
        savedAt: "2026-04-21T00:00:01.000Z",
      },
    ]);
    store.setSecret("conn-1", "shh");

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await expect(manager.removeConnection("conn-1")).resolves.toBe(true);

    expect(store.getConnections().map((connection) => connection.id)).toEqual([
      "conn-2",
    ]);
    expect(store.readHistory().map((entry) => entry.connectionId)).toEqual([
      "conn-2",
    ]);
    expect(store.readBookmarks().map((entry) => entry.connectionId)).toEqual([
      "conn-2",
    ]);
    await expect(store.getSecret("conn-1")).resolves.toBeUndefined();
  });

  it("disconnects a failed test connection and returns a normalized error", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    driverBehaviors.set("__test__", {
      connectError: {
        message: "",
        code: "ECONNREFUSED",
        errno: -61,
        name: "Error",
      },
    });

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
    );

    const result = await manager.testConnection({
      name: "Broken",
      type: "mysql",
    });

    expect(result.success).toBe(false);
    expect(result.error).toContain("ECONNREFUSED");
    expect(driverInstances).toHaveLength(1);
    expect(driverInstances[0]?.disconnectCalls).toBe(1);
  });

  it("loads the baseline database and leaves other databases catalog-only until expanded", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    driverBehaviors.set("conn-1", {
      listDatabases: [
        { name: "app_db", schemas: [] },
        { name: "archive_db", schemas: [] },
      ],
      listSchemasByDatabase: {
        app_db: [{ name: "public" }, { name: "audit" }],
        archive_db: [{ name: "public" }],
      },
      listObjectsByScope: {
        "app_db.public": [
          { schema: "public", name: "users", type: "table" },
          { schema: "public", name: "active_users", type: "view" },
          { schema: "public", name: "rebuild_cache", type: "procedure" },
        ],
        "app_db.audit": [
          { schema: "audit", name: "event_feed", type: "table" },
        ],
        "archive_db.public": [
          { schema: "public", name: "users_archive", type: "table" },
          { schema: "public", name: "summarize_archive", type: "function" },
        ],
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
        "app_db.public.active_users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
        "app_db.audit.event_feed": [
          {
            name: "event_id",
            type: "bigint",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
        "archive_db.public.users_archive": [
          {
            name: "archived_id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");

    const snapshot = await manager.getSchemaSnapshotAsync("conn-1");
    const schema = await manager.getSchemaAsync("conn-1");

    expect(snapshot).toEqual(
      expect.objectContaining({
        databases: expect.arrayContaining([
          expect.objectContaining({
            name: "app_db",
            schemas: expect.arrayContaining([
              expect.objectContaining({
                name: "public",
                objects: expect.arrayContaining([
                  expect.objectContaining({
                    name: "users",
                    type: "table",
                    columns: [{ name: "id", type: "int" }],
                  }),
                ]),
              }),
            ]),
          }),
          expect.objectContaining({
            name: "archive_db",
            schemas: [],
          }),
        ]),
      }),
    );

    expect(schema).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          database: "app_db",
          schema: "public",
          object: "users",
          columns: [{ name: "id", type: "int" }],
        }),
      ]),
    );
    expect(schema).toHaveLength(4);

    expect(driverInstances[0]?.describeTableCalls).toEqual([
      "app_db.public.users",
      "app_db.public.active_users",
      "app_db.audit.event_feed",
    ]);
  });

  it("refreshes cached schema metadata after a manual refresh request", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    driverBehaviors.set("conn-1", {
      listDatabases: [{ name: "app_db", schemas: [] }],
      listSchemasByDatabase: {
        app_db: [{ name: "public" }],
      },
      listObjectsByScope: {
        "app_db.public": [{ schema: "public", name: "users", type: "table" }],
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "mysql",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");

    const initialSchema = await manager.getSchemaAsync("conn-1");
    expect(initialSchema).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          database: "app_db",
          schema: "public",
          object: "users",
          columns: [{ name: "id", type: "int" }],
        }),
      ]),
    );

    driverBehaviors.set("conn-1", {
      listDatabases: [{ name: "app_db", schemas: [] }],
      listSchemasByDatabase: {
        app_db: [{ name: "public" }, { name: "audit" }],
      },
      listObjectsByScope: {
        "app_db.public": [{ schema: "public", name: "users", type: "table" }],
        "app_db.audit": [
          { schema: "audit", name: "event_feed", type: "table" },
        ],
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
        "app_db.audit.event_feed": [
          {
            name: "event_id",
            type: "bigint",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    manager.refreshSchemaCache("conn-1");

    const refreshedSnapshot = await manager.getSchemaSnapshotAsync("conn-1");
    const refreshedSchema = await manager.getSchemaAsync("conn-1");
    expect(refreshedSnapshot).toEqual(
      expect.objectContaining({
        databases: expect.arrayContaining([
          expect.objectContaining({
            name: "app_db",
            schemas: expect.arrayContaining([
              expect.objectContaining({
                name: "audit",
                objects: expect.arrayContaining([
                  expect.objectContaining({
                    name: "event_feed",
                    columns: [{ name: "event_id", type: "bigint" }],
                  }),
                ]),
              }),
            ]),
          }),
        ]),
      }),
    );
    expect(refreshedSchema).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          database: "app_db",
          schema: "audit",
          object: "event_feed",
          columns: [{ name: "event_id", type: "bigint" }],
        }),
      ]),
    );
    expect(refreshedSchema).toHaveLength(2);
  });

  it("loads schema snapshots concurrently for different connection ids", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const conn1Databases = createDeferred<DatabaseInfo[]>();
    const conn2Databases = createDeferred<DatabaseInfo[]>();
    const startedLoads: string[] = [];

    driverBehaviors.set("conn-1", {
      listDatabasesImpl: () => {
        startedLoads.push("conn-1");
        return conn1Databases.promise;
      },
      listSchemasByDatabase: {
        app_db: [{ name: "public" }],
      },
      listObjectsByScope: {
        "app_db.public": [{ schema: "public", name: "users", type: "table" }],
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });
    driverBehaviors.set("conn-2", {
      listDatabasesImpl: () => {
        startedLoads.push("conn-2");
        return conn2Databases.promise;
      },
      listSchemasByDatabase: {
        audit_db: [{ name: "dbo" }],
      },
      listObjectsByScope: {
        "audit_db.dbo": [{ schema: "dbo", name: "audit_log", type: "table" }],
      },
      describeTableByScope: {
        "audit_db.dbo.audit_log": [
          {
            name: "event_id",
            type: "bigint",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
      {
        id: "conn-2",
        name: "Audit",
        type: "mysql",
        database: "audit_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await Promise.all([
      manager.connectTo("conn-1"),
      manager.connectTo("conn-2"),
    ]);

    const conn1SnapshotPromise = manager.getSchemaSnapshotAsync("conn-1");
    const conn2SnapshotPromise = manager.getSchemaSnapshotAsync("conn-2");

    expect(startedLoads).toEqual(expect.arrayContaining(["conn-1", "conn-2"]));
    expect(manager.getSchemaSnapshotState("conn-1")).toMatchObject({
      status: "loading",
      isPartial: false,
    });
    expect(manager.getSchemaSnapshotState("conn-2")).toMatchObject({
      status: "loading",
      isPartial: false,
    });

    conn1Databases.resolve([{ name: "app_db", schemas: [] }]);

    await expect(conn1SnapshotPromise).resolves.toEqual({
      databases: [
        {
          name: "app_db",
          schemas: [
            {
              name: "public",
              objects: [
                {
                  name: "users",
                  type: "table",
                  columns: [{ name: "id", type: "int" }],
                },
              ],
            },
          ],
        },
      ],
    });
    expect(manager.getSchemaSnapshotState("conn-2")).toMatchObject({
      status: "loading",
      isPartial: false,
    });

    conn2Databases.resolve([{ name: "audit_db", schemas: [] }]);

    await expect(conn2SnapshotPromise).resolves.toEqual({
      databases: [
        {
          name: "audit_db",
          schemas: [
            {
              name: "dbo",
              objects: [
                {
                  name: "audit_log",
                  type: "table",
                  columns: [{ name: "event_id", type: "bigint" }],
                },
              ],
            },
          ],
        },
      ],
    });
  });

  it("awaits only the baseline root/default load in getSchemaSnapshotAsync", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const archiveSchemas = vi.fn(
      async (): Promise<SchemaInfo[]> => [{ name: "public" }],
    );

    driverBehaviors.set("conn-1", {
      listDatabases: [
        { name: "app_db", schemas: [] },
        { name: "archive_db", schemas: [] },
      ],
      listSchemasImpl: (database) => {
        if (database === "archive_db") {
          return archiveSchemas();
        }

        return [{ name: "public" }];
      },
      listObjectsByScope: {
        "app_db.public": [{ schema: "public", name: "users", type: "table" }],
        "archive_db.public": [
          { schema: "public", name: "users_archive", type: "table" },
        ],
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
        "archive_db.public.users_archive": [
          {
            name: "archived_id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");

    await expect(manager.getSchemaSnapshotAsync("conn-1")).resolves.toEqual({
      databases: [
        {
          name: "app_db",
          schemas: [
            {
              name: "public",
              objects: [
                {
                  name: "users",
                  type: "table",
                  columns: [{ name: "id", type: "int" }],
                },
              ],
            },
          ],
        },
        {
          name: "archive_db",
          schemas: [],
        },
      ],
    });
    expect(manager.getSchema("conn-1")).toEqual([
      {
        database: "app_db",
        schema: "public",
        object: "users",
        type: "table",
        columns: [{ name: "id", type: "int" }],
      },
    ]);
    expect(archiveSchemas).not.toHaveBeenCalled();
  });

  it("keeps the connection-root tree state loaded once the database catalog is available even if baseline loading is still pending", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const appSchemas = createDeferred<SchemaInfo[]>();

    driverBehaviors.set("conn-1", {
      listDatabases: [
        { name: "app_db", schemas: [] },
        { name: "archive_db", schemas: [] },
      ],
      listSchemasImpl: (database) => {
        if (database === "app_db") {
          return appSchemas.promise;
        }

        return [{ name: "public" }];
      },
      listObjectsByScope: {
        "app_db.public": [{ schema: "public", name: "users", type: "table" }],
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");

    manager.ensureSchemaScopeLoading("conn-1", { kind: "connectionRoot" });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(
      manager.getSchemaSnapshotState("conn-1", { kind: "connectionRoot" }),
    ).toEqual({
      snapshot: {
        databases: [
          { name: "app_db", schemas: [] },
          { name: "archive_db", schemas: [] },
        ],
      },
      status: "loaded",
      isPartial: false,
    });
    expect(manager.getSchemaSnapshotState("conn-1")).toMatchObject({
      status: "loading",
    });

    appSchemas.resolve([{ name: "public" }]);
    await waitForSchemaCondition(
      manager,
      "conn-1",
      () => manager.getSchemaSnapshotState("conn-1").status === "loaded",
    );
  });

  it("deduplicates expanded database and schema scope loads", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const listSchemasCalls: string[] = [];
    const listObjectsCalls: string[] = [];

    driverBehaviors.set("conn-1", {
      listDatabases: [
        { name: "app_db", schemas: [] },
        { name: "archive_db", schemas: [] },
      ],
      listSchemasImpl: (database) => {
        listSchemasCalls.push(database);
        if (database === "archive_db") {
          return [{ name: "public" }, { name: "audit" }];
        }

        return [{ name: "public" }];
      },
      listObjectsImpl: (database, schema) => {
        listObjectsCalls.push(`${database}.${schema}`);
        if (database === "archive_db" && schema === "audit") {
          return [{ schema: "audit", name: "audit_log", type: "table" }];
        }

        if (database === "app_db" && schema === "public") {
          return [{ schema: "public", name: "users", type: "table" }];
        }

        return [];
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
        "archive_db.audit.audit_log": [
          {
            name: "event_id",
            type: "bigint",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");
    await manager.getSchemaSnapshotAsync("conn-1");

    listSchemasCalls.length = 0;
    listObjectsCalls.length = 0;

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "database",
      database: "archive_db",
    });

    await waitForSchemaCondition(
      manager,
      "conn-1",
      () =>
        manager.getSchemaSnapshotState("conn-1", {
          kind: "database",
          database: "archive_db",
        }).status === "loaded",
    );

    expect(listSchemasCalls).toEqual(["archive_db"]);
    expect(listObjectsCalls).toEqual([]);

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });
    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });

    await waitForSchemaCondition(manager, "conn-1", () =>
      manager
        .getSchema("conn-1")
        .some((entry) => entry.database === "archive_db"),
    );

    expect(listObjectsCalls).toEqual(["archive_db.audit"]);
    expect(manager.getSchema("conn-1")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          database: "archive_db",
          schema: "audit",
          object: "audit_log",
          columns: [{ name: "event_id", type: "bigint" }],
        }),
      ]),
    );
  });

  it("reuses loaded non-baseline scopes from cache after collapse and re-expand", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const listSchemasCalls: string[] = [];
    const listObjectsCalls: string[] = [];

    driverBehaviors.set("conn-1", {
      listDatabases: [
        { name: "app_db", schemas: [] },
        { name: "archive_db", schemas: [] },
      ],
      listSchemasImpl: (database) => {
        listSchemasCalls.push(database);
        if (database === "archive_db") {
          return [{ name: "public" }, { name: "audit" }];
        }

        return [{ name: "public" }];
      },
      listObjectsImpl: (database, schema) => {
        listObjectsCalls.push(`${database}.${schema}`);
        if (database === "app_db" && schema === "public") {
          return [{ schema: "public", name: "users", type: "table" }];
        }

        if (database === "archive_db" && schema === "audit") {
          return [{ schema: "audit", name: "audit_log", type: "table" }];
        }

        return [];
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
        "archive_db.audit.audit_log": [
          {
            name: "event_id",
            type: "bigint",
            nullable: false,
            isPrimaryKey: false,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");
    await manager.getSchemaSnapshotAsync("conn-1");

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    await waitForSchemaCondition(
      manager,
      "conn-1",
      () =>
        manager.getSchemaSnapshotState("conn-1", {
          kind: "database",
          database: "archive_db",
        }).status === "loaded",
    );

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });
    await waitForSchemaCondition(manager, "conn-1", () =>
      manager
        .getSchema("conn-1")
        .some((entry) => entry.database === "archive_db"),
    );

    expect(listSchemasCalls).toEqual(["app_db", "archive_db"]);
    expect(listObjectsCalls).toEqual(["app_db.public", "archive_db.audit"]);

    manager.markSchemaScopeCollapsed("conn-1", {
      kind: "database",
      database: "archive_db",
    });

    expect(manager.getSchema("conn-1")).toEqual(
      expect.arrayContaining([
        {
          database: "app_db",
          schema: "public",
          object: "users",
          type: "table",
          columns: [{ name: "id", type: "int" }],
        },
        {
          database: "archive_db",
          schema: "audit",
          object: "audit_log",
          type: "table",
          columns: [{ name: "event_id", type: "bigint" }],
        },
      ]),
    );
    expect(
      manager.getSchemaSnapshotState("conn-1", {
        kind: "database",
        database: "archive_db",
      }),
    ).toMatchObject({
      status: "loaded",
      snapshot: {
        databases: [
          {
            name: "archive_db",
            schemas: [
              { name: "public", objects: [] },
              {
                name: "audit",
                objects: [],
              },
            ],
          },
        ],
      },
    });
    expect(
      manager.getSchemaSnapshotState("conn-1", {
        kind: "schema",
        database: "archive_db",
        schema: "audit",
      }),
    ).toMatchObject({
      status: "loaded",
      snapshot: {
        databases: [
          {
            name: "archive_db",
            schemas: [
              {
                name: "audit",
                objects: [
                  {
                    name: "audit_log",
                    type: "table",
                    columns: [{ name: "event_id", type: "bigint" }],
                  },
                ],
              },
            ],
          },
        ],
      },
    });

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });

    expect(listSchemasCalls).toEqual(["app_db", "archive_db"]);
    expect(listObjectsCalls).toEqual(["app_db.public", "archive_db.audit"]);

    manager.markSchemaScopeCollapsed("conn-1", {
      kind: "database",
      database: "archive_db",
    });

    manager.refreshSchemaCache({
      connectionId: "conn-1",
      reason: "manual",
    });

    await waitForSchemaCondition(
      manager,
      "conn-1",
      () =>
        manager.getSchemaSnapshotState("conn-1").status === "loaded" &&
        manager
          .getSchema("conn-1")
          .every((entry) => entry.database === "app_db"),
    );

    expect(manager.getSchema("conn-1")).toEqual([
      {
        database: "app_db",
        schema: "public",
        object: "users",
        type: "table",
        columns: [{ name: "id", type: "int" }],
      },
    ]);

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    await waitForSchemaCondition(
      manager,
      "conn-1",
      () =>
        manager.getSchemaSnapshotState("conn-1", {
          kind: "database",
          database: "archive_db",
        }).status === "loaded",
    );

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });
    await waitForSchemaCondition(manager, "conn-1", () =>
      manager
        .getSchema("conn-1")
        .some((entry) => entry.database === "archive_db"),
    );

    expect(listSchemasCalls).toEqual([
      "app_db",
      "archive_db",
      "app_db",
      "archive_db",
    ]);
    expect(listObjectsCalls).toEqual([
      "app_db.public",
      "archive_db.audit",
      "app_db.public",
      "archive_db.audit",
    ]);
  });

  it("reloads only the baseline and expanded scopes during a manual refresh", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const listSchemasCalls: string[] = [];
    const listObjectsCalls: string[] = [];
    let phase = 1;

    driverBehaviors.set("conn-1", {
      listDatabases: [
        { name: "app_db", schemas: [] },
        { name: "archive_db", schemas: [] },
        { name: "hidden_db", schemas: [] },
      ],
      listSchemasImpl: (database) => {
        listSchemasCalls.push(database);
        if (database === "archive_db") {
          return [{ name: "public" }, { name: "audit" }];
        }

        return [{ name: "public" }];
      },
      listObjectsImpl: (database, schema) => {
        listObjectsCalls.push(`${database}.${schema}`);
        if (database === "app_db" && schema === "public") {
          return phase === 1
            ? [{ schema: "public", name: "users", type: "table" }]
            : [
                { schema: "public", name: "users", type: "table" },
                { schema: "public", name: "profiles", type: "table" },
              ];
        }

        if (database === "archive_db" && schema === "audit") {
          return phase === 1
            ? [{ schema: "audit", name: "audit_log", type: "table" }]
            : [{ schema: "audit", name: "audit_log_v2", type: "table" }];
        }

        if (database === "hidden_db" && schema === "public") {
          return [{ schema: "public", name: "shadow", type: "table" }];
        }

        return [];
      },
      describeTableImpl: (database, schema, table) => [
        {
          name: `${database}_${schema}_${table}_id`,
          type: "int",
          nullable: false,
          isPrimaryKey: false,
          isForeignKey: false,
        },
      ],
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");
    await manager.getSchemaSnapshotAsync("conn-1");

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    await waitForSchemaCondition(
      manager,
      "conn-1",
      () =>
        manager.getSchemaSnapshotState("conn-1", {
          kind: "database",
          database: "archive_db",
        }).status === "loaded",
    );

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });
    await waitForSchemaCondition(manager, "conn-1", () =>
      manager.getSchema("conn-1").some((entry) => entry.object === "audit_log"),
    );

    listSchemasCalls.length = 0;
    listObjectsCalls.length = 0;
    phase = 2;

    manager.refreshSchemaCache({
      connectionId: "conn-1",
      reason: "manual",
    });

    await waitForSchemaCondition(
      manager,
      "conn-1",
      () =>
        manager
          .getSchema("conn-1")
          .some((entry) => entry.object === "profiles") &&
        manager
          .getSchema("conn-1")
          .some((entry) => entry.object === "audit_log_v2"),
    );

    expect(listSchemasCalls).toEqual(
      expect.arrayContaining(["app_db", "archive_db"]),
    );
    expect(listSchemasCalls).not.toContain("hidden_db");
    expect(listObjectsCalls).toEqual(
      expect.arrayContaining(["app_db.public", "archive_db.audit"]),
    );
    expect(listObjectsCalls).not.toContain("hidden_db.public");
  });

  it("suppresses stale scope merges after refresh invalidates an in-flight generation", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const firstArchiveObjects = createDeferred<TableInfo[]>();
    const secondArchiveObjects = createDeferred<TableInfo[]>();
    let archiveSchemaLoadCount = 0;

    driverBehaviors.set("conn-1", {
      listDatabases: [
        { name: "app_db", schemas: [] },
        { name: "archive_db", schemas: [] },
      ],
      listSchemasImpl: (database) => {
        if (database === "archive_db") {
          return [{ name: "public" }, { name: "audit" }];
        }

        return [{ name: "public" }];
      },
      listObjectsImpl: (database, schema) => {
        if (database === "archive_db" && schema === "audit") {
          archiveSchemaLoadCount += 1;
          return archiveSchemaLoadCount === 1
            ? firstArchiveObjects.promise
            : secondArchiveObjects.promise;
        }

        if (database === "app_db" && schema === "public") {
          return [{ schema: "public", name: "users", type: "table" }];
        }

        return [];
      },
      describeTableImpl: (database, schema, table) => [
        {
          name: `${database}_${schema}_${table}_id`,
          type: "int",
          nullable: false,
          isPrimaryKey: false,
          isForeignKey: false,
        },
      ],
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");
    await manager.getSchemaSnapshotAsync("conn-1");

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "database",
      database: "archive_db",
    });
    await waitForSchemaCondition(
      manager,
      "conn-1",
      () =>
        manager.getSchemaSnapshotState("conn-1", {
          kind: "database",
          database: "archive_db",
        }).status === "loaded",
    );

    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "archive_db",
      schema: "audit",
    });

    manager.refreshSchemaCache({
      connectionId: "conn-1",
      reason: "manual",
    });

    firstArchiveObjects.resolve([
      { schema: "audit", name: "stale_audit_log", type: "table" },
    ]);
    await Promise.resolve();
    await Promise.resolve();

    expect(
      manager
        .getSchema("conn-1")
        .some((entry) => entry.object === "stale_audit_log"),
    ).toBe(false);

    secondArchiveObjects.resolve([
      { schema: "audit", name: "fresh_audit_log", type: "table" },
    ]);

    await waitForSchemaCondition(manager, "conn-1", () =>
      manager
        .getSchema("conn-1")
        .some((entry) => entry.object === "fresh_audit_log"),
    );

    expect(
      manager
        .getSchema("conn-1")
        .some((entry) => entry.object === "stale_audit_log"),
    ).toBe(false);
  });

  it("deduplicates identical trailing history entries and trims to the configured limit", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setHistoryLimit(2);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.addToHistory("conn-1", "  select 1  ");
    await manager.addToHistory("conn-1", "select 1");
    await manager.addToHistory("conn-1", "select 2");
    await manager.addToHistory("conn-1", "select 3");

    expect(store.readHistory().map((entry) => entry.sql)).toEqual([
      "select 3",
      "select 2",
    ]);
  });
});
