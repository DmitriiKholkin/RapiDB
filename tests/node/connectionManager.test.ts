import * as http from "node:http";
import * as https from "node:https";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/extension/connectionManagerModels";
import type { DriverTimeoutSettingsProvider } from "../../src/extension/dbDrivers/timeout";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverCapabilities,
  DriverEntityManifest,
  IDBDriver,
  IndexMeta,
  SchemaInfo,
  TableConstraintMeta,
  TableInfo,
  TriggerMeta,
} from "../../src/extension/dbDrivers/types";
import { DEFAULT_DRIVER_ENTITY_MANIFEST } from "../../src/extension/dbDrivers/types";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { MockEventEmitter } from "../support/mockVscode";

interface DriverBehavior {
  connectError?: unknown;
  connectImpl?: () => Promise<void>;
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
  getConstraintsByScope?: Record<string, TableConstraintMeta[]>;
  getIndexesByScope?: Record<string, IndexMeta[]>;
  getConstraintsImpl?: (
    database: string,
    schema: string,
    table: string,
  ) => TableConstraintMeta[] | Promise<TableConstraintMeta[]>;
  getIndexesImpl?: (
    database: string,
    schema: string,
    table: string,
  ) => IndexMeta[] | Promise<IndexMeta[]>;
  getTriggersByScope?: Record<string, TriggerMeta[] | null>;
  getTriggersImpl?: (
    database: string,
    schema: string,
    table: string,
  ) => TriggerMeta[] | null | Promise<TriggerMeta[] | null>;
  capabilities?: DriverCapabilities;
  getCapabilitiesImpl?: (driver: FakeDriver) => DriverCapabilities;
  entityManifest?: DriverEntityManifest;
  getEntityManifestImpl?: (driver: FakeDriver) => DriverEntityManifest;
}

const driverBehaviors = new Map<string, DriverBehavior>();
const driverInstances: FakeDriver[] = [];

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
}

function createSshPgConfig(id: string): ConnectionConfig {
  return {
    id,
    name: `SSH ${id}`,
    type: "pg",
    host: "db.internal",
    port: 5432,
    database: "app",
    username: "postgres",
    sshEnabled: true,
    sshHost: "bastion.example.com",
    sshPort: 22,
    sshUsername: "tunnel",
    sshAuthMethod: "password",
    sshHostVerificationMode: "manual",
    sshPassword: "ssh-secret",
    sshHostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
  };
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

async function waitForTableDetailCondition(
  manager: {
    onDidChangeSchemaState(listener: (connectionId: string) => void): {
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
    const subscription = manager.onDidChangeSchemaState(
      (changedConnectionId) => {
        if (changedConnectionId !== connectionId || !predicate()) {
          return;
        }

        subscription.dispose();
        resolve();
      },
    );
  });
}

class FakeDriver implements IDBDriver {
  connectCalls = 0;
  disconnectCalls = 0;
  describeTableCalls: string[] = [];
  describeColumnsCalls: string[] = [];
  getConstraintsCalls: string[] = [];
  getIndexesCalls: string[] = [];
  getTriggersCalls: string[] = [];
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
    if (behavior?.connectImpl) {
      await behavior.connectImpl();
    }
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

  getEntityManifest(): DriverEntityManifest {
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.getEntityManifestImpl) {
      return behavior.getEntityManifestImpl(this);
    }

    return behavior?.entityManifest ?? DEFAULT_DRIVER_ENTITY_MANIFEST;
  }

  getCapabilities(): DriverCapabilities {
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.getCapabilitiesImpl) {
      return behavior.getCapabilitiesImpl(this);
    }

    return (
      behavior?.capabilities ?? {
        tabularRead: "sql",
      }
    );
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

  async describeColumns(
    database = "",
    schema = "",
    table = "",
  ): Promise<ColumnTypeMeta[]> {
    this.describeColumnsCalls.push(`${database}.${schema}.${table}`);
    const columns = await this.describeTable(database, schema, table);
    return columns.map((column) => ({
      ...column,
      category: "other",
      nativeType: column.type,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    }));
  }

  async getIndexes(database = "", schema = "", table = "") {
    this.getIndexesCalls.push(`${database}.${schema}.${table}`);
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.getIndexesImpl) {
      return behavior.getIndexesImpl(database, schema, table);
    }

    return (
      behavior?.getIndexesByScope?.[`${database}.${schema}.${table}`] ?? []
    );
  }

  async getForeignKeys() {
    return [];
  }

  async getConstraints(
    database = "",
    schema = "",
    table = "",
  ): Promise<TableConstraintMeta[]> {
    this.getConstraintsCalls.push(`${database}.${schema}.${table}`);
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.getConstraintsImpl) {
      return behavior.getConstraintsImpl(database, schema, table);
    }

    return (
      behavior?.getConstraintsByScope?.[`${database}.${schema}.${table}`] ?? []
    );
  }

  async getTriggers(
    database = "",
    schema = "",
    table = "",
  ): Promise<TriggerMeta[] | null> {
    this.getTriggersCalls.push(`${database}.${schema}.${table}`);
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.getTriggersImpl) {
      return behavior.getTriggersImpl(database, schema, table);
    }

    return (
      behavior?.getTriggersByScope?.[`${database}.${schema}.${table}`] ?? []
    );
  }

  async getConstraintDDL(): Promise<string> {
    return "";
  }

  async getIndexDDL(): Promise<string> {
    return "";
  }

  async getTriggerDDL(): Promise<string> {
    return "";
  }

  async getCreateTableDDL(): Promise<string> {
    return "";
  }

  async getObjectDefinition(): Promise<string | null> {
    return null;
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
    DynamoDBDriver: FakeDriver,
    ElasticsearchDriver: FakeDriver,
    MSSQLDriver: FakeDriver,
    MongoDBDriver: FakeDriver,
    MySQLDriver: FakeDriver,
    OracleDriver: FakeDriver,
    PostgresDriver: FakeDriver,
    RedisDriver: FakeDriver,
    SQLiteDriver: FakeDriver,
  };
}

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

vi.mock("../../src/extension/dbDrivers/dynamodb", driverFactory);
vi.mock("../../src/extension/dbDrivers/elasticsearch", driverFactory);
vi.mock("../../src/extension/dbDrivers/mssql", driverFactory);
vi.mock("../../src/extension/dbDrivers/mongodb", driverFactory);
vi.mock("../../src/extension/dbDrivers/mysql", driverFactory);
vi.mock("../../src/extension/dbDrivers/oracle", driverFactory);
vi.mock("../../src/extension/dbDrivers/postgres", driverFactory);
vi.mock("../../src/extension/dbDrivers/redis", driverFactory);
vi.mock("../../src/extension/dbDrivers/sqlite", driverFactory);

beforeEach(() => {
  driverBehaviors.clear();
  driverInstances.splice(0, driverInstances.length);
  vi.resetModules();
});

describe("ConnectionManager", () => {
  it("connects successfully across all supported driver types with minimal valid configs", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    const scenarios: ConnectionConfig[] = [
      {
        id: "conn-pg",
        name: "Postgres",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
      {
        id: "conn-mysql",
        name: "MySQL",
        type: "mysql",
        host: "localhost",
        database: "app",
        username: "root",
      },
      {
        id: "conn-sqlite",
        name: "SQLite",
        type: "sqlite",
        filePath: "/tmp/app.db",
      },
      {
        id: "conn-mssql",
        name: "MSSQL",
        type: "mssql",
        host: "localhost",
        database: "app",
      },
      {
        id: "conn-oracle",
        name: "Oracle",
        type: "oracle",
        database: "FREEPDB1",
      },
      {
        id: "conn-mongodb",
        name: "MongoDB",
        type: "mongodb",
        uri: "mongodb://localhost:27017/app",
        authDatabase: "admin",
        authSource: "legacy-admin",
      },
      {
        id: "conn-redis",
        name: "Redis",
        type: "redis",
        connectionUri: "redis://localhost:6379",
      },
      {
        id: "conn-elasticsearch",
        name: "Elasticsearch",
        type: "elasticsearch",
        cloudId: "deployment:ZXM=",
      },
      {
        id: "conn-dynamodb",
        name: "DynamoDB",
        type: "dynamodb",
        awsRegion: "us-east-1",
      },
    ];
    store.setConnections(scenarios);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    for (const scenario of scenarios) {
      await manager.connectTo(scenario.id);
    }

    expect(driverInstances).toHaveLength(9);
    for (const scenario of scenarios) {
      expect(manager.isConnected(scenario.id)).toBe(true);
    }
  });

  it("returns validation errors for invalid testConnection requests across all drivers", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
    );

    const scenarios: Array<{
      config: Omit<ConnectionConfig, "id">;
      expected: string;
    }> = [
      {
        config: {
          name: "Broken PG",
          type: "pg",
          host: "localhost",
          database: "app",
        },
        expected: "username",
      },
      {
        config: {
          name: "Broken MySQL",
          type: "mysql",
          host: "localhost",
          database: "app",
        },
        expected: "username",
      },
      {
        config: {
          name: "Broken SQLite",
          type: "sqlite",
        },
        expected: "filePath",
      },
      {
        config: {
          name: "Broken MSSQL",
          type: "mssql",
          database: "app",
        },
        expected: "host",
      },
      {
        config: {
          name: "Broken Oracle",
          type: "oracle",
        },
        expected: "serviceName",
      },
      {
        config: {
          name: "Broken MongoDB",
          type: "mongodb",
        },
        expected: "connectionUri",
      },
      {
        config: {
          name: "Broken Redis",
          type: "redis",
        },
        expected: "connectionUri",
      },
      {
        config: {
          name: "Broken Elasticsearch",
          type: "elasticsearch",
        },
        expected: "endpoint",
      },
      {
        config: {
          name: "Broken DynamoDB",
          type: "dynamodb",
        },
        expected: "awsRegion",
      },
    ];

    for (const scenario of scenarios) {
      driverInstances.splice(0, driverInstances.length);
      const result = await manager.testConnection(scenario.config);

      expect(result.success).toBe(false);
      expect(result.error).toContain(scenario.expected);
      expect(result.validation).toEqual(
        expect.objectContaining({
          valid: false,
          issues: expect.any(Array),
        }),
      );
      expect(driverInstances).toHaveLength(0);
    }
  });

  it("rejects connectTo for invalid stored configs and does not create a driver", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-invalid-pg",
        name: "Broken PG",
        type: "pg",
        host: "localhost",
        database: "app",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await expect(manager.connectTo("conn-invalid-pg")).rejects.toThrow(
      /username/i,
    );
    expect(manager.isConnected("conn-invalid-pg")).toBe(false);
    expect(driverInstances).toHaveLength(0);
  });

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
        host: "localhost",
        database: "app",
        username: "postgres",
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

  it("fences stale in-flight connect completion after disconnect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const connectDeferred = createDeferred<void>();
    driverBehaviors.set("conn-1", {
      connectImpl: () => connectDeferred.promise,
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    const connectPromise = manager.connectTo("conn-1");
    await Promise.resolve();

    await manager.disconnectFrom("conn-1");
    connectDeferred.resolve();
    await connectPromise;

    expect(manager.isConnected("conn-1")).toBe(false);
    expect(driverInstances).toHaveLength(1);
    expect(driverInstances[0]?.disconnectCalls).toBeGreaterThanOrEqual(1);
  });

  it("cleans per-connection runtime maps on disconnect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    const managerState = manager as unknown as {
      _driverStaticMetadataCache: Map<string, unknown>;
      _schemaCacheMap: Map<string, unknown>;
      _schemaGenerationMap: Map<string, number>;
      _schemaExpandedScopeKeyMap: Map<string, Set<string>>;
      _connectionEpochMap: Map<string, number>;
    };
    managerState._driverStaticMetadataCache.set("conn-1", {
      manifest: { dbObjectKinds: ["table"] },
    });
    managerState._schemaCacheMap.set("conn-1", { status: "loaded" });
    managerState._schemaGenerationMap.set("conn-1", 2);
    managerState._schemaExpandedScopeKeyMap.set(
      "conn-1",
      new Set(["connectionRoot"]),
    );
    managerState._connectionEpochMap.set("conn-1", 5);

    await manager.disconnectFrom("conn-1");

    expect(managerState._driverStaticMetadataCache.has("conn-1")).toBe(false);
    expect(managerState._schemaCacheMap.has("conn-1")).toBe(false);
    expect(managerState._schemaGenerationMap.has("conn-1")).toBe(false);
    expect(managerState._schemaExpandedScopeKeyMap.has("conn-1")).toBe(false);
    expect(managerState._connectionEpochMap.get("conn-1")).toBe(6);
  });

  it("allows a fresh connect attempt after disconnect fences a stale in-flight attempt", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const firstConnectDeferred = createDeferred<void>();
    let shouldBlockFirstConnect = true;
    driverBehaviors.set("conn-1", {
      connectImpl: async () => {
        if (shouldBlockFirstConnect) {
          await firstConnectDeferred.promise;
        }
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    const staleAttempt = manager.connectTo("conn-1");
    await Promise.resolve();

    await manager.disconnectFrom("conn-1");

    shouldBlockFirstConnect = false;
    const freshAttempt = manager.connectTo("conn-1");
    firstConnectDeferred.resolve();

    await Promise.all([staleAttempt, freshAttempt]);

    expect(driverInstances).toHaveLength(2);
    expect(driverInstances[0]?.disconnectCalls).toBeGreaterThanOrEqual(1);
    expect(driverInstances[1]?.connectCalls).toBe(1);
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
        host: "localhost",
        database: "app",
        username: "postgres",
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

  it("resolves disconnected static metadata from stored config without connecting", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const manifest: DriverEntityManifest = {
      dbObjectKinds: ["table"],
      tableSections: {
        columns: "supported",
        constraints: "not_applicable",
        indexes: "not_applicable",
        triggers: "not_applicable",
      },
    };
    const capabilities: DriverCapabilities = {
      tabularRead: "nosql",
      queryMode: "text",
      editorPresentation: {
        queryMode: "text",
        editorLanguage: "javascript",
        allowFormatting: false,
      },
    };

    driverBehaviors.set("conn-static", {
      entityManifest: manifest,
      capabilities,
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-static",
        name: "Static metadata",
        type: "redis",
        database: "app_db",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    expect(manager.isConnected("conn-static")).toBe(false);
    expect(manager.getDriverEntityManifest("conn-static")).toEqual(manifest);
    expect(manager.getDriverCapabilities("conn-static")).toEqual(capabilities);
    expect(manager.getQueryEditorPresentation("conn-static")).toEqual(
      capabilities.editorPresentation,
    );
    expect(manager.getDriverEntityManifest("conn-static")).toEqual(manifest);
    expect(driverInstances).toHaveLength(1);
    expect(driverInstances.every((driver) => driver.connectCalls === 0)).toBe(
      true,
    );
    expect(
      driverInstances.every((driver) => driver.isConnected() === false),
    ).toBe(true);
  });

  it("resolves connected static metadata from the live driver instance", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const disconnectedManifest: DriverEntityManifest = {
      dbObjectKinds: ["table"],
      tableSections: {
        columns: "supported",
        constraints: "not_applicable",
        indexes: "not_applicable",
        triggers: "not_applicable",
      },
    };
    const liveManifest: DriverEntityManifest = {
      dbObjectKinds: ["table", "view"],
      tableSections: {
        columns: "supported",
        constraints: "supported",
        indexes: "supported",
        triggers: "not_applicable",
      },
    };
    const disconnectedCapabilities: DriverCapabilities = {
      tabularRead: "nosql",
      queryMode: "text",
      editorPresentation: {
        queryMode: "text",
        editorLanguage: "javascript",
        allowFormatting: false,
      },
    };
    const liveCapabilities: DriverCapabilities = {
      tabularRead: "sql",
      queryMode: "sql",
      editorPresentation: {
        queryMode: "sql",
        editorLanguage: "sql",
        allowFormatting: true,
      },
    };

    driverBehaviors.set("conn-live-static", {
      getEntityManifestImpl: (driver) =>
        driver.isConnected() ? liveManifest : disconnectedManifest,
      getCapabilitiesImpl: (driver) =>
        driver.isConnected() ? liveCapabilities : disconnectedCapabilities,
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-live-static",
        name: "Live metadata",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.connectTo("conn-live-static");

    expect(driverInstances).toHaveLength(1);
    expect(manager.getDriverEntityManifest("conn-live-static")).toEqual(
      liveManifest,
    );
    expect(manager.getDriverCapabilities("conn-live-static")).toEqual(
      liveCapabilities,
    );
    expect(manager.getQueryEditorPresentation("conn-live-static")).toEqual(
      liveCapabilities.editorPresentation,
    );
    expect(driverInstances).toHaveLength(1);
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
        host: "localhost",
        database: "app",
        username: "postgres",
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
      host: "localhost",
      database: "next_db",
      username: "postgres",
    });

    expect(connectedDriver?.disconnectCalls).toBe(1);
    expect(manager.isConnected("conn-1")).toBe(false);
    expect(store.getConnections()[0]?.name).toBe("Primary Updated");
  });

  it("rejects invalid saveConnection payloads and keeps persisted settings unchanged", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await expect(
      manager.saveConnection({
        id: "conn-1",
        name: "Primary Updated",
        type: "pg",
        database: "next_db",
      }),
    ).rejects.toThrow(/username|host/i);

    expect(store.getConnections()[0]).toEqual(
      expect.objectContaining({
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      }),
    );
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

    const managerState = manager as unknown as {
      _driverStaticMetadataCache: Map<string, unknown>;
      _schemaCacheMap: Map<string, unknown>;
      _schemaGenerationMap: Map<string, number>;
      _schemaExpandedScopeKeyMap: Map<string, Set<string>>;
      _connectionEpochMap: Map<string, number>;
    };
    managerState._driverStaticMetadataCache.set("conn-1", {
      manifest: { dbObjectKinds: ["table"] },
    });
    managerState._schemaCacheMap.set("conn-1", { status: "loaded" });
    managerState._schemaGenerationMap.set("conn-1", 7);
    managerState._schemaExpandedScopeKeyMap.set(
      "conn-1",
      new Set(["connectionRoot"]),
    );
    managerState._connectionEpochMap.set("conn-1", 3);

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
    expect(managerState._driverStaticMetadataCache.has("conn-1")).toBe(false);
    expect(managerState._schemaCacheMap.has("conn-1")).toBe(false);
    expect(managerState._schemaGenerationMap.has("conn-1")).toBe(false);
    expect(managerState._schemaExpandedScopeKeyMap.has("conn-1")).toBe(false);
    expect(managerState._connectionEpochMap.get("conn-1")).toBe(4);
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
      host: "localhost",
      database: "app",
      username: "root",
    });

    expect(result.success).toBe(false);
    expect(result.error).toContain("ECONNREFUSED");
    expect(driverInstances).toHaveLength(1);
    expect(driverInstances[0]?.disconnectCalls).toBe(1);
  });

  it("hydrates stored DynamoDB credentials from Secret Storage before connecting", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-ddb",
        name: "Dynamo",
        type: "dynamodb",
        awsRegion: "us-east-1",
        useSecretStorage: true,
      },
    ]);
    store.setSecret(
      "conn-ddb",
      JSON.stringify({
        awsAccessKeyId: "AKIA123",
        awsSecretAccessKey: "secret-key",
        awsSessionToken: "session-token",
      }),
    );

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.connectTo("conn-ddb");

    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-ddb",
      type: "dynamodb",
      awsRegion: "us-east-1",
      awsAccessKeyId: "AKIA123",
      awsSecretAccessKey: "secret-key",
      awsSessionToken: "session-token",
    });
  });

  it("migrates persisted plaintext password to Secret Storage on connect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-legacy",
        name: "Legacy",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        password: "legacy-password",
        useSecretStorage: false,
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.connectTo("conn-legacy");

    const persisted = store
      .getConnections()
      .find((connection) => connection.id === "conn-legacy");
    expect(persisted).toMatchObject({
      id: "conn-legacy",
      useSecretStorage: true,
    });
    expect(persisted?.password).toBeUndefined();

    await expect(store.getSecret("conn-legacy")).resolves.toContain(
      "legacy-password",
    );
  });

  it("redacts plaintext API key from persisted config when saving to Secret Storage", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.saveConnection({
      id: "conn-es-legacy",
      name: "Legacy Elasticsearch",
      type: "elasticsearch",
      endpoint: "https://cluster.example.com",
      apiKey: "plaintext-key",
      useSecretStorage: false,
    });

    const persisted = store
      .getConnections()
      .find((connection) => connection.id === "conn-es-legacy");
    expect(persisted).toMatchObject({
      id: "conn-es-legacy",
      useSecretStorage: true,
    });
    expect(persisted?.apiKey).toBeUndefined();

    await expect(store.getSecret("conn-es-legacy")).resolves.toContain(
      "plaintext-key",
    );
  });

  it("redacts credential-bearing Elasticsearch endpoint in persisted config and hydrates it before connect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.saveConnection({
      id: "conn-es-endpoint-uri",
      name: "Elastic With Endpoint Secret",
      type: "elasticsearch",
      endpoint: "https://elastic-user:elastic-pass@cluster.example.com",
      useSecretStorage: false,
    });

    const persisted = store
      .getConnections()
      .find((connection) => connection.id === "conn-es-endpoint-uri");
    expect(persisted).toMatchObject({
      id: "conn-es-endpoint-uri",
      useSecretStorage: true,
      endpoint: "https://cluster.example.com",
    });

    const secret = await store.getSecret("conn-es-endpoint-uri");
    expect(secret).toContain(
      "https://elastic-user:elastic-pass@cluster.example.com",
    );

    await manager.connectTo("conn-es-endpoint-uri");
    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-es-endpoint-uri",
      endpoint: "https://elastic-user:elastic-pass@cluster.example.com",
    });
  });

  it("redacts credential-bearing Mongo URI in persisted config and hydrates before connect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.saveConnection({
      id: "conn-mongo-uri",
      name: "Mongo With URI Secret",
      type: "mongodb",
      uri: "mongodb://db-user:db-pass@localhost:27017/app",
      useSecretStorage: false,
    });

    const persisted = store
      .getConnections()
      .find((connection) => connection.id === "conn-mongo-uri");
    expect(persisted).toMatchObject({
      id: "conn-mongo-uri",
      useSecretStorage: true,
      uri: "mongodb://localhost:27017/app",
    });

    const secret = await store.getSecret("conn-mongo-uri");
    expect(secret).toContain("mongodb://db-user:db-pass@localhost:27017/app");

    await manager.connectTo("conn-mongo-uri");
    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-mongo-uri",
      uri: "mongodb://db-user:db-pass@localhost:27017/app",
    });
  });

  it("stores SSH private key credentials in Secret Storage and hydrates them before connect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    const createSshRuntime = vi.fn(async () => ({
      transport: {
        kind: "tcpForward" as const,
        localHost: "127.0.0.1" as const,
        localPort: 15435,
        remoteHost: "db.internal",
        remotePort: 5432,
      },
      verifiedFingerprintSha256:
        "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      dispose: vi.fn(async () => undefined),
    }));

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
      { createSshRuntime },
    );

    await manager.saveConnection({
      id: "conn-ssh-pg",
      name: "PG over SSH",
      type: "pg",
      host: "db.internal",
      database: "app",
      username: "postgres",
      sshEnabled: true,
      sshHost: "bastion.example.com",
      sshPort: 22,
      sshUsername: "tunnel",
      sshAuthMethod: "privateKey",
      sshPrivateKey:
        "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
      sshPassphrase: "key-passphrase",
      sshHostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      useSecretStorage: false,
    });

    const persisted = store
      .getConnections()
      .find((connection) => connection.id === "conn-ssh-pg");
    expect(persisted).toMatchObject({
      id: "conn-ssh-pg",
      sshEnabled: true,
      sshHost: "bastion.example.com",
      sshPort: 22,
      sshUsername: "tunnel",
      sshAuthMethod: "privateKey",
      sshHostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      useSecretStorage: true,
    });
    expect(persisted?.sshPrivateKey).toBeUndefined();
    expect(persisted?.sshPassphrase).toBeUndefined();

    await expect(store.getSecret("conn-ssh-pg")).resolves.toBe(
      JSON.stringify({
        sshPrivateKey:
          "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
        sshPassphrase: "key-passphrase",
      }),
    );

    await manager.connectTo("conn-ssh-pg");
    expect(createSshRuntime).toHaveBeenCalledTimes(1);
    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-ssh-pg",
      sshPrivateKey:
        "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
      sshPassphrase: "key-passphrase",
    });
  });

  it("rewrites SSH-enabled test connections to a local forward and disposes the runtime in finally", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const dispose = vi.fn(async () => undefined);
    const createSshRuntime = vi.fn(async () => ({
      transport: {
        kind: "tcpForward" as const,
        localHost: "127.0.0.1" as const,
        localPort: 15432,
        remoteHost: "db.internal",
        remotePort: 5432,
      },
      verifiedFingerprintSha256:
        "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      dispose,
    }));

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
      { createSshRuntime },
    );

    const { id: _connectionId, ...connectionToTest } =
      createSshPgConfig("conn-test-ssh");

    const result = await manager.testConnection(connectionToTest);

    expect(result).toEqual({ success: true });
    expect(createSshRuntime).toHaveBeenCalledTimes(1);
    expect(driverInstances[0]?.config).toMatchObject({
      host: "127.0.0.1",
      port: 15432,
      runtimeOverrides: {
        transport: {
          kind: "tcpForward",
          localHost: "127.0.0.1",
          localPort: 15432,
          remoteHost: "db.internal",
          remotePort: 5432,
        },
        tlsServername: "db.internal",
      },
    });
    expect(driverInstances[0]?.disconnectCalls).toBe(1);
    expect(dispose).toHaveBeenCalledTimes(1);
  });

  it("keeps SSH runtime alive for active connections and disposes it on disconnect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([createSshPgConfig("conn-ssh-active")]);

    const dispose = vi.fn(async () => undefined);
    const createSshRuntime = vi.fn(async () => ({
      transport: {
        kind: "tcpForward" as const,
        localHost: "127.0.0.1" as const,
        localPort: 15433,
        remoteHost: "db.internal",
        remotePort: 5432,
      },
      verifiedFingerprintSha256:
        "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      dispose,
    }));

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
      { createSshRuntime },
    );

    await manager.connectTo("conn-ssh-active");

    expect(createSshRuntime).toHaveBeenCalledTimes(1);
    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-ssh-active",
      host: "127.0.0.1",
      port: 15433,
      runtimeOverrides: {
        tlsServername: "db.internal",
      },
    });
    expect(dispose).not.toHaveBeenCalled();

    await manager.disconnectFrom("conn-ssh-active");

    expect(driverInstances[0]?.disconnectCalls).toBeGreaterThanOrEqual(1);
    expect(dispose).toHaveBeenCalledTimes(1);
  });

  it("keeps SSH HTTP-agent runtimes alive for Elasticsearch Cloud connections and disposes them on disconnect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-es-ssh",
        name: "Elastic SSH",
        type: "elasticsearch",
        cloudId: "deployment:ZXM=",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "password",
        sshPassword: "ssh-secret",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      },
    ]);

    const httpAgent = new http.Agent();
    const httpsAgent = new https.Agent();
    const dispose = vi.fn(async () => {
      httpAgent.destroy();
      httpsAgent.destroy();
    });
    const createSshRuntime = vi.fn(async () => ({
      transport: {
        kind: "httpAgent" as const,
        httpAgent,
        httpsAgent,
      },
      verifiedFingerprintSha256:
        "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      dispose,
    }));

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
      { createSshRuntime },
    );

    await manager.connectTo("conn-es-ssh");

    expect(createSshRuntime).toHaveBeenCalledTimes(1);
    expect(createSshRuntime).toHaveBeenCalledWith(
      {
        host: "bastion.example.com",
        port: 22,
        username: "tunnel",
        hostVerificationMode: "manual",
        fingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        auth: {
          kind: "password",
          password: "ssh-secret",
        },
      },
      {
        kind: "httpAgent",
      },
    );
    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-es-ssh",
      cloudId: "deployment:ZXM=",
      runtimeOverrides: {
        transport: {
          kind: "httpAgent",
          httpAgent,
          httpsAgent,
        },
      },
    });
    expect(dispose).not.toHaveBeenCalled();

    await manager.disconnectFrom("conn-es-ssh");

    expect(driverInstances[0]?.disconnectCalls).toBeGreaterThanOrEqual(1);
    expect(dispose).toHaveBeenCalledTimes(1);
  });

  it("pins the learned SSH fingerprint after the first trust-on-first-use handshake", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        ...createSshPgConfig("conn-ssh-tofu"),
        sshHostVerificationMode: "trustOnFirstUse",
        sshHostFingerprintSha256: undefined,
      },
    ]);

    const createSshRuntime = vi.fn(async () => ({
      transport: {
        kind: "tcpForward" as const,
        localHost: "127.0.0.1" as const,
        localPort: 15436,
        remoteHost: "db.internal",
        remotePort: 5432,
      },
      verifiedFingerprintSha256:
        "SHA256:LearnedTrustOnFirstUseFingerprint1234567890+/=",
      dispose: vi.fn(async () => undefined),
    }));

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
      { createSshRuntime },
    );

    await manager.connectTo("conn-ssh-tofu");

    expect(createSshRuntime).toHaveBeenCalledWith(
      {
        host: "bastion.example.com",
        port: 22,
        username: "tunnel",
        hostVerificationMode: "trustOnFirstUse",
        fingerprintSha256: undefined,
        auth: {
          kind: "password",
          password: "ssh-secret",
        },
      },
      {
        kind: "tcpForward",
        remoteHost: "db.internal",
        remotePort: 5432,
      },
    );

    expect(
      store
        .getConnections()
        .find((connection) => connection.id === "conn-ssh-tofu")
        ?.sshHostFingerprintSha256,
    ).toBe("SHA256:LearnedTrustOnFirstUseFingerprint1234567890+/=");
  });

  it("disposes SSH runtime when a connect attempt becomes stale", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([createSshPgConfig("conn-ssh-stale")]);

    const connectDeferred = createDeferred<void>();
    const connectStarted = createDeferred<void>();
    driverBehaviors.set("conn-ssh-stale", {
      connectImpl: async () => {
        connectStarted.resolve();
        await connectDeferred.promise;
      },
    });

    const dispose = vi.fn(async () => undefined);
    const createSshRuntime = vi.fn(async () => ({
      transport: {
        kind: "tcpForward" as const,
        localHost: "127.0.0.1" as const,
        localPort: 15434,
        remoteHost: "db.internal",
        remotePort: 5432,
      },
      verifiedFingerprintSha256:
        "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      dispose,
    }));

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
      { createSshRuntime },
    );

    const attempt = manager.beginConnect("conn-ssh-stale");
    await connectStarted.promise;
    await manager.disconnectFrom("conn-ssh-stale");
    connectDeferred.resolve();
    await attempt.promise;

    expect(manager.isConnected("conn-ssh-stale")).toBe(false);
    expect(driverInstances[0]?.disconnectCalls).toBeGreaterThanOrEqual(1);
    expect(dispose).toHaveBeenCalledTimes(1);
  });

  it("migrates plaintext credential-bearing connection URI to Secret Storage on connect", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-redis-legacy",
        name: "Legacy Redis",
        type: "redis",
        connectionUri: "redis://user:pass@localhost:6379",
        useSecretStorage: false,
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.connectTo("conn-redis-legacy");

    const persisted = store
      .getConnections()
      .find((connection) => connection.id === "conn-redis-legacy");
    expect(persisted).toMatchObject({
      id: "conn-redis-legacy",
      useSecretStorage: true,
      connectionUri: "redis://localhost:6379",
    });

    const secret = await store.getSecret("conn-redis-legacy");
    expect(secret).toContain("redis://user:pass@localhost:6379");

    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-redis-legacy",
      connectionUri: "redis://user:pass@localhost:6379",
    });
  });

  it("does not overwrite a newer saveConnection while background secret migration is in-flight", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-race-save",
        name: "Legacy",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        password: "legacy-password",
        useSecretStorage: false,
      },
    ]);

    const casGate = createDeferred<void>();
    const originalSaveIfRevision = store.saveConnectionsIfRevision.bind(store);
    const casSpy = vi
      .spyOn(store, "saveConnectionsIfRevision")
      .mockImplementation(async (expectedRevision, connections) => {
        await casGate.promise;
        return originalSaveIfRevision(expectedRevision, connections);
      });

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    manager.getConnections();
    await Promise.resolve();

    await manager.saveConnection({
      id: "conn-race-save",
      name: "Updated",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "postgres",
      password: "new-password",
      useSecretStorage: true,
    });

    casGate.resolve();
    await (
      manager as unknown as {
        _pendingSecretMigration: Promise<void> | null;
      }
    )._pendingSecretMigration;

    const persisted = store
      .getConnections()
      .find((connection) => connection.id === "conn-race-save");
    expect(persisted?.name).toBe("Updated");
    expect(persisted?.useSecretStorage).toBe(true);
    expect(casSpy).toHaveBeenCalledTimes(1);
  });

  it("does not resurrect a removed connection when background secret migration commits stale state", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-race-remove",
        name: "Legacy",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        password: "legacy-password",
        useSecretStorage: false,
      },
      {
        id: "conn-keep",
        name: "Keep",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
    ]);

    const casGate = createDeferred<void>();
    const originalSaveIfRevision = store.saveConnectionsIfRevision.bind(store);
    const casSpy = vi
      .spyOn(store, "saveConnectionsIfRevision")
      .mockImplementation(async (expectedRevision, connections) => {
        await casGate.promise;
        return originalSaveIfRevision(expectedRevision, connections);
      });

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    manager.getConnections();
    await Promise.resolve();

    await expect(manager.removeConnection("conn-race-remove")).resolves.toBe(
      true,
    );

    casGate.resolve();
    await (
      manager as unknown as {
        _pendingSecretMigration: Promise<void> | null;
      }
    )._pendingSecretMigration;

    expect(store.getConnections().map((connection) => connection.id)).toEqual([
      "conn-keep",
    ]);
    expect(casSpy).toHaveBeenCalledTimes(1);
  });

  it("disposes active drivers and rejects future manager operations", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.connectTo("conn-1");
    await manager.dispose();

    expect(manager.isConnected("conn-1")).toBe(false);
    await expect(manager.connectTo("conn-1")).rejects.toThrow(/disposed/i);
    expect(driverInstances[0]?.disconnectCalls).toBe(1);
  });

  it("hydrates stored Elasticsearch API keys from Secret Storage before connecting", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-es",
        name: "Elastic",
        type: "elasticsearch",
        endpoint: "https://cluster.example.com",
        useSecretStorage: true,
      },
    ]);
    store.setSecret(
      "conn-es",
      JSON.stringify({
        apiKey: "base64-api-key",
      }),
    );

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.connectTo("conn-es");

    expect(driverInstances[0]?.config).toMatchObject({
      id: "conn-es",
      type: "elasticsearch",
      endpoint: "https://cluster.example.com",
      apiKey: "base64-api-key",
    });
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
          {
            schema: "public",
            name: "daily_users",
            type: "materializedView",
          },
          { schema: "public", name: "rebuild_cache", type: "procedure" },
          { schema: "public", name: "users_id_seq", type: "sequence" },
          { schema: "public", name: "user_status", type: "type" },
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
        "app_db.public.daily_users": [
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
        host: "localhost",
        username: "postgres",
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
        expect.objectContaining({
          database: "app_db",
          schema: "public",
          object: "daily_users",
          type: "materializedView",
          columns: [{ name: "id", type: "int" }],
        }),
        expect.objectContaining({
          database: "app_db",
          schema: "public",
          object: "users_id_seq",
          type: "sequence",
          columns: [],
        }),
        expect.objectContaining({
          database: "app_db",
          schema: "public",
          object: "user_status",
          type: "type",
          columns: [],
        }),
      ]),
    );
    expect(schema).toHaveLength(7);

    expect(driverInstances[0]?.describeTableCalls).toEqual([
      "app_db.public.users",
      "app_db.public.active_users",
      "app_db.public.daily_users",
      "app_db.audit.event_feed",
    ]);
  });

  it("filters schema objects by driver entity manifest kinds", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    driverBehaviors.set("conn-manifest", {
      entityManifest: {
        dbObjectKinds: ["table", "view", "function", "procedure"],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
      },
      listDatabases: [{ name: "app_db", schemas: [] }],
      listSchemasByDatabase: {
        app_db: [{ name: "public" }],
      },
      listObjectsByScope: {
        "app_db.public": [
          { schema: "public", name: "users", type: "table" },
          { schema: "public", name: "active_users", type: "view" },
          {
            schema: "public",
            name: "daily_users",
            type: "materializedView",
          },
          { schema: "public", name: "users_id_seq", type: "sequence" },
          { schema: "public", name: "user_status", type: "type" },
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
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-manifest",
        name: "Manifested",
        type: "mysql",
        database: "app_db",
        host: "localhost",
        username: "root",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-manifest");

    const schema = await manager.getSchemaAsync("conn-manifest");
    expect(schema.map((entry) => entry.type)).toEqual(["table", "view"]);
    expect(schema.find((entry) => entry.object === "daily_users")).toBeFalsy();
    expect(schema.find((entry) => entry.object === "users_id_seq")).toBeFalsy();
    expect(schema.find((entry) => entry.object === "user_status")).toBeFalsy();
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
        host: "localhost",
        username: "root",
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
        host: "localhost",
        username: "postgres",
      },
      {
        id: "conn-2",
        name: "Audit",
        type: "mysql",
        database: "audit_db",
        host: "localhost",
        username: "root",
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
        host: "localhost",
        username: "postgres",
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
        host: "localhost",
        username: "postgres",
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
        host: "localhost",
        username: "postgres",
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
        host: "localhost",
        username: "postgres",
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
        host: "localhost",
        username: "postgres",
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
        host: "localhost",
        username: "postgres",
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

  it("reuses loaded table detail metadata after schema collapse and re-expand", async () => {
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
            isPrimaryKey: true,
            primaryKeyOrdinal: 1,
            isForeignKey: false,
          },
        ],
      },
      getConstraintsByScope: {
        "app_db.public.users": [
          {
            name: "pk_users",
            kind: "primary_key",
            columns: ["id"],
            source: "catalog",
          },
        ],
      },
      getTriggersByScope: {
        "app_db.public.users": [
          {
            name: "users_audit_trigger",
            timing: "after",
            events: ["insert"],
            orientation: "row",
            enabled: true,
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
        host: "localhost",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");
    await manager.getSchemaSnapshotAsync("conn-1");

    const request = {
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      table: "users",
      objectKind: "table",
    } as const;

    manager.ensureTableDetailLoading(request);
    await waitForTableDetailCondition(
      manager,
      "conn-1",
      () => manager.getTableDetailState(request).status === "loaded",
    );

    expect(
      manager.getTableDetailState(request).snapshot.triggers.items,
    ).toEqual([expect.objectContaining({ name: "users_audit_trigger" })]);

    manager.markSchemaScopeCollapsed("conn-1", {
      kind: "schema",
      database: "app_db",
      schema: "public",
    });
    manager.ensureSchemaScopeLoading("conn-1", {
      kind: "schema",
      database: "app_db",
      schema: "public",
    });
    manager.ensureTableDetailLoading(request);

    expect(driverInstances[0]?.describeColumnsCalls).toEqual([
      "app_db.public.users",
    ]);
    expect(driverInstances[0]?.getConstraintsCalls).toEqual([
      "app_db.public.users",
    ]);
    expect(driverInstances[0]?.getTriggersCalls).toEqual([
      "app_db.public.users",
    ]);
  });

  it("invalidates table detail metadata on manual refresh and reloads it on demand", async () => {
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
            isPrimaryKey: true,
            primaryKeyOrdinal: 1,
            isForeignKey: false,
          },
        ],
      },
      getTriggersByScope: {
        "app_db.public.users": [
          {
            name: "users_trigger_v1",
            timing: "after",
            events: ["insert"],
            orientation: "row",
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
        host: "localhost",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");
    await manager.getSchemaSnapshotAsync("conn-1");

    const request = {
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      table: "users",
      objectKind: "table",
    } as const;

    manager.ensureTableDetailLoading(request);
    await waitForTableDetailCondition(
      manager,
      "conn-1",
      () => manager.getTableDetailState(request).status === "loaded",
    );

    expect(
      manager.getTableDetailState(request).snapshot.triggers.items,
    ).toEqual([expect.objectContaining({ name: "users_trigger_v1" })]);

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
            isPrimaryKey: true,
            primaryKeyOrdinal: 1,
            isForeignKey: false,
          },
        ],
      },
      getTriggersByScope: {
        "app_db.public.users": [
          {
            name: "users_trigger_v2",
            timing: "after",
            events: ["update"],
            orientation: "row",
          },
        ],
      },
    });

    manager.refreshSchemaCache({
      connectionId: "conn-1",
      reason: "manual",
    });
    await manager.getSchemaSnapshotAsync("conn-1");

    manager.ensureTableDetailLoading(request);
    await waitForTableDetailCondition(
      manager,
      "conn-1",
      () => manager.getTableDetailState(request).status === "loaded",
    );

    expect(
      manager.getTableDetailState(request).snapshot.triggers.items,
    ).toEqual([expect.objectContaining({ name: "users_trigger_v2" })]);
    expect(driverInstances[0]?.getTriggersCalls).toEqual([
      "app_db.public.users",
      "app_db.public.users",
    ]);
  });

  it("skips non-applicable table detail loaders per manifest", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    driverBehaviors.set("conn-sections", {
      entityManifest: {
        dbObjectKinds: ["table"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "supported",
          triggers: "not_applicable",
        },
      },
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
            isPrimaryKey: true,
            primaryKeyOrdinal: 1,
            isForeignKey: false,
          },
        ],
      },
    });

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-sections",
        name: "Sectioned",
        type: "redis",
        database: "app_db",
        host: "localhost",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-sections");
    await manager.getSchemaSnapshotAsync("conn-sections");

    const request = {
      connectionId: "conn-sections",
      database: "app_db",
      schema: "public",
      table: "users",
      objectKind: "table",
    } as const;

    manager.ensureTableDetailLoading(request);
    await waitForTableDetailCondition(
      manager,
      "conn-sections",
      () => manager.getTableDetailState(request).status === "loaded",
    );

    const state = manager.getTableDetailState(request);
    expect(state.snapshot.columns.status).toBe("loaded");
    expect(state.snapshot.constraints.items).toEqual([]);
    expect(state.snapshot.triggers.items).toEqual([]);

    expect(driverInstances[0]?.describeColumnsCalls).toEqual([
      "app_db.public.users",
    ]);
    expect(driverInstances[0]?.getConstraintsCalls).toEqual([]);
    expect(driverInstances[0]?.getTriggersCalls).toEqual([]);
  });

  it("separates table detail cache entries by object kind and applies object-specific sections", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    driverBehaviors.set("conn-object-kind-details", {
      entityManifest: {
        dbObjectKinds: ["table", "view", "materializedView"],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
        tableSectionOverridesByObjectKind: {
          view: {
            constraints: "not_applicable",
            indexes: "not_applicable",
          },
          materializedView: {
            constraints: "not_applicable",
            triggers: "not_applicable",
          },
        },
      },
      listDatabases: [{ name: "app_db", schemas: [] }],
      listSchemasByDatabase: {
        app_db: [{ name: "public" }],
      },
      listObjectsByScope: {
        "app_db.public": [
          { schema: "public", name: "users", type: "table" },
          { schema: "public", name: "users", type: "view" },
          { schema: "public", name: "users_mv", type: "materializedView" },
        ],
      },
      describeTableByScope: {
        "app_db.public.users": [
          {
            name: "id",
            type: "int",
            nullable: false,
            isPrimaryKey: true,
            primaryKeyOrdinal: 1,
            isForeignKey: false,
          },
        ],
        "app_db.public.users_mv": [
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
        id: "conn-object-kind-details",
        name: "Object detail policies",
        type: "pg",
        host: "localhost",
        database: "app_db",
        username: "postgres",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-object-kind-details");
    await manager.getSchemaSnapshotAsync("conn-object-kind-details");

    const tableRequest = {
      connectionId: "conn-object-kind-details",
      database: "app_db",
      schema: "public",
      table: "users",
      objectKind: "table",
    } as const;
    const viewRequest = {
      connectionId: "conn-object-kind-details",
      database: "app_db",
      schema: "public",
      table: "users",
      objectKind: "view",
    } as const;
    const materializedViewRequest = {
      connectionId: "conn-object-kind-details",
      database: "app_db",
      schema: "public",
      table: "users_mv",
      objectKind: "materializedView",
    } as const;

    manager.ensureTableDetailLoading(tableRequest);
    await waitForTableDetailCondition(
      manager,
      "conn-object-kind-details",
      () => manager.getTableDetailState(tableRequest).status === "loaded",
    );

    manager.ensureTableDetailLoading(viewRequest);
    await waitForTableDetailCondition(
      manager,
      "conn-object-kind-details",
      () => manager.getTableDetailState(viewRequest).status === "loaded",
    );

    manager.ensureTableDetailLoading(materializedViewRequest);
    await waitForTableDetailCondition(
      manager,
      "conn-object-kind-details",
      () =>
        manager.getTableDetailState(materializedViewRequest).status ===
        "loaded",
    );

    expect(driverInstances[0]?.describeColumnsCalls).toEqual([
      "app_db.public.users",
      "app_db.public.users",
      "app_db.public.users_mv",
    ]);
    expect(driverInstances[0]?.getConstraintsCalls).toEqual([
      "app_db.public.users",
    ]);
    expect(driverInstances[0]?.getIndexesCalls).toEqual([
      "app_db.public.users",
      "app_db.public.users_mv",
    ]);
    expect(driverInstances[0]?.getTriggersCalls).toEqual([
      "app_db.public.users",
      "app_db.public.users",
    ]);
  });
});
