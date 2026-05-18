import { randomUUID } from "node:crypto";
import * as vscode from "vscode";
import { isDataDbObjectKind } from "../shared/dbObjectKinds";
import {
  type BookmarkEntry,
  type ConnectAttempt,
  type ConnectionConfig,
  type ExplorerSchemaScope,
  type HistoryEntry,
  type RefreshSchemaRequest,
  type SchemaLoadStatus,
  type SchemaObjectEntry,
  type SchemaScopeKey,
  type SchemaSnapshot,
  type SchemaSnapshotDatabaseEntry,
  type SchemaSnapshotObjectEntry,
  type SchemaSnapshotSchemaEntry,
  type SchemaSnapshotState,
  type ScopeAwareConnectionManagerApi,
  type ScopedSchemaCacheEntry,
  type ScopedSchemaFragment,
  type StoredConnectionConfig,
  type TableDetailRequest,
  type TableDetailState,
  type TableStructureSnapshot,
  type TestConnectionResult,
} from "./connectionManagerModels";
import {
  type ConnectionManagerStore,
  VSCodeConnectionManagerStore,
} from "./connectionManagerStore";
import { DynamoDBDriver } from "./dbDrivers/dynamodb";
import { ElasticsearchDriver } from "./dbDrivers/elasticsearch";
import { MongoDBDriver } from "./dbDrivers/mongodb";
import { MSSQLDriver } from "./dbDrivers/mssql";
import { MySQLDriver } from "./dbDrivers/mysql";
import { OracleDriver } from "./dbDrivers/oracle";
import { PostgresDriver } from "./dbDrivers/postgres";
import { RedisDriver } from "./dbDrivers/redis";
import { SQLiteDriver } from "./dbDrivers/sqlite";
import { createTimeoutAwareDriver } from "./dbDrivers/timeout";
import {
  DEFAULT_DRIVER_ENTITY_MANIFEST,
  type DriverCapabilities,
  type DriverEntityAvailability,
  type DriverEntityManifest,
  type IDBDriver,
} from "./dbDrivers/types";
import { pMapWithLimit } from "./utils/concurrency";
import { normalizeUnknownError } from "./utils/errorHandling";

export type {
  BookmarkEntry,
  ConnectAttempt,
  ConnectionConfig,
  ExplorerSchemaScope,
  HistoryEntry,
  RefreshSchemaRequest,
  SchemaLoadStatus,
  SchemaObjectEntry,
  SchemaScopeKey,
  SchemaSnapshot,
  SchemaSnapshotDatabaseEntry,
  SchemaSnapshotSchemaEntry,
  SchemaSnapshotState,
  ScopeAwareConnectionManagerApi,
  ScopedSchemaCacheEntry,
  TableDetailRequest,
  TableDetailState,
  TableStructureSnapshot,
  TestConnectionResult,
} from "./connectionManagerModels";

interface InternalScopedSchemaCacheEntry extends ScopedSchemaCacheEntry {
  loading: Promise<void> | null;
  retainOnCollapse: boolean;
  fullyLoaded: boolean;
  tableDetails: Map<string, InternalTableDetailCacheEntry>;
}

interface InternalTableDetailCacheEntry extends TableDetailState {
  generation: number;
  loading: Promise<void> | null;
}

interface ConnectionSchemaCacheEntry extends SchemaSnapshotState {
  loading: Promise<void> | null;
  generation: number;
  defaultDatabaseName: string;
  scopes: Map<SchemaScopeKey, InternalScopedSchemaCacheEntry>;
  expandedScopeKeys: Set<SchemaScopeKey>;
}

interface DatabaseScopeLoadResult {
  database: SchemaSnapshotDatabaseEntry;
  loadedSchemas: SchemaSnapshotSchemaEntry[];
}

type DatabaseLoadMode = "baseline" | "expanded";

const CONNECTION_ROOT_SCOPE: ExplorerSchemaScope = { kind: "connectionRoot" };

export function createConnectionRootSchemaScope(): ExplorerSchemaScope {
  return { kind: "connectionRoot" };
}

export function getExplorerSchemaScopeKey(
  scope: ExplorerSchemaScope,
): SchemaScopeKey {
  switch (scope.kind) {
    case "connectionRoot":
      return "connectionRoot";
    case "database":
      return `database:${encodeURIComponent(scope.database)}`;
    case "schema":
      return `schema:${encodeURIComponent(scope.database)}:${encodeURIComponent(scope.schema)}`;
  }
}

function parseExplorerSchemaScopeKey(
  key: SchemaScopeKey,
): ExplorerSchemaScope | undefined {
  if (key === "connectionRoot") {
    return createConnectionRootSchemaScope();
  }

  if (key.startsWith("database:")) {
    return {
      kind: "database",
      database: decodeURIComponent(key.slice("database:".length)),
    };
  }

  if (key.startsWith("schema:")) {
    const encodedParts = key.slice("schema:".length).split(":");
    if (encodedParts.length !== 2) {
      return undefined;
    }

    return {
      kind: "schema",
      database: decodeURIComponent(encodedParts[0]),
      schema: decodeURIComponent(encodedParts[1]),
    };
  }

  return undefined;
}

function isConnectionRootScope(scope?: ExplorerSchemaScope): boolean {
  return !scope || scope.kind === "connectionRoot";
}

function resolveDriverEntityManifest(
  driver: IDBDriver | undefined,
): DriverEntityManifest {
  return driver?.getEntityManifest?.() ?? DEFAULT_DRIVER_ENTITY_MANIFEST;
}

function resolveDriverCapabilities(
  driver: IDBDriver | undefined,
): DriverCapabilities | undefined {
  return driver?.getCapabilities?.();
}

function createEmptySchemaSnapshot(): SchemaSnapshot {
  return { databases: [] };
}

function createSchemaSnapshotState(
  snapshot: SchemaSnapshot,
  status: SchemaLoadStatus,
  isPartial: boolean,
  error?: string,
): SchemaSnapshotState {
  if (error) {
    return {
      snapshot,
      status,
      isPartial,
      error,
    };
  }

  return {
    snapshot,
    status,
    isPartial,
  };
}

function createEmptySchemaSnapshotState(): SchemaSnapshotState {
  return createSchemaSnapshotState(createEmptySchemaSnapshot(), "idle", false);
}

function createEmptyTableStructureSnapshot(): TableStructureSnapshot {
  return {
    columns: {
      status: "idle",
      items: [],
    },
    constraints: {
      status: "idle",
      items: [],
    },
    indexes: {
      status: "idle",
      items: [],
    },
    triggers: {
      status: "idle",
      items: [],
    },
  };
}

function createEmptyTableDetailState(
  request: TableDetailRequest,
): TableDetailState {
  return {
    request: { ...request },
    snapshot: createEmptyTableStructureSnapshot(),
    status: "idle",
    isPartial: false,
  };
}

function cloneTableStructureSnapshot(
  snapshot: TableStructureSnapshot,
): TableStructureSnapshot {
  return {
    columns: {
      status: snapshot.columns.status,
      items: snapshot.columns.items.map((column) => ({ ...column })),
      error: snapshot.columns.error,
    },
    constraints: {
      status: snapshot.constraints.status,
      items: snapshot.constraints.items.map((constraint) => ({
        ...constraint,
        columns: [...constraint.columns],
        referencedColumns: constraint.referencedColumns
          ? [...constraint.referencedColumns]
          : undefined,
      })),
      error: snapshot.constraints.error,
    },
    indexes: {
      status: snapshot.indexes.status,
      items: snapshot.indexes.items.map((index) => ({
        ...index,
        columns: [...index.columns],
      })),
      error: snapshot.indexes.error,
    },
    triggers: {
      status: snapshot.triggers.status,
      items: snapshot.triggers.items.map((trigger) => ({
        ...trigger,
        events: [...trigger.events],
      })),
      error: snapshot.triggers.error,
    },
  };
}

function cloneTableDetailState(state: TableDetailState): TableDetailState {
  return {
    request: { ...state.request },
    snapshot: cloneTableStructureSnapshot(state.snapshot),
    status: state.status,
    isPartial: state.isPartial,
    error: state.error,
  };
}

function createInternalTableDetailCacheEntry(
  request: TableDetailRequest,
  generation: number,
): InternalTableDetailCacheEntry {
  return {
    ...createEmptyTableDetailState(request),
    generation,
    loading: null,
  };
}

function getTableDetailCacheKey(
  request: Omit<TableDetailRequest, "connectionId">,
): string {
  return [request.database, request.schema, request.table]
    .map((part) => encodeURIComponent(part))
    .join(":");
}

function cloneSchemaSnapshotObjectEntry(
  object: SchemaSnapshotObjectEntry,
): SchemaSnapshotObjectEntry {
  return {
    name: object.name,
    type: object.type,
    columns: object.columns.map((column) => ({
      name: column.name,
      type: column.type,
    })),
  };
}

function cloneSchemaSnapshotSchemaEntry(
  schema: SchemaSnapshotSchemaEntry,
): SchemaSnapshotSchemaEntry {
  return {
    name: schema.name,
    objects: schema.objects.map(cloneSchemaSnapshotObjectEntry),
  };
}

function cloneSchemaSnapshotDatabaseEntry(
  database: SchemaSnapshotDatabaseEntry,
): SchemaSnapshotDatabaseEntry {
  return {
    name: database.name,
    schemas: database.schemas.map(cloneSchemaSnapshotSchemaEntry),
  };
}

function cloneSchemaSnapshot(snapshot: SchemaSnapshot): SchemaSnapshot {
  return {
    databases: snapshot.databases.map(cloneSchemaSnapshotDatabaseEntry),
  };
}

function cloneExplorerSchemaScope(
  scope: ExplorerSchemaScope,
): ExplorerSchemaScope {
  switch (scope.kind) {
    case "connectionRoot":
      return createConnectionRootSchemaScope();
    case "database":
      return { kind: "database", database: scope.database };
    case "schema":
      return {
        kind: "schema",
        database: scope.database,
        schema: scope.schema,
      };
  }
}

function createScopeSnapshotForDatabase(
  database: SchemaSnapshotDatabaseEntry,
): SchemaSnapshot {
  return {
    databases: [database],
  };
}

function createScopeSnapshotForSchema(
  databaseName: string,
  schema: SchemaSnapshotSchemaEntry,
): SchemaSnapshot {
  return {
    databases: [
      {
        name: databaseName,
        schemas: [schema],
      },
    ],
  };
}

function createScopedSchemaCacheEntry(
  scope: ExplorerSchemaScope,
  generation: number,
  state: SchemaSnapshotState,
  fragment: ScopedSchemaFragment = {},
  retainOnCollapse = false,
): InternalScopedSchemaCacheEntry {
  return {
    ...createSchemaSnapshotState(
      cloneSchemaSnapshot(state.snapshot),
      state.status,
      state.isPartial,
      state.error,
    ),
    scope: cloneExplorerSchemaScope(scope),
    key: getExplorerSchemaScopeKey(scope),
    fragment,
    generation,
    loading: null,
    retainOnCollapse,
    fullyLoaded: false,
    tableDetails: new Map<string, InternalTableDetailCacheEntry>(),
  };
}

function createConnectionSchemaCacheEntry(
  generation: number,
  expandedScopeKeys: Set<SchemaScopeKey>,
  defaultDatabaseName: string,
): ConnectionSchemaCacheEntry {
  const state = createEmptySchemaSnapshotState();
  const rootScope = createConnectionRootSchemaScope();
  const scopes = new Map<SchemaScopeKey, InternalScopedSchemaCacheEntry>([
    [
      getExplorerSchemaScopeKey(rootScope),
      createScopedSchemaCacheEntry(rootScope, generation, state, {}, true),
    ],
  ]);

  return {
    ...state,
    loading: null,
    generation,
    defaultDatabaseName,
    scopes,
    expandedScopeKeys,
  };
}

function cloneSchemaSnapshotState(
  state: SchemaSnapshotState,
): SchemaSnapshotState {
  return createSchemaSnapshotState(
    cloneSchemaSnapshot(state.snapshot),
    state.status,
    state.isPartial,
    state.error,
  );
}

function getConfiguredDefaultDatabaseName(config: ConnectionConfig): string {
  return (
    config.database || config.serviceName || (config.filePath ? "main" : "")
  );
}

function isDescendantScope(
  scope: ExplorerSchemaScope,
  ancestor: ExplorerSchemaScope,
): boolean {
  switch (ancestor.kind) {
    case "connectionRoot":
      return scope.kind !== "connectionRoot";
    case "database":
      return scope.kind === "schema" && scope.database === ancestor.database;
    case "schema":
      return false;
  }
}

function mergeSchemaIntoDatabase(
  database: SchemaSnapshotDatabaseEntry,
  schema: SchemaSnapshotSchemaEntry,
): SchemaSnapshotDatabaseEntry {
  const nextDatabase = cloneSchemaSnapshotDatabaseEntry(database);
  const schemaIndex = nextDatabase.schemas.findIndex(
    (entry) => entry.name === schema.name,
  );
  const nextSchema = cloneSchemaSnapshotSchemaEntry(schema);

  if (schemaIndex >= 0) {
    nextDatabase.schemas[schemaIndex] = nextSchema;
    return nextDatabase;
  }

  nextDatabase.schemas.push(nextSchema);
  nextDatabase.schemas.sort((left, right) =>
    left.name.localeCompare(right.name),
  );
  return nextDatabase;
}

function buildAggregateSchemaSnapshot(
  entry: ConnectionSchemaCacheEntry,
): SchemaSnapshot {
  const rootEntry = entry.scopes.get(
    getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE),
  ) as InternalScopedSchemaCacheEntry | undefined;
  const orderedDatabaseNames: string[] = [];
  const databaseMap = new Map<string, SchemaSnapshotDatabaseEntry>();

  const ensureDatabaseSlot = (databaseName: string): void => {
    if (!orderedDatabaseNames.includes(databaseName)) {
      orderedDatabaseNames.push(databaseName);
    }
    if (!databaseMap.has(databaseName)) {
      databaseMap.set(databaseName, {
        name: databaseName,
        schemas: [],
      });
    }
  };

  for (const database of rootEntry?.snapshot.databases ?? []) {
    ensureDatabaseSlot(database.name);
  }

  for (const scopedEntry of entry.scopes.values()) {
    if (
      scopedEntry.scope.kind !== "database" ||
      !scopedEntry.fragment.database
    ) {
      continue;
    }

    ensureDatabaseSlot(scopedEntry.fragment.database.name);
    databaseMap.set(
      scopedEntry.fragment.database.name,
      cloneSchemaSnapshotDatabaseEntry(scopedEntry.fragment.database),
    );
  }

  for (const scopedEntry of entry.scopes.values()) {
    if (scopedEntry.scope.kind !== "schema" || !scopedEntry.fragment.schema) {
      continue;
    }

    ensureDatabaseSlot(scopedEntry.scope.database);
    const currentDatabase = databaseMap.get(scopedEntry.scope.database) ?? {
      name: scopedEntry.scope.database,
      schemas: [],
    };
    databaseMap.set(
      scopedEntry.scope.database,
      mergeSchemaIntoDatabase(currentDatabase, scopedEntry.fragment.schema),
    );
  }

  return {
    databases: orderedDatabaseNames
      .map((databaseName) => databaseMap.get(databaseName))
      .filter(
        (database): database is SchemaSnapshotDatabaseEntry =>
          database !== undefined,
      ),
  };
}

function deriveAggregateSchemaState(
  entry: ConnectionSchemaCacheEntry,
  snapshot: SchemaSnapshot,
): Omit<SchemaSnapshotState, "snapshot"> {
  const rootEntry = entry.scopes.get(
    getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE),
  ) as InternalScopedSchemaCacheEntry | undefined;
  const baselineEntry = entry.defaultDatabaseName
    ? (entry.scopes.get(
        getExplorerSchemaScopeKey({
          kind: "database",
          database: entry.defaultDatabaseName,
        }),
      ) as InternalScopedSchemaCacheEntry | undefined)
    : undefined;

  if (!rootEntry || rootEntry.status === "idle") {
    return {
      status: "idle",
      isPartial: false,
    };
  }

  if (rootEntry.status === "error") {
    const state = {
      status: "error",
      isPartial: snapshot.databases.length > 0,
    } satisfies Omit<SchemaSnapshotState, "snapshot">;
    return rootEntry.error ? { ...state, error: rootEntry.error } : state;
  }

  if (rootEntry.status === "loading") {
    return {
      status: "loading",
      isPartial: snapshot.databases.length > 0,
    };
  }

  if (entry.defaultDatabaseName) {
    if (!baselineEntry || baselineEntry.status === "idle") {
      return {
        status: "loading",
        isPartial: snapshot.databases.length > 0,
      };
    }

    if (baselineEntry.status === "loading") {
      return {
        status: "loading",
        isPartial: snapshot.databases.length > 0,
      };
    }

    if (baselineEntry.status === "error") {
      const state = {
        status: "error",
        isPartial: snapshot.databases.length > 0,
      } satisfies Omit<SchemaSnapshotState, "snapshot">;
      return baselineEntry.error
        ? { ...state, error: baselineEntry.error }
        : state;
    }
  }

  return {
    status: "loaded",
    isPartial: false,
  };
}

function flattenSchemaSnapshot(snapshot: SchemaSnapshot): SchemaObjectEntry[] {
  return snapshot.databases.flatMap((database) =>
    database.schemas.flatMap((schema) =>
      schema.objects.map<SchemaObjectEntry>((object) => ({
        database: database.name,
        schema: schema.name,
        object: object.name,
        type: object.type,
        columns: object.columns,
      })),
    ),
  );
}

const TEST_CONNECTION_ID = "__test__";
export class ConnectionManager implements ScopeAwareConnectionManagerApi {
  private readonly store: ConnectionManagerStore;
  private driverMap = new Map<string, IDBDriver>();
  private readonly _connectingMap = new Map<string, Promise<void>>();
  readonly onDidChangeConnections: vscode.Event<void>;
  private readonly _onDidChangeConnections = new vscode.EventEmitter<void>();
  readonly onDidChangeHistory: vscode.Event<void>;
  private readonly _onDidChangeHistory = new vscode.EventEmitter<void>();
  readonly onDidChangeBookmarks: vscode.Event<void>;
  private readonly _onDidChangeBookmarks = new vscode.EventEmitter<void>();
  readonly onDidDisconnect: vscode.Event<string>;
  private readonly _onDidDisconnect = new vscode.EventEmitter<string>();
  readonly onDidConnect: vscode.Event<void>;
  private readonly _onDidConnect = new vscode.EventEmitter<void>();
  readonly onDidSchemaLoad: vscode.Event<string>;
  private readonly _onDidSchemaLoad = new vscode.EventEmitter<string>();
  readonly onDidChangeSchemaState: vscode.Event<string>;
  private readonly _onDidChangeSchemaState = new vscode.EventEmitter<string>();
  readonly onDidRefreshSchemas: vscode.Event<void>;
  private readonly _onDidRefreshSchemas = new vscode.EventEmitter<void>();
  private _connectionsCache: ConnectionConfig[] | null = null;
  private readonly _schemaCacheMap = new Map<
    string,
    ConnectionSchemaCacheEntry
  >();
  private readonly _schemaGenerationMap = new Map<string, number>();
  private readonly _schemaExpandedScopeKeyMap = new Map<
    string,
    Set<SchemaScopeKey>
  >();
  constructor(
    context: vscode.ExtensionContext,
    store: ConnectionManagerStore = new VSCodeConnectionManagerStore(context),
  ) {
    this.store = store;
    this.onDidChangeConnections = this._onDidChangeConnections.event;
    this.onDidChangeHistory = this._onDidChangeHistory.event;
    this.onDidChangeBookmarks = this._onDidChangeBookmarks.event;
    this.onDidConnect = this._onDidConnect.event;
    this.onDidDisconnect = this._onDidDisconnect.event;
    this.onDidSchemaLoad = this._onDidSchemaLoad.event;
    this.onDidChangeSchemaState = this._onDidChangeSchemaState.event;
    this.onDidRefreshSchemas = this._onDidRefreshSchemas.event;
    this.store.onDidChangeConfiguration(async (e) => {
      this._connectionsCache = null;
      if (e.affectsConfiguration("rapidb.queryHistoryLimit")) {
        await this._trimHistoryToLimit();
      }
    }, context.subscriptions);
  }
  private getHistoryLimit(): number {
    return this.store.getHistoryLimit();
  }
  getDefaultPageSize(): number {
    return this.store.getDefaultPageSize();
  }
  getQueryRowLimit(): number {
    return this.store.getQueryRowLimit();
  }
  private async _trimHistoryToLimit(): Promise<void> {
    const limit = this.getHistoryLimit();
    const all = this.store.readHistory();
    if (all.length > limit) {
      await this.store.writeHistory(all.slice(0, limit));
      this._onDidChangeHistory.fire();
    }
  }
  getConnections(): ConnectionConfig[] {
    if (this._connectionsCache) {
      return this._connectionsCache;
    }
    this._connectionsCache = this.store.getConnections().map((c) => ({
      ...c,
      id: c.id ?? randomUUID(),
      username: c.username ?? c.user,
    }));
    return this._connectionsCache;
  }
  private async saveConnections(conns: ConnectionConfig[]): Promise<void> {
    this._connectionsCache = null;
    await this.store.saveConnections(conns);
  }
  getConnection(id: string): ConnectionConfig | undefined {
    return this.getConnections().find((c) => c.id === id);
  }
  async saveConnection(config: ConnectionConfig): Promise<void> {
    const conns = this.getConnections();
    const idx = conns.findIndex((c) => c.id === config.id);
    const isEdit = idx >= 0;
    if (isEdit) {
      conns[idx] = config;
    } else {
      conns.push({ ...config, id: config.id || randomUUID() });
    }
    await this.saveConnections(conns);
    if (isEdit && this.isConnected(config.id)) {
      await this.disconnectFrom(config.id);
    }
    this._onDidChangeConnections.fire();
  }
  async removeConnection(id: string): Promise<boolean> {
    if (!this.getConnection(id)) {
      return false;
    }
    if (this.driverMap.has(id)) {
      await this.disconnectFrom(id);
    }
    await this.saveConnections(
      this.getConnections().filter((c) => c.id !== id),
    );
    try {
      await this.store.deleteSecret(id);
    } catch {}
    await this._purgeHistoryForConnection(id);
    await this._purgeBookmarksForConnection(id);
    this._onDidChangeConnections.fire();
    return true;
  }

  private parseStoredSecrets(value: string | undefined): {
    password?: string;
    awsAccessKeyId?: string;
    awsSecretAccessKey?: string;
    awsSessionToken?: string;
  } {
    if (!value) {
      return {};
    }

    try {
      const parsed = JSON.parse(value) as Record<string, unknown>;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return {
          password:
            typeof parsed.password === "string" ? parsed.password : undefined,
          awsAccessKeyId:
            typeof parsed.awsAccessKeyId === "string"
              ? parsed.awsAccessKeyId
              : undefined,
          awsSecretAccessKey:
            typeof parsed.awsSecretAccessKey === "string"
              ? parsed.awsSecretAccessKey
              : undefined,
          awsSessionToken:
            typeof parsed.awsSessionToken === "string"
              ? parsed.awsSessionToken
              : undefined,
        };
      }
    } catch {}

    return { password: value };
  }

  async _hydratePassword(config: ConnectionConfig): Promise<ConnectionConfig> {
    if (!config.useSecretStorage) {
      return config;
    }
    try {
      const stored = await this.store.getSecret(config.id);
      const secrets = this.parseStoredSecrets(stored);
      return {
        ...config,
        password: secrets.password ?? config.password ?? "",
        awsAccessKeyId: secrets.awsAccessKeyId ?? config.awsAccessKeyId,
        awsSecretAccessKey:
          secrets.awsSecretAccessKey ?? config.awsSecretAccessKey,
        awsSessionToken: secrets.awsSessionToken ?? config.awsSessionToken,
      };
    } catch {
      return { ...config, password: "" };
    }
  }
  private async _purgeEntriesForConnection<
    T extends {
      connectionId: string;
    },
  >(
    connectionId: string,
    read: () => T[],
    write: (entries: T[]) => Promise<void>,
    fire: () => void,
  ): Promise<void> {
    const all = read();
    const filtered = all.filter((e) => e.connectionId !== connectionId);
    if (filtered.length !== all.length) {
      await write(filtered);
      fire();
    }
  }
  private async _purgeHistoryForConnection(
    connectionId: string,
  ): Promise<void> {
    await this._purgeEntriesForConnection(
      connectionId,
      () => this.store.readHistory(),
      (entries) => this.store.writeHistory(entries),
      () => this._onDidChangeHistory.fire(),
    );
  }
  private createDriver(config: ConnectionConfig): IDBDriver {
    const timeoutSettingsProvider = () => this.store.getTimeoutSettings();

    const driver = (() => {
      switch (config.type) {
        case "mysql":
          return new MySQLDriver(config, timeoutSettingsProvider);
        case "pg":
          return new PostgresDriver(config, timeoutSettingsProvider);
        case "sqlite":
          return new SQLiteDriver(config, timeoutSettingsProvider);
        case "mssql":
          return new MSSQLDriver(config, timeoutSettingsProvider);
        case "oracle":
          return new OracleDriver(config, timeoutSettingsProvider);
        case "mongodb":
          return new MongoDBDriver(config);
        case "redis":
          return new RedisDriver(config);
        case "elasticsearch":
          return new ElasticsearchDriver(config);
        case "dynamodb":
          return new DynamoDBDriver(config);
        default: {
          const unknownType: never = config.type;
          throw new Error(`[RapiDB] Unknown driver type: ${unknownType}`);
        }
      }
    })();

    return createTimeoutAwareDriver(driver, timeoutSettingsProvider);
  }
  beginConnect(id: string): ConnectAttempt {
    const pending = this._connectingMap.get(id);
    if (pending) {
      return { promise: pending, isNew: false };
    }
    if (this.isConnected(id)) {
      return { promise: Promise.resolve(), isNew: false };
    }
    let resolveAttempt!: () => void;
    let rejectAttempt!: (err: unknown) => void;
    const attempt = new Promise<void>((resolve, reject) => {
      resolveAttempt = resolve;
      rejectAttempt = reject;
    });
    this._connectingMap.set(id, attempt);
    this._onDidChangeConnections.fire();
    void (async () => {
      try {
        if (this.driverMap.has(id)) {
          await this.disconnectFrom(id);
        }
        const config = this.getConnection(id);
        if (!config) {
          throw new Error(`[RapiDB] Connection "${id}" not found`);
        }
        const fullConfig = await this._hydratePassword(config);
        const driver = this.createDriver(fullConfig);
        try {
          await driver.connect();
        } catch (err) {
          try {
            await driver.disconnect();
          } catch {}
          throw err;
        }
        this.driverMap.set(id, driver);
        this._invalidateSchemaState(id);
        this._onDidConnect.fire();
        resolveAttempt();
      } catch (err) {
        rejectAttempt(err);
      } finally {
        this._connectingMap.delete(id);
        this._onDidChangeConnections.fire();
      }
    })();
    return { promise: attempt, isNew: true };
  }
  async connectTo(id: string): Promise<void> {
    await this.beginConnect(id).promise;
  }
  async disconnectFrom(id: string): Promise<void> {
    const driver = this.driverMap.get(id);
    if (driver) {
      try {
        await driver.disconnect();
      } catch {}
      this.driverMap.delete(id);
      this._invalidateSchemaState(id);
      this._onDidDisconnect.fire(id);
    }
  }
  isConnected(id: string): boolean {
    return this.driverMap.get(id)?.isConnected() ?? false;
  }
  getConnectedCount(): number {
    return [...this.driverMap.values()].filter((driver) => driver.isConnected())
      .length;
  }
  isConnecting(id: string): boolean {
    return this._connectingMap.has(id);
  }
  getDriver(id: string): IDBDriver | undefined {
    return this.driverMap.get(id);
  }

  getDriverCapabilities(connectionId: string): DriverCapabilities | undefined {
    const connectedDriver = this.getDriver(connectionId);
    if (connectedDriver) {
      return resolveDriverCapabilities(connectedDriver);
    }

    const config = this.getConnection(connectionId);
    if (!config) {
      return undefined;
    }

    return resolveDriverCapabilities(this.createDriver(config));
  }

  getQueryEditorPresentation(connectionId: string) {
    return this.getDriverCapabilities(connectionId)?.editorPresentation;
  }

  getDriverEntityManifest(connectionId: string): DriverEntityManifest {
    return resolveDriverEntityManifest(this.getDriver(connectionId));
  }
  getSchema(connectionId: string): SchemaObjectEntry[] {
    return flattenSchemaSnapshot(this.getSchemaSnapshot(connectionId));
  }

  getSchemaSnapshot(connectionId: string): SchemaSnapshot {
    return this.getSchemaSnapshotState(connectionId).snapshot;
  }

  getSchemaSnapshotState(
    connectionId: string,
    scope?: ExplorerSchemaScope,
  ): SchemaSnapshotState {
    const state = this._schemaCacheMap.get(connectionId);
    if (!state) {
      return createEmptySchemaSnapshotState();
    }

    if (!scope) {
      return cloneSchemaSnapshotState(state);
    }

    if (isConnectionRootScope(scope)) {
      const rootState = state.scopes.get(
        getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE),
      );
      return rootState
        ? cloneSchemaSnapshotState(rootState)
        : createEmptySchemaSnapshotState();
    }

    const scopedState = state.scopes.get(getExplorerSchemaScopeKey(scope));
    if (scopedState) {
      return cloneSchemaSnapshotState(scopedState);
    }

    if (state.status === "loading") {
      return createSchemaSnapshotState(
        createEmptySchemaSnapshot(),
        "loading",
        false,
      );
    }

    if (state.status === "error") {
      return createSchemaSnapshotState(
        createEmptySchemaSnapshot(),
        "error",
        false,
        state.error,
      );
    }

    return createEmptySchemaSnapshotState();
  }

  getTableDetailState(request: TableDetailRequest): TableDetailState {
    const entry = this._schemaCacheMap.get(request.connectionId);
    if (!entry) {
      return createEmptyTableDetailState(request);
    }

    const schemaEntry = entry.scopes.get(
      getExplorerSchemaScopeKey({
        kind: "schema",
        database: request.database,
        schema: request.schema,
      }),
    );
    const tableDetail = schemaEntry?.tableDetails.get(
      getTableDetailCacheKey(request),
    );

    if (tableDetail) {
      return cloneTableDetailState(tableDetail);
    }

    if (schemaEntry?.status === "loading") {
      return {
        ...createEmptyTableDetailState(request),
        status: "loading",
      };
    }

    if (schemaEntry?.status === "error") {
      return {
        ...createEmptyTableDetailState(request),
        status: "error",
        error: schemaEntry.error,
      };
    }

    return createEmptyTableDetailState(request);
  }

  ensureSchemaSnapshotLoading(connectionId: string): void {
    this.ensureSchemaScopeLoading(
      connectionId,
      createConnectionRootSchemaScope(),
    );
  }

  ensureTableDetailLoading(request: TableDetailRequest): void {
    this.ensureSchemaScopeLoading(request.connectionId, {
      kind: "schema",
      database: request.database,
      schema: request.schema,
    });

    const driver = this.getDriver(request.connectionId);
    const config = this.getConnection(request.connectionId);
    if (!driver || !config) {
      return;
    }

    const entry = this._getOrCreateSchemaCacheEntry(
      request.connectionId,
      config,
    );
    const schemaEntry = this._getOrCreateScopeEntry(
      entry,
      {
        kind: "schema",
        database: request.database,
        schema: request.schema,
      },
      getConfiguredDefaultDatabaseName(config) === request.database,
    );
    const tableDetailKey = getTableDetailCacheKey(request);
    let tableDetailEntry = schemaEntry.tableDetails.get(tableDetailKey);

    if (!tableDetailEntry || tableDetailEntry.generation !== entry.generation) {
      tableDetailEntry = createInternalTableDetailCacheEntry(
        request,
        entry.generation,
      );
      schemaEntry.tableDetails.set(tableDetailKey, tableDetailEntry);
    }

    if (tableDetailEntry.loading || tableDetailEntry.status === "loaded") {
      return;
    }

    if (tableDetailEntry.status === "error") {
      return;
    }

    tableDetailEntry.status = "loading";
    tableDetailEntry.isPartial = false;
    delete tableDetailEntry.error;
    tableDetailEntry.snapshot = {
      columns: { status: "loading", items: [] },
      constraints: { status: "loading", items: [] },
      indexes: { status: "loading", items: [] },
      triggers: { status: "loading", items: [] },
    };
    this._onDidChangeSchemaState.fire(request.connectionId);

    let loadPromise: Promise<void> | null = null;
    loadPromise = (async () => {
      try {
        if (schemaEntry.loading) {
          try {
            await schemaEntry.loading;
          } catch {}
        }

        if (
          !this._isLiveTableDetailEntry(
            request.connectionId,
            entry,
            schemaEntry,
            tableDetailKey,
            tableDetailEntry,
          )
        ) {
          return;
        }

        if (schemaEntry.status === "error") {
          tableDetailEntry.status = "error";
          tableDetailEntry.error =
            schemaEntry.error ??
            `Failed to load schema ${request.database}.${request.schema}`;
          return;
        }

        const nextState = await this._loadTableDetailInternal(driver, request);
        if (
          !this._isLiveTableDetailEntry(
            request.connectionId,
            entry,
            schemaEntry,
            tableDetailKey,
            tableDetailEntry,
          )
        ) {
          return;
        }

        tableDetailEntry.snapshot = nextState.snapshot;
        tableDetailEntry.status = nextState.status;
        tableDetailEntry.isPartial = nextState.isPartial;
        tableDetailEntry.error = nextState.error;
      } catch (err: unknown) {
        if (
          !this._isLiveTableDetailEntry(
            request.connectionId,
            entry,
            schemaEntry,
            tableDetailKey,
            tableDetailEntry,
          )
        ) {
          return;
        }

        const error = normalizeUnknownError(err);
        tableDetailEntry.status = "error";
        tableDetailEntry.isPartial = false;
        tableDetailEntry.error = error.message;
      } finally {
        if (
          loadPromise &&
          this._isLiveTableDetailEntry(
            request.connectionId,
            entry,
            schemaEntry,
            tableDetailKey,
            tableDetailEntry,
          ) &&
          tableDetailEntry.loading === loadPromise
        ) {
          tableDetailEntry.loading = null;
        }
        this._onDidChangeSchemaState.fire(request.connectionId);
      }
    })();

    tableDetailEntry.loading = loadPromise;
  }

  ensureSchemaScopeLoading(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void {
    this.markSchemaScopeExpanded(connectionId, scope);

    const config = this.getConnection(connectionId);
    const configuredDefaultDatabase = config
      ? getConfiguredDefaultDatabaseName(config)
      : "";

    switch (scope.kind) {
      case "connectionRoot":
        this._startRootSchemaLoad(connectionId, false);
        return;
      case "database": {
        this._startRootSchemaLoad(connectionId, false);
        const loadMode: DatabaseLoadMode =
          configuredDefaultDatabase === scope.database
            ? "baseline"
            : "expanded";
        this._startDatabaseScopeLoad(
          connectionId,
          scope.database,
          false,
          loadMode === "baseline",
          loadMode,
        );
        return;
      }
      case "schema":
        this._startRootSchemaLoad(connectionId, false);
        this._startSchemaScopeLoad(
          connectionId,
          scope.database,
          scope.schema,
          false,
          configuredDefaultDatabase === scope.database,
        );
        return;
    }
  }

  markSchemaScopeExpanded(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void {
    const expandedScopeKeys = this._getExpandedScopeKeys(connectionId);
    expandedScopeKeys.add(getExplorerSchemaScopeKey(scope));

    const entry = this._schemaCacheMap.get(connectionId);
    if (entry) {
      entry.expandedScopeKeys = expandedScopeKeys;
    }
  }

  markSchemaScopeCollapsed(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void {
    const expandedScopeKeys = this._getExpandedScopeKeys(connectionId);

    for (const expandedScopeKey of [...expandedScopeKeys]) {
      if (
        expandedScopeKey === getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE)
      ) {
        continue;
      }

      const expandedScope = parseExplorerSchemaScopeKey(expandedScopeKey);
      if (!expandedScope) {
        expandedScopeKeys.delete(expandedScopeKey);
        continue;
      }

      if (
        expandedScopeKey === getExplorerSchemaScopeKey(scope) ||
        isDescendantScope(expandedScope, scope)
      ) {
        expandedScopeKeys.delete(expandedScopeKey);
      }
    }

    const entry = this._schemaCacheMap.get(connectionId);
    if (!entry) {
      return;
    }

    entry.expandedScopeKeys = expandedScopeKeys;
  }

  refreshSchemaCache(request?: string | RefreshSchemaRequest): void {
    const connectionId =
      typeof request === "string" ? request : request?.connectionId;

    const connectionIds = connectionId
      ? [connectionId]
      : [
          ...new Set([
            ...this.getConnections().map((connection) => connection.id),
            ...this._schemaCacheMap.keys(),
            ...this._schemaGenerationMap.keys(),
          ]),
        ];

    for (const nextConnectionId of connectionIds) {
      this._invalidateSchemaState(nextConnectionId);
      this._restoreExpandedSchemaLoads(nextConnectionId);
    }

    this._onDidRefreshSchemas.fire();
  }

  async getSchemaAsync(connectionId: string): Promise<SchemaObjectEntry[]> {
    const snapshot = await this.getSchemaSnapshotAsync(connectionId);
    return flattenSchemaSnapshot(snapshot);
  }

  async getSchemaSnapshotAsync(connectionId: string): Promise<SchemaSnapshot> {
    this._startRootSchemaLoad(connectionId, true);
    const entry = this._schemaCacheMap.get(connectionId);
    if (entry?.loading) {
      try {
        await entry.loading;
      } catch {}
    }
    return this.getSchemaSnapshot(connectionId);
  }

  private _getSchemaGeneration(connectionId: string): number {
    return this._schemaGenerationMap.get(connectionId) ?? 0;
  }

  private _getExpandedScopeKeys(connectionId: string): Set<SchemaScopeKey> {
    let expandedScopeKeys = this._schemaExpandedScopeKeyMap.get(connectionId);
    if (!expandedScopeKeys) {
      expandedScopeKeys = new Set<SchemaScopeKey>([
        getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE),
      ]);
      this._schemaExpandedScopeKeyMap.set(connectionId, expandedScopeKeys);
    }

    return expandedScopeKeys;
  }

  private _invalidateSchemaState(connectionId: string): void {
    this._schemaGenerationMap.set(
      connectionId,
      this._getSchemaGeneration(connectionId) + 1,
    );
    this._schemaCacheMap.delete(connectionId);
    this._onDidChangeSchemaState.fire(connectionId);
  }

  private _restoreExpandedSchemaLoads(connectionId: string): void {
    if (!this.isConnected(connectionId)) {
      return;
    }

    this.ensureSchemaSnapshotLoading(connectionId);
    for (const scopeKey of this._getExpandedScopeKeys(connectionId)) {
      if (scopeKey === getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE)) {
        continue;
      }

      const scope = parseExplorerSchemaScopeKey(scopeKey);
      if (!scope) {
        continue;
      }

      this.ensureSchemaScopeLoading(connectionId, scope);
    }
  }

  private _isLiveConnectionEntry(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
  ): boolean {
    const live = this._schemaCacheMap.get(connectionId);
    if (
      live !== entry ||
      live?.generation !== entry.generation ||
      this._getSchemaGeneration(connectionId) !== entry.generation
    ) {
      return false;
    }

    return true;
  }

  private _isLiveScopeEntry(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
  ): boolean {
    if (!this._isLiveConnectionEntry(connectionId, entry)) {
      return false;
    }

    return entry.scopes.get(scopeEntry.key) === scopeEntry;
  }

  private _isLiveTableDetailEntry(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
    tableDetailKey: string,
    tableDetailEntry: InternalTableDetailCacheEntry,
  ): boolean {
    if (!this._isLiveScopeEntry(connectionId, entry, scopeEntry)) {
      return false;
    }

    return scopeEntry.tableDetails.get(tableDetailKey) === tableDetailEntry;
  }

  private _getOrCreateSchemaCacheEntry(
    connectionId: string,
    config: ConnectionConfig,
  ): ConnectionSchemaCacheEntry {
    const generation = this._getSchemaGeneration(connectionId);
    let entry = this._schemaCacheMap.get(connectionId);

    if (!entry || entry.generation !== generation) {
      entry = createConnectionSchemaCacheEntry(
        generation,
        this._getExpandedScopeKeys(connectionId),
        getConfiguredDefaultDatabaseName(config),
      );
      this._schemaCacheMap.set(connectionId, entry);
    }

    if (!entry.defaultDatabaseName) {
      entry.defaultDatabaseName = getConfiguredDefaultDatabaseName(config);
    }

    return entry;
  }

  private _getOrCreateScopeEntry(
    entry: ConnectionSchemaCacheEntry,
    scope: ExplorerSchemaScope,
    retainOnCollapse = false,
  ): InternalScopedSchemaCacheEntry {
    const scopeKey = getExplorerSchemaScopeKey(scope);
    let scopeEntry = entry.scopes.get(scopeKey);

    if (!scopeEntry || scopeEntry.generation !== entry.generation) {
      scopeEntry = createScopedSchemaCacheEntry(
        scope,
        entry.generation,
        createEmptySchemaSnapshotState(),
        {},
        retainOnCollapse,
      );
      entry.scopes.set(scopeKey, scopeEntry);
    }

    if (retainOnCollapse) {
      scopeEntry.retainOnCollapse = true;
    }

    return scopeEntry;
  }

  private _commitAggregateSchemaState(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    fireSchemaLoadEvent = false,
  ): boolean {
    if (!this._isLiveConnectionEntry(connectionId, entry)) {
      return false;
    }

    const nextSnapshot = buildAggregateSchemaSnapshot(entry);
    const nextState = deriveAggregateSchemaState(entry, nextSnapshot);

    entry.snapshot = nextSnapshot;
    entry.status = nextState.status;
    entry.isPartial = nextState.isPartial;
    if (nextState.error) {
      entry.error = nextState.error;
    } else {
      delete entry.error;
    }

    this._onDidChangeSchemaState.fire(connectionId);
    if (fireSchemaLoadEvent) {
      this._onDidSchemaLoad.fire(connectionId);
    }
    return true;
  }

  private _startRootSchemaLoad(
    connectionId: string,
    allowRetry: boolean,
  ): void {
    const driver = this.getDriver(connectionId);
    const config = this.getConnection(connectionId);
    if (!driver || !config) {
      return;
    }

    const entry = this._getOrCreateSchemaCacheEntry(connectionId, config);
    const rootEntry = this._getOrCreateScopeEntry(
      entry,
      CONNECTION_ROOT_SCOPE,
      true,
    );

    if (entry.loading) {
      return;
    }

    const baselineKey = entry.defaultDatabaseName
      ? getExplorerSchemaScopeKey({
          kind: "database",
          database: entry.defaultDatabaseName,
        })
      : undefined;
    const baselineEntry = baselineKey
      ? entry.scopes.get(baselineKey)
      : undefined;

    if (
      rootEntry.status === "loaded" &&
      (!entry.defaultDatabaseName || baselineEntry?.status === "loaded")
    ) {
      return;
    }

    if (rootEntry.status === "error" && !allowRetry) {
      return;
    }

    rootEntry.status = "loading";
    rootEntry.isPartial = rootEntry.snapshot.databases.length > 0;
    delete rootEntry.error;
    this._commitAggregateSchemaState(connectionId, entry);

    let rootLoadPromise: Promise<void> | null = null;
    rootLoadPromise = (async () => {
      try {
        const { catalogSnapshot, defaultDatabaseName } =
          await this._loadConnectionRootCatalog(driver, config);
        if (!this._isLiveScopeEntry(connectionId, entry, rootEntry)) {
          return;
        }

        entry.defaultDatabaseName = defaultDatabaseName;
        rootEntry.snapshot = catalogSnapshot;
        rootEntry.status = "loaded";
        rootEntry.isPartial = false;
        rootEntry.fullyLoaded = true;
        delete rootEntry.error;

        if (defaultDatabaseName) {
          const defaultDatabaseEntry = this._getOrCreateScopeEntry(
            entry,
            {
              kind: "database",
              database: defaultDatabaseName,
            },
            true,
          );

          defaultDatabaseEntry.retainOnCollapse = true;
          if (
            !defaultDatabaseEntry.loading &&
            defaultDatabaseEntry.status !== "loaded"
          ) {
            defaultDatabaseEntry.status = "loading";
            defaultDatabaseEntry.isPartial =
              defaultDatabaseEntry.snapshot.databases.length > 0;
            delete defaultDatabaseEntry.error;
          }
        }

        if (!this._commitAggregateSchemaState(connectionId, entry)) {
          return;
        }

        if (!defaultDatabaseName) {
          return;
        }

        this._startDatabaseScopeLoad(
          connectionId,
          defaultDatabaseName,
          true,
          true,
          "baseline",
        );

        const liveBaselineEntry = entry.scopes.get(
          getExplorerSchemaScopeKey({
            kind: "database",
            database: defaultDatabaseName,
          }),
        );
        if (liveBaselineEntry?.loading) {
          try {
            await liveBaselineEntry.loading;
          } catch {}
        }
      } catch (err: unknown) {
        if (!this._isLiveScopeEntry(connectionId, entry, rootEntry)) {
          return;
        }

        const error = normalizeUnknownError(err);
        rootEntry.status = "error";
        rootEntry.isPartial = rootEntry.snapshot.databases.length > 0;
        rootEntry.error = error.message;
        this._commitAggregateSchemaState(connectionId, entry);
      } finally {
        if (
          rootLoadPromise &&
          this._schemaCacheMap.get(connectionId) === entry &&
          entry.loading === rootLoadPromise
        ) {
          entry.loading = null;
        }
      }
    })();

    entry.loading = rootLoadPromise;
  }

  private _startDatabaseScopeLoad(
    connectionId: string,
    databaseName: string,
    allowRetry: boolean,
    retainOnCollapse: boolean,
    loadMode: DatabaseLoadMode,
  ): void {
    const driver = this.getDriver(connectionId);
    const config = this.getConnection(connectionId);
    if (!driver || !config) {
      return;
    }

    const entry = this._getOrCreateSchemaCacheEntry(connectionId, config);
    const databaseScope: ExplorerSchemaScope = {
      kind: "database",
      database: databaseName,
    };
    const databaseEntry = this._getOrCreateScopeEntry(
      entry,
      databaseScope,
      retainOnCollapse,
    );

    if (retainOnCollapse) {
      databaseEntry.retainOnCollapse = true;
    }

    if (databaseEntry.loading) {
      return;
    }

    if (databaseEntry.status === "loaded") {
      if (loadMode === "expanded" || databaseEntry.fullyLoaded) {
        return;
      }
    }

    if (databaseEntry.status === "error" && !allowRetry) {
      return;
    }

    databaseEntry.status = "loading";
    databaseEntry.isPartial = databaseEntry.snapshot.databases.length > 0;
    delete databaseEntry.error;
    this._commitAggregateSchemaState(connectionId, entry);

    let databaseLoadPromise: Promise<void> | null = null;
    databaseLoadPromise = (async () => {
      try {
        const result = await this._loadDatabaseScopeInternal(
          driver,
          databaseName,
          loadMode,
        );
        if (!this._isLiveScopeEntry(connectionId, entry, databaseEntry)) {
          return;
        }

        databaseEntry.snapshot = createScopeSnapshotForDatabase(
          result.database,
        );
        databaseEntry.status = "loaded";
        databaseEntry.isPartial = false;
        databaseEntry.fragment = {
          database: cloneSchemaSnapshotDatabaseEntry(result.database),
        };
        databaseEntry.fullyLoaded =
          loadMode === "baseline" || result.database.schemas.length <= 1;
        delete databaseEntry.error;

        for (const schema of result.loadedSchemas) {
          this._upsertLoadedSchemaScope(
            entry,
            databaseName,
            schema,
            retainOnCollapse,
          );
        }

        this._commitAggregateSchemaState(connectionId, entry, true);
      } catch (err: unknown) {
        if (!this._isLiveScopeEntry(connectionId, entry, databaseEntry)) {
          return;
        }

        const error = normalizeUnknownError(err);
        databaseEntry.status = "error";
        databaseEntry.isPartial = databaseEntry.snapshot.databases.length > 0;
        databaseEntry.error = error.message;
        this._commitAggregateSchemaState(connectionId, entry);
      } finally {
        if (
          databaseLoadPromise &&
          this._isLiveScopeEntry(connectionId, entry, databaseEntry) &&
          databaseEntry.loading === databaseLoadPromise
        ) {
          databaseEntry.loading = null;
        }
      }
    })();

    databaseEntry.loading = databaseLoadPromise;
  }

  private _startSchemaScopeLoad(
    connectionId: string,
    databaseName: string,
    schemaName: string,
    allowRetry: boolean,
    retainOnCollapse: boolean,
  ): void {
    const driver = this.getDriver(connectionId);
    const config = this.getConnection(connectionId);
    if (!driver || !config) {
      return;
    }

    const entry = this._getOrCreateSchemaCacheEntry(connectionId, config);
    const schemaScope: ExplorerSchemaScope = {
      kind: "schema",
      database: databaseName,
      schema: schemaName,
    };
    const schemaEntry = this._getOrCreateScopeEntry(
      entry,
      schemaScope,
      retainOnCollapse,
    );

    if (retainOnCollapse) {
      schemaEntry.retainOnCollapse = true;
    }

    if (schemaEntry.loading || schemaEntry.status === "loaded") {
      return;
    }

    if (schemaEntry.status === "error" && !allowRetry) {
      return;
    }

    schemaEntry.status = "loading";
    schemaEntry.isPartial = schemaEntry.snapshot.databases.length > 0;
    delete schemaEntry.error;
    this._commitAggregateSchemaState(connectionId, entry);

    let schemaLoadPromise: Promise<void> | null = null;
    schemaLoadPromise = (async () => {
      try {
        const schema = await this._loadSchemaScopeInternal(
          driver,
          databaseName,
          schemaName,
        );
        if (!this._isLiveScopeEntry(connectionId, entry, schemaEntry)) {
          return;
        }

        schemaEntry.snapshot = createScopeSnapshotForSchema(
          databaseName,
          schema,
        );
        schemaEntry.status = "loaded";
        schemaEntry.isPartial = false;
        schemaEntry.fragment = {
          schema: cloneSchemaSnapshotSchemaEntry(schema),
        };
        schemaEntry.fullyLoaded = true;
        delete schemaEntry.error;
        this._commitAggregateSchemaState(connectionId, entry, true);
      } catch (err: unknown) {
        if (!this._isLiveScopeEntry(connectionId, entry, schemaEntry)) {
          return;
        }

        const error = normalizeUnknownError(err);
        schemaEntry.status = "error";
        schemaEntry.isPartial = schemaEntry.snapshot.databases.length > 0;
        schemaEntry.error = error.message;
        this._commitAggregateSchemaState(connectionId, entry);
      } finally {
        if (
          schemaLoadPromise &&
          this._isLiveScopeEntry(connectionId, entry, schemaEntry) &&
          schemaEntry.loading === schemaLoadPromise
        ) {
          schemaEntry.loading = null;
        }
      }
    })();

    schemaEntry.loading = schemaLoadPromise;
  }

  private _upsertLoadedSchemaScope(
    entry: ConnectionSchemaCacheEntry,
    databaseName: string,
    schema: SchemaSnapshotSchemaEntry,
    retainOnCollapse: boolean,
  ): void {
    const schemaEntry = this._getOrCreateScopeEntry(
      entry,
      {
        kind: "schema",
        database: databaseName,
        schema: schema.name,
      },
      retainOnCollapse,
    );

    schemaEntry.snapshot = createScopeSnapshotForSchema(databaseName, schema);
    schemaEntry.status = "loaded";
    schemaEntry.isPartial = false;
    schemaEntry.fragment = {
      schema: cloneSchemaSnapshotSchemaEntry(schema),
    };
    schemaEntry.retainOnCollapse =
      schemaEntry.retainOnCollapse || retainOnCollapse;
    schemaEntry.fullyLoaded = true;
    schemaEntry.loading = null;
    delete schemaEntry.error;
  }

  private async _loadConnectionRootCatalog(
    driver: IDBDriver,
    config: ConnectionConfig,
  ): Promise<{
    catalogSnapshot: SchemaSnapshot;
    defaultDatabaseName: string;
  }> {
    const configuredDb = getConfiguredDefaultDatabaseName(config);
    const allDbs = await driver.listDatabases().catch(() => []);
    const databaseNames = [
      ...new Set(
        [configuredDb, ...allDbs.map((database) => database.name)].filter(
          (name): name is string => typeof name === "string" && name.length > 0,
        ),
      ),
    ];

    return {
      catalogSnapshot: {
        databases: databaseNames.map((databaseName) => ({
          name: databaseName,
          schemas: [],
        })),
      },
      defaultDatabaseName: configuredDb || databaseNames[0] || "",
    };
  }

  private async _loadDatabaseScopeInternal(
    driver: IDBDriver,
    databaseName: string,
    loadMode: DatabaseLoadMode,
  ): Promise<DatabaseScopeLoadResult> {
    const schemas = await driver.listSchemas(databaseName).catch(() => []);
    const schemaNames = [
      ...new Set(
        (schemas.length > 0 ? schemas : [{ name: databaseName }])
          .map((schema) => schema.name)
          .filter(
            (name): name is string =>
              typeof name === "string" && name.length > 0,
          ),
      ),
    ];

    if (schemaNames.length <= 1) {
      const schema = await this._loadSchemaScopeInternal(
        driver,
        databaseName,
        schemaNames[0] ?? databaseName,
      );

      return {
        database: {
          name: databaseName,
          schemas: [schema],
        },
        loadedSchemas: [schema],
      };
    }

    if (loadMode === "expanded") {
      return {
        database: {
          name: databaseName,
          schemas: schemaNames.map((schemaName) => ({
            name: schemaName,
            objects: [],
          })),
        },
        loadedSchemas: [],
      };
    }

    const loadedSchemas = await pMapWithLimit(
      schemaNames,
      4,
      async (schemaName) =>
        this._loadSchemaScopeInternal(driver, databaseName, schemaName),
    );

    return {
      database: {
        name: databaseName,
        schemas: loadedSchemas,
      },
      loadedSchemas,
    };
  }

  private async _loadSchemaScopeInternal(
    driver: IDBDriver,
    databaseName: string,
    schemaName: string,
  ): Promise<SchemaSnapshotSchemaEntry> {
    const manifest = resolveDriverEntityManifest(driver);
    const supportedKinds = new Set(manifest.dbObjectKinds);
    const objects = await driver
      .listObjects(databaseName, schemaName)
      .catch(() => []);
    const objectsForSchema = objects.filter((object) =>
      supportedKinds.has(object.type),
    );
    const describedColumns = await pMapWithLimit(
      objectsForSchema,
      10,
      async (object) => {
        if (!isDataDbObjectKind(object.type)) {
          return [];
        }

        try {
          return await driver.describeTable(
            databaseName,
            schemaName,
            object.name,
          );
        } catch {
          return [];
        }
      },
    );

    return {
      name: schemaName,
      objects: objectsForSchema.map<SchemaSnapshotObjectEntry>(
        (object, index) => ({
          name: object.name,
          type: object.type,
          columns: isDataDbObjectKind(object.type)
            ? describedColumns[index].map((column) => ({
                name: column.name,
                type: column.type,
              }))
            : [],
        }),
      ),
    };
  }

  private async _loadTableDetailInternal(
    driver: IDBDriver,
    request: TableDetailRequest,
  ): Promise<
    Pick<TableDetailState, "snapshot" | "status" | "isPartial" | "error">
  > {
    const manifest = resolveDriverEntityManifest(driver);

    const loadSection = async <T>(
      availability: DriverEntityAvailability,
      loader: () => Promise<T[] | null>,
    ): Promise<{
      status: SchemaLoadStatus;
      items: T[];
      error?: string;
    }> => {
      if (availability === "not_applicable") {
        return {
          status: "loaded",
          items: [],
        };
      }

      try {
        const items = await loader();
        if (items === null) {
          return {
            status: "loaded",
            items: [],
          };
        }
        return {
          status: "loaded",
          items,
        };
      } catch (error: unknown) {
        return {
          status: "error",
          items: [],
          error: normalizeUnknownError(error).message,
        };
      }
    };

    const [columns, constraints, indexes, triggers] = await Promise.all([
      loadSection(manifest.tableSections.columns, async () =>
        driver.describeColumns(request.database, request.schema, request.table),
      ),
      loadSection(manifest.tableSections.constraints, async () =>
        driver.getConstraints(request.database, request.schema, request.table),
      ),
      loadSection(manifest.tableSections.indexes, async () =>
        driver.getIndexes(request.database, request.schema, request.table),
      ),
      loadSection(manifest.tableSections.triggers, async () =>
        driver.getTriggers(request.database, request.schema, request.table),
      ),
    ]);

    const snapshot: TableStructureSnapshot = {
      columns,
      constraints,
      indexes,
      triggers,
    };

    const sectionStates = [
      snapshot.columns,
      snapshot.constraints,
      snapshot.indexes,
      snapshot.triggers,
    ];
    const allErrored = sectionStates.every((state) => state.status === "error");
    const hasError = sectionStates.some((state) => state.status === "error");

    return {
      snapshot,
      status: allErrored ? "error" : "loaded",
      isPartial: hasError,
      error: allErrored
        ? sectionStates.find((state) => state.error)?.error
        : undefined,
    };
  }
  async testConnection(
    config: Omit<ConnectionConfig, "id">,
  ): Promise<TestConnectionResult> {
    const driver = this.createDriver({ ...config, id: TEST_CONNECTION_ID });
    try {
      await driver.connect();
      await driver.disconnect();
      return { success: true };
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      try {
        await driver.disconnect();
      } catch {}
      return { success: false, error: error.message };
    }
  }
  async disconnectAll(): Promise<void> {
    await Promise.allSettled(
      [...this.driverMap.keys()].map((id) => this.disconnectFrom(id)),
    );
  }
  getHistory(connectionId?: string): HistoryEntry[] {
    const all = this.store.readHistory();
    if (connectionId) {
      return all.filter((e) => e.connectionId === connectionId);
    }
    return all;
  }
  async addToHistory(connectionId: string, sql: string): Promise<void> {
    const trimmed = sql.trim();
    if (!trimmed) {
      return;
    }
    const all = this.store.readHistory();
    const latest = all[0];
    if (
      latest &&
      latest.sql === trimmed &&
      latest.connectionId === connectionId
    ) {
      return;
    }
    const entry: HistoryEntry = {
      id: randomUUID(),
      sql: trimmed,
      connectionId,
      executedAt: new Date().toISOString(),
    };
    const updated = [entry, ...all].slice(0, this.getHistoryLimit());
    await this.store.writeHistory(updated);
    this._onDidChangeHistory.fire();
  }
  async clearHistory(): Promise<void> {
    await this.store.writeHistory([]);
    this._onDidChangeHistory.fire();
  }
  getBookmarks(connectionId?: string): BookmarkEntry[] {
    const all = this.store.readBookmarks();
    if (connectionId) {
      return all.filter((b) => b.connectionId === connectionId);
    }
    return all;
  }
  getBookmark(id: string): BookmarkEntry | undefined {
    return this.store.readBookmarks().find((bookmark) => bookmark.id === id);
  }
  async addBookmark(connectionId: string, sql: string): Promise<BookmarkEntry> {
    const trimmed = sql.trim();
    const entry: BookmarkEntry = {
      id: randomUUID(),
      sql: trimmed,
      connectionId,
      savedAt: new Date().toISOString(),
    };
    const all = this.store.readBookmarks();
    await this.store.writeBookmarks([entry, ...all]);
    this._onDidChangeBookmarks.fire();
    return entry;
  }
  async removeBookmark(id: string): Promise<boolean> {
    const all = this.store.readBookmarks();
    if (!all.some((bookmark) => bookmark.id === id)) {
      return false;
    }
    await this.store.writeBookmarks(
      all.filter((bookmark) => bookmark.id !== id),
    );
    this._onDidChangeBookmarks.fire();
    return true;
  }
  private async _purgeBookmarksForConnection(
    connectionId: string,
  ): Promise<void> {
    await this._purgeEntriesForConnection(
      connectionId,
      () => this.store.readBookmarks(),
      (entries) => this.store.writeBookmarks(entries),
      () => this._onDidChangeBookmarks.fire(),
    );
  }
  async clearBookmarks(): Promise<void> {
    await this.store.writeBookmarks([]);
    this._onDidChangeBookmarks.fire();
  }
}
