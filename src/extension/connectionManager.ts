import { randomUUID } from "node:crypto";
import * as vscode from "vscode";
import { isDataDbObjectKind } from "../shared/dbObjectKinds";
import {
  type BookmarkEntry,
  type ConnectAttempt,
  type ConnectionConfig,
  type ConnectionManagerLifecycleApi,
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
import {
  type ConnectionSecretSnapshot,
  hasPersistedConnectionConfigChanges,
  sanitizePersistedConnectionConfig,
  serializeConnectionSecretsForConfig,
  shouldForceSecretStorage,
} from "./connectionSecrets";
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
  type DriverStaticMetadata,
  type IDBDriver,
  resolveDriverTableSectionAvailability,
} from "./dbDrivers/types";
import type { DriverConnectionConfig } from "./driverRuntimeConfig";
import { ConnectionValidationService } from "./services/connectionValidationService";
import {
  type ConnectionSshSettings,
  createSshRuntime,
  type SshRuntime,
  type SshRuntimeRequest,
} from "./services/sshRuntime";
import { pMapWithLimit } from "./utils/concurrency";
import { normalizeUnknownError } from "./utils/errorHandling";

export type {
  BookmarkEntry,
  ConnectAttempt,
  ConnectionConfig,
  ConnectionManagerLifecycleApi,
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
  return [request.database, request.schema, request.objectKind, request.table]
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
  if (config.type === "elasticsearch") {
    return "default";
  }

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

interface ConnectionManagerDependencies {
  createSshRuntime?: typeof createSshRuntime;
}

export class ConnectionManager
  implements ScopeAwareConnectionManagerApi, ConnectionManagerLifecycleApi
{
  private readonly validationService = new ConnectionValidationService();
  private readonly store: ConnectionManagerStore;
  private driverMap = new Map<string, IDBDriver>();
  private readonly sshRuntimeMap = new Map<string, SshRuntime>();
  private readonly createSshRuntimeForConnection: typeof createSshRuntime;
  private readonly _connectingMap = new Map<
    string,
    {
      promise: Promise<void>;
      epoch: number;
    }
  >();
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
  private readonly _driverStaticMetadataCache = new Map<
    string,
    DriverStaticMetadata
  >();
  private readonly _schemaCacheMap = new Map<
    string,
    ConnectionSchemaCacheEntry
  >();
  private readonly _schemaGenerationMap = new Map<string, number>();
  private readonly _schemaExpandedScopeKeyMap = new Map<
    string,
    Set<SchemaScopeKey>
  >();
  private readonly _connectionEpochMap = new Map<string, number>();
  private _pendingSecretMigration: Promise<void> | null = null;
  private _disposed = false;
  constructor(
    context: vscode.ExtensionContext,
    store: ConnectionManagerStore = new VSCodeConnectionManagerStore(context),
    dependencies: ConnectionManagerDependencies = {},
  ) {
    this.store = store;
    this.createSshRuntimeForConnection =
      dependencies.createSshRuntime ?? createSshRuntime;
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

  private _assertNotDisposed(): void {
    if (this._disposed) {
      throw new Error("[RapiDB] ConnectionManager has been disposed");
    }
  }

  private _nextConnectionEpoch(connectionId: string): number {
    const nextEpoch = (this._connectionEpochMap.get(connectionId) ?? 0) + 1;
    this._connectionEpochMap.set(connectionId, nextEpoch);
    return nextEpoch;
  }

  private _isStaleConnectEpoch(connectionId: string, epoch: number): boolean {
    return (
      this._disposed ||
      (this._connectionEpochMap.get(connectionId) ?? 0) !== epoch
    );
  }

  private _scheduleSecretMigration(): void {
    if (this._pendingSecretMigration) {
      return;
    }

    const pending = Promise.resolve()
      .then(() => this._migrateAllStoredConnectionSecrets())
      .catch(() => undefined)
      .finally(() => {
        this._pendingSecretMigration = null;
      });

    this._pendingSecretMigration = pending;
  }

  private async _persistConnectionSecretsIfNeeded(
    config: ConnectionConfig,
  ): Promise<void> {
    if (!shouldForceSecretStorage(config)) {
      return;
    }

    const previousSecretSnapshot = await this.store.getSecret(config.id);
    const serializedSecrets = serializeConnectionSecretsForConfig(
      config,
      this.parseStoredSecrets(previousSecretSnapshot),
    );

    if (serializedSecrets === previousSecretSnapshot) {
      return;
    }

    if (!serializedSecrets) {
      await this.store.deleteSecret(config.id);
      return;
    }

    await this.store.storeSecret(config.id, serializedSecrets);
  }

  private async _migrateSingleConnectionSecretsIfNeeded(
    config: ConnectionConfig,
  ): Promise<void> {
    if (!shouldForceSecretStorage(config)) {
      return;
    }

    const persisted = sanitizePersistedConnectionConfig(config);
    const needsPersistedConfigUpdate = hasPersistedConnectionConfigChanges(
      persisted,
      config,
    );

    await this._persistConnectionSecretsIfNeeded(config);

    if (!needsPersistedConfigUpdate) {
      return;
    }

    const expectedRevision = this.store.getConnectionsRevision();
    const storedConnections = this.getConnections();
    const index = storedConnections.findIndex(
      (connection) => connection.id === config.id,
    );
    if (index < 0) {
      return;
    }

    storedConnections[index] = persisted;
    await this.saveConnections(storedConnections, {
      expectedRevision,
      skipIfRevisionMismatch: true,
    });
  }

  private async _migrateAllStoredConnectionSecrets(): Promise<void> {
    const expectedRevision = this.store.getConnectionsRevision();
    const storedConnections = this.getConnections();
    let hasChanges = false;

    for (const connection of storedConnections) {
      if (!shouldForceSecretStorage(connection)) {
        continue;
      }

      await this._persistConnectionSecretsIfNeeded(connection);
      const persisted = sanitizePersistedConnectionConfig(connection);
      if (hasPersistedConnectionConfigChanges(persisted, connection)) {
        const index = storedConnections.findIndex(
          (item) => item.id === connection.id,
        );
        if (index >= 0) {
          storedConnections[index] = persisted;
          hasChanges = true;
        }
      }
    }

    if (hasChanges) {
      const persisted = await this.saveConnections(storedConnections, {
        expectedRevision,
        skipIfRevisionMismatch: true,
      });
      if (persisted) {
        this._onDidChangeConnections.fire();
      }
    }
  }

  getConnections(): ConnectionConfig[] {
    this._assertNotDisposed();

    if (this._connectionsCache) {
      return this._connectionsCache;
    }

    this._scheduleSecretMigration();

    this._connectionsCache = this.store.getConnections().map((c) => ({
      ...c,
      id: c.id ?? randomUUID(),
      username: c.username ?? c.user,
    }));
    return this._connectionsCache;
  }
  private async saveConnections(
    conns: ConnectionConfig[],
    options?: {
      expectedRevision?: string;
      skipIfRevisionMismatch?: boolean;
    },
  ): Promise<boolean> {
    this._connectionsCache = null;
    if (options?.expectedRevision) {
      const saved = await this.store.saveConnectionsIfRevision(
        options.expectedRevision,
        conns,
      );
      if (!saved && !options.skipIfRevisionMismatch) {
        throw new Error(
          "[RapiDB] Cannot persist connections: configuration changed concurrently.",
        );
      }
      return saved;
    }

    await this.store.saveConnections(conns);
    return true;
  }
  private invalidateDriverStaticMetadata(connectionId: string): void {
    this._driverStaticMetadataCache.delete(connectionId);
  }
  getConnection(id: string): ConnectionConfig | undefined {
    return this.getConnections().find((c) => c.id === id);
  }
  async saveConnection(config: ConnectionConfig): Promise<void> {
    this._assertNotDisposed();

    const validation = this.validationService.validate(config);
    if (!validation.valid) {
      throw new Error(validation.message ?? "Connection settings are invalid.");
    }

    await this._persistConnectionSecretsIfNeeded(config);

    const persistedConfig = sanitizePersistedConnectionConfig(config);

    const conns = this.getConnections();
    const idx = conns.findIndex((c) => c.id === config.id);
    const isEdit = idx >= 0;
    if (isEdit) {
      conns[idx] = persistedConfig;
    } else {
      conns.push({
        ...persistedConfig,
        id: persistedConfig.id || randomUUID(),
      });
    }
    this.invalidateDriverStaticMetadata(config.id);
    await this.saveConnections(conns);
    if (isEdit && this.isConnected(config.id)) {
      await this.disconnectFrom(config.id);
    }
    this._onDidChangeConnections.fire();
  }
  async removeConnection(id: string): Promise<boolean> {
    this._assertNotDisposed();

    if (!this.getConnection(id)) {
      return false;
    }
    this.invalidateDriverStaticMetadata(id);
    await this.disconnectFrom(id);
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

  async renameFolder(
    currentFolderName: string,
    nextFolderName: string,
  ): Promise<number> {
    this._assertNotDisposed();

    const sourceFolder = currentFolderName.trim();
    const targetFolder = nextFolderName.trim();
    if (!sourceFolder || !targetFolder || sourceFolder === targetFolder) {
      return 0;
    }

    const connections = this.getConnections();
    let renamedCount = 0;

    const updatedConnections = connections.map((connection) => {
      if (connection.folder?.trim() !== sourceFolder) {
        return connection;
      }

      renamedCount += 1;
      return {
        ...connection,
        folder: targetFolder,
      };
    });

    if (renamedCount === 0) {
      return 0;
    }

    await this.saveConnections(updatedConnections);
    this._onDidChangeConnections.fire();
    return renamedCount;
  }

  async moveConnectionsToFolder(
    connectionIds: readonly string[],
    folderName?: string,
  ): Promise<number> {
    this._assertNotDisposed();

    const targetFolder = folderName?.trim() || undefined;
    const idsToMove = new Set(
      connectionIds.map((connectionId) => connectionId.trim()).filter(Boolean),
    );
    if (idsToMove.size === 0) {
      return 0;
    }

    const connections = this.getConnections();
    let updatedCount = 0;

    const updatedConnections = connections.map((connection) => {
      if (!idsToMove.has(connection.id)) {
        return connection;
      }

      const currentFolder = connection.folder?.trim() || undefined;
      if (currentFolder === targetFolder) {
        return connection;
      }

      updatedCount += 1;
      return {
        ...connection,
        folder: targetFolder,
      };
    });

    if (updatedCount === 0) {
      return 0;
    }

    await this.saveConnections(updatedConnections);
    this._onDidChangeConnections.fire();
    return updatedCount;
  }

  async removeFolder(folderName: string): Promise<number> {
    this._assertNotDisposed();

    const targetFolder = folderName.trim();
    if (!targetFolder) {
      return 0;
    }

    const connections = this.getConnections();
    let updatedCount = 0;

    const updatedConnections = connections.map((connection) => {
      if (connection.folder?.trim() !== targetFolder) {
        return connection;
      }

      updatedCount += 1;
      return {
        ...connection,
        folder: undefined,
      };
    });

    if (updatedCount === 0) {
      return 0;
    }

    await this.saveConnections(updatedConnections);
    this._onDidChangeConnections.fire();
    return updatedCount;
  }

  private parseStoredSecrets(value: string | undefined): {
    password?: string;
    apiKey?: string;
    awsAccessKeyId?: string;
    awsSecretAccessKey?: string;
    awsSessionToken?: string;
    connectionUri?: string;
    uri?: string;
    endpoint?: string;
    awsEndpoint?: string;
    sshPassword?: string;
    sshPrivateKey?: string;
    sshPassphrase?: string;
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
          apiKey: typeof parsed.apiKey === "string" ? parsed.apiKey : undefined,
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
          connectionUri:
            typeof parsed.connectionUri === "string"
              ? parsed.connectionUri
              : undefined,
          uri: typeof parsed.uri === "string" ? parsed.uri : undefined,
          endpoint:
            typeof parsed.endpoint === "string" ? parsed.endpoint : undefined,
          awsEndpoint:
            typeof parsed.awsEndpoint === "string"
              ? parsed.awsEndpoint
              : undefined,
          sshPassword:
            typeof parsed.sshPassword === "string"
              ? parsed.sshPassword
              : undefined,
          sshPrivateKey:
            typeof parsed.sshPrivateKey === "string"
              ? parsed.sshPrivateKey
              : undefined,
          sshPassphrase:
            typeof parsed.sshPassphrase === "string"
              ? parsed.sshPassphrase
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
        apiKey: secrets.apiKey ?? config.apiKey,
        awsAccessKeyId: secrets.awsAccessKeyId ?? config.awsAccessKeyId,
        awsSecretAccessKey:
          secrets.awsSecretAccessKey ?? config.awsSecretAccessKey,
        awsSessionToken: secrets.awsSessionToken ?? config.awsSessionToken,
        connectionUri: secrets.connectionUri ?? config.connectionUri,
        uri: secrets.uri ?? config.uri,
        endpoint: secrets.endpoint ?? config.endpoint,
        awsEndpoint: secrets.awsEndpoint ?? config.awsEndpoint,
        sshPassword: secrets.sshPassword ?? config.sshPassword,
        sshPrivateKey: secrets.sshPrivateKey ?? config.sshPrivateKey,
        sshPassphrase: secrets.sshPassphrase ?? config.sshPassphrase,
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

  private buildConnectionSshSettings(
    config: ConnectionConfig,
  ): ConnectionSshSettings | undefined {
    if (config.sshEnabled !== true) {
      return undefined;
    }

    const host = config.sshHost?.trim();
    const username = config.sshUsername?.trim();
    const fingerprintSha256 = config.sshHostFingerprintSha256?.trim();
    const hostVerificationMode =
      config.sshHostVerificationMode === "trustOnFirstUse"
        ? "trustOnFirstUse"
        : "manual";
    if (
      !host ||
      !config.sshPort ||
      !username ||
      (hostVerificationMode === "manual" && !fingerprintSha256)
    ) {
      throw new Error("[RapiDB] SSH settings are incomplete.");
    }

    if (config.sshAuthMethod === "password") {
      if (!config.sshPassword) {
        throw new Error("[RapiDB] SSH password is missing.");
      }

      return {
        host,
        port: config.sshPort,
        username,
        hostVerificationMode,
        fingerprintSha256,
        auth: {
          kind: "password",
          password: config.sshPassword,
        },
      };
    }

    if (!config.sshPrivateKey) {
      throw new Error("[RapiDB] SSH private key is missing.");
    }

    return {
      host,
      port: config.sshPort,
      username,
      hostVerificationMode,
      fingerprintSha256,
      auth: {
        kind: "privateKey",
        privateKey: config.sshPrivateKey,
        passphrase: config.sshPassphrase,
      },
    };
  }

  private resolveMongoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    const rawUri = config.connectionUri ?? config.uri;
    if (rawUri) {
      const parsed = new URL(rawUri);
      return {
        host: parsed.hostname,
        port: parsed.port ? Number.parseInt(parsed.port, 10) : 27017,
      };
    }

    return {
      host: config.host?.trim() || "localhost",
      port: config.port ?? 27017,
    };
  }

  private resolveRedisRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    if (config.connectionUri) {
      const parsed = new URL(config.connectionUri);
      return {
        host: parsed.hostname,
        port: parsed.port ? Number.parseInt(parsed.port, 10) : 6379,
      };
    }

    return {
      host: config.host?.trim() || "127.0.0.1",
      port: config.port ?? 6379,
    };
  }

  private resolveUrlRemoteTarget(
    value: string | undefined,
    defaultPort: number,
  ): { host: string; port: number } | undefined {
    if (!value) {
      return undefined;
    }

    const url = new URL(value);
    return {
      host: url.hostname,
      port:
        url.port.length > 0
          ? Number.parseInt(url.port, 10)
          : url.protocol === "https:"
            ? 443
            : defaultPort,
    };
  }

  private resolveElasticsearchRemoteTarget(
    config: ConnectionConfig,
  ): { host: string; port: number } | undefined {
    return (
      this.resolveUrlRemoteTarget(
        config.connectionUri ?? config.endpoint,
        config.ssl ? 443 : 9200,
      ) ??
      (config.host?.trim()
        ? {
            host: config.host.trim(),
            port: config.port ?? 9200,
          }
        : undefined)
    );
  }

  private resolveDynamoEndpoint(config: ConnectionConfig): string {
    if (config.endpoint) {
      return config.endpoint;
    }

    if (config.awsEndpoint) {
      return config.awsEndpoint;
    }

    const region = config.awsRegion?.trim() || "us-east-1";
    return `https://dynamodb.${region}.amazonaws.com`;
  }

  private resolveDynamoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    return (
      this.resolveUrlRemoteTarget(
        config.endpoint ?? config.awsEndpoint,
        443,
      ) ?? {
        host: `dynamodb.${config.awsRegion?.trim() || "us-east-1"}.amazonaws.com`,
        port: 443,
      }
    );
  }

  private resolveTcpRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    switch (config.type) {
      case "pg":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 5432,
        };
      case "mysql":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 3306,
        };
      case "mssql":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 1433,
        };
      case "oracle":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 1521,
        };
      case "mongodb":
        return this.resolveMongoRemoteTarget(config);
      case "redis":
        return this.resolveRedisRemoteTarget(config);
      case "elasticsearch": {
        const remoteTarget = this.resolveElasticsearchRemoteTarget(config);
        if (remoteTarget) {
          return remoteTarget;
        }

        throw new Error(
          "[RapiDB] Elasticsearch over SSH requires a fixed host, endpoint, or connection URI when Cloud ID is not used.",
        );
      }
      case "dynamodb":
        return this.resolveDynamoRemoteTarget(config);
      default: {
        const unsupported = config.type;
        throw new Error(
          `[RapiDB] SSH TCP forwarding is not supported for ${unsupported}.`,
        );
      }
    }
  }

  private resolveSshRuntimeRequest(
    config: ConnectionConfig,
  ): SshRuntimeRequest {
    switch (config.type) {
      case "pg":
      case "mysql":
      case "mssql":
      case "oracle":
      case "mongodb":
      case "redis": {
        const remoteTarget = this.resolveTcpRemoteTarget(config);
        return {
          kind: "tcpForward",
          remoteHost: remoteTarget.host,
          remotePort: remoteTarget.port,
        };
      }
      case "elasticsearch": {
        if (config.cloudId) {
          return { kind: "httpAgent" };
        }

        const remoteTarget = this.resolveTcpRemoteTarget(config);
        return {
          kind: "tcpForward",
          remoteHost: remoteTarget.host,
          remotePort: remoteTarget.port,
        };
      }
      case "dynamodb": {
        const remoteTarget = this.resolveTcpRemoteTarget(config);
        return {
          kind: "tcpForward",
          remoteHost: remoteTarget.host,
          remotePort: remoteTarget.port,
        };
      }
      default:
        throw new Error(
          `[RapiDB] SSH is not supported for ${config.type} connections.`,
        );
    }
  }

  private rewriteUriHostPort(
    value: string | undefined,
    host: string,
    port: number,
  ): string | undefined {
    if (!value) {
      return undefined;
    }

    const url = new URL(value);
    url.hostname = host;
    url.port = String(port);
    return url.toString();
  }

  private applySshRuntimeToConfig(
    config: ConnectionConfig,
    runtime: SshRuntime,
  ): DriverConnectionConfig {
    const runtimeConfig = config as DriverConnectionConfig;
    if (runtime.transport.kind === "httpAgent") {
      return {
        ...config,
        runtimeOverrides: {
          ...runtimeConfig.runtimeOverrides,
          transport: runtime.transport,
        },
      } as DriverConnectionConfig;
    }

    const remoteTarget = this.resolveTcpRemoteTarget(config);
    const baseConfig: DriverConnectionConfig = {
      ...config,
      host: runtime.transport.localHost,
      port: runtime.transport.localPort,
      runtimeOverrides: {
        ...runtimeConfig.runtimeOverrides,
        transport: runtime.transport,
      },
    };

    switch (config.type) {
      case "pg":
      case "mysql":
        baseConfig.runtimeOverrides = {
          ...baseConfig.runtimeOverrides,
          tlsServername: remoteTarget.host,
        };
        break;
      case "mssql":
        baseConfig.runtimeOverrides = {
          ...baseConfig.runtimeOverrides,
          mssqlServerName: remoteTarget.host,
        };
        break;
      case "mongodb": {
        const rewrittenConnectionUri = this.rewriteUriHostPort(
          config.connectionUri,
          runtime.transport.localHost,
          runtime.transport.localPort,
        );
        const rewrittenLegacyUri = this.rewriteUriHostPort(
          config.uri,
          runtime.transport.localHost,
          runtime.transport.localPort,
        );

        return {
          ...baseConfig,
          connectionUri: rewrittenConnectionUri,
          uri: rewrittenLegacyUri,
          directConnection: true,
        };
      }
      case "redis":
        return {
          ...baseConfig,
          runtimeOverrides: {
            ...baseConfig.runtimeOverrides,
            tlsServername: remoteTarget.host,
          },
          connectionUri: this.rewriteUriHostPort(
            config.connectionUri,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
        };
      case "elasticsearch":
        return {
          ...baseConfig,
          runtimeOverrides: {
            ...baseConfig.runtimeOverrides,
            tlsServername: remoteTarget.host,
          },
          connectionUri: this.rewriteUriHostPort(
            config.connectionUri,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
          endpoint: this.rewriteUriHostPort(
            config.endpoint,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
        };
      case "dynamodb":
        return {
          ...baseConfig,
          runtimeOverrides: {
            ...baseConfig.runtimeOverrides,
            tlsServername: remoteTarget.host,
          },
          endpoint: this.rewriteUriHostPort(
            config.endpoint ?? this.resolveDynamoEndpoint(config),
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
          awsEndpoint: this.rewriteUriHostPort(
            config.awsEndpoint,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
        };
      default:
        break;
    }

    return baseConfig;
  }

  private async prepareDriverConfig(config: ConnectionConfig): Promise<{
    config: DriverConnectionConfig;
    runtime?: SshRuntime;
  }> {
    const sshSettings = this.buildConnectionSshSettings(config);
    if (!sshSettings) {
      return {
        config: config as DriverConnectionConfig,
      };
    }

    const runtime = await this.createSshRuntimeForConnection(
      sshSettings,
      this.resolveSshRuntimeRequest(config),
    );

    return {
      config: this.applySshRuntimeToConfig(config, runtime),
      runtime,
    };
  }

  private async persistTrustedSshFingerprintIfNeeded(
    connectionId: string,
    fingerprintSha256: string,
  ): Promise<void> {
    const persistedConnection = this.getConnection(connectionId);
    if (!persistedConnection || persistedConnection.sshEnabled !== true) {
      return;
    }

    const hostVerificationMode =
      persistedConnection.sshHostVerificationMode === "trustOnFirstUse"
        ? "trustOnFirstUse"
        : "manual";
    if (hostVerificationMode !== "trustOnFirstUse") {
      return;
    }

    if (
      persistedConnection.sshHostFingerprintSha256?.trim() === fingerprintSha256
    ) {
      return;
    }

    const connections = this.getConnections().map((connection) =>
      connection.id === connectionId
        ? {
            ...connection,
            sshHostFingerprintSha256: fingerprintSha256,
          }
        : connection,
    );

    this.invalidateDriverStaticMetadata(connectionId);
    await this.saveConnections(connections);
    this._onDidChangeConnections.fire();
  }
  beginConnect(id: string): ConnectAttempt {
    this._assertNotDisposed();

    const pending = this._connectingMap.get(id);
    if (pending) {
      return { promise: pending.promise, isNew: false };
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
    const connectEpoch = this._nextConnectionEpoch(id);
    this._connectingMap.set(id, {
      promise: attempt,
      epoch: connectEpoch,
    });
    this._onDidChangeConnections.fire();
    void (async () => {
      let pendingRuntime: SshRuntime | undefined;
      try {
        if (this.driverMap.has(id)) {
          await this.disconnectFrom(id);
        }
        const config = this.getConnection(id);
        if (!config) {
          throw new Error(`[RapiDB] Connection "${id}" not found`);
        }

        await this._migrateSingleConnectionSecretsIfNeeded(config);

        if (this._isStaleConnectEpoch(id, connectEpoch)) {
          resolveAttempt();
          return;
        }

        const liveConfig = this.getConnection(id) ?? config;
        const fullConfig = await this._hydratePassword(liveConfig);
        const validation = this.validationService.validate(fullConfig);
        if (!validation.valid) {
          throw new Error(
            validation.message ?? "Connection settings are invalid.",
          );
        }
        const prepared = await this.prepareDriverConfig(fullConfig);
        pendingRuntime = prepared.runtime;
        if (pendingRuntime) {
          await this.persistTrustedSshFingerprintIfNeeded(
            id,
            pendingRuntime.verifiedFingerprintSha256,
          );
        }
        const driver = this.createDriver(prepared.config);
        try {
          await driver.connect();
        } catch (err) {
          try {
            await driver.disconnect();
          } catch {}
          if (pendingRuntime) {
            await pendingRuntime.dispose().catch(() => undefined);
            pendingRuntime = undefined;
          }
          throw err;
        }

        if (this._isStaleConnectEpoch(id, connectEpoch)) {
          try {
            await driver.disconnect();
          } catch {}
          if (pendingRuntime) {
            await pendingRuntime.dispose().catch(() => undefined);
            pendingRuntime = undefined;
          }
          resolveAttempt();
          return;
        }

        this.driverMap.set(id, driver);
        if (pendingRuntime) {
          this.sshRuntimeMap.set(id, pendingRuntime);
          pendingRuntime = undefined;
        }
        this._invalidateSchemaState(id);
        this._onDidConnect.fire();
        resolveAttempt();
      } catch (err) {
        if (pendingRuntime) {
          await pendingRuntime.dispose().catch(() => undefined);
        }
        rejectAttempt(err);
      } finally {
        const pendingAttempt = this._connectingMap.get(id);
        if (pendingAttempt?.epoch === connectEpoch) {
          this._connectingMap.delete(id);
        }
        this._onDidChangeConnections.fire();
      }
    })();
    return { promise: attempt, isNew: true };
  }
  async connectTo(id: string): Promise<void> {
    await this.beginConnect(id).promise;
  }
  async disconnectFrom(id: string): Promise<void> {
    this._nextConnectionEpoch(id);
    const hadDriver = this.driverMap.has(id);
    const hadPendingConnect = this._connectingMap.has(id);
    this._connectingMap.delete(id);

    const driver = this.driverMap.get(id);
    if (driver) {
      try {
        await driver.disconnect();
      } catch {}
    }

    await this._cleanupConnectionRuntimeState(id);

    if (hadDriver || hadPendingConnect) {
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

  private resolveDriverStaticMetadata(
    connectionId: string,
  ): DriverStaticMetadata | undefined {
    const driver = this.getDriver(connectionId);
    if (driver) {
      const capabilities = resolveDriverCapabilities(driver);
      return {
        manifest: resolveDriverEntityManifest(driver),
        capabilities,
        editorPresentation: capabilities?.editorPresentation,
      };
    }

    const cachedMetadata = this._driverStaticMetadataCache.get(connectionId);
    if (cachedMetadata) {
      return cachedMetadata;
    }

    const config = this.getConnection(connectionId);
    if (!config) {
      return undefined;
    }

    const metadataDriver = this.createDriver(config);
    const capabilities = resolveDriverCapabilities(metadataDriver);
    const metadata = {
      manifest: resolveDriverEntityManifest(metadataDriver),
      capabilities,
      editorPresentation: capabilities?.editorPresentation,
    };

    this._driverStaticMetadataCache.set(connectionId, metadata);

    return metadata;
  }

  getDriverCapabilities(connectionId: string): DriverCapabilities | undefined {
    return this.resolveDriverStaticMetadata(connectionId)?.capabilities;
  }

  getQueryEditorPresentation(connectionId: string) {
    return this.resolveDriverStaticMetadata(connectionId)?.editorPresentation;
  }

  getDriverEntityManifest(connectionId: string): DriverEntityManifest {
    return (
      this.resolveDriverStaticMetadata(connectionId)?.manifest ??
      DEFAULT_DRIVER_ENTITY_MANIFEST
    );
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

  private _startScopeLoadForScope(
    connectionId: string,
    scope: ExplorerSchemaScope,
    configuredDefaultDatabase: string,
  ): void {
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

  ensureSchemaScopeLoading(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void {
    this.markSchemaScopeExpanded(connectionId, scope);

    const config = this.getConnection(connectionId);
    const configuredDefaultDatabase = config
      ? getConfiguredDefaultDatabaseName(config)
      : "";

    this._startScopeLoadForScope(
      connectionId,
      scope,
      configuredDefaultDatabase,
    );
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

  private async _disposeSshRuntime(connectionId: string): Promise<void> {
    const runtime = this.sshRuntimeMap.get(connectionId);
    this.sshRuntimeMap.delete(connectionId);
    if (!runtime) {
      return;
    }

    try {
      await runtime.dispose();
    } catch {}
  }

  private async _cleanupConnectionRuntimeState(
    connectionId: string,
  ): Promise<void> {
    await this._disposeSshRuntime(connectionId);
    this.driverMap.delete(connectionId);
    this._connectingMap.delete(connectionId);
    this.invalidateDriverStaticMetadata(connectionId);
    this._schemaCacheMap.delete(connectionId);
    this._schemaGenerationMap.delete(connectionId);
    this._schemaExpandedScopeKeyMap.delete(connectionId);
    // Keep epoch fences to ensure stale in-flight connect attempts remain stale
    // even if a fresh connect starts immediately after disconnect.
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

  private _enterScopeLoadingState(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
  ): void {
    scopeEntry.status = "loading";
    scopeEntry.isPartial = scopeEntry.snapshot.databases.length > 0;
    delete scopeEntry.error;
    this._commitAggregateSchemaState(connectionId, entry);
  }

  private _markScopeLoadError(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
    err: unknown,
  ): void {
    const error = normalizeUnknownError(err);
    scopeEntry.status = "error";
    scopeEntry.isPartial = scopeEntry.snapshot.databases.length > 0;
    scopeEntry.error = error.message;
    this._commitAggregateSchemaState(connectionId, entry);
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

  private _prepareScopeLoadContext(
    connectionId: string,
    scope: ExplorerSchemaScope,
    allowRetry: boolean,
    retainOnCollapse: boolean,
  ):
    | {
        driver: IDBDriver;
        entry: ConnectionSchemaCacheEntry;
        scopeEntry: InternalScopedSchemaCacheEntry;
      }
    | undefined {
    const driver = this.getDriver(connectionId);
    const config = this.getConnection(connectionId);
    if (!driver || !config) {
      return undefined;
    }

    const entry = this._getOrCreateSchemaCacheEntry(connectionId, config);
    const scopeEntry = this._getOrCreateScopeEntry(
      entry,
      scope,
      retainOnCollapse,
    );

    if (retainOnCollapse) {
      scopeEntry.retainOnCollapse = true;
    }

    if (scopeEntry.loading) {
      return undefined;
    }

    if (scopeEntry.status === "error" && !allowRetry) {
      return undefined;
    }

    return { driver, entry, scopeEntry };
  }

  private _startDatabaseScopeLoad(
    connectionId: string,
    databaseName: string,
    allowRetry: boolean,
    retainOnCollapse: boolean,
    loadMode: DatabaseLoadMode,
  ): void {
    const databaseScope: ExplorerSchemaScope = {
      kind: "database",
      database: databaseName,
    };
    const scopeContext = this._prepareScopeLoadContext(
      connectionId,
      databaseScope,
      allowRetry,
      retainOnCollapse,
    );
    if (!scopeContext) {
      return;
    }
    const { driver, entry, scopeEntry: databaseEntry } = scopeContext;

    if (databaseEntry.status === "loaded") {
      if (loadMode === "expanded" || databaseEntry.fullyLoaded) {
        return;
      }
    }

    this._enterScopeLoadingState(connectionId, entry, databaseEntry);

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
        this._markScopeLoadError(connectionId, entry, databaseEntry, err);
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
    const schemaScope: ExplorerSchemaScope = {
      kind: "schema",
      database: databaseName,
      schema: schemaName,
    };
    const scopeContext = this._prepareScopeLoadContext(
      connectionId,
      schemaScope,
      allowRetry,
      retainOnCollapse,
    );
    if (!scopeContext) {
      return;
    }
    const { driver, entry, scopeEntry: schemaEntry } = scopeContext;

    if (schemaEntry.status === "loaded") {
      return;
    }

    this._enterScopeLoadingState(connectionId, entry, schemaEntry);

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
        this._markScopeLoadError(connectionId, entry, schemaEntry, err);
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
      loadSection(
        resolveDriverTableSectionAvailability(
          manifest,
          request.objectKind,
          "columns",
        ),
        async () =>
          driver.describeColumns(
            request.database,
            request.schema,
            request.table,
          ),
      ),
      loadSection(
        resolveDriverTableSectionAvailability(
          manifest,
          request.objectKind,
          "constraints",
        ),
        async () =>
          driver.getConstraints(
            request.database,
            request.schema,
            request.table,
          ),
      ),
      loadSection(
        resolveDriverTableSectionAvailability(
          manifest,
          request.objectKind,
          "indexes",
        ),
        async () =>
          driver.getIndexes(request.database, request.schema, request.table),
      ),
      loadSection(
        resolveDriverTableSectionAvailability(
          manifest,
          request.objectKind,
          "triggers",
        ),
        async () =>
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
    const configWithId = { ...config, id: TEST_CONNECTION_ID };
    const validation = this.validationService.validate(configWithId);
    if (!validation.valid) {
      return {
        success: false,
        error: validation.message ?? "Connection settings are invalid.",
        validation,
      };
    }
    let runtime: SshRuntime | undefined;
    const prepared = await this.prepareDriverConfig(configWithId);
    runtime = prepared.runtime;
    const driver = this.createDriver(prepared.config);
    try {
      await driver.connect();
      return { success: true };
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      return { success: false, error: error.message };
    } finally {
      try {
        await driver.disconnect();
      } catch {}
      if (runtime) {
        await runtime.dispose().catch(() => undefined);
      }
    }
  }
  async disconnectAll(): Promise<void> {
    await Promise.allSettled(
      [...this.driverMap.keys()].map((id) => this.disconnectFrom(id)),
    );
  }

  async dispose(): Promise<void> {
    if (this._disposed) {
      return;
    }

    this._disposed = true;

    for (const connectionId of this._connectingMap.keys()) {
      this._nextConnectionEpoch(connectionId);
    }
    for (const connectionId of this.driverMap.keys()) {
      this._nextConnectionEpoch(connectionId);
    }

    await this.disconnectAll();
    this.driverMap.clear();
    this.sshRuntimeMap.clear();
    this._connectingMap.clear();
    this._connectionsCache = null;
    this._driverStaticMetadataCache.clear();
    this._schemaCacheMap.clear();
    this._schemaGenerationMap.clear();
    this._schemaExpandedScopeKeyMap.clear();

    this._onDidChangeConnections.dispose();
    this._onDidChangeHistory.dispose();
    this._onDidChangeBookmarks.dispose();
    this._onDidDisconnect.dispose();
    this._onDidConnect.dispose();
    this._onDidSchemaLoad.dispose();
    this._onDidChangeSchemaState.dispose();
    this._onDidRefreshSchemas.dispose();
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
