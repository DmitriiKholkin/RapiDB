import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { isDeepStrictEqual } from "node:util";
import * as vscode from "vscode";
import {
  isConnectionTlsEnabled,
  resolveConnectionTlsMode,
} from "../shared/connectionConfig";
import { resolveMongoRemoteTarget } from "../shared/mongoConnection";
import { resolveMongoDatabaseName } from "../shared/mongoDatabase";
import { getConfiguredRedisDatabaseName } from "../shared/redisDatabase";
import { resolveUrlRemoteTarget } from "../shared/urlRemoteTarget";
import {
  type ConnectionSavePrecondition,
  connectionEditState,
  connectionEditStatesMatch,
  preserveTrustedSshFingerprint,
} from "./connectionEditState";
import type {
  BookmarkEntry,
  ConnectAttempt,
  ConnectionConfig,
  ConnectionConnectIntent,
  ConnectionManagerLifecycleApi,
  DriverCapabilitiesApi,
  DriverMetadataApi,
  ExplorerSchemaScope,
  HistoryEntry,
  QueryEditorPresentationApi,
  RefreshSchemaRequest,
  SchemaLoadStatus,
  SchemaObjectEntry,
  SchemaScopeKey,
  SchemaSnapshot,
  SchemaSnapshotDatabaseEntry,
  SchemaSnapshotObjectEntry,
  SchemaSnapshotSchemaEntry,
  SchemaSnapshotState,
  ScopeAwareConnectionManagerApi,
  ScopedSchemaCacheEntry,
  ScopedSchemaFragment,
  TableDetailRequest,
  TableDetailState,
  TableStructureSnapshot,
  TestConnectionResult,
} from "./connectionManagerModels";
import {
  type ConnectionIdsByScope,
  type ConnectionManagerStore,
  canonicalizeOracleServiceName,
  connectionIdsMatch,
  connectionSessionSettings,
  VSCodeConnectionManagerStore,
} from "./connectionManagerStore";
import {
  type ConnectionSecretClearOptions,
  hasPersistedConnectionConfigChanges,
  omitAmbiguousCredentialBearingUris,
  resolveStoredCredentialBearingUri,
  sanitizeConnectionConfigForResponse,
  sanitizePersistedConnectionConfig,
  serializeConnectionSecretsForConfig,
  serializeConnectionSecretsForStoredConfig,
  shouldForceSecretStorage,
  shouldKeepHandWrittenSecretsInSettings,
} from "./connectionSecrets";
import { parseStoredConnectionSecrets } from "./connectionSecretsData";
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
  type DriverEntityManifest,
  type DriverStaticMetadata,
  type IDBDriver,
} from "./dbDrivers/types";
import type { DriverConnectionConfig } from "./driverRuntimeConfig";
import {
  type DatabaseLoadMode,
  type DatabaseScopeLoadResult,
  loadConnectionRootCatalog,
  loadDatabaseScope,
  loadSchemaScope,
  loadTableDetail,
  PartialSchemaLoadError,
} from "./schema/schemaLoaders";
import { ConnectionValidationService } from "./services/connectionValidationService";
import {
  type ConnectionSshSettings,
  createSshRuntime,
  normalizeSshFingerprintSha256,
  type SshRuntime,
  type SshRuntimeRequest,
} from "./services/sshRuntime";
import { runAbortableFilesystemOperation } from "./utils/abortableFilesystem";
import { normalizeUnknownError } from "./utils/errorHandling";
import { logger } from "./utils/logger";

export type {
  BookmarkEntry,
  ConnectAttempt,
  ConnectionConfig,
  ConnectionConnectIntent,
  ExplorerSchemaScope,
  HistoryEntry,
  SchemaSnapshot,
  ScopeAwareConnectionManagerApi,
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
  waitingForSchema: Promise<void> | null;
}

interface ConnectionSchemaCacheEntry extends SchemaSnapshotState {
  loading: Promise<void> | null;
  generation: number;
  defaultDatabaseName: string;
  scopes: Map<SchemaScopeKey, InternalScopedSchemaCacheEntry>;
  expandedScopeKeys: Set<SchemaScopeKey>;
}

const CONNECTION_ROOT_SCOPE: ExplorerSchemaScope = { kind: "connectionRoot" };
const SECRET_STORAGE_READ_HINT =
  "Secret Storage could not be read; saved credentials were not used. Unlock your keychain and retry.";

function connectionSessionChanged(
  previous: ConnectionConfig,
  next: ConnectionConfig,
): boolean {
  const before = connectionSessionSettings(previous);
  const after = connectionSessionSettings(next);
  if (isDeepStrictEqual(before, after)) return false;
  if (next.useSecretStorage !== true) return true;
  try {
    const sanitizedAfter = connectionSessionSettings(
      sanitizeConnectionConfigForResponse(next),
    );
    return !(
      isDeepStrictEqual(after, sanitizedAfter) &&
      isDeepStrictEqual(
        connectionSessionSettings(
          sanitizeConnectionConfigForResponse(previous),
        ),
        sanitizedAfter,
      )
    );
  } catch {
    return true;
  }
}

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
    waitingForSchema: null,
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
    ...(object.routineIdentity
      ? { routineIdentity: object.routineIdentity }
      : {}),
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
    ...(schema.objectsTruncated ? { objectsTruncated: true } : {}),
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
  if (config.type === "mongodb") {
    return resolveMongoDatabaseName(config);
  }

  if (config.type === "elasticsearch") {
    return "default";
  }

  if (config.type === "dynamodb") {
    return config.awsRegion || "default";
  }

  if (config.type === "oracle") {
    return config.serviceName || config.database || "";
  }

  if (config.type === "redis") {
    return getConfiguredRedisDatabaseName(config);
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

  const failedScope = [...entry.scopes.values()].find(
    (scope) => scope.status === "error",
  );
  return failedScope
    ? {
        status: "error",
        isPartial: snapshot.databases.length > 0,
        error: failedScope.error,
      }
    : { status: "loaded", isPartial: false };
}

function flattenSchemaSnapshot(snapshot: SchemaSnapshot): SchemaObjectEntry[] {
  return snapshot.databases.flatMap((database) =>
    database.schemas.flatMap((schema) =>
      schema.objects.map<SchemaObjectEntry>((object) => ({
        database: database.name,
        schema: schema.name,
        object: object.name,
        type: object.type,
        ...(object.routineIdentity
          ? { routineIdentity: object.routineIdentity }
          : {}),
        columns: object.columns,
      })),
    ),
  );
}

const TEST_CONNECTION_ID = "__test__";
const CONNECTION_CLEANUP_BUDGET_MS = 1_500;
const ORPHANED_SECRET_CLEANUP_DELAY_MS = 5 * 60_000;

async function awaitBoundedCleanup(
  label: string,
  cleanup: () => void | Promise<void>,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const completed = await Promise.race([
    Promise.resolve()
      .then(cleanup)
      .then(
        () => true,
        (error: unknown) => {
          logger.error(`${label} failed during cleanup`, error);
          return true;
        },
      ),
    new Promise<false>((resolve) => {
      timer = setTimeout(() => resolve(false), CONNECTION_CLEANUP_BUDGET_MS);
      timer.unref?.();
    }),
  ]);
  if (timer) clearTimeout(timer);
  if (!completed) {
    logger.warn(
      `${label} did not finish within ${CONNECTION_CLEANUP_BUDGET_MS}ms; continuing shutdown.`,
    );
  }
}

interface ConnectionManagerDependencies {
  createSshRuntime?: typeof createSshRuntime;
}

export class ConnectionManager
  implements
    ScopeAwareConnectionManagerApi,
    ConnectionManagerLifecycleApi,
    QueryEditorPresentationApi,
    DriverMetadataApi,
    DriverCapabilitiesApi
{
  private readonly validationService = new ConnectionValidationService();
  private readonly store: ConnectionManagerStore;
  private driverMap = new Map<string, IDBDriver>();
  private readonly automaticReconnectBlocks = new Map<string, string>();
  private readonly sshRuntimeMap = new Map<string, SshRuntime>();
  private readonly disposedSshRuntimes = new WeakSet<SshRuntime>();
  private readonly createSshRuntimeForConnection: typeof createSshRuntime;
  private readonly _secretReadFailures = new Set<string>();
  private readonly _connectingMap = new Map<
    string,
    {
      promise: Promise<void>;
      epoch: number;
    }
  >();
  private readonly _disconnectingMap = new Map<string, Promise<void>>();
  private readonly _connectAbortControllerMap = new Map<
    string,
    AbortController
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
  readonly onDidInvalidateConnectionMetadata: vscode.Event<string>;
  private readonly _onDidInvalidateConnectionMetadata =
    new vscode.EventEmitter<string>();
  readonly onDidSchemaLoad: vscode.Event<string>;
  private readonly _onDidSchemaLoad = new vscode.EventEmitter<string>();
  readonly onDidChangeSchemaState: vscode.Event<string>;
  private readonly _onDidChangeSchemaState = new vscode.EventEmitter<string>();
  readonly onDidRefreshSchemas: vscode.Event<string | undefined>;
  private readonly _onDidRefreshSchemas = new vscode.EventEmitter<
    string | undefined
  >();
  readonly onDidChangeConnectionSession: vscode.Event<string>;
  private readonly _onDidChangeConnectionSession =
    new vscode.EventEmitter<string>();
  private readonly _connectionSessionGenerations = new Map<string, number>();
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
  private _historyMutationTail: Promise<void> = Promise.resolve();
  private _bookmarkMutationTail: Promise<void> = Promise.resolve();
  private _pendingSecretMigration: Promise<void> | null = null;
  private _disposed = false;
  private lastKnownConnections: ConnectionConfig[] = [];
  private readonly pendingTrustedFingerprintWrites = new Map<
    string,
    ConnectionConfig
  >();
  private readonly orphanedSecretTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();
  private lastKnownUserScopeIds = new Set<string>();
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
    this.onDidInvalidateConnectionMetadata =
      this._onDidInvalidateConnectionMetadata.event;
    this.onDidDisconnect = this._onDidDisconnect.event;
    this.onDidSchemaLoad = this._onDidSchemaLoad.event;
    this.onDidChangeSchemaState = this._onDidChangeSchemaState.event;
    this.onDidRefreshSchemas = this._onDidRefreshSchemas.event;
    this.onDidChangeConnectionSession =
      this._onDidChangeConnectionSession.event;
    this.store.onDidChangeConfiguration(async (e) => {
      if (this._disposed) return;
      const previous = this.lastKnownConnections;
      this._connectionsCache = null;
      if (e.affectsConfiguration("rapidb.connections")) {
        const current = new Map(
          this.getConnections().map((config) => [config.id, config]),
        );
        this.reconcileOrphanedSecretCleanup(false);
        for (const before of previous) {
          const next = current.get(before.id);
          const expected = this.pendingTrustedFingerprintWrites.get(before.id);
          if (
            next &&
            expected &&
            !connectionSessionChanged(
              {
                ...before,
                ssh: {
                  ...before.ssh,
                  hostFingerprintSha256: expected.ssh?.hostFingerprintSha256,
                },
              },
              next,
            )
          )
            continue;
          if (next && !connectionSessionChanged(before, next)) continue;
          this.markConnectionSessionChanged(before.id);
          this.invalidateDriverStaticMetadata(before.id);
          if (this.driverMap.has(before.id) || this.isConnecting(before.id)) {
            void this.disconnectFrom(before.id).catch((error) => {
              logger.error("Failed to disconnect changed connection", error);
            });
          }
        }
        this._onDidChangeConnections.fire();
      }
      if (e.affectsConfiguration("rapidb.queryHistoryLimit")) {
        await this._trimHistoryToLimit();
      }
    }, context.subscriptions);
    void this.store.clearLegacyOrphanedSecretIds?.().catch((error) => {
      logger.error("Could not clear legacy orphaned secret ids", error);
    });
    this.reconcileOrphanedSecretCleanup(true);
  }

  getConnectionSessionGeneration(id: string): number {
    return this._connectionSessionGenerations.get(id) ?? 0;
  }

  getConnectionSessionNonce(id: string): string | undefined {
    try {
      return this.store.readConnectionSessionNonce?.(id);
    } catch {
      return undefined;
    }
  }

  private markConnectionSessionChanged(id: string): void {
    this._connectionSessionGenerations.set(
      id,
      this.getConnectionSessionGeneration(id) + 1,
    );
    void this.store
      .writeConnectionSessionNonce?.(id, randomUUID())
      .catch((error) => {
        logger.error("Could not record connection session change", error);
      });
    this._onDidChangeConnectionSession.fire(id);
  }

  private isOrphanedSecretCleanupDisabled(): boolean {
    try {
      return this.store.isRemoteWindow?.() === true;
    } catch {
      return true;
    }
  }

  private reconcileOrphanedSecretCleanup(startup: boolean): void {
    let scopes: ConnectionIdsByScope | undefined;
    try {
      scopes = this.store.getConnectionIdsByScope?.();
    } catch (error) {
      logger.error("Could not read connection settings scopes", error);
    }
    if (!scopes) return;
    const cleanupDisabled = this.isOrphanedSecretCleanupDisabled();
    const previousUserIds = this.lastKnownUserScopeIds;
    this.lastKnownUserScopeIds = new Set(scopes.user);
    void this._recordWorkspaceScopedConnectionIds(
      cleanupDisabled
        ? [...scopes.workspace, ...scopes.user]
        : scopes.workspace,
    );
    for (const id of this.store.readOrphanedSecretIds?.() ?? []) {
      if (this.isConnectionIdInUse(id)) {
        this.cancelOrphanedSecretCleanup(id);
      } else if (startup && !cleanupDisabled) {
        this.armOrphanedSecretTimer(id);
      }
    }
    if (cleanupDisabled) return;
    for (const id of previousUserIds) {
      if (!this.lastKnownUserScopeIds.has(id)) {
        void this._scheduleOrphanedSecretCleanupIfStored(id);
      }
    }
  }

  private isConnectionIdInUse(
    id: string,
    connections: readonly { id?: unknown }[] = [],
  ): boolean {
    if (
      !this.store.hasConnectionIdInAnyScope ||
      !this.store.readWorkspaceScopedConnectionIds
    )
      return true;
    try {
      return (
        connections.some(
          (connection) =>
            typeof connection.id === "string" &&
            connectionIdsMatch(connection.id, id),
        ) ||
        this.store.hasConnectionIdInAnyScope(id) ||
        this.store
          .readWorkspaceScopedConnectionIds()
          .some((candidate) => connectionIdsMatch(candidate, id))
      );
    } catch {
      return true;
    }
  }

  private isConnectionIdInOtherScope(id: string): boolean {
    try {
      return this.store.hasConnectionIdOutsideActiveScope?.(id) ?? false;
    } catch {
      return true;
    }
  }

  private isOrphanedSecretCleanupPending(id: string): boolean {
    return this.store.readOrphanedSecretIds?.().includes(id) ?? false;
  }

  private async _recordWorkspaceScopedConnectionIds(
    ids: string[],
  ): Promise<void> {
    if (
      !this.store.readWorkspaceScopedConnectionIds ||
      !this.store.writeWorkspaceScopedConnectionIds
    )
      return;
    try {
      const current = this.store.readWorkspaceScopedConnectionIds();
      const missing = ids.filter((id) => !current.includes(id));
      if (missing.length === 0) return;
      await this.store.writeWorkspaceScopedConnectionIds([
        ...current,
        ...missing,
      ]);
    } catch (error) {
      logger.error("Could not record workspace connection ids", error);
    }
  }

  private async _scheduleOrphanedSecretCleanupIfStored(
    id: string,
  ): Promise<void> {
    if (this.isConnectionIdInUse(id)) return;
    let stored: string | undefined;
    try {
      stored = await this.store.getSecret(id);
    } catch {
      return;
    }
    if (
      stored === undefined ||
      this._disposed ||
      this.isOrphanedSecretCleanupDisabled() ||
      this.lastKnownUserScopeIds.has(id) ||
      this.isConnectionIdInUse(id)
    )
      return;
    this.armOrphanedSecretTimer(id);
    void this._updateOrphanedSecretIds((ids) =>
      ids.includes(id) ? ids : [...ids, id],
    );
  }

  private armOrphanedSecretTimer(id: string): void {
    if (!id || this._disposed || this.isOrphanedSecretCleanupDisabled()) return;
    const pending = this.orphanedSecretTimers.get(id);
    if (pending) clearTimeout(pending);
    const timer = setTimeout(() => {
      if (this.orphanedSecretTimers.get(id) !== timer) return;
      this.orphanedSecretTimers.delete(id);
      void this._deleteOrphanedConnectionSecret(id);
    }, ORPHANED_SECRET_CLEANUP_DELAY_MS);
    timer.unref?.();
    this.orphanedSecretTimers.set(id, timer);
  }

  private cancelOrphanedSecretCleanup(id: string): void {
    const pending = this.orphanedSecretTimers.get(id);
    if (pending) clearTimeout(pending);
    this.orphanedSecretTimers.delete(id);
    void this._updateOrphanedSecretIds((ids) => ids.filter((x) => x !== id));
  }

  private async _updateOrphanedSecretIds(
    update: (ids: string[]) => string[],
  ): Promise<void> {
    if (!this.store.readOrphanedSecretIds || !this.store.writeOrphanedSecretIds)
      return;
    try {
      const current = this.store.readOrphanedSecretIds();
      const next = update(current);
      if (isDeepStrictEqual(current, next)) return;
      await this.store.writeOrphanedSecretIds(next);
    } catch (error) {
      logger.error("Could not record orphaned connection secrets", error);
    }
  }

  private async _deleteOrphanedConnectionSecret(id: string): Promise<void> {
    if (
      this._disposed ||
      this.isOrphanedSecretCleanupDisabled() ||
      !this.isOrphanedSecretCleanupPending(id)
    )
      return;
    try {
      await this.store.mutateConnections(async (connections) => {
        const stillOrphaned = () =>
          !this._disposed &&
          !this.orphanedSecretTimers.has(id) &&
          this.isOrphanedSecretCleanupPending(id) &&
          !this.isConnectionIdInUse(id, connections);
        if (!stillOrphaned()) return { result: undefined };
        const previousSecret = await this.store.getSecret(id);
        await this.store.deleteSecret(id);
        if (
          previousSecret !== undefined &&
          !stillOrphaned() &&
          (await this.store.getSecret(id).catch(() => undefined)) === undefined
        ) {
          await this.store.storeSecret(id, previousSecret);
        }
        return { result: undefined };
      });
    } catch (error) {
      logger.error(
        `Could not remove Secret Storage entry of deleted connection "${id}"`,
        error,
      );
      if (
        !this._disposed &&
        !this.orphanedSecretTimers.has(id) &&
        this.isOrphanedSecretCleanupPending(id)
      ) {
        this.armOrphanedSecretTimer(id);
      }
      return;
    }
    if (this._disposed || this.orphanedSecretTimers.has(id)) return;
    await this._updateOrphanedSecretIds((ids) => ids.filter((x) => x !== id));
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
  getSkipTableMutationPreview(): boolean {
    return this.store.getSkipTableMutationPreview();
  }
  private async _trimHistoryToLimit(): Promise<void> {
    await this._enqueueHistoryMutation(async () => {
      const limit = this.getHistoryLimit();
      const all = this.store.readHistory();
      if (limit === 0) {
        if (all.length > 0) {
          await this.store.writeHistory([]);
          this._onDidChangeHistory.fire();
        }
        return;
      }
      if (all.length > limit) {
        await this.store.writeHistory(all.slice(0, limit));
        this._onDidChangeHistory.fire();
      }
    });
  }

  private async _enqueueHistoryMutation<T>(
    operation: () => Promise<T>,
  ): Promise<T> {
    const run = this._historyMutationTail.then(operation, operation);
    this._historyMutationTail = run.then(
      () => undefined,
      () => undefined,
    );
    return await run;
  }

  private async _enqueueBookmarkMutation<T>(
    operation: () => Promise<T>,
  ): Promise<T> {
    const run = this._bookmarkMutationTail.then(operation, operation);
    this._bookmarkMutationTail = run.then(
      () => undefined,
      () => undefined,
    );
    return await run;
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
    clearOptions: ConnectionSecretClearOptions = {},
    ignorePreviousSecrets = false,
  ): Promise<(() => Promise<void>) | undefined> {
    const previousSecretSnapshot = await this.store.getSecret(config.id);
    const nextSecretSnapshot = shouldForceSecretStorage(config)
      ? serializeConnectionSecretsForStoredConfig(
          config,
          ignorePreviousSecrets ? undefined : previousSecretSnapshot,
          clearOptions,
        )
      : undefined;
    if (nextSecretSnapshot === previousSecretSnapshot) {
      return undefined;
    }

    const restore = async (): Promise<void> => {
      if (previousSecretSnapshot === undefined) {
        await this.store.deleteSecret(config.id);
      } else {
        await this.store.storeSecret(config.id, previousSecretSnapshot);
      }
    };

    try {
      if (nextSecretSnapshot === undefined) {
        await this.store.deleteSecret(config.id);
      } else {
        await this.store.storeSecret(config.id, nextSecretSnapshot);
      }
    } catch (writeError) {
      try {
        await restore();
      } catch (rollbackError) {
        throw new AggregateError(
          [writeError, rollbackError],
          `Secret update and rollback both failed for connection "${config.id}".`,
        );
      }
      throw writeError;
    }

    return restore;
  }

  private _withSecretReadHint(id: string, error: unknown): unknown {
    if (!this._secretReadFailures.has(id)) return error;
    const message = error instanceof Error ? error.message : String(error);
    const hinted = new Error(`${message} ${SECRET_STORAGE_READ_HINT}`, {
      cause: error,
    });
    if (error && typeof error === "object" && "code" in error) {
      Object.assign(hinted, { code: (error as { code: unknown }).code });
    }
    return hinted;
  }

  private async _removeUnusedConnectionSecrets(id: string): Promise<void> {
    try {
      await this.store.mutateConnections(async (connections) => {
        const current = connections.find((connection) => connection.id === id);
        if (
          current &&
          (current.useSecretStorage === false || current.type === "sqlite") &&
          (await this.store.getSecret(id)) !== undefined
        ) {
          await this.store.deleteSecret(id);
        }
        return { result: undefined };
      });
    } catch (error) {
      logger.error(
        `Could not remove unused Secret Storage entry for connection "${id}"`,
        error,
      );
    }
  }

  private async _hasUnflaggedStoredSecrets(
    config: ConnectionConfig,
  ): Promise<boolean> {
    return (
      typeof config.useSecretStorage !== "boolean" &&
      config.type !== "sqlite" &&
      !shouldKeepHandWrittenSecretsInSettings(config) &&
      (await this.store.getSecret(config.id)) !== undefined
    );
  }

  private async _migrateSingleConnectionSecretsIfNeeded(
    config: ConnectionConfig,
  ): Promise<void> {
    if (config.type === "sqlite" || !shouldForceSecretStorage(config)) {
      return;
    }
    await this.store.mutateConnections(async (storedConnections) => {
      const index = storedConnections.findIndex(
        (connection) => connection.id === config.id,
      );
      if (index < 0 || !shouldForceSecretStorage(storedConnections[index])) {
        return { result: undefined };
      }

      const liveConfig = canonicalizeOracleServiceName(
        storedConnections[index],
      );
      const rollback = await this._persistConnectionSecretsIfNeeded(
        liveConfig,
        { retainCredentialBearingUris: true },
      );
      const persisted = sanitizePersistedConnectionConfig(liveConfig);
      if (!hasPersistedConnectionConfigChanges(persisted, liveConfig)) {
        return { result: undefined, rollback };
      }
      const next = [...storedConnections];
      next[index] = persisted;
      return { connections: next, result: undefined, rollback };
    });
    this._connectionsCache = null;
  }

  private async _migrateAllStoredConnectionSecrets(): Promise<void> {
    const changed = await this.store.mutateConnections(
      async (storedConnections) => {
        const next = [...storedConnections];
        const rollbacks: Array<() => Promise<void>> = [];
        let hasChanges = false;
        for (let index = 0; index < next.length; index += 1) {
          const connection = canonicalizeOracleServiceName(next[index]);
          try {
            if (await this._hasUnflaggedStoredSecrets(connection)) {
              next[index] = { ...next[index], useSecretStorage: true };
              hasChanges = true;
              continue;
            }
            if (
              connection.type === "sqlite" ||
              !shouldForceSecretStorage(connection)
            ) {
              continue;
            }
            const rollback = await this._persistConnectionSecretsIfNeeded(
              connection,
              { retainCredentialBearingUris: true },
            );
            if (rollback) {
              rollbacks.push(rollback);
            }
            const persisted = sanitizePersistedConnectionConfig(connection);
            if (hasPersistedConnectionConfigChanges(persisted, connection)) {
              next[index] = persisted;
              hasChanges = true;
            }
          } catch (error) {
            logger.error(
              `Skipped secret migration for connection "${connection.id}"`,
              error,
            );
          }
        }

        const rollback = async (): Promise<void> => {
          const rollbackErrors: unknown[] = [];
          for (const restore of rollbacks.reverse()) {
            try {
              await restore();
            } catch (error) {
              rollbackErrors.push(error);
            }
          }
          if (rollbackErrors.length > 0) {
            throw new AggregateError(
              rollbackErrors,
              "Secret migration rollback failed.",
            );
          }
        };
        return {
          connections: hasChanges ? next : undefined,
          result: hasChanges,
          rollback: rollbacks.length > 0 ? rollback : undefined,
        };
      },
    );
    this._connectionsCache = null;
    if (changed) {
      this._onDidChangeConnections.fire();
    }
  }

  getConnections(): ConnectionConfig[] {
    this._assertNotDisposed();

    if (this._connectionsCache) {
      return this._connectionsCache;
    }

    this._scheduleSecretMigration();

    this._connectionsCache = this.store.getConnections().map((c) => ({
      ...canonicalizeOracleServiceName({
        ...c,
        id: c.id ?? randomUUID(),
        username: c.username ?? c.user,
      }),
    }));
    this.lastKnownConnections = structuredClone(this._connectionsCache);
    return this._connectionsCache;
  }
  private invalidateDriverStaticMetadata(connectionId: string): void {
    this._driverStaticMetadataCache.delete(connectionId);
  }
  getConnection(id: string): ConnectionConfig | undefined {
    return this.getConnections().find((c) => c.id === id);
  }
  async saveConnection(
    config: ConnectionConfig,
    clearOptions: ConnectionSecretClearOptions = {},
    precondition?: ConnectionSavePrecondition,
  ): Promise<ConnectionConfig> {
    this._assertNotDisposed();

    const canonicalConfig = canonicalizeOracleServiceName({
      ...config,
      id:
        typeof config.id === "string" && config.id.trim()
          ? config.id
          : randomUUID(),
    });

    const validation = this.validationService.validate(canonicalConfig);
    if (!validation.valid) {
      throw new Error(validation.message ?? "Connection settings are invalid.");
    }

    let persistedConfig = sanitizePersistedConnectionConfig(canonicalConfig);
    let replacesSecretProfileWithSqlite = false;
    const sessionChanged = await this.store.mutateConnections(
      async (connections) => {
        const idx = connections.findIndex((c) => c.id === canonicalConfig.id);
        const previous = connections[idx];
        replacesSecretProfileWithSqlite =
          canonicalConfig.type === "sqlite" &&
          previous !== undefined &&
          previous.type !== "sqlite" &&
          previous.useSecretStorage !== false;
        if (precondition?.kind === "create" && previous) {
          throw new Error(
            "A connection with this ID already exists. Reopen the form to create a new connection.",
          );
        }
        if (precondition?.kind === "edit") {
          if (precondition.id !== canonicalConfig.id || !previous) {
            throw new Error(
              "This connection was deleted. It was not recreated; close this editor.",
            );
          }
          if (!precondition.state) {
            throw new Error(
              "Secret Storage was unavailable when the editor was opened. Reopen Edit Connection before saving.",
            );
          }
          if (
            !connectionEditStatesMatch(
              precondition.state,
              connectionEditState(
                previous,
                previous.useSecretStorage === false ||
                  previous.type === "sqlite"
                  ? {}
                  : parseStoredConnectionSecrets(
                      await this.store.getSecret(previous.id),
                    ),
              ),
            )
          ) {
            throw new Error(
              "This connection changed since the editor was opened. Reopen Edit Connection before saving.",
            );
          }
          persistedConfig = preserveTrustedSshFingerprint(
            persistedConfig,
            precondition.state,
            previous,
          );
        }
        let sessionChanged = false;
        if (previous) {
          const safePrevious = omitAmbiguousCredentialBearingUris(previous);
          const hydratedPrevious = await this._hydratePassword(safePrevious);
          const previousSecrets =
            serializeConnectionSecretsForConfig(hydratedPrevious);
          const nextSecrets = shouldForceSecretStorage(canonicalConfig)
            ? serializeConnectionSecretsForStoredConfig(
                canonicalConfig,
                previousSecrets,
                clearOptions,
              )
            : serializeConnectionSecretsForConfig(canonicalConfig);
          sessionChanged =
            connectionSessionChanged(
              sanitizeConnectionConfigForResponse(safePrevious),
              sanitizeConnectionConfigForResponse(persistedConfig),
            ) ||
            !isDeepStrictEqual(
              parseStoredConnectionSecrets(previousSecrets),
              parseStoredConnectionSecrets(nextSecrets),
            );
        }
        const rollback =
          canonicalConfig.type !== "sqlite" &&
          shouldForceSecretStorage(canonicalConfig)
            ? await this._persistConnectionSecretsIfNeeded(
                canonicalConfig,
                clearOptions,
                previous?.useSecretStorage === false ||
                  previous?.type === "sqlite",
              )
            : undefined;
        const next = [...connections];
        if (idx >= 0) {
          next[idx] = persistedConfig;
        } else {
          next.push(persistedConfig);
        }
        return {
          connections: next,
          result: sessionChanged,
          rollback,
        };
      },
    );
    this._connectionsCache = null;
    if (sessionChanged) this.markConnectionSessionChanged(canonicalConfig.id);
    if (
      persistedConfig.useSecretStorage === false ||
      replacesSecretProfileWithSqlite
    ) {
      await this._removeUnusedConnectionSecrets(canonicalConfig.id);
    }
    this.invalidateDriverStaticMetadata(canonicalConfig.id);
    if (
      sessionChanged &&
      (this.driverMap.has(canonicalConfig.id) ||
        this.isConnecting(canonicalConfig.id))
    ) {
      await this.disconnectFrom(canonicalConfig.id);
    }
    this._onDidChangeConnections.fire();
    return sanitizeConnectionConfigForResponse(persistedConfig);
  }
  async removeConnection(id: string): Promise<boolean> {
    this._assertNotDisposed();

    await this.disconnectFrom(id);
    const removed = await this.store.mutateConnections(async (connections) => {
      if (!connections.some((connection) => connection.id === id)) {
        return { result: false };
      }
      const remaining = connections.filter(
        (connection) => connection.id !== id,
      );
      if (this.isConnectionIdInOtherScope(id)) {
        return { connections: remaining, result: true };
      }
      const previousSecret = await this.store.getSecret(id);
      try {
        await this.store.deleteSecret(id);
      } catch (deleteError) {
        if (previousSecret !== undefined) {
          try {
            await this.store.storeSecret(id, previousSecret);
          } catch (rollbackError) {
            throw new AggregateError(
              [deleteError, rollbackError],
              `Secret deletion and rollback failed for connection "${id}".`,
            );
          }
        }
        throw deleteError;
      }
      return {
        connections: remaining,
        result: true,
        rollback:
          previousSecret === undefined
            ? undefined
            : () => this.store.storeSecret(id, previousSecret),
      };
    });
    if (!removed) {
      return false;
    }
    this._connectionsCache = null;
    this.automaticReconnectBlocks.delete(id);
    this.invalidateDriverStaticMetadata(id);
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

    const renamedCount = await this.store.mutateConnections((connections) => {
      let count = 0;
      const updated = connections.map((connection) => {
        if (connection.folder?.trim() !== sourceFolder) return connection;
        count += 1;
        return { ...connection, folder: targetFolder };
      });
      return { connections: count > 0 ? updated : undefined, result: count };
    });
    if (renamedCount === 0) return 0;
    this._connectionsCache = null;
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

    const updatedCount = await this.store.mutateConnections((connections) => {
      let count = 0;
      const updated = connections.map((connection) => {
        if (!idsToMove.has(connection.id)) return connection;
        const currentFolder = connection.folder?.trim() || undefined;
        if (currentFolder === targetFolder) return connection;
        count += 1;
        return { ...connection, folder: targetFolder };
      });
      return { connections: count > 0 ? updated : undefined, result: count };
    });
    if (updatedCount === 0) return 0;
    this._connectionsCache = null;
    this._onDidChangeConnections.fire();
    return updatedCount;
  }

  async removeFolder(folderName: string): Promise<number> {
    this._assertNotDisposed();

    const targetFolder = folderName.trim();
    if (!targetFolder) {
      return 0;
    }

    const updatedCount = await this.store.mutateConnections((connections) => {
      let count = 0;
      const updated = connections.map((connection) => {
        if (connection.folder?.trim() !== targetFolder) return connection;
        count += 1;
        return { ...connection, folder: undefined };
      });
      return { connections: count > 0 ? updated : undefined, result: count };
    });
    if (updatedCount === 0) return 0;
    this._connectionsCache = null;
    this._onDidChangeConnections.fire();
    return updatedCount;
  }

  async _hydratePassword(config: ConnectionConfig): Promise<ConnectionConfig> {
    if (
      config.useSecretStorage === false ||
      config.type === "sqlite" ||
      shouldKeepHandWrittenSecretsInSettings(config)
    ) {
      return config;
    }
    let stored: string | undefined;
    try {
      stored = await this.store.getSecret(config.id);
      this._secretReadFailures.delete(config.id);
    } catch (error) {
      if (config.useSecretStorage === true) throw error;
      this._secretReadFailures.add(config.id);
      logger.error(
        `Could not read Secret Storage for connection "${config.id}"`,
        error,
      );
      return config;
    }
    if (config.useSecretStorage !== true && stored === undefined) {
      return config;
    }
    const secrets = parseStoredConnectionSecrets(stored);
    const ssh = config.ssh
      ? {
          ...config.ssh,
          password: secrets.sshPassword ?? config.ssh.password,
          privateKey: secrets.sshPrivateKey ?? config.ssh.privateKey,
          passphrase: secrets.sshPassphrase ?? config.ssh.passphrase,
        }
      : undefined;
    return {
      ...config,
      password: secrets.password ?? config.password ?? "",
      apiKey: secrets.apiKey ?? config.apiKey,
      awsAccessKeyId: secrets.awsAccessKeyId ?? config.awsAccessKeyId,
      awsSecretAccessKey:
        secrets.awsSecretAccessKey ?? config.awsSecretAccessKey,
      awsSessionToken: secrets.awsSessionToken ?? config.awsSessionToken,
      connectionUri: resolveStoredCredentialBearingUri(
        config.connectionUri,
        secrets.connectionUri,
      ),
      uri: resolveStoredCredentialBearingUri(config.uri, secrets.uri),
      endpoint: resolveStoredCredentialBearingUri(
        config.endpoint,
        secrets.endpoint,
      ),
      awsEndpoint: resolveStoredCredentialBearingUri(
        config.awsEndpoint,
        secrets.awsEndpoint,
      ),
      ssh,
      tls:
        config.tls !== undefined
          ? {
              ...config.tls,
              keyPassphrase:
                secrets.tlsKeyPassphrase ?? config.tls.keyPassphrase,
            }
          : config.tls,
    };
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
    await this._enqueueHistoryMutation(() =>
      this._purgeEntriesForConnection(
        connectionId,
        () => this.store.readHistory(),
        (entries) => this.store.writeHistory(entries),
        () => this._onDidChangeHistory.fire(),
      ),
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
          return new MongoDBDriver(config, timeoutSettingsProvider);
        case "redis":
          return new RedisDriver(config, timeoutSettingsProvider);
        case "elasticsearch":
          return new ElasticsearchDriver(config, timeoutSettingsProvider);
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
    if (!config.ssh) {
      return undefined;
    }

    const ssh = config.ssh;
    const host = ssh.host?.trim();
    const port = ssh.port ?? 22;
    const username = ssh.username?.trim();
    const fingerprintSha256 = ssh.hostFingerprintSha256?.trim();
    const hostVerificationMode =
      ssh.hostVerificationMode === "trustOnFirstUse"
        ? "trustOnFirstUse"
        : "manual";
    if (
      !host ||
      !port ||
      !username ||
      (hostVerificationMode === "manual" && !fingerprintSha256)
    ) {
      throw new Error("[RapiDB] SSH settings are incomplete.");
    }
    const readyTimeoutMs = this.store.getTimeoutSettings().connectionTimeoutMs;

    if (ssh.authMethod === "password") {
      if (!ssh.password) {
        throw new Error("[RapiDB] SSH password is missing.");
      }

      return {
        host,
        port,
        username,
        hostVerificationMode,
        fingerprintSha256,
        readyTimeoutMs,
        auth: {
          kind: "password",
          password: ssh.password,
        },
      };
    }

    if (!ssh.privateKey) {
      throw new Error("[RapiDB] SSH private key is missing.");
    }

    return {
      host,
      port,
      username,
      hostVerificationMode,
      fingerprintSha256,
      readyTimeoutMs,
      auth: {
        kind: "privateKey",
        privateKey: ssh.privateKey,
        passphrase: ssh.passphrase,
      },
    };
  }

  private resolveMongoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    return resolveMongoRemoteTarget(config);
  }

  private resolveRedisRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    if (config.connectionUri) {
      const parsed = new URL(config.connectionUri);
      return {
        host: parsed.hostname.replace(/^\[|\]$/g, ""),
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
    return resolveUrlRemoteTarget(value, defaultPort);
  }

  private resolveElasticsearchRemoteTarget(
    config: ConnectionConfig,
  ): { host: string; port: number } | undefined {
    return (
      this.resolveUrlRemoteTarget(
        config.connectionUri ?? config.endpoint,
        isConnectionTlsEnabled(resolveConnectionTlsMode(config)) ? 443 : 9200,
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
          runtimeOverrides: {
            ...baseConfig.runtimeOverrides,
            tlsServername: remoteTarget.host,
          },
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

  private async prepareDriverConfig(
    config: ConnectionConfig,
    signal?: AbortSignal,
  ): Promise<{
    config: DriverConnectionConfig;
    runtime?: SshRuntime;
  }> {
    const sshSettings = this.buildConnectionSshSettings(config);
    if (!sshSettings) {
      return {
        config: config as DriverConnectionConfig,
      };
    }

    const request = this.resolveSshRuntimeRequest(config);
    const runtime = signal
      ? await this.createSshRuntimeForConnection(sshSettings, request, {
          signal,
        })
      : await this.createSshRuntimeForConnection(sshSettings, request);

    try {
      return {
        config: this.applySshRuntimeToConfig(config, runtime),
        runtime,
      };
    } catch (error) {
      await this.disposeUnboundConnectionResources(undefined, runtime);
      throw error;
    }
  }

  private async persistTrustedSshFingerprintIfNeeded(
    config: ConnectionConfig,
    fingerprintSha256: string,
    signal: AbortSignal,
  ): Promise<void> {
    const ssh = config.ssh;
    if (ssh?.hostVerificationMode !== "trustOnFirstUse") return;
    const verifiedFingerprint =
      normalizeSshFingerprintSha256(fingerprintSha256);
    const connectionId = config.id;
    let expectedWrite: ConnectionConfig | undefined;
    let changed: boolean;
    try {
      changed = await this.store.mutateConnections((connections) => {
        signal.throwIfAborted();
        const index = connections.findIndex((c) => c.id === connectionId);
        const connection = connections[index];
        const currentSsh = connection?.ssh;
        const currentFingerprint = currentSsh?.hostFingerprintSha256
          ? normalizeSshFingerprintSha256(currentSsh.hostFingerprintSha256)
          : undefined;
        if (
          currentSsh?.hostVerificationMode !== "trustOnFirstUse" ||
          currentSsh.host?.trim() !== ssh.host?.trim() ||
          (currentSsh.port ?? 22) !== (ssh.port ?? 22) ||
          currentSsh.username?.trim() !== ssh.username?.trim() ||
          (ssh.hostFingerprintSha256?.trim() &&
            !currentSsh.hostFingerprintSha256?.trim()) ||
          (currentSsh.hostFingerprintSha256?.trim() &&
            currentFingerprint !== verifiedFingerprint)
        ) {
          throw new Error(
            "[RapiDB] SSH trust settings changed during connection setup. Please retry.",
          );
        }
        if (currentFingerprint === verifiedFingerprint) {
          return { result: false };
        }
        const next = [...connections];
        next[index] = {
          ...connection,
          ssh: { ...currentSsh, hostFingerprintSha256: fingerprintSha256 },
        };
        expectedWrite = next[index];
        this.pendingTrustedFingerprintWrites.set(connectionId, expectedWrite);
        return { connections: next, result: true };
      });
    } finally {
      if (
        this.pendingTrustedFingerprintWrites.get(connectionId) === expectedWrite
      ) {
        this.pendingTrustedFingerprintWrites.delete(connectionId);
      }
    }
    if (!changed) return;
    this._connectionsCache = null;
    this.getConnections();
    this.invalidateDriverStaticMetadata(connectionId);
    this._onDidChangeConnections.fire();
  }

  private async resolveValidatedConnectionConfig(
    id: string,
    fallbackConfig: ConnectionConfig,
    connectEpoch: number,
  ): Promise<ConnectionConfig | null> {
    await this._migrateSingleConnectionSecretsIfNeeded(fallbackConfig);

    if (this._isStaleConnectEpoch(id, connectEpoch)) {
      return null;
    }

    const liveConfig = this.getConnection(id) ?? fallbackConfig;
    const fullConfig = await this._hydratePassword(liveConfig);
    const validation = this.validationService.validate(fullConfig);
    if (!validation.valid) {
      throw new Error(validation.message ?? "Connection settings are invalid.");
    }

    return fullConfig;
  }

  private async connectPreparedDriver(
    config: ConnectionConfig,
    signal: AbortSignal,
    persistTrustedFingerprint = true,
  ): Promise<{
    driver: IDBDriver;
    runtime?: SshRuntime;
  }> {
    const prepared = await this.prepareDriverConfig(config, signal);
    let driver: IDBDriver | undefined;
    let rejectAbort: ((reason: Error) => void) | undefined;
    let tunnelClosedError: Error | undefined;

    const onAbort = () => {
      void driver?.disconnect().catch(() => undefined);
      rejectAbort?.(new DOMException("Aborted", "AbortError"));
    };
    const tunnelCloseSubscription = prepared.runtime?.onDidClose?.((error) => {
      tunnelClosedError ??= new Error(
        "[RapiDB] SSH tunnel closed before the database connection was established.",
        { cause: error },
      );
      void driver?.disconnect().catch(() => undefined);
      rejectAbort?.(tunnelClosedError);
    });

    try {
      if (prepared.runtime && persistTrustedFingerprint) {
        await this.persistTrustedSshFingerprintIfNeeded(
          config,
          prepared.runtime.verifiedFingerprintSha256,
          signal,
        );
      }
      if (tunnelClosedError) throw tunnelClosedError;
      driver = this.createDriver(prepared.config);
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) {
        throw new DOMException("Aborted", "AbortError");
      }
      const abortPromise = new Promise<never>((_resolve, reject) => {
        rejectAbort = reject;
      });
      const pendingConnect = driver.connect().finally(async () => {
        if (signal.aborted || tunnelClosedError) {
          await this.disposeUnboundConnectionResources(
            driver,
            prepared.runtime,
          );
        }
      });
      await Promise.race([pendingConnect, abortPromise]);
      signal.throwIfAborted();
      if (tunnelClosedError) throw tunnelClosedError;
      return { driver, runtime: prepared.runtime };
    } catch (err) {
      await this.disposeUnboundConnectionResources(driver, prepared.runtime);
      throw normalizeUnknownError(err, config);
    } finally {
      signal.removeEventListener("abort", onAbort);
      tunnelCloseSubscription?.dispose();
    }
  }

  private async disposeUnboundConnectionResources(
    driver?: IDBDriver,
    runtime?: SshRuntime,
  ): Promise<void> {
    if (driver) {
      await awaitBoundedCleanup("Unbound database driver disconnect", () =>
        driver.disconnect(),
      );
    }

    if (runtime) {
      await this.disposeSshRuntime(runtime, "Unbound SSH runtime disposal");
    }
  }

  private watchSshRuntime(
    id: string,
    runtime: SshRuntime,
    connectEpoch: number,
  ): void {
    runtime.onDidClose?.(() => {
      if (
        this._disposed ||
        this.disposedSshRuntimes.has(runtime) ||
        this.sshRuntimeMap.get(id) !== runtime ||
        this._isStaleConnectEpoch(id, connectEpoch)
      ) {
        return;
      }
      logger.warn(
        `SSH tunnel for connection "${id}" closed unexpectedly; disconnecting.`,
      );
      void this.disconnectFrom(id)
        .then(() => {
          if (!this._disposed) this._onDidChangeConnections.fire();
        })
        .catch((error) => {
          logger.error("Failed to disconnect after SSH tunnel closed", error);
        });
    });
  }

  private async disposeSshRuntime(
    runtime: SshRuntime,
    label: string,
  ): Promise<void> {
    if (this.disposedSshRuntimes.has(runtime)) return;
    this.disposedSshRuntimes.add(runtime);
    await awaitBoundedCleanup(label, () => runtime.dispose());
  }

  private finalizeConnectAttempt(id: string, connectEpoch: number): void {
    const pendingAttempt = this._connectingMap.get(id);
    if (pendingAttempt?.epoch === connectEpoch) {
      this._connectingMap.delete(id);
    }
    this._onDidChangeConnections.fire();
  }

  private async disconnectRegisteredDriver(
    id: string,
    preserveConnectingAttempt = false,
  ): Promise<boolean> {
    const hadDriver = this.driverMap.has(id);
    const driver = this.driverMap.get(id);
    const owner = {
      epoch: this._connectionEpochMap.get(id),
      driver,
      runtime: this.sshRuntimeMap.get(id),
    };
    if (driver) {
      await awaitBoundedCleanup(`Driver disconnect for ${id}`, () =>
        driver.disconnect(),
      );
    }

    await this._cleanupConnectionRuntimeState(
      id,
      preserveConnectingAttempt,
      owner,
    );
    return hadDriver;
  }

  beginConnect(
    id: string,
    intent: ConnectionConnectIntent = "automatic",
  ): ConnectAttempt {
    this._assertNotDisposed();

    if (this._disconnectingMap.has(id)) {
      return {
        promise: Promise.reject(
          new Error(
            `[RapiDB] Disconnect in progress: ${id}. Retry Connect after it finishes.`,
          ),
        ),
        isNew: true,
      };
    }
    const reconnectBlock = this.getAutomaticReconnectBlockReason(id);
    if (intent === "automatic" && reconnectBlock) {
      return {
        promise: Promise.reject(new Error(reconnectBlock)),
        isNew: true,
      };
    }

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
    const ac = new AbortController();
    this._connectAbortControllerMap.set(id, ac);
    this._connectingMap.set(id, {
      promise: attempt,
      epoch: connectEpoch,
    });
    this._onDidInvalidateConnectionMetadata.fire(id);
    this._onDidChangeConnections.fire();
    void (async () => {
      this._secretReadFailures.delete(id);
      try {
        if (this.driverMap.has(id)) {
          await this.disconnectRegisteredDriver(id, true);
        }
        if (this._isStaleConnectEpoch(id, connectEpoch)) {
          resolveAttempt();
          return;
        }
        const config = this.getConnection(id);
        if (!config) {
          throw new Error(`[RapiDB] Connection "${id}" not found`);
        }

        const fullConfig = await this.resolveValidatedConnectionConfig(
          id,
          config,
          connectEpoch,
        );
        if (!fullConfig) {
          resolveAttempt();
          return;
        }

        const { driver, runtime } = await this.connectPreparedDriver(
          fullConfig,
          ac.signal,
        );

        if (this._isStaleConnectEpoch(id, connectEpoch)) {
          await this.disposeUnboundConnectionResources(driver, runtime);
          resolveAttempt();
          return;
        }

        this.driverMap.set(id, driver);
        if (intent === "explicit") this.automaticReconnectBlocks.delete(id);
        if (runtime) {
          this.sshRuntimeMap.set(id, runtime);
          this.watchSshRuntime(id, runtime, connectEpoch);
        }
        this._invalidateSchemaState(id);
        this._onDidInvalidateConnectionMetadata.fire(id);
        this._onDidConnect.fire();
        resolveAttempt();
      } catch (err) {
        if (ac.signal.aborted && this._isStaleConnectEpoch(id, connectEpoch)) {
          resolveAttempt();
        } else {
          rejectAttempt(this._withSecretReadHint(id, err));
        }
      } finally {
        if (this._connectAbortControllerMap.get(id) === ac) {
          this._connectAbortControllerMap.delete(id);
        }
        this.finalizeConnectAttempt(id, connectEpoch);
      }
    })();
    return { promise: attempt, isNew: true };
  }
  async connectTo(
    id: string,
    intent: ConnectionConnectIntent = "automatic",
  ): Promise<void> {
    await this.beginConnect(id, intent).promise;
  }
  disconnectFrom(id: string): Promise<void> {
    const pending = this._disconnectingMap.get(id);
    if (pending) return pending;
    let resolveDisconnect!: () => void;
    let rejectDisconnect!: (error: unknown) => void;
    const disconnect = new Promise<void>((resolve, reject) => {
      resolveDisconnect = resolve;
      rejectDisconnect = reject;
    });
    this._disconnectingMap.set(id, disconnect);
    this._onDidInvalidateConnectionMetadata.fire(id);
    void (async () => {
      try {
        this._nextConnectionEpoch(id);
        const hadPendingConnect = this._connectingMap.has(id);
        this._connectingMap.delete(id);

        const ac = this._connectAbortControllerMap.get(id);
        if (ac) ac.abort();

        const hadDriver = await this.disconnectRegisteredDriver(id);
        if (hadDriver || hadPendingConnect) {
          this._onDidDisconnect.fire(id);
        }
        resolveDisconnect();
      } catch (error: unknown) {
        rejectDisconnect(error);
      } finally {
        this._disconnectingMap.delete(id);
      }
    })();
    return disconnect;
  }
  isConnected(id: string): boolean {
    return this.getDriver(id)?.isConnected() ?? false;
  }
  getConnectedCount(): number {
    return [...this.driverMap.keys()].filter((id) => this.isConnected(id))
      .length;
  }
  isConnecting(id: string): boolean {
    return this._connectingMap.has(id);
  }
  getDriver(id: string): IDBDriver | undefined {
    return this._disconnectingMap.has(id) ? undefined : this.driverMap.get(id);
  }
  getAutomaticReconnectBlockReason(id: string): string | undefined {
    const reason = this.driverMap.get(id)?.getAutomaticReconnectBlockReason?.();
    if (reason) this.automaticReconnectBlocks.set(id, reason);
    return this.automaticReconnectBlocks.get(id);
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
    tableDetailEntry.waitingForSchema = schemaEntry.loading;
    this._onDidChangeSchemaState.fire(request.connectionId);

    let loadPromise: Promise<void> | null = null;
    loadPromise = (async () => {
      try {
        if (tableDetailEntry.waitingForSchema) {
          try {
            await tableDetailEntry.waitingForSchema;
          } catch {}
        }
        tableDetailEntry.waitingForSchema = null;

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

        if (
          schemaEntry.status === "error" &&
          !schemaEntry.fragment.schema?.objects.some(
            (object) =>
              object.name === request.table &&
              object.type === request.objectKind,
          )
        ) {
          tableDetailEntry.status = "error";
          tableDetailEntry.error =
            schemaEntry.error ??
            `Failed to load schema ${request.database}.${request.schema}`;
          return;
        }

        const nextState = await loadTableDetail(driver, request);
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
    this.warmSchemaScope(connectionId, scope);
  }

  warmSchemaScope(connectionId: string, scope: ExplorerSchemaScope): void {
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

  isSchemaScopeExpanded(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): boolean {
    const expandedScopeKeys = this._getExpandedScopeKeys(connectionId);
    return expandedScopeKeys.has(getExplorerSchemaScopeKey(scope));
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
      this._invalidateSchemaState(nextConnectionId, true);
      this._restoreExpandedSchemaLoads(nextConnectionId);
    }

    this._onDidRefreshSchemas.fire(connectionId);
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

  private _invalidateSchemaState(
    connectionId: string,
    retainSnapshot = false,
  ): void {
    const previous = retainSnapshot
      ? this._schemaCacheMap.get(connectionId)
      : undefined;
    this._schemaGenerationMap.set(
      connectionId,
      this._getSchemaGeneration(connectionId) + 1,
    );
    this._schemaCacheMap.delete(connectionId);
    if (previous) {
      const next = createConnectionSchemaCacheEntry(
        this._getSchemaGeneration(connectionId),
        this._getExpandedScopeKeys(connectionId),
        previous.defaultDatabaseName,
      );
      for (const scope of previous.scopes.values()) {
        if (!scope.retainOnCollapse && !next.expandedScopeKeys.has(scope.key)) {
          continue;
        }
        next.scopes.set(
          scope.key,
          createScopedSchemaCacheEntry(
            scope.scope,
            next.generation,
            { snapshot: scope.snapshot, status: "idle", isPartial: true },
            scope.fragment,
            scope.retainOnCollapse,
          ),
        );
      }
      next.snapshot = cloneSchemaSnapshot(previous.snapshot);
      this._schemaCacheMap.set(connectionId, next);
    }
    this._onDidChangeSchemaState.fire(connectionId);
  }

  private async _cleanupConnectionRuntimeState(
    connectionId: string,
    preserveConnectingAttempt: boolean,
    owner: {
      epoch: number | undefined;
      driver: IDBDriver | undefined;
      runtime: SshRuntime | undefined;
    },
  ): Promise<void> {
    if (
      this._connectionEpochMap.get(connectionId) === owner.epoch &&
      this.driverMap.get(connectionId) === owner.driver &&
      this.sshRuntimeMap.get(connectionId) === owner.runtime
    ) {
      this.getAutomaticReconnectBlockReason(connectionId);
      this.driverMap.delete(connectionId);
      this.sshRuntimeMap.delete(connectionId);
      if (!preserveConnectingAttempt) {
        this._connectingMap.delete(connectionId);
      }
      this.invalidateDriverStaticMetadata(connectionId);
      this._schemaCacheMap.delete(connectionId);
      this._schemaGenerationMap.delete(connectionId);
      this._schemaExpandedScopeKeyMap.delete(connectionId);
      this._onDidChangeSchemaState.fire(connectionId);
    }
    if (owner.runtime) {
      await this.disposeSshRuntime(
        owner.runtime,
        `SSH runtime disposal for ${connectionId}`,
      );
    }
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
    commit = true,
  ): void {
    if (
      err instanceof PartialSchemaLoadError &&
      scopeEntry.scope.kind === "schema"
    ) {
      const schema = err.withCachedColumns(scopeEntry.fragment.schema);
      scopeEntry.snapshot = createScopeSnapshotForSchema(
        scopeEntry.scope.database,
        schema,
      );
      this._replaceSchemaScopeFragment(scopeEntry, schema);
    }
    const error = normalizeUnknownError(err);
    scopeEntry.status = "error";
    scopeEntry.fullyLoaded = false;
    scopeEntry.isPartial = scopeEntry.snapshot.databases.length > 0;
    scopeEntry.error = error.message;
    if (commit) {
      this._commitAggregateSchemaState(connectionId, entry);
    }
  }

  private _commitAggregateSchemaState(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    fireSchemaLoadEvent = false,
  ): boolean {
    if (!this._isLiveConnectionEntry(connectionId, entry)) {
      return false;
    }

    if (!this.isConnected(connectionId)) {
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

    if (
      (rootEntry.status === "error" || baselineEntry?.status === "error") &&
      !allowRetry
    ) {
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
        const databaseNames = new Set(
          catalogSnapshot.databases.map((database) => database.name),
        );
        for (const [key, cached] of entry.scopes) {
          if (
            cached.scope.kind !== "connectionRoot" &&
            !databaseNames.has(cached.scope.database)
          ) {
            entry.scopes.delete(key);
          }
        }

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
        const schemaNames = new Set(
          result.database.schemas.map((schema) => schema.name),
        );
        for (const [key, cached] of entry.scopes) {
          if (
            cached.scope.kind === "schema" &&
            cached.scope.database === databaseName &&
            !schemaNames.has(cached.scope.schema)
          ) {
            entry.scopes.delete(key);
          }
        }

        for (const schema of result.loadedSchemas) {
          this._upsertLoadedSchemaScope(
            entry,
            databaseName,
            schema,
            retainOnCollapse,
          );
        }

        for (const failed of result.failedSchemas ?? []) {
          const schemaEntry = this._getOrCreateScopeEntry(
            entry,
            { kind: "schema", database: databaseName, schema: failed.name },
            retainOnCollapse,
          );
          this._markScopeLoadError(
            connectionId,
            entry,
            schemaEntry,
            failed.error,
            false,
          );
        }
        if (result.failedSchemas?.length) {
          const database = buildAggregateSchemaSnapshot(entry).databases.find(
            (database) => database.name === databaseName,
          );
          if (database) {
            databaseEntry.snapshot = createScopeSnapshotForDatabase(database);
            databaseEntry.fragment = { database };
          }
          this._markScopeLoadError(
            connectionId,
            entry,
            databaseEntry,
            new Error(
              result.failedSchemas
                .map(
                  (failed) =>
                    `${failed.name}: ${normalizeUnknownError(failed.error).message}`,
                )
                .join("; "),
            ),
            false,
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
        this._replaceSchemaScopeFragment(
          schemaEntry,
          schema,
          schemaLoadPromise,
        );
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
    this._replaceSchemaScopeFragment(schemaEntry, schema);
    schemaEntry.retainOnCollapse =
      schemaEntry.retainOnCollapse || retainOnCollapse;
    schemaEntry.fullyLoaded = true;
    schemaEntry.loading = null;
    delete schemaEntry.error;
  }

  private _replaceSchemaScopeFragment(
    schemaEntry: InternalScopedSchemaCacheEntry,
    schema: SchemaSnapshotSchemaEntry,
    completedLoad: Promise<void> | null = null,
  ): void {
    for (const [key, detail] of schemaEntry.tableDetails) {
      if (!completedLoad || detail.waitingForSchema !== completedLoad) {
        schemaEntry.tableDetails.delete(key);
      }
    }
    schemaEntry.fragment = {
      schema: cloneSchemaSnapshotSchemaEntry(schema),
    };
  }

  private async _loadConnectionRootCatalog(
    driver: IDBDriver,
    config: ConnectionConfig,
  ): Promise<{
    catalogSnapshot: SchemaSnapshot;
    defaultDatabaseName: string;
  }> {
    return loadConnectionRootCatalog(driver, config);
  }

  private async _loadDatabaseScopeInternal(
    driver: IDBDriver,
    databaseName: string,
    loadMode: DatabaseLoadMode,
  ): Promise<DatabaseScopeLoadResult> {
    return loadDatabaseScope(driver, databaseName, loadMode);
  }

  private async _loadSchemaScopeInternal(
    driver: IDBDriver,
    databaseName: string,
    schemaName: string,
  ): Promise<SchemaSnapshotSchemaEntry> {
    return loadSchemaScope(driver, databaseName, schemaName);
  }

  async testConnection(
    config: Omit<ConnectionConfig, "id">,
    signal?: AbortSignal,
  ): Promise<TestConnectionResult> {
    const configWithId = canonicalizeOracleServiceName({
      ...config,
      id: TEST_CONNECTION_ID,
    });
    const validation = this.validationService.validate(configWithId);
    if (!validation.valid) {
      return {
        success: false,
        error: validation.message ?? "Connection settings are invalid.",
        validation,
      };
    }
    let runtime: SshRuntime | undefined;
    let driver: IDBDriver | undefined;
    try {
      const testSignal = signal ?? new AbortController().signal;
      let testConfig = configWithId;
      let successMessage: string | undefined;
      if (
        configWithId.type === "sqlite" &&
        configWithId.filePath &&
        configWithId.filePath !== ":memory:" &&
        !configWithId.readOnly
      ) {
        const target = path.resolve(configWithId.filePath);
        let exists = true;
        try {
          await runAbortableFilesystemOperation(testSignal, () =>
            fs.stat(target),
          );
        } catch (error) {
          const code = (error as NodeJS.ErrnoException).code;
          if (code === "ENOTDIR")
            throw new Error(
              `Directory does not exist: ${path.dirname(target)}`,
            );
          if (code !== "ENOENT") throw error;
          exists = false;
          try {
            const link = await runAbortableFilesystemOperation(testSignal, () =>
              fs.lstat(target),
            );
            if (link.isSymbolicLink())
              throw new Error(
                "SQLite test cannot safely verify a dangling symbolic link.",
              );
          } catch (linkError) {
            if ((linkError as NodeJS.ErrnoException).code !== "ENOENT")
              throw linkError;
          }
        }
        if (exists) {
          const file = await runAbortableFilesystemOperation(
            testSignal,
            () => fs.open(target, "r+"),
            (handle) => handle.close(),
          );
          try {
            testSignal.throwIfAborted();
          } finally {
            const closeFile = file.close();
            void closeFile.catch(() => undefined);
            await runAbortableFilesystemOperation(testSignal, () => closeFile);
          }
        }
        testSignal.throwIfAborted();
        const directory = exists
          ? path.dirname(
              await runAbortableFilesystemOperation(testSignal, () =>
                fs.realpath(target),
              ),
            )
          : path.dirname(target);
        let probe: string;
        try {
          probe = await runAbortableFilesystemOperation(
            testSignal,
            () => fs.mkdtemp(path.join(directory, ".rapidb-test-")),
            (dir) => fs.rmdir(dir),
          );
        } catch (error) {
          const code = (error as NodeJS.ErrnoException).code;
          if (code === "ENOENT" || code === "ENOTDIR")
            throw new Error(`Directory does not exist: ${directory}`);
          throw error;
        }
        const cleanupProbe = fs.rmdir(probe);
        void cleanupProbe.catch(() => undefined);
        await runAbortableFilesystemOperation(testSignal, () => cleanupProbe);
        testSignal.throwIfAborted();
        testConfig = exists
          ? { ...configWithId, readOnly: true }
          : { ...configWithId, filePath: ":memory:", readOnly: false };
        successMessage = exists
          ? "SQLite opened read-only; file and directory write permissions were checked."
          : "SQLite runtime and directory write permissions verified; the database will be created when connecting.";
      } else if (
        configWithId.type === "sqlite" &&
        configWithId.filePath !== ":memory:"
      ) {
        if (configWithId.filePath) {
          const target = path.resolve(configWithId.filePath);
          try {
            await runAbortableFilesystemOperation(testSignal, () =>
              fs.stat(target),
            );
          } catch (error) {
            const code = (error as NodeJS.ErrnoException).code;
            if (code === "ENOENT" || code === "ENOTDIR")
              throw new Error(
                "SQLite file does not exist; read-only connections do not create a database.",
              );
            throw error;
          }
        }
        testConfig = { ...configWithId, readOnly: true };
      }
      const prepared = await this.connectPreparedDriver(
        testConfig,
        testSignal,
        false,
      );
      runtime = prepared.runtime;
      driver = prepared.driver;
      return {
        success: true,
        ...(successMessage ? { message: successMessage } : {}),
      };
    } catch (err: unknown) {
      const error = normalizeUnknownError(err, configWithId);
      return { success: false, error: error.message };
    } finally {
      await this.disposeUnboundConnectionResources(driver, runtime);
    }
  }
  async disconnectAll(): Promise<void> {
    const ids = new Set([
      ...this.driverMap.keys(),
      ...this._connectAbortControllerMap.keys(),
      ...this._disconnectingMap.keys(),
    ]);
    await Promise.allSettled([...ids].map((id) => this.disconnectFrom(id)));
  }

  async dispose(): Promise<void> {
    if (this._disposed) {
      return;
    }

    this._disposed = true;
    for (const timer of this.orphanedSecretTimers.values()) clearTimeout(timer);
    this.orphanedSecretTimers.clear();

    for (const connectionId of this._connectingMap.keys()) {
      this._nextConnectionEpoch(connectionId);
    }
    for (const connectionId of this.driverMap.keys()) {
      this._nextConnectionEpoch(connectionId);
    }

    for (const [id] of this._connectAbortControllerMap) {
      this._nextConnectionEpoch(id);
    }
    for (const ac of this._connectAbortControllerMap.values()) {
      ac.abort();
    }
    this._connectAbortControllerMap.clear();

    await this.disconnectAll();
    this.driverMap.clear();
    this.automaticReconnectBlocks.clear();
    this.sshRuntimeMap.clear();
    this._connectingMap.clear();
    this._connectionsCache = null;
    this.lastKnownConnections = [];
    this.pendingTrustedFingerprintWrites.clear();
    this._driverStaticMetadataCache.clear();
    this._schemaCacheMap.clear();
    this._schemaGenerationMap.clear();
    this._schemaExpandedScopeKeyMap.clear();

    this._onDidChangeConnections.dispose();
    this._onDidChangeHistory.dispose();
    this._onDidChangeBookmarks.dispose();
    this._onDidDisconnect.dispose();
    this._onDidConnect.dispose();
    this._onDidInvalidateConnectionMetadata.dispose();
    this._onDidSchemaLoad.dispose();
    this._onDidChangeSchemaState.dispose();
    this._onDidRefreshSchemas.dispose();
    this._onDidChangeConnectionSession.dispose();
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
    if (this.getHistoryLimit() === 0) {
      return;
    }
    await this._enqueueHistoryMutation(async () => {
      const all = this.store.readHistory();
      const latest = all[0];
      if (latest?.sql === trimmed && latest.connectionId === connectionId)
        return;
      const entry: HistoryEntry = {
        id: randomUUID(),
        sql: trimmed,
        connectionId,
        executedAt: new Date().toISOString(),
      };
      const updated = [entry, ...all].slice(0, this.getHistoryLimit());
      await this.store.writeHistory(updated);
      this._onDidChangeHistory.fire();
    });
  }
  async clearHistory(): Promise<void> {
    await this._enqueueHistoryMutation(async () => {
      await this.store.writeHistory([]);
      this._onDidChangeHistory.fire();
    });
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
    return await this._enqueueBookmarkMutation(async () => {
      const all = this.store.readBookmarks();
      await this.store.writeBookmarks([entry, ...all]);
      this._onDidChangeBookmarks.fire();
      return entry;
    });
  }
  async removeBookmark(id: string): Promise<boolean> {
    return await this._enqueueBookmarkMutation(async () => {
      const all = this.store.readBookmarks();
      if (!all.some((bookmark) => bookmark.id === id)) return false;
      await this.store.writeBookmarks(
        all.filter((bookmark) => bookmark.id !== id),
      );
      this._onDidChangeBookmarks.fire();
      return true;
    });
  }
  private async _purgeBookmarksForConnection(
    connectionId: string,
  ): Promise<void> {
    await this._enqueueBookmarkMutation(() =>
      this._purgeEntriesForConnection(
        connectionId,
        () => this.store.readBookmarks(),
        (entries) => this.store.writeBookmarks(entries),
        () => this._onDidChangeBookmarks.fire(),
      ),
    );
  }
  async clearBookmarks(): Promise<void> {
    await this._enqueueBookmarkMutation(async () => {
      await this.store.writeBookmarks([]);
      this._onDidChangeBookmarks.fire();
    });
  }
}
