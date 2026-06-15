import * as vscode from "vscode";
import { isDataDbObjectKind } from "../shared/dbObjectKinds";
import type {
  BookmarkEntry,
  ConnectionConfig,
  ExplorerSchemaScope,
  SchemaLoadStatus,
  SchemaObjectEntry,
  SchemaScopeKey,
  SchemaSnapshot,
  SchemaSnapshotDatabaseEntry,
  SchemaSnapshotObjectEntry,
  SchemaSnapshotSchemaEntry,
  SchemaSnapshotState,
  ScopedSchemaFragment,
  TableDetailRequest,
  TableDetailState,
  TableStructureSnapshot,
} from "./connectionManagerModels";
import {
  DEFAULT_DRIVER_ENTITY_MANIFEST,
  type DriverCapabilities,
  type DriverEntityAvailability,
  type DriverEntityManifest,
  type DriverStaticMetadata,
  type IDBDriver,
  resolveDriverTableSectionAvailability,
} from "./dbDrivers/types";
import {
  resolveDriverCapabilities,
  resolveDriverEntityManifest,
} from "./schema/schemaHelpers";
import type {
  ConnectionSchemaCacheEntry,
  InternalScopedSchemaCacheEntry,
  InternalTableDetailCacheEntry,
} from "./schemaCacheUtils";
import { pMapWithLimit } from "./utils/concurrency";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "./utils/errorHandling";

// ─── Internal Types ───────────────────────────────────────────────────────────
// Types moved to schemaCacheUtils.ts

interface DatabaseScopeLoadResult {
  database: SchemaSnapshotDatabaseEntry;
  loadedSchemas: SchemaSnapshotSchemaEntry[];
}

type DatabaseLoadMode = "baseline" | "expanded";

// ─── Re-exported from schemaCacheUtils ───────────────────────────────────────
import {
  buildAggregateSchemaSnapshot,
  CONNECTION_ROOT_SCOPE,
  cloneExplorerSchemaScope,
  cloneSchemaSnapshot,
  cloneSchemaSnapshotDatabaseEntry,
  cloneSchemaSnapshotSchemaEntry,
  cloneSchemaSnapshotState,
  cloneTableDetailState,
  cloneTableStructureSnapshot,
  createConnectionRootSchemaScope,
  createConnectionSchemaCacheEntry,
  createEmptySchemaSnapshot,
  createEmptySchemaSnapshotState,
  createEmptyTableDetailState,
  createEmptyTableStructureSnapshot,
  createInternalTableDetailCacheEntry,
  createSchemaSnapshotState,
  createScopedSchemaCacheEntry,
  createScopeSnapshotForDatabase,
  createScopeSnapshotForSchema,
  deriveAggregateSchemaState,
  flattenSchemaSnapshot,
  getConfiguredDefaultDatabaseName,
  getExplorerSchemaScopeKey,
  getTableDetailCacheKey,
  isConnectionRootScope,
  isDescendantScope,
  mergeSchemaIntoDatabase,
  parseExplorerSchemaScopeKey,
} from "./schemaCacheUtils";

export {
  buildAggregateSchemaSnapshot,
  CONNECTION_ROOT_SCOPE,
  cloneExplorerSchemaScope,
  cloneSchemaSnapshot,
  cloneSchemaSnapshotDatabaseEntry,
  cloneSchemaSnapshotSchemaEntry,
  cloneSchemaSnapshotState,
  cloneTableDetailState,
  cloneTableStructureSnapshot,
  createConnectionRootSchemaScope,
  createConnectionSchemaCacheEntry,
  createEmptySchemaSnapshot,
  createEmptySchemaSnapshotState,
  createEmptyTableDetailState,
  createEmptyTableStructureSnapshot,
  createInternalTableDetailCacheEntry,
  createSchemaSnapshotState,
  createScopedSchemaCacheEntry,
  createScopeSnapshotForDatabase,
  createScopeSnapshotForSchema,
  deriveAggregateSchemaState,
  flattenSchemaSnapshot,
  getConfiguredDefaultDatabaseName,
  getExplorerSchemaScopeKey,
  getTableDetailCacheKey,
  isConnectionRootScope,
  isDescendantScope,
  mergeSchemaIntoDatabase,
  parseExplorerSchemaScopeKey,
  resolveDriverCapabilities,
  resolveDriverEntityManifest,
};

// ─── SchemaCacheManager ───────────────────────────────────────────────────────

export interface SchemaCacheManagerDependencies {
  getDriver: (connectionId: string) => IDBDriver | undefined;
  getConnection: (connectionId: string) => ConnectionConfig | undefined;
  isConnected: (connectionId: string) => boolean;
}

export class SchemaCacheManager {
  private readonly schemaCacheMap = new Map<
    string,
    ConnectionSchemaCacheEntry
  >();
  private readonly schemaGenerationMap = new Map<string, number>();
  private readonly schemaExpandedScopeKeyMap = new Map<
    string,
    Set<SchemaScopeKey>
  >();
  private readonly connectionEpochMap = new Map<string, number>();
  private disposed = false;

  private readonly onDidChangeSchemaStateEmitter =
    new vscode.EventEmitter<string>();
  readonly onDidChangeSchemaState = this.onDidChangeSchemaStateEmitter.event;

  private readonly onDidSchemaLoadEmitter = new vscode.EventEmitter<string>();
  readonly onDidSchemaLoad = this.onDidSchemaLoadEmitter.event;

  private readonly onDidRefreshSchemasEmitter = new vscode.EventEmitter<void>();
  readonly onDidRefreshSchemas = this.onDidRefreshSchemasEmitter.event;

  constructor(private readonly dependencies: SchemaCacheManagerDependencies) {}

  private nextConnectionEpoch(connectionId: string): number {
    const nextEpoch = (this.connectionEpochMap.get(connectionId) ?? 0) + 1;
    this.connectionEpochMap.set(connectionId, nextEpoch);
    return nextEpoch;
  }

  isStaleConnectEpoch(connectionId: string, epoch: number): boolean {
    return (
      this.disposed ||
      (this.connectionEpochMap.get(connectionId) ?? 0) !== epoch
    );
  }

  private getSchemaGeneration(connectionId: string): number {
    return this.schemaGenerationMap.get(connectionId) ?? 0;
  }

  private getExpandedScopeKeys(connectionId: string): Set<SchemaScopeKey> {
    let expandedScopeKeys = this.schemaExpandedScopeKeyMap.get(connectionId);
    if (!expandedScopeKeys) {
      expandedScopeKeys = new Set<SchemaScopeKey>([
        getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE),
      ]);
      this.schemaExpandedScopeKeyMap.set(connectionId, expandedScopeKeys);
    }

    return expandedScopeKeys;
  }

  private invalidateSchemaState(connectionId: string): void {
    this.schemaGenerationMap.set(
      connectionId,
      this.getSchemaGeneration(connectionId) + 1,
    );
    this.schemaCacheMap.delete(connectionId);
    this.onDidChangeSchemaStateEmitter.fire(connectionId);
  }

  private isLiveConnectionEntry(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
  ): boolean {
    const live = this.schemaCacheMap.get(connectionId);
    if (
      live !== entry ||
      live?.generation !== entry.generation ||
      this.getSchemaGeneration(connectionId) !== entry.generation
    ) {
      return false;
    }

    return true;
  }

  private isLiveScopeEntry(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
  ): boolean {
    if (!this.isLiveConnectionEntry(connectionId, entry)) {
      return false;
    }

    return entry.scopes.get(scopeEntry.key) === scopeEntry;
  }

  private isLiveTableDetailEntry(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
    tableDetailKey: string,
    tableDetailEntry: InternalTableDetailCacheEntry,
  ): boolean {
    if (!this.isLiveScopeEntry(connectionId, entry, scopeEntry)) {
      return false;
    }

    return scopeEntry.tableDetails.get(tableDetailKey) === tableDetailEntry;
  }

  private getOrCreateSchemaCacheEntry(
    connectionId: string,
    config: ConnectionConfig,
  ): ConnectionSchemaCacheEntry {
    const generation = this.getSchemaGeneration(connectionId);
    let entry = this.schemaCacheMap.get(connectionId);

    if (!entry || entry.generation !== generation) {
      entry = createConnectionSchemaCacheEntry(
        generation,
        this.getExpandedScopeKeys(connectionId),
        getConfiguredDefaultDatabaseName(config),
      );
      this.schemaCacheMap.set(connectionId, entry);
    }

    if (!entry.defaultDatabaseName) {
      entry.defaultDatabaseName = getConfiguredDefaultDatabaseName(config);
    }

    return entry;
  }

  private getOrCreateScopeEntry(
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

  private enterScopeLoadingState(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
  ): void {
    scopeEntry.status = "loading";
    scopeEntry.isPartial = scopeEntry.snapshot.databases.length > 0;
    delete scopeEntry.error;
    this.commitAggregateSchemaState(connectionId, entry);
  }

  private markScopeLoadError(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    scopeEntry: InternalScopedSchemaCacheEntry,
    err: unknown,
  ): void {
    const error = normalizeUnknownError(err);
    scopeEntry.status = "error";
    scopeEntry.isPartial = scopeEntry.snapshot.databases.length > 0;
    scopeEntry.error = error.message;
    this.commitAggregateSchemaState(connectionId, entry);
  }

  private commitAggregateSchemaState(
    connectionId: string,
    entry: ConnectionSchemaCacheEntry,
    fireSchemaLoadEvent = false,
  ): boolean {
    if (!this.isLiveConnectionEntry(connectionId, entry)) {
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

    this.onDidChangeSchemaStateEmitter.fire(connectionId);
    if (fireSchemaLoadEvent) {
      this.onDidSchemaLoadEmitter.fire(connectionId);
    }
    return true;
  }

  private restoreExpandedSchemaLoads(connectionId: string): void {
    if (!this.dependencies.isConnected(connectionId)) {
      return;
    }

    this.ensureSchemaSnapshotLoading(connectionId);
    for (const scopeKey of this.getExpandedScopeKeys(connectionId)) {
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

  private prepareScopeLoadContext(
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
    const driver = this.dependencies.getDriver(connectionId);
    const config = this.dependencies.getConnection(connectionId);
    if (!driver || !config) {
      return undefined;
    }

    const entry = this.getOrCreateSchemaCacheEntry(connectionId, config);
    const scopeEntry = this.getOrCreateScopeEntry(
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

  private upsertLoadedSchemaScope(
    entry: ConnectionSchemaCacheEntry,
    databaseName: string,
    schema: SchemaSnapshotSchemaEntry,
    retainOnCollapse: boolean,
  ): void {
    const schemaEntry = this.getOrCreateScopeEntry(
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

  private async loadConnectionRootCatalog(
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

  private async loadDatabaseScopeInternal(
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
      const schema = await this.loadSchemaScopeInternal(
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
        this.loadSchemaScopeInternal(driver, databaseName, schemaName),
    );

    return {
      database: {
        name: databaseName,
        schemas: loadedSchemas,
      },
      loadedSchemas,
    };
  }

  private async loadSchemaScopeInternal(
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
          ...(object.routineIdentity
            ? { routineIdentity: object.routineIdentity }
            : {}),
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

  private async loadTableDetailInternal(
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

  // ─── Public API ───────────────────────────────────────────────────────────

  getSchemaSnapshot(connectionId: string): SchemaSnapshot {
    return this.getSchemaSnapshotState(connectionId).snapshot;
  }

  getSchemaSnapshotState(
    connectionId: string,
    scope?: ExplorerSchemaScope,
  ): SchemaSnapshotState {
    const state = this.schemaCacheMap.get(connectionId);
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
    const entry = this.schemaCacheMap.get(request.connectionId);
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

    const driver = this.dependencies.getDriver(request.connectionId);
    const config = this.dependencies.getConnection(request.connectionId);
    if (!driver || !config) {
      return;
    }

    const entry = this.getOrCreateSchemaCacheEntry(
      request.connectionId,
      config,
    );
    const schemaEntry = this.getOrCreateScopeEntry(
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
    this.onDidChangeSchemaStateEmitter.fire(request.connectionId);

    let loadPromise: Promise<void> | null = null;
    loadPromise = (async () => {
      try {
        if (schemaEntry.loading) {
          try {
            await schemaEntry.loading;
          } catch (e) {
            logErrorWithContext("Failed to await schema loading", e);
          }
        }

        if (
          !this.isLiveTableDetailEntry(
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

        const nextState = await this.loadTableDetailInternal(driver, request);
        if (
          !this.isLiveTableDetailEntry(
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
        if (nextState.error) {
          tableDetailEntry.error = nextState.error;
        } else {
          delete tableDetailEntry.error;
        }

        this.onDidChangeSchemaStateEmitter.fire(request.connectionId);
      } catch (err: unknown) {
        if (
          !this.isLiveTableDetailEntry(
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
        tableDetailEntry.error = error.message;
        this.onDidChangeSchemaStateEmitter.fire(request.connectionId);
      } finally {
        if (
          loadPromise &&
          this.isLiveTableDetailEntry(
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
      }
    })();

    tableDetailEntry.loading = loadPromise;
  }

  ensureSchemaScopeLoading(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void {
    this.markSchemaScopeExpanded(connectionId, scope);

    const config = this.dependencies.getConnection(connectionId);
    const configuredDefaultDatabase = config
      ? getConfiguredDefaultDatabaseName(config)
      : "";

    this.startScopeLoadForScope(connectionId, scope, configuredDefaultDatabase);
  }

  markSchemaScopeExpanded(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void {
    const expandedScopeKeys = this.getExpandedScopeKeys(connectionId);
    expandedScopeKeys.add(getExplorerSchemaScopeKey(scope));

    const entry = this.schemaCacheMap.get(connectionId);
    if (entry) {
      entry.expandedScopeKeys = expandedScopeKeys;
    }
  }

  markSchemaScopeCollapsed(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void {
    const expandedScopeKeys = this.getExpandedScopeKeys(connectionId);

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

    const entry = this.schemaCacheMap.get(connectionId);
    if (!entry) {
      return;
    }

    entry.expandedScopeKeys = expandedScopeKeys;
  }

  refreshSchemaCache(connectionIds?: string[]): void {
    const ids = connectionIds
      ? connectionIds
      : [
          ...new Set([
            ...this.schemaCacheMap.keys(),
            ...this.schemaGenerationMap.keys(),
          ]),
        ];

    for (const nextConnectionId of ids) {
      this.invalidateSchemaState(nextConnectionId);
      this.restoreExpandedSchemaLoads(nextConnectionId);
    }

    this.onDidRefreshSchemasEmitter.fire();
  }

  async getSchemaAsync(connectionId: string): Promise<SchemaObjectEntry[]> {
    const snapshot = await this.getSchemaSnapshotAsync(connectionId);
    return flattenSchemaSnapshot(snapshot);
  }

  async getSchemaSnapshotAsync(connectionId: string): Promise<SchemaSnapshot> {
    this.startRootSchemaLoad(connectionId, true);
    const entry = this.schemaCacheMap.get(connectionId);
    if (entry?.loading) {
      try {
        await entry.loading;
      } catch (e) {
        logErrorWithContext("Failed to await schema snapshot loading", e);
      }
    }
    return this.getSchemaSnapshot(connectionId);
  }

  invalidateDriverStaticMetadata(_connectionId: string): void {
    // This is handled by ConnectionManager
  }

  // ─── Private Load Methods ────────────────────────────────────────────────

  private startScopeLoadForScope(
    connectionId: string,
    scope: ExplorerSchemaScope,
    configuredDefaultDatabase: string,
  ): void {
    switch (scope.kind) {
      case "connectionRoot":
        this.startRootSchemaLoad(connectionId, false);
        return;
      case "database": {
        this.startRootSchemaLoad(connectionId, false);
        const loadMode: DatabaseLoadMode =
          configuredDefaultDatabase === scope.database
            ? "baseline"
            : "expanded";
        this.startDatabaseScopeLoad(
          connectionId,
          scope.database,
          false,
          loadMode === "baseline",
          loadMode,
        );
        return;
      }
      case "schema":
        this.startRootSchemaLoad(connectionId, false);
        this.startSchemaScopeLoad(
          connectionId,
          scope.database,
          scope.schema,
          false,
          configuredDefaultDatabase === scope.database,
        );
        return;
    }
  }

  private startRootSchemaLoad(connectionId: string, allowRetry: boolean): void {
    const driver = this.dependencies.getDriver(connectionId);
    const config = this.dependencies.getConnection(connectionId);
    if (!driver || !config) {
      return;
    }

    const entry = this.getOrCreateSchemaCacheEntry(connectionId, config);
    const rootEntry = this.getOrCreateScopeEntry(
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
    this.commitAggregateSchemaState(connectionId, entry);

    let rootLoadPromise: Promise<void> | null = null;
    rootLoadPromise = (async () => {
      try {
        const { catalogSnapshot, defaultDatabaseName } =
          await this.loadConnectionRootCatalog(driver, config);
        if (!this.isLiveScopeEntry(connectionId, entry, rootEntry)) {
          return;
        }

        entry.defaultDatabaseName = defaultDatabaseName;
        rootEntry.snapshot = catalogSnapshot;
        rootEntry.status = "loaded";
        rootEntry.isPartial = false;
        rootEntry.fullyLoaded = true;
        delete rootEntry.error;

        if (defaultDatabaseName) {
          const defaultDatabaseEntry = this.getOrCreateScopeEntry(
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

        if (!this.commitAggregateSchemaState(connectionId, entry)) {
          return;
        }

        if (!defaultDatabaseName) {
          return;
        }

        this.startDatabaseScopeLoad(
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
          } catch (e) {
            logErrorWithContext("Failed to await baseline schema loading", e);
          }
        }
      } catch (err: unknown) {
        if (!this.isLiveScopeEntry(connectionId, entry, rootEntry)) {
          return;
        }

        const error = normalizeUnknownError(err);
        rootEntry.status = "error";
        rootEntry.isPartial = rootEntry.snapshot.databases.length > 0;
        rootEntry.error = error.message;
        this.commitAggregateSchemaState(connectionId, entry);
      } finally {
        if (
          rootLoadPromise &&
          this.schemaCacheMap.get(connectionId) === entry &&
          entry.loading === rootLoadPromise
        ) {
          entry.loading = null;
        }
      }
    })();

    entry.loading = rootLoadPromise;
  }

  private startDatabaseScopeLoad(
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
    const scopeContext = this.prepareScopeLoadContext(
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

    this.enterScopeLoadingState(connectionId, entry, databaseEntry);

    let databaseLoadPromise: Promise<void> | null = null;
    databaseLoadPromise = (async () => {
      try {
        const result = await this.loadDatabaseScopeInternal(
          driver,
          databaseName,
          loadMode,
        );
        if (!this.isLiveScopeEntry(connectionId, entry, databaseEntry)) {
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
          this.upsertLoadedSchemaScope(
            entry,
            databaseName,
            schema,
            retainOnCollapse,
          );
        }

        this.commitAggregateSchemaState(connectionId, entry, true);
      } catch (err: unknown) {
        if (!this.isLiveScopeEntry(connectionId, entry, databaseEntry)) {
          return;
        }
        this.markScopeLoadError(connectionId, entry, databaseEntry, err);
      } finally {
        if (
          databaseLoadPromise &&
          this.isLiveScopeEntry(connectionId, entry, databaseEntry) &&
          databaseEntry.loading === databaseLoadPromise
        ) {
          databaseEntry.loading = null;
        }
      }
    })();

    databaseEntry.loading = databaseLoadPromise;
  }

  private startSchemaScopeLoad(
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
    const scopeContext = this.prepareScopeLoadContext(
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

    this.enterScopeLoadingState(connectionId, entry, schemaEntry);

    let schemaLoadPromise: Promise<void> | null = null;
    schemaLoadPromise = (async () => {
      try {
        const schema = await this.loadSchemaScopeInternal(
          driver,
          databaseName,
          schemaName,
        );
        if (!this.isLiveScopeEntry(connectionId, entry, schemaEntry)) {
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
        this.commitAggregateSchemaState(connectionId, entry, true);
      } catch (err: unknown) {
        if (!this.isLiveScopeEntry(connectionId, entry, schemaEntry)) {
          return;
        }
        this.markScopeLoadError(connectionId, entry, schemaEntry, err);
      } finally {
        if (
          schemaLoadPromise &&
          this.isLiveScopeEntry(connectionId, entry, schemaEntry) &&
          schemaEntry.loading === schemaLoadPromise
        ) {
          schemaEntry.loading = null;
        }
      }
    })();

    schemaEntry.loading = schemaLoadPromise;
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────

  markConnectionEpoch(connectionId: string): number {
    return this.nextConnectionEpoch(connectionId);
  }

  cleanupConnectionRuntimeState(connectionId: string): void {
    this.schemaCacheMap.delete(connectionId);
    this.schemaGenerationMap.delete(connectionId);
    this.schemaExpandedScopeKeyMap.delete(connectionId);
    this.onDidChangeSchemaStateEmitter.fire(connectionId);
  }

  dispose(): void {
    if (this.disposed) {
      return;
    }

    this.disposed = true;

    for (const connectionId of this.connectionEpochMap.keys()) {
      this.nextConnectionEpoch(connectionId);
    }

    this.schemaCacheMap.clear();
    this.schemaGenerationMap.clear();
    this.schemaExpandedScopeKeyMap.clear();

    this.onDidChangeSchemaStateEmitter.dispose();
    this.onDidSchemaLoadEmitter.dispose();
    this.onDidRefreshSchemasEmitter.dispose();
  }
}
