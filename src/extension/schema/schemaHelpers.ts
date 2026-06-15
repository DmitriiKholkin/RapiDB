import { isDataDbObjectKind } from "../../shared/dbObjectKinds";
import type {
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
} from "../connectionManagerModels";
import {
  DEFAULT_DRIVER_ENTITY_MANIFEST,
  type DriverCapabilities,
  type DriverEntityAvailability,
  type DriverEntityManifest,
  type IDBDriver,
  resolveDriverTableSectionAvailability,
} from "../dbDrivers/types";

// ─── Constants ──────────────────────────────────────────────────────────────

export const CONNECTION_ROOT_SCOPE: ExplorerSchemaScope = {
  kind: "connectionRoot",
};

// ─── Scope Functions ────────────────────────────────────────────────────────

/**
 * Creates a connection root schema scope.
 */
export function createConnectionRootSchemaScope(): ExplorerSchemaScope {
  return { kind: "connectionRoot" };
}

/**
 * Gets the scope key for an explorer schema scope.
 */
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

/**
 * Parses a scope key back into an explorer schema scope.
 */
export function parseExplorerSchemaScopeKey(
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

/**
 * Checks if a scope is the connection root scope.
 */
export function isConnectionRootScope(scope?: ExplorerSchemaScope): boolean {
  return !scope || scope.kind === "connectionRoot";
}

/**
 * Checks if a scope is a descendant of an ancestor scope.
 */
export function isDescendantScope(
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

// ─── Driver Functions ───────────────────────────────────────────────────────

/**
 * Resolves the driver entity manifest.
 */
export function resolveDriverEntityManifest(
  driver: IDBDriver | undefined,
): DriverEntityManifest {
  return driver?.getEntityManifest?.() ?? DEFAULT_DRIVER_ENTITY_MANIFEST;
}

/**
 * Resolves driver capabilities.
 */
export function resolveDriverCapabilities(
  driver: IDBDriver | undefined,
): DriverCapabilities | undefined {
  return driver?.getCapabilities?.();
}

// ─── Snapshot Functions ─────────────────────────────────────────────────────

/**
 * Creates an empty schema snapshot.
 */
export function createEmptySchemaSnapshot(): SchemaSnapshot {
  return { databases: [] };
}

/**
 * Creates a schema snapshot state.
 */
export function createSchemaSnapshotState(
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

/**
 * Creates an empty schema snapshot state.
 */
export function createEmptySchemaSnapshotState(): SchemaSnapshotState {
  return createSchemaSnapshotState(createEmptySchemaSnapshot(), "idle", false);
}

/**
 * Creates an empty table structure snapshot.
 */
export function createEmptyTableStructureSnapshot(): TableStructureSnapshot {
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

/**
 * Creates an empty table detail state.
 */
export function createEmptyTableDetailState(
  request: TableDetailRequest,
): TableDetailState {
  return {
    request: { ...request },
    snapshot: createEmptyTableStructureSnapshot(),
    status: "idle",
    isPartial: false,
  };
}

// ─── Clone Functions ────────────────────────────────────────────────────────

/**
 * Clones a table structure snapshot.
 */
export function cloneTableStructureSnapshot(
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

/**
 * Clones a table detail state.
 */
export function cloneTableDetailState(
  state: TableDetailState,
): TableDetailState {
  return {
    request: { ...state.request },
    snapshot: cloneTableStructureSnapshot(state.snapshot),
    status: state.status,
    isPartial: state.isPartial,
    error: state.error,
  };
}

/**
 * Clones a schema snapshot object entry.
 */
export function cloneSchemaSnapshotObjectEntry(
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

/**
 * Clones a schema snapshot schema entry.
 */
export function cloneSchemaSnapshotSchemaEntry(
  schema: SchemaSnapshotSchemaEntry,
): SchemaSnapshotSchemaEntry {
  return {
    name: schema.name,
    objects: schema.objects.map(cloneSchemaSnapshotObjectEntry),
  };
}

/**
 * Clones a schema snapshot database entry.
 */
export function cloneSchemaSnapshotDatabaseEntry(
  database: SchemaSnapshotDatabaseEntry,
): SchemaSnapshotDatabaseEntry {
  return {
    name: database.name,
    schemas: database.schemas.map(cloneSchemaSnapshotSchemaEntry),
  };
}

/**
 * Clones a schema snapshot.
 */
export function cloneSchemaSnapshot(snapshot: SchemaSnapshot): SchemaSnapshot {
  return {
    databases: snapshot.databases.map(cloneSchemaSnapshotDatabaseEntry),
  };
}

/**
 * Clones an explorer schema scope.
 */
export function cloneExplorerSchemaScope(
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

/**
 * Clones a schema snapshot state.
 */
export function cloneSchemaSnapshotState(
  state: SchemaSnapshotState,
): SchemaSnapshotState {
  return createSchemaSnapshotState(
    cloneSchemaSnapshot(state.snapshot),
    state.status,
    state.isPartial,
    state.error,
  );
}

// ─── Scope Snapshot Functions ───────────────────────────────────────────────

/**
 * Creates a scope snapshot for a database.
 */
export function createScopeSnapshotForDatabase(
  database: SchemaSnapshotDatabaseEntry,
): SchemaSnapshot {
  return {
    databases: [database],
  };
}

/**
 * Creates a scope snapshot for a schema.
 */
export function createScopeSnapshotForSchema(
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

// ─── Cache Entry Functions ──────────────────────────────────────────────────

/**
 * Creates an internal table detail cache entry.
 */
export function createInternalTableDetailCacheEntry(
  request: TableDetailRequest,
  generation: number,
): import("../schemaCacheUtils").InternalTableDetailCacheEntry {
  return {
    ...createEmptyTableDetailState(request),
    generation,
    loading: null,
  };
}

/**
 * Gets the cache key for a table detail request.
 */
export function getTableDetailCacheKey(
  request: Omit<TableDetailRequest, "connectionId">,
): string {
  return [request.database, request.schema, request.objectKind, request.table]
    .map((part) => encodeURIComponent(part))
    .join(":");
}

// ─── Schema Merge Functions ─────────────────────────────────────────────────

/**
 * Merges a schema into a database entry.
 */
export function mergeSchemaIntoDatabase(
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

// ─── Config Functions ───────────────────────────────────────────────────────

/**
 * Gets the configured default database name for a connection.
 */
export function getConfiguredDefaultDatabaseName(
  config: ConnectionConfig,
): string {
  if (config.type === "elasticsearch") {
    return "default";
  }

  if (config.type === "dynamodb") {
    return config.awsRegion || "default";
  }

  if (config.type === "oracle") {
    return config.serviceName || config.database || "";
  }

  return (
    config.database || config.serviceName || (config.filePath ? "main" : "")
  );
}

// ─── Aggregate Functions ────────────────────────────────────────────────────

/**
 * Builds an aggregate schema snapshot from a connection schema cache entry.
 */
export function buildAggregateSchemaSnapshot(
  entry: import("../schemaCacheUtils").ConnectionSchemaCacheEntry,
): SchemaSnapshot {
  const rootEntry = entry.scopes.get(
    getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE),
  ) as import("../schemaCacheUtils").InternalScopedSchemaCacheEntry | undefined;
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

/**
 * Derives the aggregate schema state from a connection schema cache entry.
 */
export function deriveAggregateSchemaState(
  entry: import("../schemaCacheUtils").ConnectionSchemaCacheEntry,
  snapshot: SchemaSnapshot,
): Omit<SchemaSnapshotState, "snapshot"> {
  const rootEntry = entry.scopes.get(
    getExplorerSchemaScopeKey(CONNECTION_ROOT_SCOPE),
  ) as import("../schemaCacheUtils").InternalScopedSchemaCacheEntry | undefined;
  const baselineEntry = entry.defaultDatabaseName
    ? (entry.scopes.get(
        getExplorerSchemaScopeKey({
          kind: "database",
          database: entry.defaultDatabaseName,
        }),
      ) as
        | import("../schemaCacheUtils").InternalScopedSchemaCacheEntry
        | undefined)
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

/**
 * Flattens a schema snapshot into a flat array of schema object entries.
 */
export function flattenSchemaSnapshot(
  snapshot: SchemaSnapshot,
): SchemaObjectEntry[] {
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
