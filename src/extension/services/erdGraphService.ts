import type { Disposable } from "vscode";
import type { IndexMeta } from "../../shared/tableTypes";
import type {
  ErdGraph,
  ErdRelationshipEdge,
  ErdTableNode,
} from "../../shared/webviewContracts";
import type {
  ErdExternalReference,
  ErdOmittedTable,
} from "../../shared/webviewContracts/erd";
import type { ConnectionManager } from "../connectionManager";
import type { ForeignKeyMeta, IDBDriver } from "../dbDrivers/types";
import { pMapWithLimit } from "../utils/concurrency";

export interface ErdGraphRequest {
  connectionId: string;
  database?: string;
  schema?: string;
}

interface ErdGraphResult {
  graph: ErdGraph;
  fromCache: boolean;
  partial: boolean;
}

type SchemaSnapshotResult = Awaited<
  ReturnType<ConnectionManager["getSchemaSnapshotAsync"]>
>;

interface GraphSourceObject {
  database: string;
  schema: string;
  table: string;
  isView: boolean;
}

interface GraphSourceDetail {
  object: GraphSourceObject;
  columns: Array<{ name: string; nullable: boolean }>;
  foreignKeys: ForeignKeyMeta[];
  indexes: IndexMeta[];
}

const ERD_SCOPE_WARMUP_TIMEOUT_MS = 4000;

function foldAsciiCase(value: string): string {
  return value.replace(/[A-Z]/g, (char) => char.toLowerCase());
}

function isAbortError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { name?: unknown }).name === "AbortError"
  );
}

type GraphObjectName = Pick<GraphSourceObject, "database" | "schema" | "table">;

function compareGraphSourceObjects(
  left: GraphObjectName,
  right: GraphObjectName,
): number {
  if (left.database !== right.database) {
    return left.database.localeCompare(right.database);
  }
  if (left.schema !== right.schema) {
    return left.schema.localeCompare(right.schema);
  }
  return left.table.localeCompare(right.table);
}

interface ScopeCacheWarmupApi {
  warmSchemaScope(
    connectionId: string,
    scope:
      | { kind: "database"; database: string }
      | { kind: "schema"; database: string; schema: string },
  ): void;
  getSchemaSnapshot(
    connectionId: string,
  ): Awaited<ReturnType<ConnectionManager["getSchemaSnapshotAsync"]>>;
  getSchemaSnapshotState(
    connectionId: string,
    scope:
      | { kind: "database"; database: string }
      | { kind: "schema"; database: string; schema: string },
  ): {
    status: "idle" | "loading" | "loaded" | "error";
    error?: string;
  };
  onDidChangeSchemaState(listener: (connectionId: string) => void): Disposable;
}

function supportsScopeCacheWarmup(
  connectionManager: ConnectionManager,
): connectionManager is ConnectionManager & ScopeCacheWarmupApi {
  return (
    typeof connectionManager.warmSchemaScope === "function" &&
    typeof connectionManager.getSchemaSnapshot === "function" &&
    typeof connectionManager.getSchemaSnapshotState === "function" &&
    typeof connectionManager.onDidChangeSchemaState === "function"
  );
}

export class ErdGraphService {
  private readonly cache = new Map<
    string,
    { graph: ErdGraph; partial: boolean }
  >();
  private readonly connectionGenerations = new Map<string, number>();
  private readonly connectionLifecycleGenerations = new Map<string, number>();
  private readonly connectionInvalidationGenerations = new Map<
    string,
    number
  >();
  private readonly requestGenerations = new Map<string, number>();
  private schemaRefreshGeneration = 0;
  private readonly subscriptions: Disposable[];
  private readonly lifecycleAbortController = new AbortController();

  constructor(private readonly connectionManager: ConnectionManager) {
    this.subscriptions = [
      this.connectionManager.onDidDisconnect((connectionId) => {
        this.bumpConnectionGeneration(connectionId);
        this.bumpConnectionLifecycleGeneration(connectionId);
        this.bumpConnectionInvalidationGeneration(connectionId);
        this.invalidateConnection(connectionId);
      }),
      this.connectionManager.onDidRefreshSchemas((connectionId) => {
        if (connectionId) {
          this.bumpConnectionGeneration(connectionId);
          this.bumpConnectionLifecycleGeneration(connectionId);
          this.invalidateConnection(connectionId);
        } else {
          this.schemaRefreshGeneration += 1;
          this.cache.clear();
        }
      }),
    ];
    const metadataInvalidation =
      this.connectionManager.onDidInvalidateConnectionMetadata?.(
        (connectionId) => {
          this.bumpConnectionGeneration(connectionId);
          this.bumpConnectionLifecycleGeneration(connectionId);
          this.bumpConnectionInvalidationGeneration(connectionId);
          this.invalidateConnection(connectionId);
        },
      );
    if (metadataInvalidation) this.subscriptions.push(metadataInvalidation);
  }

  dispose(): void {
    this.lifecycleAbortController.abort();
    for (const subscription of this.subscriptions) {
      subscription.dispose();
    }
    this.subscriptions.length = 0;
    this.cache.clear();
    this.connectionGenerations.clear();
    this.connectionLifecycleGenerations.clear();
    this.connectionInvalidationGenerations.clear();
    this.requestGenerations.clear();
  }

  async getGraph(
    request: ErdGraphRequest,
    forceReload = false,
    signal?: AbortSignal,
    retryAfterSchemaRefresh = true,
  ): Promise<ErdGraphResult> {
    signal?.throwIfAborted();
    const normalized = this.normalizeRequest(request);
    const cacheKey = this.makeCacheKey(normalized);
    if (
      this.connectionManager.isConnecting?.(normalized.connectionId) ||
      this.connectionManager.isConnected?.(normalized.connectionId) === false
    ) {
      throw new Error("Not connected");
    }

    if (!forceReload) {
      const cached = this.cache.get(cacheKey);
      if (cached) {
        return {
          graph: cached.graph,
          fromCache: true,
          partial: cached.partial,
        };
      }
    } else {
      this.connectionManager.refreshSchemaCache?.({
        connectionId: normalized.connectionId,
        reason: "manual",
      });
    }
    const requestGeneration = (this.requestGenerations.get(cacheKey) ?? 0) + 1;
    this.requestGenerations.set(cacheKey, requestGeneration);

    const connectionLifecycleGeneration =
      this.connectionLifecycleGenerations.get(normalized.connectionId) ?? 0;
    const schemaRefreshGeneration = this.schemaRefreshGeneration;
    const invalidationGeneration =
      this.connectionInvalidationGenerations.get(normalized.connectionId) ?? 0;
    const driver = this.connectionManager.getDriver(normalized.connectionId);
    if (!driver) {
      throw new Error("Not connected");
    }
    let snapshot = await this.connectionManager.getSchemaSnapshotAsync(
      normalized.connectionId,
    );
    signal?.throwIfAborted();
    const warmup = await this.warmRequestedScopeSnapshot(
      normalized,
      driver,
      snapshot,
      signal,
    );
    signal?.throwIfAborted();
    snapshot = warmup.snapshot;
    const generation =
      this.connectionGenerations.get(normalized.connectionId) ?? 0;
    const built = await this.buildGraph(
      normalized,
      driver,
      snapshot,
      warmup.unloadedSchemas,
      signal,
    );
    signal?.throwIfAborted();
    const metadataIncomplete = built.partial || warmup.partial;
    const graph: ErdGraph = metadataIncomplete
      ? { ...built.graph, metadataIncomplete: true }
      : built.graph;
    const partial =
      metadataIncomplete || (graph.omittedTables?.length ?? 0) > 0;
    if (
      generation !==
        (this.connectionGenerations.get(normalized.connectionId) ?? 0) ||
      connectionLifecycleGeneration !==
        (this.connectionLifecycleGenerations.get(normalized.connectionId) ??
          0) ||
      schemaRefreshGeneration !== this.schemaRefreshGeneration ||
      requestGeneration !== this.requestGenerations.get(cacheKey)
    ) {
      if (
        retryAfterSchemaRefresh &&
        !this.lifecycleAbortController.signal.aborted &&
        requestGeneration === this.requestGenerations.get(cacheKey) &&
        invalidationGeneration ===
          (this.connectionInvalidationGenerations.get(
            normalized.connectionId,
          ) ?? 0)
      ) {
        return await this.getGraph(request, false, signal, false);
      }
      throw new Error("Connection changed while the ERD was loading.");
    }
    if (metadataIncomplete) {
      this.cache.delete(cacheKey);
    } else {
      this.cache.set(cacheKey, { graph, partial });
    }
    return { graph, fromCache: false, partial };
  }

  private normalizeRequest(request: ErdGraphRequest): ErdGraphRequest {
    return {
      connectionId: request.connectionId,
      database: request.database || undefined,
      schema: request.schema || undefined,
    };
  }

  private makeCacheKey(request: ErdGraphRequest): string {
    return JSON.stringify([
      request.connectionId,
      request.database ?? null,
      request.schema ?? null,
    ]);
  }

  private invalidateConnection(connectionId: string): void {
    for (const key of this.cache.keys()) {
      const parsed = JSON.parse(key) as [string, string | null, string | null];
      if (parsed[0] === connectionId) {
        this.cache.delete(key);
      }
    }
  }

  private bumpConnectionGeneration(connectionId: string): void {
    this.connectionGenerations.set(
      connectionId,
      (this.connectionGenerations.get(connectionId) ?? 0) + 1,
    );
  }

  private bumpConnectionInvalidationGeneration(connectionId: string): void {
    this.connectionInvalidationGenerations.set(
      connectionId,
      (this.connectionInvalidationGenerations.get(connectionId) ?? 0) + 1,
    );
  }

  private bumpConnectionLifecycleGeneration(connectionId: string): void {
    this.connectionLifecycleGenerations.set(
      connectionId,
      (this.connectionLifecycleGenerations.get(connectionId) ?? 0) + 1,
    );
  }

  private async buildGraph(
    request: ErdGraphRequest,
    driver: IDBDriver,
    snapshot: SchemaSnapshotResult,
    unloadedSchemas: string[],
    signal?: AbortSignal,
  ): Promise<{ graph: ErdGraph; partial: boolean }> {
    let objects = this.collectObjects(snapshot, request);
    let partial = false;
    if (objects.length === 0 && request.database) {
      const discovered = await this.discoverObjectsFromDriver(
        driver,
        request,
        undefined,
        signal,
      );
      objects = discovered.objects;
      partial = !discovered.complete;
    } else if (unloadedSchemas.length > 0) {
      const discovered = await this.discoverObjectsFromDriver(
        driver,
        request,
        unloadedSchemas,
        signal,
      );
      const retained = objects.filter(
        (object) => !discovered.listedSchemas.has(object.schema),
      );
      const seenIds = new Set(
        retained.map((object) =>
          this.tableId(object.database, object.schema, object.table),
        ),
      );
      objects = [
        ...retained,
        ...discovered.objects.filter(
          (object) =>
            !seenIds.has(
              this.tableId(object.database, object.schema, object.table),
            ),
        ),
      ].sort(compareGraphSourceObjects);
      partial = !discovered.complete;
    }

    if (objects.length === 0) {
      return {
        graph: {
          nodes: [],
          edges: [],
          scope: {
            database: request.database,
            schema: request.schema,
          },
        },
        partial,
      };
    }

    signal?.throwIfAborted();
    const failures: Array<{ object: GraphSourceObject; error: unknown }> = [];
    const loaded = await pMapWithLimit(
      objects,
      3,
      async (object) => {
        try {
          const [columns, foreignKeys, indexes] = await Promise.all([
            driver.describeColumns(
              object.database,
              object.schema,
              object.table,
            ),
            driver.getForeignKeys(object.database, object.schema, object.table),
            driver.getIndexes(object.database, object.schema, object.table),
          ]);

          return {
            object,
            columns,
            foreignKeys,
            indexes,
          };
        } catch (error) {
          if (
            signal?.aborted ||
            this.lifecycleAbortController.signal.aborted ||
            isAbortError(error)
          ) {
            throw error;
          }
          failures.push({ object, error });
          return null;
        }
      },
      signal,
    );
    signal?.throwIfAborted();
    const details = loaded.filter(
      (detail): detail is NonNullable<typeof detail> => detail !== null,
    );
    if (details.length === 0 && failures.length > 0) {
      throw failures[0].error;
    }
    const omittedTables: ErdOmittedTable[] = failures
      .map(({ object, error }) => ({
        database: object.database,
        schema: object.schema,
        table: object.table,
        error: error instanceof Error ? error.message : String(error),
      }))
      .sort(compareGraphSourceObjects);

    const columnCount = Math.max(1, Math.ceil(Math.sqrt(details.length)));
    const nodes: ErdTableNode[] = details.map((detail, index) => ({
      id: this.tableId(
        detail.object.database,
        detail.object.schema,
        detail.object.table,
      ),
      database: detail.object.database,
      schema: detail.object.schema,
      table: detail.object.table,
      isView: detail.object.isView,
      columns: detail.columns.map((column) => ({
        name: column.name,
        type: column.nativeType,
        isPrimaryKey: column.isPrimaryKey,
        primaryKeyRole: column.primaryKeyRole,
        isForeignKey: column.isForeignKey,
        nullable: column.nullable,
      })),
      position: {
        x: (index % columnCount) * 440,
        y: Math.floor(index / columnCount) * 280,
      },
    }));

    const connectionType = this.connectionManager.getConnection?.(
      request.connectionId,
    )?.type;
    const { edges, externalReferences } = this.collectEdges(
      details,
      omittedTables,
      connectionType === "sqlite",
      connectionType === "mysql",
    );
    edges.sort((left, right) => left.id.localeCompare(right.id));

    return {
      graph: {
        nodes,
        edges,
        scope: {
          database: request.database,
          schema: request.schema,
        },
        ...(omittedTables.length > 0 ? { omittedTables } : {}),
        ...(externalReferences.length > 0 ? { externalReferences } : {}),
      },
      partial,
    };
  }

  private collectObjects(
    snapshot: Awaited<ReturnType<ConnectionManager["getSchemaSnapshotAsync"]>>,
    request: ErdGraphRequest,
  ): GraphSourceObject[] {
    const objects: GraphSourceObject[] = [];

    for (const databaseEntry of snapshot.databases) {
      if (request.database && databaseEntry.name !== request.database) {
        continue;
      }

      for (const schemaEntry of databaseEntry.schemas) {
        if (request.schema && schemaEntry.name !== request.schema) {
          continue;
        }

        for (const objectEntry of schemaEntry.objects) {
          if (objectEntry.type !== "table") {
            continue;
          }
          objects.push({
            database: databaseEntry.name,
            schema: schemaEntry.name,
            table: objectEntry.name,
            isView: false,
          });
        }
      }
    }

    objects.sort(compareGraphSourceObjects);

    return objects;
  }

  private async discoverObjectsFromDriver(
    driver: IDBDriver,
    request: ErdGraphRequest,
    schemaNamesOverride?: string[],
    signal?: AbortSignal,
  ): Promise<{
    objects: GraphSourceObject[];
    complete: boolean;
    listedSchemas: Set<string>;
  }> {
    const listedSchemas = new Set<string>();
    signal?.throwIfAborted();
    if (this.lifecycleAbortController.signal.aborted) {
      return { objects: [], complete: false, listedSchemas };
    }
    const database = request.database;
    if (!database) {
      return { objects: [], complete: true, listedSchemas };
    }

    let complete = true;
    const schemaNames =
      schemaNamesOverride ??
      (request.schema
        ? [request.schema]
        : [
            ...new Set(
              (
                await driver.listSchemas(database).catch(() => {
                  complete = false;
                  return [];
                })
              )
                .map((schema) => schema.name)
                .filter((name): name is string => Boolean(name)),
            ),
          ]);

    signal?.throwIfAborted();
    if (schemaNames.length === 0) {
      return { objects: [], complete, listedSchemas };
    }

    const discoveredBySchema = await pMapWithLimit(
      schemaNames,
      4,
      async (schemaName) => {
        const objects = await driver.listObjects(database, schemaName).then(
          (listed) => {
            listedSchemas.add(schemaName);
            return listed;
          },
          () => {
            complete = false;
            return [];
          },
        );
        return objects
          .filter((object) => object.type === "table")
          .map<GraphSourceObject>((object) => ({
            database,
            schema: schemaName,
            table: object.name,
            isView: false,
          }));
      },
      signal,
    );
    signal?.throwIfAborted();

    return {
      objects: discoveredBySchema.flat().sort(compareGraphSourceObjects),
      complete,
      listedSchemas,
    };
  }

  private async warmRequestedScopeSnapshot(
    request: ErdGraphRequest,
    driver: IDBDriver,
    snapshot: SchemaSnapshotResult,
    signal?: AbortSignal,
  ): Promise<{
    snapshot: SchemaSnapshotResult;
    unloadedSchemas: string[];
    partial: boolean;
  }> {
    if (
      !request.database ||
      !supportsScopeCacheWarmup(this.connectionManager)
    ) {
      return { snapshot, unloadedSchemas: [], partial: false };
    }
    const connectionManager = this.connectionManager;
    const database = request.database;

    const databaseScopeFailed = await this.ensureDatabaseScopeLoaded(
      request.connectionId,
      database,
      signal,
    ).then(
      () => false,
      () => true,
    );
    signal?.throwIfAborted();

    snapshot = connectionManager.getSchemaSnapshot(request.connectionId);

    let partial = false;
    const schemaNames = await this.resolveTargetSchemaNames(
      snapshot,
      request,
      driver,
      databaseScopeFailed,
    ).catch(() => {
      partial = true;
      return [];
    });
    signal?.throwIfAborted();
    if (schemaNames.length === 0) {
      return { snapshot, unloadedSchemas: [], partial };
    }

    const unloadedSchemas: string[] = [];
    await pMapWithLimit(
      schemaNames,
      4,
      async (schemaName) => {
        const loaded = await this.ensureSchemaScopeLoaded(
          request.connectionId,
          database,
          schemaName,
          signal,
        ).then(
          () => true,
          () => false,
        );
        if (
          !loaded ||
          connectionManager.getSchemaSnapshotState(request.connectionId, {
            kind: "schema",
            database,
            schema: schemaName,
          }).status !== "loaded"
        ) {
          unloadedSchemas.push(schemaName);
        }
      },
      signal,
    );
    signal?.throwIfAborted();

    return {
      snapshot: connectionManager.getSchemaSnapshot(request.connectionId),
      unloadedSchemas,
      partial,
    };
  }

  private async ensureDatabaseScopeLoaded(
    connectionId: string,
    database: string,
    signal?: AbortSignal,
  ): Promise<void> {
    signal?.throwIfAborted();
    if (!supportsScopeCacheWarmup(this.connectionManager)) {
      return;
    }

    const scope = {
      kind: "database" as const,
      database,
    };
    const currentState = this.connectionManager.getSchemaSnapshotState(
      connectionId,
      scope,
    );
    if (currentState.status === "loaded") {
      return;
    }
    if (currentState.status === "error") {
      throw new Error(
        currentState.error ?? `Failed to load ${database} database scope`,
      );
    }

    this.connectionManager.warmSchemaScope(connectionId, scope);

    const loadingState = this.connectionManager.getSchemaSnapshotState(
      connectionId,
      scope,
    );
    if (loadingState.status === "loaded") {
      return;
    }
    if (loadingState.status === "error") {
      throw new Error(
        loadingState.error ?? `Failed to load ${database} database scope`,
      );
    }
    if (loadingState.status === "idle") {
      return;
    }

    const abortSignal = signal
      ? AbortSignal.any([signal, this.lifecycleAbortController.signal])
      : this.lifecycleAbortController.signal;
    if (abortSignal.aborted) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let subscription: Disposable | undefined;
      let abortListener: (() => void) | undefined;
      const timer = setTimeout(() => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        reject(new Error(`Timed out loading ${database} database scope`));
      }, ERD_SCOPE_WARMUP_TIMEOUT_MS);

      const cleanup = (): void => {
        clearTimeout(timer);
        subscription?.dispose();
        if (abortListener) {
          abortSignal.removeEventListener("abort", abortListener);
        }
      };

      const settle = (): void => {
        if (settled || abortSignal.aborted) {
          return;
        }

        const activeDriver = this.connectionManager.getDriver(connectionId);
        if (!activeDriver?.isConnected()) {
          settled = true;
          cleanup();
          resolve();
          return;
        }

        const nextState = this.connectionManager.getSchemaSnapshotState(
          connectionId,
          scope,
        );
        if (nextState.status === "loaded") {
          settled = true;
          cleanup();
          resolve();
          return;
        }
        if (nextState.status === "error") {
          settled = true;
          cleanup();
          reject(
            new Error(
              nextState.error ?? `Failed to load ${database} database scope`,
            ),
          );
          return;
        }
        if (nextState.status === "idle") {
          settled = true;
          cleanup();
          resolve();
        }
      };

      abortListener = (): void => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        resolve();
      };

      abortSignal.addEventListener("abort", abortListener, { once: true });

      subscription = this.connectionManager.onDidChangeSchemaState(
        (changedConnectionId) => {
          if (changedConnectionId !== connectionId) {
            return;
          }

          settle();
        },
      );

      settle();
    });
  }

  private async resolveTargetSchemaNames(
    snapshot: Awaited<ReturnType<ConnectionManager["getSchemaSnapshotAsync"]>>,
    request: ErdGraphRequest,
    driver: IDBDriver,
    includeDriverSchemas: boolean,
  ): Promise<string[]> {
    if (!request.database) {
      return [];
    }

    if (request.schema) {
      return [request.schema];
    }

    const schemaNamesFromSnapshot =
      snapshot.databases
        .find((database) => database.name === request.database)
        ?.schemas.map((schema) => schema.name)
        .filter((name) => name.length > 0) ?? [];

    if (schemaNamesFromSnapshot.length > 0 && !includeDriverSchemas) {
      return [...new Set(schemaNamesFromSnapshot)];
    }

    return [
      ...new Set([
        ...schemaNamesFromSnapshot,
        ...(await driver.listSchemas(request.database))
          .map((schema) => schema.name)
          .filter((name): name is string => Boolean(name)),
      ]),
    ];
  }

  private async ensureSchemaScopeLoaded(
    connectionId: string,
    database: string,
    schema: string,
    signal?: AbortSignal,
  ): Promise<void> {
    signal?.throwIfAborted();
    if (!supportsScopeCacheWarmup(this.connectionManager)) {
      return;
    }

    const scope = {
      kind: "schema" as const,
      database,
      schema,
    };
    const currentState = this.connectionManager.getSchemaSnapshotState(
      connectionId,
      scope,
    );
    if (currentState.status === "loaded") {
      return;
    }
    if (currentState.status === "error") {
      throw new Error(
        currentState.error ?? `Failed to load ${database}.${schema} schema`,
      );
    }

    this.connectionManager.warmSchemaScope(connectionId, scope);

    const loadingState = this.connectionManager.getSchemaSnapshotState(
      connectionId,
      scope,
    );
    if (loadingState.status === "loaded") {
      return;
    }
    if (loadingState.status === "error") {
      throw new Error(
        loadingState.error ?? `Failed to load ${database}.${schema} schema`,
      );
    }
    if (loadingState.status === "idle") {
      return;
    }

    const abortSignal = signal
      ? AbortSignal.any([signal, this.lifecycleAbortController.signal])
      : this.lifecycleAbortController.signal;
    if (abortSignal.aborted) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let subscription: Disposable | undefined;
      let abortListener: (() => void) | undefined;
      const timer = setTimeout(() => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        reject(new Error(`Timed out loading ${database}.${schema} schema`));
      }, ERD_SCOPE_WARMUP_TIMEOUT_MS);

      const cleanup = (): void => {
        clearTimeout(timer);
        subscription?.dispose();
        if (abortListener) {
          abortSignal.removeEventListener("abort", abortListener);
        }
      };

      const settle = (): void => {
        if (settled || abortSignal.aborted) {
          return;
        }

        const activeDriver = this.connectionManager.getDriver(connectionId);
        if (!activeDriver?.isConnected()) {
          settled = true;
          cleanup();
          resolve();
          return;
        }

        const nextState = this.connectionManager.getSchemaSnapshotState(
          connectionId,
          scope,
        );
        if (nextState.status === "loaded") {
          settled = true;
          cleanup();
          resolve();
          return;
        }
        if (nextState.status === "error") {
          settled = true;
          cleanup();
          reject(
            new Error(
              nextState.error ?? `Failed to load ${database}.${schema} schema`,
            ),
          );
          return;
        }
        if (nextState.status === "idle") {
          settled = true;
          cleanup();
          resolve();
        }
      };

      abortListener = (): void => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        resolve();
      };

      abortSignal.addEventListener("abort", abortListener, { once: true });

      subscription = this.connectionManager.onDidChangeSchemaState(
        (changedConnectionId) => {
          if (changedConnectionId !== connectionId) {
            return;
          }

          settle();
        },
      );

      settle();
    });
  }

  private collectEdges(
    details: GraphSourceDetail[],
    omittedTables: GraphObjectName[],
    caseInsensitiveIdentifiers: boolean,
    schemaIsDatabase: boolean,
  ): {
    edges: ErdRelationshipEdge[];
    externalReferences: ErdExternalReference[];
  } {
    const seen = new Set<string>();
    const edges: ErdRelationshipEdge[] = [];
    const externalReferences: ErdExternalReference[] = [];
    const detailsById = new Map<string, GraphSourceDetail>();
    const foldedTableIds = new Map<string, string[]>();
    for (const detail of details) {
      const id = this.tableId(
        detail.object.database,
        detail.object.schema,
        detail.object.table,
      );
      detailsById.set(id, detail);
      const foldedId = this.tableId(
        detail.object.database,
        detail.object.schema,
        foldAsciiCase(detail.object.table),
      );
      foldedTableIds.set(foldedId, [
        ...(foldedTableIds.get(foldedId) ?? []),
        id,
      ]);
    }
    const resolveTableId = (
      database: string,
      schema: string,
      table: string,
    ): string | undefined => {
      const exactId = this.tableId(database, schema, table);
      if (detailsById.has(exactId)) {
        return exactId;
      }
      if (!caseInsensitiveIdentifiers) {
        return undefined;
      }
      const candidates = foldedTableIds.get(
        this.tableId(database, schema, foldAsciiCase(table)),
      );
      return candidates?.length === 1 ? candidates[0] : undefined;
    };
    const resolveColumn = (
      detail: GraphSourceDetail,
      column: string,
    ): string => {
      if (
        !caseInsensitiveIdentifiers ||
        detail.columns.some((candidate) => candidate.name === column)
      ) {
        return column;
      }
      const foldedColumn = foldAsciiCase(column);
      const candidates = detail.columns.filter(
        (candidate) => foldAsciiCase(candidate.name) === foldedColumn,
      );
      return candidates.length === 1 ? candidates[0].name : column;
    };
    const foldTableName = (table: string): string =>
      caseInsensitiveIdentifiers ? foldAsciiCase(table) : table;
    const omittedTableIds = new Set(
      omittedTables.map((object) =>
        this.tableId(
          object.database,
          object.schema,
          foldTableName(object.table),
        ),
      ),
    );
    const externalSeen = new Set<string>();
    const externalByConstraint = new Map<string, ErdExternalReference>();

    for (const detail of details) {
      const fromTableId = this.tableId(
        detail.object.database,
        detail.object.schema,
        detail.object.table,
      );
      const nullableByColumn = new Map(
        detail.columns.map((column) => [column.name, column.nullable]),
      );
      const uniqueColumnSets = this.collectUniqueColumnSets(detail.indexes);
      const columnsByConstraint = new Map<string, Set<string>>();
      for (const foreignKey of detail.foreignKeys) {
        const constraintKey = this.foreignKeyConstraintKey(
          foreignKey,
          detail.object.schema,
        );
        const constraintColumns =
          columnsByConstraint.get(constraintKey) ?? new Set<string>();
        constraintColumns.add(foreignKey.column);
        columnsByConstraint.set(constraintKey, constraintColumns);
      }

      for (const foreignKey of detail.foreignKeys) {
        const targetSchema =
          foreignKey.referencedSchema || detail.object.schema;
        const targetDatabase =
          schemaIsDatabase && foreignKey.referencedSchema
            ? foreignKey.referencedSchema
            : detail.object.database;
        const toTableId = resolveTableId(
          targetDatabase,
          targetSchema,
          foreignKey.referencedTable,
        );
        const toDetail = toTableId ? detailsById.get(toTableId) : undefined;

        if (!toTableId || !toDetail) {
          const targetId = this.tableId(
            targetDatabase,
            targetSchema,
            foldTableName(foreignKey.referencedTable),
          );
          if (omittedTableIds.has(targetId)) {
            continue;
          }
          const groupKey = JSON.stringify([
            fromTableId,
            foreignKey.constraintName,
            targetDatabase,
            targetSchema,
            foreignKey.referencedTable,
            foreignKey.constraintName ? null : foreignKey.column,
          ]);
          const columnKey = JSON.stringify([
            groupKey,
            foreignKey.column,
            foreignKey.referencedColumn,
          ]);
          if (externalSeen.has(columnKey)) {
            continue;
          }
          externalSeen.add(columnKey);
          const existing = externalByConstraint.get(groupKey);
          if (existing) {
            existing.fromColumns.push(foreignKey.column);
            existing.columns.push(foreignKey.referencedColumn);
            continue;
          }
          const reference: ErdExternalReference = {
            fromTableId,
            fromColumns: [foreignKey.column],
            constraintName: foreignKey.constraintName,
            database: targetDatabase,
            schema: targetSchema,
            table: foreignKey.referencedTable,
            columns: [foreignKey.referencedColumn],
          };
          externalByConstraint.set(groupKey, reference);
          externalReferences.push(reference);
          continue;
        }
        const toColumn = resolveColumn(toDetail, foreignKey.referencedColumn);

        const edgeId = JSON.stringify([
          fromTableId,
          toTableId,
          foreignKey.constraintName,
          foreignKey.column,
          toColumn,
        ]);

        if (seen.has(edgeId)) {
          continue;
        }
        seen.add(edgeId);

        const constraintColumns = columnsByConstraint.get(
          this.foreignKeyConstraintKey(foreignKey, detail.object.schema),
        );
        edges.push({
          id: edgeId,
          fromTableId,
          toTableId,
          fromColumn: foreignKey.column,
          toColumn,
          constraintName: foreignKey.constraintName,
          cardinality: this.isUniqueColumnSet(
            constraintColumns,
            uniqueColumnSets,
          )
            ? "one-to-one"
            : "many-to-one",
          sourceNullable: [...(constraintColumns ?? [foreignKey.column])].some(
            (column) => nullableByColumn.get(column) ?? false,
          ),
        });
      }
    }

    return { edges, externalReferences };
  }

  private tableId(database: string, schema: string, table: string): string {
    return JSON.stringify([database, schema, table]);
  }

  private foreignKeyConstraintKey(
    foreignKey: ForeignKeyMeta,
    sourceSchema: string,
  ): string {
    return JSON.stringify([
      foreignKey.constraintName,
      foreignKey.referencedSchema || sourceSchema,
      foreignKey.referencedTable,
      foreignKey.constraintName ? null : foreignKey.column,
    ]);
  }

  private isUniqueColumnSet(
    columns: Set<string> | undefined,
    uniqueColumnSets: string[][],
  ): boolean {
    if (!columns || columns.size === 0) {
      return false;
    }
    return uniqueColumnSets.some((uniqueColumns) =>
      uniqueColumns.every((column) => columns.has(column)),
    );
  }

  private collectUniqueColumnSets(indexes: IndexMeta[]): string[][] {
    const columnSets: string[][] = [];

    for (const index of indexes) {
      if ((!index.unique && !index.primary) || index.partial) {
        continue;
      }
      if (
        index.columns.length === 0 ||
        index.columns.some((column) => !column)
      ) {
        continue;
      }
      columnSets.push(index.columns);
    }

    return columnSets;
  }
}
