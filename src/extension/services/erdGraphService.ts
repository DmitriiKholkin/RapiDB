import type { Disposable } from "vscode";
import type { IndexMeta } from "../../shared/tableTypes";
import type {
  ErdGraph,
  ErdRelationshipEdge,
  ErdTableNode,
} from "../../shared/webviewContracts";
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
}

interface GraphSourceObject {
  database: string;
  schema: string;
  table: string;
  isView: boolean;
}

interface ScopeCacheWarmupApi {
  ensureSchemaScopeLoading(
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
    typeof connectionManager.ensureSchemaScopeLoading === "function" &&
    typeof connectionManager.getSchemaSnapshot === "function" &&
    typeof connectionManager.getSchemaSnapshotState === "function" &&
    typeof connectionManager.onDidChangeSchemaState === "function"
  );
}

export class ErdGraphService {
  private readonly cache = new Map<string, ErdGraph>();
  private readonly subscriptions: Disposable[];

  constructor(private readonly connectionManager: ConnectionManager) {
    this.subscriptions = [
      this.connectionManager.onDidDisconnect((connectionId) => {
        this.invalidateConnection(connectionId);
      }),
      this.connectionManager.onDidRefreshSchemas(() => {
        this.cache.clear();
      }),
      this.connectionManager.onDidChangeSchemaState((connectionId) => {
        this.invalidateConnection(connectionId);
      }),
    ];
  }

  dispose(): void {
    for (const subscription of this.subscriptions) {
      subscription.dispose();
    }
    this.subscriptions.length = 0;
    this.cache.clear();
  }

  async getGraph(
    request: ErdGraphRequest,
    forceReload = false,
  ): Promise<ErdGraphResult> {
    const normalized = this.normalizeRequest(request);
    const cacheKey = this.makeCacheKey(normalized);

    if (!forceReload) {
      const cached = this.cache.get(cacheKey);
      if (cached) {
        return { graph: cached, fromCache: true };
      }
    }

    const graph = await this.buildGraph(normalized);
    this.cache.set(cacheKey, graph);
    return { graph, fromCache: false };
  }

  private normalizeRequest(request: ErdGraphRequest): ErdGraphRequest {
    return {
      connectionId: request.connectionId,
      database: request.database?.trim() || undefined,
      schema: request.schema?.trim() || undefined,
    };
  }

  private makeCacheKey(request: ErdGraphRequest): string {
    return [
      request.connectionId,
      request.database ?? "*",
      request.schema ?? "*",
    ].join("::");
  }

  private invalidateConnection(connectionId: string): void {
    for (const key of this.cache.keys()) {
      if (key.startsWith(`${connectionId}::`)) {
        this.cache.delete(key);
      }
    }
  }

  private async buildGraph(request: ErdGraphRequest): Promise<ErdGraph> {
    const driver = this.connectionManager.getDriver(request.connectionId);
    if (!driver) {
      throw new Error("Not connected");
    }

    let snapshot = await this.connectionManager.getSchemaSnapshotAsync(
      request.connectionId,
    );

    snapshot = await this.warmRequestedScopeSnapshot(request, driver, snapshot);

    let objects = this.collectObjects(snapshot, request);
    if (objects.length === 0 && request.database) {
      objects = await this.discoverObjectsFromDriver(driver, request);
    }

    if (objects.length === 0) {
      return {
        nodes: [],
        edges: [],
        scope: {
          database: request.database,
          schema: request.schema,
        },
      };
    }

    const details = await pMapWithLimit(objects, 3, async (object) => {
      const [columns, foreignKeys, indexes] = await Promise.all([
        driver
          .describeColumns(object.database, object.schema, object.table)
          .catch(() => []),
        driver
          .getForeignKeys(object.database, object.schema, object.table)
          .catch(() => []),
        driver
          .getIndexes(object.database, object.schema, object.table)
          .catch(() => []),
      ]);

      return {
        object,
        columns,
        foreignKeys,
        indexes,
      };
    });

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
        isForeignKey: column.isForeignKey,
        nullable: column.nullable,
      })),
      position: {
        x: (index % columnCount) * 440,
        y: Math.floor(index / columnCount) * 280,
      },
    }));

    const nodeIds = new Set(nodes.map((node) => node.id));
    const edges = this.collectEdges(details, nodeIds).sort((left, right) =>
      left.id.localeCompare(right.id),
    );

    return {
      nodes,
      edges,
      scope: {
        database: request.database,
        schema: request.schema,
      },
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

    objects.sort((left, right) => {
      if (left.database !== right.database) {
        return left.database.localeCompare(right.database);
      }
      if (left.schema !== right.schema) {
        return left.schema.localeCompare(right.schema);
      }
      return left.table.localeCompare(right.table);
    });

    return objects;
  }

  private async discoverObjectsFromDriver(
    driver: IDBDriver,
    request: ErdGraphRequest,
  ): Promise<GraphSourceObject[]> {
    const database = request.database;
    if (!database) {
      return [];
    }

    const schemaNames = request.schema
      ? [request.schema]
      : [
          ...new Set(
            (await driver.listSchemas(database).catch(() => []))
              .map((schema) => schema.name?.trim())
              .filter((name): name is string => Boolean(name)),
          ),
        ];

    if (schemaNames.length === 0) {
      return [];
    }

    const discoveredBySchema = await pMapWithLimit(
      schemaNames,
      4,
      async (schemaName) => {
        const objects = await driver
          .listObjects(database, schemaName)
          .catch(() => []);
        return objects
          .filter((object) => object.type === "table")
          .map<GraphSourceObject>((object) => ({
            database,
            schema: schemaName,
            table: object.name,
            isView: false,
          }));
      },
    );

    return discoveredBySchema.flat().sort((left, right) => {
      if (left.database !== right.database) {
        return left.database.localeCompare(right.database);
      }
      if (left.schema !== right.schema) {
        return left.schema.localeCompare(right.schema);
      }
      return left.table.localeCompare(right.table);
    });
  }

  private async warmRequestedScopeSnapshot(
    request: ErdGraphRequest,
    driver: IDBDriver,
    snapshot: Awaited<ReturnType<ConnectionManager["getSchemaSnapshotAsync"]>>,
  ): Promise<Awaited<ReturnType<ConnectionManager["getSchemaSnapshotAsync"]>>> {
    if (
      !request.database ||
      !supportsScopeCacheWarmup(this.connectionManager)
    ) {
      return snapshot;
    }

    await this.ensureDatabaseScopeLoaded(
      request.connectionId,
      request.database,
    ).catch(() => undefined);

    snapshot = this.connectionManager.getSchemaSnapshot(request.connectionId);

    const schemaNames = await this.resolveTargetSchemaNames(
      snapshot,
      request,
      driver,
    );
    if (schemaNames.length === 0) {
      return snapshot;
    }

    await pMapWithLimit(schemaNames, 4, async (schemaName) => {
      await this.ensureSchemaScopeLoaded(
        request.connectionId,
        request.database as string,
        schemaName,
      ).catch(() => undefined);
    });

    return this.connectionManager.getSchemaSnapshot(request.connectionId);
  }

  private async ensureDatabaseScopeLoaded(
    connectionId: string,
    database: string,
  ): Promise<void> {
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

    this.connectionManager.ensureSchemaScopeLoading(connectionId, scope);

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

    await new Promise<void>((resolve, reject) => {
      let subscription: Disposable | undefined;
      const settle = (): void => {
        const nextState = this.connectionManager.getSchemaSnapshotState(
          connectionId,
          scope,
        );
        if (nextState.status === "loaded") {
          subscription?.dispose();
          resolve();
          return;
        }
        if (nextState.status === "error") {
          subscription?.dispose();
          reject(
            new Error(
              nextState.error ?? `Failed to load ${database} database scope`,
            ),
          );
        }
      };

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
        .filter((name) => name.trim().length > 0) ?? [];

    if (schemaNamesFromSnapshot.length > 0) {
      return [...new Set(schemaNamesFromSnapshot)];
    }

    return [
      ...new Set(
        (await driver.listSchemas(request.database).catch(() => []))
          .map((schema) => schema.name?.trim())
          .filter((name): name is string => Boolean(name)),
      ),
    ];
  }

  private async ensureSchemaScopeLoaded(
    connectionId: string,
    database: string,
    schema: string,
  ): Promise<void> {
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

    this.connectionManager.ensureSchemaScopeLoading(connectionId, scope);

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

    await new Promise<void>((resolve, reject) => {
      let subscription: Disposable | undefined;
      const settle = (): void => {
        const nextState = this.connectionManager.getSchemaSnapshotState(
          connectionId,
          scope,
        );
        if (nextState.status === "loaded") {
          subscription?.dispose();
          resolve();
          return;
        }
        if (nextState.status === "error") {
          subscription?.dispose();
          reject(
            new Error(
              nextState.error ?? `Failed to load ${database}.${schema} schema`,
            ),
          );
        }
      };

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
    details: Array<{
      object: GraphSourceObject;
      columns: Array<{ name: string; nullable: boolean }>;
      foreignKeys: ForeignKeyMeta[];
      indexes: IndexMeta[];
    }>,
    nodeIds: Set<string>,
  ): ErdRelationshipEdge[] {
    const seen = new Set<string>();
    const edges: ErdRelationshipEdge[] = [];

    for (const detail of details) {
      const fromTableId = this.tableId(
        detail.object.database,
        detail.object.schema,
        detail.object.table,
      );
      const nullableByColumn = new Map(
        detail.columns.map((column) => [column.name, column.nullable]),
      );
      const uniqueColumns = this.collectUniqueColumns(detail.indexes);

      for (const foreignKey of detail.foreignKeys) {
        const targetSchema =
          foreignKey.referencedSchema?.trim() || detail.object.schema;
        const toTableId = this.tableId(
          detail.object.database,
          targetSchema,
          foreignKey.referencedTable,
        );

        if (!nodeIds.has(toTableId)) {
          continue;
        }

        const edgeId = [
          fromTableId,
          toTableId,
          foreignKey.constraintName,
          foreignKey.column,
          foreignKey.referencedColumn,
        ].join("::");

        if (seen.has(edgeId)) {
          continue;
        }
        seen.add(edgeId);

        edges.push({
          id: edgeId,
          fromTableId,
          toTableId,
          fromColumn: foreignKey.column,
          toColumn: foreignKey.referencedColumn,
          constraintName: foreignKey.constraintName,
          cardinality: uniqueColumns.has(foreignKey.column)
            ? "one-to-one"
            : "many-to-one",
          sourceNullable: nullableByColumn.get(foreignKey.column) ?? false,
        });
      }
    }

    return edges;
  }

  private tableId(database: string, schema: string, table: string): string {
    return `${database}.${schema}.${table}`;
  }

  private collectUniqueColumns(indexes: IndexMeta[]): Set<string> {
    const columns = new Set<string>();

    for (const index of indexes) {
      if (!index.unique && !index.primary) {
        continue;
      }
      if (index.columns.length !== 1) {
        continue;
      }
      const column = index.columns[0];
      if (column) {
        columns.add(column);
      }
    }

    return columns;
  }
}
