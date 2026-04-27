import { randomUUID } from "node:crypto";
import * as vscode from "vscode";
import {
  type BookmarkEntry,
  type ConnectAttempt,
  type ConnectionConfig,
  type HistoryEntry,
  type SchemaObjectEntry,
  type SchemaSnapshot,
  type SchemaSnapshotDatabaseEntry,
  type SchemaSnapshotObjectEntry,
  type StoredConnectionConfig,
  type TestConnectionResult,
} from "./connectionManagerModels";
import {
  type ConnectionManagerStore,
  VSCodeConnectionManagerStore,
} from "./connectionManagerStore";
import { MSSQLDriver } from "./dbDrivers/mssql";
import { MySQLDriver } from "./dbDrivers/mysql";
import { OracleDriver } from "./dbDrivers/oracle";
import { PostgresDriver } from "./dbDrivers/postgres";
import { SQLiteDriver } from "./dbDrivers/sqlite";
import type { IDBDriver } from "./dbDrivers/types";
import { pMapWithLimit } from "./utils/concurrency";
import { normalizeUnknownError } from "./utils/errorHandling";

export type {
  BookmarkEntry,
  ConnectAttempt,
  ConnectionConfig,
  HistoryEntry,
  SchemaObjectEntry,
  SchemaSnapshot,
  SchemaSnapshotDatabaseEntry,
  SchemaSnapshotSchemaEntry,
  TestConnectionResult,
} from "./connectionManagerModels";

interface SchemaCacheEntry {
  snapshot: SchemaSnapshot;
  loading: Promise<void> | null;
}

function createEmptySchemaSnapshot(): SchemaSnapshot {
  return { databases: [] };
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
export class ConnectionManager {
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
  readonly onDidRefreshSchemas: vscode.Event<void>;
  private readonly _onDidRefreshSchemas = new vscode.EventEmitter<void>();
  private _connectionsCache: ConnectionConfig[] | null = null;
  private readonly _schemaCacheMap = new Map<string, SchemaCacheEntry>();
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
  async _hydratePassword(config: ConnectionConfig): Promise<ConnectionConfig> {
    if (!config.useSecretStorage) {
      return config;
    }
    try {
      const stored = await this.store.getSecret(config.id);
      return { ...config, password: stored ?? "" };
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
    switch (config.type) {
      case "mysql":
        return new MySQLDriver(config);
      case "pg":
        return new PostgresDriver(config);
      case "sqlite":
        return new SQLiteDriver(config);
      case "mssql":
        return new MSSQLDriver(config);
      case "oracle":
        return new OracleDriver(config);
      default: {
        const unknownType: never = config.type;
        throw new Error(`[RapiDB] Unknown driver type: ${unknownType}`);
      }
    }
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
        this._schemaCacheMap.delete(id);
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
      this._schemaCacheMap.delete(id);
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
  getSchema(connectionId: string): SchemaObjectEntry[] {
    return flattenSchemaSnapshot(this.getSchemaSnapshot(connectionId));
  }

  getSchemaSnapshot(connectionId: string): SchemaSnapshot {
    return (
      this._schemaCacheMap.get(connectionId)?.snapshot ??
      createEmptySchemaSnapshot()
    );
  }

  refreshSchemaCache(connectionId?: string): void {
    if (connectionId) {
      this._schemaCacheMap.delete(connectionId);
    } else {
      this._schemaCacheMap.clear();
    }
    this._onDidRefreshSchemas.fire();
  }

  async getSchemaAsync(connectionId: string): Promise<SchemaObjectEntry[]> {
    const snapshot = await this.getSchemaSnapshotAsync(connectionId);
    return flattenSchemaSnapshot(snapshot);
  }

  async getSchemaSnapshotAsync(connectionId: string): Promise<SchemaSnapshot> {
    const existing = this._schemaCacheMap.get(connectionId);
    if (
      !existing ||
      (!existing.loading && existing.snapshot.databases.length === 0)
    ) {
      this._startSchemaLoad(connectionId);
    }
    const entry = this._schemaCacheMap.get(connectionId);
    if (entry?.loading) {
      try {
        await entry.loading;
      } catch {}
    }
    return this.getSchemaSnapshot(connectionId);
  }
  private _startSchemaLoad(connectionId: string): void {
    const driver = this.getDriver(connectionId);
    const config = this.getConnection(connectionId);
    if (!driver || !config) {
      return;
    }
    let entry = this._schemaCacheMap.get(connectionId);
    if (!entry) {
      entry = { snapshot: createEmptySchemaSnapshot(), loading: null };
      this._schemaCacheMap.set(connectionId, entry);
    }
    if (entry.loading) {
      return;
    }
    const capturedEntry = entry;
    capturedEntry.loading = this._loadSchemaInternal(driver, config)
      .then((snapshot) => {
        const live = this._schemaCacheMap.get(connectionId);
        if (live === capturedEntry) {
          live.snapshot = snapshot;
          live.loading = null;
        }
        this._onDidSchemaLoad.fire(connectionId);
      })
      .catch(() => {
        const live = this._schemaCacheMap.get(connectionId);
        if (live === capturedEntry) {
          live.loading = null;
        }
      });
  }
  private async _loadSchemaInternal(
    driver: IDBDriver,
    config: ConnectionConfig,
  ): Promise<SchemaSnapshot> {
    const configuredDb =
      config.database || config.serviceName || (config.filePath ? "main" : "");
    const allDbs = await driver.listDatabases().catch(() => []);
    const databaseInfoMap = new Map(
      allDbs.map((database) => [database.name, database] as const),
    );
    const databaseNames = [
      ...new Set(
        [configuredDb, ...allDbs.map((database) => database.name)].filter(
          (name): name is string => typeof name === "string" && name.length > 0,
        ),
      ),
    ];
    if (databaseNames.length === 0) {
      return createEmptySchemaSnapshot();
    }

    const columnMetadataDatabase = configuredDb || databaseNames[0] || "";
    const databases = await pMapWithLimit(
      databaseNames,
      4,
      async (databaseName): Promise<SchemaSnapshotDatabaseEntry> => {
        const schemasFromDatabase = databaseInfoMap.get(databaseName)?.schemas;
        const schemas =
          schemasFromDatabase && schemasFromDatabase.length > 0
            ? schemasFromDatabase
            : await driver
                .listSchemas(databaseName)
                .catch(() => [{ name: databaseName }]);
        const perSchemaResults = await pMapWithLimit(
          schemas,
          4,
          async (schema) => {
            const objects = await driver
              .listObjects(databaseName, schema.name)
              .catch(() => []);
            const objectsForSchema = objects.filter(
              (object) =>
                object.type === "table" ||
                object.type === "view" ||
                object.type === "function" ||
                object.type === "procedure",
            );
            const loadColumns = databaseName === columnMetadataDatabase;
            const allColumns = loadColumns
              ? await pMapWithLimit(objectsForSchema, 10, async (object) => {
                  if (object.type === "table" || object.type === "view") {
                    try {
                      return await driver.describeTable(
                        databaseName,
                        schema.name,
                        object.name,
                      );
                    } catch {
                      return [];
                    }
                  }
                  return [];
                })
              : objectsForSchema.map(() => []);

            return {
              name: schema.name,
              objects: objectsForSchema.map<SchemaSnapshotObjectEntry>(
                (object, index) => ({
                  name: object.name,
                  type: object.type,
                  columns:
                    object.type === "table" || object.type === "view"
                      ? allColumns[index].map((column) => ({
                          name: column.name,
                          type: column.type,
                        }))
                      : [],
                }),
              ),
            };
          },
        );
        return {
          name: databaseName,
          schemas: perSchemaResults,
        };
      },
    );

    return { databases };
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
