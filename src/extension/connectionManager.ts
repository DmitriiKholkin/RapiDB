import { randomUUID } from "crypto";
import * as vscode from "vscode";
import type { ConnectionConfig } from "../shared/connectionConfig";
import { MSSQLDriver } from "./dbDrivers/mssql";
import { MySQLDriver } from "./dbDrivers/mysql";
import { OracleDriver } from "./dbDrivers/oracle";
import { PostgresDriver } from "./dbDrivers/postgres";
import { SQLiteDriver } from "./dbDrivers/sqlite";
import type { IDBDriver } from "./dbDrivers/types";
import { normalizeUnknownError } from "./utils/errorHandling";

export type { ConnectionConfig } from "../shared/connectionConfig";

export interface TestConnectionResult {
  success: boolean;
  error?: string;
}

export interface HistoryEntry {
  id: string;
  sql: string;
  connectionId: string;
  executedAt: string;
}

export interface BookmarkEntry {
  id: string;
  sql: string;
  connectionId: string;
  savedAt: string;
}

export interface ConnectAttempt {
  promise: Promise<void>;
  isNew: boolean;
}

export interface SchemaTableEntry {
  schema: string;
  table: string;
  columns: { name: string; type: string }[];
}

interface SchemaCacheEntry {
  tables: SchemaTableEntry[];
  loading: Promise<void> | null;
}

type StoredConnectionConfig = ConnectionConfig & { user?: string };

async function pMapWithLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  const indexed = items.map((item, i) => ({ item, i }));
  const workers = Array.from(
    { length: Math.min(limit, indexed.length) },
    async () => {
      while (indexed.length > 0) {
        const next = indexed.shift();
        if (!next) break;
        results[next.i] = await fn(next.item).catch(() => [] as unknown as R);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

const HISTORY_STATE_KEY = "rapidb.queryHistory";
const BOOKMARKS_STATE_KEY = "rapidb.bookmarks";
const HISTORY_LIMIT_DEFAULT = 100;
const HISTORY_LIMIT_MAX = 10000;
const VALID_PAGE_SIZES = [25, 100, 500, 1000] as const;

export class ConnectionManager {
  private readonly context: vscode.ExtensionContext;
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

  private _connectionsCache: ConnectionConfig[] | null = null;
  private readonly _schemaCacheMap = new Map<string, SchemaCacheEntry>();

  constructor(context: vscode.ExtensionContext) {
    this.context = context;
    this.onDidChangeConnections = this._onDidChangeConnections.event;
    this.onDidChangeHistory = this._onDidChangeHistory.event;
    this.onDidChangeBookmarks = this._onDidChangeBookmarks.event;
    this.onDidConnect = this._onDidConnect.event;
    this.onDidDisconnect = this._onDidDisconnect.event;
    this.onDidSchemaLoad = this._onDidSchemaLoad.event;

    vscode.workspace.onDidChangeConfiguration(
      async (e) => {
        this._connectionsCache = null;
        if (e.affectsConfiguration("rapidb.queryHistoryLimit")) {
          await this._trimHistoryToLimit();
        }
      },
      undefined,
      context.subscriptions,
    );
  }

  private getHistoryLimit(): number {
    const cfg = vscode.workspace.getConfiguration("rapidb");
    const raw = cfg.get<number>("queryHistoryLimit", HISTORY_LIMIT_DEFAULT);
    const clamped = Math.max(1, Math.min(HISTORY_LIMIT_MAX, Math.round(raw)));
    return clamped;
  }

  getDefaultPageSize(): number {
    const cfg = vscode.workspace.getConfiguration("rapidb");
    const raw = cfg.get<number>("defaultPageSize", 25);
    return (VALID_PAGE_SIZES as readonly number[]).includes(raw) ? raw : 25;
  }

  getQueryRowLimit(): number {
    const cfg = vscode.workspace.getConfiguration("rapidb");
    const raw = cfg.get<number>("queryRowLimit", 10000);
    return Math.max(100, Math.min(100000, Math.round(raw)));
  }

  private async _trimHistoryToLimit(): Promise<void> {
    const limit = this.getHistoryLimit();
    const all =
      this.context.globalState.get<HistoryEntry[]>(HISTORY_STATE_KEY) ?? [];
    if (all.length > limit) {
      await this.context.globalState.update(
        HISTORY_STATE_KEY,
        all.slice(0, limit),
      );
      this._onDidChangeHistory.fire();
    }
  }

  getConnections(): ConnectionConfig[] {
    if (this._connectionsCache) {
      return this._connectionsCache;
    }
    const config = vscode.workspace.getConfiguration("rapidb");
    this._connectionsCache = (
      config.get<StoredConnectionConfig[]>("connections") ?? []
    ).map((c) => ({
      ...c,
      id: c.id ?? randomUUID(),
      username: c.username ?? c.user,
    }));
    return this._connectionsCache;
  }

  private async saveConnections(conns: ConnectionConfig[]): Promise<void> {
    this._connectionsCache = null;
    await vscode.workspace
      .getConfiguration("rapidb")
      .update("connections", conns, vscode.ConfigurationTarget.Global);
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

  async deleteConnection(id: string): Promise<boolean> {
    const conn = this.getConnection(id);
    const confirm = await vscode.window.showWarningMessage(
      `Delete connection "${conn?.name ?? id}"?`,
      { modal: true },
      "Delete",
    );
    if (confirm !== "Delete") {
      return false;
    }
    if (this.driverMap.has(id)) {
      await this.disconnectFrom(id);
    }
    await this.saveConnections(
      this.getConnections().filter((c) => c.id !== id),
    );

    try {
      await this.context.secrets.delete(id);
    } catch {}

    await this._purgeHistoryForConnection(id);

    await this._purgeBookmarksForConnection(id);
    return true;
  }

  async _hydratePassword(config: ConnectionConfig): Promise<ConnectionConfig> {
    if (!config.useSecretStorage) {
      return config;
    }
    try {
      const stored = await this.context.secrets.get(config.id);
      return { ...config, password: stored ?? "" };
    } catch {
      return { ...config, password: "" };
    }
  }

  private async _purgeHistoryForConnection(
    connectionId: string,
  ): Promise<void> {
    const all =
      this.context.globalState.get<HistoryEntry[]>(HISTORY_STATE_KEY) ?? [];
    const filtered = all.filter((e) => e.connectionId !== connectionId);
    if (filtered.length !== all.length) {
      await this.context.globalState.update(HISTORY_STATE_KEY, filtered);
      this._onDidChangeHistory.fire();
    }
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
        this._startSchemaLoad(id);
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

  getSchema(connectionId: string): SchemaTableEntry[] {
    return this._schemaCacheMap.get(connectionId)?.tables ?? [];
  }

  async getSchemaAsync(connectionId: string): Promise<SchemaTableEntry[]> {
    const entry = this._schemaCacheMap.get(connectionId);
    if (entry?.loading) {
      try {
        await entry.loading;
      } catch {}
    }
    return this._schemaCacheMap.get(connectionId)?.tables ?? [];
  }

  private _startSchemaLoad(connectionId: string): void {
    const driver = this.getDriver(connectionId);
    const config = this.getConnection(connectionId);
    if (!driver || !config) {
      return;
    }

    let entry = this._schemaCacheMap.get(connectionId);
    if (!entry) {
      entry = { tables: [], loading: null };
      this._schemaCacheMap.set(connectionId, entry);
    }
    if (entry.loading) {
      return;
    }

    const capturedEntry = entry;
    capturedEntry.loading = this._loadSchemaInternal(driver, config)
      .then((tables) => {
        const live = this._schemaCacheMap.get(connectionId);
        if (live === capturedEntry) {
          live.tables = tables;
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
  ): Promise<SchemaTableEntry[]> {
    const result: SchemaTableEntry[] = [];
    const configuredDb = config.database || (config.filePath ? "main" : "");
    const allDbs = await driver.listDatabases().catch(() => []);
    const primaryDb = configuredDb || allDbs[0]?.name || "";
    if (!primaryDb) {
      return result;
    }

    const primarySchemas = await driver
      .listSchemas(primaryDb)
      .catch(() => [{ name: primaryDb }]);

    for (const schema of primarySchemas.slice(0, 10)) {
      const objects = await driver
        .listObjects(primaryDb, schema.name)
        .catch(() => []);
      const tables = objects
        .filter((o) => o.type === "table" || o.type === "view")
        .slice(0, 100);

      const allCols = await pMapWithLimit(tables, 5, (tbl) =>
        driver.describeTable(primaryDb, schema.name, tbl.name).catch(() => []),
      );

      tables.forEach((tbl, i) => {
        result.push({
          schema: schema.name,
          table: tbl.name,
          columns: allCols[i].map((c) => ({ name: c.name, type: c.type })),
        });
      });
    }

    return result;
  }

  async testConnection(
    config: Omit<ConnectionConfig, "id">,
  ): Promise<TestConnectionResult> {
    const driver = this.createDriver({ ...config, id: "__test__" });
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

  async pickConnection(): Promise<string | undefined> {
    const conns = this.getConnections();
    if (conns.length === 0) {
      vscode.window.showInformationMessage(
        "[RapiDB] No connections configured. Use 'Add Connection' first.",
      );
      return;
    }
    const pick = await vscode.window.showQuickPick(
      conns.map((c) => ({
        label: `${this.isConnected(c.id) ? "$(circle-filled)" : "$(circle-outline)"} ${c.name}`,
        description: c.type,
        id: c.id,
      })),
      { placeHolder: "Select a connection" },
    );
    return pick?.id;
  }

  getHistory(connectionId?: string): HistoryEntry[] {
    const all =
      this.context.globalState.get<HistoryEntry[]>(HISTORY_STATE_KEY) ?? [];
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

    const all =
      this.context.globalState.get<HistoryEntry[]>(HISTORY_STATE_KEY) ?? [];

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

    await this.context.globalState.update(HISTORY_STATE_KEY, updated);
    this._onDidChangeHistory.fire();
  }

  async clearHistory(): Promise<void> {
    await this.context.globalState.update(HISTORY_STATE_KEY, []);
    this._onDidChangeHistory.fire();
  }

  getBookmarks(connectionId?: string): BookmarkEntry[] {
    const all =
      this.context.globalState.get<BookmarkEntry[]>(BOOKMARKS_STATE_KEY) ?? [];
    if (connectionId) {
      return all.filter((b) => b.connectionId === connectionId);
    }
    return all;
  }

  async addBookmark(connectionId: string, sql: string): Promise<BookmarkEntry> {
    const trimmed = sql.trim();
    const entry: BookmarkEntry = {
      id: randomUUID(),
      sql: trimmed,
      connectionId,
      savedAt: new Date().toISOString(),
    };
    const all =
      this.context.globalState.get<BookmarkEntry[]>(BOOKMARKS_STATE_KEY) ?? [];
    await this.context.globalState.update(BOOKMARKS_STATE_KEY, [entry, ...all]);
    this._onDidChangeBookmarks.fire();
    return entry;
  }

  async deleteBookmark(id: string): Promise<boolean> {
    const all =
      this.context.globalState.get<BookmarkEntry[]>(BOOKMARKS_STATE_KEY) ?? [];
    const target = all.find((b) => b.id === id);
    const preview = target
      ? target.sql.slice(0, 60).replace(/\s+/g, " ") +
        (target.sql.length > 60 ? "…" : "")
      : id;

    const answer = await vscode.window.showWarningMessage(
      `[RapiDB] Delete bookmark: "${preview}"?`,
      { modal: true },
      "Delete",
    );
    if (answer !== "Delete") {
      return false;
    }

    await this.context.globalState.update(
      BOOKMARKS_STATE_KEY,
      all.filter((b) => b.id !== id),
    );
    this._onDidChangeBookmarks.fire();
    return true;
  }

  private async _purgeBookmarksForConnection(
    connectionId: string,
  ): Promise<void> {
    const all =
      this.context.globalState.get<BookmarkEntry[]>(BOOKMARKS_STATE_KEY) ?? [];
    const filtered = all.filter((b) => b.connectionId !== connectionId);
    if (filtered.length !== all.length) {
      await this.context.globalState.update(BOOKMARKS_STATE_KEY, filtered);
      this._onDidChangeBookmarks.fire();
    }
  }

  async clearBookmarks(): Promise<void> {
    await this.context.globalState.update(BOOKMARKS_STATE_KEY, []);
    this._onDidChangeBookmarks.fire();
  }
}
