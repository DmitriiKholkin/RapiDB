import * as vscode from "vscode";
import type {
  BookmarkEntry,
  ConnectionConfig,
  HistoryEntry,
  StoredConnectionConfig,
} from "./connectionManagerModels";
import {
  CONNECTION_TIMEOUT_SECONDS_DEFAULT,
  createDriverTimeoutSettingsSnapshot,
  DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
  type DriverTimeoutSettingsSnapshot,
} from "./dbDrivers/timeout";

const HISTORY_STATE_KEY = "rapidb.queryHistory";
const BOOKMARKS_STATE_KEY = "rapidb.bookmarks";
const HISTORY_LIMIT_DEFAULT = 100;
const HISTORY_LIMIT_MAX = 10000;
const VALID_PAGE_SIZES = [25, 100, 500, 1000] as const;

export interface ConnectionManagerStore {
  onDidChangeConfiguration(
    listener: (event: vscode.ConfigurationChangeEvent) => void,
    subscriptions: vscode.Disposable[],
  ): void;
  getConnections(): StoredConnectionConfig[];
  saveConnections(connections: ConnectionConfig[]): Promise<void>;
  readHistory(): HistoryEntry[];
  writeHistory(entries: HistoryEntry[]): Promise<void>;
  readBookmarks(): BookmarkEntry[];
  writeBookmarks(entries: BookmarkEntry[]): Promise<void>;
  getSecret(id: string): Promise<string | undefined>;
  deleteSecret(id: string): Promise<void>;
  getHistoryLimit(): number;
  getDefaultPageSize(): number;
  getQueryRowLimit(): number;
  getTimeoutSettings(): DriverTimeoutSettingsSnapshot;
}

export class VSCodeConnectionManagerStore implements ConnectionManagerStore {
  constructor(private readonly context: vscode.ExtensionContext) {}

  onDidChangeConfiguration(
    listener: (event: vscode.ConfigurationChangeEvent) => void,
    subscriptions: vscode.Disposable[],
  ): void {
    vscode.workspace.onDidChangeConfiguration(
      listener,
      undefined,
      subscriptions,
    );
  }

  getConnections(): StoredConnectionConfig[] {
    return (
      vscode.workspace
        .getConfiguration("rapidb")
        .get<StoredConnectionConfig[]>("connections") ?? []
    );
  }

  async saveConnections(connections: ConnectionConfig[]): Promise<void> {
    await vscode.workspace
      .getConfiguration("rapidb")
      .update("connections", connections, vscode.ConfigurationTarget.Global);
  }

  readHistory(): HistoryEntry[] {
    return (
      this.context.globalState.get<HistoryEntry[]>(HISTORY_STATE_KEY) ?? []
    );
  }

  async writeHistory(entries: HistoryEntry[]): Promise<void> {
    await this.context.globalState.update(HISTORY_STATE_KEY, entries);
  }

  readBookmarks(): BookmarkEntry[] {
    return (
      this.context.globalState.get<BookmarkEntry[]>(BOOKMARKS_STATE_KEY) ?? []
    );
  }

  async writeBookmarks(entries: BookmarkEntry[]): Promise<void> {
    await this.context.globalState.update(BOOKMARKS_STATE_KEY, entries);
  }

  async getSecret(id: string): Promise<string | undefined> {
    return await this.context.secrets.get(id);
  }

  async deleteSecret(id: string): Promise<void> {
    await this.context.secrets.delete(id);
  }

  getHistoryLimit(): number {
    const raw = vscode.workspace
      .getConfiguration("rapidb")
      .get<number>("queryHistoryLimit", HISTORY_LIMIT_DEFAULT);
    return Math.max(1, Math.min(HISTORY_LIMIT_MAX, Math.round(raw)));
  }

  getDefaultPageSize(): number {
    const raw = vscode.workspace
      .getConfiguration("rapidb")
      .get<number>("defaultPageSize", 25);
    return (VALID_PAGE_SIZES as readonly number[]).includes(raw) ? raw : 25;
  }

  getQueryRowLimit(): number {
    const raw = vscode.workspace
      .getConfiguration("rapidb")
      .get<number>("queryRowLimit", 10000);
    return Math.max(100, Math.min(100000, Math.round(raw)));
  }

  getTimeoutSettings(): DriverTimeoutSettingsSnapshot {
    const configuration = vscode.workspace.getConfiguration("rapidb");
    return createDriverTimeoutSettingsSnapshot({
      connectionTimeoutSeconds: configuration.get<number>(
        "connectionTimeoutSeconds",
        CONNECTION_TIMEOUT_SECONDS_DEFAULT,
      ),
      dbOperationTimeoutSeconds: configuration.get<number>(
        "dbOperationTimeoutSeconds",
        DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
      ),
    });
  }
}
