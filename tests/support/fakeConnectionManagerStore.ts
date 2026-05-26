import { createHash } from "node:crypto";
import type {
  BookmarkEntry,
  ConnectionConfig,
  HistoryEntry,
  StoredConnectionConfig,
} from "../../src/extension/connectionManagerModels";
import type { ConnectionManagerStore } from "../../src/extension/connectionManagerStore";
import {
  createDriverTimeoutSettingsSnapshot,
  type DriverTimeoutSettingsSnapshot,
} from "../../src/extension/dbDrivers/timeout";

type ConfigurationListener = (event: {
  affectsConfiguration(section: string): boolean;
}) => void;

function stableSerialize(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerialize(item)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort((a, b) => a.localeCompare(b));
    return `{${keys
      .map((key) => `${JSON.stringify(key)}:${stableSerialize(record[key])}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
}

function computeConnectionsRevision(
  connections: StoredConnectionConfig[],
): string {
  return createHash("sha256")
    .update(stableSerialize(connections))
    .digest("hex");
}

export class FakeConnectionManagerStore implements ConnectionManagerStore {
  private connections: StoredConnectionConfig[] = [];
  private history: HistoryEntry[] = [];
  private bookmarks: BookmarkEntry[] = [];
  private readonly secrets = new Map<string, string>();
  private readonly listeners: ConfigurationListener[] = [];
  private historyLimit = 100;
  private defaultPageSize = 25;
  private queryRowLimit = 1_000;
  private skipTableMutationPreview = false;
  private timeoutSettings = createDriverTimeoutSettingsSnapshot();

  onDidChangeConfiguration(
    listener: ConfigurationListener,
    subscriptions: Array<{ dispose(): void }>,
  ): void {
    this.listeners.push(listener);
    subscriptions.push({
      dispose: () => {
        const index = this.listeners.indexOf(listener);
        if (index >= 0) {
          this.listeners.splice(index, 1);
        }
      },
    });
  }

  getConnections(): StoredConnectionConfig[] {
    return this.connections.map((connection) => ({ ...connection }));
  }

  async saveConnections(connections: ConnectionConfig[]): Promise<void> {
    this.connections = connections.map((connection) => ({ ...connection }));
  }

  getConnectionsRevision(): string {
    return computeConnectionsRevision(this.connections);
  }

  async saveConnectionsIfRevision(
    expectedRevision: string,
    connections: ConnectionConfig[],
  ): Promise<boolean> {
    if (this.getConnectionsRevision() !== expectedRevision) {
      return false;
    }

    this.connections = connections.map((connection) => ({ ...connection }));
    return true;
  }

  readHistory(): HistoryEntry[] {
    return this.history.map((entry) => ({ ...entry }));
  }

  async writeHistory(entries: HistoryEntry[]): Promise<void> {
    this.history = entries.map((entry) => ({ ...entry }));
  }

  readBookmarks(): BookmarkEntry[] {
    return this.bookmarks.map((entry) => ({ ...entry }));
  }

  async writeBookmarks(entries: BookmarkEntry[]): Promise<void> {
    this.bookmarks = entries.map((entry) => ({ ...entry }));
  }

  async getSecret(id: string): Promise<string | undefined> {
    return this.secrets.get(id);
  }

  async storeSecret(id: string, value: string): Promise<void> {
    this.secrets.set(id, value);
  }

  async deleteSecret(id: string): Promise<void> {
    this.secrets.delete(id);
  }

  getHistoryLimit(): number {
    return this.historyLimit;
  }

  getDefaultPageSize(): number {
    return this.defaultPageSize;
  }

  getQueryRowLimit(): number {
    return this.queryRowLimit;
  }

  getSkipTableMutationPreview(): boolean {
    return this.skipTableMutationPreview;
  }

  getTimeoutSettings(): DriverTimeoutSettingsSnapshot {
    return { ...this.timeoutSettings };
  }

  setConnections(connections: StoredConnectionConfig[]): void {
    this.connections = connections.map((connection) => ({ ...connection }));
  }

  setHistory(entries: HistoryEntry[]): void {
    this.history = entries.map((entry) => ({ ...entry }));
  }

  setBookmarks(entries: BookmarkEntry[]): void {
    this.bookmarks = entries.map((entry) => ({ ...entry }));
  }

  setSecret(id: string, value: string): void {
    this.secrets.set(id, value);
  }

  setHistoryLimit(limit: number): void {
    this.historyLimit = limit;
  }

  setDefaultPageSize(pageSize: number): void {
    this.defaultPageSize = pageSize;
  }

  setQueryRowLimit(limit: number): void {
    this.queryRowLimit = limit;
  }

  setSkipTableMutationPreview(skip: boolean): void {
    this.skipTableMutationPreview = skip;
  }

  setTimeoutSettings(settings: {
    connectionTimeoutSeconds?: number;
    dbOperationTimeoutSeconds?: number;
  }): void {
    this.timeoutSettings = createDriverTimeoutSettingsSnapshot(settings);
  }

  fireConfigurationChange(...sections: string[]): void {
    const affected = new Set(sections);
    for (const listener of this.listeners) {
      listener({
        affectsConfiguration: (section: string) => affected.has(section),
      });
    }
  }
}

export function createExtensionContextStub(): {
  subscriptions: Array<{ dispose(): void }>;
  secrets: {
    get(key: string): Promise<string | undefined>;
    store(key: string, value: string): Promise<void>;
    delete(key: string): Promise<void>;
  };
  globalState: {
    get<T>(key: string): T | undefined;
    update(key: string, value: unknown): Promise<void>;
  };
} {
  const secretStore = new Map<string, string>();
  const state = new Map<string, unknown>();

  return {
    subscriptions: [],
    secrets: {
      async get(key: string) {
        return secretStore.get(key);
      },
      async store(key: string, value: string) {
        secretStore.set(key, value);
      },
      async delete(key: string) {
        secretStore.delete(key);
      },
    },
    globalState: {
      get<T>(key: string) {
        return state.get(key) as T | undefined;
      },
      async update(key: string, value: unknown) {
        state.set(key, value);
      },
    },
  };
}
