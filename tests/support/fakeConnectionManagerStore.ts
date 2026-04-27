import type {
  BookmarkEntry,
  ConnectionConfig,
  HistoryEntry,
  StoredConnectionConfig,
} from "../../src/extension/connectionManagerModels";
import type { ConnectionManagerStore } from "../../src/extension/connectionManagerStore";

type ConfigurationListener = (event: {
  affectsConfiguration(section: string): boolean;
}) => void;

export class FakeConnectionManagerStore implements ConnectionManagerStore {
  private connections: StoredConnectionConfig[] = [];
  private history: HistoryEntry[] = [];
  private bookmarks: BookmarkEntry[] = [];
  private readonly secrets = new Map<string, string>();
  private readonly listeners: ConfigurationListener[] = [];
  private historyLimit = 100;
  private defaultPageSize = 25;
  private queryRowLimit = 10_000;

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
