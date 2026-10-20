import { randomUUID } from "node:crypto";
import * as vscode from "vscode";
import type { BookmarkEntry, HistoryEntry } from "./connectionManagerModels";
import type { ConnectionManagerStore } from "./connectionManagerStore";

export class ConnectionHistoryService {
  readonly onDidChangeHistory: vscode.Event<void>;
  private readonly _onDidChangeHistory = new vscode.EventEmitter<void>();

  readonly onDidChangeBookmarks: vscode.Event<void>;
  private readonly _onDidChangeBookmarks = new vscode.EventEmitter<void>();

  constructor(private readonly store: ConnectionManagerStore) {
    this.onDidChangeHistory = this._onDidChangeHistory.event;
    this.onDidChangeBookmarks = this._onDidChangeBookmarks.event;
  }

  private getHistoryLimit(): number {
    return this.store.getHistoryLimit();
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

  async purgeHistoryForConnection(connectionId: string): Promise<void> {
    await this.purgeEntriesForConnection(
      connectionId,
      () => this.store.readHistory(),
      (entries) => this.store.writeHistory(entries),
      () => this._onDidChangeHistory.fire(),
    );
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

  async purgeBookmarksForConnection(connectionId: string): Promise<void> {
    await this.purgeEntriesForConnection(
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

  async trimHistoryToLimit(): Promise<boolean> {
    const limit = this.getHistoryLimit();
    const all = this.store.readHistory();
    if (limit === 0) {
      if (all.length > 0) {
        await this.store.writeHistory([]);
        this._onDidChangeHistory.fire();
        return true;
      }
      return false;
    }
    if (all.length > limit) {
      await this.store.writeHistory(all.slice(0, limit));
      this._onDidChangeHistory.fire();
      return true;
    }
    return false;
  }

  private async purgeEntriesForConnection<T extends { connectionId: string }>(
    connectionId: string,
    readEntries: () => T[],
    writeEntries: (entries: T[]) => Promise<void>,
    fireEvent: () => void,
  ): Promise<void> {
    const entries = readEntries();
    const filtered = entries.filter((e) => e.connectionId !== connectionId);
    if (filtered.length === entries.length) {
      return;
    }
    await writeEntries(filtered);
    fireEvent();
  }

  dispose(): void {
    this._onDidChangeHistory.dispose();
    this._onDidChangeBookmarks.dispose();
  }
}
