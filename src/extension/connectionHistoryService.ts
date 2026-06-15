import { randomUUID } from "node:crypto";
import * as vscode from "vscode";
import type { BookmarkEntry, HistoryEntry } from "./connectionManagerModels";
import type { ConnectionManagerStore } from "./connectionManagerStore";

/**
 * Service for managing connection history and bookmarks.
 * Encapsulates all history/bookmark CRUD operations and event firing.
 */
export class ConnectionHistoryService {
  readonly onDidChangeHistory: vscode.Event<void>;
  private readonly _onDidChangeHistory = new vscode.EventEmitter<void>();

  readonly onDidChangeBookmarks: vscode.Event<void>;
  private readonly _onDidChangeBookmarks = new vscode.EventEmitter<void>();

  constructor(private readonly store: ConnectionManagerStore) {
    this.onDidChangeHistory = this._onDidChangeHistory.event;
    this.onDidChangeBookmarks = this._onDidChangeBookmarks.event;
  }

  /**
   * Returns the history limit from configuration.
   */
  private getHistoryLimit(): number {
    return this.store.getHistoryLimit();
  }

  /**
   * Gets history entries, optionally filtered by connection ID.
   */
  getHistory(connectionId?: string): HistoryEntry[] {
    const all = this.store.readHistory();
    if (connectionId) {
      return all.filter((e) => e.connectionId === connectionId);
    }
    return all;
  }

  /**
   * Adds a SQL query to the history.
   */
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

  /**
   * Clears all history entries.
   */
  async clearHistory(): Promise<void> {
    await this.store.writeHistory([]);
    this._onDidChangeHistory.fire();
  }

  /**
   * Purges history entries for a specific connection.
   */
  async purgeHistoryForConnection(connectionId: string): Promise<void> {
    await this.purgeEntriesForConnection(
      connectionId,
      () => this.store.readHistory(),
      (entries) => this.store.writeHistory(entries),
      () => this._onDidChangeHistory.fire(),
    );
  }

  /**
   * Gets bookmark entries, optionally filtered by connection ID.
   */
  getBookmarks(connectionId?: string): BookmarkEntry[] {
    const all = this.store.readBookmarks();
    if (connectionId) {
      return all.filter((b) => b.connectionId === connectionId);
    }
    return all;
  }

  /**
   * Gets a specific bookmark by ID.
   */
  getBookmark(id: string): BookmarkEntry | undefined {
    return this.store.readBookmarks().find((bookmark) => bookmark.id === id);
  }

  /**
   * Adds a new bookmark.
   */
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

  /**
   * Removes a bookmark by ID.
   */
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

  /**
   * Purges bookmarks for a specific connection.
   */
  async purgeBookmarksForConnection(connectionId: string): Promise<void> {
    await this.purgeEntriesForConnection(
      connectionId,
      () => this.store.readBookmarks(),
      (entries) => this.store.writeBookmarks(entries),
      () => this._onDidChangeBookmarks.fire(),
    );
  }

  /**
   * Clears all bookmarks.
   */
  async clearBookmarks(): Promise<void> {
    await this.store.writeBookmarks([]);
    this._onDidChangeBookmarks.fire();
  }

  /**
   * Trims history to the configured limit.
   * Returns true if history was modified.
   */
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

  /**
   * Generic method to purge entries for a connection.
   */
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

  /**
   * Disposes event emitters.
   */
  dispose(): void {
    this._onDidChangeHistory.dispose();
    this._onDidChangeBookmarks.dispose();
  }
}
