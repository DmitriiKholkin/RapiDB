import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../../shared/commandIds";
import type {
  BookmarkEntry,
  ConnectionManager,
  HistoryEntry,
} from "../connectionManager";
import { confirmBookmarkRemoval } from "../connectionManagerPrompts";
import { QueryPanel } from "../panels/queryPanel";

/**
 * Context required for saved entry commands.
 */
export interface SavedEntryCommandContext {
  readonly context: vscode.ExtensionContext;
  readonly connectionManager: ConnectionManager;
}

/**
 * Shows a saved query in a query panel.
 */
function showSavedQuery(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  entry: Pick<HistoryEntry, "connectionId" | "sql">,
  options?: {
    forceNew?: boolean;
    formatOnOpen?: boolean;
    isBookmarked?: boolean;
  },
): void {
  if (!entry.connectionId || !entry.sql) {
    return;
  }

  QueryPanel.createOrShow(
    context,
    connectionManager,
    entry.connectionId,
    entry.sql,
    options?.forceNew,
    options?.formatOnOpen,
    options?.isBookmarked,
  );
}

/**
 * Clears saved entries (history or bookmarks) after user confirmation.
 */
async function clearSavedEntries(
  prompt: string,
  successMessage: string,
  action: () => Promise<void>,
): Promise<void> {
  const answer = await vscode.window.showWarningMessage(
    prompt,
    { modal: true },
    "Clear",
  );
  if (answer === "Clear") {
    await action();
    vscode.window.showInformationMessage(successMessage);
  }
}

/**
 * Registers all saved entry commands (history and bookmarks).
 *
 * Commands:
 * - rapidb.openHistoryEntry: Open a history entry
 * - rapidb.openBookmarkEntry: Open a bookmark entry
 * - rapidb.deleteBookmark: Delete a bookmark
 * - rapidb.clearBookmarks: Clear all bookmarks
 * - rapidb.clearHistory: Clear all history
 */
export function registerSavedEntryCommands(
  ctx: SavedEntryCommandContext,
  registerCommand: <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => vscode.Disposable,
): void {
  const { context, connectionManager } = ctx;

  // ─── Open History Entry ────────────────────────────────────────────
  registerCommand(CMD.openHistoryEntry, (entry: HistoryEntry) => {
    showSavedQuery(context, connectionManager, entry);
  });

  // ─── Open Bookmark Entry ───────────────────────────────────────────
  registerCommand(CMD.openBookmarkEntry, (entry: BookmarkEntry) => {
    showSavedQuery(context, connectionManager, entry, {
      forceNew: true,
      formatOnOpen: false,
      isBookmarked: true,
    });
  });

  // ─── Delete Bookmark ───────────────────────────────────────────────
  registerCommand(
    CMD.deleteBookmark,
    async (node?: {
      entry?: {
        id?: string;
      };
      id?: string;
    }) => {
      const id = node?.entry?.id ?? node?.id;
      if (!id) {
        return;
      }

      await confirmBookmarkRemoval(connectionManager, id);
    },
  );

  // ─── Clear Bookmarks ──────────────────────────────────────────────
  registerCommand(CMD.clearBookmarks, async () => {
    await clearSavedEntries(
      "[RapiDB] Clear all bookmarks?",
      "[RapiDB] All bookmarks cleared.",
      () => connectionManager.clearBookmarks(),
    );
  });

  // ─── Clear History ─────────────────────────────────────────────────
  registerCommand(CMD.clearHistory, async () => {
    await clearSavedEntries(
      "[RapiDB] Clear all query history?",
      "[RapiDB] Query history cleared.",
      () => connectionManager.clearHistory(),
    );
  });
}
