import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../../shared/commandIds";
import { readQueryDatabaseScope } from "../../shared/queryDatabaseTarget";
import { parseQueryTargetBinding } from "../../shared/webviewContracts/query";
import type {
  BookmarkEntry,
  ConnectionManager,
  HistoryEntry,
} from "../connectionManager";
import { confirmBookmarkRemoval } from "../connectionManagerPrompts";
import { QueryPanel } from "../panels/queryPanel";

export interface SavedEntryCommandContext {
  readonly context: vscode.ExtensionContext;
  readonly connectionManager: ConnectionManager;
}

async function showSavedQuery(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  entry: Pick<
    HistoryEntry,
    | "connectionId"
    | "sql"
    | "postgresDatabase"
    | "mssqlDatabase"
    | "targetSessionKey"
  >,
  options?: {
    forceNew?: boolean;
    formatOnOpen?: boolean;
    isBookmarked?: boolean;
  },
): Promise<void> {
  if (!entry.connectionId || !entry.sql) {
    return;
  }
  const databaseScope = readQueryDatabaseScope(entry);
  if (databaseScope === null) {
    const label =
      entry.mssqlDatabase === undefined ? "PostgreSQL" : "SQL Server";
    vscode.window.showErrorMessage(
      `[RapiDB] Cannot open this saved query: its ${label} database is invalid.`,
    );
    return;
  }

  const initializing =
    connectionManager.initializeConnectionCredentialRevision?.(
      entry.connectionId,
    );
  if (initializing && !(await initializing)) {
    vscode.window.showErrorMessage(
      "[RapiDB] Could not verify the connection's saved credentials. Unlock Secret Storage and retry.",
    );
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
    undefined,
    databaseScope,
    // Legacy entries still open, but cannot inherit the current target until
    // the user explicitly confirms it in the query panel.
    parseQueryTargetBinding({
      connectionId: entry.connectionId,
      sessionKey: entry.targetSessionKey,
    }) ?? { connectionId: entry.connectionId, sessionKey: null },
  );
}

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

export function registerSavedEntryCommands(
  ctx: SavedEntryCommandContext,
  registerCommand: <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => vscode.Disposable,
): void {
  const { context, connectionManager } = ctx;

  registerCommand(CMD.openHistoryEntry, (entry: HistoryEntry) =>
    showSavedQuery(context, connectionManager, entry),
  );

  registerCommand(CMD.openBookmarkEntry, (entry: BookmarkEntry) =>
    showSavedQuery(context, connectionManager, entry, {
      forceNew: true,
      formatOnOpen: false,
      isBookmarked: true,
    }),
  );

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

  registerCommand(CMD.clearBookmarks, async () => {
    await clearSavedEntries(
      "[RapiDB] Clear all bookmarks?",
      "[RapiDB] All bookmarks cleared.",
      () => connectionManager.clearBookmarks(),
    );
  });

  registerCommand(CMD.clearHistory, async () => {
    await clearSavedEntries(
      "[RapiDB] Clear all query history?",
      "[RapiDB] Query history cleared.",
      () => connectionManager.clearHistory(),
    );
  });
}
