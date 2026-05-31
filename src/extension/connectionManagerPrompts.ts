import * as vscode from "vscode";
import type { ConnectionManager } from "./connectionManager";

function confirmDestructiveAction(
  message: string,
  actionLabel: string,
): Thenable<string | undefined> {
  return vscode.window.showWarningMessage(
    message,
    { modal: true },
    actionLabel,
  );
}

function formatConnectionQuickPickLabel(
  isConnected: boolean,
  name: string,
): string {
  return `${isConnected ? "$(circle-filled)" : "$(circle-outline)"} ${name}`;
}

function formatBookmarkPreview(sql: string): string {
  const compactSql = sql.slice(0, 60).replace(/\s+/g, " ");
  return compactSql + (sql.length > 60 ? "…" : "");
}

export async function pickConnectionWithPrompt(
  connectionManager: Pick<ConnectionManager, "getConnections" | "isConnected">,
): Promise<string | undefined> {
  const connections = connectionManager.getConnections();
  if (connections.length === 0) {
    vscode.window.showInformationMessage(
      "[RapiDB] No connections configured. Use 'Add Connection' first.",
    );
    return undefined;
  }

  const pickedConnection = await vscode.window.showQuickPick(
    connections.map((connection) => ({
      label: formatConnectionQuickPickLabel(
        connectionManager.isConnected(connection.id),
        connection.name,
      ),
      description: connection.type,
      id: connection.id,
    })),
    { placeHolder: "Select a connection" },
  );

  return pickedConnection?.id;
}

export async function confirmConnectionRemoval(
  connectionManager: Pick<
    ConnectionManager,
    "getConnection" | "removeConnection"
  >,
  connectionId: string,
): Promise<boolean> {
  const connection = connectionManager.getConnection(connectionId);
  const answer = await confirmDestructiveAction(
    `Delete connection "${connection?.name ?? connectionId}"?`,
    "Delete",
  );

  if (answer !== "Delete") {
    return false;
  }

  return connectionManager.removeConnection(connectionId);
}

export async function confirmConnectionFolderRemoval(
  connectionManager: Pick<ConnectionManager, "getConnections" | "removeFolder">,
  folderName: string,
): Promise<boolean> {
  const trimmedFolderName = folderName.trim();
  if (!trimmedFolderName) {
    return false;
  }

  const matchingConnections = connectionManager
    .getConnections()
    .filter((connection) => connection.folder?.trim() === trimmedFolderName);
  if (matchingConnections.length === 0) {
    return false;
  }

  const answer = await confirmDestructiveAction(
    `[RapiDB] Delete folder "${trimmedFolderName}"? ${matchingConnections.length} connection${matchingConnections.length === 1 ? "" : "s"} will be moved to the root level.`,
    "Delete Folder",
  );

  if (answer !== "Delete Folder") {
    return false;
  }

  return (await connectionManager.removeFolder(trimmedFolderName)) > 0;
}

export async function confirmBookmarkRemoval(
  connectionManager: Pick<ConnectionManager, "getBookmark" | "removeBookmark">,
  bookmarkId: string,
): Promise<boolean> {
  const bookmark = connectionManager.getBookmark(bookmarkId);
  const preview = bookmark ? formatBookmarkPreview(bookmark.sql) : bookmarkId;

  const answer = await confirmDestructiveAction(
    `[RapiDB] Delete bookmark: "${preview}"?`,
    "Delete",
  );

  if (answer !== "Delete") {
    return false;
  }

  return connectionManager.removeBookmark(bookmarkId);
}
