import * as vscode from "vscode";
import type { ConnectionManager } from "./connectionManager";

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
      label: `${connectionManager.isConnected(connection.id) ? "$(circle-filled)" : "$(circle-outline)"} ${connection.name}`,
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
  const answer = await vscode.window.showWarningMessage(
    `Delete connection "${connection?.name ?? connectionId}"?`,
    { modal: true },
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

  const answer = await vscode.window.showWarningMessage(
    `[RapiDB] Delete folder "${trimmedFolderName}"? ${matchingConnections.length} connection${matchingConnections.length === 1 ? "" : "s"} will be moved to the root level.`,
    { modal: true },
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
  const preview = bookmark
    ? bookmark.sql.slice(0, 60).replace(/\s+/g, " ") +
      (bookmark.sql.length > 60 ? "…" : "")
    : bookmarkId;

  const answer = await vscode.window.showWarningMessage(
    `[RapiDB] Delete bookmark: "${preview}"?`,
    { modal: true },
    "Delete",
  );

  if (answer !== "Delete") {
    return false;
  }

  return connectionManager.removeBookmark(bookmarkId);
}
