import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";

type ProgressRunner = typeof vscode.window.withProgress;

export async function connectWithProgress(
  connectionManager: Pick<ConnectionManager, "beginConnect" | "isConnected">,
  connectionId: string,
  title: string,
  waitForExisting: boolean,
  withProgress: ProgressRunner = vscode.window.withProgress,
): Promise<boolean> {
  if (connectionManager.isConnected(connectionId)) {
    return true;
  }

  const attempt = connectionManager.beginConnect(connectionId);
  if (!attempt.isNew) {
    if (!waitForExisting) {
      return false;
    }
    await attempt.promise;
    return connectionManager.isConnected(connectionId);
  }

  await withProgress(
    {
      location: vscode.ProgressLocation.Window,
      title,
    },
    () => attempt.promise,
  );

  return connectionManager.isConnected(connectionId);
}
