import * as vscode from "vscode";
import type {
  ConnectAttempt,
  ConnectionConnectIntent,
} from "../connectionManager";

type ProgressRunner = typeof vscode.window.withProgress;

export interface ConnectProgressClient {
  beginConnect(
    connectionId: string,
    intent?: ConnectionConnectIntent,
  ): ConnectAttempt;
  isConnected(connectionId: string): boolean;
}

export async function connectWithProgress(
  connectionManager: ConnectProgressClient,
  connectionId: string,
  title: string,
  waitForExisting: boolean,
  withProgress: ProgressRunner = vscode.window.withProgress,
  intent: ConnectionConnectIntent = "automatic",
): Promise<boolean> {
  if (connectionManager.isConnected(connectionId)) {
    return true;
  }

  const attempt = connectionManager.beginConnect(connectionId, intent);
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
