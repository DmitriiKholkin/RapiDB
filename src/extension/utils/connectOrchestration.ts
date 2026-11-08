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
  disconnectFrom(connectionId: string): Promise<void>;
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
      location: vscode.ProgressLocation.Notification,
      title,
      cancellable: true,
    },
    (_progress, token) => {
      const cancellation = token?.onCancellationRequested(() => {
        void connectionManager
          .disconnectFrom(connectionId)
          .catch(() => undefined);
      });
      return attempt.promise.finally(() => cancellation?.dispose());
    },
  );

  return connectionManager.isConnected(connectionId);
}
