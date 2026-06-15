/**
 * Wraps the `connectionManager.beginConnect` / `isConnected` pair with
 * the VSCode `withProgress` notification. Three small concerns:
 *
 *  - if the connection is already established, return immediately;
 *  - if another caller already started a connect attempt, optionally
 *    wait for it;
 *  - otherwise, show progress and await the attempt.
 *
 * The `withProgress` runner is injected so the function can be unit
 * tested without spinning up the VSCode host.
 */

import * as vscode from "vscode";
import type { ConnectAttempt, ConnectionManager } from "../connectionManager";

type ProgressRunner = typeof vscode.window.withProgress;

/** Minimal surface of `ConnectionManager` that this helper depends on. */
export interface ConnectProgressClient {
  beginConnect(connectionId: string): ConnectAttempt;
  isConnected(connectionId: string): boolean;
}

export async function connectWithProgress(
  connectionManager: ConnectProgressClient,
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
