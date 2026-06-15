/**
 * Reflects the number of connected databases in the explorer view's
 * badge. The badge is hidden when no connections are active.
 */

import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";
import type { RapiDBNode } from "../providers/connectionProvider";

export type ExplorerBadgeUpdater = () => void;

/**
 * Build a zero-arg updater that, when called, reads the live
 * `connectedCount` from the connection manager and applies it to the
 * explorer tree view.
 */
export function createBadgeUpdater(
  treeView: vscode.TreeView<RapiDBNode>,
  connectionManager: ConnectionManager,
): ExplorerBadgeUpdater {
  return () => {
    const connectedCount = connectionManager.getConnectedCount();
    treeView.badge =
      connectedCount > 0
        ? {
            value: connectedCount,
            tooltip: `${connectedCount} connected database${connectedCount === 1 ? "" : "s"}`,
          }
        : undefined;
  };
}
