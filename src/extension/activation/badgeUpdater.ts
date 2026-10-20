import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";
import type { RapiDBNode } from "../providers/connectionProvider";

export type ExplorerBadgeUpdater = () => void;

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
