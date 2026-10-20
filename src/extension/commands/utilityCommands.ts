import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../../shared/commandIds";
import type { ConnectionManager } from "../connectionManager";
import type {
  ConnectionProvider,
  RapiDBNode,
} from "../providers/connectionProvider";

export interface UtilityCommandContext {
  readonly connectionManager: ConnectionManager;
  readonly connectionProvider: ConnectionProvider;
  readonly refresh: () => void;
}

export function registerUtilityCommands(
  ctx: UtilityCommandContext,
  registerCommand: <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => vscode.Disposable,
): void {
  const { connectionManager, connectionProvider, refresh } = ctx;

  registerCommand(CMD.refreshAll, () => {
    connectionManager.refreshSchemaCache({ reason: "manual" });
    connectionProvider.refreshConnectionTree();
  });

  registerCommand(CMD.refresh, (node?: RapiDBNode) => {
    const connectionId = node?.connectionId;
    connectionManager.refreshSchemaCache({
      connectionId,
      reason: "manual",
    });
    connectionProvider.refreshConnectionTree(connectionId);
  });

  registerCommand(CMD.showConnectedOnly, async () => {
    await connectionProvider.setConnectedOnly(true);
  });

  registerCommand(CMD.showAllConnections, async () => {
    await connectionProvider.setConnectedOnly(false);
  });

  registerCommand(CMD.disconnectAll, async () => {
    connectionProvider.collapseAllConnectionRoots();
    await connectionManager.disconnectAll();
    refresh();
  });
}
