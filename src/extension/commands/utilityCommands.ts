import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../../shared/commandIds";
import type { ConnectionManager } from "../connectionManager";
import type {
  ConnectionProvider,
  RapiDBNode,
} from "../providers/connectionProvider";

/**
 * Context required for utility commands.
 */
export interface UtilityCommandContext {
  readonly connectionManager: ConnectionManager;
  readonly connectionProvider: ConnectionProvider;
  readonly refresh: () => void;
}

/**
 * Registers all utility commands.
 *
 * Commands:
 * - rapidb.refresh: Refresh connection tree
 * - rapidb.showConnectedOnly: Toggle connected-only view
 * - rapidb.showAllConnections: Show all connections
 * - rapidb.disconnectAll: Disconnect from all connections
 */
export function registerUtilityCommands(
  ctx: UtilityCommandContext,
  registerCommand: <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => vscode.Disposable,
): void {
  const { connectionManager, connectionProvider, refresh } = ctx;

  // ─── Refresh ───────────────────────────────────────────────────────
  registerCommand(CMD.refresh, (node?: RapiDBNode) => {
    connectionManager.refreshSchemaCache({
      connectionId: node?.connectionId,
      reason: "manual",
    });
    connectionProvider.refreshConnectionTree(node?.connectionId);
  });

  // ─── Show Connected Only ───────────────────────────────────────────
  registerCommand(CMD.showConnectedOnly, async () => {
    await connectionProvider.toggleConnectedOnly();
  });

  // ─── Show All Connections ──────────────────────────────────────────
  registerCommand(CMD.showAllConnections, async () => {
    await connectionProvider.toggleConnectedOnly();
  });

  // ─── Disconnect All ────────────────────────────────────────────────
  registerCommand(CMD.disconnectAll, async () => {
    await connectionManager.disconnectAll();
    refresh();
  });
}
