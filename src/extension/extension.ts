/**
 * RapiDB extension entry point.
 *
 * This module is intentionally thin: it wires together focused
 * collaborators (activation submodules, panels, providers) and
 * delegates every non-trivial concern to a dedicated module. The
 * lifecycle is split into:
 *
 *   - `activateOnce` — guarded against duplicate calls (the test runner
 *      re-imports this module between cases).
 *   - `deactivate`   — disposes everything that was created during
 *      activation.
 *
 * Keep this file boring: if you need to add a feature, create or extend
 * a module under `activation/`, `commands/`, `panels/`, or
 * `providers/` and wire it in here.
 */

import * as vscode from "vscode";
import {
  type ActivationServices,
  createBadgeUpdater,
  createCommandRegistrar,
  getExplorerSchemaScopeForNode,
  hasActiveState,
  isConnectionRootNode,
  setActiveState,
  takeActiveState,
} from "./activation";
import {
  registerConnectionCommands,
  registerExplorerCommands,
  registerSavedEntryCommands,
  registerUtilityCommands,
} from "./commands";
import type { ExplorerSchemaScope } from "./connectionManager";
import { ConnectionManager } from "./connectionManager";
import { ErdPanel } from "./panels/erdPanel";
import { QueryPanel } from "./panels/queryPanel";
import { TablePanel } from "./panels/tablePanel";
import { BookmarksProvider } from "./providers/bookmarksProvider";
import {
  ConnectionProvider,
  type RapiDBNode,
} from "./providers/connectionProvider";
import { HistoryProvider } from "./providers/historyProvider";
import { logger } from "./utils/logger";
import {
  configureSQLiteInstaller,
  warmupSQLiteRuntime,
} from "./utils/sqliteInstaller";

function registerAllCommands(
  services: ActivationServices,
  registrar: ReturnType<typeof createCommandRegistrar>,
): void {
  const { context, connectionManager, connectionProvider, refresh } = services;
  const reg = registrar.register;

  registerConnectionCommands({ context, connectionManager, refresh }, reg);
  registerExplorerCommands({ context, connectionManager, refresh }, reg);
  registerSavedEntryCommands({ context, connectionManager }, reg);
  registerUtilityCommands(
    { connectionManager, connectionProvider, refresh },
    reg,
  );
}

function trackExplorerTreeView(
  treeView: vscode.TreeView<RapiDBNode>,
  services: ActivationServices,
): vscode.Disposable[] {
  const { connectionManager, connectionProvider } = services;

  return [
    treeView.onDidExpandElement(({ element }) => {
      const scope: ExplorerSchemaScope | undefined =
        getExplorerSchemaScopeForNode(element);
      if (!scope) {
        return;
      }

      connectionManager.markSchemaScopeExpanded(element.connectionId, scope);
      connectionManager.ensureSchemaScopeLoading(element.connectionId, scope);
    }),
    treeView.onDidCollapseElement(({ element }) => {
      const scope = getExplorerSchemaScopeForNode(element);
      if (!scope) {
        return;
      }

      connectionManager.markSchemaScopeCollapsed(element.connectionId, scope);
      if (isConnectionRootNode(element)) {
        connectionProvider.markConnectionRootExpanded(
          element.connectionId,
          false,
        );
      }
    }),
  ];
}

function createSupportTreeView<
  TProvider extends { disposable: vscode.Disposable },
>(
  id: "rapidb-history" | "rapidb-bookmarks",
  provider: TProvider,
): vscode.TreeView<unknown> {
  return vscode.window.createTreeView(id, {
    treeDataProvider: provider as unknown as vscode.TreeDataProvider<unknown>,
    showCollapseAll: false,
  });
}

function activateOnce(context: vscode.ExtensionContext): void {
  if (hasActiveState()) {
    logger.warn("activate() called again — skipping duplicate registration");
    return;
  }

  logger.info("Extension activated");

  if (typeof context.globalStorageUri?.fsPath === "string") {
    configureSQLiteInstaller({
      storageRoot: context.globalStorageUri.fsPath,
      log: (message) => logger.info(message),
    });
    void warmupSQLiteRuntime(__dirname).catch((err: unknown) => {
      // Best-effort warmup — failures shouldn't block extension activation,
      // but unhandled rejections hide real issues in production logs.
      logger.error("SQLite runtime warmup failed", err);
    });
  }

  const connectionManager = new ConnectionManager(context);
  const connectionProvider = new ConnectionProvider(connectionManager);
  const explorerView = vscode.window.createTreeView("rapidb-explorer", {
    treeDataProvider: connectionProvider,
    dragAndDropController: connectionProvider,
    showCollapseAll: true,
  });

  const disposables: vscode.Disposable[] = [
    ...trackExplorerTreeView(explorerView, {
      context,
      connectionManager,
      connectionProvider,
      refresh: () => connectionProvider.refresh(),
    }),
  ];

  const updateExplorerBadge = createBadgeUpdater(
    explorerView,
    connectionManager,
  );
  updateExplorerBadge();

  disposables.push(
    explorerView,
    connectionProvider.disposable,
    connectionManager.onDidConnect(updateExplorerBadge),
    connectionManager.onDidDisconnect(updateExplorerBadge),
  );

  const historyProvider = new HistoryProvider(connectionManager);
  const historyView = createSupportTreeView("rapidb-history", historyProvider);
  disposables.push(historyView, historyProvider.disposable);

  const bookmarksProvider = new BookmarksProvider(connectionManager);
  const bookmarksView = createSupportTreeView(
    "rapidb-bookmarks",
    bookmarksProvider,
  );
  disposables.push(bookmarksView, bookmarksProvider.disposable);

  const services: ActivationServices = {
    context,
    connectionManager,
    connectionProvider,
    refresh: () => connectionProvider.refresh(),
  };

  const registrar = createCommandRegistrar(context);
  registerAllCommands(services, registrar);

  if (registrar.failures.length > 0) {
    logger.warn(
      `Skipped ${registrar.failures.length} command(s): ${registrar.failures.join(", ")}`,
    );
  }

  // Persist disposables so deactivate can clean them up.
  for (const d of disposables) {
    context.subscriptions.push(d);
  }

  setActiveState({ services, disposables, connectionManager });
}

export function activate(context: vscode.ExtensionContext): void {
  activateOnce(context);
}

export async function deactivate(): Promise<void> {
  const state = takeActiveState();

  QueryPanel.disposeAll();
  TablePanel.disposeAll();
  ErdPanel.disposeAll();

  if (state) {
    for (const d of state.disposables) {
      d.dispose();
    }
  }

  if (state?.connectionManager) {
    try {
      await state.connectionManager.dispose();
    } catch (e) {
      logger.error("Failed to dispose connection manager", e);
    }
  }

  logger.info("Extension deactivated");
}
