import * as vscode from "vscode";
import type {
  BookmarkEntry,
  ExplorerSchemaScope,
  HistoryEntry,
} from "./connectionManager";
import { ConnectionManager } from "./connectionManager";
import {
  confirmBookmarkRemoval,
  confirmConnectionRemoval,
  pickConnectionWithPrompt,
} from "./connectionManagerPrompts";
import { ConnectionFormPanel } from "./panels/connectionFormPanel";
import { QueryPanel } from "./panels/queryPanel";
import { SchemaPanel } from "./panels/schemaPanel";
import { TablePanel } from "./panels/tablePanel";
import { BookmarksProvider } from "./providers/bookmarksProvider";
import {
  ConnectionProvider,
  type RapiDBNode,
} from "./providers/connectionProvider";
import { HistoryProvider } from "./providers/historyProvider";
import { connectWithProgress } from "./utils/connectOrchestration";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "./utils/errorHandling";

let _activated = false;
const CMD = {
  addConnection: "rapidb.addConnection",
  editConnection: "rapidb.editConnection",
  deleteConnection: "rapidb.deleteConnection",
  connect: "rapidb.connect",
  disconnect: "rapidb.disconnect",
  newQuery: "rapidb.newQuery",
  openTableData: "rapidb.openTableData",
  showDDL: "rapidb.showDDL",
  copyNodeName: "rapidb.copyNodeName",
  openSchema: "rapidb.openSchema",
  openRoutine: "rapidb.openRoutine",
  openHistoryEntry: "rapidb.openHistoryEntry",
  openBookmarkEntry: "rapidb.openBookmarkEntry",
  deleteBookmark: "rapidb.deleteBookmark",
  clearBookmarks: "rapidb.clearBookmarks",
  clearHistory: "rapidb.clearHistory",
  disconnectAll: "rapidb.disconnectAll",
  refresh: "rapidb.refresh",
} as const;
let _connectionManager: import("./connectionManager").ConnectionManager | null =
  null;
async function resolveConnectionId(
  node: RapiDBNode | undefined,
  connectionManager: ConnectionManager,
): Promise<string | undefined> {
  return node?.connectionId ?? pickConnectionWithPrompt(connectionManager);
}

type RegisterCommand = <TArgs extends unknown[]>(
  command: string,
  callback: (...args: TArgs) => unknown,
) => vscode.Disposable;

type ActivationServices = {
  context: vscode.ExtensionContext;
  connectionManager: ConnectionManager;
  connectionProvider: ConnectionProvider;
  refresh: () => void;
};

function createBadgeUpdater(
  treeView: vscode.TreeView<RapiDBNode>,
  connectionManager: ConnectionManager,
): () => void {
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

function getExplorerSchemaScopeForNode(
  node: RapiDBNode | undefined,
): ExplorerSchemaScope | undefined {
  if (!node?.connectionId) {
    return undefined;
  }

  if (
    node.kind === "connectionNode_connected" ||
    node.kind === "connectionNode_disconnected"
  ) {
    return { kind: "connectionRoot" };
  }

  if (node.kind === "database" && node.database) {
    return {
      kind: "database",
      database: node.database,
    };
  }

  if (node.kind === "schema" && node.database && node.schema) {
    return {
      kind: "schema",
      database: node.database,
      schema: node.schema,
    };
  }

  return undefined;
}

function showSavedQuery(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  entry: Pick<HistoryEntry, "connectionId" | "sql">,
  options?: {
    forceNew?: boolean;
    formatOnOpen?: boolean;
    isBookmarked?: boolean;
  },
): void {
  if (!entry.connectionId || !entry.sql) {
    return;
  }

  QueryPanel.createOrShow(
    context,
    connectionManager,
    entry.connectionId,
    entry.sql,
    options?.forceNew,
    options?.formatOnOpen,
    options?.isBookmarked,
  );
}

async function clearSavedEntries(
  prompt: string,
  successMessage: string,
  action: () => Promise<void>,
): Promise<void> {
  const answer = await vscode.window.showWarningMessage(
    prompt,
    { modal: true },
    "Clear",
  );
  if (answer === "Clear") {
    await action();
    vscode.window.showInformationMessage(successMessage);
  }
}

function registerCommands(
  services: ActivationServices,
  reg: RegisterCommand,
): void {
  const { context, connectionManager, connectionProvider, refresh } = services;

  reg(CMD.addConnection, async () => {
    const result = await ConnectionFormPanel.show(context, connectionManager);
    if (result) {
      vscode.window.showInformationMessage(
        `[RapiDB] Connection "${result.name}" saved.`,
      );
      refresh();
    }
  });

  reg(CMD.editConnection, async (node?: RapiDBNode) => {
    const id = await resolveConnectionId(node, connectionManager);
    if (!id) {
      return;
    }
    const existing = connectionManager.getConnection(id);
    if (!existing) {
      return;
    }
    const result = await ConnectionFormPanel.show(
      context,
      connectionManager,
      existing,
    );
    if (result) {
      refresh();
    }
  });

  reg(CMD.deleteConnection, async (node?: RapiDBNode) => {
    const id = await resolveConnectionId(node, connectionManager);
    if (!id) {
      return;
    }
    const deleted = await confirmConnectionRemoval(connectionManager, id);
    if (deleted) {
      refresh();
    }
  });

  reg(CMD.connect, async (node?: RapiDBNode) => {
    const id = await resolveConnectionId(node, connectionManager);
    if (!id) {
      return;
    }
    if (
      connectionManager.isConnected(id) ||
      connectionManager.isConnecting(id)
    ) {
      refresh();
      return;
    }
    const conn = connectionManager.getConnection(id);
    try {
      await connectWithProgress(
        connectionManager,
        id,
        `RapiDB: Connecting to "${conn?.name ?? id}"…`,
        false,
      );
      refresh();
    } catch (err: unknown) {
      const error = logErrorWithContext(
        `Connect command failed for ${conn?.name ?? id}`,
        err,
      );
      const action = await vscode.window.showErrorMessage(
        `[RapiDB] Cannot connect to "${conn?.name ?? id}": ${error.message}`,
        "Edit Connection",
      );
      if (action === "Edit Connection") {
        const existing = connectionManager.getConnection(id);
        if (existing) {
          await ConnectionFormPanel.show(context, connectionManager, existing);
          refresh();
        }
      }
    }
  });

  reg(CMD.disconnect, async (node?: RapiDBNode) => {
    const id = await resolveConnectionId(node, connectionManager);
    if (!id) {
      return;
    }
    await connectionManager.disconnectFrom(id);
    refresh();
  });

  reg(CMD.newQuery, async (node?: RapiDBNode) => {
    const connectionId = await resolveConnectionId(node, connectionManager);
    if (!connectionId) {
      return;
    }
    if (!connectionManager.isConnected(connectionId)) {
      try {
        await connectWithProgress(
          connectionManager,
          connectionId,
          "RapiDB: Connecting…",
          true,
        );
        refresh();
      } catch (err: unknown) {
        const error = logErrorWithContext(
          `New query connect failed for ${connectionId}`,
          err,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Cannot connect: ${error.message}`,
        );
        return;
      }
    }
    QueryPanel.createOrShow(
      context,
      connectionManager,
      connectionId,
      undefined,
      true,
    );
  });

  reg(CMD.openTableData, (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }
    const isView = node.kind === "view";
    TablePanel.createOrShow(
      context,
      connectionManager,
      node.connectionId,
      node.database ?? "",
      node.schema ?? "",
      node.objectName,
      isView,
    );
  });

  reg(CMD.showDDL, async (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      vscode.window.showWarningMessage(
        "[RapiDB] Select a table or view node first.",
      );
      return;
    }
    const driver = connectionManager.getDriver(node.connectionId);
    if (!driver) {
      vscode.window.showErrorMessage("[RapiDB] Not connected. Connect first.");
      return;
    }
    try {
      const ddl = await driver.getCreateTableDDL(
        node.database ?? "",
        node.schema ?? "",
        node.objectName,
      );
      QueryPanel.createOrShow(
        context,
        connectionManager,
        node.connectionId,
        ddl,
        true,
        true,
      );
    } catch (err: unknown) {
      const error = logErrorWithContext(
        `Load DDL failed for ${node.objectName}`,
        err,
      );
      vscode.window.showErrorMessage(`[RapiDB] DDL error: ${error.message}`);
    }
  });

  reg(CMD.copyNodeName, async (node?: RapiDBNode) => {
    const name = node?.objectName ?? node?.label?.toString();
    if (name) {
      await vscode.env.clipboard.writeText(name);
    }
  });

  reg(CMD.openSchema, (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }
    SchemaPanel.createOrShow(
      context,
      connectionManager,
      node.connectionId,
      node.database ?? "",
      node.schema ?? "",
      node.objectName,
    );
  });

  reg(CMD.openRoutine, async (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }
    const kind = node.kind as "function" | "procedure";
    if (!connectionManager.isConnected(node.connectionId)) {
      try {
        await connectWithProgress(
          connectionManager,
          node.connectionId,
          "RapiDB: Connecting…",
          true,
        );
        refresh();
      } catch (err: unknown) {
        const error = logErrorWithContext(
          `Open routine connect failed for ${node.objectName}`,
          err,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Cannot connect: ${error.message}`,
        );
        return;
      }
    }
    const driver = connectionManager.getDriver(node.connectionId);
    if (!driver) {
      return;
    }
    try {
      const sql = await driver.getRoutineDefinition(
        node.database ?? "",
        node.schema ?? "",
        node.objectName,
        kind,
      );
      QueryPanel.createOrShow(
        context,
        connectionManager,
        node.connectionId,
        sql,
      );
    } catch (err: unknown) {
      const error = logErrorWithContext(
        `Load routine definition failed for ${node.objectName}`,
        err,
      );
      vscode.window.showErrorMessage(
        `[RapiDB] Cannot load ${kind} definition: ${error.message}`,
      );
    }
  });

  reg(CMD.openHistoryEntry, (entry: HistoryEntry) => {
    showSavedQuery(context, connectionManager, entry);
  });

  reg(CMD.openBookmarkEntry, (entry: BookmarkEntry) => {
    showSavedQuery(context, connectionManager, entry, {
      forceNew: true,
      formatOnOpen: false,
      isBookmarked: true,
    });
  });

  reg(
    CMD.deleteBookmark,
    async (node?: {
      entry?: {
        id?: string;
      };
      id?: string;
    }) => {
      const id = node?.entry?.id ?? node?.id;
      if (!id) {
        return;
      }
      await confirmBookmarkRemoval(connectionManager, id);
    },
  );

  reg(CMD.clearBookmarks, async () => {
    await clearSavedEntries(
      "[RapiDB] Clear all bookmarks?",
      "[RapiDB] All bookmarks cleared.",
      () => connectionManager.clearBookmarks(),
    );
  });

  reg(CMD.clearHistory, async () => {
    await clearSavedEntries(
      "[RapiDB] Clear all query history?",
      "[RapiDB] Query history cleared.",
      () => connectionManager.clearHistory(),
    );
  });

  reg(CMD.disconnectAll, async () => {
    await connectionManager.disconnectAll();
    refresh();
  });

  reg(CMD.refresh, (node?: RapiDBNode) => {
    connectionManager.refreshSchemaCache({
      connectionId: node?.connectionId,
      reason: "manual",
    });
    connectionProvider.refreshConnectionTree(node?.connectionId);
  });
}

export function activate(context: vscode.ExtensionContext): void {
  if (_activated) {
    console.warn(
      "[RapiDB] activate() called again — skipping duplicate registration",
    );
    return;
  }
  _activated = true;
  console.log("[RapiDB] Extension activated");
  const connectionManager = new ConnectionManager(context);
  _connectionManager = connectionManager;
  const connectionProvider = new ConnectionProvider(connectionManager);
  const treeView = vscode.window.createTreeView("rapidb-explorer", {
    treeDataProvider: connectionProvider,
    showCollapseAll: true,
  });
  context.subscriptions.push(
    treeView.onDidExpandElement(({ element }) => {
      const scope = getExplorerSchemaScopeForNode(element);
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
    }),
  );
  const updateExplorerBadge = createBadgeUpdater(treeView, connectionManager);
  updateExplorerBadge();
  context.subscriptions.push(treeView, connectionProvider.disposable);
  context.subscriptions.push(
    connectionManager.onDidConnect(() => {
      updateExplorerBadge();
    }),
    connectionManager.onDidDisconnect(() => {
      updateExplorerBadge();
    }),
  );
  const historyProvider = new HistoryProvider(connectionManager);
  const historyView = vscode.window.createTreeView("rapidb-history", {
    treeDataProvider: historyProvider,
    showCollapseAll: false,
  });
  context.subscriptions.push(historyView, historyProvider.disposable);
  const bookmarksProvider = new BookmarksProvider(connectionManager);
  const bookmarksView = vscode.window.createTreeView("rapidb-bookmarks", {
    treeDataProvider: bookmarksProvider,
    showCollapseAll: false,
  });
  context.subscriptions.push(bookmarksView, bookmarksProvider.disposable);
  const refresh = () => connectionProvider.refresh();
  function reg<TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ): vscode.Disposable {
    try {
      const d = vscode.commands.registerCommand(command, callback);
      context.subscriptions.push(d);
      return d;
    } catch (err: unknown) {
      const error = normalizeUnknownError(err);
      console.warn(
        `[RapiDB] Could not register "${command}": ${error.message}`,
      );
      return { dispose: () => {} };
    }
  }
  registerCommands(
    { context, connectionManager, connectionProvider, refresh },
    reg,
  );
}
export function deactivate(): void {
  _activated = false;
  QueryPanel.disposeAll();
  TablePanel.disposeAll();
  SchemaPanel.disposeAll();
  try {
    _connectionManager?.disconnectAll().catch(() => {});
  } catch {}
  _connectionManager = null;
  console.log("[RapiDB] Extension deactivated");
}
