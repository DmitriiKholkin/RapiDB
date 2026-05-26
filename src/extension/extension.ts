import * as vscode from "vscode";
import {
  isDbObjectKind,
  isDdlOnlyDbObjectKind,
  isRoutineDbObjectKind,
} from "../shared/dbObjectKinds";
import type { QueryEditorLanguage } from "../shared/webviewContracts";
import type {
  BookmarkEntry,
  ExplorerSchemaScope,
  HistoryEntry,
} from "./connectionManager";
import { ConnectionManager } from "./connectionManager";
import {
  confirmBookmarkRemoval,
  confirmConnectionFolderRemoval,
  confirmConnectionRemoval,
  pickConnectionWithPrompt,
} from "./connectionManagerPrompts";
import { DEFAULT_DRIVER_ENTITY_MANIFEST } from "./dbDrivers/types";
import { ConnectionFormPanel } from "./panels/connectionFormPanel";
import { ErdPanel } from "./panels/erdPanel";
import { QueryPanel } from "./panels/queryPanel";
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
import { isOpenDdlSupportedForNode } from "./utils/openDdlEligibility";

let _activated = false;
const CMD = {
  addConnection: "rapidb.addConnection",
  editConnection: "rapidb.editConnection",
  deleteConnection: "rapidb.deleteConnection",
  renameConnectionFolder: "rapidb.renameConnectionFolder",
  deleteConnectionFolder: "rapidb.deleteConnectionFolder",
  connect: "rapidb.connect",
  disconnect: "rapidb.disconnect",
  newQuery: "rapidb.newQuery",
  openTableData: "rapidb.openTableData",
  showDDL: "rapidb.showDDL",
  copyNodeName: "rapidb.copyNodeName",
  openRoutine: "rapidb.openRoutine",
  openHistoryEntry: "rapidb.openHistoryEntry",
  openBookmarkEntry: "rapidb.openBookmarkEntry",
  openErd: "rapidb.openErd",
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

function getErdScopeForNode(node: RapiDBNode | undefined): {
  database?: string;
  schema?: string;
} {
  if (!node) {
    return {};
  }

  return {
    database: node.database,
    schema: node.schema,
  };
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

function getOpenDdlPresentation(): {
  formatOnOpen: boolean;
  editorLanguage?: QueryEditorLanguage;
} {
  return {
    formatOnOpen: true,
  };
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

  reg(CMD.renameConnectionFolder, async (node?: RapiDBNode) => {
    const currentFolderName =
      node?.kind === "folder" ? node.objectName?.trim() : "";
    if (!currentFolderName) {
      return;
    }

    const nextFolderName = await vscode.window.showInputBox({
      title: "Rename Connection Folder",
      prompt: `Rename folder "${currentFolderName}"`,
      value: currentFolderName,
      ignoreFocusOut: true,
      validateInput: (value) => {
        const trimmedValue = value.trim();
        if (!trimmedValue) {
          return "Folder name cannot be empty.";
        }
        if (trimmedValue === currentFolderName) {
          return "Enter a new folder name.";
        }
        return null;
      },
    });

    if (nextFolderName === undefined) {
      return;
    }

    const renamedCount = await connectionManager.renameFolder(
      currentFolderName,
      nextFolderName,
    );
    if (renamedCount === 0) {
      return;
    }

    vscode.window.showInformationMessage(
      `[RapiDB] Folder "${currentFolderName}" renamed to "${nextFolderName.trim()}".`,
    );
    refresh();
  });

  reg(CMD.deleteConnectionFolder, async (node?: RapiDBNode) => {
    const folderName = node?.kind === "folder" ? node.objectName?.trim() : "";
    if (!folderName) {
      return;
    }

    const deleted = await confirmConnectionFolderRemoval(
      connectionManager,
      folderName,
    );
    if (!deleted) {
      return;
    }

    vscode.window.showInformationMessage(
      `[RapiDB] Folder "${folderName}" deleted. Connections moved to the root level.`,
    );
    refresh();
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
      undefined,
      true,
    );
  });

  reg(CMD.openTableData, (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }
    const isView = node.kind === "view" || node.kind === "materializedView";
    TablePanel.createOrShow(
      context,
      connectionManager,
      node.connectionId,
      node.database ?? "",
      node.schema ?? "",
      node.objectName,
      isView,
      node.kind === "materializedView"
        ? "materializedView"
        : node.kind === "view"
          ? "view"
          : "table",
    );
  });

  reg(CMD.showDDL, async (node?: RapiDBNode) => {
    if (!node?.connectionId) {
      vscode.window.showWarningMessage(
        "[RapiDB] Select a table, view, materialized view, function, procedure, sequence, type, constraint, index, or trigger node first.",
      );
      return;
    }
    const connectionType = connectionManager.getConnection(
      node.connectionId,
    )?.type;
    const managerWithManifest = connectionManager as ConnectionManager & {
      getDriverEntityManifest?: (
        id: string,
      ) => typeof DEFAULT_DRIVER_ENTITY_MANIFEST;
    };
    const entityManifest =
      managerWithManifest.getDriverEntityManifest?.(node.connectionId) ??
      DEFAULT_DRIVER_ENTITY_MANIFEST;
    if (
      !isOpenDdlSupportedForNode(node.kind, connectionType, entityManifest, {
        indexDdlSupport: node.ddlSupport,
      })
    ) {
      vscode.window.showWarningMessage(
        "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
      );
      return;
    }

    const driver = connectionManager.getDriver(node.connectionId);
    if (!driver) {
      vscode.window.showErrorMessage("[RapiDB] Not connected. Connect first.");
      return;
    }
    try {
      let ddl: string | null = null;
      const objectKind = isDbObjectKind(node.kind) ? node.kind : undefined;
      if (
        (node.kind === "table" ||
          node.kind === "view" ||
          node.kind === "materializedView") &&
        node.objectName
      ) {
        ddl = await driver.getCreateTableDDL(
          node.database ?? "",
          node.schema ?? "",
          node.objectName,
        );
      } else if (
        objectKind &&
        isRoutineDbObjectKind(objectKind) &&
        node.objectName
      ) {
        ddl = await driver.getRoutineDefinition(
          node.database ?? "",
          node.schema ?? "",
          node.objectName,
          objectKind,
        );
      } else if (
        objectKind &&
        isDdlOnlyDbObjectKind(objectKind) &&
        node.objectName
      ) {
        ddl = await driver.getObjectDefinition(
          node.database ?? "",
          node.schema ?? "",
          node.objectName,
          objectKind,
        );
      } else if (
        node.kind === "table_detail_constraint" &&
        node.parentTable &&
        node.objectName
      ) {
        ddl = await driver.getConstraintDDL(
          node.database ?? "",
          node.schema ?? "",
          node.parentTable,
          node.objectName,
        );
      } else if (
        node.kind === "table_detail_index" &&
        node.parentTable &&
        node.objectName
      ) {
        ddl = await driver.getIndexDDL(
          node.database ?? "",
          node.schema ?? "",
          node.parentTable,
          node.objectName,
        );
      } else if (
        node.kind === "table_detail_trigger" &&
        node.parentTable &&
        node.objectName
      ) {
        ddl = await driver.getTriggerDDL(
          node.database ?? "",
          node.schema ?? "",
          node.parentTable,
          node.objectName,
        );
      }
      if (!ddl) {
        vscode.window.showWarningMessage(
          "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
        );
        return;
      }
      const managerWithPresentation = connectionManager as ConnectionManager & {
        getQueryEditorPresentation?: (connectionId: string) =>
          | {
              formatOnOpen?: boolean;
              editorLanguage?: QueryEditorLanguage;
            }
          | undefined;
      };
      const ddlPresentation =
        managerWithPresentation.getQueryEditorPresentation?.(
          node.connectionId,
        ) ?? getOpenDdlPresentation();
      if (objectKind && isRoutineDbObjectKind(objectKind)) {
        QueryPanel.createOrShow(
          context,
          connectionManager,
          node.connectionId,
          ddl,
        );
      } else {
        if (ddlPresentation.editorLanguage) {
          QueryPanel.createOrShow(
            context,
            connectionManager,
            node.connectionId,
            ddl,
            true,
            ddlPresentation.formatOnOpen,
            false,
            ddlPresentation.editorLanguage,
          );
        } else {
          QueryPanel.createOrShow(
            context,
            connectionManager,
            node.connectionId,
            ddl,
            true,
            ddlPresentation.formatOnOpen,
          );
        }
      }
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

  reg(CMD.openErd, async (node?: RapiDBNode) => {
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
          `Open ERD connect failed for ${connectionId}`,
          err,
        );
        vscode.window.showErrorMessage(
          `[RapiDB] Cannot connect: ${error.message}`,
        );
        return;
      }
    }

    const scope = getErdScopeForNode(node);
    if (!scope.database) {
      vscode.window.showInformationMessage(
        "[RapiDB] Please open ERD from a database or schema node.",
      );
      return;
    }
    ErdPanel.createOrShow(context, connectionManager, {
      connectionId,
      database: scope.database,
      schema: scope.schema,
    });
  });

  reg(CMD.openRoutine, async (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }
    const objectKind = isDbObjectKind(node.kind) ? node.kind : undefined;
    if (!objectKind || !isRoutineDbObjectKind(objectKind)) {
      return;
    }
    const kind = objectKind;
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
    dragAndDropController: connectionProvider,
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
      if (
        element.kind === "connectionNode_connected" ||
        element.kind === "connectionNode_disconnected"
      ) {
        connectionProvider.markConnectionRootExpanded(
          element.connectionId,
          false,
        );
      }
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
export async function deactivate(): Promise<void> {
  _activated = false;
  QueryPanel.disposeAll();
  TablePanel.disposeAll();
  ErdPanel.disposeAll();
  const connectionManager = _connectionManager;
  _connectionManager = null;
  try {
    if (connectionManager) {
      await connectionManager.dispose();
    }
  } catch {}
  console.log("[RapiDB] Extension deactivated");
}
