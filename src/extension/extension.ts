import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../shared/commandIds";
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
import {
  configureSQLiteInstaller,
  warmupSQLiteRuntime,
} from "./utils/sqliteInstaller";

let _activated = false;
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

type ActivationCommandRegistrar = (
  services: ActivationServices,
  reg: RegisterCommand,
) => void;

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

function readNodeLocation(node: RapiDBNode): {
  database: string;
  schema: string;
} {
  return {
    database: node.database ?? "",
    schema: node.schema ?? "",
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

async function ensureConnectionReady(
  connectionManager: ConnectionManager,
  connectionId: string,
  refresh: () => void,
  failureContext: string,
): Promise<boolean> {
  if (connectionManager.isConnected(connectionId)) {
    return true;
  }

  try {
    await connectWithProgress(
      connectionManager,
      connectionId,
      "RapiDB: Connecting…",
      true,
    );
    refresh();
    return true;
  } catch (err: unknown) {
    const error = logErrorWithContext(failureContext, err);
    vscode.window.showErrorMessage(`[RapiDB] Cannot connect: ${error.message}`);
    return false;
  }
}

function openDdlInQueryPanel(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  connectionId: string,
  ddl: string,
  presentation: {
    formatOnOpen: boolean;
    editorLanguage?: QueryEditorLanguage;
  },
): void {
  if (presentation.editorLanguage) {
    QueryPanel.createOrShow(
      context,
      connectionManager,
      connectionId,
      ddl,
      true,
      presentation.formatOnOpen,
      false,
      presentation.editorLanguage,
    );
    return;
  }

  QueryPanel.createOrShow(
    context,
    connectionManager,
    connectionId,
    ddl,
    true,
    presentation.formatOnOpen,
  );
}

function registerConnectionCommands(
  services: ActivationServices,
  reg: RegisterCommand,
): void {
  const { context, connectionManager, refresh } = services;

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
}

function registerExplorerCommands(
  services: ActivationServices,
  reg: RegisterCommand,
): void {
  const { context, connectionManager, refresh } = services;

  reg(CMD.newQuery, async (node?: RapiDBNode) => {
    const connectionId = await resolveConnectionId(node, connectionManager);
    if (!connectionId) {
      return;
    }
    const connected = await ensureConnectionReady(
      connectionManager,
      connectionId,
      refresh,
      `New query connect failed for ${connectionId}`,
    );
    if (!connected) {
      return;
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
    const location = readNodeLocation(node);
    TablePanel.createOrShow(
      context,
      connectionManager,
      node.connectionId,
      location.database,
      location.schema,
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
      let attemptedSupportedLookup = false;
      const objectKind = isDbObjectKind(node.kind) ? node.kind : undefined;
      const location = readNodeLocation(node);
      if (
        (node.kind === "table" ||
          node.kind === "view" ||
          node.kind === "materializedView") &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getCreateTableDDL(
          location.database,
          location.schema,
          node.objectName,
        );
      } else if (
        objectKind &&
        isRoutineDbObjectKind(objectKind) &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getRoutineDefinition(
          location.database,
          location.schema,
          node.objectName,
          objectKind,
          node.detailKey,
        );
      } else if (
        objectKind &&
        isDdlOnlyDbObjectKind(objectKind) &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getObjectDefinition(
          location.database,
          location.schema,
          node.objectName,
          objectKind,
        );
      } else if (
        node.kind === "table_detail_constraint" &&
        node.parentTable &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getConstraintDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      } else if (
        node.kind === "table_detail_index" &&
        node.parentTable &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getIndexDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      } else if (
        node.kind === "table_detail_trigger" &&
        node.parentTable &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getTriggerDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      }
      if (!ddl) {
        if (attemptedSupportedLookup) {
          const kindLabel = objectKind ?? node.kind;
          const objectName =
            node.objectName ?? node.parentTable ?? "selected node";
          vscode.window.showWarningMessage(
            `[RapiDB] DDL is currently unavailable for ${kindLabel} "${objectName}". Check object permissions (for example, DBMS_METADATA access on Oracle) and retry.`,
          );
        } else {
          vscode.window.showWarningMessage(
            "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
          );
        }
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
      const ddlPresentationResolved = {
        formatOnOpen: ddlPresentation.formatOnOpen ?? false,
        editorLanguage: ddlPresentation.editorLanguage,
      };
      if (objectKind && isRoutineDbObjectKind(objectKind)) {
        QueryPanel.createOrShow(
          context,
          connectionManager,
          node.connectionId,
          ddl,
        );
      } else {
        openDdlInQueryPanel(
          context,
          connectionManager,
          node.connectionId,
          ddl,
          ddlPresentationResolved,
        );
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

    const connected = await ensureConnectionReady(
      connectionManager,
      connectionId,
      refresh,
      `Open ERD connect failed for ${connectionId}`,
    );
    if (!connected) {
      return;
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
    const connected = await ensureConnectionReady(
      connectionManager,
      node.connectionId,
      refresh,
      `Open routine connect failed for ${node.objectName}`,
    );
    if (!connected) {
      return;
    }
    const driver = connectionManager.getDriver(node.connectionId);
    if (!driver) {
      return;
    }
    try {
      const location = readNodeLocation(node);
      const sql = await driver.getRoutineDefinition(
        location.database,
        location.schema,
        node.objectName,
        kind,
        node.detailKey,
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
}

function registerSavedEntryCommands(
  services: ActivationServices,
  reg: RegisterCommand,
): void {
  const { context, connectionManager } = services;

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
}

function registerUtilityCommands(
  services: ActivationServices,
  reg: RegisterCommand,
): void {
  const { connectionManager, connectionProvider, refresh } = services;

  reg(CMD.refresh, (node?: RapiDBNode) => {
    connectionManager.refreshSchemaCache({
      connectionId: node?.connectionId,
      reason: "manual",
    });
    connectionProvider.refreshConnectionTree(node?.connectionId);
  });

  reg(CMD.showConnectedOnly, async () => {
    await connectionProvider.toggleConnectedOnly();
  });

  reg(CMD.showAllConnections, async () => {
    await connectionProvider.toggleConnectedOnly();
  });

  reg(CMD.disconnectAll, async () => {
    await connectionManager.disconnectAll();
    refresh();
  });
}

function registerCommands(
  services: ActivationServices,
  reg: RegisterCommand,
): void {
  const registrars: readonly ActivationCommandRegistrar[] = [
    registerConnectionCommands,
    registerExplorerCommands,
    registerSavedEntryCommands,
    registerUtilityCommands,
  ];

  for (const registerCommandGroup of registrars) {
    registerCommandGroup(services, reg);
  }
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
  if (typeof context.globalStorageUri?.fsPath === "string") {
    configureSQLiteInstaller({
      storageRoot: context.globalStorageUri.fsPath,
      log: (message) => console.log(message),
    });
    void warmupSQLiteRuntime(__dirname);
  }
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
