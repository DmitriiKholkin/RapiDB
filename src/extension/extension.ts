import * as vscode from "vscode";
import type { BookmarkEntry, HistoryEntry } from "./connectionManager";
import { ConnectionManager } from "./connectionManager";
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

let _activated = false;

let _connectionManager: import("./connectionManager").ConnectionManager | null =
  null;

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
  context.subscriptions.push(treeView, connectionProvider.disposable);

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
  const connectionProgressMap = new Map<string, Promise<boolean>>();

  async function connectIfNeeded(
    connectionId: string,
    title: string,
    waitForExisting: boolean,
  ): Promise<boolean> {
    if (connectionManager.isConnected(connectionId)) {
      return true;
    }

    const existingProgress = connectionProgressMap.get(connectionId);
    if (existingProgress) {
      if (!waitForExisting) {
        return false;
      }
      return existingProgress;
    }

    if (connectionManager.isConnecting(connectionId)) {
      if (!waitForExisting) {
        return false;
      }
      await connectionManager.connectTo(connectionId);
      return connectionManager.isConnected(connectionId);
    }

    const progressPromise = Promise.resolve(
      vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Window,
          title,
        },
        () => connectionManager.connectTo(connectionId),
      ),
    ).then(() => connectionManager.isConnected(connectionId));

    connectionProgressMap.set(connectionId, progressPromise);
    try {
      return await progressPromise;
    } finally {
      connectionProgressMap.delete(connectionId);
    }
  }

  function reg(
    command: string,
    callback: (...args: any[]) => any,
  ): vscode.Disposable {
    try {
      const d = vscode.commands.registerCommand(command, callback);
      context.subscriptions.push(d);
      return d;
    } catch (err: any) {
      console.warn(`[RapiDB] Could not register "${command}": ${err?.message}`);
      return { dispose: () => {} };
    }
  }

  reg("rapidb.addConnection", async () => {
    const result = await ConnectionFormPanel.show(context, connectionManager);
    if (result) {
      vscode.window.showInformationMessage(
        `[RapiDB] Connection "${result.name}" saved.`,
      );
      refresh();
    }
  });

  reg("rapidb.editConnection", async (node?: RapiDBNode) => {
    const id = node?.connectionId ?? (await connectionManager.pickConnection());
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

  reg("rapidb.deleteConnection", async (node?: RapiDBNode) => {
    const id = node?.connectionId ?? (await connectionManager.pickConnection());
    if (!id) {
      return;
    }
    const deleted = await connectionManager.deleteConnection(id);
    if (deleted) {
      refresh();
    }
  });

  reg("rapidb.connect", async (node?: RapiDBNode) => {
    const id = node?.connectionId ?? (await connectionManager.pickConnection());
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
      await connectIfNeeded(
        id,
        `RapiDB: Connecting to "${conn?.name ?? id}"…`,
        false,
      );
      refresh();
    } catch (err: any) {
      const action = await vscode.window.showErrorMessage(
        `[RapiDB] Cannot connect to "${conn?.name ?? id}": ${err.message}`,
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

  reg("rapidb.disconnect", async (node?: RapiDBNode) => {
    const id = node?.connectionId ?? (await connectionManager.pickConnection());
    if (!id) {
      return;
    }
    await connectionManager.disconnectFrom(id);
    refresh();
  });

  reg("rapidb.newQuery", async (node?: RapiDBNode) => {
    const connectionId =
      node?.connectionId ?? (await connectionManager.pickConnection());
    if (!connectionId) {
      return;
    }
    if (!connectionManager.isConnected(connectionId)) {
      try {
        await connectIfNeeded(connectionId, "RapiDB: Connecting…", true);
        refresh();
      } catch (err: any) {
        vscode.window.showErrorMessage(
          `[RapiDB] Cannot connect: ${err.message}`,
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

  reg("rapidb.openTableData", (node?: RapiDBNode) => {
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

  reg("rapidb.showDDL", async (node?: RapiDBNode) => {
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
    } catch (err: any) {
      vscode.window.showErrorMessage(
        `[RapiDB] DDL error: ${err?.message ?? String(err)}`,
      );
    }
  });

  reg("rapidb.copyNodeName", async (node?: RapiDBNode) => {
    const name = node?.objectName ?? node?.label?.toString();
    if (name) {
      await vscode.env.clipboard.writeText(name);
    }
  });

  reg("rapidb.openSchema", (node?: RapiDBNode) => {
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

  reg("rapidb.openRoutine", async (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }
    const kind = node.kind as "function" | "procedure";

    if (!connectionManager.isConnected(node.connectionId)) {
      try {
        await connectIfNeeded(node.connectionId, "RapiDB: Connecting…", true);
        refresh();
      } catch (err: any) {
        vscode.window.showErrorMessage(
          `[RapiDB] Cannot connect: ${err.message}`,
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
    } catch (err: any) {
      vscode.window.showErrorMessage(
        `[RapiDB] Cannot load ${kind} definition: ${err.message}`,
      );
    }
  });

  reg("rapidb.openHistoryEntry", (entry: HistoryEntry) => {
    if (!entry?.connectionId || !entry?.sql) {
      return;
    }
    QueryPanel.createOrShow(
      context,
      connectionManager,
      entry.connectionId,
      entry.sql,
    );
  });

  reg("rapidb.openBookmarkEntry", (entry: BookmarkEntry) => {
    if (!entry?.connectionId || !entry?.sql) {
      return;
    }
    QueryPanel.createOrShow(
      context,
      connectionManager,
      entry.connectionId,
      entry.sql,
      true,
      false,
      true,
    );
  });

  reg("rapidb.deleteBookmark", async (node?: any) => {
    const id = node?.entry?.id ?? node?.id;
    if (!id) {
      return;
    }
    await connectionManager.deleteBookmark(id);
  });

  reg("rapidb.clearBookmarks", async () => {
    const answer = await vscode.window.showWarningMessage(
      "[RapiDB] Clear all bookmarks?",
      { modal: true },
      "Clear",
    );
    if (answer === "Clear") {
      await connectionManager.clearBookmarks();
      vscode.window.showInformationMessage("[RapiDB] All bookmarks cleared.");
    }
  });

  reg("rapidb.clearHistory", async () => {
    const answer = await vscode.window.showWarningMessage(
      "[RapiDB] Clear all query history?",
      { modal: true },
      "Clear",
    );
    if (answer === "Clear") {
      await connectionManager.clearHistory();
      vscode.window.showInformationMessage("[RapiDB] Query history cleared.");
    }
  });

  reg("rapidb.disconnectAll", async () => {
    await connectionManager.disconnectAll();
    refresh();
  });

  reg("rapidb.refresh", () => {
    refresh();
  });
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
