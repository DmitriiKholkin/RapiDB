import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../../shared/commandIds";
import type { ConnectionManager } from "../connectionManager";
import {
  confirmConnectionFolderRemoval,
  confirmConnectionRemoval,
} from "../connectionManagerPrompts";
import { ConnectionFormPanel } from "../panels/connectionFormPanel";
import type { RapiDBNode } from "../providers/connectionProvider";
import { connectWithProgress } from "../utils/connectOrchestration";
import { logErrorWithContext } from "../utils/errorHandling";
import { resolveConnectionId } from "../utils/resolveConnectionId";

export interface ConnectionCommandContext {
  readonly context: vscode.ExtensionContext;
  readonly connectionManager: ConnectionManager;
  readonly refresh: () => void;
}

export function registerConnectionCommands(
  ctx: ConnectionCommandContext,
  registerCommand: <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => vscode.Disposable,
): void {
  const { context, connectionManager, refresh } = ctx;

  registerCommand(CMD.addConnection, async () => {
    const result = await ConnectionFormPanel.show(context, connectionManager);
    if (result) {
      vscode.window.showInformationMessage(
        `[RapiDB] Connection "${result.name}" saved.`,
      );
      refresh();
    }
  });

  registerCommand(CMD.editConnection, async (node?: RapiDBNode) => {
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

  registerCommand(CMD.deleteConnection, async (node?: RapiDBNode) => {
    const id = await resolveConnectionId(node, connectionManager);
    if (!id) {
      return;
    }

    const deleted = await confirmConnectionRemoval(connectionManager, id);
    if (deleted) {
      refresh();
    }
  });

  registerCommand(CMD.renameConnectionFolder, async (node?: RapiDBNode) => {
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

  registerCommand(CMD.deleteConnectionFolder, async (node?: RapiDBNode) => {
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

  registerCommand(CMD.connect, async (node?: RapiDBNode) => {
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
        undefined,
        "explicit",
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

  registerCommand(CMD.disconnect, async (node?: RapiDBNode) => {
    const id = await resolveConnectionId(node, connectionManager);
    if (!id) {
      return;
    }

    await connectionManager.disconnectFrom(id);
    refresh();
  });
}
