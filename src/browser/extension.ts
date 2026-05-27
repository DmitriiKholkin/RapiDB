import * as vscode from "vscode";

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

const EXPLORER_MESSAGE =
  "RapiDB runs in degraded browser mode here. SQLite and other desktop-only drivers need the desktop extension host.";
const HISTORY_MESSAGE =
  "Query history is unavailable in the browser placeholder mode.";
const BOOKMARKS_MESSAGE =
  "Bookmarks are unavailable in the browser placeholder mode.";

class PlaceholderProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
  private readonly emitter = new vscode.EventEmitter<void>();

  constructor(private readonly lines: readonly string[]) {}

  readonly onDidChangeTreeData = this.emitter.event;

  refresh(): void {
    this.emitter.fire();
  }

  getTreeItem(item: vscode.TreeItem): vscode.TreeItem {
    return item;
  }

  getChildren(): vscode.TreeItem[] {
    return this.lines.map((line, index) => {
      const item = new vscode.TreeItem(
        line,
        vscode.TreeItemCollapsibleState.None,
      );
      item.id = `rapidb-browser-placeholder-${index}-${line}`;
      item.contextValue = "placeholder";
      return item;
    });
  }
}

async function openSqlDocument(contents: string): Promise<void> {
  const document = await vscode.workspace.openTextDocument({
    language: "sql",
    content: contents,
  });
  await vscode.window.showTextDocument(document, { preview: false });
}

function showBrowserUnsupportedMessage(
  action: string,
): Thenable<string | undefined> {
  return vscode.window.showWarningMessage(
    `[RapiDB] ${action} is not available in the browser target. Use desktop VS Code for SQLite and other desktop-only drivers.`,
  );
}

export async function activate(
  context: vscode.ExtensionContext,
): Promise<void> {
  const explorerProvider = new PlaceholderProvider([EXPLORER_MESSAGE]);
  const historyProvider = new PlaceholderProvider([HISTORY_MESSAGE]);
  const bookmarksProvider = new PlaceholderProvider([BOOKMARKS_MESSAGE]);

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("rapidb-explorer", explorerProvider),
    vscode.window.registerTreeDataProvider("rapidb-history", historyProvider),
    vscode.window.registerTreeDataProvider(
      "rapidb-bookmarks",
      bookmarksProvider,
    ),
  );

  const register = <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => {
    context.subscriptions.push(
      vscode.commands.registerCommand(command, callback),
    );
  };

  register(CMD.newQuery, async () => {
    await openSqlDocument("-- RapiDB browser target scratch query\n");
  });

  register(CMD.openHistoryEntry, async (entry?: { sql?: string }) => {
    await openSqlDocument(
      entry?.sql ?? "-- History entry is unavailable in browser mode\n",
    );
  });

  register(CMD.openBookmarkEntry, async (entry?: { sql?: string }) => {
    await openSqlDocument(
      entry?.sql ?? "-- Bookmark entry is unavailable in browser mode\n",
    );
  });

  register(CMD.copyNodeName, async (node?: { label?: string }) => {
    const value =
      typeof node?.label === "string" ? node.label : EXPLORER_MESSAGE;
    await vscode.env.clipboard.writeText(value);
  });

  register(CMD.refresh, async () => {
    explorerProvider.refresh();
    historyProvider.refresh();
    bookmarksProvider.refresh();
  });

  for (const [command, label] of [
    [CMD.addConnection, "Adding connections"],
    [CMD.editConnection, "Editing connections"],
    [CMD.deleteConnection, "Deleting connections"],
    [CMD.renameConnectionFolder, "Renaming folders"],
    [CMD.deleteConnectionFolder, "Deleting folders"],
    [CMD.connect, "Connecting"],
    [CMD.disconnect, "Disconnecting"],
    [CMD.openTableData, "Opening table data"],
    [CMD.showDDL, "Showing DDL"],
    [CMD.openRoutine, "Opening routine definitions"],
    [CMD.openErd, "Opening ERD"],
    [CMD.deleteBookmark, "Deleting bookmarks"],
    [CMD.clearBookmarks, "Clearing bookmarks"],
    [CMD.clearHistory, "Clearing history"],
    [CMD.disconnectAll, "Disconnecting all connections"],
  ] as const) {
    register(command, async () => {
      await showBrowserUnsupportedMessage(label);
    });
  }

  console.log("[RapiDB] Browser extension activated");
}

export function deactivate(): void {}
