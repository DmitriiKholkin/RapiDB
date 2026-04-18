import * as vscode from "vscode";
import type { BookmarkEntry, ConnectionManager } from "../connectionManager";

export class BookmarkNode extends vscode.TreeItem {
  constructor(
    public readonly entry: BookmarkEntry,
    connectionName: string,
  ) {
    const firstLine =
      entry.sql
        .split("\n")
        .map((l) => l.trim())
        .find((l) => l.length > 0) ?? entry.sql;

    super(firstLine, vscode.TreeItemCollapsibleState.None);

    this.id = entry.id;
    this.contextValue = "bookmarkEntry";
    this.iconPath = new vscode.ThemeIcon("bookmark");

    this.description = connectionName;

    const date = new Date(entry.savedAt);
    const dateStr = date.toLocaleString();
    this.tooltip = new vscode.MarkdownString(
      `**${connectionName}** — ${dateStr}\n\`\`\`sql\n${entry.sql}\n\`\`\``,
    );

    this.command = {
      command: "rapidb.openBookmarkEntry",
      title: "Open in SQL Editor",
      arguments: [entry],
    };
  }
}

export class BookmarksProvider
  implements vscode.TreeDataProvider<BookmarkNode>
{
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<
    BookmarkNode | undefined | null
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private readonly _subscription: vscode.Disposable;

  constructor(private readonly cm: ConnectionManager) {
    this._subscription = cm.onDidChangeBookmarks(() =>
      this._onDidChangeTreeData.fire(undefined),
    );
  }

  get disposable(): vscode.Disposable {
    return this._subscription;
  }

  getTreeItem(element: BookmarkNode): vscode.TreeItem {
    return element;
  }

  getChildren(_element?: BookmarkNode): BookmarkNode[] {
    if (_element) {
      return [];
    }

    const entries = this.cm.getBookmarks();
    if (entries.length === 0) {
      return [];
    }

    const connections = this.cm.getConnections();
    const nameMap = new Map<string, string>(
      connections.map((c) => [c.id, `${c.name} (${c.type})`]),
    );

    return entries.map(
      (e) => new BookmarkNode(e, nameMap.get(e.connectionId) ?? e.connectionId),
    );
  }
}
