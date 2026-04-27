import * as vscode from "vscode";
import type { BookmarkEntry, ConnectionManager } from "../connectionManager";
import { extractFirstSqlLine, SqlEntryProvider } from "./sqlEntryProvider";

export class BookmarkNode extends vscode.TreeItem {
  constructor(
    public readonly entry: BookmarkEntry,
    connectionName: string,
  ) {
    super(extractFirstSqlLine(entry.sql), vscode.TreeItemCollapsibleState.None);

    this.id = entry.id;
    this.contextValue = "bookmarkEntry";
    this.iconPath = new vscode.ThemeIcon("bookmark");
    this.description = connectionName;

    const dateStr = new Date(entry.savedAt).toLocaleString();
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

export class BookmarksProvider extends SqlEntryProvider<BookmarkEntry, BookmarkNode> {
  constructor(cm: ConnectionManager) {
    super(cm, cm.onDidChangeBookmarks);
  }

  protected getEntries(): BookmarkEntry[] {
    return this.cm.getBookmarks();
  }

  protected makeNode(entry: BookmarkEntry, connectionName: string): BookmarkNode {
    return new BookmarkNode(entry, connectionName);
  }
}
