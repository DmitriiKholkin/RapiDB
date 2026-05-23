import type { BookmarkEntry, ConnectionManager } from "../connectionManager";
import { SqlEntryNode, SqlEntryProvider } from "./sqlEntryProvider";

export class BookmarkNode extends SqlEntryNode<BookmarkEntry> {
  constructor(entry: BookmarkEntry, connectionName: string) {
    super(entry, {
      connectionName,
      iconId: "bookmark",
      contextValue: "bookmarkEntry",
      dateLabel: new Date(entry.savedAt).toLocaleString(),
      command: "rapidb.openBookmarkEntry",
      commandTitle: "Open in Query Editor",
    });
  }
}

export class BookmarksProvider extends SqlEntryProvider<
  BookmarkEntry,
  BookmarkNode
> {
  constructor(cm: ConnectionManager) {
    super(cm, cm.onDidChangeBookmarks);
  }

  protected getEntries(): BookmarkEntry[] {
    return this.cm.getBookmarks();
  }

  protected makeNode(
    entry: BookmarkEntry,
    connectionName: string,
  ): BookmarkNode {
    return new BookmarkNode(entry, connectionName);
  }
}
