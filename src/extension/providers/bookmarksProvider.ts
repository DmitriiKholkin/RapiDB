import type { BookmarkEntry, ConnectionManager } from "../connectionManager";
import { createSqlEntryProvider, SqlEntryNode } from "./sqlEntryProvider";

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

export const BookmarksProvider = createSqlEntryProvider<
  BookmarkEntry,
  BookmarkNode
>({
  onDidChange: (cm: ConnectionManager) => cm.onDidChangeBookmarks,
  getEntries: (cm: ConnectionManager) => cm.getBookmarks(),
  makeNode: (entry: BookmarkEntry, connectionName: string) =>
    new BookmarkNode(entry, connectionName),
});
