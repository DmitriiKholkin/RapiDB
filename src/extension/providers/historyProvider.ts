import type { ConnectionManager, HistoryEntry } from "../connectionManager";
import { SqlEntryNode, SqlEntryProvider } from "./sqlEntryProvider";

export class HistoryNode extends SqlEntryNode<HistoryEntry> {
  constructor(entry: HistoryEntry, connectionName: string) {
    super(entry, {
      connectionName,
      iconId: "history",
      contextValue: "historyEntry",
      dateLabel: new Date(entry.executedAt).toLocaleString(),
      command: "rapidb.openHistoryEntry",
      commandTitle: "Open in Query Editor",
    });
  }
}

export class HistoryProvider extends SqlEntryProvider<
  HistoryEntry,
  HistoryNode
> {
  constructor(cm: ConnectionManager) {
    super(cm, cm.onDidChangeHistory);
  }

  protected override getEntries(): HistoryEntry[] {
    return this.cm.getHistory();
  }

  protected override makeNode(
    entry: HistoryEntry,
    connectionName: string,
  ): HistoryNode {
    return new HistoryNode(entry, connectionName);
  }
}
