import * as vscode from "vscode";
import type { ConnectionManager, HistoryEntry } from "../connectionManager";
import { extractFirstSqlLine, SqlEntryProvider } from "./sqlEntryProvider";

export class HistoryNode extends vscode.TreeItem {
  constructor(
    public readonly entry: HistoryEntry,
    connectionName: string,
  ) {
    super(extractFirstSqlLine(entry.sql), vscode.TreeItemCollapsibleState.None);

    this.id = entry.id;
    this.contextValue = "historyEntry";
    this.iconPath = new vscode.ThemeIcon("history");
    this.description = connectionName;

    const dateStr = new Date(entry.executedAt).toLocaleString();
    this.tooltip = new vscode.MarkdownString(
      `**${connectionName}** — ${dateStr}\n\`\`\`sql\n${entry.sql}\n\`\`\``,
    );

    this.command = {
      command: "rapidb.openHistoryEntry",
      title: "Open in SQL Editor",
      arguments: [entry],
    };
  }
}

export class HistoryProvider extends SqlEntryProvider<HistoryEntry, HistoryNode> {
  constructor(cm: ConnectionManager) {
    super(cm, cm.onDidChangeHistory);
  }

  protected getEntries(): HistoryEntry[] {
    return this.cm.getHistory();
  }

  protected makeNode(entry: HistoryEntry, connectionName: string): HistoryNode {
    return new HistoryNode(entry, connectionName);
  }
}
