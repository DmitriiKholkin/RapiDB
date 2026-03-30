import * as vscode from "vscode";
import type { ConnectionManager, HistoryEntry } from "../connectionManager";

export class HistoryNode extends vscode.TreeItem {
  constructor(
    public readonly entry: HistoryEntry,
    connectionName: string,
  ) {
    const firstLine =
      entry.sql
        .split("\n")
        .map((l) => l.trim())
        .find((l) => l.length > 0) ?? entry.sql;

    super(firstLine, vscode.TreeItemCollapsibleState.None);

    this.id = entry.id;
    this.contextValue = "historyEntry";
    this.iconPath = new vscode.ThemeIcon("history");

    this.description = connectionName;

    const date = new Date(entry.executedAt);
    const dateStr = date.toLocaleString();
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

export class HistoryProvider implements vscode.TreeDataProvider<HistoryNode> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<
    HistoryNode | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private readonly _subscription: vscode.Disposable;

  constructor(private readonly cm: ConnectionManager) {
    this._subscription = cm.onDidChangeHistory(() =>
      this._onDidChangeTreeData.fire(),
    );
  }

  get disposable(): vscode.Disposable {
    return this._subscription;
  }

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: HistoryNode): vscode.TreeItem {
    return element;
  }

  getChildren(_element?: HistoryNode): HistoryNode[] {
    if (_element) {
      return [];
    }

    const entries = this.cm.getHistory();
    if (entries.length === 0) {
      return [];
    }

    const connections = this.cm.getConnections();
    const nameMap = new Map<string, string>(
      connections.map((c) => [c.id, `${c.name} (${c.type})`]),
    );

    return entries.map(
      (e) => new HistoryNode(e, nameMap.get(e.connectionId) ?? e.connectionId),
    );
  }
}
