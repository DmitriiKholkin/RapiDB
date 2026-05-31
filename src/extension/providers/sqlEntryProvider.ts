import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";

export function extractFirstSqlLine(sql: string): string {
  return (
    sql
      .split("\n")
      .map((line) => line.trim())
      .find((line) => line.length > 0) ?? sql
  );
}

function escapeMarkdownCodeFence(text: string): string {
  return text.replace(/```/g, "``\\`");
}

interface SqlEntryNodeOptions {
  connectionName: string;
  iconId: string;
  contextValue: string;
  dateLabel: string;
  command: string;
  commandTitle: string;
}

export abstract class SqlEntryNode<
  TEntry extends {
    id: string;
    sql: string;
  },
> extends vscode.TreeItem {
  constructor(
    public readonly entry: TEntry,
    options: SqlEntryNodeOptions,
  ) {
    super(extractFirstSqlLine(entry.sql), vscode.TreeItemCollapsibleState.None);

    this.id = entry.id;
    this.contextValue = options.contextValue;
    this.iconPath = new vscode.ThemeIcon(options.iconId);
    this.description = options.connectionName;
    this.tooltip = new vscode.MarkdownString(
      `**${options.connectionName}** — ${options.dateLabel}\n\`\`\`\n${escapeMarkdownCodeFence(entry.sql)}\n\`\`\``,
    );
    this.command = {
      command: options.command,
      title: options.commandTitle,
      arguments: [entry],
    };
  }
}

export abstract class SqlEntryProvider<
  TEntry extends {
    id: string;
    sql: string;
    connectionId: string;
  },
  TNode extends vscode.TreeItem,
> implements vscode.TreeDataProvider<TNode>
{
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<
    TNode | undefined | null
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;
  private readonly _subscription: vscode.Disposable;
  constructor(
    protected readonly cm: ConnectionManager,
    onDidChange: vscode.Event<void>,
  ) {
    this._subscription = onDidChange(() =>
      this._onDidChangeTreeData.fire(undefined),
    );
  }
  get disposable(): vscode.Disposable {
    return this._subscription;
  }
  getTreeItem(element: TNode): vscode.TreeItem {
    return element;
  }
  getChildren(_element?: TNode): TNode[] {
    if (_element) return [];
    const entries = this.getEntries();
    if (entries.length === 0) return [];
    const connections = this.cm.getConnections();
    const nameMap = new Map<string, string>(
      connections.map((c) => [c.id, `${c.name} (${c.type})`]),
    );
    return entries.map((entry) =>
      this.makeNode(
        entry,
        nameMap.get(entry.connectionId) ?? entry.connectionId,
      ),
    );
  }
  protected abstract getEntries(): TEntry[];

  protected abstract makeNode(_entry: TEntry, _connectionName: string): TNode;
}

export function createSqlEntryProvider<
  TEntry extends {
    id: string;
    sql: string;
    connectionId: string;
  },
  TNode extends vscode.TreeItem,
>(options: {
  onDidChange: (cm: ConnectionManager) => vscode.Event<void>;
  getEntries: (cm: ConnectionManager) => TEntry[];
  makeNode: (entry: TEntry, connectionName: string) => TNode;
}): new (
  cm: ConnectionManager,
) => SqlEntryProvider<TEntry, TNode> {
  return class extends SqlEntryProvider<TEntry, TNode> {
    constructor(cm: ConnectionManager) {
      super(cm, options.onDidChange(cm));
    }

    protected override getEntries(): TEntry[] {
      return options.getEntries(this.cm);
    }

    protected override makeNode(entry: TEntry, connectionName: string): TNode {
      return options.makeNode(entry, connectionName);
    }
  };
}
