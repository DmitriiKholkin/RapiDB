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
  protected abstract makeNode(entry: TEntry, connectionName: string): TNode;
}
