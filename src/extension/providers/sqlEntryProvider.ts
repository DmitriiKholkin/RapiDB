import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";

/**
 * Extracts the first non-empty, trimmed line from a SQL string.
 * Used as a short display label for history and bookmark nodes.
 */
export function extractFirstSqlLine(sql: string): string {
  return (
    sql
      .split("\n")
      .map((line) => line.trim())
      .find((line) => line.length > 0) ?? sql
  );
}

/**
 * Abstract base for tree-view providers that display a list of SQL entries
 * (history entries, bookmarks, etc.) grouped by connection.
 *
 * Subclasses only need to provide:
 *  - which event triggers a refresh   (`onDidChange` constructor argument)
 *  - how to load the entries          (`getEntries()`)
 *  - how to create a tree node        (`makeNode()`)
 */
export abstract class SqlEntryProvider<
  TEntry extends { id: string; sql: string; connectionId: string },
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

  /** Dispose the event subscription when the view is torn down. */
  get disposable(): vscode.Disposable {
    return this._subscription;
  }

  getTreeItem(element: TNode): vscode.TreeItem {
    return element;
  }

  getChildren(_element?: TNode): TNode[] {
    // Leaf nodes have no children.
    if (_element) return [];

    const entries = this.getEntries();
    if (entries.length === 0) return [];

    // Build a lookup of connectionId → "Name (type)" once per refresh.
    const connections = this.cm.getConnections();
    const nameMap = new Map<string, string>(
      connections.map((c) => [c.id, `${c.name} (${c.type})`]),
    );

    return entries.map((entry) =>
      this.makeNode(entry, nameMap.get(entry.connectionId) ?? entry.connectionId),
    );
  }

  /** Return the full list of entries to display (newest first). */
  protected abstract getEntries(): TEntry[];

  /**
   * Create a tree node for a single entry.
   * @param entry          The data entry.
   * @param connectionName Resolved display name for the connection.
   */
  protected abstract makeNode(entry: TEntry, connectionName: string): TNode;
}
