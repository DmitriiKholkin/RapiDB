import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class EventEmitter<T> {
    private readonly listeners = new Set<(value: T) => void>();
    disposed = false;

    readonly event = (listener: (value: T) => void) => {
      this.listeners.add(listener);
      return { dispose: () => this.listeners.delete(listener) };
    };

    fire(value: T): void {
      for (const listener of this.listeners) listener(value);
    }

    dispose(): void {
      this.disposed = true;
      this.listeners.clear();
    }
  }

  class Disposable {
    static from(...items: { dispose(): unknown }[]) {
      return new Disposable(() => {
        for (const item of items) item.dispose();
      });
    }

    constructor(private readonly callOnDispose: () => void) {}

    dispose(): void {
      this.callOnDispose();
    }
  }

  class TreeItem {
    constructor(
      public label: string,
      public collapsibleState?: number,
    ) {}
  }

  return {
    EventEmitter,
    Disposable,
    TreeItem,
    ThemeIcon: class ThemeIcon {
      constructor(readonly id: string) {}
    },
    MarkdownString: class MarkdownString {
      constructor(readonly value: string) {}
    },
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
  };
});

async function createManager() {
  const vscode = await import("vscode");
  const connections = new vscode.EventEmitter<void>();
  const history = new vscode.EventEmitter<void>();
  const bookmarks = new vscode.EventEmitter<void>();
  let name = "Primary";
  return {
    rename(next: string) {
      name = next;
      connections.fire();
    },
    connections,
    manager: {
      onDidChangeConnections: connections.event,
      onDidChangeHistory: history.event,
      onDidChangeBookmarks: bookmarks.event,
      getConnections: () => [{ id: "conn-1", name, type: "pg" }],
      getHistory: () => [
        {
          id: "history-1",
          connectionId: "conn-1",
          sql: "SELECT 1",
          executedAt: "2026-05-23T10:00:00.000Z",
        },
      ],
      getBookmarks: () => [
        {
          id: "bookmark-1",
          connectionId: "conn-1",
          sql: "SELECT 2",
          savedAt: "2026-05-23T10:00:00.000Z",
        },
      ],
    },
  };
}

describe("history and bookmarks after a connection rename", () => {
  it.each([
    ["HistoryProvider", "../../src/extension/providers/historyProvider"],
    ["BookmarksProvider", "../../src/extension/providers/bookmarksProvider"],
  ])("%s refreshes when connections change", async (exportName, path) => {
    const module = (await import(path)) as Record<
      string,
      new (
        cm: never,
      ) => {
        onDidChangeTreeData(listener: () => void): unknown;
        getChildren(): { description?: string }[];
        disposable: { dispose(): void };
      }
    >;
    const { manager, rename } = await createManager();
    const provider = new module[exportName](manager as never);
    const refreshed = vi.fn();
    provider.onDidChangeTreeData(refreshed);

    expect(provider.getChildren()[0]?.description).toBe("Primary (pg)");
    rename("Renamed");

    expect(refreshed).toHaveBeenCalledTimes(1);
    expect(provider.getChildren()[0]?.description).toBe("Renamed (pg)");

    provider.disposable.dispose();
    rename("Again");
    expect(refreshed).toHaveBeenCalledTimes(1);
  });
});
