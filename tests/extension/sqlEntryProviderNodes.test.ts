import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class ThemeIcon {
    constructor(readonly id: string) {}
  }

  class MarkdownString {
    constructor(readonly value: string) {}
  }

  class TreeItem {
    label: string;
    collapsibleState?: number;
    id?: string;
    contextValue?: string;
    iconPath?: unknown;
    description?: string;
    tooltip?: unknown;
    command?: {
      command: string;
      title: string;
      arguments?: unknown[];
    };

    constructor(label: string, collapsibleState?: number) {
      this.label = label;
      this.collapsibleState = collapsibleState;
    }
  }

  return {
    ThemeIcon,
    MarkdownString,
    TreeItem,
    TreeItemCollapsibleState: {
      None: 0,
      Collapsed: 1,
      Expanded: 2,
    },
  };
});

describe("SqlEntryNode-backed providers", () => {
  it("builds HistoryNode view metadata from shared SqlEntryNode", async () => {
    const { HistoryNode } = await import(
      "../../src/extension/providers/historyProvider"
    );

    const entry = {
      id: "history-1",
      connectionId: "conn-1",
      sql: "\n  SELECT * FROM users\n",
      executedAt: "2026-05-23T10:00:00.000Z",
    };

    const node = new HistoryNode(entry as never, "Primary (pg)");

    expect(node.label).toBe("SELECT * FROM users");
    expect(node.id).toBe("history-1");
    expect(node.contextValue).toBe("historyEntry");
    expect((node.iconPath as { id: string }).id).toBe("history");
    expect(node.description).toBe("Primary (pg)");
    expect(node.command).toEqual({
      command: "rapidb.openHistoryEntry",
      title: "Open in Query Editor",
      arguments: [entry],
    });
    expect((node.tooltip as { value: string }).value).toContain("Primary (pg)");
    expect((node.tooltip as { value: string }).value).toContain(entry.sql);
  });

  it("builds BookmarkNode view metadata from shared SqlEntryNode", async () => {
    const { BookmarkNode } = await import(
      "../../src/extension/providers/bookmarksProvider"
    );

    const entry = {
      id: "bookmark-1",
      connectionId: "conn-1",
      sql: "\n\n  SELECT 1;\n",
      savedAt: "2026-05-23T11:00:00.000Z",
    };

    const node = new BookmarkNode(entry as never, "Primary (pg)");

    expect(node.label).toBe("SELECT 1;");
    expect(node.id).toBe("bookmark-1");
    expect(node.contextValue).toBe("bookmarkEntry");
    expect((node.iconPath as { id: string }).id).toBe("bookmark");
    expect(node.description).toBe("Primary (pg)");
    expect(node.command).toEqual({
      command: "rapidb.openBookmarkEntry",
      title: "Open in Query Editor",
      arguments: [entry],
    });
    expect((node.tooltip as { value: string }).value).toContain("Primary (pg)");
    expect((node.tooltip as { value: string }).value).toContain(entry.sql);
  });
});
