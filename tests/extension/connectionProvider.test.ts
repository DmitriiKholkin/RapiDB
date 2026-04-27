import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class EventEmitter<T> {
    private readonly listeners = new Set<(value: T) => void>();

    readonly event = (listener: (value: T) => void) => {
      this.listeners.add(listener);
      return {
        dispose: () => {
          this.listeners.delete(listener);
        },
      };
    };

    fire(value: T): void {
      for (const listener of this.listeners) {
        listener(value);
      }
    }
  }

  return {
    EventEmitter,
    ThemeIcon: class ThemeIcon {
      constructor(
        readonly id: string,
        readonly color?: { id: string },
      ) {}
    },
    ThemeColor: class ThemeColor {
      constructor(readonly id: string) {}
    },
    TreeItem: class TreeItem {
      label: string;
      collapsibleState?: number;

      constructor(label: string, collapsibleState?: number) {
        this.label = label;
        this.collapsibleState = collapsibleState;
      }
    },
    TreeItemCollapsibleState: {
      None: 0,
      Collapsed: 1,
      Expanded: 2,
    },
    MarkdownString: class MarkdownString {
      constructor(readonly value: string) {}
    },
  };
});

describe("ConnectionProvider", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders multi-schema databases from the shared schema snapshot", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "public",
                objects: [
                  { name: "users", type: "table", columns: [] },
                  { name: "active_users", type: "view", columns: [] },
                ],
              },
              {
                name: "audit",
                objects: [
                  { name: "sync_events", type: "procedure", columns: [] },
                ],
              },
            ],
          },
        ],
      })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    expect(roots).toHaveLength(1);
    expect(roots[0]?.label).toBe("Primary");

    const databases = await provider.getChildren(roots[0]);
    expect(databases).toHaveLength(1);
    expect(databases[0]?.label).toBe("app_db");

    const schemas = await provider.getChildren(databases[0]);
    expect(schemas.map((node) => node.label)).toEqual(["public", "audit"]);

    const categories = await provider.getChildren(schemas[0]);
    expect(
      categories.map((node) => ({
        label: node.label,
        description: node.description,
      })),
    ).toEqual([
      { label: "Tables", description: "(1)" },
      { label: "Views", description: "(1)" },
      { label: "Functions", description: "(0)" },
      { label: "Procedures", description: "(0)" },
    ]);

    const tableNodes = await provider.getChildren(categories[0]);
    expect(tableNodes.map((node) => node.label)).toEqual(["users"]);
    expect(connectionManager.getDriver).not.toHaveBeenCalled();
  });

  it("renders single-schema databases without an extra schema level", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "mysql" },
      ]),
      isConnected: vi.fn((id: string) => id === "conn-1"),
      isConnecting: vi.fn(() => false),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "app_db",
            schemas: [
              {
                name: "app_db",
                objects: [
                  { name: "users", type: "table", columns: [] },
                  { name: "refresh_users", type: "procedure", columns: [] },
                ],
              },
            ],
          },
        ],
      })),
      getDriver: vi.fn(() => {
        throw new Error("ConnectionProvider should not query drivers directly");
      }),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider } = await import(
      "../../src/extension/providers/connectionProvider"
    );

    const provider = new ConnectionProvider(connectionManager as never);

    const roots = await provider.getChildren();
    const databases = await provider.getChildren(roots[0]);
    const categories = await provider.getChildren(databases[0]);

    expect(categories.map((node) => node.label)).toEqual([
      "Tables",
      "Views",
      "Functions",
      "Procedures",
    ]);

    const procedureNodes = await provider.getChildren(categories[3]);
    expect(procedureNodes.map((node) => node.label)).toEqual(["refresh_users"]);
    expect(connectionManager.getDriver).not.toHaveBeenCalled();
  });
});
