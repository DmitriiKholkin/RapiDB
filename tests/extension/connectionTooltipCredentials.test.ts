import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class EventEmitter<T> {
    private readonly listeners = new Set<(value: T) => void>();

    readonly event = (listener: (value: T) => void) => {
      this.listeners.add(listener);
      return { dispose: () => this.listeners.delete(listener) };
    };

    fire(value: T): void {
      for (const listener of this.listeners) listener(value);
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
      constructor(
        public label: string,
        public collapsibleState?: number,
      ) {}
    },
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    DataTransferItem: class DataTransferItem {
      constructor(readonly value: string) {}
    },
    MarkdownString: class MarkdownString {
      constructor(readonly value: string) {}
    },
    commands: { executeCommand: vi.fn() },
  };
});

async function tooltipFor(config: Record<string, unknown>): Promise<string> {
  const connectionManager = {
    getConnections: vi.fn(() => [config]),
    getConnection: vi.fn(() => config),
    isConnected: vi.fn(() => false),
    isConnecting: vi.fn(() => false),
    ensureSchemaScopeLoading: vi.fn(),
    getSchemaSnapshotState: vi.fn(() => ({
      snapshot: { databases: [] },
      status: "loaded",
      isPartial: false,
    })),
    onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
    onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
    onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
    isSchemaScopeExpanded: vi.fn(() => false),
    onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
  };
  const { ConnectionProvider } = await import(
    "../../src/extension/providers/connectionProvider"
  );
  const provider = new ConnectionProvider(connectionManager as never);
  const [node] = await provider.getChildren();
  return (node?.tooltip as { value: string }).value;
}

describe("connection tooltip credentials", () => {
  it.each([
    ["connectionUri", "https://elastic:pw-secret@es.example.com:9200"],
    ["endpoint", "https://elastic:pw-secret@es.example.com:9200"],
    ["endpoint", "https://es.example.com:9200/?api_key=pw-secret"],
  ])("redacts credentials from the Elasticsearch %s", async (field, value) => {
    const tooltip = await tooltipFor({
      id: "conn-es",
      name: "Elastic",
      type: "elasticsearch",
      [field]: value,
    });

    expect(tooltip).not.toContain("pw-secret");
    expect(tooltip).toContain("es.example.com:9200");
  });

  it("does not render an ambiguous credential-bearing endpoint", async () => {
    const tooltip = await tooltipFor({
      id: "conn-es",
      name: "Elastic",
      type: "elasticsearch",
      endpoint: "https://elastic:pa/ss-secret@es.example.com:9200",
    });

    expect(tooltip).not.toContain("secret");
    expect(tooltip).toContain("Endpoint: `—`");
  });
});
