import { beforeEach, describe, expect, it, vi } from "vitest";

const { queryPanelCreateOrShow, showErrorMessage, showWarningMessage } =
  vi.hoisted(() => ({
    queryPanelCreateOrShow: vi.fn(),
    showErrorMessage: vi.fn(),
    showWarningMessage: vi.fn(),
  }));

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
    DataTransferItem: class DataTransferItem {
      constructor(readonly value: string) {}
    },
    MarkdownString: class MarkdownString {
      constructor(readonly value: string) {}
    },
    commands: {
      executeCommand: vi.fn(),
    },
    window: {
      showWarningMessage,
      showErrorMessage,
      showInformationMessage: vi.fn(),
    },
  };
});

vi.mock("../../src/extension/panels/queryPanel", () => ({
  QueryPanel: { createOrShow: queryPanelCreateOrShow },
}));
vi.mock("../../src/extension/panels/tablePanel", () => ({
  TablePanel: { createOrShow: vi.fn() },
}));
vi.mock("../../src/extension/panels/erdPanel", () => ({
  ErdPanel: { createOrShow: vi.fn() },
}));

describe("explorer constraint detail nodes with shared names", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("gives same-name constraints of different kinds distinct ids and opens each one's DDL", async () => {
    const driver = {
      getConstraintDDL: vi.fn(
        async (
          _database: string,
          _schema: string,
          _table: string,
          name: string,
          kind?: string,
        ) =>
          kind === "foreign_key"
            ? `FK DDL ${name}`
            : kind === "unique"
              ? `UNIQUE DDL ${name}`
              : `UNKNOWN DDL ${name}`,
      ),
    };
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "mssql" },
      ]),
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "mssql",
      })),
      getDriverEntityManifest: vi.fn(() => undefined),
      getDriver: vi.fn(() => driver),
      getQueryEditorPresentation: vi.fn(() => undefined),
      isConnected: vi.fn(() => true),
      isConnecting: vi.fn(() => false),
      ensureSchemaScopeLoading: vi.fn(),
      ensureTableDetailLoading: vi.fn(),
      getTableDetailState: vi.fn(() => ({
        request: {
          connectionId: "conn-1",
          database: "app",
          schema: "dbo",
          table: "users",
          objectKind: "table",
        },
        status: "loaded",
        isPartial: false,
        snapshot: {
          columns: { status: "loaded", items: [] },
          constraints: {
            status: "loaded",
            items: [
              {
                name: "shared_name",
                kind: "unique",
                columns: ["login"],
                source: "derived",
              },
              {
                name: "shared_name",
                kind: "foreign_key",
                columns: ["org_id"],
                referencedSchema: "dbo",
                referencedTable: "orgs",
                referencedColumns: ["id"],
                source: "derived",
              },
            ],
          },
          indexes: { status: "loaded", items: [] },
          triggers: { status: "loaded", items: [] },
        },
      })),
      getSchemaSnapshotState: vi.fn(() => ({
        status: "loaded",
        isPartial: false,
        snapshot: {
          databases: [
            {
              name: "app",
              schemas: [
                {
                  name: "dbo",
                  objects: [{ name: "users", type: "table", columns: [] }],
                },
                {
                  name: "audit",
                  objects: [],
                },
              ],
            },
          ],
        },
      })),
      isSchemaScopeExpanded: vi.fn(() => false),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };

    const { ConnectionProvider, RapiDBNode } = await import(
      "../../src/extension/providers/connectionProvider"
    );
    const { registerExplorerCommands } = await import(
      "../../src/extension/commands/explorerCommands"
    );
    const provider = new ConnectionProvider(connectionManager as never);
    async function collect(
      parent?: InstanceType<typeof RapiDBNode>,
    ): Promise<InstanceType<typeof RapiDBNode>[]> {
      const children = await provider.getChildren(parent);
      const descendants = await Promise.all(
        children
          .filter((node) => node.collapsibleState !== 0)
          .map((node) => collect(node)),
      );
      return [...children, ...descendants.flat()];
    }
    const constraintNodes = (await collect()).filter(
      (node) => node.kind === "table_detail_constraint",
    );
    expect(constraintNodes.map((node) => node.description)).toEqual([
      "unique - login",
      "foreign key - org_id - references dbo.orgs id",
    ]);
    expect(new Set(constraintNodes.map((node) => node.id)).size).toBe(2);

    const commands = new Map<string, (...args: unknown[]) => unknown>();
    registerExplorerCommands(
      {
        context: {} as never,
        connectionManager: connectionManager as never,
        refresh: vi.fn(),
      },
      (command, callback) => {
        commands.set(command, callback as (...args: unknown[]) => unknown);
        return { dispose: vi.fn() } as never;
      },
    );
    const showDdl = commands.get("rapidb.showDDL");
    if (!showDdl) {
      throw new Error("Show DDL command was not registered.");
    }

    for (const node of constraintNodes) {
      await showDdl(node);
    }

    expect(driver.getConstraintDDL.mock.calls).toEqual([
      ["app", "dbo", "users", "shared_name", "unique"],
      ["app", "dbo", "users", "shared_name", "foreign_key"],
    ]);
    expect(queryPanelCreateOrShow.mock.calls.map((call) => call[3])).toEqual([
      "UNIQUE DDL shared_name",
      "FK DDL shared_name",
    ]);
    expect(showWarningMessage).not.toHaveBeenCalled();
    expect(showErrorMessage).not.toHaveBeenCalled();
  });

  it("forwards the constraint kind through the timeout-aware driver wrapper", async () => {
    const { createTimeoutAwareDriver, getDefaultDriverTimeoutSettings } =
      await import("../../src/extension/dbDrivers/timeout");
    const driver = {
      getConstraintDDL: vi.fn(async (..._args: unknown[]) => "DDL"),
    };
    const wrapped = createTimeoutAwareDriver(
      driver,
      getDefaultDriverTimeoutSettings,
    );

    await expect(
      wrapped.getConstraintDDL("app", "dbo", "users", "shared_name", "unique"),
    ).resolves.toBe("DDL");
    expect(driver.getConstraintDDL).toHaveBeenCalledWith(
      "app",
      "dbo",
      "users",
      "shared_name",
      "unique",
    );
  });
});
