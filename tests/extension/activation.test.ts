import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createMockVscodeModule } from "../support/mockVscode";

const expectedCommands = [
  "rapidb.addConnection",
  "rapidb.editConnection",
  "rapidb.deleteConnection",
  "rapidb.connect",
  "rapidb.disconnect",
  "rapidb.newQuery",
  "rapidb.openTableData",
  "rapidb.showDDL",
  "rapidb.copyNodeName",
  "rapidb.openSchema",
  "rapidb.openRoutine",
  "rapidb.openHistoryEntry",
  "rapidb.openBookmarkEntry",
  "rapidb.deleteBookmark",
  "rapidb.clearBookmarks",
  "rapidb.clearHistory",
  "rapidb.disconnectAll",
  "rapidb.refresh",
];

describe("extension activation", () => {
  let vscodeState: ReturnType<typeof createMockVscodeModule>["state"];
  let connectionManagerInstance: Record<string, unknown>;
  let connectionProviderInstances: Array<{
    refresh: ReturnType<typeof vi.fn>;
    disposable: { dispose(): void };
  }>;
  let connectionFormShow: ReturnType<typeof vi.fn>;
  let connectWithProgress: ReturnType<typeof vi.fn>;
  let queryPanelDisposeAll: ReturnType<typeof vi.fn>;
  let tablePanelDisposeAll: ReturnType<typeof vi.fn>;
  let schemaPanelDisposeAll: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.resetModules();
    connectionProviderInstances = [];
    connectionFormShow = vi.fn();
    connectWithProgress = vi.fn();
    queryPanelDisposeAll = vi.fn();
    tablePanelDisposeAll = vi.fn();
    schemaPanelDisposeAll = vi.fn();

    const vscodeMock = createMockVscodeModule();
    vscodeState = vscodeMock.state;
    vi.doMock("vscode", () => vscodeMock.module);

    connectionManagerInstance = {
      getConnectedCount: vi.fn(() => 2),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      isConnected: vi.fn(() => false),
      isConnecting: vi.fn(() => false),
      getConnection: vi.fn(() => ({ id: "conn-1", name: "Primary" })),
      disconnectFrom: vi.fn(),
      disconnectAll: vi.fn().mockResolvedValue(undefined),
      clearBookmarks: vi.fn(),
      clearHistory: vi.fn(),
      getDriver: vi.fn(),
    };

    function ConnectionManagerMock() {
      return connectionManagerInstance;
    }

    vi.doMock("../../src/extension/connectionManager", () => ({
      ConnectionManager: ConnectionManagerMock,
    }));

    function ConnectionProviderMock() {
      const instance = {
        refresh: vi.fn(),
        disposable: { dispose: vi.fn() },
      };
      connectionProviderInstances.push(instance);
      return instance;
    }

    function HistoryProviderMock() {
      return { disposable: { dispose: vi.fn() } };
    }

    function BookmarksProviderMock() {
      return { disposable: { dispose: vi.fn() } };
    }

    vi.doMock("../../src/extension/providers/connectionProvider", () => ({
      ConnectionProvider: ConnectionProviderMock,
    }));
    vi.doMock("../../src/extension/providers/historyProvider", () => ({
      HistoryProvider: HistoryProviderMock,
    }));
    vi.doMock("../../src/extension/providers/bookmarksProvider", () => ({
      BookmarksProvider: BookmarksProviderMock,
    }));
    vi.doMock("../../src/extension/panels/connectionFormPanel", () => ({
      ConnectionFormPanel: {
        show: connectionFormShow,
      },
    }));
    vi.doMock("../../src/extension/panels/queryPanel", () => ({
      QueryPanel: {
        createOrShow: vi.fn(),
        disposeAll: queryPanelDisposeAll,
      },
    }));
    vi.doMock("../../src/extension/panels/schemaPanel", () => ({
      SchemaPanel: {
        createOrShow: vi.fn(),
        disposeAll: schemaPanelDisposeAll,
      },
    }));
    vi.doMock("../../src/extension/panels/tablePanel", () => ({
      TablePanel: {
        createOrShow: vi.fn(),
        disposeAll: tablePanelDisposeAll,
      },
    }));
    vi.doMock("../../src/extension/connectionManagerPrompts", () => ({
      confirmBookmarkRemoval: vi.fn(),
      confirmConnectionRemoval: vi.fn(),
      pickConnectionWithPrompt: vi.fn(),
    }));
    vi.doMock("../../src/extension/utils/connectOrchestration", () => ({
      connectWithProgress,
    }));
  });

  afterEach(async () => {
    const extension = await import("../../src/extension/extension");
    extension.deactivate();
  });

  it("registers commands and tree views exactly once on activation", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);
    extension.activate(context as never);

    expect(vscodeState.createTreeView).toHaveBeenCalledTimes(3);
    expect(vscodeState.registerCommand).toHaveBeenCalledTimes(
      expectedCommands.length,
    );
    expect(
      vscodeState.registerCommand.mock.calls.map(([command]) => command),
    ).toEqual(expectedCommands);
    expect(connectionProviderInstances).toHaveLength(1);
  });

  it("invokes the add-connection command and refreshes the explorer when a connection is saved", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    connectionFormShow.mockResolvedValue({
      id: "conn-1",
      name: "Analytics",
      type: "pg",
    });

    extension.activate(context as never);

    const addCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.addConnection",
    )?.[1] as (() => Promise<void>) | undefined;

    if (!addCommand) {
      throw new Error("Add connection command was not registered.");
    }

    await addCommand();

    expect(connectionFormShow).toHaveBeenCalledTimes(1);
    expect(connectionProviderInstances[0]?.refresh).toHaveBeenCalledTimes(1);
    expect(vscodeState.showInformationMessage).toHaveBeenCalledWith(
      '[RapiDB] Connection "Analytics" saved.',
    );
  });

  it("deactivates panels and disconnects all active connections", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);
    extension.deactivate();

    expect(queryPanelDisposeAll).toHaveBeenCalledTimes(1);
    expect(tablePanelDisposeAll).toHaveBeenCalledTimes(1);
    expect(schemaPanelDisposeAll).toHaveBeenCalledTimes(1);
    expect(connectionManagerInstance.disconnectAll).toHaveBeenCalledTimes(1);
  });
});
