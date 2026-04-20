import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type * as vscode from "vscode";

interface MockConnectionManagerInstance {
  emitConnect(nextCount: number): void;
  emitDisconnect(nextCount: number): void;
}

const {
  MockEventEmitter,
  createTreeView,
  registerCommand,
  onDidChangeConfiguration,
  getConfiguration,
  showInformationMessage,
  showWarningMessage,
  showErrorMessage,
  writeText,
} = vi.hoisted(() => {
  class HoistedMockEventEmitter<T> {
    private listeners: Array<(event: T) => void> = [];

    readonly event = (listener: (event: T) => void) => {
      this.listeners.push(listener);
      return {
        dispose: () => {
          this.listeners = this.listeners.filter(
            (current) => current !== listener,
          );
        },
      };
    };

    fire(event: T): void {
      for (const listener of this.listeners) {
        listener(event);
      }
    }

    dispose(): void {
      this.listeners = [];
    }
  }

  return {
    MockEventEmitter: HoistedMockEventEmitter,
    createTreeView: vi.fn(),
    registerCommand: vi.fn(() => ({ dispose: vi.fn() })),
    onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
    getConfiguration: vi.fn(() => ({
      get: vi.fn(),
      update: vi.fn(),
    })),
    showInformationMessage: vi.fn(),
    showWarningMessage: vi.fn(),
    showErrorMessage: vi.fn(),
    writeText: vi.fn(),
  };
});

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  commands: {
    registerCommand,
  },
  env: {
    clipboard: {
      writeText,
    },
  },
  window: {
    createTreeView,
    showErrorMessage,
    showInformationMessage,
    showWarningMessage,
  },
  workspace: {
    getConfiguration,
    onDidChangeConfiguration,
  },
}));

const MockProvider = vi.hoisted(
  () =>
    class {
      readonly disposable = { dispose: vi.fn() };

      refresh(): void {}
    },
);

vi.mock("../../src/extension/providers/connectionProvider", () => ({
  ConnectionProvider: MockProvider,
}));

vi.mock("../../src/extension/providers/historyProvider", () => ({
  HistoryProvider: MockProvider,
}));

vi.mock("../../src/extension/providers/bookmarksProvider", () => ({
  BookmarksProvider: MockProvider,
}));

vi.mock("../../src/extension/panels/connectionFormPanel", () => ({
  ConnectionFormPanel: { show: vi.fn() },
}));

vi.mock("../../src/extension/panels/queryPanel", () => ({
  QueryPanel: {
    createOrShow: vi.fn(),
    disposeAll: vi.fn(),
  },
}));

vi.mock("../../src/extension/panels/schemaPanel", () => ({
  SchemaPanel: {
    createOrShow: vi.fn(),
    disposeAll: vi.fn(),
  },
}));

vi.mock("../../src/extension/panels/tablePanel", () => ({
  TablePanel: {
    createOrShow: vi.fn(),
    disposeAll: vi.fn(),
  },
}));

vi.mock("../../src/extension/utils/connectOrchestration", () => ({
  connectWithProgress: vi.fn(),
}));

vi.mock("../../src/extension/utils/errorHandling", () => ({
  logErrorWithContext: vi.fn((message: string) => new Error(message)),
  normalizeUnknownError: vi.fn((error: unknown) =>
    error instanceof Error ? error : new Error(String(error)),
  ),
}));

const connectionManagerState = vi.hoisted(() => ({
  initialConnectedCount: 0,
  instances: [] as MockConnectionManagerInstance[],
  reset() {
    this.initialConnectedCount = 0;
    this.instances = [];
  },
}));

vi.mock("../../src/extension/connectionManager", () => ({
  ConnectionManager: class {
    readonly onDidConnect: vscode.Event<void>;
    readonly onDidDisconnect: vscode.Event<string>;

    private readonly didConnectEmitter = new MockEventEmitter<void>();
    private readonly didDisconnectEmitter = new MockEventEmitter<string>();
    private connectedCount = connectionManagerState.initialConnectedCount;

    constructor(_context: vscode.ExtensionContext) {
      this.onDidConnect = this.didConnectEmitter.event;
      this.onDidDisconnect = this.didDisconnectEmitter.event;
      connectionManagerState.instances.push(
        this as unknown as MockConnectionManagerInstance,
      );
    }

    getConnectedCount(): number {
      return this.connectedCount;
    }

    emitConnect(nextCount: number): void {
      this.connectedCount = nextCount;
      this.didConnectEmitter.fire();
    }

    emitDisconnect(nextCount: number): void {
      this.connectedCount = nextCount;
      this.didDisconnectEmitter.fire("conn-1");
    }

    disconnectAll(): Promise<void> {
      return Promise.resolve();
    }
  },
}));

import { activate, deactivate } from "../../src/extension/extension";

function makeContext(): vscode.ExtensionContext {
  return {
    subscriptions: [],
  } as unknown as vscode.ExtensionContext;
}

describe("extension explorer badge", () => {
  beforeEach(() => {
    connectionManagerState.reset();
    createTreeView.mockReset();
    registerCommand.mockClear();

    createTreeView.mockImplementation((id: string) => ({
      id,
      badge: undefined,
      dispose: vi.fn(),
    }));
  });

  afterEach(() => {
    deactivate();
  });

  it("sets an initial badge when connected databases already exist", () => {
    connectionManagerState.initialConnectedCount = 2;
    const context = makeContext();

    activate(context);

    const explorerView = createTreeView.mock.results[0]?.value as
      | vscode.TreeView<unknown>
      | undefined;

    expect(explorerView?.badge).toEqual({
      value: 2,
      tooltip: "2 connected databases",
    });
  });

  it("applies the badge only to the explorer tree view", () => {
    connectionManagerState.initialConnectedCount = 4;

    activate(makeContext());

    const explorerView = createTreeView.mock.results[0]?.value as
      | vscode.TreeView<unknown>
      | undefined;
    const historyView = createTreeView.mock.results[1]?.value as
      | vscode.TreeView<unknown>
      | undefined;
    const bookmarksView = createTreeView.mock.results[2]?.value as
      | vscode.TreeView<unknown>
      | undefined;

    expect(createTreeView).toHaveBeenNthCalledWith(
      1,
      "rapidb-explorer",
      expect.any(Object),
    );
    expect(createTreeView).toHaveBeenNthCalledWith(
      2,
      "rapidb-history",
      expect.any(Object),
    );
    expect(createTreeView).toHaveBeenNthCalledWith(
      3,
      "rapidb-bookmarks",
      expect.any(Object),
    );
    expect(explorerView?.badge).toEqual({
      value: 4,
      tooltip: "4 connected databases",
    });
    expect(historyView?.badge).toBeUndefined();
    expect(bookmarksView?.badge).toBeUndefined();
  });

  it("updates and clears the badge from connection lifecycle events", () => {
    const context = makeContext();

    activate(context);

    const manager = connectionManagerState.instances[0];
    const explorerView = createTreeView.mock.results[0]?.value as
      | vscode.TreeView<unknown>
      | undefined;

    expect(explorerView?.badge).toBeUndefined();

    manager?.emitConnect(1);
    expect(explorerView?.badge).toEqual({
      value: 1,
      tooltip: "1 connected database",
    });

    manager?.emitConnect(3);
    expect(explorerView?.badge).toEqual({
      value: 3,
      tooltip: "3 connected databases",
    });

    manager?.emitDisconnect(0);
    expect(explorerView?.badge).toBeUndefined();
  });
});
