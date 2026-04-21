import { vi } from "vitest";

export class MockEventEmitter<T> {
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

  dispose(): void {
    this.listeners.clear();
  }
}

export interface MockWebview {
  html: string;
  postMessage: ReturnType<typeof vi.fn>;
  onDidReceiveMessage(listener: (message: unknown) => void): {
    dispose(): void;
  };
  dispatchMessage(message: unknown): Promise<void>;
}

export interface MockWebviewPanel {
  readonly webview: MockWebview;
  readonly onDidDispose: (listener: () => void) => { dispose(): void };
  readonly dispose: () => void;
}

export interface MockVscodeState {
  registerCommand: ReturnType<typeof vi.fn>;
  createTreeView: ReturnType<typeof vi.fn>;
  showInformationMessage: ReturnType<typeof vi.fn>;
  showWarningMessage: ReturnType<typeof vi.fn>;
  showErrorMessage: ReturnType<typeof vi.fn>;
  withProgress: ReturnType<typeof vi.fn>;
  writeClipboard: ReturnType<typeof vi.fn>;
  createWebviewPanel: ReturnType<typeof vi.fn>;
  panels: MockWebviewPanel[];
}

function createMockWebview(): MockWebview {
  const messageListeners = new Set<(message: unknown) => void>();

  return {
    html: "",
    postMessage: vi.fn(),
    onDidReceiveMessage(listener) {
      messageListeners.add(listener);
      return {
        dispose: () => {
          messageListeners.delete(listener);
        },
      };
    },
    async dispatchMessage(message) {
      for (const listener of messageListeners) {
        await listener(message);
      }
    },
  };
}

function createMockPanel(): MockWebviewPanel {
  const disposeListeners = new Set<() => void>();
  const webview = createMockWebview();

  return {
    webview,
    onDidDispose(listener) {
      disposeListeners.add(listener);
      return {
        dispose: () => {
          disposeListeners.delete(listener);
        },
      };
    },
    dispose() {
      for (const listener of disposeListeners) {
        listener();
      }
    },
  };
}

export function createMockVscodeModule(): {
  module: Record<string, unknown>;
  state: MockVscodeState;
} {
  const panels: MockWebviewPanel[] = [];

  const state: MockVscodeState = {
    registerCommand: vi.fn(
      (command: string, callback: (...args: unknown[]) => unknown) => ({
        command,
        callback,
        dispose: vi.fn(),
      }),
    ),
    createTreeView: vi.fn((id: string) => ({
      id,
      badge: undefined,
      dispose: vi.fn(),
    })),
    showInformationMessage: vi.fn(),
    showWarningMessage: vi.fn(),
    showErrorMessage: vi.fn(),
    withProgress: vi.fn(async (_options, task: () => Promise<unknown>) =>
      task(),
    ),
    writeClipboard: vi.fn(),
    createWebviewPanel: vi.fn(() => {
      const panel = createMockPanel();
      panels.push(panel);
      return panel;
    }),
    panels,
  };

  return {
    module: {
      EventEmitter: MockEventEmitter,
      ProgressLocation: { Window: 10 },
      ViewColumn: { One: 1 },
      ConfigurationTarget: { Global: 1 },
      window: {
        createTreeView: state.createTreeView,
        createWebviewPanel: state.createWebviewPanel,
        showInformationMessage: state.showInformationMessage,
        showWarningMessage: state.showWarningMessage,
        showErrorMessage: state.showErrorMessage,
        withProgress: state.withProgress,
      },
      commands: {
        registerCommand: state.registerCommand,
      },
      env: {
        clipboard: {
          writeText: state.writeClipboard,
        },
      },
      workspace: {
        onDidChangeConfiguration: vi.fn(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn(),
          update: vi.fn(),
        })),
      },
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
          readonly label: string,
          readonly collapsibleState?: number,
        ) {}
      },
      TreeItemCollapsibleState: {
        None: 0,
        Collapsed: 1,
        Expanded: 2,
      },
    },
    state,
  };
}
