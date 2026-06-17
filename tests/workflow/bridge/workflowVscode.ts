import { vi } from "vitest";
import { BridgeEmitter } from "./bridgeEmitter";
import {
  createFakeWebviewPanel,
  type FakeWebviewPanelHandle,
  type FakeWebviewUri,
} from "./fakeWebviewPanel";

export interface WorkflowVscodeState {
  panels: FakeWebviewPanelHandle[];
  showSaveDialog: ReturnType<typeof vi.fn>;
  showOpenDialog: ReturnType<typeof vi.fn>;
  showInformationMessage: ReturnType<typeof vi.fn>;
  showWarningMessage: ReturnType<typeof vi.fn>;
  showErrorMessage: ReturnType<typeof vi.fn>;
  showInputBox: ReturnType<typeof vi.fn>;
  withProgress: ReturnType<typeof vi.fn>;
  writeClipboard: ReturnType<typeof vi.fn>;
  readClipboard: ReturnType<typeof vi.fn>;
  createWebviewPanel: ReturnType<typeof vi.fn>;
  createTreeView: ReturnType<typeof vi.fn>;
  registerCommand: ReturnType<typeof vi.fn>;
  fireConfigurationChange: (...sections: string[]) => void;
  configurationListeners: Set<
    (event: { affectsConfiguration: (section: string) => boolean }) => void
  >;
  lastExportFile: { path: string; content: string } | null;
  exportDirectory: string;
}

class ProgressToken {
  private cancellationListeners = new BridgeEmitter<void>();
  readonly onCancellationRequested = this.cancellationListeners.event;
  cancel(): void {
    this.cancellationListeners.fire();
  }
}

class BridgeUri {
  constructor(
    readonly scheme: string,
    readonly path: string,
    readonly fsPath: string,
  ) {}
  toString(): string {
    return `${this.scheme}://${this.path}`;
  }
  with(change: { scheme?: string; path?: string; fsPath?: string }): BridgeUri {
    const nextPath = change.path ?? this.path;
    const nextScheme = change.scheme ?? this.scheme;
    const nextFsPath = change.fsPath ?? this.fsPath ?? nextPath;
    return new BridgeUri(nextScheme, nextPath, nextFsPath);
  }
  static joinPath(base: BridgeUri, ...paths: string[]): BridgeUri {
    return new BridgeUri(
      base.scheme,
      [...paths].join("/"),
      `${base.fsPath}/${[...paths].join("/")}`,
    );
  }
  static file(fsPath: string): BridgeUri {
    return new BridgeUri("file", fsPath, fsPath);
  }
  static parse(value: string): BridgeUri {
    const match = /^([a-z0-9+\-.]+):\/\/(.*)$/i.exec(value);
    if (!match) {
      return new BridgeUri("file", value, value);
    }
    return new BridgeUri(match[1], match[2], value);
  }
}

export function createWorkflowVscodeState(): WorkflowVscodeState {
  const panels: FakeWebviewPanelHandle[] = [];
  const configurationListeners = new Set<
    (event: { affectsConfiguration: (section: string) => boolean }) => void
  >();

  const state: WorkflowVscodeState = {
    panels,
    showSaveDialog: vi.fn(async (options: { defaultUri?: BridgeUri }) => {
      const fsPath =
        options?.defaultUri?.fsPath ?? `${state.exportDirectory}/export.csv`;
      return new BridgeUri("file", fsPath, fsPath);
    }),
    showOpenDialog: vi.fn(),
    showInformationMessage: vi.fn(),
    showWarningMessage: vi.fn(),
    showErrorMessage: vi.fn(),
    showInputBox: vi.fn(),
    withProgress: vi.fn(
      async (
        _options: unknown,
        task: (progress: unknown, token: ProgressToken) => Promise<unknown>,
      ) => {
        return task({}, new ProgressToken());
      },
    ),
    writeClipboard: vi.fn(async (text: string) => text),
    readClipboard: vi.fn(async () => ""),
    createWebviewPanel: vi.fn((viewType: string, title: string) => {
      const panel = createFakeWebviewPanel({ viewType, title });
      panels.push(panel);
      return panel.panel;
    }),
    createTreeView: vi.fn((id: string) => ({
      id,
      badge: undefined,
      dispose: vi.fn(),
      onDidExpandElement: () => ({ dispose: vi.fn() }),
      onDidCollapseElement: () => ({ dispose: vi.fn() }),
    })),
    registerCommand: vi.fn(
      (command: string, callback: (...args: unknown[]) => unknown) => ({
        command,
        callback,
        dispose: vi.fn(),
      }),
    ),
    fireConfigurationChange: (...sections: string[]) => {
      const affected = new Set(sections);
      for (const listener of Array.from(configurationListeners)) {
        listener({
          affectsConfiguration: (section: string) => affected.has(section),
        });
      }
    },
    configurationListeners,
    lastExportFile: null,
    exportDirectory: "",
  };

  return state;
}

export function buildWorkflowVscodeModule(
  state: WorkflowVscodeState,
): Record<string, unknown> {
  class EventEmitter<T> extends BridgeEmitter<T> {}

  class Disposable {
    private readonly cleanup: () => void;
    constructor(cleanup: () => void) {
      this.cleanup = cleanup;
    }
    dispose(): void {
      this.cleanup();
    }
  }

  return {
    EventEmitter,
    Disposable,
    Uri: BridgeUri,
    ProgressLocation: { Notification: 15, Window: 10 },
    ViewColumn: { One: 1, Two: 2, Three: 3, Active: -1, Beside: -2 },
    ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
    FileType: { Unknown: 0, File: 1, Directory: 2, SymbolicLink: 64 },
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
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
    CancellationTokenSource: class CancellationTokenSource {
      readonly token = new ProgressToken();
      cancel(): void {
        this.token.cancel();
      }
      dispose(): void {}
    },
    workspace: {
      onDidChangeConfiguration: (
        listener: (event: {
          affectsConfiguration: (section: string) => boolean;
        }) => void,
      ) => {
        state.configurationListeners.add(listener);
        return {
          dispose: () => {
            state.configurationListeners.delete(listener);
          },
        };
      },
      getConfiguration: (section: string) => ({
        get: (_key: string, fallback?: unknown) => fallback,
        update: async () => undefined,
        inspect: () => undefined,
        has: () => true,
        section,
      }),
      workspaceFolders: undefined,
      fs: {
        readFile: async () => new Uint8Array(),
        writeFile: async (uri: { fsPath: string }, content: Uint8Array) => {
          const text = new TextDecoder().decode(content);
          state.lastExportFile = { path: uri.fsPath, content: text };
        },
      },
      asRelativePath: (uri: { fsPath: string } | string) =>
        typeof uri === "string" ? uri : uri.fsPath,
    },
    window: {
      createWebviewPanel: state.createWebviewPanel,
      createTreeView: state.createTreeView,
      showInformationMessage: state.showInformationMessage,
      showWarningMessage: state.showWarningMessage,
      showErrorMessage: state.showErrorMessage,
      showInputBox: state.showInputBox,
      showSaveDialog: state.showSaveDialog,
      showOpenDialog: state.showOpenDialog,
      withProgress: state.withProgress,
      activeTextEditor: undefined,
      visibleTextEditors: [],
    },
    commands: {
      registerCommand: state.registerCommand,
      executeCommand: async () => undefined,
    },
    env: {
      clipboard: {
        writeText: state.writeClipboard,
        readText: state.readClipboard,
      },
      openExternal: async () => true,
      uri: { scheme: "vscode-resource" },
    },
    extensions: {
      getExtension: () => undefined,
    },
  };
}

export type { FakeWebviewUri };
