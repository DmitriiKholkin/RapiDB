/**
 * Test harness for the workflow tests. This file is imported by every
 * workflow test as the first import; it installs the vscode mock,
 * the Monaco editor mock, the react-virtual mock, and exposes a shared
 * state object that tests can use to capture panels and mock state.
 */
import { vi } from "vitest";

const { workflowState, vscodeMockFactory } = vi.hoisted(() => {
  const state = {
    panels: [] as unknown[],
    showInformationMessage: () => undefined,
    showWarningMessage: () => undefined,
    showErrorMessage: () => undefined,
    showInputBox: () => undefined,
    showSaveDialog: () => undefined,
    showOpenDialog: () => undefined,
    withProgress: () => undefined,
    writeClipboard: () => undefined,
    readClipboard: () => undefined,
    createWebviewPanel: () => ({}),
    createTreeView: () => ({}),
    registerCommand: () => ({}),
    configurationListeners: new Set<
      (event: { affectsConfiguration: (s: string) => boolean }) => void
    >(),
    lastExportFile: null,
    exportDirectory: "",
    fireConfigurationChange: () => undefined,
  };
  const factory = () => {
    return {
      EventEmitter: class EventEmitter<T> {
        private readonly listeners = new Set<(value: T) => void>();
        readonly event = (
          listener: (value: T) => void,
        ): { dispose(): void } => {
          this.listeners.add(listener);
          return { dispose: () => this.listeners.delete(listener) };
        };
        fire(value: T): void {
          for (const l of Array.from(this.listeners)) {
            l(value);
          }
        }
        dispose(): void {
          this.listeners.clear();
        }
      },
      Uri: class Uri {
        constructor(
          readonly scheme: string,
          readonly path: string,
          readonly fsPath: string,
        ) {}
        toString(): string {
          return `${this.scheme}://${this.path}`;
        }
        with(change: { path?: string; fsPath?: string }): Uri {
          return new Uri(
            this.scheme,
            change.path ?? this.path,
            change.fsPath ?? this.fsPath,
          );
        }
        static joinPath(base: Uri, ...paths: string[]): Uri {
          return new Uri(
            base.scheme,
            paths.join("/"),
            `${base.fsPath}/${paths.join("/")}`,
          );
        }
        static file(fsPath: string): Uri {
          return new Uri("file", fsPath, fsPath);
        }
        static parse(value: string): Uri {
          const m = /^([a-z0-9+\-.]+):\/\/(.*)$/i.exec(value);
          return m ? new Uri(m[1], m[2], value) : new Uri("file", value, value);
        }
      },
      window: {
        get createWebviewPanel() {
          return state.createWebviewPanel;
        },
        get createTreeView() {
          return state.createTreeView;
        },
        get showInformationMessage() {
          return state.showInformationMessage;
        },
        get showWarningMessage() {
          return state.showWarningMessage;
        },
        get showErrorMessage() {
          return state.showErrorMessage;
        },
        get showInputBox() {
          return state.showInputBox;
        },
        get showSaveDialog() {
          return state.showSaveDialog;
        },
        get showOpenDialog() {
          return state.showOpenDialog;
        },
        get withProgress() {
          return state.withProgress;
        },
      },
      workspace: {
        onDidChangeConfiguration: (
          listener: (event: {
            affectsConfiguration: (s: string) => boolean;
          }) => { dispose(): void },
        ) => {
          state.configurationListeners.add(listener);
          return {
            dispose: () => state.configurationListeners.delete(listener),
          };
        },
        getConfiguration: () => ({
          get: () => undefined,
          update: async () => undefined,
          has: () => true,
        }),
        workspaceFolders: undefined,
        fs: {
          readFile: async () => new Uint8Array(),
          writeFile: async (uri: { fsPath: string }, content: Uint8Array) => {
            const text = new TextDecoder().decode(content);
            (
              state as {
                lastExportFile: { path: string; content: string } | null;
              }
            ).lastExportFile = {
              path: uri.fsPath,
              content: text,
            };
          },
        },
        asRelativePath: () => "",
      },
      commands: {
        get registerCommand() {
          return state.registerCommand;
        },
        executeCommand: async () => undefined,
      },
      env: {
        clipboard: {
          get writeText() {
            return state.writeClipboard;
          },
          get readText() {
            return state.readClipboard;
          },
        },
        openExternal: async () => true,
      },
      extensions: { getExtension: () => undefined },
      ProgressLocation: { Notification: 15, Window: 10 },
      ViewColumn: { One: 1, Two: 2 },
      ConfigurationTarget: { Global: 1 },
      FileType: { Unknown: 0, File: 1, Directory: 2 },
      TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
      ThemeIcon: class ThemeIconImpl {},
      ThemeColor: class ThemeColor {},
      TreeItem: class TreeItem {},
      Disposable: class Disposable {
        dispose() {}
      },
    };
  };
  return { workflowState: state, vscodeMockFactory: factory };
});

vi.mock("vscode", vscodeMockFactory);

vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue,
    ariaLabel,
    language,
    dialect,
  }: {
    initialValue?: string;
    onChange?: (value: string) => void;
    readOnly?: boolean;
    ariaLabel?: string;
    schema?: Array<unknown>;
    dialect?: string;
    language?: string;
  }) => (
    <div>
      <div data-testid="monaco-language">{language ?? "sql"}</div>
      <div data-testid="monaco-dialect">{dialect ?? "none"}</div>
      <textarea
        aria-label={ariaLabel ?? "SQL editor"}
        defaultValue={initialValue ?? ""}
        data-testid="sql-editor"
      />
    </div>
  ),
  connTypeToDialect: (type: string) => {
    switch (type) {
      case "mysql":
        return "mysql";
      case "pg":
        return "postgresql";
      case "sqlite":
        return "sqlite";
      case "mssql":
        return "transactsql";
      case "oracle":
        return "plsql";
      case "mongodb":
        return "javascript";
      case "redis":
        return "javascript";
      case "elasticsearch":
        return "javascript";
      case "dynamodb":
        return "javascript";
      default:
        return "sql";
    }
  },
}));

vi.mock("../../src/webview/components/table/TableGrid", () => ({
  TableGrid: ({
    columns = [],
    rows = [],
  }: {
    columns?: Array<{ name: string }>;
    rows?: Array<Record<string, unknown>>;
  }) => {
    return (
      <table data-testid="workflow-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.name} data-column-id={col.name}>
                {col.name}
                <button
                  type="button"
                  aria-label={`Resize ${col.name} column`}
                  data-testid={`resize-${col.name}`}
                  style={{ width: 10, height: 20 }}
                />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr
              key={
                (row as { id?: string | number }).id !== undefined
                  ? String((row as { id?: string | number }).id)
                  : `row-${rowIndex}`
              }
            >
              {columns.map((col) => (
                <td key={col.name}>{String(row[col.name] ?? "")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    );
  },
}));

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 26,
        end: (index + 1) * 26,
      })),
    getTotalSize: () => count * 26,
  }),
}));

vi.mock("@vscode/codicons/dist/codicon.css", () => ({}));

vi.mock("@xyflow/react/dist/style.css", () => ({}));

vi.mock("@xyflow/react", () => ({
  ReactFlow: () => null,
  ReactFlowProvider: ({ children }: { children: unknown }) => children,
  useNodesState: (initial: unknown) => [
    initial,
    () => undefined,
    () => undefined,
  ],
  useEdgesState: (initial: unknown) => [
    initial,
    () => undefined,
    () => undefined,
  ],
  Controls: () => null,
  Background: () => null,
  BackgroundVariant: { Dots: "dots" },
  Handle: () => null,
  Position: { Top: "top", Bottom: "bottom", Left: "left", Right: "right" },
  MarkerType: { ArrowClosed: "arrow-closed" },
}));

export { workflowState };
