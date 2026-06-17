import { act, type RenderResult, render } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import type {
  TableInitialState,
  WebviewInitialState,
} from "../../../src/shared/webviewContracts";
import { App } from "../../../src/webview/components/App";
import {
  type FakeWebviewPanelHandle,
  type WorkflowMessageEnvelope,
} from "./fakeWebviewPanel";
import { type WorkflowVscodeState } from "./workflowVscode";

export interface BridgeOptions {
  readonly state: WorkflowVscodeState;
  readonly initialState: unknown;
  readonly stubGlobals?: {
    readonly now?: () => number;
  };
}

export interface BridgeSession {
  readonly panel: FakeWebviewPanelHandle;
  readonly render: RenderResult;
  readonly user: ReturnType<typeof userEvent.setup>;
  readonly hostMessages: () => WorkflowMessageEnvelope[];
  readonly dispatchHost: (message: WorkflowMessageEnvelope) => Promise<void>;
  readonly sendFromWebview: (message: WorkflowMessageEnvelope) => Promise<void>;
  readonly waitForHost: (
    predicate: (messages: WorkflowMessageEnvelope[]) => boolean,
    options?: { timeout?: number },
  ) => Promise<void>;
  readonly unmount: () => void;
  readonly stubInitialState: (state: unknown) => void;
}

export interface BridgeGlobals {
  window: Window & {
    __vscode?: unknown;
    __RAPIDB_INITIAL_STATE__?: unknown;
  };
  ackedWebviewMessages: WorkflowMessageEnvelope[];
  vscode: {
    postMessage: (message: WorkflowMessageEnvelope) => void;
    getState: () => unknown;
    setState: (state: unknown) => void;
  };
}

export function buildBridgeGlobals(): BridgeGlobals {
  const vscode = {
    postMessage: (_message: WorkflowMessageEnvelope) => undefined,
    getState: () => ({}),
    setState: (_state: unknown) => undefined,
  };
  return {
    window: window as BridgeGlobals["window"],
    ackedWebviewMessages: [],
    vscode,
  };
}

// NOTE: startBridge mutates window.__vscode and window.__RAPIDB_INITIAL_STATE__.
// This is safe because the db-workflow vitest project has fileParallelism: false.
// If that changes, this function must be made reentrant (e.g. per-session globals).
export async function startBridge(
  options: BridgeOptions,
): Promise<BridgeSession> {
  const panel = options.state.panels.at(-1);
  if (!panel) {
    throw new Error(
      "[workflow bridge] No panel was created before startBridge() was called.",
    );
  }

  const hostWindow = window as Window & {
    __vscode?: unknown;
    __RAPIDB_INITIAL_STATE__?: unknown;
  };

  if (hostWindow.__vscode !== undefined) {
    throw new Error(
      "[workflow bridge] window.__vscode is already set — " +
        "are multiple bridge sessions running concurrently? " +
        "Ensure fileParallelism: false in the vitest workspace config.",
    );
  }

  const previousInitialState = hostWindow.__RAPIDB_INITIAL_STATE__;
  const previousVscode = hostWindow.__vscode;

  hostWindow.__RAPIDB_INITIAL_STATE__ = options.initialState as
    | WebviewInitialState
    | undefined;

  const ackedWebviewMessages: WorkflowMessageEnvelope[] = [];
  const vscodeBridge = {
    postMessage: (message: WorkflowMessageEnvelope) => {
      ackedWebviewMessages.push(message);
      void panel.dispatchMessage(message);
    },
    getState: <T,>(): T => {
      if (
        previousVscode &&
        typeof previousVscode === "object" &&
        typeof (previousVscode as { getState?: () => unknown }).getState ===
          "function"
      ) {
        return (previousVscode as { getState: () => T }).getState();
      }
      return {} as T;
    },
    setState: (state: unknown) => {
      const next =
        previousVscode && typeof previousVscode === "object"
          ? (previousVscode as { setState?: (state: unknown) => void })
          : null;
      next?.setState?.(state);
    },
  };

  hostWindow.__vscode = vscodeBridge as unknown as typeof window.__vscode;

  // Override the panel's INNER webview.postMessage to dispatch a MessageEvent
  // to the window, so the webview's message handler (from messaging.ts) receives it.
  // The panel returned by the fake's createWebviewPanel is `handle.panel`,
  // which has its own webview. We need to override that one.
  const innerPanel = (panel.panel ?? panel) as {
    webview: {
      postMessage: (message: WorkflowMessageEnvelope) => Thenable<void>;
    };
  };
  const panelWebview = innerPanel.webview;
  const originalPostMessage = panelWebview.postMessage;
  panelWebview.postMessage = (message: WorkflowMessageEnvelope) => {
    hostWindow.dispatchEvent(new MessageEvent("message", { data: message }));
    return originalPostMessage(message);
  };

  const user = userEvent.setup({
    delay: null,
    pointerEventsCheck: 0,
  });

  let renderResult: RenderResult | null = null;
  await act(async () => {
    renderResult = render(<App />);
  });
  if (!renderResult) {
    throw new Error("[workflow bridge] Failed to render App");
  }

  // Wait for the webview's effects to register their message handlers
  // before letting the host respond.
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 50));
  });

  const session: BridgeSession = {
    panel,
    render: renderResult,
    user,
    hostMessages: () => panel.hostMessages(),
    dispatchHost: async (message: WorkflowMessageEnvelope) => {
      await act(async () => {
        await panel.dispatchMessage(message);
      });
    },
    sendFromWebview: async (message: WorkflowMessageEnvelope) => {
      await act(async () => {
        ackedWebviewMessages.push(message);
        await panel.dispatchMessage(message);
      });
    },
    waitForHost: async (
      predicate: (messages: WorkflowMessageEnvelope[]) => boolean,
      waitOptions?: { timeout?: number },
    ) => {
      const deadline = Date.now() + (waitOptions?.timeout ?? 2000);
      while (Date.now() < deadline) {
        if (predicate(panel.hostMessages())) {
          return;
        }
        await act(async () => {
          await Promise.resolve();
        });
      }
      throw new Error(
        "[workflow bridge] waitForHost timed out waiting for matching host message",
      );
    },
    unmount: () => {
      renderResult?.unmount();
      panelWebview.postMessage = originalPostMessage;
      hostWindow.__RAPIDB_INITIAL_STATE__ = previousInitialState;
      hostWindow.__vscode = previousVscode;
    },
    stubInitialState: (state: unknown) => {
      hostWindow.__RAPIDB_INITIAL_STATE__ = state as
        | WebviewInitialState
        | undefined;
    },
  };

  return session;
}

export function connectionConfigFromPanel(panel: FakeWebviewPanelHandle): {
  database?: string;
  schema?: string;
  table?: string;
  connectionId?: string;
  isView?: boolean;
} {
  const initial = panel.lastInitialState() as
    | Partial<TableInitialState>
    | undefined;
  if (!initial) {
    return {};
  }
  return {
    database: initial.database,
    schema: initial.schema,
    table: initial.table,
    connectionId: initial.connectionId,
    isView: initial.isView,
  };
}

export interface TablePanelState extends TableInitialState {
  connectionReadOnly?: boolean;
  defaultPageSize?: number;
}

export function tableInitialStateFor(
  connection: Pick<ConnectionConfig, "id" | "name" | "type" | "readOnly">,
  options: {
    database: string;
    schema: string;
    table: string;
    isView?: boolean;
    defaultPageSize?: number;
  },
): TablePanelState {
  return {
    view: "table",
    connectionId: connection.id,
    database: options.database,
    schema: options.schema,
    table: options.table,
    isView: options.isView ?? false,
    connectionReadOnly: connection.readOnly ?? false,
    defaultPageSize: options.defaultPageSize ?? 25,
  };
}

export function queryInitialStateFor(
  connection: Pick<ConnectionConfig, "id" | "type">,
  options: { initialSql?: string; editorLanguage?: "sql" | "javascript" } = {},
): {
  view: "query";
  connectionId: string;
  connectionType: string;
  queryText: string;
  initialSql: string;
  formatOnOpen: boolean;
  isBookmarked: boolean;
  editorLanguage: "sql" | "javascript";
} {
  return {
    view: "query",
    connectionId: connection.id,
    connectionType: connection.type,
    queryText: options.initialSql ?? "",
    initialSql: options.initialSql ?? "",
    formatOnOpen: false,
    isBookmarked: false,
    editorLanguage: options.editorLanguage ?? "sql",
  };
}

export function connectionFormInitialStateFor(existing?: ConnectionConfig): {
  view: "connection";
  existing: unknown;
} {
  return {
    view: "connection",
    existing: existing ?? null,
  };
}

export function erdInitialStateFor(
  connection: Pick<ConnectionConfig, "id">,
  scope: { database?: string; schema?: string } = {},
): {
  view: "erd";
  connectionId: string;
  database?: string;
  schema?: string;
} {
  return {
    view: "erd",
    connectionId: connection.id,
    database: scope.database,
    schema: scope.schema,
  };
}
