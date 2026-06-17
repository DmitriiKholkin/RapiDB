import { BridgeEmitter } from "./bridgeEmitter";

export interface WorkflowMessageEnvelope {
  type: string;
  payload?: unknown;
}

export interface FakeWebviewUri {
  readonly scheme: string;
  readonly path: string;
  readonly toString: () => string;
  readonly fsPath: string;
}

export interface FakeWebviewPanelHandle {
  readonly viewType: string;
  readonly title: string;
  readonly panel: unknown;
  readonly webview: {
    readonly html: string;
    setHtml?: never;
    readonly options: {
      enableScripts: boolean;
      localResourceRoots: FakeWebviewUri[];
    };
    setOptions?: never;
    readonly cspSource: string;
    readonly asWebviewUri: (uri: FakeWebviewUri) => FakeWebviewUri;
    postMessage: (message: WorkflowMessageEnvelope) => Thenable<void>;
    onDidReceiveMessage: (
      listener: (message: WorkflowMessageEnvelope) => void,
    ) => { dispose(): void };
  };
  readonly setHtml: (value: string) => void;
  readonly reveal: () => void;
  readonly dispose: () => void;
  readonly onDidDispose: (listener: () => void) => { dispose(): void };
  readonly hostMessages: () => WorkflowMessageEnvelope[];
  readonly lastInitialState: () => unknown;
  readonly dispatchMessage: (message: WorkflowMessageEnvelope) => Promise<void>;
}

export interface CreateFakeWebviewPanelOptions {
  viewType: string;
  title: string;
}

interface EventSubscription {
  dispose(): void;
}

function makeUri(value: string): FakeWebviewUri {
  const parsed = /^([a-z0-9+\-.]+):\/\/(.*)$/i.exec(value);
  const scheme = parsed ? parsed[1] : "file";
  const path = parsed ? parsed[2] : value;
  const fsPath = scheme === "file" ? path : value;
  return {
    scheme,
    path,
    toString: () => value,
    fsPath,
  };
}

const ASSIGNMENT_MARKER = "window.__RAPIDB_INITIAL_STATE__";

function extractInitialState(html: string): unknown {
  const idx = html.indexOf(ASSIGNMENT_MARKER);
  if (idx === -1) {
    return undefined;
  }
  const jsonStart = html.indexOf("{", idx);
  if (jsonStart === -1) {
    return undefined;
  }
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = jsonStart; i < html.length; i++) {
    const ch = html[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (ch === "\\") {
      escape = true;
      continue;
    }
    if (ch === '"') {
      inString = !inString;
      continue;
    }
    if (inString) {
      continue;
    }
    if (ch === "{") {
      depth++;
    } else if (ch === "}") {
      depth--;
      if (depth === 0) {
        const jsonCandidate = html.slice(jsonStart, i + 1);
        try {
          return JSON.parse(jsonCandidate);
        } catch {
          return undefined;
        }
      }
    }
  }
  return undefined;
}

export function createFakeWebviewPanel(
  options: CreateFakeWebviewPanelOptions,
): FakeWebviewPanelHandle {
  const disposeEmitter = new BridgeEmitter<void>();
  const receiveEmitter = new BridgeEmitter<WorkflowMessageEnvelope>();
  const hostMessages: WorkflowMessageEnvelope[] = [];

  let html = "";
  let panelOptions: {
    enableScripts: boolean;
    localResourceRoots: FakeWebviewUri[];
  } = {
    enableScripts: true,
    localResourceRoots: [makeUri("vscode-resource://workflow")],
  };
  const cspSource = "vscode-resource://workflow";

  const panel: {
    viewType: string;
    title: string;
    visible: boolean;
    active: boolean;
    viewColumn: number;
    options: { enableScripts: boolean; localResourceRoots: FakeWebviewUri[] };
    reveal: () => void;
    dispose: () => void;
    onDidDispose: (listener: () => void) => EventSubscription;
    webview: {
      html: string;
      options: { enableScripts: boolean; localResourceRoots: FakeWebviewUri[] };
      cspSource: string;
      asWebviewUri: (uri: FakeWebviewUri) => FakeWebviewUri;
      postMessage: (message: WorkflowMessageEnvelope) => Thenable<void>;
      onDidReceiveMessage: (
        listener: (message: WorkflowMessageEnvelope) => void,
      ) => { dispose(): void };
    };
  } = {
    viewType: options.viewType,
    title: options.title,
    visible: true,
    active: true,
    viewColumn: 1,
    get options() {
      return panelOptions;
    },
    set options(value: {
      enableScripts: boolean;
      localResourceRoots: FakeWebviewUri[];
    }) {
      panelOptions = value;
    },
    reveal: () => undefined,
    dispose: () => {
      disposeEmitter.fire();
    },
    onDidDispose: (listener: () => void): EventSubscription => {
      return disposeEmitter.event(listener);
    },
    webview: {
      get cspSource(): string {
        return cspSource;
      },
      get html(): string {
        return html;
      },
      set html(value: string) {
        html = value;
      },
      get options() {
        return panelOptions;
      },
      set options(value: {
        enableScripts: boolean;
        localResourceRoots: FakeWebviewUri[];
      }) {
        panelOptions = value;
      },
      asWebviewUri: (uri) => uri,
      postMessage: (message: WorkflowMessageEnvelope) => {
        hostMessages.push(message);
        return Promise.resolve();
      },
      onDidReceiveMessage: (listener) => receiveEmitter.event(listener),
    },
  };

  return {
    get viewType(): string {
      return panel.viewType;
    },
    get title(): string {
      return panel.title;
    },
    get panel(): unknown {
      return panel;
    },
    webview: {
      get html() {
        return html;
      },
      set html(value: string) {
        html = value;
      },
      get cspSource() {
        return cspSource;
      },
      get options() {
        return panelOptions;
      },
      set options(value) {
        panelOptions = value;
      },
      asWebviewUri: (uri) => uri,
      postMessage: (message: WorkflowMessageEnvelope) => {
        hostMessages.push(message);
        return Promise.resolve();
      },
      onDidReceiveMessage: (listener) => receiveEmitter.event(listener),
    },
    setHtml: (value: string) => {
      html = value;
    },
    reveal: () => undefined,
    dispose: () => panel.dispose(),
    onDidDispose: (listener: () => void) => disposeEmitter.event(listener),
    hostMessages: () => hostMessages.slice(),
    lastInitialState: () => extractInitialState(html),
    dispatchMessage: async (message: WorkflowMessageEnvelope) => {
      receiveEmitter.fire(message);
    },
  };
}
