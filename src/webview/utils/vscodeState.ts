import type { WebviewInitialState } from "../../shared/webviewContracts";

export function readWebviewState<T>(fallback: T): T {
  return window.__vscode?.getState<T>() ?? fallback;
}

export function writeWebviewState<T>(state: T): void {
  window.__vscode?.setState<T>(state);
}

export function updateWebviewState(
  update: (state: Record<string, unknown>) => Record<string, unknown>,
): void {
  const vscode = window.__vscode;
  if (!vscode) return;
  const current = vscode.getState<Record<string, unknown>>() ?? {};
  vscode.setState(update(current));
}

export function syncInitialWebviewState(
  vscode: VSCodeAPI,
  initialState: WebviewInitialState | undefined,
): void {
  const persistedState = vscode.getState<Record<string, unknown>>();

  if (
    initialState === undefined &&
    persistedState?.initialState !== undefined
  ) {
    window.__RAPIDB_INITIAL_STATE__ =
      persistedState.initialState as WebviewInitialState;
  }

  if (window.__RAPIDB_INITIAL_STATE__ !== undefined) {
    // Merge initialState into the existing persisted state so that other
    // webview data (e.g. ERD viewport, node positions) is not overwritten.
    vscode.setState({
      ...persistedState,
      initialState: window.__RAPIDB_INITIAL_STATE__,
    });
  }
}
