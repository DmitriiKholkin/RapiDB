import type { WebviewInitialState } from "../../shared/webviewContracts";

export function readWebviewState<T>(fallback: T): T {
  return window.__vscode?.getState<T>() ?? fallback;
}

export function writeWebviewState<T>(state: T): void {
  window.__vscode?.setState<T>(state);
}

export function syncInitialWebviewState(
  vscode: VSCodeAPI,
  initialState: WebviewInitialState | undefined,
): void {
  const persistedState = vscode.getState<{
    initialState?: WebviewInitialState;
  }>();

  if (
    initialState === undefined &&
    persistedState?.initialState !== undefined
  ) {
    window.__RAPIDB_INITIAL_STATE__ = persistedState.initialState;
  }

  if (window.__RAPIDB_INITIAL_STATE__ !== undefined) {
    vscode.setState({
      initialState: window.__RAPIDB_INITIAL_STATE__,
    });
  }
}
