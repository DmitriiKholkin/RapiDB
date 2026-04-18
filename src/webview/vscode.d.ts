import type { WebviewInitialState } from "../shared/webviewContracts";

declare global {
  interface VSCodeAPI {
    postMessage(message: unknown): void;

    getState<T = unknown>(): T | undefined;

    setState<T = unknown>(state: T): void;
  }

  function acquireVsCodeApi(): VSCodeAPI;

  interface Window {
    __vscode?: VSCodeAPI;
    __RAPIDB_INITIAL_STATE__?: WebviewInitialState;
  }
}
