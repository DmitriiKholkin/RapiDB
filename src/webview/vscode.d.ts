interface VSCodeAPI {
  postMessage(message: unknown): void;

  getState<T = unknown>(): T | undefined;

  setState<T = unknown>(state: T): void;
}

declare function acquireVsCodeApi(): VSCodeAPI;
