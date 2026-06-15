import { cleanup } from "@testing-library/react";
import { afterEach, beforeEach, vi } from "vitest";
import {
  useConnectionStore,
  useQueryStore,
  useSchemaStore,
} from "../../src/webview/store";

interface StubVsCodeApi {
  postMessage: ReturnType<typeof vi.fn>;
  getState: ReturnType<typeof vi.fn>;
  setState: ReturnType<typeof vi.fn>;
}

/**
 * Minimal `ClipboardEvent` polyfill for JSDOM 29+, which no longer
 * ships a default implementation. We only need a class that the
 * paste-handler tests can `new` against — neither the test nor the
 * production code reads the event's `clipboardData` payload.
 */
if (typeof globalThis.ClipboardEvent === "undefined") {
  globalThis.ClipboardEvent = Event as unknown as typeof ClipboardEvent;
}

function createStubCanvasContext(): CanvasRenderingContext2D {
  return {
    canvas: document.createElement("canvas"),
    font: "12px monospace",
    measureText(text: string): TextMetrics {
      return {
        width: text.length * 8,
      } as TextMetrics;
    },
  } as CanvasRenderingContext2D;
}

function createStubVsCodeApi(): StubVsCodeApi {
  return {
    postMessage: vi.fn(),
    getState: vi.fn(),
    setState: vi.fn(),
  };
}

beforeEach(() => {
  const vscodeApi = createStubVsCodeApi();
  vi.stubGlobal("acquireVsCodeApi", () => vscodeApi);
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockImplementation(
    (contextId: string) => {
      return contextId === "2d" ? createStubCanvasContext() : null;
    },
  );
  const windowWithState = window as Window & {
    __vscode?: StubVsCodeApi;
    __RAPIDB_INITIAL_STATE__?: unknown;
  };
  windowWithState.__vscode = vscodeApi as unknown as StubVsCodeApi &
    Window["__vscode"];
  delete windowWithState.__RAPIDB_INITIAL_STATE__;
});

afterEach(() => {
  cleanup();

  useQueryStore.setState({ status: "idle", result: null });
  useConnectionStore.setState({ connections: [], activeConnectionId: "" });
  useSchemaStore.setState({ schemaByConnection: {} });
});
