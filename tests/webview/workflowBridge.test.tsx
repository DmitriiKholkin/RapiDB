import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createFakeWebviewPanel,
  type FakeWebviewPanelHandle,
} from "../workflow/bridge/fakeWebviewPanel";
import {
  type BridgeSession,
  startBridge,
} from "../workflow/bridge/hostWebviewBridge";
import { createWorkflowVscodeState } from "../workflow/bridge/workflowVscode";

vi.mock("../../src/webview/components/App", () => ({
  App: () => <div>Bridge fixture</div>,
}));

function innerWebview(panel: FakeWebviewPanelHandle) {
  return (panel.panel as { webview: FakeWebviewPanelHandle["webview"] })
    .webview;
}

let session: BridgeSession | undefined;
let previousVscode: Window["__vscode"];

beforeEach(() => {
  previousVscode = window.__vscode;
});

afterEach(() => {
  session?.unmount();
  session = undefined;
  window.__vscode = previousVscode;
});

async function bridge(panel: FakeWebviewPanelHandle) {
  delete window.__vscode;
  const state = createWorkflowVscodeState();
  state.panels.push(panel);
  session = await startBridge({ state, initialState: {} });
  return session;
}

describe("workflow host delivery contract", () => {
  it("returns true and records delivery through both fake webview handles", async () => {
    const panel = createFakeWebviewPanel({ viewType: "table", title: "Table" });
    const first = { type: "tableInit" };
    const second = { type: "applyResult" };
    expect(await panel.webview.postMessage(first)).toBe(true);
    expect(await innerWebview(panel).postMessage(second)).toBe(true);
    expect(panel.hostMessages()).toEqual([first, second]);
  });

  it("returns false without recording delivery after disposal", async () => {
    const panel = createFakeWebviewPanel({ viewType: "table", title: "Table" });
    const disposed = vi.fn();
    panel.onDidDispose(disposed);
    panel.dispose();
    panel.dispose();
    expect(disposed).toHaveBeenCalledTimes(1);
    expect(await panel.webview.postMessage({ type: "tableInit" })).toBe(false);
    expect(await innerWebview(panel).postMessage({ type: "tableInit" })).toBe(
      false,
    );
    expect(panel.hostMessages()).toEqual([]);
  });

  it("dispatches once only after a successful original delivery", async () => {
    const panel = createFakeWebviewPanel({ viewType: "table", title: "Table" });
    const webview = innerWebview(panel);
    const deliver = webview.postMessage;
    let acknowledge: (delivered: boolean) => void = () => {};
    webview.postMessage = (message) =>
      new Promise<boolean>((resolve) => {
        acknowledge = (delivered) => {
          if (delivered) void deliver(message);
          resolve(delivered);
        };
      });
    await bridge(panel);
    const dispatch = vi.spyOn(window, "dispatchEvent");
    const message = {
      type: "tableMutationPreview",
      payload: { previewId: "p" },
    };
    const pending = webview.postMessage(message);
    expect(dispatch).not.toHaveBeenCalled();
    acknowledge(true);
    expect(await pending).toBe(true);
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(dispatch.mock.calls[0][0]).toMatchObject({
      type: "message",
      data: message,
    });
    expect(panel.hostMessages()).toEqual([message]);
  });

  it("does not dispatch a declined message and allows a later successful delivery", async () => {
    const panel = createFakeWebviewPanel({ viewType: "table", title: "Table" });
    const webview = innerWebview(panel);
    const deliver = webview.postMessage;
    webview.postMessage = vi.fn(deliver).mockResolvedValueOnce(false);
    await bridge(panel);
    const dispatch = vi.spyOn(window, "dispatchEvent");
    const message = { type: "tableMutationPreview" };
    expect(await webview.postMessage(message)).toBe(false);
    expect(dispatch).not.toHaveBeenCalled();
    expect(panel.hostMessages()).toEqual([]);
    expect(await webview.postMessage(message)).toBe(true);
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(panel.hostMessages()).toEqual([message]);
  });

  it("does not dispatch a rejected delivery or a disposed panel's message", async () => {
    const panel = createFakeWebviewPanel({ viewType: "table", title: "Table" });
    const webview = innerWebview(panel);
    webview.postMessage = vi
      .fn(webview.postMessage)
      .mockRejectedValueOnce(new Error("transport rejected"));
    await bridge(panel);
    const dispatch = vi.spyOn(window, "dispatchEvent");
    await expect(webview.postMessage({ type: "applyResult" })).rejects.toThrow(
      "transport rejected",
    );
    panel.dispose();
    expect(await webview.postMessage({ type: "applyResult" })).toBe(false);
    expect(dispatch).not.toHaveBeenCalled();
    expect(panel.hostMessages()).toEqual([]);
  });
});
