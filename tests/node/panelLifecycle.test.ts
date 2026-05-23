import { describe, expect, it, vi } from "vitest";
import {
  attachPanelDisposables,
  disposePanelInstances,
} from "../../src/extension/panels/panelLifecycle";

describe("panelLifecycle", () => {
  it("disposes attached disposables when panel is disposed", () => {
    let disposeHandler: (() => void) | undefined;
    const panel = {
      onDidDispose(listener: () => void) {
        disposeHandler = listener;
        return { dispose: vi.fn() };
      },
    };

    const first = { dispose: vi.fn() };
    const second = { dispose: vi.fn() };

    attachPanelDisposables(panel as never, first, second);
    disposeHandler?.();

    expect(first.dispose).toHaveBeenCalledOnce();
    expect(second.dispose).toHaveBeenCalledOnce();
  });

  it("continues disposing remaining disposables after one throws", () => {
    let disposeHandler: (() => void) | undefined;
    const panel = {
      onDidDispose(listener: () => void) {
        disposeHandler = listener;
        return { dispose: vi.fn() };
      },
    };

    const first = {
      dispose: vi.fn(() => {
        throw new Error("dispose failed");
      }),
    };
    const second = { dispose: vi.fn() };

    attachPanelDisposables(panel as never, first, second);

    expect(() => disposeHandler?.()).not.toThrow();
    expect(first.dispose).toHaveBeenCalledOnce();
    expect(second.dispose).toHaveBeenCalledOnce();
  });

  it("continues disposing all panel instances even when one fails", () => {
    const calls: number[] = [];
    const instances = [1, 2, 3];

    expect(() =>
      disposePanelInstances(instances, (instance) => {
        calls.push(instance);
        if (instance === 2) {
          throw new Error("dispose failed");
        }
      }),
    ).not.toThrow();

    expect(calls).toEqual([1, 2, 3]);
  });
});
