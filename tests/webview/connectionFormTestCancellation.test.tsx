import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import * as vscode from "vscode";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import type { ConnectionFormExistingState } from "../../src/shared/webviewContracts";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";
import type { MockWebviewPanel } from "../support/mockVscode";
import { dispatchWindowMessage } from "./testUtils";

vi.mock("vscode", async () => {
  const { createMockVscodeModule } = await import("../support/mockVscode");
  return createMockVscodeModule().module;
});
vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

const existing: ConnectionFormExistingState = {
  id: "cancelled-test",
  name: "Cancelled test",
  type: "pg",
  host: "localhost",
  database: "app",
  username: "reader",
};

describe("connection test cancellation through real host and webview handlers", () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([
    "success",
    "secrets-error",
    "driver-error",
  ])("does not consume B's %s result when cancelled A rejects", async (outcome) => {
    const first = deferred<string | undefined>();
    const second = deferred<string | undefined>();
    const currentDriver = deferred<{ success: boolean }>();
    const context = {
      secrets: {
        get: vi
          .fn()
          .mockResolvedValueOnce(undefined)
          .mockReturnValueOnce(first.promise)
          .mockReturnValueOnce(second.promise),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(() => currentDriver.promise),
      saveConnection: vi.fn(),
    };
    const result = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await waitFor(() =>
      expect(vscode.window.createWebviewPanel).toHaveBeenCalledOnce(),
    );
    const panel = vi.mocked(vscode.window.createWebviewPanel).mock.results[0]
      .value as MockWebviewPanel;
    panel.webview.postMessage.mockImplementation((message: unknown) => {
      dispatchWindowMessage(message);
      return Promise.resolve(true);
    });
    const requests: Promise<void>[] = [];
    if (!window.__vscode) throw new Error("Expected VS Code API");
    vi.mocked(window.__vscode.postMessage).mockImplementation((message) => {
      requests.push(panel.webview.dispatchMessage(message));
    });
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    try {
      await user.click(screen.getByRole("button", { name: "Test Connection" }));
      expect(context.secrets.get).toHaveBeenCalledTimes(2);
      await user.click(screen.getByRole("button", { name: "Cancel Testing" }));
      await user.click(screen.getByRole("button", { name: "Test Connection" }));
      expect(context.secrets.get).toHaveBeenCalledTimes(3);

      await act(async () => {
        first.reject(new Error("Stale A failure"));
        await requests[0];
      });
      expect(panel.webview.postMessage).not.toHaveBeenCalled();
      expect(screen.queryByText(/Stale A failure/)).toBeNull();
      expect(
        screen.getByRole("button", { name: "Cancel Testing" }),
      ).toBeTruthy();
      expect(
        screen
          .getByRole("button", { name: "Save Changes" })
          .matches(":disabled"),
      ).toBe(true);

      await act(async () => {
        if (outcome === "secrets-error") {
          second.reject(new Error("Current B SecretStorage failure"));
        } else {
          second.resolve(undefined);
          await waitFor(() =>
            expect(manager.testConnection).toHaveBeenCalledOnce(),
          );
          if (outcome === "driver-error") {
            currentDriver.reject(new Error("Current B driver failure"));
          } else {
            currentDriver.resolve({ success: true });
          }
        }
        await requests[2];
      });
      expect(
        screen.getByText(
          outcome === "success"
            ? /Connection successful/
            : /Current B .*failure/,
        ),
      ).toBeTruthy();
      expect(screen.queryByText(/Stale A failure/)).toBeNull();
      expect(
        screen.queryByRole("button", { name: "Cancel Testing" }),
      ).toBeNull();
      expect(
        screen
          .getByRole("button", { name: "Save Changes" })
          .matches(":disabled"),
      ).toBe(false);
      expect(panel.webview.postMessage).toHaveBeenCalledOnce();
      expect(manager.saveConnection).not.toHaveBeenCalled();
    } finally {
      panel.dispose();
      await result;
    }
  });
});
