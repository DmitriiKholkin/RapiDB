import { beforeEach, describe, expect, it, vi } from "vitest";

const vscodeMocks = vi.hoisted(() => ({
  createWebviewPanel: vi.fn(),
}));

vi.mock("vscode", () => ({
  ViewColumn: { One: 1 },
  window: {
    createWebviewPanel: vscodeMocks.createWebviewPanel,
  },
}));

vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";

describe("ConnectionFormPanel", () => {
  beforeEach(() => {
    vscodeMocks.createWebviewPanel.mockReset();
  });

  it("does not rehydrate a stored secret into plaintext settings when secret storage is disabled", async () => {
    let onMessage:
      | ((message: { type: string; payload?: unknown }) => Promise<void>)
      | undefined;
    const postMessage = vi.fn();
    const dispose = vi.fn();

    vscodeMocks.createWebviewPanel.mockReturnValue({
      webview: {
        html: "",
        postMessage,
        onDidReceiveMessage: vi.fn((handler) => {
          onMessage = handler;
          return { dispose: vi.fn() };
        }),
      },
      onDidDispose: vi.fn(() => ({ dispose: vi.fn() })),
      dispose,
    });

    const context = {
      secrets: {
        get: vi.fn().mockResolvedValue("super-secret"),
        delete: vi.fn().mockResolvedValue(undefined),
        store: vi.fn().mockResolvedValue(undefined),
      },
      extensionUri: { path: "/extension" },
    };

    const connectionManager = {
      getConnection: vi.fn().mockReturnValue({
        id: "conn-1",
        name: "Analytics",
        type: "pg",
        useSecretStorage: true,
      }),
      saveConnection: vi.fn().mockResolvedValue(undefined),
      testConnection: vi.fn(),
    };

    const pending = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-1",
        name: "Analytics",
        type: "pg",
        host: "localhost",
        port: 5432,
        useSecretStorage: true,
      } as never,
    );

    await Promise.resolve();
    expect(onMessage).toBeTypeOf("function");

    await onMessage?.({
      type: "saveConnection",
      payload: {
        id: "conn-1",
        name: "Analytics",
        type: "pg",
        host: "localhost",
        port: 5432,
        username: "reader",
        password: "",
        useSecretStorage: false,
        hasStoredSecret: true,
      },
    });

    await expect(pending).resolves.toEqual(
      expect.objectContaining({
        id: "conn-1",
        useSecretStorage: false,
        password: "",
      }),
    );
    expect(context.secrets.get).toHaveBeenCalledTimes(1);
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "conn-1",
        useSecretStorage: false,
        password: "",
      }),
    );
    expect(context.secrets.delete).toHaveBeenCalledWith("conn-1");
    expect(dispose).toHaveBeenCalledTimes(1);
    expect(postMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ type: "saveResult" }),
    );
  });
});
