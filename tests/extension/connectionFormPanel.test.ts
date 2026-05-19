import { beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";

const vscodeMock = vi.hoisted(() => {
  const createWebviewPanel = vi.fn(() => {
    const disposeListeners = new Set<() => void>();
    const messageListeners = new Set<(message: unknown) => void>();
    const panel = {
      webview: {
        html: "",
        postMessage: vi.fn(),
        onDidReceiveMessage(listener: (message: unknown) => void) {
          messageListeners.add(listener);
          return {
            dispose: () => {
              messageListeners.delete(listener);
            },
          };
        },
        async dispatchMessage(message: unknown) {
          for (const listener of messageListeners) {
            await listener(message);
          }
        },
      },
      onDidDispose(listener: () => void) {
        disposeListeners.add(listener);
        return {
          dispose: () => {
            disposeListeners.delete(listener);
          },
        };
      },
      dispose() {
        for (const listener of disposeListeners) {
          listener();
        }
      },
    };
    return panel;
  });

  return {
    createWebviewPanel,
    module: {
      ViewColumn: { One: 1 },
      window: {
        createWebviewPanel,
      },
    },
  };
});

vi.mock("vscode", () => vscodeMock.module);
vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

type ConnectionFormPanelPrototype = {
  handleMessage(message: unknown): Promise<void>;
};

describe("ConnectionFormPanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("reuses stored secrets on save and resolves the sanitized config", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => "stored-secret"),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Warehouse",
        type: "pg",
        useSecretStorage: true,
      })),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-1",
        name: "Warehouse",
        type: "pg",
        useSecretStorage: true,
        password: "ignored",
      },
    );

    await Promise.resolve();

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "conn-1",
        name: "Warehouse",
        type: "pg",
        host: "db.local",
        database: "warehouse",
        username: "reader",
        useSecretStorage: true,
        hasStoredSecret: true,
        password: "",
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-1",
        name: "Warehouse",
        type: "pg",
        useSecretStorage: true,
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.not.objectContaining({ password: expect.anything() }),
    );
    expect(context.secrets.store).not.toHaveBeenCalled();
  });

  it("posts test results back to the webview and resolves undefined on cancel", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => undefined),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(() => undefined),
      testConnection: vi.fn(async () => ({ success: true })),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    await panel.webview.dispatchMessage({
      type: "testConnection",
      payload: {
        id: "conn-2",
        name: "Read Replica",
        type: "mysql",
        host: "db.local",
        database: "replica",
        username: "reader",
        password: "pw",
      },
    });

    expect(connectionManager.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({ password: "pw" }),
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "testResult",
      payload: { success: true },
    });

    await panel.webview.dispatchMessage({ type: "cancel" });

    await expect(promise).resolves.toBeUndefined();
  });

  it("stores DynamoDB credentials in Secret Storage and saves a sanitized config", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => undefined),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => undefined),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );

    await Promise.resolve();

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "conn-ddb",
        name: "Dynamo Local",
        type: "dynamodb",
        awsRegion: "us-east-1",
        awsAccessKeyId: "AKIA123",
        awsSecretAccessKey: "secret-key",
        awsSessionToken: "session-token",
        endpoint: "http://localhost:8000",
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-ddb",
        type: "dynamodb",
        awsRegion: "us-east-1",
      }),
    );
    expect(context.secrets.store).toHaveBeenCalledWith(
      "conn-ddb",
      JSON.stringify({
        awsAccessKeyId: "AKIA123",
        awsSecretAccessKey: "secret-key",
        awsSessionToken: "session-token",
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.not.objectContaining({
        awsAccessKeyId: expect.anything(),
        awsSecretAccessKey: expect.anything(),
        awsSessionToken: expect.anything(),
      }),
    );
  });

  it("forces Elasticsearch credentials into Secret Storage and saves a sanitized config", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => undefined),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => undefined),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );

    await Promise.resolve();

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "conn-es",
        name: "Elastic Cloud",
        type: "elasticsearch",
        endpoint: "https://cluster.example.com",
        apiKey: "base64-api-key",
        cloudId: "deployment:ZXM=",
        useSecretStorage: false,
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-es",
        type: "elasticsearch",
        endpoint: "https://cluster.example.com",
        cloudId: "deployment:ZXM=",
        useSecretStorage: true,
      }),
    );
    expect(context.secrets.store).toHaveBeenCalledWith(
      "conn-es",
      JSON.stringify({ apiKey: "base64-api-key" }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "elasticsearch",
        useSecretStorage: true,
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.not.objectContaining({ apiKey: expect.anything() }),
    );
  });

  it("falls back to a testResult message when unhandled errors occur for malformed input", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => undefined),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(() => undefined),
      testConnection: vi.fn(),
    };
    const consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);

    const handleMessageSpy = vi
      .spyOn(
        ConnectionFormPanel.prototype as unknown as ConnectionFormPanelPrototype,
        "handleMessage",
      )
      .mockRejectedValue(new Error("boom"));

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    try {
      await expect(
        panel.webview.dispatchMessage(null),
      ).resolves.toBeUndefined();

      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "testResult",
        payload: { success: false, error: "boom" },
      });

      panel.dispose();
      await expect(promise).resolves.toBeUndefined();
    } finally {
      handleMessageSpy.mockRestore();
      consoleErrorSpy.mockRestore();
    }
  });
});
