import { beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import { createWebviewShell } from "../../src/extension/panels/webviewShell";

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
  APP_WEBVIEW_SHELL_LAYOUT: {
    htmlStyles: "height: 100%; overflow: hidden;",
    bodyStyles: "height: 100%; overflow: hidden;",
    rootStyles: "height: 100vh;",
  },
  FULLSCREEN_WEBVIEW_SHELL_LAYOUT: {
    htmlStyles: "height: 100%; overflow: hidden;",
    bodyStyles: "height: 100%; overflow: hidden;",
    rootStyles: "height: 100vh; overflow: hidden;",
  },
  WEBVIEW_SCROLLBAR_STYLES: "",
  MONACO_SCROLLBAR_STYLES: "",
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

    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      1,
      expect.objectContaining({
        enableScripts: true,
        retainContextWhenHidden: false,
      }),
    );

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
      expect.objectContaining({
        password: "stored-secret",
        useSecretStorage: true,
      }),
    );
    expect(context.secrets.store).not.toHaveBeenCalled();
  });

  it("prefers Elasticsearch basic auth over previously stored API key", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => JSON.stringify({ apiKey: "stored-api-key" })),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => ({
        id: "conn-es-basic",
        name: "Elastic",
        type: "elasticsearch",
        useSecretStorage: true,
      })),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-es-basic",
        name: "Elastic",
        type: "elasticsearch",
        endpoint: "http://localhost:9200",
        useSecretStorage: true,
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
        id: "conn-es-basic",
        name: "Elastic",
        type: "elasticsearch",
        endpoint: "http://localhost:9200",
        username: "elastic",
        password: "basic-secret",
        apiKey: "",
        hasStoredApiKey: true,
        useSecretStorage: true,
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-es-basic",
        type: "elasticsearch",
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        username: "elastic",
        password: "basic-secret",
        apiKey: undefined,
      }),
    );
  });

  it("normalizes Oracle legacy database payloads to serviceName on save", async () => {
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
        id: "conn-oracle",
        name: "Oracle",
        type: "oracle",
        host: "oracle.local",
        port: 1521,
        database: "XEPDB1",
        username: "system",
        password: "pw",
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-oracle",
        type: "oracle",
        serviceName: "XEPDB1",
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "conn-oracle",
        type: "oracle",
        serviceName: "XEPDB1",
        database: undefined,
      }),
    );
  });

  it("sanitizes the initial edit state before passing it into the webview", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () =>
          JSON.stringify({
            sshPrivateKey:
              "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
            sshPassphrase: "stored-passphrase",
          }),
        ),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(() => undefined),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-edit-sanitized",
        name: "Elastic SSH",
        type: "elasticsearch",
        endpoint: "https://elastic-user:elastic-pass@cluster.example.com",
        apiKey: "inline-api-key",
        awsAccessKeyId: "AKIA123",
        awsSecretAccessKey: "secret-key",
        awsSessionToken: "session-token",
        password: "db-password",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "privateKey",
        sshPrivateKey:
          "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
        sshPassphrase: "inline-passphrase",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        useSecretStorage: true,
      },
    );

    await Promise.resolve();

    const shellCall = vi.mocked(createWebviewShell).mock.calls[0]?.[0];
    expect(shellCall?.initialState).toEqual(
      expect.objectContaining({
        view: "connection",
        existing: expect.objectContaining({
          id: "conn-edit-sanitized",
          endpoint: "https://cluster.example.com",
          hasStoredSshPrivateKey: true,
          hasStoredSshPassphrase: true,
        }),
      }),
    );
    expect(shellCall?.initialState).toEqual(
      expect.not.objectContaining({
        existing: expect.objectContaining({
          password: expect.anything(),
          apiKey: expect.anything(),
          awsAccessKeyId: expect.anything(),
          awsSecretAccessKey: expect.anything(),
          awsSessionToken: expect.anything(),
          sshPrivateKey: expect.anything(),
          sshPassphrase: expect.anything(),
        }),
      }),
    );

    const panel = createdPanel();
    panel?.dispose();
    await expect(promise).resolves.toBeUndefined();
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
      expect.objectContaining({
        awsAccessKeyId: expect.anything(),
        awsSecretAccessKey: expect.anything(),
        awsSessionToken: expect.anything(),
      }),
    );
  });

  it("returns validation details on save and does not call saveConnection for invalid payload", async () => {
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
        id: "conn-invalid-sqlite",
        name: "SQLite Missing File",
        type: "sqlite",
      },
    });

    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("filePath"),
        validation: expect.objectContaining({
          valid: false,
          missingRequired: expect.arrayContaining(["filePath"]),
        }),
      }),
    });

    panel.dispose();
    await expect(promise).resolves.toBeUndefined();
  });

  it("preserves SQLite WAL mode in edit state and save payloads", async () => {
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
      {
        id: "conn-sqlite",
        name: "Local SQLite",
        type: "sqlite",
        filePath: "/tmp/app.db",
        sqliteWalMode: "off",
      },
    );

    await Promise.resolve();

    const shellCall = vi.mocked(createWebviewShell).mock.calls.at(-1)?.[0];
    expect(shellCall?.initialState).toEqual(
      expect.objectContaining({
        view: "connection",
        existing: expect.objectContaining({
          id: "conn-sqlite",
          filePath: "/tmp/app.db",
          sqliteWalMode: "off",
        }),
      }),
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "conn-sqlite",
        name: "Local SQLite",
        type: "sqlite",
        filePath: "/tmp/app.db",
        sqliteWalMode: "off",
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-sqlite",
        type: "sqlite",
        sqliteWalMode: "off",
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "conn-sqlite",
        sqliteWalMode: "off",
      }),
    );
  });

  it("returns validation details on test and does not call manager testConnection for invalid payload", async () => {
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
        id: "conn-invalid-mysql",
        name: "MySQL Missing Host",
        type: "mysql",
        database: "app",
        username: "root",
      },
    });

    expect(connectionManager.testConnection).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "testResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("host"),
        validation: expect.objectContaining({
          valid: false,
          missingRequired: expect.arrayContaining(["host"]),
        }),
      }),
    });

    panel.dispose();
    await expect(promise).resolves.toBeUndefined();
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
        endpoint: "https://elastic-user:elastic-pass@cluster.example.com",
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
      JSON.stringify({
        apiKey: "base64-api-key",
        endpoint: "https://elastic-user:elastic-pass@cluster.example.com",
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "elasticsearch",
        endpoint: "https://elastic-user:elastic-pass@cluster.example.com",
        apiKey: "base64-api-key",
        useSecretStorage: true,
      }),
    );
  });

  it("forces SSH credentials into Secret Storage and preserves stored private key secrets", async () => {
    const previousSecretSnapshot = JSON.stringify({
      sshPrivateKey:
        "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
      sshPassphrase: "stored-passphrase",
    });
    const context = {
      secrets: {
        get: vi.fn(async () => previousSecretSnapshot),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => ({
        id: "conn-ssh",
        name: "PG over SSH",
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "privateKey",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        useSecretStorage: true,
      })),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-ssh",
        name: "PG over SSH",
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "privateKey",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        useSecretStorage: true,
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
        id: "conn-ssh",
        name: "PG over SSH",
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "privateKey",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        useSecretStorage: false,
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
        sshPrivateKey: "",
        sshPassphrase: "new-passphrase",
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-ssh",
        sshEnabled: true,
        useSecretStorage: true,
      }),
    );
    expect(context.secrets.store).toHaveBeenCalledWith(
      "conn-ssh",
      JSON.stringify({
        sshPrivateKey:
          "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
        sshPassphrase: "new-passphrase",
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "conn-ssh",
        sshEnabled: true,
        sshPrivateKey:
          "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
        sshPassphrase: "new-passphrase",
        useSecretStorage: true,
      }),
    );
  });

  it("prevents credential loss when disabling secret storage if keychain cannot return existing secret", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => {
          throw new Error("keychain unavailable");
        }),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => ({
        id: "conn-legacy",
        name: "Legacy",
        type: "pg",
        useSecretStorage: true,
      })),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-legacy",
        name: "Legacy",
        type: "pg",
        useSecretStorage: true,
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
        id: "conn-legacy",
        name: "Legacy",
        type: "pg",
        host: "db.local",
        database: "legacy",
        username: "reader",
        useSecretStorage: false,
        hasStoredSecret: true,
        password: "",
      },
    });

    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("SecretStorage unavailable"),
      }),
    });

    panel.dispose();
    await expect(promise).resolves.toBeUndefined();
  });

  it("blocks secret mutation when previous snapshot cannot be read", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => {
          throw new Error("keychain unavailable");
        }),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => ({
        id: "conn-secret-read-failed",
        name: "Secret Read Failed",
        type: "pg",
        useSecretStorage: true,
      })),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-secret-read-failed",
        name: "Secret Read Failed",
        type: "pg",
        useSecretStorage: true,
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
        id: "conn-secret-read-failed",
        name: "Secret Read Failed",
        type: "pg",
        host: "db.local",
        database: "app",
        username: "reader",
        useSecretStorage: true,
        password: "new-secret",
      },
    });

    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("SecretStorage unavailable"),
      }),
    });

    panel.dispose();
    await expect(promise).resolves.toBeUndefined();
  });

  it("rolls back secret storage snapshot when config save fails after secret update", async () => {
    const previousSecretSnapshot = JSON.stringify({ password: "old-secret" });
    const context = {
      secrets: {
        get: vi.fn(async () => previousSecretSnapshot),
        store: vi.fn(async () => undefined),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn(async () => {
        throw new Error("config write failed");
      }),
      getConnection: vi.fn(() => ({
        id: "conn-rollback",
        name: "Rollback",
        type: "pg",
        useSecretStorage: true,
      })),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-rollback",
        name: "Rollback",
        type: "pg",
        useSecretStorage: true,
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
        id: "conn-rollback",
        name: "Rollback",
        type: "pg",
        host: "db.local",
        database: "app",
        username: "reader",
        useSecretStorage: true,
        password: "new-secret",
      },
    });

    expect(connectionManager.saveConnection).toHaveBeenCalledTimes(1);
    expect(context.secrets.store).toHaveBeenNthCalledWith(
      1,
      "conn-rollback",
      JSON.stringify({ password: "new-secret" }),
    );
    expect(context.secrets.store).toHaveBeenNthCalledWith(
      2,
      "conn-rollback",
      previousSecretSnapshot,
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("SecretStorage unavailable"),
      }),
    });

    panel.dispose();
    await expect(promise).resolves.toBeUndefined();
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
