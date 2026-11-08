import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  type ConnectionSecretClearOptions,
  serializeConnectionSecretsForStoredConfig,
} from "../../src/extension/connectionSecrets";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import { createWebviewShell } from "../../src/extension/panels/webviewShell";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

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
        showWarningMessage: vi.fn(async () => "Save in Plaintext"),
        showErrorMessage: vi.fn(async () => undefined),
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

const noSecretClears: ConnectionSecretClearOptions = {
  password: false,
  apiKey: false,
  credentialBearingUris: false,
  awsCredentials: false,
};

type ConnectionFormPanelPrototype = {
  handleMessage(message: unknown): Promise<void>;
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((complete, fail) => {
    resolve = complete;
    reject = fail;
  });
  return { promise, resolve, reject };
}

describe("ConnectionFormPanel", () => {
  it.each([
    "cancel",
    "replace",
    "dispose",
  ])("ignores a delayed test preparation rejection after %s", async (action) => {
    const first = deferred<string | undefined>();
    const second = deferred<string | undefined>();
    const existing: ConnectionConfig = {
      id: "delayed-test",
      name: "Delayed",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "reader",
    };
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
      testConnection: vi.fn(async () => ({ success: true })),
      saveConnection: vi.fn(),
    };
    const result = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const panel = createdPanel();
    if (!panel) throw new Error("Expected form");
    const message = { type: "testConnection", payload: existing };
    const testingFirst = panel.webview.dispatchMessage(message);
    if (action === "dispose") {
      panel.dispose();
    } else if (action === "cancel") {
      await panel.webview.dispatchMessage({ type: "cancelTestConnection" });
    }
    const testingSecond =
      action === "dispose" ? undefined : panel.webview.dispatchMessage(message);
    first.reject(new Error("Stale SecretStorage failure"));
    await testingFirst;
    expect(panel.webview.postMessage).not.toHaveBeenCalled();
    if (testingSecond) {
      second.reject(new Error("Current SecretStorage failure"));
      await testingSecond;
      expect(panel.webview.postMessage).toHaveBeenCalledExactlyOnceWith({
        type: "testResult",
        payload: { success: false, error: "Current SecretStorage failure" },
      });
      panel.dispose();
    }
    expect(manager.testConnection).not.toHaveBeenCalled();
    expect(manager.saveConnection).not.toHaveBeenCalled();
    await result;
  });

  it.each([
    "secrets",
    "consent",
    "write",
  ])("ignores Cancel and duplicate Save during delayed %s", async (stage) => {
    const reached = deferred<void>();
    const release = deferred<void>();
    const pause = async () => {
      reached.resolve();
      await release.promise;
    };
    const existing: ConnectionConfig = {
      id: "delayed-save",
      name: "Delayed",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "reader",
    };
    const context = {
      secrets: {
        get: vi
          .fn()
          .mockResolvedValueOnce(undefined)
          .mockImplementation(async () => {
            if (stage === "secrets") await pause();
            return undefined;
          }),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      saveConnection: vi.fn(async (config: ConnectionConfig) => {
        if (stage === "write") await pause();
        return config;
      }),
    };
    if (stage === "consent") {
      vscodeMock.module.window.showWarningMessage.mockImplementationOnce(
        async () => {
          await pause();
          return "Save in Plaintext";
        },
      );
    }
    const result = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    const settled = vi.fn();
    void result.then(settled);
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const panel = createdPanel();
    if (!panel) throw new Error("Expected form");
    const dispose = vi.spyOn(panel, "dispose");
    const message = {
      type: "saveConnection",
      payload: {
        ...existing,
        ...(stage === "consent"
          ? { password: "secret", useSecretStorage: false }
          : {}),
      },
    };
    const saving = panel.webview.dispatchMessage(message);
    await reached.promise;
    await panel.webview.dispatchMessage({ type: "cancel" });
    const duplicate = panel.webview.dispatchMessage(message);
    expect(dispose).not.toHaveBeenCalled();
    expect(settled).not.toHaveBeenCalled();
    release.resolve();
    await Promise.all([saving, duplicate]);
    expect(await result).toMatchObject({ id: "delayed-save" });
    expect(manager.saveConnection).toHaveBeenCalledOnce();
    expect(dispose).toHaveBeenCalledOnce();
  });

  it.each([
    "secrets",
    "write",
    "write-error",
  ])("handles external disposal during delayed %s", async (stage) => {
    const reached = deferred<void>();
    const release = deferred<void>();
    const existing: ConnectionConfig = {
      id: "disposed-save",
      name: "Disposed",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "reader",
    };
    const context = {
      secrets: {
        get: vi
          .fn()
          .mockResolvedValueOnce(undefined)
          .mockImplementation(async () => {
            if (stage === "secrets") {
              reached.resolve();
              await release.promise;
            }
            return undefined;
          }),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      saveConnection: vi.fn(async (config: ConnectionConfig) => {
        if (stage !== "secrets") {
          reached.resolve();
          await release.promise;
        }
        if (stage === "write-error") throw new Error("Delayed write failed");
        return config;
      }),
    };
    const result = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const panel = createdPanel();
    if (!panel) throw new Error("Expected form");
    const dispose = vi.spyOn(panel, "dispose");
    const saving = panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: existing,
    });
    await reached.promise;
    panel.dispose();
    await expect(result).resolves.toBeUndefined();
    release.resolve();
    await saving;
    expect(manager.saveConnection).toHaveBeenCalledTimes(
      stage === "secrets" ? 0 : 1,
    );
    expect(panel.webview.postMessage).not.toHaveBeenCalled();
    expect(dispose).toHaveBeenCalledOnce();
  });

  it("allows Cancel after a delayed Save fails", async () => {
    const reached = deferred<void>();
    const release = deferred<void>();
    const context = { secrets: { get: vi.fn() } };
    const manager = {
      getConnection: vi.fn(),
      saveConnection: vi.fn(async () => {
        reached.resolve();
        await release.promise;
        throw new Error("Delayed failure");
      }),
    };
    const result = ConnectionFormPanel.show(context as never, manager as never);
    const panel = createdPanel();
    if (!panel) throw new Error("Expected form");
    const saving = panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "failed-save",
        name: "Failed",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "reader",
      },
    });
    await reached.promise;
    release.resolve();
    await saving;
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: {
        success: false,
        error: "Could not save connection: Delayed failure",
      },
    });
    await panel.webview.dispatchMessage({ type: "cancel" });
    await expect(result).resolves.toBeUndefined();
  });

  it.each([
    false,
    true,
  ])("clears stored SSH/TLS passphrases consistently for Test and Save (replace=%s)", async (replace) => {
    const existing: ConnectionConfig = {
      id: "passphrases",
      name: "Keys",
      type: "pg",
      host: "db.local",
      database: "app",
      username: "reader",
      useSecretStorage: true,
      ssh: {
        host: "bastion",
        username: "qa",
        authMethod: "privateKey",
        hostVerificationMode: "trustOnFirstUse",
      },
      tls: {
        mode: "mutualTls",
        certFilePath: "/tmp/client.crt",
        keyFilePath: "/tmp/client.key",
      },
    };
    const secrets = JSON.stringify({
      sshPrivateKey: "private-key",
      sshPassphrase: "old-ssh",
      tlsKeyPassphrase: "old-tls",
    });
    const context = { secrets: { get: vi.fn(async () => secrets) } };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(async (_config: ConnectionConfig) => ({
        success: true,
      })),
      saveConnection: vi.fn(
        async (
          _config: ConnectionConfig,
          _clear: ConnectionSecretClearOptions,
        ) => undefined,
      ),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const panel = createdPanel();
    if (!panel) throw new Error("Expected form");
    const payload = {
      ...existing,
      clearSshPassphrase: true,
      clearTlsKeyPassphrase: true,
      ssh: { ...existing.ssh, passphrase: replace ? "new-ssh" : "" },
      tls: { ...existing.tls, keyPassphrase: replace ? "new-tls" : "" },
    };
    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    const tested = manager.testConnection.mock.calls[0]?.[0];
    expect(tested?.ssh?.passphrase).toBe(replace ? "new-ssh" : undefined);
    expect(tested?.tls?.keyPassphrase).toBe(replace ? "new-tls" : undefined);
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    expect(manager.saveConnection.mock.calls[0]?.[0]).toEqual(tested);
    const clear = manager.saveConnection.mock.calls[0]?.[1];
    expect(clear).toMatchObject({
      sshPassphrase: true,
      tlsKeyPassphrase: true,
    });
    const persisted = JSON.parse(
      serializeConnectionSecretsForStoredConfig(
        tested as ConnectionConfig,
        secrets,
        clear,
      ) ?? "{}",
    );
    expect(persisted.sshPassphrase).toBe(replace ? "new-ssh" : undefined);
    expect(persisted.tlsKeyPassphrase).toBe(replace ? "new-tls" : undefined);
    await pending;
  });
  beforeEach(() => {
    vi.clearAllMocks();
    vscodeMock.module.window.showWarningMessage.mockResolvedValue(
      "Save in Plaintext",
    );
  });

  it.each([
    "pg",
    "sqlite",
  ])("saves nonsecret %s opt-outs without a plaintext consent prompt", async (type) => {
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(),
      testConnection: vi.fn(),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected a webview panel to be created.");
    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "nonsecret",
        name: "Nonsecret",
        type,
        host: "localhost",
        database: "app",
        username: "user",
        filePath: "/app.db",
        password: "",
        useSecretStorage: false,
      },
    });
    expect((await promise)?.useSecretStorage).toBe(false);
    expect(vscodeMock.module.window.showWarningMessage).not.toHaveBeenCalled();
    expect(connectionManager.saveConnection).toHaveBeenCalledOnce();
  });

  it.each([
    "password",
    "connectionUri",
    "tls",
  ])("requires explicit consent for plaintext %s and leaves persistence untouched on dismissal", async (field) => {
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(),
      testConnection: vi.fn(async () => ({ success: true })),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected a webview panel to be created.");
    const payload = {
      id: "consent",
      name: "Consent",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "user",
      useSecretStorage: false,
      ...(field === "password"
        ? { password: " secret " }
        : field === "connectionUri"
          ? { connectionUri: "postgres://user:uri-secret@localhost/app" }
          : {
              tls: {
                mode: "mutualTls",
                certFilePath: "/cert",
                keyFilePath: "/key",
                keyPassphrase: "tls-secret",
              },
            }),
    };
    const warning = vscodeMock.module.window.showWarningMessage;
    warning.mockResolvedValueOnce(undefined as never);
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    expect(warning).toHaveBeenCalledWith(
      expect.stringContaining("settings.json"),
      { modal: true },
      "Save in Plaintext",
    );
    expect(JSON.stringify(warning.mock.calls)).not.toContain("uri-secret");
    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(context.secrets.delete).not.toHaveBeenCalled();
    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    expect(connectionManager.testConnection).toHaveBeenCalledWith(
      expect.objectContaining(payload),
      expect.any(AbortSignal),
    );
    expect(warning).toHaveBeenCalledOnce();
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    const response = await promise;
    expect(JSON.stringify(response)).not.toContain("secret");
    expect(response?.useSecretStorage).toBe(false);
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining(payload),
      noSecretClears,
      { kind: "create" },
    );
  });

  it("does not save if the form closes while plaintext consent is pending", async () => {
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(),
      testConnection: vi.fn(),
    };
    let accept!: (value: string) => void;
    vscodeMock.module.window.showWarningMessage.mockImplementationOnce(
      () =>
        new Promise<string>((resolve) => {
          accept = resolve;
        }),
    );
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected a webview panel to be created.");
    const save = panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "closed-consent",
        name: "Closed consent",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "user",
        password: "secret",
        useSecretStorage: false,
      },
    });
    await vi.waitFor(() =>
      expect(
        vscodeMock.module.window.showWarningMessage,
      ).toHaveBeenCalledOnce(),
    );
    panel.dispose();
    accept("Save in Plaintext");
    await save;
    await expect(promise).resolves.toBeUndefined();
    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(context.secrets.delete).not.toHaveBeenCalled();
  });

  it.each([
    "apiKey",
    "aws",
    "tls",
    "connectionUri",
  ])("preserves stored %s credentials when explicitly disabling storage", async (field) => {
    const secrets = {
      apiKey: "api-secret",
      awsAccessKeyId: "access-secret",
      awsSecretAccessKey: "aws-secret",
      awsSessionToken: "token-secret",
      tlsKeyPassphrase: "tls-secret",
      connectionUri: "mongodb://user:uri-secret@localhost/app",
    };
    const context = {
      secrets: {
        get: vi.fn(async () => JSON.stringify(secrets)),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const existing = {
      id: "switch",
      name: "Switch",
      type:
        field === "apiKey"
          ? "elasticsearch"
          : field === "aws"
            ? "dynamodb"
            : field === "connectionUri"
              ? "mongodb"
              : "pg",
      host: "localhost",
      database: "app",
      username: "user",
      useSecretStorage: true,
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      existing as never,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const payload = {
      ...existing,
      useSecretStorage: false,
      ...(field === "apiKey"
        ? { endpoint: "https://localhost", apiKey: "" }
        : field === "aws"
          ? { awsRegion: "us-east-1" }
          : field === "tls"
            ? {
                tls: {
                  mode: "mutualTls",
                  certFilePath: "/cert",
                  keyFilePath: "/key",
                  keyPassphrase: "",
                },
              }
            : { connectionUri: "mongodb://localhost/app" }),
    };
    const panel = createdPanel();
    if (!panel) throw new Error("Expected a webview panel to be created.");
    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload,
    });
    const response = await promise;
    const saved = connectionManager.saveConnection.mock.calls[0]?.[0];
    expect(saved?.useSecretStorage).toBe(false);
    if (field === "apiKey") expect(saved?.apiKey).toBe(secrets.apiKey);
    if (field === "aws")
      expect(saved).toMatchObject({
        awsAccessKeyId: secrets.awsAccessKeyId,
        awsSecretAccessKey: secrets.awsSecretAccessKey,
        awsSessionToken: secrets.awsSessionToken,
      });
    if (field === "tls")
      expect(saved?.tls?.keyPassphrase).toBe(secrets.tlsKeyPassphrase);
    if (field === "connectionUri")
      expect(saved?.connectionUri).toBe(secrets.connectionUri);
    expect(JSON.stringify(response)).not.toContain("secret");
    expect(response?.useSecretStorage).toBe(false);
    expect(vscodeMock.module.window.showWarningMessage).toHaveBeenCalledOnce();
  });

  it("keeps plaintext edit metadata secret-free and blank submissions retain the user's password", async () => {
    const existing = {
      id: "plain-edit",
      name: "Plain edit",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "user",
      password: " original-secret ",
      useSecretStorage: false,
    };
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      existing as never,
    );
    await Promise.resolve();
    const initial =
      vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    expect(JSON.stringify(initial)).not.toContain("original-secret");
    expect(initial).toMatchObject({
      existing: { hasStoredSecret: true, useSecretStorage: false },
    });
    const panel = createdPanel();
    if (!panel) throw new Error("Expected a webview panel to be created.");
    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...existing, password: "", hasStoredSecret: true },
    });
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        password: existing.password,
        useSecretStorage: false,
      }),
      noSecretClears,
      expect.objectContaining({ kind: "edit", id: existing.id }),
    );
    expect((await promise)?.password).toBeUndefined();
  });

  it.each([
    "saveConnection",
    "testConnection",
  ])("blocks %s with malformed TLS before accessing secrets or the manager", async (type) => {
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => undefined),
      testConnection: vi.fn(async () => ({ success: true })),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected the connection form panel to open.");
    const payload = {
      id: "tls-regression",
      name: "TLS",
      type: "pg",
      host: "db.local",
      database: "app",
      username: "reader",
    };
    for (const tls of [
      null,
      [],
      {},
      "requireVerifyFull",
      { mode: "unknown", caFilePath: "/certs/ca.pem" },
      { mode: "requireVerifyFull ", caFilePath: "/certs/ca.pem" },
      { mode: 1 },
      { mode: "requireVerifyFull", caFilePath: 42 },
      { mode: "mutualTls", certFilePath: {}, keyFilePath: "/certs/key.pem" },
    ]) {
      await panel.webview.dispatchMessage({
        type,
        payload: { ...payload, tls },
      });
    }
    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(connectionManager.testConnection).not.toHaveBeenCalled();
    expect(connectionManager.getConnection).not.toHaveBeenCalled();
    expect(context.secrets.get).not.toHaveBeenCalled();
    expect(context.secrets.store).not.toHaveBeenCalled();

    const tls = { mode: "requireVerifyFull", caFilePath: "/certs/ca.pem" };
    await panel.webview.dispatchMessage({ type, payload: { ...payload, tls } });
    const action =
      type === "saveConnection"
        ? connectionManager.saveConnection
        : connectionManager.testConnection;
    if (type === "testConnection") {
      expect(action).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ tls }),
        expect.any(AbortSignal),
      );
    } else {
      expect(action).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ tls }),
        noSecretClears,
        { kind: "create" },
      );
    }
    panel.dispose();
    await promise;
  });

  it("reuses stored TLS key passphrases and resolves a sanitized config", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () =>
          JSON.stringify({ tlsKeyPassphrase: "stored-tls-passphrase" }),
        ),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => ({
        id: "conn-tls",
        name: "Warehouse TLS",
        type: "pg",
        useSecretStorage: true,
        tls: {
          mode: "mutualTls",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
        },
      })),
      testConnection: vi.fn(),
    };

    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      {
        id: "conn-tls",
        name: "Warehouse TLS",
        type: "pg",
        useSecretStorage: true,
        tls: {
          mode: "mutualTls",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
        },
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
        id: "conn-tls",
        name: "Warehouse TLS",
        type: "pg",
        host: "db.local",
        database: "warehouse",
        username: "reader",
        useSecretStorage: true,
        hasStoredTlsKeyPassphrase: true,
        tls: {
          mode: "mutualTls",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
          keyPassphrase: "",
        },
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-tls",
        tls: expect.objectContaining({
          mode: "mutualTls",
          keyPassphrase: undefined,
        }),
      }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        tls: expect.objectContaining({
          keyPassphrase: "stored-tls-passphrase",
        }),
      }),
      noSecretClears,
      expect.objectContaining({ kind: "edit", id: "conn-tls" }),
    );
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
        retainContextWhenHidden: true,
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
      noSecretClears,
      expect.objectContaining({ kind: "edit", id: "conn-1" }),
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
      noSecretClears,
      expect.objectContaining({ kind: "edit", id: "conn-es-basic" }),
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
      noSecretClears,
      { kind: "create" },
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
        endpoint: "https://elastic-user:elastic-p@ss@cluster.example.com",
        apiKey: "inline-api-key",
        awsAccessKeyId: "AKIA123",
        awsSecretAccessKey: "secret-key",
        awsSessionToken: "session-token",
        password: "db-password",
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "privateKey",
          privateKey:
            "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
          passphrase: "inline-passphrase",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
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
          ssh: expect.objectContaining({
            privateKey: expect.anything(),
            passphrase: expect.anything(),
            password: expect.anything(),
          }),
        }),
      }),
    );

    const panel = createdPanel();
    panel?.dispose();
    await expect(promise).resolves.toBeUndefined();
  });

  it.each([
    "/",
    "?",
    "#",
    " ",
  ])("rejects ambiguous URI credentials with %j in form saves", async (delimiter) => {
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
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected the connection form panel to open.");
    const connectionUri = `redis://user:pa${delimiter}ss@host:6379/0`;
    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { id: "ambiguous", name: "Redis", type: "redis", connectionUri },
    });
    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("Ambiguous URI credentials"),
      }),
    });
    expect(JSON.stringify(panel.webview.postMessage.mock.calls)).not.toContain(
      "ss@host",
    );
    panel.dispose();
    await expect(promise).resolves.toBeUndefined();
  });

  it("correlates test results and does not report unrelated failures as test results", async () => {
    const context = {
      secrets: {
        get: vi.fn(async () => undefined),
        store: vi.fn(),
        delete: vi.fn(),
      },
      globalState: {
        get: vi.fn(() => {
          throw new Error("state unavailable");
        }),
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
        id: "conn-3",
        name: "Primary",
        type: "mysql",
        host: "db.local",
        database: "app",
        username: "reader",
        password: "pw",
        requestId: "request-1",
      },
    });
    expect(connectionManager.testConnection).toHaveBeenCalledWith(
      expect.not.objectContaining({ requestId: expect.anything() }),
      expect.any(AbortSignal),
    );
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "testResult",
      payload: { success: true, requestId: "request-1" },
    });

    await panel.webview.dispatchMessage({
      type: "testConnection",
      payload: { id: "conn-3", type: "mysql", requestId: "request-2" },
    });
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "testResult",
      payload: {
        requestId: "request-2",
        success: false,
        error: "Connection settings are invalid.",
      },
    });
    expect(connectionManager.testConnection).toHaveBeenCalledOnce();

    panel.webview.postMessage.mockClear();
    await panel.webview.dispatchMessage({
      type: "browseFile",
      payload: { target: "filePath" },
    });
    expect(panel.webview.postMessage).not.toHaveBeenCalled();
    expect(vscodeMock.module.window.showErrorMessage).toHaveBeenCalledWith(
      "[RapiDB] Unexpected error: state unavailable",
    );

    panel.dispose();
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
      expect.any(AbortSignal),
    );
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "testResult",
      payload: { success: true },
    });

    await panel.webview.dispatchMessage({ type: "cancel" });

    await expect(promise).resolves.toBeUndefined();
  });

  it.each([
    "cancel",
    "dispose",
  ] as const)("aborts an active connection test on %s", async (action) => {
    let resolveTest!: (result: { success: boolean }) => void;
    let receivedSignal: AbortSignal | undefined;
    const connectionManager = {
      saveConnection: vi.fn(),
      getConnection: vi.fn(() => undefined),
      testConnection: vi.fn(async (_config: unknown, signal: AbortSignal) => {
        receivedSignal = signal;
        return await new Promise<{ success: boolean }>((resolve) => {
          resolveTest = resolve;
        });
      }),
    };
    const promise = ConnectionFormPanel.show(
      {
        secrets: {
          get: vi.fn(async () => undefined),
          store: vi.fn(),
          delete: vi.fn(),
        },
      } as never,
      connectionManager as never,
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected a webview panel to be created.");

    const pendingMessage = panel.webview.dispatchMessage({
      type: "testConnection",
      payload: {
        id: "pending-test",
        name: "Pending",
        type: "mysql",
        host: "db.local",
        database: "app",
        username: "user",
        password: "pw",
      },
    });
    await vi.waitFor(() => expect(receivedSignal).toBeDefined());

    if (action === "cancel") {
      await panel.webview.dispatchMessage({ type: "cancelTestConnection" });
    } else {
      panel.dispose();
    }

    expect(receivedSignal?.aborted).toBe(true);
    resolveTest({ success: true });
    await pendingMessage;
    expect(panel.webview.postMessage).not.toHaveBeenCalledWith({
      type: "testResult",
      payload: { success: true },
    });

    if (action === "cancel") panel.dispose();
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
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        awsAccessKeyId: expect.anything(),
        awsSecretAccessKey: expect.anything(),
        awsSessionToken: expect.anything(),
      }),
      { ...noSecretClears, awsCredentials: true },
      { kind: "create" },
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

  it("drops legacy SQLite WAL mode from save payloads", async () => {
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

    const saved = await promise;
    expect(saved).toEqual(
      expect.objectContaining({
        id: "conn-sqlite",
        type: "sqlite",
        filePath: "/tmp/app.db",
      }),
    );
    expect(saved).not.toHaveProperty("sqliteWalMode");
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({ id: "conn-sqlite", filePath: "/tmp/app.db" }),
      noSecretClears,
      expect.objectContaining({ kind: "edit", id: "conn-sqlite" }),
    );
    expect(
      vi.mocked(connectionManager.saveConnection).mock.calls.at(-1)?.[0],
    ).not.toHaveProperty("sqliteWalMode");
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

  it("respects useSecretStorage: false for Elasticsearch and stores credentials as plaintext", async () => {
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
        useSecretStorage: false,
      }),
    );
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "elasticsearch",
        endpoint: "https://elastic-user:elastic-pass@cluster.example.com",
        apiKey: "base64-api-key",
        useSecretStorage: false,
      }),
      noSecretClears,
      { kind: "create" },
    );
  });

  it("resolves SSH secrets from keychain into plaintext config when useSecretStorage is disabled", async () => {
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
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "privateKey",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
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
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "privateKey",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
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
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "privateKey",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
          privateKey: "",
          passphrase: "new-passphrase",
        },
        useSecretStorage: false,
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({
        id: "conn-ssh",
        ssh: expect.objectContaining({
          host: "bastion.example.com",
        }),
        useSecretStorage: false,
      }),
    );
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(context.secrets.delete).not.toHaveBeenCalled();
    expect((await promise)?.ssh?.privateKey).toBeUndefined();
    expect((await promise)?.ssh?.passphrase).toBeUndefined();
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "conn-ssh",
        ssh: expect.objectContaining({
          privateKey:
            "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
          passphrase: "new-passphrase",
        }),
        useSecretStorage: false,
      }),
      noSecretClears,
      expect.objectContaining({ kind: "edit", id: "conn-ssh" }),
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

  it("delegates rollback to the connection manager when saving fails", async () => {
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
    expect(context.secrets.store).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("Could not save connection"),
      }),
    });

    panel.dispose();
    await expect(promise).resolves.toBeUndefined();
  });

  it("falls back to a correlated testResult only for unhandled test connection errors", async () => {
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
      expect(panel.webview.postMessage).not.toHaveBeenCalled();
      expect(vscodeMock.module.window.showErrorMessage).toHaveBeenCalledWith(
        "[RapiDB] Unexpected error: boom",
      );

      await panel.webview.dispatchMessage({
        type: "testConnection",
        payload: {
          id: "conn-1",
          name: "Primary",
          type: "pg",
          requestId: "request-1",
        },
      });
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "testResult",
        payload: { requestId: "request-1", success: false, error: "boom" },
      });

      panel.dispose();
      await expect(promise).resolves.toBeUndefined();
    } finally {
      handleMessageSpy.mockRestore();
      consoleErrorSpy.mockRestore();
    }
  });
});

describe("MongoDB form credential lifecycle", () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([
    {
      method: "uri",
      keep: true,
      expected: "mongodb://u:p%40ss@localhost/edited?authSource=admin",
    },
    {
      method: "uri",
      keep: false,
      expected: "mongodb://localhost/edited?authSource=admin",
    },
    { method: "fields", keep: false, expected: undefined },
  ])("uses the same resolved settings for Test and Save: $method / keep=$keep", async ({
    method,
    keep,
    expected,
  }) => {
    const existing = {
      id: "mongo-lifecycle",
      name: "Mongo lifecycle",
      type: "mongodb",
      mongoConnectionMode: "uri",
      connectionUri: "mongodb://localhost/app?authSource=admin",
      useSecretStorage: true,
    };
    const context = {
      secrets: {
        get: vi.fn(async () =>
          JSON.stringify({
            connectionUri: "mongodb://u:p%40ss@localhost/app?authSource=admin",
            password: "stored-fields-password",
          }),
        ),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(async (_config: Record<string, unknown>) => ({
        success: true,
      })),
      saveConnection: vi.fn(
        async (_config: Record<string, unknown>) => undefined,
      ),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing as never,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    const payload = {
      ...existing,
      mongoConnectionMode: method,
      connectionUri: "mongodb://localhost/edited?authSource=admin",
      keepMongoUriCredentials: keep,
      host: "localhost",
      database: "app",
      username: "field-user",
      password: "",
      mongoAuthMethod: "password",
      authSource: "admin",
    };
    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    const tested = manager.testConnection.mock.calls[0]?.[0];
    expect(tested?.connectionUri).toBe(expected);
    if (method === "fields")
      expect(tested?.password).toBe("stored-fields-password");
    else {
      expect(tested?.database).toBe("app");
      expect(tested?.authSource).toBeUndefined();
      expect(tested?.password).toBeUndefined();
      expect(tested?.mongoUriHasCredentials).toBe(keep);
    }
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    expect(manager.saveConnection.mock.calls[0]?.[0]).toEqual(tested);
    await pending;
  });
});

describe("MongoDB legacy URI migration", () => {
  beforeEach(() => vi.clearAllMocks());

  it("moves legacy overrides into the URI and retains its credentials on save", async () => {
    const existing = {
      id: "mongo-legacy",
      name: "Legacy",
      type: "mongodb",
      connectionUri: "mongodb://localhost/original?retryWrites=false",
      database: "app",
      authSource: "admin",
      useSecretStorage: true,
    };
    const context = {
      secrets: {
        get: vi.fn(async () =>
          JSON.stringify({
            connectionUri:
              "mongodb://u:secret@localhost/original?retryWrites=false",
          }),
        ),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      saveConnection: vi.fn(
        async (_config: Record<string, unknown>) => undefined,
      ),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing as never,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const state = vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    if (state?.view !== "connection" || !state.existing)
      throw new Error("Expected MongoDB edit state");
    expect(state.existing.connectionUri).toBe(
      "mongodb://localhost/original?retryWrites=false&authSource=admin",
    );
    expect(state.existing.hasStoredMongoUriCredentials).toBe(true);
    await createdPanel().webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        ...state.existing,
        mongoConnectionMode: "uri",
        keepMongoUriCredentials: true,
      },
    });
    expect(manager.saveConnection.mock.calls[0]?.[0]).toMatchObject({
      connectionUri:
        "mongodb://u:secret@localhost/original?retryWrites=false&authSource=admin",
      mongoUriHasCredentials: true,
    });
    await pending;
  });
});

describe("Redis and Elasticsearch URI credential editing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vscodeMock.module.window.showWarningMessage.mockResolvedValue(
      "Save in Plaintext",
    );
  });

  it.each([
    {
      database: "4",
      connectionUri: "redis://cache.local:6380/1",
      port: undefined,
      expectedUri: "redis://cache.local:6380/4",
    },
    {
      database: "db4",
      connectionUri: "redis://cache.local:6380/1",
      port: undefined,
      expectedUri: "redis://cache.local:6380/4",
    },
    {
      database: "0",
      connectionUri: "redis://cache.local",
      port: 6380,
      expectedUri: "redis://cache.local:6380/0",
    },
    {
      database: "4",
      connectionUri: "redis://cache.local:6379/1",
      port: 6380,
      expectedUri: "redis://cache.local:6379/4",
    },
  ])("migrates legacy Redis database and fallback port into the URI ($connectionUri, $port, $database)", async ({
    database,
    connectionUri,
    port,
    expectedUri,
  }) => {
    const existing: ConnectionConfig = {
      id: "redis-working-db",
      name: "Redis override",
      type: "redis",
      connectionUri,
      port,
      database,
    };
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(async (_config: ConnectionConfig) => ({
        success: true,
      })),
      saveConnection: vi.fn(async (_config: ConnectionConfig) => undefined),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await Promise.resolve();
    const state = vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    if (state?.view !== "connection" || !state.existing)
      throw new Error("Expected edit form");
    expect(state.existing.connectionUri).toBe(expectedUri);
    const payload = { ...state.existing, database: undefined, port: undefined };
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    const tested = manager.testConnection.mock.calls[0]?.[0];
    expect(tested?.connectionUri).toBe(expectedUri);
    expect(tested?.database).toBeUndefined();
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    expect(manager.saveConnection.mock.calls[0]?.[0]).toEqual(tested);
    await pending;
  });

  it("requires re-entering or removing an omitted ambiguous stored endpoint before Save or Test", async () => {
    const existing: ConnectionConfig = {
      id: "dynamo-ambiguous",
      name: "Dynamo local",
      type: "dynamodb",
      awsRegion: "us-east-1",
      awsEndpoint: "http://u:p/w@localhost:8000",
    };
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
      saveConnection: vi.fn(async (_config: ConnectionConfig) => undefined),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await Promise.resolve();
    const state = vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    if (state?.view !== "connection" || !state.existing)
      throw new Error("Expected edit form");
    expect(state.existing.hasAmbiguousStoredUri).toBe(true);
    expect(state.existing.awsEndpoint).toBeUndefined();
    expect(JSON.stringify(state)).not.toContain("p/w");
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    const payload = { ...state.existing, name: "Renamed" };

    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });

    expect(manager.testConnection).not.toHaveBeenCalled();
    expect(manager.saveConnection).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: {
        success: false,
        error: expect.stringContaining("ambiguous credentials"),
      },
    });

    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...payload, clearAmbiguousStoredUri: true },
    });

    const saved = manager.saveConnection.mock.calls[0]?.[0];
    expect(saved).toMatchObject({ id: existing.id, name: "Renamed" });
    expect(saved?.awsEndpoint).toBeUndefined();
    expect(saved).not.toHaveProperty("clearAmbiguousStoredUri");
    await pending;
  });

  it("accepts a re-entered DynamoDB endpoint for an ambiguous stored awsEndpoint", async () => {
    const existing: ConnectionConfig = {
      id: "dynamo-reentered",
      name: "Dynamo local",
      type: "dynamodb",
      awsRegion: "us-east-1",
      awsEndpoint: "http://u:p/w@localhost:8000",
    };
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(async (_config: ConnectionConfig) => ({
        success: true,
      })),
      saveConnection: vi.fn(async (_config: ConnectionConfig) => undefined),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await Promise.resolve();
    const state = vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    if (state?.view !== "connection" || !state.existing)
      throw new Error("Expected edit form");
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    const endpoint = "http://localhost:8000";
    const payload = { ...state.existing, endpoint };

    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });

    expect(manager.testConnection.mock.calls[0]?.[0]).toMatchObject({
      endpoint,
    });
    expect(manager.saveConnection.mock.calls[0]?.[0]).toMatchObject({
      id: existing.id,
      endpoint,
    });
    expect(
      manager.saveConnection.mock.calls[0]?.[0]?.awsEndpoint,
    ).toBeUndefined();
    await pending;
  });

  it("opens Edit when a stored URI secret has ambiguous credentials", async () => {
    const existing: ConnectionConfig = {
      id: "redis-ambiguous-secret",
      name: "Redis",
      type: "redis",
      connectionUri: "redis://cache.local:6379/0",
      useSecretStorage: true,
    };
    const context = {
      secrets: {
        get: vi.fn(async () =>
          JSON.stringify({
            connectionUri: "redis://user:pa/ss@cache.local:6379/0",
          }),
        ),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
      saveConnection: vi.fn(),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const state = vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    if (state?.view !== "connection" || !state.existing)
      throw new Error("Expected edit form");
    expect(state.existing.connectionUri).toBe(existing.connectionUri);
    expect(JSON.stringify(state)).not.toContain("pa/ss");
    createdPanel()?.dispose();
    await expect(pending).resolves.toBeUndefined();
  });

  it("opens and saves a stored Redis URI with ambiguous credentials without exposing them", async () => {
    const existing: ConnectionConfig = {
      id: "redis-ambiguous",
      name: "Redis ambiguous",
      type: "redis",
      connectionUri: "redis://user:pa/ss@cache.local:6379/0",
    };
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
      saveConnection: vi.fn(async (_config: ConnectionConfig) => undefined),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await Promise.resolve();
    const state = vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    if (state?.view !== "connection" || !state.existing)
      throw new Error("Expected edit form");
    expect(JSON.stringify(state)).not.toContain("pa/ss");
    expect(state.existing.connectionUri).toBeUndefined();
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    const connectionUri = "redis://user:pa%2Fss@cache.local:6379/0";
    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...state.existing, connectionUri },
    });
    expect(manager.saveConnection.mock.calls[0]?.[0]).toMatchObject({
      id: existing.id,
      connectionUri,
    });
    expect(JSON.stringify(panel.webview.postMessage.mock.calls)).not.toContain(
      "pa/ss",
    );
    await pending;
  });

  it.each(
    [
      { username: "cache user", password: " pass:@/雪% ", storage: true },
      { username: undefined, password: "default-password", storage: true },
      { username: "inline-user", password: "inline-password", storage: false },
    ].flatMap((scenario) =>
      [true, false].map((keep) => ({ ...scenario, keep })),
    ),
  )("retains or removes legacy Redis URI authentication through Test and Save ($storage, $username, keep=$keep)", async ({
    username,
    password,
    storage,
    keep,
  }) => {
    const existing: ConnectionConfig = {
      id: "redis-legacy-auth",
      name: "Legacy Redis",
      type: "redis",
      connectionUri: "redis://cache.local:6380/4?socket_timeout=2",
      username,
      password: storage ? undefined : password,
      useSecretStorage: storage,
    };
    const serialized = storage ? JSON.stringify({ password }) : undefined;
    const context = {
      secrets: {
        get: vi.fn(async () => serialized),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(async (_config: ConnectionConfig) => ({
        success: true,
      })),
      saveConnection: vi.fn(
        async (
          _config: ConnectionConfig,
          _options: ConnectionSecretClearOptions,
        ) => undefined,
      ),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await Promise.resolve();
    const initial =
      vi.mocked(createWebviewShell).mock.calls[0]?.[0].initialState;
    expect(initial).toMatchObject({
      existing: {
        connectionUri: existing.connectionUri,
        hasStoredConnectionUriCredentials: true,
      },
    });
    expect(JSON.stringify(initial)).not.toContain(password);
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    const payload = {
      id: existing.id,
      name: "Renamed Redis",
      type: "redis",
      connectionUri: existing.connectionUri,
      useSecretStorage: storage,
      hasStoredSecret: true,
      keepConnectionUriCredentials: keep,
    };
    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    const tested = manager.testConnection.mock.calls[0]?.[0];
    if (!tested?.connectionUri) throw new Error("Expected resolved Redis URI");
    const uri = new URL(tested.connectionUri);
    expect(decodeURIComponent(uri.username)).toBe(keep ? (username ?? "") : "");
    expect(decodeURIComponent(uri.password)).toBe(keep ? password : "");
    expect(uri.search).toBe("?socket_timeout=2");
    expect(tested.password).toBeUndefined();
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    const save = manager.saveConnection.mock.calls[0];
    if (!save) throw new Error("Expected saved Redis connection");
    expect(save[0]).toEqual(tested);
    const persisted = serializeConnectionSecretsForStoredConfig(
      save[0],
      serialized,
      save[1],
    );
    expect(JSON.parse(persisted ?? "{}")).toEqual(
      keep ? { connectionUri: tested.connectionUri } : {},
    );
    expect((await pending)?.connectionUri).toBe(existing.connectionUri);
  });

  it.each([
    {
      type: "redis",
      existing: {
        id: "redis-uri-edit",
        name: "Redis URI",
        type: "redis",
        connectionUri: "redis://cache.local:6380/4",
        useSecretStorage: true,
      },
      secrets: {
        connectionUri: "redis://user:pass@cache.local:6380/4?token=old-secret",
      },
      payload: {
        connectionUri: "redis://cache.local:6380/5?socket_timeout=2",
      },
      expectedField: "connectionUri",
      retained:
        "redis://user:pass@cache.local:6380/5?socket_timeout=2&token=old-secret",
      safe: "redis://cache.local:6380/5?socket_timeout=2",
    },
    {
      type: "elasticsearch",
      existing: {
        id: "elastic-uri-edit",
        name: "Elastic URI",
        type: "elasticsearch",
        connectionUri: "https://elastic.local/base",
        useSecretStorage: true,
      },
      secrets: {
        connectionUri:
          "https://elastic-user:pass@elastic.local/base?api_key=old-secret",
      },
      payload: { endpoint: "https://elastic.local/edited" },
      expectedField: "endpoint",
      retained:
        "https://elastic-user:pass@elastic.local/edited?api_key=old-secret",
      safe: "https://elastic.local/edited",
    },
  ] as const)("uses the same credential-aware config for Test and Save for $type (keep=$keep)", async ({
    type,
    existing,
    secrets,
    payload: uriPayload,
    expectedField,
    retained,
    safe,
  }) => {
    for (const keep of [true, false]) {
      vi.clearAllMocks();
      const context = {
        secrets: {
          get: vi.fn(async () => JSON.stringify(secrets)),
          store: vi.fn(),
          delete: vi.fn(),
        },
      };
      const manager = {
        getConnection: vi.fn(() => existing),
        testConnection: vi.fn(async (_config: Record<string, unknown>) => ({
          success: true,
        })),
        saveConnection: vi.fn(
          async (_config: Record<string, unknown>) => undefined,
        ),
      };
      const pending = ConnectionFormPanel.show(
        context as never,
        manager as never,
        existing as never,
      );
      await vi.waitFor(() => expect(createdPanel()).toBeDefined());
      const panel = createdPanel();
      if (!panel) throw new Error("Expected connection form");
      const payload = {
        ...existing,
        connectionUri:
          expectedField === "connectionUri"
            ? uriPayload.connectionUri
            : undefined,
        endpoint:
          expectedField === "endpoint" ? uriPayload.endpoint : undefined,
        keepConnectionUriCredentials: keep,
      };

      await panel.webview.dispatchMessage({ type: "testConnection", payload });
      const tested = manager.testConnection.mock.calls[0]?.[0];
      expect(tested?.[expectedField]).toBe(keep ? retained : safe);
      if (type === "elasticsearch")
        expect(tested?.connectionUri).toBeUndefined();

      await panel.webview.dispatchMessage({ type: "saveConnection", payload });
      expect(manager.saveConnection.mock.calls[0]?.[0]).toEqual(tested);
      await pending;
    }
  });
});

describe("explicit saved credential removal", () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([
    "pg",
    "mysql",
    "mssql",
    "oracle",
    "redis",
  ] as const)("removes a plaintext %s password with the same config for Test and Save", async (type) => {
    const existing: ConnectionConfig = {
      id: "plaintext-clear",
      name: "Plaintext password",
      type,
      host: "localhost",
      database: type === "redis" ? "0" : "app",
      username: "user",
      password: "old-password",
      useSecretStorage: false,
    };
    const context = {
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(async (_config: ConnectionConfig) => ({
        success: true,
      })),
      saveConnection: vi.fn(
        async (
          _config: ConnectionConfig,
          _options: ConnectionSecretClearOptions,
        ) => undefined,
      ),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
      existing,
    );
    await Promise.resolve();
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    const payload = {
      ...existing,
      password: "",
      hasStoredSecret: true,
      clearPassword: true,
    };
    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    const tested = manager.testConnection.mock.calls[0]?.[0];
    expect(tested?.password).toBe("");
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    expect(manager.saveConnection).toHaveBeenCalledWith(
      tested,
      {
        password: true,
        apiKey: false,
        credentialBearingUris: false,
        awsCredentials: false,
      },
      expect.objectContaining({ kind: "edit", id: existing.id }),
    );
    expect(vscodeMock.module.window.showWarningMessage).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ type: "saveResult" }),
    );
    await pending;
  });

  it.each([
    {
      type: "pg",
      existing: {
        id: "pg-clear-password",
        name: "Postgres",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "user",
        useSecretStorage: true,
      },
      secrets: { password: "old-password" },
      payload: { clearPassword: true },
      expected: { password: "" },
    },
    {
      type: "elasticsearch",
      existing: {
        id: "elastic-clear-api-key",
        name: "Elasticsearch",
        type: "elasticsearch",
        endpoint: "https://elastic.local",
        useSecretStorage: true,
      },
      secrets: { apiKey: "old-api-key" },
      payload: { clearApiKey: true },
      expected: { apiKey: undefined },
    },
    {
      type: "dynamodb",
      existing: {
        id: "dynamo-clear-keys",
        name: "DynamoDB",
        type: "dynamodb",
        awsRegion: "us-east-1",
        useSecretStorage: true,
      },
      secrets: {
        awsAccessKeyId: "old-access-key",
        awsSecretAccessKey: "old-secret-key",
        awsSessionToken: "old-session-token",
      },
      payload: { clearAwsCredentials: true },
      expected: {
        awsAccessKeyId: undefined,
        awsSecretAccessKey: undefined,
        awsSessionToken: undefined,
      },
    },
    {
      type: "dynamodb",
      existing: {
        id: "dynamo-profile",
        name: "DynamoDB",
        type: "dynamodb",
        awsRegion: "us-east-1",
        useSecretStorage: true,
      },
      secrets: {
        awsAccessKeyId: "old-access-key",
        awsSecretAccessKey: "old-secret-key",
        awsSessionToken: "old-session-token",
      },
      payload: { awsProfile: "team" },
      expected: {
        awsProfile: "team",
        awsAccessKeyId: undefined,
        awsSecretAccessKey: undefined,
        awsSessionToken: undefined,
      },
    },
  ] as const)("clears saved $type credentials for both Test and Save", async ({
    existing,
    secrets,
    payload: clearFlags,
    expected,
  }) => {
    const context = {
      secrets: {
        get: vi.fn(async () => JSON.stringify(secrets)),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const manager = {
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(async (_config: Record<string, unknown>) => ({
        success: true,
      })),
      saveConnection: vi.fn(
        async (_config: Record<string, unknown>) => undefined,
      ),
    };
    const pending = ConnectionFormPanel.show(
      context as never,
      manager as never,
    );
    const panel = createdPanel();
    if (!panel) throw new Error("Expected connection form");
    const payload = { ...existing, ...clearFlags };

    await panel.webview.dispatchMessage({ type: "testConnection", payload });
    const tested = manager.testConnection.mock.calls[0]?.[0];
    expect(tested).toMatchObject(expected);
    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    expect(manager.saveConnection.mock.calls[0]?.[0]).toEqual(tested);
    await pending;
  });
});

describe("uniform secret storage choice", () => {
  beforeEach(() => vi.clearAllMocks());

  const payloads = [
    {
      name: "Elasticsearch",
      payload: {
        type: "elasticsearch",
        endpoint: "https://es.example.com",
        apiKey: "es-key",
      },
    },
    {
      name: "DynamoDB",
      payload: {
        type: "dynamodb",
        awsRegion: "us-east-1",
        awsAccessKeyId: "AKIA123",
        awsSecretAccessKey: "secret-key",
      },
    },
    {
      name: "SSH",
      payload: {
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        password: "db-secret",
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "password",
          password: "ssh-secret",
          hostVerificationMode: "manual",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
      },
    },
    {
      name: "TLS key passphrase",
      payload: {
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        tls: {
          mode: "mutualTls",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
          keyPassphrase: "tls-secret",
        },
      },
    },
    {
      name: "credential URI",
      payload: {
        type: "redis",
        connectionUri: "redis://user:redis-secret@localhost:6379/0",
      },
    },
  ];

  it.each(
    payloads.flatMap((entry) =>
      [false, true].map((useSecretStorage) => ({ ...entry, useSecretStorage })),
    ),
  )("saves $name with the submitted storage choice $useSecretStorage", async ({
    payload,
    useSecretStorage,
  }) => {
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
    if (!panel) throw new Error("Expected a webview panel to be created.");

    await panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { id: "uniform", name: "Uniform", ...payload, useSecretStorage },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({ id: "uniform", useSecretStorage }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({ useSecretStorage }),
      expect.any(Object),
      { kind: "create" },
    );
    expect(vscodeMock.module.window.showWarningMessage).toHaveBeenCalledTimes(
      useSecretStorage ? 0 : 1,
    );
    expect(context.secrets.store).not.toHaveBeenCalled();
  });

  it("moves stored secrets into the plaintext config when the choice is turned off", async () => {
    const existing: ConnectionConfig = {
      id: "toggle-off",
      name: "Toggle off",
      type: "elasticsearch",
      endpoint: "https://es.example.com",
      useSecretStorage: true,
    };
    const context = {
      secrets: {
        get: vi.fn(async () => JSON.stringify({ apiKey: "stored-key" })),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());

    await createdPanel().webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: existing.id,
        name: existing.name,
        type: "elasticsearch",
        endpoint: "https://es.example.com",
        hasStoredApiKey: true,
        useSecretStorage: false,
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({ useSecretStorage: false }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        apiKey: "stored-key",
        useSecretStorage: false,
      }),
      expect.any(Object),
      expect.objectContaining({ kind: "edit" }),
    );
  });

  it("blocks saving a flagless profile when Secret Storage cannot be read at save", async () => {
    const existing: ConnectionConfig = {
      id: "flagless-locked",
      name: "Flagless locked",
      type: "elasticsearch",
      endpoint: "https://es.example.com",
    };
    const context = {
      secrets: {
        get: vi.fn(async () => JSON.stringify({ apiKey: "stored-key" })),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
    };
    void ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    context.secrets.get.mockRejectedValueOnce(new Error("keychain locked"));

    await createdPanel().webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: existing.id,
        name: existing.name,
        type: "elasticsearch",
        endpoint: "https://es.example.com",
        hasStoredApiKey: true,
        useSecretStorage: false,
      },
    });

    expect(connectionManager.saveConnection).not.toHaveBeenCalled();
    expect(createdPanel().webview.postMessage).toHaveBeenCalledWith({
      type: "saveResult",
      payload: {
        success: false,
        error:
          "SecretStorage unavailable: keychain locked. Existing connection data was not changed.",
      },
    });
    expect(context.secrets.delete).not.toHaveBeenCalled();
    createdPanel().dispose();
  });

  it("does not read Secret Storage when saving a plaintext profile", async () => {
    const existing: ConnectionConfig = {
      id: "plaintext-save",
      name: "Plaintext",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "postgres",
      password: "plain",
      useSecretStorage: false,
    };
    const context = {
      secrets: {
        get: vi.fn(async () => {
          throw new Error("keychain prompt");
        }),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());

    await createdPanel().webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: existing.id,
        name: "Renamed",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        hasStoredSecret: true,
        useSecretStorage: false,
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({ name: "Renamed" }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({ password: "plain", useSecretStorage: false }),
      expect.any(Object),
      expect.objectContaining({ kind: "edit" }),
    );
    expect(context.secrets.get).not.toHaveBeenCalled();
  });

  it("ignores a stale Secret Storage entry for a plaintext profile", async () => {
    const existing: ConnectionConfig = {
      id: "stale-plaintext",
      name: "Stale plaintext",
      type: "elasticsearch",
      endpoint: "https://es.example.com",
      apiKey: "current-key",
      useSecretStorage: false,
    };
    const context = {
      secrets: {
        get: vi.fn(async () =>
          JSON.stringify({ apiKey: "stale-key", password: "stale-password" }),
        ),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());
    const shellCall = vi.mocked(createWebviewShell).mock.calls[0]?.[0] as
      | {
          initialState?: {
            existing?: { hasStoredApiKey?: boolean; hasStoredSecret?: boolean };
          };
        }
      | undefined;
    expect(shellCall?.initialState?.existing?.hasStoredApiKey).toBe(true);
    expect(shellCall?.initialState?.existing?.hasStoredSecret).toBeUndefined();

    await createdPanel().webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: existing.id,
        name: existing.name,
        type: "elasticsearch",
        endpoint: "https://es.example.com",
        hasStoredApiKey: true,
        useSecretStorage: false,
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({ useSecretStorage: false }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        apiKey: "current-key",
        useSecretStorage: false,
      }),
      expect.any(Object),
      expect.objectContaining({ kind: "edit" }),
    );
    expect(connectionManager.saveConnection.mock.calls[0][0].password).not.toBe(
      "stale-password",
    );
  });

  it("moves plaintext secrets into the submitted config when the choice is turned on", async () => {
    const existing: ConnectionConfig = {
      id: "toggle-on",
      name: "Toggle on",
      type: "dynamodb",
      awsRegion: "us-east-1",
      awsAccessKeyId: "plain-access",
      awsSecretAccessKey: "plain-secret",
      useSecretStorage: false,
    };
    const context = {
      secrets: {
        get: vi.fn(async () => undefined),
        store: vi.fn(),
        delete: vi.fn(),
      },
    };
    const connectionManager = {
      saveConnection: vi.fn().mockResolvedValue(undefined),
      getConnection: vi.fn(() => existing),
      testConnection: vi.fn(),
    };
    const promise = ConnectionFormPanel.show(
      context as never,
      connectionManager as never,
      existing,
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());

    await createdPanel().webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: existing.id,
        name: existing.name,
        type: "dynamodb",
        awsRegion: "us-east-1",
        hasStoredAwsCredentials: true,
        useSecretStorage: true,
      },
    });

    await expect(promise).resolves.toEqual(
      expect.objectContaining({ useSecretStorage: true }),
    );
    expect(connectionManager.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        awsAccessKeyId: "plain-access",
        awsSecretAccessKey: "plain-secret",
        useSecretStorage: true,
      }),
      expect.any(Object),
      expect.objectContaining({ kind: "edit" }),
    );
    expect(vscodeMock.module.window.showWarningMessage).not.toHaveBeenCalled();
  });

  it.each([
    [JSON.stringify({ apiKey: "stored-key" }), true],
    [undefined, true],
  ])("ticks the storage choice for a flagless profile (%s)", async (stored, expected) => {
    const context = {
      secrets: {
        get: vi.fn(async () => stored),
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
        id: "flagless",
        name: "Flagless",
        type: "elasticsearch",
        endpoint: "https://es.example.com",
      },
    );
    await vi.waitFor(() => expect(createdPanel()).toBeDefined());

    const shellCall = vi.mocked(createWebviewShell).mock.calls[0]?.[0] as
      | { initialState?: { existing?: { useSecretStorage?: boolean } } }
      | undefined;
    expect(shellCall?.initialState?.existing?.useSecretStorage).toBe(expected);
    createdPanel()?.dispose();
    await expect(promise).resolves.toBeUndefined();
  });
});
