import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  type ConnectionSecretClearOptions,
  serializeConnectionSecretsForStoredConfig,
} from "../../src/extension/connectionSecrets";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const vscodeMock = vi.hoisted(() => {
  const createWebviewPanel = vi.fn(() => {
    const messageListeners = new Set<(message: unknown) => void>();
    const disposeListeners = new Set<() => void>();
    return {
      webview: {
        html: "",
        postMessage: vi.fn(),
        onDidReceiveMessage(listener: (message: unknown) => void) {
          messageListeners.add(listener);
          return { dispose: () => messageListeners.delete(listener) };
        },
        async dispatchMessage(message: unknown) {
          for (const listener of messageListeners) await listener(message);
        },
      },
      onDidDispose(listener: () => void) {
        disposeListeners.add(listener);
        return { dispose: () => disposeListeners.delete(listener) };
      },
      dispose() {
        for (const listener of disposeListeners) listener();
      },
    };
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
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

const STORED_SECRETS = JSON.stringify({ password: "pg-secret" });

async function save(
  existing: ConnectionConfig,
  payload: Record<string, unknown>,
): Promise<{ config: ConnectionConfig; clears: ConnectionSecretClearOptions }> {
  const context = {
    secrets: {
      get: vi.fn(async () =>
        existing.useSecretStorage === false ? undefined : STORED_SECRETS,
      ),
      store: vi.fn(),
      delete: vi.fn(),
    },
  };
  const manager = {
    getConnection: vi.fn(() => existing),
    saveConnection: vi.fn(async (config: ConnectionConfig) => config),
    testConnection: vi.fn(),
  };
  const result = ConnectionFormPanel.show(
    context as never,
    manager as never,
    existing,
  );
  await vi.waitFor(() =>
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalled(),
  );
  const panel = vscodeMock.createWebviewPanel.mock.results[0]?.value;
  await panel.webview.dispatchMessage({
    type: "saveConnection",
    payload: { id: existing.id, name: existing.name, ...payload },
  });
  await result;
  expect(manager.saveConnection).toHaveBeenCalledTimes(1);
  const [config, clears] = manager.saveConnection.mock.calls[0] as unknown as [
    ConnectionConfig,
    ConnectionSecretClearOptions,
  ];
  return { config, clears };
}

function persistedPassword(
  config: ConnectionConfig,
  clears: ConnectionSecretClearOptions,
): string | undefined {
  const serialized = serializeConnectionSecretsForStoredConfig(
    config,
    STORED_SECRETS,
    clears,
  );
  return serialized
    ? (JSON.parse(serialized) as { password?: string }).password
    : undefined;
}

const pgProfile: ConnectionConfig = {
  id: "conn-1",
  name: "Warehouse",
  type: "pg",
  host: "db.local",
  database: "app",
  username: "reader",
  useSecretStorage: true,
};

describe("stored password after a connection type or auth change", () => {
  beforeEach(() => {
    vscodeMock.createWebviewPanel.mockClear();
  });

  it.each([
    [
      "DynamoDB",
      { type: "dynamodb", awsRegion: "us-east-1", useSecretStorage: true },
    ],
    [
      "Elasticsearch without basic auth",
      {
        type: "elasticsearch",
        endpoint: "https://es.local:9200",
        password: "",
        useSecretStorage: true,
        hasStoredSecret: true,
      },
    ],
    [
      "MySQL without a re-entered password",
      {
        type: "mysql",
        host: "db.local",
        port: 3306,
        database: "app",
        username: "reader",
        password: "",
        useSecretStorage: true,
        hasStoredSecret: true,
      },
    ],
  ])("drops the old pg password when switching to %s", async (_label, payload) => {
    const { config, clears } = await save(pgProfile, payload);

    expect(config.password ?? "").toBe("");
    expect(clears.password).toBe(true);
    expect(persistedPassword(config, clears)).toBeUndefined();
  });

  it("keeps a password re-entered after the type change", async () => {
    const { config, clears } = await save(pgProfile, {
      type: "mysql",
      host: "db.local",
      port: 3306,
      database: "app",
      username: "reader",
      password: "mysql-secret",
      useSecretStorage: true,
      hasStoredSecret: true,
    });

    expect(persistedPassword(config, clears)).toBe("mysql-secret");
  });

  it("keeps the stored password when the type is unchanged", async () => {
    const { config, clears } = await save(pgProfile, {
      type: "pg",
      host: "db.local",
      database: "app",
      username: "reader",
      password: "",
      useSecretStorage: true,
      hasStoredSecret: true,
    });

    expect(config.password).toBe("pg-secret");
    expect(clears.password).toBe(false);
  });

  it("does not keep a plaintext pg password after switching to DynamoDB", async () => {
    const { config } = await save(
      { ...pgProfile, useSecretStorage: false, password: "pg-secret" },
      {
        type: "dynamodb",
        awsRegion: "us-east-1",
        useSecretStorage: false,
        hasStoredSecret: true,
      },
    );

    expect(config.password).toBeUndefined();
  });

  it("drops a leftover password from an Elasticsearch profile without a username", () => {
    expect(
      persistedPassword(
        {
          id: "conn-es",
          name: "Elastic",
          type: "elasticsearch",
          endpoint: "https://es.local:9200",
          useSecretStorage: true,
        },
        {},
      ),
    ).toBeUndefined();
    expect(
      persistedPassword(
        {
          id: "conn-es",
          name: "Elastic",
          type: "elasticsearch",
          endpoint: "https://es.local:9200",
          username: "elastic",
          useSecretStorage: true,
        },
        {},
      ),
    ).toBe("pg-secret");
  });
});
