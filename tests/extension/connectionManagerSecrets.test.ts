import { describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createExtensionContextStub } from "../support/fakeConnectionManagerStore";

const configuration = vi.hoisted(() => ({
  connections: [] as ConnectionConfig[],
  rejectAfterApply: false,
}));

vi.mock("vscode", async () => {
  const { createMockVscodeModule } = await import("../support/mockVscode");
  const mock = createMockVscodeModule();
  mock.state.getConfiguration.mockImplementation(() => ({
    get: vi.fn((key: string, fallback?: unknown) =>
      key === "connections" ? configuration.connections : fallback,
    ),
    inspect: () => ({ globalValue: configuration.connections }),
    update: vi.fn(async (_key: string, value: ConnectionConfig[]) => {
      configuration.connections = JSON.parse(JSON.stringify(value));
      if (configuration.rejectAfterApply)
        throw new Error("acknowledgement failed");
    }),
  }));
  return mock.module;
});

describe("ConnectionManager with VSCode SecretStorage", () => {
  it("rejects an Elasticsearch HTTP endpoint before persisting required TLS credentials", async () => {
    configuration.connections = [];
    const context = createExtensionContextStub();
    const manager = new ConnectionManager(context as never);
    try {
      await expect(
        manager.saveConnection({
          id: "elastic-http",
          name: "Elasticsearch",
          type: "elasticsearch",
          connectionUri: "http://db.internal:9200",
          password: "elastic-secret",
          tls: { mode: "requireVerifyFull" },
        }),
      ).rejects.toThrow("requires an HTTPS");
      expect(configuration.connections).toEqual([]);
      await expect(
        context.secrets.get("elastic-http"),
      ).resolves.toBeUndefined();
    } finally {
      await manager.dispose();
    }
  });

  it.each([
    "proxyPassword",
    "tlsCertificateKeyFilePassword",
    "passphrase",
  ])("migrates an existing plaintext MongoDB URI %s before connecting", async (key) => {
    const connectionUri = `mongodb://db.internal/app?${key}=mongo-secret&replicaSet=rs`;
    const config: ConnectionConfig = {
      id: "mongo-uri-migration",
      name: "MongoDB",
      type: "mongodb",
      connectionUri,
      useSecretStorage: true,
    };
    configuration.connections = [config];
    const context = createExtensionContextStub();
    const manager = new ConnectionManager(context as never);
    const state = manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
    };
    let connected = false;
    const factory = vi.spyOn(state, "createDriver").mockReturnValue({
      connect: vi.fn(async () => {
        connected = true;
      }),
      disconnect: vi.fn(async () => {
        connected = false;
      }),
      isConnected: () => connected,
    } as unknown as IDBDriver);
    try {
      await manager.connectTo(config.id);
      expect(configuration.connections[0].connectionUri).toBe(
        "mongodb://db.internal/app?replicaSet=rs",
      );
      expect(
        JSON.parse((await context.secrets.get(config.id)) ?? "{}"),
      ).toMatchObject({ connectionUri });
      expect(factory).toHaveBeenLastCalledWith(
        expect.objectContaining({ connectionUri }),
      );
    } finally {
      await manager.dispose();
    }
  });

  it.each([
    "proxyPassword",
    "tlsCertificateKeyFilePassword",
    "passphrase",
  ])("keeps MongoDB URI %s in SecretStorage through JSON save, edit and runtime hydration", async (key) => {
    configuration.connections = [];
    const context = createExtensionContextStub();
    const manager = new ConnectionManager(context as never);
    const state = manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
    };
    let connected = false;
    const factory = vi.spyOn(state, "createDriver").mockReturnValue({
      connect: vi.fn(async () => {
        connected = true;
      }),
      disconnect: vi.fn(async () => {
        connected = false;
      }),
      isConnected: () => connected,
    } as unknown as IDBDriver);
    const connectionUri = `mongodb://db.internal/app?${key}=mongo-secret&replicaSet=rs`;
    const config: ConnectionConfig = {
      id: "mongo-uri-secret",
      name: "MongoDB",
      type: "mongodb",
      connectionUri,
      useSecretStorage: true,
    };
    try {
      const saved = await manager.saveConnection(config);
      const redacted = "mongodb://db.internal/app?replicaSet=rs";
      expect(saved.connectionUri).toBe(redacted);
      expect(configuration.connections[0].connectionUri).toBe(redacted);
      expect(
        JSON.parse((await context.secrets.get(config.id)) ?? "{}"),
      ).toMatchObject({ connectionUri });
      await manager.saveConnection({ ...saved, name: "Renamed" });
      await manager.connectTo(saved.id);
      expect(factory).toHaveBeenLastCalledWith(
        expect.objectContaining({ connectionUri }),
      );
      expect(configuration.connections[0].connectionUri).toBe(redacted);
    } finally {
      await manager.dispose();
    }
  });

  it.each([
    false,
    true,
  ])("retains new credentials and connects after a JSON-persisted save applies then rejects (existing=%s)", async (existing) => {
    const context = createExtensionContextStub();
    const config: ConnectionConfig = {
      id: "json-commit",
      name: "JSON commit",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "user",
      useSecretStorage: true,
      password: " new password ",
      connectionUri: "postgres://user:uri-secret@localhost/app",
      tls: {
        mode: "mutualTls",
        certFilePath: "/cert",
        keyFilePath: "/key",
        keyPassphrase: "tls-secret",
      },
    };
    configuration.connections = existing
      ? [
          {
            id: config.id,
            name: "Before",
            type: "pg",
            host: "localhost",
            database: "app",
            username: "user",
            useSecretStorage: true,
          },
        ]
      : [];
    if (existing)
      await context.secrets.store(
        config.id,
        JSON.stringify({ password: "old password", future: 42 }),
      );
    const deletes = vi.spyOn(context.secrets, "delete");
    const manager = new ConnectionManager(context as never);
    const state = manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
    };
    let connected = false;
    const driver = {
      connect: vi.fn(async () => {
        connected = true;
      }),
      disconnect: vi.fn(async () => {
        connected = false;
      }),
      isConnected: () => connected,
    };
    const factory = vi
      .spyOn(state, "createDriver")
      .mockReturnValue(driver as unknown as IDBDriver);
    configuration.rejectAfterApply = true;
    try {
      const saved = await manager.saveConnection(config);
      expect(saved.password).toBeUndefined();
      expect(configuration.connections[0]).toMatchObject({
        useSecretStorage: true,
        connectionUri: "postgres://localhost/app",
      });
      expect(configuration.connections[0]).not.toHaveProperty("ssh");
      expect(configuration.connections[0].tls).not.toHaveProperty(
        "keyPassphrase",
      );
      const raw = await context.secrets.get(config.id);
      expect(JSON.parse(raw ?? "{}")).toMatchObject({
        password: config.password,
        connectionUri: config.connectionUri,
        tlsKeyPassphrase: config.tls?.keyPassphrase,
      });
      if (existing) expect(JSON.parse(raw ?? "{}").future).toBe(42);
      expect(deletes).not.toHaveBeenCalled();
      await manager.connectTo(saved.id);
      expect(factory).toHaveBeenCalledWith(
        expect.objectContaining({
          password: config.password,
          connectionUri: config.connectionUri,
          tls: config.tls,
        }),
      );
      expect(manager.isConnected(saved.id)).toBe(true);
      await expect(context.secrets.get(config.id)).resolves.toBe(raw);
    } finally {
      configuration.rejectAfterApply = false;
      await manager.dispose();
    }
  });

  it.each([
    false,
    true,
  ])("returns only sanitized metadata while preserving save/edit/runtime credentials (storage=%s)", async (useSecretStorage) => {
    const context = createExtensionContextStub();
    configuration.connections = [];
    const manager = new ConnectionManager(context as never);
    const state = manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
    };
    let connected = false;
    const driver = {
      connect: vi.fn(async () => {
        connected = true;
      }),
      disconnect: vi.fn(async () => {
        connected = false;
      }),
      isConnected: () => connected,
    };
    const factory = vi
      .spyOn(state, "createDriver")
      .mockReturnValue(driver as unknown as IDBDriver);
    const config: ConnectionConfig = {
      id: "outward-save",
      name: "Outward save",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "user",
      password: " db-secret ",
      useSecretStorage,
      connectionUri: "postgres://user:uri-secret@localhost/app",
    };
    try {
      const response = await manager.saveConnection(config);
      expect(response.password).toBeUndefined();
      expect(response.connectionUri).toBe("postgres://localhost/app");
      expect(response.useSecretStorage).toBe(useSecretStorage);
      expect(config.password).toBe(" db-secret ");
      expect(configuration.connections[0].password).toBe(
        useSecretStorage ? undefined : config.password,
      );
      await manager.connectTo(response.id);
      expect(factory).toHaveBeenLastCalledWith(
        expect.objectContaining({
          password: config.password,
          connectionUri: config.connectionUri,
        }),
      );
      const savedConfig = manager.getConnection(response.id);
      if (!savedConfig) throw new Error("Expected the saved connection.");
      const updated = {
        ...savedConfig,
        password: " new-secret ",
      };
      const edited = await manager.saveConnection(updated);
      expect(edited.password).toBeUndefined();
      expect(edited.useSecretStorage).toBe(useSecretStorage);
      await manager.connectTo(edited.id);
      expect(factory).toHaveBeenLastCalledWith(
        expect.objectContaining({
          password: " new-secret ",
          connectionUri: config.connectionUri,
        }),
      );
      if (!useSecretStorage)
        await expect(context.secrets.get(response.id)).resolves.toBeUndefined();
    } finally {
      await manager.dispose();
    }
  });

  it.each([
    1, 2,
  ])("preserves the last-read error and credentials across %s failed attempt(s), then reconnects", async (failures) => {
    const context = createExtensionContextStub();
    const password = " database password ";
    const rawSecret = '{ "password": " database password ", "future": 42 }';
    const config: ConnectionConfig = {
      id: "keychain-hydration",
      name: "Keychain hydration",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "postgres",
      password,
    };
    configuration.connections = [config];
    await context.secrets.store(config.id, rawSecret);
    const readSecret = context.secrets.get.bind(context.secrets);
    const reads = vi.spyOn(context.secrets, "get");
    const writes = vi.spyOn(context.secrets, "store");
    const deletes = vi.spyOn(context.secrets, "delete");
    const createSshRuntime = vi.fn();
    // Exercise the default VSCodeConnectionManagerStore, including its checked
    // migration transaction; replace only the external driver/SSH boundaries.
    const manager = new ConnectionManager(context as never, undefined, {
      createSshRuntime,
    });
    const state = manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
      _connectAbortControllerMap: Map<string, AbortController>;
      sshRuntimeMap: Map<string, unknown>;
    };
    let connected = false;
    const driver = {
      connect: vi.fn(async () => {
        connected = true;
      }),
      disconnect: vi.fn(async () => {
        connected = false;
      }),
      isConnected: () => connected,
    };
    const factory = vi
      .spyOn(state, "createDriver")
      .mockReturnValue(driver as unknown as IDBDriver);
    const originalError = new Error("SecretStorage read failed", {
      cause: new Error("Keychain locked"),
    });
    const hydrate = manager._hydratePassword.bind(manager);
    const hydration = vi
      .spyOn(manager, "_hydratePassword")
      .mockImplementation((liveConfig) => {
        expect(configuration.connections[0].useSecretStorage).toBe(true);
        expect(configuration.connections[0].password).toBeUndefined();
        expect(liveConfig.password).toBeUndefined();
        // The actual context SecretStorage read fails only after migration.
        reads.mockRejectedValueOnce(originalError);
        return hydrate(liveConfig);
      });

    try {
      for (let attempt = 0; attempt < failures; attempt += 1) {
        await expect(manager.connectTo(config.id)).rejects.toBe(originalError);
        expect(manager.isConnecting(config.id)).toBe(false);
        expect(manager.isConnected(config.id)).toBe(false);
        expect(manager.getDriver(config.id)).toBeUndefined();
        expect(state._connectAbortControllerMap.has(config.id)).toBe(false);
        expect(state.sshRuntimeMap.has(config.id)).toBe(false);
        expect(factory).not.toHaveBeenCalled();
        expect(createSshRuntime).not.toHaveBeenCalled();
        await expect(readSecret(config.id)).resolves.toBe(rawSecret);
        expect(config.password).toBe(password);
      }
      expect(hydration).toHaveBeenCalledTimes(failures);
      expect(writes).not.toHaveBeenCalled();
      expect(deletes).not.toHaveBeenCalled();

      hydration.mockRestore();
      await manager.connectTo(config.id);
      expect(factory).toHaveBeenCalledOnce();
      expect(factory).toHaveBeenCalledWith(
        expect.objectContaining({ id: config.id, password }),
      );
      expect(driver.connect).toHaveBeenCalledOnce();
      expect(manager.isConnected(config.id)).toBe(true);
      expect(manager.isConnecting(config.id)).toBe(false);
      await expect(readSecret(config.id)).resolves.toBe(rawSecret);
    } finally {
      await manager.dispose();
    }
  });
});
