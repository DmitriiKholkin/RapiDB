import { describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createExtensionContextStub } from "../support/fakeConnectionManagerStore";

const configuration = vi.hoisted(() => ({
  connections: [] as ConnectionConfig[],
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
      configuration.connections = value;
    }),
  }));
  return mock.module;
});

describe("ConnectionManager with VSCode SecretStorage", () => {
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
