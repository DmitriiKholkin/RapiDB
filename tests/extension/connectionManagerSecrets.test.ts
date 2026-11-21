import { describe, expect, it, vi } from "vitest";
import * as vscode from "vscode";
import { connectionEditState } from "../../src/extension/connectionEditState";
import { ConnectionManager } from "../../src/extension/connectionManager";
import {
  connectionSessionKey,
  VSCodeConnectionManagerStore,
} from "../../src/extension/connectionManagerStore";
import { bindConnectionSecret } from "../../src/extension/connectionSecretBinding";
import {
  readConnectionCredentialRevision,
  withConnectionCredentialRevision,
} from "../../src/extension/connectionSecretsData";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { QueryPanel } from "../../src/extension/panels/queryPanel";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import { normalizeUnknownError } from "../../src/extension/utils/errorHandling";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { registerConnectionStorageFixture } from "../support/connectionStorageContext";
import { FakeConnectionManagerStore } from "../support/fakeConnectionManagerStore";

const createExtensionContextStub = registerConnectionStorageFixture();

const configuration = vi.hoisted(() => ({
  connections: [] as ConnectionConfig[],
  afterApply: undefined as (() => void) | undefined,
  rejectAfterApply: false,
  rejectBeforeApply: false,
  writes: 0,
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
      if (configuration.rejectBeforeApply)
        throw new Error("settings write failed");
      configuration.connections = JSON.parse(JSON.stringify(value));
      configuration.writes += 1;
      configuration.afterApply?.();
      if (configuration.rejectAfterApply)
        throw new Error("acknowledgement failed");
    }),
  }));
  return mock.module;
});

function fireConnectionsConfigurationChange() {
  const event = {
    affectsConfiguration: (section: string) => section === "rapidb.connections",
  };
  for (const [listener] of vi.mocked(vscode.workspace.onDidChangeConfiguration)
    .mock.calls) {
    listener(event);
  }
}

vi.mock("../../src/extension/panels/webviewShell", () => ({
  APP_WEBVIEW_SHELL_LAYOUT: {},
  MONACO_SCROLLBAR_STYLES: "",
  WEBVIEW_SCROLLBAR_STYLES: "",
  createWebviewShell: () => "<html></html>",
}));

function sharedSecretStorage() {
  const listeners = new Set<(event: { key: string }) => void>();
  const entries = new Map<string, string>();
  const { globalState } = createExtensionContextStub();
  const emit = (key: string) => {
    for (const listener of listeners) listener({ key });
  };
  let notify = true;
  const store = vi.fn(async (key: string, value: string) => {
    entries.set(key, value);
    if (notify) emit(key);
  });
  const get = vi.fn(async (key: string) => entries.get(key));
  return {
    emit,
    entries,
    listeners,
    muteEvents: () => {
      notify = false;
    },
    context: () => ({
      ...createExtensionContextStub(),
      globalState,
      secrets: {
        onDidChange(listener: (event: { key: string }) => void) {
          listeners.add(listener);
          return { dispose: () => listeners.delete(listener) };
        },
        get,
        store,
        async delete(key: string) {
          if (entries.delete(key) && notify) emit(key);
        },
      },
    }),
    get,
    store,
  };
}

function credentialDriverFactory(manager: ConnectionManager) {
  return vi.spyOn(
    manager as unknown as { createDriver(config: ConnectionConfig): IDBDriver },
    "createDriver",
  );
}

function credentialDriver(hold?: () => Promise<void>) {
  let connected = false;
  return {
    connect: vi.fn(async () => {
      await hold?.();
      connected = true;
    }),
    disconnect: vi.fn(async () => {
      connected = false;
    }),
    isConnected: () => connected,
    getDynamoDBTargetFingerprint: () => "a".repeat(64),
    verifyDynamoDBTarget: vi.fn(async () => undefined),
    query: vi.fn(async () => ({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    })),
  };
}

const sharedCredentialProfile = {
  id: "shared-static-credentials",
  name: "DynamoDB",
  type: "dynamodb",
  useSecretStorage: true,
  awsRegion: "us-east-1",
  awsCredentialMode: "static",
} satisfies ConnectionConfig;

async function rememberPreviouslyVerifiedTarget(
  context: vscode.ExtensionContext,
  manager: ConnectionManager,
): Promise<void> {
  const config = sharedCredentialProfile;
  const settingsKey = connectionSessionKey(
    config,
    manager.getConnectionSessionNonce(config.id),
  );
  if (!settingsKey) throw new Error("missing connection settings key");
  await new VSCodeConnectionManagerStore(context).writeDynamoDBTarget(
    config.id,
    {
      settingsKey,
      fingerprint: "a".repeat(64),
    },
  );
}

const originalCredentials = {
  awsAccessKeyId: "synthetic-access-key-A",
  awsSecretAccessKey: "synthetic-secret-key-A",
};
const replacementCredentials = {
  awsAccessKeyId: "synthetic-access-key-B",
  awsSecretAccessKey: "synthetic-secret-key-B",
};
const originalRevision = "00000000-0000-4000-8000-000000000001";

describe("credential documents are fenced by the saved profile", () => {
  const original: ConnectionConfig = {
    id: "bound-profile",
    name: "Profile",
    type: "pg",
    useSecretStorage: true,
    host: "first.invalid",
    port: 5432,
    username: "reader",
    database: "app",
  };

  it.each([
    false,
    true,
  ])("keeps a peer from using staged credentials with old settings (rollback=%s)", async (rollback) => {
    const context = createExtensionContextStub();
    const writerStore = new FakeConnectionManagerStore();
    const peerStore = new FakeConnectionManagerStore();
    const legacy = '{"password":"previous"}';
    writerStore.setConnections([original]);
    writerStore.setSecret(original.id, legacy);
    peerStore.setConnections([original]);
    vi.spyOn(peerStore, "getSecret").mockImplementation((key) =>
      writerStore.getSecret(key),
    );
    const writer = new ConnectionManager(context as never, writerStore);
    const peer = new ConnectionManager(context as never, peerStore);
    const peerDriver = credentialDriver();
    const factory = credentialDriverFactory(peer).mockReturnValue(
      peerDriver as unknown as IDBDriver,
    );
    const storeSecret = writerStore.storeSecret.bind(writerStore);
    vi.spyOn(writerStore, "storeSecret").mockImplementationOnce(
      async (key, value) => {
        await storeSecret(key, value);
        await expect(peer.connectTo(original.id, "explicit")).rejects.toThrow(
          "do not match",
        );
        expect(factory).not.toHaveBeenCalled();
        expect(peerDriver.connect).not.toHaveBeenCalled();
      },
    );
    if (rollback)
      writerStore.failNextConnectionWrite(new Error("settings unavailable"));
    try {
      const saving = writer.saveConnection({
        ...original,
        host: "second.invalid",
        password: "replacement",
      });
      if (rollback) {
        await expect(saving).rejects.toThrow("settings unavailable");
        await expect(writerStore.getSecret(original.id)).resolves.toBe(legacy);
      } else {
        await saving;
        peerStore.setConnections(writerStore.getConnections());
        peerStore.fireConfigurationChange("rapidb.connections");
      }
      await peer.connectTo(original.id, "explicit");
      expect(factory).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          host: rollback ? "first.invalid" : "second.invalid",
          password: rollback ? "previous" : "replacement",
        }),
      );
    } finally {
      await Promise.all([writer.dispose(), peer.dispose()]);
    }
  });

  it("does not inherit an old password on a manager-level target change", async () => {
    const context = createExtensionContextStub();
    const store = new FakeConnectionManagerStore();
    store.setConnections([original]);
    store.setSecret(original.id, "previous");
    const manager = new ConnectionManager(context as never, store);
    try {
      await manager.saveConnection({ ...original, host: "second.invalid" });
      const [saved] = store.getConnections();
      await expect(manager._hydratePassword(saved)).resolves.toMatchObject({
        host: "second.invalid",
        password: "",
      });
      await expect(manager._hydratePassword(original)).rejects.toThrow(
        "do not match",
      );
      await store.deleteSecret(original.id);
      const driver = credentialDriver();
      const factory = credentialDriverFactory(manager).mockReturnValue(
        driver as unknown as IDBDriver,
      );
      await expect(manager.connectTo(original.id, "explicit")).rejects.toThrow(
        "do not match",
      );
      expect(factory).not.toHaveBeenCalled();
      expect(driver.connect).not.toHaveBeenCalled();
    } finally {
      await manager.dispose();
    }
  });

  it("rotates the profile link when URI credentials change under the same redacted URI", async () => {
    const context = createExtensionContextStub();
    const store = new FakeConnectionManagerStore();
    const manager = new ConnectionManager(context as never, store);
    const config: ConnectionConfig = {
      id: "uri-profile",
      name: "URI profile",
      type: "mongodb",
      mongoConnectionMode: "uri",
      useSecretStorage: true,
      connectionUri: "mongodb://original:original-password@db.invalid/app",
    };
    try {
      const first = await manager.saveConnection(config);
      const second = await manager.saveConnection({
        ...config,
        connectionUri: "mongodb://replacement:new-password@db.invalid/app",
      });
      expect(first.connectionUri).toBe(second.connectionUri);
      expect(first.secretStorageProfile).not.toBe(second.secretStorageProfile);
      await expect(manager._hydratePassword(first)).rejects.toThrow(
        "do not match",
      );
      await expect(manager._hydratePassword(second)).resolves.toMatchObject({
        connectionUri: "mongodb://replacement:new-password@db.invalid/app",
      });
    } finally {
      await manager.dispose();
    }
  });
});

describe("legacy credentials are bound to their target on first use", () => {
  const legacyProfile: ConnectionConfig = {
    id: "legacy-profile",
    name: "Legacy",
    type: "pg",
    host: "first.invalid",
    port: 5432,
    username: "reader",
    database: "app",
  };

  it("binds an unbound document on first use and then refuses a changed host", async () => {
    const context = createExtensionContextStub();
    const store = new FakeConnectionManagerStore();
    store.setConnections([legacyProfile]);
    store.setSecret(legacyProfile.id, '{"password":"legacy-password"}');
    const manager = new ConnectionManager(context as never, store);
    const driver = credentialDriver();
    const factory = credentialDriverFactory(manager).mockReturnValue(
      driver as unknown as IDBDriver,
    );
    try {
      await manager.connectTo(legacyProfile.id, "explicit");
      expect(factory).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          host: "first.invalid",
          password: "legacy-password",
        }),
      );
      const bound = JSON.parse(
        (await store.getSecret(legacyProfile.id)) as string,
      );
      expect(bound).toMatchObject({ password: "legacy-password" });
      expect(typeof bound.rapidbSecretTargetV1).toBe("string");
      await manager.disconnectFrom(legacyProfile.id);

      // A host change made outside Edit Connection must not receive the
      // password that was trusted for the first host.
      store.setConnections([{ ...legacyProfile, host: "attacker.invalid" }]);
      store.fireConfigurationChange("rapidb.connections");
      factory.mockClear();
      await expect(
        manager.connectTo(legacyProfile.id, "explicit"),
      ).rejects.toThrow(/do not match.*Edit Connection/);
      expect(factory).not.toHaveBeenCalled();
    } finally {
      await manager.dispose();
    }
  });

  it("still connects when Secret Storage cannot persist the binding", async () => {
    const context = createExtensionContextStub();
    const store = new FakeConnectionManagerStore();
    store.setConnections([legacyProfile]);
    store.setSecret(legacyProfile.id, "plain-legacy");
    vi.spyOn(store, "storeSecret").mockRejectedValue(
      new Error("keychain unavailable"),
    );
    const manager = new ConnectionManager(context as never, store);
    const driver = credentialDriver();
    const factory = credentialDriverFactory(manager).mockReturnValue(
      driver as unknown as IDBDriver,
    );
    try {
      await manager.connectTo(legacyProfile.id, "explicit");
      expect(factory).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ password: "plain-legacy" }),
      );
      await expect(store.getSecret(legacyProfile.id)).resolves.toBe(
        "plain-legacy",
      );
    } finally {
      await manager.dispose();
    }
  });

  it("does not bind when the settings target changed before the lock was taken", async () => {
    const context = createExtensionContextStub();
    const store = new FakeConnectionManagerStore();
    store.setConnections([legacyProfile]);
    store.setSecret(legacyProfile.id, '{"password":"legacy-password"}');
    const mutate = store.mutateConnections.bind(store);
    vi.spyOn(store, "mutateConnections").mockImplementation(async (fn) => {
      store.setConnections([{ ...legacyProfile, host: "other.invalid" }]);
      return mutate(fn);
    });
    const manager = new ConnectionManager(context as never, store);
    credentialDriverFactory(manager).mockReturnValue(
      credentialDriver() as unknown as IDBDriver,
    );
    try {
      await manager.connectTo(legacyProfile.id, "explicit");
      await expect(store.getSecret(legacyProfile.id)).resolves.toBe(
        '{"password":"legacy-password"}',
      );
    } finally {
      await manager.dispose();
    }
  });
});

describe("credential revisions across VS Code windows", () => {
  it("disconnects both live sessions once, leaves other keys alone, and requires a new query target binding", async () => {
    const config = sharedCredentialProfile;
    const unrelated = { ...config, id: "a-different-profile" };
    configuration.connections = [config, unrelated];
    const secrets = sharedSecretStorage();
    secrets.entries.set(
      config.id,
      bindConnectionSecret(
        config,
        JSON.stringify({ ...originalCredentials, future: 42 }),
      ),
    );
    secrets.entries.set(
      unrelated.id,
      bindConnectionSecret(unrelated, JSON.stringify(originalCredentials)),
    );
    const origin = new ConnectionManager(secrets.context() as never);
    const peer = new ConnectionManager(secrets.context() as never);
    const originSession = vi.fn();
    const peerSession = vi.fn();
    origin.onDidChangeConnectionSession(originSession);
    peer.onDidChangeConnectionSession(peerSession);
    const originDriver = credentialDriver();
    const peerDriver = credentialDriver();
    const otherDriver = credentialDriver();
    const replacementDriver = credentialDriver();
    const originFactory = credentialDriverFactory(origin).mockReturnValue(
      originDriver as unknown as IDBDriver,
    );
    const peerFactory = credentialDriverFactory(peer)
      .mockReturnValueOnce(peerDriver as unknown as IDBDriver)
      .mockReturnValueOnce(otherDriver as unknown as IDBDriver)
      .mockReturnValue(replacementDriver as unknown as IDBDriver);
    try {
      await Promise.all([
        origin.connectTo(config.id),
        peer.connectTo(config.id),
      ]);
      await peer.connectTo(unrelated.id);
      const originalKey = peer.getQueryTargetSessionKey(config.id) as string;
      expect(peerFactory).toHaveBeenCalledWith(
        expect.objectContaining(originalCredentials),
      );
      expect(originFactory).toHaveBeenCalledWith(
        expect.objectContaining(originalCredentials),
      );
      await origin.saveConnection({ ...config, ...originalCredentials });
      expect(originSession).not.toHaveBeenCalled();
      expect(secrets.store).not.toHaveBeenCalled();
      const beforeRotation = configuration.writes;

      const saved = await origin.saveConnection({
        ...config,
        ...replacementCredentials,
      });
      await vi.waitFor(() =>
        expect(peerDriver.disconnect).toHaveBeenCalledOnce(),
      );
      expect(saved).not.toHaveProperty("awsSecretAccessKey");
      expect(configuration.writes).toBe(beforeRotation);
      expect(configuration.connections).toEqual([config, unrelated]);
      expect(JSON.parse(secrets.entries.get(config.id) ?? "{}")).toMatchObject({
        ...replacementCredentials,
        future: 42,
      });
      expect(originDriver.disconnect).toHaveBeenCalledOnce();
      expect(originSession).toHaveBeenCalledExactlyOnceWith(config.id);
      expect(peerSession).toHaveBeenCalledExactlyOnceWith(config.id);
      expect(peer.isConnected(unrelated.id)).toBe(true);
      expect(peerFactory).toHaveBeenCalledTimes(2);
      expect(peer.getDriver(config.id)).toBeUndefined();
      await expect(
        peer.getDriverForQuery(config.id, originalKey),
      ).resolves.toBeUndefined();
      expect(peer.getQueryTargetSessionKey(config.id)).toBeNull();
      expect(origin.getQueryTargetSessionKey(config.id)).toBeNull();
      secrets.emit(config.id); // writer echo / duplicate or delayed broadcast resolved by read
      await peer.connectTo(config.id, "explicit");
      const currentKey = peer.getQueryTargetSessionKey(config.id) as string;
      expect(currentKey).not.toBe(originalKey);
      expect(origin.getQueryTargetSessionKey(config.id)).toBe(currentKey);
      await expect(peer.getDriverForQuery(config.id, currentKey)).resolves.toBe(
        replacementDriver,
      );
      expect(peerFactory).toHaveBeenLastCalledWith(
        expect.objectContaining(replacementCredentials),
      );
      expect(peerSession).toHaveBeenCalledOnce();
      expect(originSession).toHaveBeenCalledOnce();
      expect(peer.getDriver(config.id)).toBe(replacementDriver);

      const restart = new ConnectionManager(secrets.context() as never);
      try {
        await expect(
          restart.initializeConnectionCredentialRevision(config.id),
        ).resolves.toBe(true);
        expect(restart.getConnectionSessionGeneration(config.id)).toBe(0);
        expect(restart.getQueryTargetSessionKey(config.id)).toBe(currentKey);
      } finally {
        await restart.dispose();
      }
    } finally {
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
    expect(secrets.listeners.size).toBe(0);
  });

  it.each([
    false,
    true,
  ])("cancels an in-flight peer connect and does not publish a late old driver (with rename: %s)", async (rename) => {
    const config = sharedCredentialProfile;
    configuration.connections = [config];
    const secrets = sharedSecretStorage();
    secrets.entries.set(
      config.id,
      bindConnectionSecret(config, JSON.stringify(originalCredentials)),
    );
    const origin = new ConnectionManager(secrets.context() as never);
    const peerContext = secrets.context() as unknown as vscode.ExtensionContext;
    const peer = new ConnectionManager(peerContext);
    const sessions = vi.fn();
    const connected = vi.fn();
    peer.onDidChangeConnectionSession(sessions);
    peer.onDidConnect(connected);
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const oldDriver = credentialDriver(() => held);
    const newDriver = credentialDriver();
    const factory = credentialDriverFactory(peer)
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(newDriver as unknown as IDBDriver);
    try {
      if (rename) configuration.afterApply = fireConnectionsConfigurationChange;
      await peer.initializeConnectionCredentialRevision(config.id);
      await rememberPreviouslyVerifiedTarget(peerContext, peer);
      const oldKey = peer.getQueryTargetSessionKey(config.id) as string;
      const oldQuery = peer.getDriverForQuery(config.id, oldKey);
      await vi.waitFor(() => expect(oldDriver.connect).toHaveBeenCalledOnce());
      await origin.saveConnection({
        ...config,
        ...replacementCredentials,
        name: rename ? "Renamed in flight" : config.name,
      });
      await expect(oldQuery).resolves.toBeUndefined();
      await vi.waitFor(() => expect(peer.isConnecting(config.id)).toBe(false));
      expect(sessions).toHaveBeenCalledExactlyOnceWith(config.id);
      expect(connected).not.toHaveBeenCalled();
      expect(peer.getDriver(config.id)).toBeUndefined();
      // Wait for teardown, then a user-initiated reconnect uses the new secret.
      await peer.disconnectFrom(config.id);
      await peer.connectTo(config.id, "explicit");
      expect(factory).toHaveBeenLastCalledWith(
        expect.objectContaining(replacementCredentials),
      );
      release();
      await vi.waitFor(() => expect(oldDriver.isConnected()).toBe(false));
      expect(connected).toHaveBeenCalledOnce();
      expect(peer.getDriver(config.id)).toBe(newDriver);
    } finally {
      configuration.afterApply = undefined;
      release();
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
  });

  it.each([
    "settings first",
    "secret first",
    "settings acknowledgement lost",
    "secret acknowledgement lost",
  ] as const)("publishes a combined presentation and credential save once (%s)", async (delivery) => {
    const config = sharedCredentialProfile;
    const edited = { ...config, name: "Renamed", color: "red", folder: "Ops" };
    configuration.connections = [config];
    const secrets = sharedSecretStorage();
    const original = bindConnectionSecret(
      config,
      withConnectionCredentialRevision(
        JSON.stringify({ ...originalCredentials, future: 42 }),
        originalRevision,
      ),
    );
    secrets.entries.set(config.id, original);
    const origin = new ConnectionManager(secrets.context() as never);
    const peer = new ConnectionManager(secrets.context() as never);
    const originDriver = credentialDriver();
    const peerDriver = credentialDriver();
    const replacementDriver = credentialDriver();
    credentialDriverFactory(origin).mockReturnValue(
      originDriver as unknown as IDBDriver,
    );
    const peerFactory = credentialDriverFactory(peer)
      .mockReturnValueOnce(peerDriver as unknown as IDBDriver)
      .mockReturnValue(replacementDriver as unknown as IDBDriver);
    const originSession = vi.fn();
    const peerSession = vi.fn();
    origin.onDidChangeConnectionSession(originSession);
    peer.onDidChangeConnectionSession(peerSession);
    try {
      await Promise.all([
        origin.connectTo(config.id),
        peer.connectTo(config.id),
      ]);
      const oldKey = peer.getQueryTargetSessionKey(config.id) as string;
      const order: string[] = [];
      secrets.listeners.add(() => {
        const stored = secrets.entries.get(config.id);
        order.push(
          readConnectionCredentialRevision(stored) === originalRevision
            ? "staged secret event"
            : "committed secret event",
        );
      });
      configuration.afterApply = () => {
        order.push("settings applied");
        const staged = secrets.entries.get(config.id);
        expect(JSON.parse(staged ?? "{}")).toMatchObject(
          replacementCredentials,
        );
        expect(readConnectionCredentialRevision(staged)).toBe(originalRevision);
        expect(peerSession).not.toHaveBeenCalled();
        if (delivery !== "secret first") {
          order.push("settings event");
          fireConnectionsConfigurationChange();
        }
        if (delivery === "secret acknowledgement lost") {
          secrets.store.mockImplementationOnce(async (key, value) => {
            secrets.entries.set(key, value);
            secrets.emit(key);
            throw new Error("keychain acknowledgement failed");
          });
        }
      };
      configuration.rejectAfterApply =
        delivery === "settings acknowledgement lost";
      await expect(
        origin.saveConnection(
          { ...edited, ...replacementCredentials },
          {},
          {
            kind: "edit",
            id: config.id,
            state: connectionEditState(
              config,
              originalCredentials,
              originalRevision,
            ),
          },
        ),
      ).resolves.toMatchObject(edited);
      await vi.waitFor(() =>
        expect(peerDriver.disconnect).toHaveBeenCalledOnce(),
      );
      if (delivery === "secret first") {
        order.push("settings event");
        fireConnectionsConfigurationChange();
      }
      expect(order).toEqual([
        "staged secret event",
        "settings applied",
        ...(delivery === "secret first"
          ? ["committed secret event", "settings event"]
          : ["settings event", "committed secret event"]),
      ]);
      expect(secrets.store).toHaveBeenCalledTimes(2);
      const committed = secrets.entries.get(config.id);
      expect(JSON.parse(committed ?? "{}")).toMatchObject({
        ...replacementCredentials,
        future: 42,
      });
      expect(readConnectionCredentialRevision(committed)).toEqual(
        expect.any(String),
      );
      expect(readConnectionCredentialRevision(committed)).not.toBe(
        originalRevision,
      );
      expect(configuration.connections).toEqual([edited]);
      expect(peer.getConnection(config.id)).toMatchObject(edited);
      expect(originDriver.disconnect).toHaveBeenCalledOnce();
      await expect(
        peer.getDriverForQuery(config.id, oldKey),
      ).resolves.toBeUndefined();
      expect(peer.getQueryTargetSessionKey(config.id)).toBeNull();
      expect(origin.getQueryTargetSessionKey(config.id)).toBeNull();
      secrets.emit(config.id);
      await peer.connectTo(config.id, "explicit");
      const currentKey = peer.getQueryTargetSessionKey(config.id) as string;
      expect(currentKey).not.toBe(oldKey);
      expect(origin.getQueryTargetSessionKey(config.id)).toBe(currentKey);
      await expect(peer.getDriverForQuery(config.id, currentKey)).resolves.toBe(
        replacementDriver,
      );
      expect(peerFactory).toHaveBeenLastCalledWith(
        expect.objectContaining(replacementCredentials),
      );
      expect(originSession).toHaveBeenCalledExactlyOnceWith(config.id);
      expect(peerSession).toHaveBeenCalledExactlyOnceWith(config.id);

      // Saving the same credentials with another display edit isn't a rotation.
      configuration.afterApply = undefined;
      configuration.rejectAfterApply = false;
      await origin.saveConnection({
        ...edited,
        name: "Display only",
        ...replacementCredentials,
      });
      expect(secrets.store).toHaveBeenCalledTimes(2);
      expect(originSession).toHaveBeenCalledOnce();
      expect(peerSession).toHaveBeenCalledOnce();
    } finally {
      configuration.afterApply = undefined;
      configuration.rejectAfterApply = false;
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
  });

  it.each([
    "newer credential commit",
    "credential store failure",
    "rollback failure",
  ] as const)("does not acknowledge a combined save with a %s after settings applies", async (failure) => {
    const config = sharedCredentialProfile;
    const edited = { ...config, name: "Renamed", color: "red" };
    configuration.connections = [config];
    const secrets = sharedSecretStorage();
    const original = bindConnectionSecret(
      config,
      withConnectionCredentialRevision(
        JSON.stringify(originalCredentials),
        originalRevision,
      ),
    );
    const newerCredentials = {
      awsAccessKeyId: "synthetic-access-key-C",
      awsSecretAccessKey: "synthetic-secret-key-C",
    };
    const newer = bindConnectionSecret(
      config,
      withConnectionCredentialRevision(
        JSON.stringify(newerCredentials),
        "00000000-0000-4000-8000-000000000002",
      ),
    );
    secrets.entries.set(config.id, original);
    const origin = new ConnectionManager(secrets.context() as never);
    const peer = new ConnectionManager(secrets.context() as never);
    const originDriver = credentialDriver();
    credentialDriverFactory(origin).mockReturnValue(
      originDriver as unknown as IDBDriver,
    );
    const driver = credentialDriver();
    const reconnected = credentialDriver();
    const factory = credentialDriverFactory(peer)
      .mockReturnValueOnce(driver as unknown as IDBDriver)
      .mockReturnValue(reconnected as unknown as IDBDriver);
    const sessions = vi.fn();
    peer.onDidChangeConnectionSession(sessions);
    try {
      await Promise.all([
        origin.connectTo(config.id),
        peer.connectTo(config.id),
      ]);
      const key = peer.getQueryTargetSessionKey(config.id) as string;
      configuration.afterApply = () => {
        fireConnectionsConfigurationChange();
        if (failure === "newer credential commit") {
          // Simulate a different extension host, outside the local queue.
          secrets.entries.set(config.id, newer);
          secrets.emit(config.id);
        } else {
          secrets.store.mockRejectedValueOnce(
            new Error("keychain write failed"),
          );
          if (failure === "rollback failure") {
            secrets.store.mockRejectedValueOnce(
              new Error("keychain restore failed"),
            );
          }
        }
      };
      await expect(
        origin.saveConnection({ ...edited, ...replacementCredentials }),
      ).rejects.toThrow(
        failure === "rollback failure"
          ? "credential update could not be confirmed and rollback failed"
          : "credential update could not be confirmed",
      );
      expect(configuration.connections).toEqual([edited]);
      if (failure === "newer credential commit") {
        expect(secrets.store).toHaveBeenCalledOnce();
        expect(secrets.entries.get(config.id)).toBe(newer);
        await vi.waitFor(() =>
          expect(driver.disconnect).toHaveBeenCalledOnce(),
        );
        await expect(
          peer.getDriverForQuery(config.id, key),
        ).resolves.toBeUndefined();
        await peer.connectTo(config.id, "explicit");
        expect(factory).toHaveBeenLastCalledWith(
          expect.objectContaining(newerCredentials),
        );
        expect(sessions).toHaveBeenCalledExactlyOnceWith(config.id);
      } else {
        if (failure === "rollback failure") {
          const staged = secrets.entries.get(config.id);
          expect(JSON.parse(staged ?? "{}")).toMatchObject(
            replacementCredentials,
          );
          expect(readConnectionCredentialRevision(staged)).toBe(
            originalRevision,
          );
          expect(originDriver.disconnect).toHaveBeenCalledOnce();
        } else {
          expect(secrets.entries.get(config.id)).toBe(original);
          expect(originDriver.disconnect).not.toHaveBeenCalled();
          await expect(peer.getDriverForQuery(config.id, key)).resolves.toBe(
            driver,
          );
        }
        expect(sessions).not.toHaveBeenCalled();
        expect(driver.disconnect).not.toHaveBeenCalled();
      }
    } finally {
      configuration.afterApply = undefined;
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
  });

  it("fences automatic reconnect against newly read credentials before the peer receives the event", async () => {
    const config = sharedCredentialProfile;
    configuration.connections = [config];
    const secrets = sharedSecretStorage();
    secrets.entries.set(
      config.id,
      bindConnectionSecret(config, JSON.stringify(originalCredentials)),
    );
    const origin = new ConnectionManager(secrets.context() as never);
    const peerContext = secrets.context() as unknown as vscode.ExtensionContext;
    const peer = new ConnectionManager(peerContext);
    const driver = credentialDriver();
    const factory = credentialDriverFactory(peer).mockReturnValue(
      driver as unknown as IDBDriver,
    );
    try {
      await peer.initializeConnectionCredentialRevision(config.id);
      await rememberPreviouslyVerifiedTarget(peerContext, peer);
      const oldKey = peer.getQueryTargetSessionKey(config.id) as string;
      secrets.muteEvents();
      await origin.saveConnection({ ...config, ...replacementCredentials });
      await expect(
        peer.getDriverForQuery(config.id, oldKey),
      ).resolves.toBeUndefined();
      expect(factory).not.toHaveBeenCalled();
      await peer.disconnectFrom(config.id);
      expect(peer.getQueryTargetSessionKey(config.id)).toBeNull();
      await peer.connectTo(config.id, "explicit");
      const currentKey = peer.getQueryTargetSessionKey(config.id) as string;
      expect(currentKey).not.toBe(oldKey);
      await expect(peer.getDriverForQuery(config.id, currentKey)).resolves.toBe(
        driver,
      );
      expect(factory).toHaveBeenCalledWith(
        expect.objectContaining(replacementCredentials),
      );
    } finally {
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
  });

  it("does not broadcast a failed atomic save or a rolled-back settings+secret save", async () => {
    const config = sharedCredentialProfile;
    configuration.connections = [config];
    const secrets = sharedSecretStorage();
    const original = bindConnectionSecret(
      config,
      JSON.stringify(originalCredentials),
    );
    secrets.entries.set(config.id, original);
    const origin = new ConnectionManager(secrets.context() as never);
    const peer = new ConnectionManager(secrets.context() as never);
    const driver = credentialDriver();
    credentialDriverFactory(peer).mockReturnValue(
      driver as unknown as IDBDriver,
    );
    const sessions = vi.fn();
    peer.onDidChangeConnectionSession(sessions);
    try {
      await peer.connectTo(config.id);
      const key = peer.getQueryTargetSessionKey(config.id) as string;
      secrets.store.mockRejectedValueOnce(new Error("keychain write failed"));
      await expect(
        origin.saveConnection({ ...config, ...replacementCredentials }),
      ).rejects.toThrow("keychain write failed");
      configuration.rejectBeforeApply = true;
      await expect(
        origin.saveConnection({
          ...config,
          ...replacementCredentials,
          name: "New name",
        }),
      ).rejects.toThrow("settings write failed");
      expect(secrets.store).toHaveBeenCalledTimes(3);
      expect(configuration.connections).toEqual([config]);
      await expect(peer.getDriverForQuery(config.id, key)).resolves.toBe(
        driver,
      );
      expect(secrets.entries.get(config.id)).toBe(original);
      expect(sessions).not.toHaveBeenCalled();
      expect(driver.disconnect).not.toHaveBeenCalled();

      // A second write must not trust the cached baseline after an uncertain
      // acknowledgement, even though this test knows the first RPC failed.
      await expect(
        origin.saveConnection({
          ...config,
          ...replacementCredentials,
          awsRegion: "us-west-2",
        }),
      ).rejects.toThrow("stale");
      expect(secrets.store).toHaveBeenCalledTimes(3);
      expect(secrets.entries.get(config.id)).toBe(original);
      await expect(peer.getDriverForQuery(config.id, key)).resolves.toBe(
        driver,
      );
      expect(sessions).not.toHaveBeenCalled();
      expect(driver.disconnect).not.toHaveBeenCalled();
    } finally {
      configuration.rejectBeforeApply = false;
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
  });

  it("requires an explicit mode switch before peers reconnect without the old static credentials", async () => {
    const config = sharedCredentialProfile;
    configuration.connections = [config];
    const secrets = sharedSecretStorage();
    secrets.entries.set(
      config.id,
      bindConnectionSecret(config, JSON.stringify(originalCredentials)),
    );
    const origin = new ConnectionManager(secrets.context() as never);
    const peer = new ConnectionManager(secrets.context() as never);
    const driver = credentialDriver();
    const factory = credentialDriverFactory(peer).mockReturnValue(
      driver as unknown as IDBDriver,
    );
    try {
      await peer.connectTo(config.id);
      await expect(
        origin.saveConnection(config, { awsCredentials: true }),
      ).rejects.toThrow(/static AWS credentials/i);
      configuration.afterApply = fireConnectionsConfigurationChange;
      await origin.saveConnection(
        { ...config, awsCredentialMode: "default" },
        { awsCredentials: true },
      );
      const stored = JSON.parse(secrets.entries.get(config.id) ?? "{}");
      expect(stored).not.toHaveProperty("awsAccessKeyId");
      expect(stored).not.toHaveProperty("awsSecretAccessKey");
      await vi.waitFor(() => expect(driver.disconnect).toHaveBeenCalledOnce());
      await peer.disconnectFrom(config.id);
      await peer.connectTo(config.id, "explicit");
      expect(factory.mock.calls.at(-1)?.[0].awsAccessKeyId).toBeUndefined();
      expect(factory.mock.calls.at(-1)?.[0].awsSecretAccessKey).toBeUndefined();
      expect(peer.isConnected(config.id)).toBe(true);
    } finally {
      configuration.afterApply = undefined;
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
  });

  it("requires confirmation in an open peer query panel and rejects a stale restored table draft", async () => {
    const config = sharedCredentialProfile;
    configuration.connections = [config];
    const secrets = sharedSecretStorage();
    secrets.entries.set(
      config.id,
      bindConnectionSecret(config, JSON.stringify(originalCredentials)),
    );
    const origin = new ConnectionManager(secrets.context() as never);
    const context = secrets.context() as unknown as vscode.ExtensionContext;
    const peer = new ConnectionManager(context);
    const oldDriver = credentialDriver();
    const newDriver = credentialDriver();
    credentialDriverFactory(peer)
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(newDriver as unknown as IDBDriver);
    vi.spyOn(peer, "getQueryEditorPresentation").mockReturnValue(undefined);
    vi.spyOn(peer, "getSchemaAsync").mockResolvedValue([]);
    vi.spyOn(peer, "getDriverCapabilities").mockReturnValue({
      boundedQueryResults: true,
      tabularRead: "nosql",
    });
    try {
      await peer.connectTo(config.id);
      const oldKey = peer.getQueryTargetSessionKey(config.id) as string;
      QueryPanel.createOrShow(context, peer, config.id, undefined, true);
      const panel = vi
        .mocked(vscode.window.createWebviewPanel)
        .mock.results.at(-1)?.value;
      if (!panel) throw new Error("Missing query panel");
      const webview = panel.webview as typeof panel.webview & {
        dispatchMessage(message: unknown): Promise<void>;
      };
      await origin.saveConnection({ ...config, ...replacementCredentials });
      await vi.waitFor(() => expect(peer.getDriver(config.id)).toBeUndefined());
      await peer.disconnectFrom(config.id);
      await peer.connectTo(config.id, "explicit");
      const newKey = peer.getQueryTargetSessionKey(config.id) as string;
      expect(webview.postMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "connections",
          payload: [
            expect.objectContaining({
              id: config.id,
              targetSessionKey: newKey,
            }),
          ],
        }),
      );
      const run = (key: string) =>
        webview.dispatchMessage({
          type: "executeQuery",
          payload: {
            queryText: 'SELECT * FROM "items"',
            connectionId: config.id,
            targetSessionKey: key,
          },
        });
      await run(oldKey);
      await run(newKey);
      expect(oldDriver.query).not.toHaveBeenCalled();
      expect(newDriver.query).not.toHaveBeenCalled();
      await webview.dispatchMessage({
        type: "activeConnectionChanged",
        payload: { connectionId: config.id, targetSessionKey: newKey },
      });
      await run(newKey);
      expect(newDriver.query).toHaveBeenCalledOnce();

      const coldContext =
        secrets.context() as unknown as vscode.ExtensionContext;
      const cold = new ConnectionManager(coldContext);
      try {
        TablePanel.registerSerializer(coldContext, cold);
        const serializer = vi
          .mocked(vscode.window.registerWebviewPanelSerializer)
          .mock.calls.at(-1)?.[1];
        if (!serializer) throw new Error("Missing table serializer");
        const restored = vscode.window.createWebviewPanel(
          "rapidb.tablePanel",
          "Test",
          vscode.ViewColumn.One,
        );
        const closed = vi.fn();
        restored.onDidDispose(closed);
        await serializer.deserializeWebviewPanel(restored, {
          initialState: {
            connectionId: config.id,
            database: "us-east-1",
            schema: "",
            table: "items",
            connectionSessionKey: oldKey,
          },
          tableDraft: {},
        });
        expect(closed).toHaveBeenCalledOnce();
        expect(cold.getQueryTargetSessionKey(config.id)).toBe(newKey);
      } finally {
        await cold.dispose();
      }
    } finally {
      QueryPanel.disposeAll();
      TablePanel.disposeAll();
      await Promise.all([origin.dispose(), peer.dispose()]);
    }
  });
});

describe("ConnectionManager with VSCode SecretStorage", () => {
  it("reuses the credentials supplied to a driver for later diagnostics without rereading Secret Storage", async () => {
    const password = "synthetic-stored-password-11";
    const config: ConnectionConfig = {
      id: "diagnostic-context",
      name: "Postgres",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "postgres",
      useSecretStorage: true,
    };
    configuration.connections = [config];
    const context = createExtensionContextStub();
    await context.secrets.store(config.id, JSON.stringify({ password }));
    const reads = vi.spyOn(context.secrets, "get");
    const manager = new ConnectionManager(context as never);
    const state = manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
    };
    const connect = vi.fn(async () => {});
    const factory = vi.spyOn(state, "createDriver").mockReturnValue({
      connect,
      disconnect: async () => {},
      isConnected: () => true,
    } as unknown as IDBDriver);
    try {
      const error = new Error(`Connection refused for ${password}`);
      connect.mockRejectedValueOnce(error);
      await expect(manager.connectTo(config.id)).rejects.toThrow(
        "Connection refused for [REDACTED]",
      );
      await manager.connectTo(config.id);
      expect(factory).toHaveBeenLastCalledWith(
        expect.objectContaining({ password }),
      );
      expect(manager.getConnection(config.id)?.password).toBeUndefined();
      const driver = manager.getDriver(config.id);
      expect(driver).toBeDefined();
      await manager.disconnectFrom(config.id);
      reads.mockClear();
      // A late rejection can still be redacted using its original driver.
      expect(
        normalizeUnknownError(
          error,
          manager.getConnectionDiagnosticContext(config.id, driver),
        ).message,
      ).toBe("Connection refused for [REDACTED]");
      expect(reads).not.toHaveBeenCalled();
      expect(error.message).toContain(password);
    } finally {
      await manager.dispose();
    }
  });

  it.each([
    {
      name: "password",
      config: {
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        password: "hand-password",
      },
      expected: { password: "hand-password" },
    },
    {
      name: "API key",
      config: {
        type: "elasticsearch",
        endpoint: "https://es.example.com",
        apiKey: "hand-key",
      },
      expected: { apiKey: "hand-key" },
    },
    {
      name: "AWS credentials",
      config: {
        type: "dynamodb",
        awsRegion: "us-east-1",
        awsAccessKeyId: "hand-access",
        awsSecretAccessKey: "hand-secret",
        awsSessionToken: "hand-token",
      },
      expected: {
        awsAccessKeyId: "hand-access",
        awsSecretAccessKey: "hand-secret",
        awsSessionToken: "hand-token",
      },
    },
    {
      name: "TLS key passphrase",
      config: {
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        tls: {
          mode: "mutualTls",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
          keyPassphrase: "hand-tls",
        },
      },
      expected: { tls: expect.objectContaining({ keyPassphrase: "hand-tls" }) },
    },
    {
      name: "credential URI",
      config: {
        type: "redis",
        connectionUri: "redis://user:hand-uri@localhost:6379/0",
      },
      expected: { connectionUri: "redis://user:hand-uri@localhost:6379/0" },
    },
  ] as Array<{
    name: string;
    config: Partial<ConnectionConfig>;
    expected: Partial<ConnectionConfig>;
  }>)("keeps a hand-written $name in settings.json and marks it plaintext", async ({
    config: partial,
    expected,
  }) => {
    const config = {
      ...partial,
      id: "hand-written",
      name: "Hand written",
    } as ConnectionConfig;
    configuration.connections = [structuredClone(config)];
    configuration.writes = 0;
    const context = createExtensionContextStub();
    const stores = vi.spyOn(context.secrets, "store");
    const deletes = vi.spyOn(context.secrets, "delete");
    const manager = new ConnectionManager(context as never);
    const state = manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
      _migrateAllStoredConnectionSecrets(): Promise<void>;
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
      expect(manager.getConnections()).toEqual([
        { ...config, useSecretStorage: false },
      ]);
      await state._migrateAllStoredConnectionSecrets();
      await manager.connectTo(config.id);
      expect(factory).toHaveBeenCalledWith(expect.objectContaining(expected));
      expect(configuration.connections).toEqual([
        { ...config, useSecretStorage: false },
      ]);
      expect(configuration.writes).toBe(1);
      await state._migrateAllStoredConnectionSecrets();
      await manager.saveConnection({
        id: "other",
        name: "Other",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        password: "other",
        useSecretStorage: true,
      });
      expect(configuration.connections[0]).toEqual({
        ...config,
        useSecretStorage: false,
      });
      expect(configuration.writes).toBe(2);
      expect(stores).toHaveBeenCalledTimes(1);
      expect(stores).toHaveBeenCalledWith("other", expect.any(String));
      expect(
        JSON.parse((await context.secrets.get("other")) ?? "{}"),
      ).toMatchObject({ password: "other" });
      expect(deletes).not.toHaveBeenCalled();
      await expect(context.secrets.get(config.id)).resolves.toBeUndefined();
    } finally {
      await manager.dispose();
    }
  });

  it("still migrates legacy plaintext stored alongside useSecretStorage true", async () => {
    const config: ConnectionConfig = {
      id: "legacy-true",
      name: "Legacy",
      type: "pg",
      host: "localhost",
      password: "legacy-password",
      useSecretStorage: true,
    };
    configuration.connections = [structuredClone(config)];
    const context = createExtensionContextStub();
    const manager = new ConnectionManager(context as never);
    try {
      await (
        manager as unknown as {
          _migrateAllStoredConnectionSecrets(): Promise<void>;
        }
      )._migrateAllStoredConnectionSecrets();
      expect(configuration.connections[0].password).toBeUndefined();
      expect(configuration.connections[0].useSecretStorage).toBe(true);
      expect(
        JSON.parse((await context.secrets.get(config.id)) ?? "{}"),
      ).toMatchObject({ password: "legacy-password" });
    } finally {
      await manager.dispose();
    }
  });

  it("moves Elasticsearch URI credentials into the canonical endpoint secret on edit", async () => {
    configuration.connections = [];
    const context = createExtensionContextStub();
    const existing: ConnectionConfig = {
      id: "elastic-uri-migration",
      name: "Elastic URI",
      type: "elasticsearch",
      connectionUri: "https://elastic.local/base",
      useSecretStorage: true,
    };
    const fullEndpoint =
      "https://elastic-user:elastic-pass@elastic.local/edited?api_key=key-secret";
    configuration.connections = [existing];
    await context.secrets.store(
      existing.id,
      JSON.stringify({
        connectionUri:
          "https://elastic-user:elastic-pass@elastic.local/base?api_key=key-secret",
      }),
    );
    const manager = new ConnectionManager(context as never);
    try {
      const saved = await manager.saveConnection({
        ...existing,
        connectionUri: undefined,
        endpoint: fullEndpoint,
      });

      expect(saved.connectionUri).toBeUndefined();
      expect(saved.endpoint).toBe("https://elastic.local/edited");
      expect(configuration.connections[0].endpoint).toBe(
        "https://elastic.local/edited",
      );
      expect(configuration.connections[0].connectionUri).toBeUndefined();
      expect(
        JSON.parse((await context.secrets.get(existing.id)) ?? "{}"),
      ).toMatchObject({ endpoint: fullEndpoint });
    } finally {
      await manager.dispose();
    }
  });

  it.each([
    ["user:uri-secret@host/db", "host/db"],
    ["//user:uri-secret@host/db", "//host/db"],
    [
      "mongodb://host/db?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN%3ASESSION_SECRET&retryWrites=true",
      "mongodb://host/db?authMechanism=MONGODB-AWS&retryWrites=true",
    ],
  ])("handles legacy URI credentials safely through save and edit: %s", async (connectionUri, redacted) => {
    for (const useSecretStorage of [undefined, true]) {
      // Replace the fixture with a new revision between the subcases.
      const independentSettings = {
        id: `other-${String(useSecretStorage)}`,
        name: "Other",
        type: "sqlite" as const,
      };
      configuration.connections = [independentSettings];
      const context = createExtensionContextStub();
      const manager = new ConnectionManager(context as never);
      const config: ConnectionConfig = {
        id: "nonstandard-uri-save",
        name: "Nonstandard URI",
        type: "mongodb",
        connectionUri,
        useSecretStorage,
      };
      try {
        if (!connectionUri.startsWith("mongodb://")) {
          await expect(manager.saveConnection(config)).rejects.toThrow(
            "Enter a valid MongoDB URI",
          );
          expect(configuration.connections).toEqual([independentSettings]);
          expect(await context.secrets.get(config.id)).toBeUndefined();
          continue;
        }
        const saved = await manager.saveConnection(config);
        expect(saved.connectionUri).toBe(redacted);
        if (useSecretStorage === undefined) {
          expect(configuration.connections).toEqual([
            independentSettings,
            { ...config, useSecretStorage: false },
          ]);
          expect(await context.secrets.get(config.id)).toBeUndefined();
          continue;
        }
        expect(configuration.connections[1].connectionUri).toBe(redacted);
        expect(configuration.connections[1].useSecretStorage).toBe(true);
        expect(
          JSON.parse((await context.secrets.get(config.id)) ?? "{}"),
        ).toMatchObject({
          connectionUri,
        });
        const edited = await manager.saveConnection({
          ...saved,
          name: "Renamed",
        });
        expect(edited.connectionUri).toBe(redacted);
        expect(configuration.connections[1].connectionUri).toBe(redacted);
        expect(
          JSON.parse((await context.secrets.get(config.id)) ?? "{}"),
        ).toMatchObject({
          connectionUri,
        });
      } finally {
        await manager.dispose();
      }
    }
  });

  it.each([
    "jdbc:postgresql://user:uri-secret@host/db",
    "alice:supersecret://tail@host/db",
  ])("rejects credential-bearing nested schemes on save and legacy connect: %s", async (connectionUri) => {
    for (const useSecretStorage of [undefined, false, true]) {
      const config: ConnectionConfig = {
        id: "ambiguous-nested-scheme",
        name: "Ambiguous nested scheme",
        type: "mongodb",
        connectionUri,
        useSecretStorage,
      };
      configuration.connections = [];
      const context = createExtensionContextStub();
      const manager = new ConnectionManager(context as never);
      try {
        await expect(manager.saveConnection(config)).rejects.toThrow(
          "Ambiguous URI credentials",
        );
        expect(configuration.connections).toEqual([]);
        await expect(context.secrets.get(config.id)).resolves.toBeUndefined();
      } finally {
        await manager.dispose();
      }
      configuration.connections = [config];
      const legacyManager = new ConnectionManager(context as never);
      const state = legacyManager as unknown as {
        createDriver(config: ConnectionConfig): IDBDriver;
      };
      const factory = vi.spyOn(state, "createDriver");
      try {
        await expect(legacyManager.connectTo(config.id)).rejects.toThrow(
          "Ambiguous URI credentials",
        );
        expect(factory).not.toHaveBeenCalled();
        await vi.waitFor(() =>
          expect(configuration.connections).toEqual([
            { ...config, useSecretStorage: useSecretStorage ?? false },
          ]),
        );
        await expect(context.secrets.get(config.id)).resolves.toBeUndefined();
      } finally {
        await legacyManager.dispose();
      }
    }
  });

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
      expect(JSON.parse(raw ?? "{}")).not.toHaveProperty("future");
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
    const config: ConnectionConfig = {
      id: "keychain-hydration",
      name: "Keychain hydration",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "postgres",
      password,
      useSecretStorage: true,
    };
    const rawSecret = '{ "password": " database password ", "future": 42 }';
    configuration.connections = [config];
    await context.secrets.store(config.id, rawSecret);
    const readSecret = context.secrets.get.bind(context.secrets);
    const reads = vi.spyOn(context.secrets, "get");
    const writes = vi.spyOn(context.secrets, "store");
    const deletes = vi.spyOn(context.secrets, "delete");
    const createSshRuntime = vi.fn();
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
      // Credentials are preserved; the legacy document is only bound to its
      // target on first successful use.
      expect(JSON.parse((await readSecret(config.id)) as string)).toEqual({
        ...JSON.parse(rawSecret),
        rapidbSecretTargetV1: expect.stringMatching(/^[0-9a-f]{64}$/),
      });
    } finally {
      await manager.dispose();
    }
  });
});
