import { describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/extension/connectionManagerModels";
import { connectionSessionKey } from "../../src/extension/connectionManagerStore";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

vi.mock("vscode", async () => ({
  EventEmitter: (await import("../support/mockVscode")).MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

class SessionStore extends FakeConnectionManagerStore {
  readonly nonces = new Map<string, string>();

  readConnectionSessionNonce(id: string): string | undefined {
    return this.nonces.get(id);
  }

  async writeConnectionSessionNonce(id: string, nonce: string): Promise<void> {
    this.nonces.set(id, nonce);
  }
}

const staging: ConnectionConfig = {
  id: "ddb",
  name: "Orders",
  type: "dynamodb",
  awsRegion: "us-east-1",
  awsAccessKeyId: "AKIASTAGING",
  awsSecretAccessKey: "staging-secret",
  useSecretStorage: true,
};

async function createManager(store = new SessionStore()) {
  const { ConnectionManager } = await import(
    "../../src/extension/connectionManager"
  );
  const manager = new ConnectionManager(
    createExtensionContextStub() as never,
    store,
  );
  await manager.saveConnection(staging);
  const changes: string[] = [];
  manager.onDidChangeConnectionSession((id) => changes.push(id));
  return { manager, store, changes };
}

function sessionKey(
  manager: Awaited<ReturnType<typeof createManager>>["manager"],
) {
  return connectionSessionKey(
    manager.getConnection(staging.id),
    manager.getConnectionSessionNonce(staging.id),
  );
}

describe("PNL-2 connection session generation", () => {
  it("changes when only the AWS access keys change while disconnected", async () => {
    const { manager, store, changes } = await createManager();
    const generation = manager.getConnectionSessionGeneration(staging.id);
    const key = sessionKey(manager);
    expect(typeof key).toBe("string");

    await manager.saveConnection({
      ...staging,
      awsAccessKeyId: "AKIAPROD",
      awsSecretAccessKey: "prod-secret",
    });

    expect(manager.isConnected(staging.id)).toBe(false);
    expect(changes).toEqual([staging.id]);
    expect(manager.getConnectionSessionGeneration(staging.id)).toBe(
      generation + 1,
    );
    expect(sessionKey(manager)).not.toBe(key);
    const nonce = store.nonces.get(staging.id) ?? "";
    expect(nonce).not.toContain("AKIA");
    expect(nonce).not.toContain("secret");
    await manager.dispose();
  });

  it("does not change for cosmetic edits or an unchanged save", async () => {
    const { manager, changes } = await createManager();
    const key = sessionKey(manager);

    await manager.saveConnection({ ...staging, name: "Renamed" });
    await manager.saveConnection({ ...staging, name: "Renamed" });

    expect(changes).toEqual([]);
    expect(manager.getConnectionSessionGeneration(staging.id)).toBe(0);
    expect(sessionKey(manager)).toBe(key);
    await manager.dispose();
  });

  it("changes when settings.json is edited by hand", async () => {
    const { manager, store, changes } = await createManager();
    manager.getConnections();
    store.setConnections([
      { ...store.getConnections()[0], awsRegion: "eu-west-1" },
    ]);
    store.fireConfigurationChange("rapidb.connections");

    expect(changes).toEqual([staging.id]);
    expect(manager.getConnectionSessionGeneration(staging.id)).toBe(1);
    await manager.dispose();
  });

  it("keeps the persisted session key free of secret material and stable across managers", async () => {
    const store = new SessionStore();
    const first = await createManager(store);
    await first.manager.saveConnection({
      ...staging,
      awsAccessKeyId: "AKIAPROD",
      awsSecretAccessKey: "prod-secret",
    });
    const key = sessionKey(first.manager);
    await first.manager.dispose();

    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    const reloaded = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    expect(sessionKey(reloaded)).toBe(key);
    expect(JSON.stringify([...store.nonces])).not.toMatch(/AKIA|secret/);
    await reloaded.dispose();
  });

  it("treats ambiguous URI credentials as an unknown session instead of throwing", () => {
    expect(
      connectionSessionKey({
        id: "mongo",
        name: "Mongo",
        type: "mongodb",
        mongoConnectionMode: "uri",
        connectionUri: "mongodb://user:pa/ss@db/app",
      } as ConnectionConfig),
    ).toBeNull();
    expect(connectionSessionKey(undefined)).toBeUndefined();
  });
});
