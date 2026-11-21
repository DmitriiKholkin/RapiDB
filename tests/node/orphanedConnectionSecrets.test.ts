import { describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { MockEventEmitter } from "../support/mockVscode";

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

const config = {
  id: "shared-profile",
  name: "Shared",
  type: "pg",
  host: "db.invalid",
  database: "app",
  username: "reader",
  useSecretStorage: true,
} satisfies ConnectionConfig;

const raw = '{"password":"synthetic test value"}';

async function openWindows() {
  const { ConnectionManager } = await import(
    "../../src/extension/connectionManager"
  );
  const owner = new FakeConnectionManagerStore();
  const peer = new FakeConnectionManagerStore();
  owner.setConnections([config]);
  peer.setConnections([config]);
  owner.setSecret(config.id, raw);
  vi.spyOn(peer, "getSecret").mockImplementation((id) => owner.getSecret(id));
  const deleteSecret = vi.spyOn(owner, "deleteSecret");
  const main = new ConnectionManager(
    createExtensionContextStub() as never,
    owner,
  );
  const other = new ConnectionManager(
    createExtensionContextStub() as never,
    peer,
  );
  return { owner, main, other, deleteSecret };
}

describe("shared SecretStorage entries are not garbage-collected", () => {
  it("does not touch credentials before or after deleting a settings entry", async () => {
    const { owner, main, other, deleteSecret } = await openWindows();
    try {
      owner.failNextConnectionWrite(new Error("settings write failed"));
      await expect(main.removeConnection(config.id)).rejects.toThrow(
        "settings write failed",
      );
      expect(owner.getConnections()).toEqual([config]);
      await expect(main.removeConnection(config.id)).resolves.toBe(true);
      expect(owner.getConnections()).toEqual([]);
      expect(deleteSecret).not.toHaveBeenCalled();
      await expect(other._hydratePassword(config)).resolves.toMatchObject({
        password: "synthetic test value",
      });
    } finally {
      await Promise.all([main.dispose(), other.dispose()]);
    }
  });

  it("reacts to manual settings removal without resuming old cleanup on restart", async () => {
    vi.useFakeTimers();
    const { owner, main, other, deleteSecret } = await openWindows();
    const context = createExtensionContextStub();
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    await context.globalState.update("rapidb.orphanedUserConnectionSecrets", [
      config.id,
    ]);
    owner.getConnections();
    main.getConnections();
    const changed = vi.fn();
    main.onDidChangeConnections(changed);
    try {
      owner.setConnections([]);
      owner.fireConfigurationChange("rapidb.connections");
      expect(changed).toHaveBeenCalledOnce();
      expect(main.getConnections()).toEqual([]);
      await main.dispose();

      const restarted = new ConnectionManager(context as never, owner);
      try {
        await vi.advanceTimersByTimeAsync(6 * 60_000);
        expect(deleteSecret).not.toHaveBeenCalled();
        await expect(other._hydratePassword(config)).resolves.toMatchObject({
          password: "synthetic test value",
        });
      } finally {
        await restarted.dispose();
      }
    } finally {
      vi.useRealTimers();
      await Promise.all([main.dispose(), other.dispose()]);
    }
  });
});
