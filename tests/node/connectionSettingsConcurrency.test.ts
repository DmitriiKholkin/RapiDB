import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { StoredConnectionConfig } from "../../src/extension/connectionManagerModels";
import {
  MAX_EXACT_FENCED_DIGESTS,
  withConnectionSettingsLock,
} from "../../src/extension/connectionSettingsCoordinator";
import { createProjectTempDir } from "../runtime/tempDirectories";

const profile = (id: string): StoredConnectionConfig => ({
  id,
  name: id,
  type: "sqlite",
});

const OWNER_STATE_KEY = "rapidb.connectionSettingsOwner.v1";
type Scope = "globalValue" | "workspaceValue" | "workspaceFolderValue";
const targets = { Global: 1, Workspace: 2, WorkspaceFolder: 3 };
const uri = (fsPath: string, scheme = "file") => ({
  fsPath,
  scheme,
  toString: () => `${scheme}:${fsPath}`,
});

async function host(
  directory: string,
  scope: Scope,
  initial: StoredConnectionConfig[],
  update: (next: StoredConnectionConfig[]) => Promise<void>,
  options: {
    remote?: boolean;
    hostKind?: number;
    resourceScheme?: string;
    localNativeStorage?: boolean;
    workspaceFile?: string;
    profileState?: Map<string, unknown>;
  } = {},
) {
  vi.resetModules(); // separate class statics, as in independent extension hosts
  let cached = initial;
  const root = uri(directory, options.resourceScheme);
  const write = vi.fn(
    async (_key: string, next: StoredConnectionConfig[], _target: number) =>
      update(next),
  );
  const common = {
    get: () => cached,
    inspect: () => ({ [scope]: cached }),
    update: write,
  };
  vi.doMock("vscode", () => ({
    ConfigurationTarget: targets,
    ExtensionKind: { Workspace: 2 },
    UIKind: { Desktop: 1 },
    env: { uiKind: 1, remoteName: options.remote ? "ssh-remote" : undefined },
    workspace: {
      workspaceFolders: [{ uri: root }],
      workspaceFile: options.workspaceFile && uri(options.workspaceFile),
      getConfiguration: () => common,
    },
  }));
  const { VSCodeConnectionManagerStore } = await import(
    "../../src/extension/connectionManagerStore"
  );
  const secrets = { get: vi.fn(), store: vi.fn(), delete: vi.fn() };
  return {
    store: new VSCodeConnectionManagerStore({
      globalStorageUri: uri(
        directory,
        options.localNativeStorage ? "vscode-userdata" : "file",
      ),
      extension: { extensionKind: options.hostKind },
      secrets,
      globalState: options.profileState && {
        get: (key: string) => options.profileState?.get(key),
        update: async (key: string, value: unknown) => {
          options.profileState?.set(key, value);
        },
      },
    } as never),
    publish: (next: StoredConnectionConfig[]) => {
      cached = next;
    },
    write,
    secrets,
  };
}

afterEach(() => {
  vi.doUnmock("vscode");
});

describe("coordination between extension hosts", () => {
  it.each([
    ["globalValue", { localNativeStorage: true }],
    ["workspaceValue", {}],
    ["workspaceFolderValue", {}],
    ["workspaceValue", { remote: true, hostKind: 2 }],
  ] as const)("serializes %s read–modify–write and rejects stale peer caches (%j)", async (scope, options) => {
    const directory = await createProjectTempDir("unit-node", "writers-");
    let saved = [profile("initial")];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const first = await host(
      directory,
      scope,
      saved,
      async (next) => {
        await gate;
        saved = next;
        first.publish(next);
      },
      options,
    );
    const second = await host(
      directory,
      scope,
      saved,
      async (next) => {
        saved = next;
        second.publish(next);
      },
      options,
    );
    expect(first.store.constructor).not.toBe(second.store.constructor);
    const append = (id: string) =>
      vi.fn((current: StoredConnectionConfig[]) => ({
        connections: [...current, profile(id)],
        result: true,
      }));
    const writer = first.store.mutateConnections(append("a"));
    await vi.waitFor(() => expect(first.write).toHaveBeenCalledOnce());

    const appendB = append("b");
    const contender = second.store.mutateConnections(appendB);
    const rejected = expect(contender).rejects.toThrow("stale");
    expect(appendB).not.toHaveBeenCalled();
    release();
    await writer;
    await rejected;
    expect(second.write).not.toHaveBeenCalled();
    expect(saved.map(({ id }) => id)).toEqual(["initial", "a"]);
    second.publish(saved); // delayed onDidChangeConfiguration delivery
    await expect(second.store.mutateConnections(appendB)).resolves.toBe(true);
    expect(saved.map(({ id }) => id)).toEqual(["initial", "a", "b"]);
  });

  it("fences a failed acknowledgement even when the writer's own read is stale", async () => {
    const directory = await createProjectTempDir("unit-node", "unknown-write-");
    const initial = [profile("initial")];
    let saved = initial;
    const first = await host(
      directory,
      "globalValue",
      initial,
      async (next) => {
        saved = next;
        throw new Error("lost acknowledgement");
      },
    );
    const second = await host(directory, "globalValue", initial, vi.fn());
    const before = first.store.getConnectionsRevision();
    const attempted = [...initial, profile("a")];
    await expect(
      first.store.saveConnectionsIfRevision(before, attempted),
    ).rejects.toThrow("lost acknowledgement");
    expect(saved).toEqual(attempted);
    const noMutation = vi.fn(() => ({ result: undefined }));
    await expect(second.store.mutateConnections(noMutation)).rejects.toThrow(
      "stale",
    );
    second.publish(saved);
    await expect(second.store.mutateConnections(noMutation)).rejects.toThrow(
      "stale",
    );
    expect(noMutation).not.toHaveBeenCalled();
    expect(second.write).not.toHaveBeenCalled();
  });

  it("locks a multi-root workspace until the first write completes and uses the refreshed array", async () => {
    const directory = await createProjectTempDir(
      "unit-node",
      "workspace-file-",
    );
    const workspaceFile = join(directory, "project.code-workspace");
    await writeFile(workspaceFile, "{}");
    const initial = [profile("initial")];
    const change = vi.fn((current: StoredConnectionConfig[]) => ({
      connections: [...current, profile("b")],
      result: true,
    }));
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const first = await host(
      directory,
      "workspaceValue",
      initial,
      async (next) => {
        await gate;
        first.publish(next);
        second.publish(next);
      },
      { workspaceFile },
    );
    const second = await host(
      directory,
      "workspaceValue",
      initial,
      async (next) => second.publish(next),
      { workspaceFile },
    );
    const a = first.store.mutateConnections((current) => ({
      connections: [...current, profile("a")],
      result: true,
    }));
    await vi.waitFor(() => expect(first.write).toHaveBeenCalledOnce());
    const b = second.store.mutateConnections(change);
    expect(change).not.toHaveBeenCalled();
    release();
    await expect(Promise.all([a, b])).resolves.toEqual([true, true]);
    expect(change).toHaveBeenCalledWith([...initial, profile("a")]);
    expect(second.write).toHaveBeenCalledWith(
      "connections",
      [...initial, profile("a"), profile("b")],
      targets.Workspace,
    );
  });

  it("keeps older digests fenced across several writes and a new window", async () => {
    const directory = await createProjectTempDir("unit-node", "older-cache-");
    const initial = [profile("initial")];
    const first = await host(directory, "globalValue", initial, async (next) =>
      first.publish(next),
    );
    const update = (id: string) =>
      first.store.saveConnectionsIfRevision(
        first.store.getConnectionsRevision(),
        [profile(id)],
      );
    await update("A");
    await update("B");
    const cold = await host(directory, "globalValue", initial, vi.fn());
    await expect(
      cold.store.saveConnectionsIfRevision(
        cold.store.getConnectionsRevision(),
        [],
      ),
    ).rejects.toThrow("stale");
    // The writer can deliberately restore an earlier setting. Without a
    // stable profile identity, an identical digest is still ambiguous.
    await update("initial");
    await expect(
      cold.store.saveConnectionsIfRevision(
        cold.store.getConnectionsRevision(),
        [],
      ),
    ).rejects.toThrow("stale");
    expect(cold.write).not.toHaveBeenCalled();
  });

  it.each([
    ["globalValue", { localNativeStorage: true }],
    ["workspaceValue", {}],
  ] as const)("allows restoring an earlier digest (ABA) across restarts in the same %s file", async (scope, options) => {
    const directory = await createProjectTempDir("unit-node", "aba-");
    const profileState = new Map<string, unknown>();
    // A profile that has its own identity in its own profile state.
    const otherProfile = new Map<string, unknown>([
      [OWNER_STATE_KEY, "00000000-0000-4000-8000-000000000001"],
    ]);
    const initial = [profile("initial")];
    let saved = initial;
    const windows: Array<Awaited<ReturnType<typeof host>>> = [];
    const open = async (state: Map<string, unknown>) => {
      const window = await host(
        directory,
        scope,
        saved,
        async (next) => {
          saved = next;
          for (const peer of windows) peer.publish(next);
        },
        { ...options, profileState: state },
      );
      windows.push(window);
      return window;
    };
    const first = await open(profileState);
    const peer = await open(profileState);
    const toggle = (
      window: Awaited<ReturnType<typeof host>>,
      readOnly: boolean,
    ) =>
      window.store.saveConnectionsIfRevision(
        window.store.getConnectionsRevision(),
        [{ ...profile("initial"), readOnly }],
      );
    // On: A -> B, then off: B -> A (a digest that is already fenced).
    await toggle(first, true);
    await toggle(first, false);
    await toggle(first, true);
    await toggle(first, false);
    // Another window and a restarted window of the same profile/file are not
    // blocked forever.
    await expect(toggle(peer, true)).resolves.toBe(true);
    await expect(toggle(peer, false)).resolves.toBe(true);
    windows.length = 0;
    const restarted = await open(profileState);
    await expect(toggle(restarted, true)).resolves.toBe(true);
    await expect(toggle(restarted, false)).resolves.toBe(true);
    // A stale cache of an intermediate value is still rejected.
    const lagging = await host(
      directory,
      scope,
      [{ ...profile("initial"), readOnly: true }],
      vi.fn(),
      { ...options, profileState },
    );
    await expect(toggle(lagging, false)).rejects.toThrow("stale");
    expect(lagging.write).not.toHaveBeenCalled();
    if (scope === "globalValue") {
      // A different VS Code profile sharing the global storage cannot prove
      // that its identical-looking value is the restored one.
      const foreign = await host(directory, scope, saved, vi.fn(), {
        ...options,
        profileState: otherProfile,
      });
      await expect(toggle(foreign, true)).rejects.toThrow("stale");
      expect(foreign.write).not.toHaveBeenCalled();
    }
  });

  it("keeps one User owner when windows of a profile create it concurrently", async () => {
    const directory = await createProjectTempDir("unit-node", "owner-race-");
    const options = { localNativeStorage: true };
    const initial = [profile("initial")];
    let saved = initial;
    const windows: Array<Awaited<ReturnType<typeof host>>> = [];
    const open = async (state: Map<string, unknown>) => {
      const window = await host(
        directory,
        "globalValue",
        saved,
        async (next) => {
          saved = next;
          for (const peer of windows) peer.publish(next);
        },
        { ...options, profileState: state },
      );
      windows.push(window);
      return window;
    };
    const toggle = (
      window: Awaited<ReturnType<typeof host>>,
      readOnly: boolean,
    ) =>
      window.store.saveConnectionsIfRevision(
        window.store.getConnectionsRevision(),
        [{ ...profile("initial"), readOnly }],
      );
    // Each window has its own globalState cache; neither has seen the other's
    // first update yet.
    const stateA = new Map<string, unknown>();
    const stateB = new Map<string, unknown>();
    const a = await open(stateA);
    const b = await open(stateB);
    await toggle(b, true);
    await toggle(a, false);
    await toggle(b, true);
    // A restores an earlier digest, so the fence records A's owner.
    await toggle(a, false);
    expect(stateA.get(OWNER_STATE_KEY)).toBe(stateB.get(OWNER_STATE_KEY));
    const stored = JSON.parse(
      await readFile(
        join(directory, "connection-settings-v1", "user", "owner.json"),
        "utf8",
      ),
    );
    expect(stored.token).toBe(stateB.get(OWNER_STATE_KEY));
    // globalState then converges on one window's value (last writer wins),
    // and every window is reloaded.
    const converged = new Map(stateB);
    windows.length = 0;
    const restarted = await open(new Map(converged));
    const peer = await open(new Map(converged));
    await expect(toggle(restarted, true)).resolves.toBe(true);
    await expect(toggle(peer, false)).resolves.toBe(true);
    await expect(toggle(restarted, true)).resolves.toBe(true);
    // A profile with its own identity and the same restored content is not
    // trusted for the other profile's restored value.
    await toggle(restarted, false);
    const foreign = await host(directory, "globalValue", saved, vi.fn(), {
      ...options,
      profileState: new Map([
        [OWNER_STATE_KEY, "00000000-0000-4000-8000-000000000002"],
      ]),
    });
    await expect(toggle(foreign, true)).rejects.toThrow("stale");
    expect(foreign.write).not.toHaveBeenCalled();
  });

  it("fails closed when the User owner record is unreadable", async () => {
    const directory = await createProjectTempDir("unit-node", "owner-bad-");
    const user = join(directory, "connection-settings-v1", "user");
    await mkdir(user, { recursive: true });
    await writeFile(join(user, "owner.json"), "{");
    const window = await host(
      directory,
      "globalValue",
      [profile("initial")],
      vi.fn(),
      { localNativeStorage: true, profileState: new Map() },
    );
    await expect(
      window.store.saveConnectionsIfRevision(
        window.store.getConnectionsRevision(),
        [],
      ),
    ).rejects.toThrow("coordination state is invalid");
    expect(window.write).not.toHaveBeenCalled();
  });

  it.each([
    ["globalValue", { remote: true, hostKind: 2 }],
    ["workspaceValue", { remote: true, hostKind: 1 }],
    ["workspaceValue", { resourceScheme: "virtual" }],
  ] as const)("keeps unsupported %s scope read-only (%j) before secret changes", async (scope, options) => {
    const directory = await createProjectTempDir("unit-node", "unsupported-");
    const initial = [profile("initial")];
    const window = await host(directory, scope, initial, vi.fn(), options);
    const mutate = vi.fn(() => ({ connections: [], result: true }));
    expect(window.store.getConnections()).toEqual(initial);
    await expect(window.store.mutateConnections(mutate)).rejects.toThrow(
      "cannot safely edit",
    );
    expect(mutate).not.toHaveBeenCalled();
    expect(window.write).not.toHaveBeenCalled();
    expect(window.secrets.get).not.toHaveBeenCalled();
    expect(await readdir(directory)).toEqual([]);
  });

  it("does not steal a live lock at timeout and releases its own lock after failure", async () => {
    const directory = await createProjectTempDir("unit-node", "lock-timeout-");
    const lock = await plantLock(directory, { pid: process.pid });
    const mutation = vi.fn(async () => undefined);
    await expect(
      withConnectionSettingsLock(directory, "peer", false, mutation, 10),
    ).rejects.toThrow("busy");
    expect(mutation).not.toHaveBeenCalled();
    expect(await readdir(directory)).toContain("write.lock");
    await rm(lock, { recursive: true });
    await expect(
      withConnectionSettingsLock(directory, "owner", false, async () => {
        throw new Error("writer failed");
      }),
    ).rejects.toThrow("writer failed");
    await withConnectionSettingsLock(directory, "peer", false, mutation, 10);
    expect(mutation).toHaveBeenCalledOnce();
    expect(await readdir(directory)).not.toContain("write.lock");
  });

  it("recovers a lock abandoned by a process that has exited on this host", async () => {
    const directory = await createProjectTempDir("unit-node", "lock-dead-");
    const pid = await exitedPid();
    await plantLock(directory, { pid });
    const mutation = vi.fn(async () => "done");
    await expect(
      withConnectionSettingsLock(directory, "owner", false, mutation, 200),
    ).resolves.toBe("done");
    expect(mutation).toHaveBeenCalledOnce();
    expect(await readdir(directory)).not.toContain("write.lock");
  });

  it.each([
    ["another host", { host: "some-other-machine" }],
    ["an unreadable owner record", { corrupt: true }],
  ] as const)("never takes over a lock held by %s", async (_label, options) => {
    const directory = await createProjectTempDir("unit-node", "lock-foreign-");
    await plantLock(directory, { pid: await exitedPid(), ...options });
    const mutation = vi.fn(async () => undefined);
    await expect(
      withConnectionSettingsLock(directory, "owner", false, mutation, 50),
    ).rejects.toThrow("busy");
    expect(mutation).not.toHaveBeenCalled();
    expect(await readdir(join(directory, "write.lock"))).toHaveLength(1);
  });

  it("lets only one contender take over an abandoned lock at a time", async () => {
    const directory = await createProjectTempDir("unit-node", "lock-race-");
    await plantLock(directory, { pid: await exitedPid() });
    let active = 0;
    let maxActive = 0;
    const critical = async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 30));
      active -= 1;
    };
    await Promise.all(
      Array.from({ length: 6 }, () =>
        withConnectionSettingsLock(directory, "o", false, critical, 5_000),
      ),
    );
    expect(maxActive).toBe(1);
    expect(await readdir(directory)).not.toContain("write.lock");
  });

  it("bounds the stored fence without forgetting evicted digests", async () => {
    const directory = await createProjectTempDir("unit-node", "fence-bound-");
    const digest = (n: number) =>
      createHash("sha256").update(String(n)).digest("hex");
    const total = MAX_EXACT_FENCED_DIGESTS + 200;
    for (let n = 0; n < total; n += 1) {
      await withConnectionSettingsLock(directory, "o", false, async (fence) => {
        const finish = await fence.beginWrite(digest(n), digest(n + 1));
        await finish(true);
      });
    }
    const raw = await readFile(join(directory, "revisions.json"), "utf8");
    const state = JSON.parse(raw);
    expect(state.fenced).toHaveLength(MAX_EXACT_FENCED_DIGESTS);
    expect(raw.length).toBeLessThan(64 * 1024);
    await withConnectionSettingsLock(directory, "o", false, async (fence) => {
      expect(() => fence.assertFresh(digest(0))).toThrow("stale");
      expect(() => fence.assertFresh(digest(100))).toThrow("stale");
      expect(() => fence.assertFresh(digest(total))).not.toThrow();
      expect(() => fence.assertFresh(digest(-1))).not.toThrow();
    });
  });
});

async function exitedPid(): Promise<number> {
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  await new Promise((resolve) => child.once("exit", resolve));
  return child.pid as number;
}

async function plantLock(
  directory: string,
  options: { pid: number; host?: string; corrupt?: boolean },
): Promise<string> {
  const lock = join(directory, "write.lock");
  const token = randomBytes(16).toString("hex");
  await mkdir(lock, { recursive: true });
  await writeFile(
    join(lock, `owner-${token}.json`),
    options.corrupt
      ? "{"
      : JSON.stringify({
          token,
          pid: options.pid,
          host: options.host ?? hostname(),
          acquiredAt: Date.now(),
        }),
  );
  return lock;
}
