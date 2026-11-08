import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionConfig,
  StoredConnectionConfig,
} from "../../src/extension/connectionManagerModels";
import type {
  ConnectionIdsByScope,
  ConnectionStoreMutation,
} from "../../src/extension/connectionManagerStore";
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

const CLEANUP_DELAY_MS = 5 * 60_000;

class SharedProfile {
  readonly secrets = new Map<string, string>();
  readonly windows = new Set<WindowStore>();
  userConnections: StoredConnectionConfig[] = [];
  pendingIds: string[] = [];
  workspaceScopedIds: string[] = [];

  setUserConnections(connections: StoredConnectionConfig[]): void {
    this.userConnections = connections.map((connection) => ({
      ...connection,
    }));
    for (const window of this.windows) {
      window.fireConfigurationChange("rapidb.connections");
    }
  }
}

function ids(connections: readonly StoredConnectionConfig[] = []): string[] {
  return connections.map((connection) => connection.id);
}

class WindowStore extends FakeConnectionManagerStore {
  private tail: Promise<void> = Promise.resolve();
  failNextMutation: Error | undefined;
  afterDeleteSecret: (() => void) | undefined;
  remoteUserConnections: StoredConnectionConfig[] | undefined;
  clearLegacyOrphanedSecretIds = vi.fn(async () => undefined);

  constructor(
    readonly profile: SharedProfile,
    public workspaceConnections?: StoredConnectionConfig[],
    readonly remote = false,
  ) {
    super();
    profile.windows.add(this);
  }

  isRemoteWindow(): boolean {
    return this.remote;
  }

  setRemoteUserConnections(connections: StoredConnectionConfig[]): void {
    this.remoteUserConnections = connections.map((connection) => ({
      ...connection,
    }));
    this.fireConfigurationChange("rapidb.connections");
  }

  private userConnections(): StoredConnectionConfig[] {
    return this.remoteUserConnections ?? this.profile.userConnections;
  }

  close(): void {
    this.profile.windows.delete(this);
  }

  setWorkspaceConnections(connections: StoredConnectionConfig[]): void {
    this.workspaceConnections = connections.map((connection) => ({
      ...connection,
    }));
    this.fireConfigurationChange("rapidb.connections");
  }

  override getConnections(): StoredConnectionConfig[] {
    return (this.workspaceConnections ?? this.userConnections()).map(
      (connection) => ({ ...connection }),
    );
  }

  override async mutateConnections<TResult>(
    mutate: (
      current: StoredConnectionConfig[],
    ) =>
      | ConnectionStoreMutation<TResult>
      | Promise<ConnectionStoreMutation<TResult>>,
  ): Promise<TResult> {
    const operation = async (): Promise<TResult> => {
      const failure = this.failNextMutation;
      this.failNextMutation = undefined;
      if (failure) throw failure;
      const mutation = await mutate(this.getConnections());
      if (mutation.connections) {
        if (this.workspaceConnections) {
          this.setWorkspaceConnections(mutation.connections);
        } else {
          this.profile.setUserConnections(mutation.connections);
        }
      }
      return mutation.result;
    };
    const run = this.tail.then(operation, operation);
    this.tail = run.then(
      () => undefined,
      () => undefined,
    );
    return await run;
  }

  override async getSecret(id: string): Promise<string | undefined> {
    return this.profile.secrets.get(id);
  }

  override async storeSecret(id: string, value: string): Promise<void> {
    this.profile.secrets.set(id, value);
  }

  override async deleteSecret(id: string): Promise<void> {
    this.profile.secrets.delete(id);
    const afterDelete = this.afterDeleteSecret;
    this.afterDeleteSecret = undefined;
    afterDelete?.();
  }

  getConnectionIdsByScope(): ConnectionIdsByScope {
    return {
      user: ids(this.userConnections()),
      workspace: ids(this.workspaceConnections),
      effective: ids(this.getConnections()),
    };
  }

  hasConnectionIdInAnyScope(id: string): boolean {
    const scopes = this.getConnectionIdsByScope();
    return [...scopes.user, ...scopes.workspace, ...scopes.effective].includes(
      id,
    );
  }

  readOrphanedSecretIds(): string[] {
    return [...this.profile.pendingIds];
  }

  async writeOrphanedSecretIds(next: string[]): Promise<void> {
    this.profile.pendingIds = [...next];
  }

  readWorkspaceScopedConnectionIds(): string[] {
    return [...this.profile.workspaceScopedIds];
  }

  async writeWorkspaceScopedConnectionIds(next: string[]): Promise<void> {
    this.profile.workspaceScopedIds = [...next];
  }
}

const config: ConnectionConfig = {
  id: "removed-in-settings",
  name: "Production",
  type: "pg",
  host: "db.internal",
  database: "app",
  username: "admin",
  useSecretStorage: true,
};
const secret = JSON.stringify({ password: "old-password" });

function createProfile(userConnections: ConnectionConfig[] = [config]) {
  const profile = new SharedProfile();
  profile.userConnections = userConnections.map((connection) => ({
    ...connection,
  }));
  profile.secrets.set(config.id, secret);
  return profile;
}

async function openWindow(
  profile: SharedProfile,
  workspaceConnections?: ConnectionConfig[],
  remote = false,
) {
  const { ConnectionManager } = await import(
    "../../src/extension/connectionManager"
  );
  const store = new WindowStore(profile, workspaceConnections, remote);
  const manager = new ConnectionManager(
    createExtensionContextStub() as never,
    store,
  );
  manager.getConnections();
  await vi.advanceTimersByTimeAsync(0);
  return {
    manager,
    store,
    async close() {
      store.close();
      await manager.dispose();
    },
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("Secret Storage cleanup for connections removed outside RapiDB", () => {
  it("deletes the secret once the connection stays removed from user settings", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const window = await openWindow(profile);
    try {
      profile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS - 1_000);
      expect(profile.secrets.get(config.id)).toBe(secret);
      expect(profile.pendingIds).toEqual([config.id]);

      await vi.advanceTimersByTimeAsync(1_000);
      await vi.waitFor(() =>
        expect(profile.secrets.get(config.id)).toBeUndefined(),
      );
      await vi.waitFor(() => expect(profile.pendingIds).toEqual([]));

      profile.setUserConnections([config]);
      const restored = await window.manager._hydratePassword(
        window.manager.getConnection(config.id) as ConnectionConfig,
      );
      expect(restored.password).not.toBe("old-password");
    } finally {
      await window.close();
    }
  });

  it("keeps the secret when the entry comes back before cleanup runs", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const window = await openWindow(profile);
    try {
      profile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(0);
      profile.setUserConnections([config]);
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(profile.secrets.get(config.id)).toBe(secret);
      expect(profile.pendingIds).toEqual([]);
    } finally {
      await window.close();
    }
  });

  it("never deletes the secret of a connection removed from workspace settings", async () => {
    vi.useFakeTimers();
    const profile = createProfile([]);
    const project = await openWindow(profile, [config]);
    const other = await openWindow(profile);
    try {
      project.store.setWorkspaceConnections([]);
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(profile.secrets.get(config.id)).toBe(secret);
      expect(profile.pendingIds).toEqual([]);
    } finally {
      await other.close();
      await project.close();
    }
  });

  it("keeps the secret while the same id lives in another workspace, open or closed", async () => {
    vi.useFakeTimers();
    const closedProfile = createProfile();
    const closedProject = await openWindow(closedProfile, [config]);
    await closedProject.close();
    const userWindow = await openWindow(closedProfile);
    try {
      closedProfile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(closedProfile.secrets.get(config.id)).toBe(secret);
      expect(closedProfile.pendingIds).toEqual([]);
    } finally {
      await userWindow.close();
    }

    const openProfile = createProfile();
    const openUserWindow = await openWindow(openProfile);
    const openProject = await openWindow(openProfile, [config]);
    try {
      openProfile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(openProfile.secrets.get(config.id)).toBe(secret);
      expect(openProfile.pendingIds).toEqual([]);
    } finally {
      await openProject.close();
      await openUserWindow.close();
    }
  });

  it("stops when another window cancelled the pending cleanup", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const window = await openWindow(profile);
    try {
      profile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(0);
      expect(profile.pendingIds).toEqual([config.id]);
      profile.pendingIds = [];
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(profile.secrets.get(config.id)).toBe(secret);
    } finally {
      await window.close();
    }
  });

  it("does not schedule a second cleanup for a connection removed through RapiDB", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const window = await openWindow(profile);
    const other = await openWindow(profile);
    try {
      await expect(window.manager.removeConnection(config.id)).resolves.toBe(
        true,
      );
      await vi.advanceTimersByTimeAsync(0);
      expect(profile.secrets.get(config.id)).toBeUndefined();
      expect(profile.userConnections).toEqual([]);
      expect(profile.pendingIds).toEqual([]);
    } finally {
      await other.close();
      await window.close();
    }
  });

  it("retries the cleanup when the settings write queue fails", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const window = await openWindow(profile);
    try {
      profile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(0);
      window.store.failNextMutation = new Error(
        "Connection settings changed during the operation. Please retry.",
      );
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS);
      expect(profile.secrets.get(config.id)).toBe(secret);
      expect(profile.pendingIds).toEqual([config.id]);

      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS);
      await vi.waitFor(() =>
        expect(profile.secrets.get(config.id)).toBeUndefined(),
      );
      await vi.waitFor(() => expect(profile.pendingIds).toEqual([]));
    } finally {
      await window.close();
    }
  });

  it("resumes a pending user-scope cleanup in a later window unless that window uses the id", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const first = await openWindow(profile);
    profile.setUserConnections([]);
    await vi.advanceTimersByTimeAsync(0);
    await first.close();
    await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
    expect(profile.secrets.get(config.id)).toBe(secret);
    expect(profile.pendingIds).toEqual([config.id]);

    const project = await openWindow(profile, [config]);
    try {
      expect(profile.pendingIds).toEqual([]);
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(profile.secrets.get(config.id)).toBe(secret);
    } finally {
      await project.close();
    }

    const resumedProfile = createProfile();
    const remover = await openWindow(resumedProfile);
    resumedProfile.setUserConnections([]);
    await vi.advanceTimersByTimeAsync(0);
    await remover.close();
    const later = await openWindow(resumedProfile);
    try {
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS);
      await vi.waitFor(() =>
        expect(resumedProfile.secrets.get(config.id)).toBeUndefined(),
      );
      await vi.waitFor(() => expect(resumedProfile.pendingIds).toEqual([]));
    } finally {
      await later.close();
    }
  });

  it("never schedules cleanup from a remote window whose user scope merges remote settings", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const remote = await openWindow(profile, undefined, true);
    try {
      remote.store.setRemoteUserConnections([
        { ...config, id: "remote-only", name: "Remote" },
      ]);
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(profile.secrets.get(config.id)).toBe(secret);
      expect(profile.pendingIds).toEqual([]);
    } finally {
      await remote.close();
    }
  });

  it("does not resume a pending cleanup in a remote window", async () => {
    vi.useFakeTimers();
    const profile = createProfile([]);
    profile.pendingIds = [config.id];
    const remote = await openWindow(profile, undefined, true);
    try {
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS * 2);
      expect(profile.secrets.get(config.id)).toBe(secret);
      expect(profile.pendingIds).toEqual([config.id]);
    } finally {
      await remote.close();
    }
  });

  it("restores the secret when the entry comes back while it is being deleted", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const window = await openWindow(profile);
    try {
      profile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(0);
      window.store.afterDeleteSecret = () => {
        profile.userConnections = [{ ...config }];
      };
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS);
      await vi.waitFor(() =>
        expect(window.store.afterDeleteSecret).toBeUndefined(),
      );
      await vi.waitFor(() =>
        expect(profile.secrets.get(config.id)).toBe(secret),
      );
    } finally {
      await window.close();
    }
  });

  it("does not overwrite a secret another window stored while this one was deleting", async () => {
    vi.useFakeTimers();
    const profile = createProfile();
    const window = await openWindow(profile);
    const newSecret = JSON.stringify({ password: "new-password" });
    try {
      profile.setUserConnections([]);
      await vi.advanceTimersByTimeAsync(0);
      window.store.afterDeleteSecret = () => {
        profile.userConnections = [{ ...config }];
        profile.secrets.set(config.id, newSecret);
      };
      await vi.advanceTimersByTimeAsync(CLEANUP_DELAY_MS);
      await vi.waitFor(() =>
        expect(window.store.afterDeleteSecret).toBeUndefined(),
      );
      await vi.advanceTimersByTimeAsync(0);
      expect(profile.secrets.get(config.id)).toBe(newSecret);
    } finally {
      await window.close();
    }
  });

  it("clears the obsolete round-one pending list at startup", async () => {
    vi.useFakeTimers();
    const window = await openWindow(createProfile());
    try {
      expect(window.store.clearLegacyOrphanedSecretIds).toHaveBeenCalledOnce();
    } finally {
      await window.close();
    }
  });
});

describe("VSCodeConnectionManagerStore connection scopes", () => {
  it("separates user ids from workspace and every workspace folder id", async () => {
    vi.resetModules();
    const folderUri = { fsPath: "/folder-b" };
    const scopes = {
      effective: [{ id: "effective" }] as StoredConnectionConfig[],
      globalValue: [{ id: " global " }] as StoredConnectionConfig[],
      workspaceValue: [{ id: "workspace" }] as StoredConnectionConfig[],
      folderValue: [{ id: "folder" }] as StoredConnectionConfig[],
    };
    vi.doMock("vscode", () => ({
      workspace: {
        workspaceFolders: [
          { uri: { fsPath: "/folder-a" } },
          { uri: folderUri },
        ],
        getConfiguration: (_section: string, resource?: unknown) => ({
          get: () => scopes.effective,
          inspect: () =>
            resource === folderUri
              ? { workspaceFolderValue: scopes.folderValue }
              : {
                  globalValue: scopes.globalValue,
                  workspaceValue: scopes.workspaceValue,
                },
        }),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
    }));
    try {
      const { VSCodeConnectionManagerStore } = await import(
        "../../src/extension/connectionManagerStore"
      );
      const state = new Map<string, unknown>();
      const store = new VSCodeConnectionManagerStore({
        globalState: {
          get: (key: string) => state.get(key),
          update: async (key: string, value: unknown) => {
            state.set(key, value);
          },
        },
        secrets: {},
      } as never);
      expect(store.getConnectionIdsByScope()).toEqual({
        user: [" global "],
        workspace: ["workspace", "folder"],
        effective: ["effective"],
      });
      for (const id of ["effective", "global", "workspace", "folder"]) {
        expect(store.hasConnectionIdInAnyScope(id)).toBe(true);
      }
      expect(store.hasConnectionIdInAnyScope("missing")).toBe(false);

      await store.writeWorkspaceScopedConnectionIds(["workspace"]);
      await store.writeOrphanedSecretIds(["gone"]);
      expect(store.readWorkspaceScopedConnectionIds()).toEqual(["workspace"]);
      expect(store.readOrphanedSecretIds()).toEqual(["gone"]);
    } finally {
      vi.doUnmock("vscode");
      vi.resetModules();
    }
  });

  it("detects remote windows and clears the obsolete round-one pending list", async () => {
    vi.resetModules();
    const env: { remoteName?: string } = { remoteName: "ssh-remote" };
    vi.doMock("vscode", () => ({
      env,
      workspace: {
        getConfiguration: () => ({ get: () => [] }),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
    }));
    try {
      const { VSCodeConnectionManagerStore } = await import(
        "../../src/extension/connectionManagerStore"
      );
      const state = new Map<string, unknown>([
        ["rapidb.orphanedConnectionSecrets", ["legacy"]],
      ]);
      const update = vi.fn(async (key: string, value: unknown) => {
        if (value === undefined) state.delete(key);
        else state.set(key, value);
      });
      const store = new VSCodeConnectionManagerStore({
        globalState: { get: (key: string) => state.get(key), update },
        secrets: {},
      } as never);
      expect(store.isRemoteWindow()).toBe(true);
      env.remoteName = undefined;
      expect(store.isRemoteWindow()).toBe(false);

      await store.clearLegacyOrphanedSecretIds();
      expect(state.has("rapidb.orphanedConnectionSecrets")).toBe(false);
      await store.clearLegacyOrphanedSecretIds();
      expect(update).toHaveBeenCalledOnce();
    } finally {
      vi.doUnmock("vscode");
      vi.resetModules();
    }
  });

  it("reports no user-scope ids when the settings cannot be inspected", async () => {
    vi.resetModules();
    vi.doMock("vscode", () => ({
      workspace: {
        getConfiguration: () => ({ get: () => [{ id: "effective" }] }),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
    }));
    try {
      const { VSCodeConnectionManagerStore } = await import(
        "../../src/extension/connectionManagerStore"
      );
      const store = new VSCodeConnectionManagerStore({
        globalState: { get: vi.fn(), update: vi.fn() },
        secrets: {},
      } as never);
      expect(store.getConnectionIdsByScope()).toEqual({
        user: [],
        workspace: [],
        effective: ["effective"],
      });
    } finally {
      vi.doUnmock("vscode");
      vi.resetModules();
    }
  });
});
