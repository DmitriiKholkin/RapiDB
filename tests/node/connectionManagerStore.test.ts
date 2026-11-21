import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StoredConnectionConfig } from "../../src/extension/connectionManagerModels";
import { QUERY_LIMIT_POLICY } from "../../src/shared/safetyContracts";
import { createProjectTempDir } from "../runtime/tempDirectories";

let storagePath: string;
beforeEach(async () => {
  storagePath = await createProjectTempDir("unit-node", "connections-");
});

const storageUri = () => ({ scheme: "file", fsPath: storagePath });
const workspaceFolders = () => [{ uri: storageUri() }];

async function createRepairStore(generatedIds: string[] = []) {
  vi.resetModules();
  const state = {
    connections: [
      { id: "duplicate", name: "First", type: "pg", host: "first.invalid" },
      { id: "duplicate", name: "Second", type: "pg", host: "second.invalid" },
    ] as StoredConnectionConfig[],
    userConnections: [] as StoredConnectionConfig[],
    target: 1,
  };
  const raw = '{ "password": " secret ", "futureField": {"value":42} }';
  const documents = new Map([["duplicate", raw]]);
  const secrets = {
    get: vi.fn(async (id: string) => documents.get(id)),
    store: vi.fn(async (id: string, value: string) => {
      documents.set(id, value);
    }),
    delete: vi.fn(async (id: string) => {
      documents.delete(id);
    }),
  };
  const update = vi.fn(
    async (_key: string, value: StoredConnectionConfig[]) => {
      state.connections = JSON.parse(JSON.stringify(value));
    },
  );
  const showWarningMessage = vi.fn(async () => undefined);
  vi.doMock("vscode", () => ({
    window: { showWarningMessage },
    env: {},
    workspace: {
      workspaceFolders: workspaceFolders(),
      getConfiguration: () => ({
        get: () => state.connections,
        inspect: () =>
          state.target === 1
            ? { globalValue: state.connections }
            : {
                workspaceValue: state.connections,
                globalValue: state.userConnections,
              },
        update,
      }),
      onDidChangeConfiguration: vi.fn(),
    },
    ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
  }));
  if (generatedIds.length > 0) {
    vi.doMock("node:crypto", async (importOriginal) => {
      const original = await importOriginal<typeof import("node:crypto")>();
      return {
        ...original,
        randomUUID: () => generatedIds.shift() ?? original.randomUUID(),
      };
    });
  }
  const { readConnectionSecret, VSCodeConnectionManagerStore } = await import(
    "../../src/extension/connectionManagerStore"
  );
  vi.doUnmock("node:crypto");
  const store = new VSCodeConnectionManagerStore({
    globalStorageUri: storageUri(),
    globalState: { get: vi.fn(), update: vi.fn() },
    secrets,
  } as never);
  return {
    store,
    state,
    documents,
    secrets,
    update,
    raw,
    showWarningMessage,
    readConnectionSecret: (id: string) =>
      readConnectionSecret(secrets as never, id),
  };
}

describe("connection repair transactions", () => {
  it.each([
    { omitted: undefined, nested: { omitted: undefined } },
    { entries: [undefined, Array(1), null, { omitted: undefined }] },
    { nonfinite: [NaN, Infinity, -Infinity], negativeZero: -0 },
    { timestamp: new Date("2026-10-05T00:00:00.000Z") },
  ])("hashes the persisted JSON representation of extra fields %#", async (extra) => {
    const { store, state } = await createRepairStore();
    state.connections = [
      { id: "json-revision", name: "JSON", type: "pg", ...extra },
    ];
    const revision = store.getConnectionsRevision();
    state.connections = JSON.parse(JSON.stringify(state.connections));
    expect(store.getConnectionsRevision()).toBe(revision);
    state.connections = [{ ...state.connections[0], name: "External update" }];
    expect(store.getConnectionsRevision()).not.toBe(revision);
  });

  it("retains secret mutations after JSON-persisted settings apply before rejecting", async () => {
    const { store, state, documents, update } = await createRepairStore();
    state.connections = [state.connections[0]];
    const rollback = vi.fn(async () => {
      await store.deleteSecret("duplicate");
    });
    update.mockImplementationOnce(async (_key, value) => {
      state.connections = JSON.parse(JSON.stringify(value));
      throw new Error("acknowledgement failed");
    });
    await expect(
      store.mutateConnections(async (current) => {
        await store.storeSecret("duplicate", "new credentials");
        return {
          connections: current.map((connection) => ({
            ...connection,
            useSecretStorage: true,
            password: undefined,
            connectionUri: undefined,
            ssh: undefined,
            tls: {
              mode: "mutualTls" as const,
              certFilePath: "/cert",
              keyFilePath: "/key",
              keyPassphrase: undefined,
            },
          })),
          result: "saved",
          rollback,
        };
      }),
    ).resolves.toBe("saved");
    expect(rollback).not.toHaveBeenCalled();
    expect(documents.get("duplicate")).toBe("new credentials");
    expect(state.connections[0]).toMatchObject({ useSecretStorage: true });
    expect(state.connections[0]).not.toHaveProperty("password");
  });

  it.each([
    "revision",
    "target",
  ])("does not mistake an external %s change after a JSON write for its own commit", async (change) => {
    const { store, state, documents, update, raw } = await createRepairStore();
    state.connections = [state.connections[0]];
    let external!: StoredConnectionConfig[];
    const rollback = vi.fn(async () => {
      await store.storeSecret("duplicate", raw);
    });
    update.mockImplementationOnce(async (_key, value) => {
      state.connections = JSON.parse(JSON.stringify(value));
      if (change === "revision") state.connections[0].name = "External";
      else state.target = 2;
      external = state.connections;
      throw new Error("acknowledgement failed");
    });
    await expect(
      store.mutateConnections(async (current) => {
        await store.storeSecret("duplicate", "new credentials");
        return {
          connections: current.map((c) => ({
            ...c,
            name: "Saved",
            password: undefined,
          })),
          result: undefined,
          rollback,
        };
      }),
    ).rejects.toThrow("acknowledgement failed");
    expect(rollback).toHaveBeenCalledOnce();
    expect(documents.get("duplicate")).toBe(raw);
    expect(state.connections).toBe(external);
    expect(update).toHaveBeenCalledOnce();
  });

  it.each([
    ["", ""],
    [" \t ", " "],
  ])("does not guess ownership of ambiguous blank IDs %j and %j", async (first, second) => {
    const { store, state, documents, secrets, raw } = await createRepairStore();
    state.connections.push(
      { id: first, name: "Invalid first", type: "pg" },
      { id: second, name: "Invalid second", type: "pg" },
    );
    documents.set(first, raw);
    documents.set(second, raw);
    const repaired = store.getConnections();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(state.connections).toEqual(repaired);
    expect(new Set(repaired.map((c) => c.id)).size).toBe(4);
    expect(documents.get("duplicate")).toBe(raw);
    for (const connection of repaired.slice(1))
      expect(documents.has(connection.id)).toBe(false);
    expect(documents.get(first)).toBe(raw);
    expect(documents.get(second)).toBe(raw);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
  });

  it.each([
    false,
    true,
  ])("retains the source and staged copy after a settings acknowledgement failure (applied=%s)", async (applied) => {
    const { store, state, documents, secrets, update, raw } =
      await createRepairStore();
    state.connections = [{ id: " ", name: "Invalid", type: "pg" }];
    documents.set(" ", raw);
    update.mockImplementationOnce(async (_key, value) => {
      expect(documents.get(" ")).toBe(raw);
      if (applied) state.connections = JSON.parse(JSON.stringify(value));
      throw new Error("settings failed");
    });
    const [repair] = store.getConnections();
    const pending = store.mutateConnections(() => ({ result: undefined }));
    if (applied) {
      await expect(pending).resolves.toBeUndefined();
      expect(state.connections).toEqual([repair]);
    } else {
      await expect(pending).rejects.toThrow("stale");
      expect(state.connections[0].id).toBe(" ");
    }
    expect(documents.get(repair.id)).toBe(raw);
    expect(documents.get(" ")).toBe(raw);
    expect(secrets.delete).not.toHaveBeenCalled();
  });

  it.each([
    { change: "revision", secretOnly: false },
    { change: "target", secretOnly: false },
    { change: "revision", secretOnly: true },
    { change: "target", secretOnly: true },
  ])("rejects a gated mutation after an external $change change (secretOnly=$secretOnly) and restores secrets", async ({
    change,
    secretOnly,
  }) => {
    const { store, state, documents, raw, update } = await createRepairStore();
    state.connections = [state.connections[0]];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const rollback = vi.fn(async () => {
      await store.storeSecret("duplicate", raw);
    });
    const pending = store.mutateConnections(async (current) => {
      await store.storeSecret("duplicate", "changed");
      await gate;
      return {
        connections: secretOnly
          ? undefined
          : current.map((c) => ({ ...c, name: "Saved" })),
        result: undefined,
        rollback,
      };
    });
    await vi.waitFor(() => expect(documents.get("duplicate")).toBe("changed"));
    if (change === "target") state.target = 2;
    else state.connections = [{ ...state.connections[0], name: "External" }];
    const external = state.connections;
    const rejected = expect(pending).rejects.toThrow("settings changed");
    release();
    await rejected;
    expect(update).not.toHaveBeenCalled();
    expect(state.connections).toBe(external);
    expect(rollback).toHaveBeenCalledOnce();
    expect(documents.get("duplicate")).toBe(raw);
  });

  it("retains a secret-only mutation without writing settings when there is no conflict", async () => {
    const { store, state, documents, update } = await createRepairStore();
    state.connections = [state.connections[0]];
    const rollback = vi.fn(async () => undefined);
    await expect(
      store.mutateConnections(async () => {
        await store.storeSecret("duplicate", "changed");
        return { result: "saved", rollback };
      }),
    ).resolves.toBe("saved");
    expect(documents.get("duplicate")).toBe("changed");
    expect(update).not.toHaveBeenCalled();
    expect(rollback).not.toHaveBeenCalled();
  });

  it.each([
    "revision",
    "target",
    "none",
  ])("checks the post-normalization snapshot before conditional save with %s change", async (change) => {
    const { store, state, update } = await createRepairStore();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    update.mockImplementationOnce(async (_key, value) => {
      state.connections = value;
      await gate;
    });
    const replacement: StoredConnectionConfig[] = [
      { id: "replacement", name: "Replacement", type: "sqlite" },
    ];
    const pending = store.saveConnectionsIfRevision(
      store.getConnectionsRevision(),
      replacement,
    );
    await vi.waitFor(() => expect(update).toHaveBeenCalledOnce());
    expect(state.connections[1].id).not.toBe("duplicate");
    if (change === "revision") {
      state.connections = [
        { id: "external", name: "External", type: "sqlite" },
      ];
    } else if (change === "target") {
      state.target = 2;
    }
    const external = state.connections;
    release();
    await expect(pending).resolves.toBe(change === "none");
    expect(update).toHaveBeenCalledTimes(change === "none" ? 2 : 1);
    if (change === "none") expect(state.connections).toEqual(replacement);
    else expect(state.connections).toBe(external);
  });

  it.each([
    "exact",
    "trim-equivalent",
  ])("does not read either original key when repairing an %s duplicate", async (kind) => {
    const { store, state, documents, raw, secrets, showWarningMessage } =
      await createRepairStore();
    const source = state.connections[1];
    if (kind === "trim-equivalent") {
      source.id = " duplicate ";
      documents.set(source.id, raw);
    }
    source.useSecretStorage = true;
    source.secretStorageProfile = "copied-profile-link";
    const repaired = store.getConnections();
    expect(repaired[0]).toEqual(state.connections[0]);
    expect(repaired[1]).toMatchObject({
      host: source.host,
      useSecretStorage: true,
    });
    expect(repaired[1]).not.toHaveProperty("secretStorageProfile");
    expect(repaired[1].id).not.toBe(source.id);
    await store.mutateConnections(() => ({ result: undefined }));
    expect(state.connections).toEqual(repaired);
    expect(secrets.get).toHaveBeenCalledExactlyOnceWith(repaired[1].id);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(documents.has(repaired[1].id)).toBe(false);
    expect(documents.get("duplicate")).toBe(raw);
    expect(documents.get(source.id)).toBe(raw);
    expect(showWarningMessage).toHaveBeenCalledExactlyOnceWith(
      expect.stringContaining("Edit Connection"),
    );
  });

  it("preserves hand-written secrets and repairs non-string IDs without reading an old key", async () => {
    const { store, state, documents, raw, secrets, showWarningMessage } =
      await createRepairStore();
    const handWritten = {
      id: "duplicate",
      name: "Hand written",
      type: "pg",
      password: "inline-password",
      ssh: { password: "inline-ssh-password" },
      extra: { untouched: true },
    };
    state.connections = [
      state.connections[0],
      handWritten,
      { id: 42, name: "Numeric", type: "pg" },
    ] as StoredConnectionConfig[];
    documents.set("42", raw);
    const repaired = store.getConnections();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(state.connections).toEqual(repaired);
    expect(repaired[1]).toEqual({
      ...handWritten,
      id: repaired[1].id,
      useSecretStorage: false,
    });
    expect(repaired[1].id).not.toBe(handWritten.id);
    expect(repaired[2].id).toMatch(/^[0-9a-f-]{36}$/);
    expect(secrets.get.mock.calls).toEqual([
      [repaired[1].id],
      [repaired[2].id],
    ]);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(documents.get("duplicate")).toBe(raw);
    expect(documents.get("42")).toBe(raw);
    expect(showWarningMessage).not.toHaveBeenCalled();
  });

  it("does not move a blank ID's secret if a different settings scope also references it", async () => {
    const { store, state, documents, secrets, raw } = await createRepairStore();
    state.target = 2;
    state.connections = [{ id: " ", name: "Workspace", type: "pg" }];
    state.userConnections = [{ id: " ", name: "User", type: "pg" }];
    documents.set(" ", raw);
    const [repaired] = store.getConnections();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(state.connections).toEqual([repaired]);
    expect(secrets.get).toHaveBeenCalledExactlyOnceWith(repaired.id);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(documents.get(" ")).toBe(raw);
  });

  it("reserves later IDs and other scopes, and never exposes or replaces an occupied generated key", async () => {
    const later = "00000000-0000-4000-8000-000000000001";
    const other = "00000000-0000-4000-8000-000000000002";
    const occupied = "00000000-0000-4000-8000-000000000003";
    const fresh = "00000000-0000-4000-8000-000000000004";
    const {
      store,
      state,
      documents,
      secrets,
      raw,
      update,
      showWarningMessage,
      readConnectionSecret,
    } = await createRepairStore([later, other, occupied, fresh]);
    state.connections.push({ id: ` ${later} `, name: "Later", type: "pg" });
    state.target = 2;
    state.userConnections = [{ id: other, name: "User", type: "pg" }];
    documents.set(occupied, raw);
    const tentative = store.getConnections();
    expect(tentative.map(({ id }) => id)).toEqual([
      "duplicate",
      occupied,
      ` ${later} `,
    ]);
    await expect(readConnectionSecret(occupied)).resolves.toBeUndefined();
    expect(update).not.toHaveBeenCalled();
    expect(showWarningMessage).not.toHaveBeenCalled();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(state.connections.map(({ id }) => id)).toEqual([
      "duplicate",
      fresh,
      ` ${later} `,
    ]);
    await expect(readConnectionSecret(occupied)).resolves.toBeUndefined();
    expect(documents.get(occupied)).toBe(raw);
    expect(documents.get("duplicate")).toBe(raw);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(showWarningMessage).toHaveBeenCalledOnce();
  });

  it("blocks a retry after an unconfirmed repair without copying duplicate credentials", async () => {
    const {
      store,
      state,
      documents,
      raw,
      update,
      secrets,
      showWarningMessage,
    } = await createRepairStore();
    update.mockRejectedValueOnce(new Error("settings unavailable"));
    const [first, second] = store.getConnections();
    await expect(
      store.mutateConnections(() => ({ result: undefined })),
    ).rejects.toThrow("stale");
    expect(store.getConnections()).toEqual([first, second]);
    expect(state.connections[1].id).toBe("duplicate");
    expect(update).toHaveBeenCalledOnce();
    expect(documents.get("duplicate")).toBe(raw);
    expect(documents.has(second.id)).toBe(false);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(showWarningMessage).not.toHaveBeenCalled();
  });

  it("allocates a fresh repair ID after a secret copy applies before rejecting", async () => {
    const { store, state, secrets, documents, update, raw } =
      await createRepairStore();
    state.connections = [{ id: "", name: "Invalid", type: "pg" }];
    documents.set("", raw);
    secrets.store.mockImplementationOnce(async (id, value) => {
      documents.set(id, value);
      throw new Error("secret write failed");
    });
    await expect(
      store.mutateConnections(() => ({ result: undefined })),
    ).rejects.toThrow("secret write failed");
    expect(update).not.toHaveBeenCalled();
    const [id] = secrets.store.mock.calls[0];
    expect(documents.get(id)).toBe(raw);
    expect(documents.get("")).toBe(raw);
    expect(store.getConnections()[0].id).not.toBe(id);
    await store.mutateConnections(() => ({ result: undefined }));
    expect(documents.get(state.connections[0].id)).toBe(raw);
    expect(secrets.delete).not.toHaveBeenCalled();
  });

  it("does not delete either secret when settings change during normalization", async () => {
    const { store, state, secrets, documents, update, raw } =
      await createRepairStore();
    state.connections = [{ id: "", name: "Invalid", type: "pg" }];
    documents.set("", raw);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    secrets.store.mockImplementationOnce(async (id, value) => {
      documents.set(id, value);
      await gate;
    });
    const id = store.getConnections()[0].id;
    await vi.waitFor(() => expect(documents.get(id)).toBe(raw));
    state.connections = [{ id: "external", name: "External", type: "sqlite" }];
    release();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(update).not.toHaveBeenCalled();
    expect(documents.get("")).toBe(raw);
    expect(documents.get(id)).toBe(raw);
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(store.getConnections()).toEqual(state.connections);
  });

  it("keeps a duplicate isolated when normalization applies before rejecting", async () => {
    const {
      store,
      state,
      documents,
      update,
      raw,
      secrets,
      showWarningMessage,
    } = await createRepairStore();
    state.connections = state.connections.map((connection) => ({
      ...connection,
      connectionUri: undefined,
    }));
    update.mockImplementationOnce(async (_key, value) => {
      state.connections = JSON.parse(JSON.stringify(value));
      throw new Error("acknowledgement failed");
    });
    const first = store.getConnections();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(store.getConnections()).toEqual(first);
    expect(documents.has(first[1].id)).toBe(false);
    expect(documents.get("duplicate")).toBe(raw);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(showWarningMessage).toHaveBeenCalledOnce();
  });

  it("does not require credentials to repair a unique invalid ID", async () => {
    const { store, state, documents, secrets, showWarningMessage } =
      await createRepairStore();
    state.connections = [{ id: "", name: "Invalid", type: "pg" }];
    const [repaired] = store.getConnections();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(state.connections).toEqual([repaired]);
    expect(documents.has(repaired.id)).toBe(false);
    expect(secrets.store).not.toHaveBeenCalled();
    expect(secrets.delete).not.toHaveBeenCalled();
    expect(showWarningMessage).not.toHaveBeenCalled();
  });

  it.each([
    "revision",
    "target",
  ])("retains both normalization keys without clobbering an external %s update after JSON persistence", async (change) => {
    const { store, state, documents, secrets, update, raw } =
      await createRepairStore();
    state.connections = [
      { id: " ", name: "Invalid", type: "pg", connectionUri: undefined },
    ];
    documents.clear();
    documents.set(" ", raw);
    let external!: StoredConnectionConfig[];
    let repairedId!: string;
    update.mockImplementationOnce(async (_key, value) => {
      state.connections = JSON.parse(JSON.stringify(value));
      repairedId = state.connections[0].id;
      if (change === "revision")
        state.connections = [{ id: "external", name: "External", type: "pg" }];
      else state.target = 2;
      external = state.connections;
      throw new Error("acknowledgement failed");
    });
    await expect(
      store.mutateConnections(() => ({ result: undefined })),
    ).rejects.toThrow("acknowledgement failed");
    expect(state.connections).toBe(external);
    expect(update).toHaveBeenCalledOnce();
    expect(documents.get(" ")).toBe(raw);
    expect(documents.get(repairedId)).toBe(raw);
    expect(secrets.delete).not.toHaveBeenCalled();
  });
});

describe("VSCodeConnectionManagerStore", () => {
  it("reads and normalizes timeout settings from the rapidb configuration", async () => {
    vi.resetModules();

    const getConfiguration = vi.fn(() => ({
      get: vi.fn((section: string, fallback?: number) => {
        switch (section) {
          case "connectionTimeoutSeconds":
            return 0.4;
          case "dbOperationTimeoutSeconds":
            return 999999;
          default:
            return fallback;
        }
      }),
      update: vi.fn(),
    }));

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration,
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    expect(store.getTimeoutSettings()).toEqual({
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutSeconds: 86400,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutMs: 86400000,
    });
    expect(getConfiguration).toHaveBeenCalledWith("rapidb");
  });

  it("caps query row limit at hard cap from safety policy", async () => {
    vi.resetModules();

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn((section: string, fallback?: number) => {
            if (section === "queryRowLimit") {
              return QUERY_LIMIT_POLICY.hardCap + 1234;
            }
            return fallback;
          }),
          update: vi.fn(),
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    expect(store.getQueryRowLimit()).toBe(QUERY_LIMIT_POLICY.hardCap);
  });

  it("enforces minimum query row limit to prevent zero-row queries", async () => {
    vi.resetModules();

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn((section: string, fallback?: number) => {
            if (section === "queryRowLimit") {
              return 0;
            }
            return fallback;
          }),
          update: vi.fn(),
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    expect(store.getQueryRowLimit()).toBe(10);
  });

  it.each([
    "abc",
    Number.NaN,
    Number.POSITIVE_INFINITY,
    null,
  ])("falls back to the defaults for a non-finite history or row limit (%s)", async (value) => {
    vi.resetModules();

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn((section: string, fallback?: unknown) =>
            section === "queryHistoryLimit" || section === "queryRowLimit"
              ? value
              : fallback,
          ),
          update: vi.fn(),
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    expect(store.getHistoryLimit()).toBe(100);
    expect(store.getQueryRowLimit()).toBe(1000);
  });

  it("reads skipTableMutationPreview from the rapidb configuration", async () => {
    vi.resetModules();

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn((section: string, fallback?: boolean) => {
            if (section === "skipTableMutationPreview") {
              return true;
            }
            return fallback;
          }),
          update: vi.fn(),
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    expect(store.getSkipTableMutationPreview()).toBe(true);
  });

  it("saves connections when revision matches current configuration", async () => {
    vi.resetModules();

    let connections = [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
      },
    ];
    const update = vi.fn(async (_key: string, value: unknown) => {
      connections = value as typeof connections;
    });
    const get = vi.fn((section: string, fallback?: unknown) => {
      if (section === "connections") {
        return connections;
      }
      return fallback;
    });

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get,
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    const revision = store.getConnectionsRevision();
    const saved = await store.saveConnectionsIfRevision(revision, [
      {
        id: "conn-1",
        name: "Updated",
        type: "pg",
        host: "localhost",
      },
    ]);

    expect(saved).toBe(true);
    expect(update).toHaveBeenCalledTimes(1);
    expect(connections).toEqual([
      {
        id: "conn-1",
        name: "Updated",
        type: "pg",
        host: "localhost",
      },
    ]);
  });

  it("rejects connection save when expected revision is stale", async () => {
    vi.resetModules();

    let connections = [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
      },
    ];
    const update = vi.fn(async (_key: string, value: unknown) => {
      connections = value as typeof connections;
    });
    const get = vi.fn((section: string, fallback?: unknown) => {
      if (section === "connections") {
        return connections;
      }
      return fallback;
    });

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get,
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    const staleRevision = store.getConnectionsRevision();
    connections = [
      {
        id: "conn-2",
        name: "Concurrent",
        type: "pg",
        host: "localhost",
      },
    ];

    const saved = await store.saveConnectionsIfRevision(staleRevision, [
      {
        id: "conn-1",
        name: "Stale write",
        type: "pg",
        host: "localhost",
      },
    ]);

    expect(saved).toBe(false);
    expect(update).not.toHaveBeenCalled();
    expect(connections).toEqual([
      {
        id: "conn-2",
        name: "Concurrent",
        type: "pg",
        host: "localhost",
      },
    ]);
  });

  it("migrates legacy ssl=true rejectUnauthorized=true to tls requireVerifyFull", async () => {
    vi.resetModules();

    const storedConnections = [
      {
        id: "conn-1",
        name: "Legacy",
        type: "pg",
        host: "localhost",
        ssl: true,
        rejectUnauthorized: true,
      },
    ];
    const update = vi.fn(async () => undefined);
    const get = vi.fn((section: string, fallback?: unknown) => {
      if (section === "connections") {
        return storedConnections;
      }
      return fallback;
    });

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get,
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    const connections = store.getConnections();

    expect(connections).toEqual([
      {
        id: "conn-1",
        name: "Legacy",
        type: "pg",
        host: "localhost",
        tls: { mode: "requireVerifyFull" },
      },
    ]);
    await vi.waitFor(() => {
      expect(update).toHaveBeenCalledWith(
        "connections",
        expect.arrayContaining([
          expect.objectContaining({
            tls: { mode: "requireVerifyFull" },
          }),
        ]),
        1,
      );
    });
  });

  it("drops the removed legacy sqliteWalMode field from stored SQLite connections", async () => {
    vi.resetModules();

    const storedConnections = [
      {
        id: "sqlite-1",
        name: "Legacy SQLite",
        type: "sqlite",
        filePath: "/tmp/legacy.sqlite",
        sqliteWalMode: "off",
      },
    ];
    const update = vi.fn(async () => undefined);
    const get = vi.fn((section: string, fallback?: unknown) => {
      if (section === "connections") {
        return storedConnections;
      }
      return fallback;
    });

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get,
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    const expected = [
      {
        id: "sqlite-1",
        name: "Legacy SQLite",
        type: "sqlite",
        filePath: "/tmp/legacy.sqlite",
      },
    ];
    expect(store.getConnections()).toStrictEqual(expected);
    await vi.waitFor(() => {
      expect(update).toHaveBeenCalledWith("connections", expected, 1);
    });
  });

  it("migrates ssl=true rejectUnauthorized=false to tls requireTrustServerCertificate", async () => {
    vi.resetModules();

    const storedConnections = [
      {
        id: "conn-2",
        name: "Legacy Trust",
        type: "mysql",
        host: "localhost",
        ssl: true,
        rejectUnauthorized: false,
      },
    ];
    const update = vi.fn(async () => undefined);
    const get = vi.fn((section: string, fallback?: unknown) => {
      if (section === "connections") {
        return storedConnections;
      }
      return fallback;
    });

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get,
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    const connections = store.getConnections();

    expect(connections).toEqual([
      {
        id: "conn-2",
        name: "Legacy Trust",
        type: "mysql",
        host: "localhost",
        tls: { mode: "requireTrustServerCertificate" },
      },
    ]);
  });

  it("migrates ssl=false to tls disabled", async () => {
    vi.resetModules();

    const storedConnections = [
      {
        id: "conn-3",
        name: "No TLS",
        type: "pg",
        host: "localhost",
        ssl: false,
      },
    ];
    const update = vi.fn(async () => undefined);
    const get = vi.fn((section: string, fallback?: unknown) => {
      if (section === "connections") {
        return storedConnections;
      }
      return fallback;
    });

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get,
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    const connections = store.getConnections();

    expect(connections).toEqual([
      {
        id: "conn-3",
        name: "No TLS",
        type: "pg",
        host: "localhost",
        tls: { mode: "disabled" },
      },
    ]);
  });

  it("does not migrate connections that already have tls", async () => {
    vi.resetModules();

    const storedConnections = [
      {
        id: "conn-4",
        name: "Modern",
        type: "pg",
        host: "localhost",
        tls: { mode: "requireVerifyCa", caFilePath: "/tmp/ca.pem" },
      },
    ];
    const update = vi.fn();
    const get = vi.fn((section: string, fallback?: unknown) => {
      if (section === "connections") {
        return storedConnections;
      }
      return fallback;
    });

    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get,
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: {
        Global: 1,
      },
    }));

    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );

    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: {
        get: vi.fn(),
        update: vi.fn(),
      },
      secrets: {
        get: vi.fn(),
        store: vi.fn(),
        delete: vi.fn(),
      },
    } as never);

    const connections = store.getConnections();

    expect(connections).toEqual([
      {
        id: "conn-4",
        name: "Modern",
        type: "pg",
        host: "localhost",
        tls: { mode: "requireVerifyCa", caFilePath: "/tmp/ca.pem" },
      },
    ]);
    expect(update).not.toHaveBeenCalled();
  });

  it("writes connection changes back to the workspace scope they came from", async () => {
    vi.resetModules();
    const workspaceConnections = [
      { id: "conn-1", name: "Workspace", type: "sqlite" },
    ];
    const update = vi.fn(async () => undefined);
    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn(() => workspaceConnections),
          inspect: vi.fn(() => ({ workspaceValue: workspaceConnections })),
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
    }));
    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );
    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: { get: vi.fn(), update: vi.fn() },
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    } as never);

    await store.saveConnectionsIfRevision(store.getConnectionsRevision(), [
      { id: "conn-1", name: "Updated", type: "sqlite" },
    ]);

    expect(update).toHaveBeenCalledWith("connections", expect.any(Array), 2);
  });

  it.each([
    {
      name: "password",
      connection: { type: "pg", host: "db", password: "hand-password" },
    },
    {
      name: "API key",
      connection: {
        type: "elasticsearch",
        endpoint: "https://es",
        apiKey: "hand-key",
      },
    },
    {
      name: "AWS credentials",
      connection: {
        type: "dynamodb",
        awsRegion: "us-east-1",
        awsAccessKeyId: "hand-access",
        awsSecretAccessKey: "hand-secret",
        awsSessionToken: "hand-token",
      },
    },
    {
      name: "SSH secrets",
      connection: {
        type: "mysql",
        host: "db",
        ssh: {
          host: "bastion",
          username: "tunnel",
          authMethod: "privateKey",
          privateKey: "hand-private-key",
          passphrase: "hand-passphrase",
        },
      },
    },
    {
      name: "SSH password",
      connection: {
        type: "pg",
        host: "db",
        ssh: {
          host: "bastion",
          username: "tunnel",
          authMethod: "password",
          password: "hand-ssh-password",
        },
      },
    },
    {
      name: "TLS key passphrase",
      connection: {
        type: "pg",
        host: "db",
        tls: {
          mode: "mutualTls",
          certFilePath: "/cert",
          keyFilePath: "/key",
          keyPassphrase: "hand-tls",
        },
      },
    },
    {
      name: "credential URI",
      connection: {
        type: "mongodb",
        connectionUri: "mongodb://user:hand-uri@db/app",
      },
    },
    {
      name: "ambiguous credential URI",
      connection: {
        type: "mongodb",
        connectionUri: "mongodb://admin:pa/ss@db/app",
      },
    },
  ])("marks hand-written $name as plaintext in the scope it came from", async ({
    connection,
  }) => {
    for (const [scope, target] of [
      ["globalValue", 1],
      ["workspaceValue", 2],
      ["workspaceFolderValue", 2],
    ] as const) {
      vi.resetModules();
      storagePath = await createProjectTempDir("unit-node", "scope-");
      const handWritten = {
        id: "hand",
        name: "Hand",
        ...connection,
      } as unknown as StoredConnectionConfig;
      const untouched = [
        { id: "flag-true", name: "T", type: "pg", useSecretStorage: true },
        {
          id: "flag-false",
          name: "F",
          type: "pg",
          password: "plain",
          useSecretStorage: false,
        },
        { id: "no-secret", name: "N", type: "elasticsearch" },
      ] as StoredConnectionConfig[];
      let connections: StoredConnectionConfig[] = [
        structuredClone(handWritten),
        ...structuredClone(untouched),
      ];
      const update = vi.fn(
        async (_key: string, value: StoredConnectionConfig[]) => {
          connections = JSON.parse(JSON.stringify(value));
        },
      );
      vi.doMock("vscode", () => ({
        env: {},
        workspace: {
          workspaceFolders: workspaceFolders(),
          getConfiguration: vi.fn(() => ({
            get: vi.fn(() => connections),
            inspect: vi.fn(() => ({ [scope]: connections })),
            update,
          })),
          onDidChangeConfiguration: vi.fn(),
        },
        ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
      }));
      const { VSCodeConnectionManagerStore } = await import(
        "../../src/extension/connectionManagerStore"
      );
      const secrets = { get: vi.fn(), store: vi.fn(), delete: vi.fn() };
      const store = new VSCodeConnectionManagerStore({
        globalStorageUri: storageUri(),
        globalState: { get: vi.fn(), update: vi.fn() },
        secrets,
      } as never);

      const expected = [
        { ...handWritten, useSecretStorage: false },
        ...untouched,
      ];
      expect(store.getConnections()).toEqual(expected);
      await vi.waitFor(() => expect(update).toHaveBeenCalledOnce());
      expect(update).toHaveBeenCalledWith("connections", expected, target);
      expect(connections).toEqual(expected);
      expect(store.getConnections()).toEqual(expected);
      await store.mutateConnections(() => ({ result: undefined }));
      expect(update).toHaveBeenCalledOnce();
      expect(secrets.store).not.toHaveBeenCalled();
      expect(secrets.delete).not.toHaveBeenCalled();
    }
  });

  it("serializes concurrent mutations so neither connection is lost", async () => {
    vi.resetModules();
    let connections: Array<{ id: string; name: string; type: "sqlite" }> = [];
    let releaseFirst!: () => void;
    const firstWriteGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let writes = 0;
    const update = vi.fn(async (_key: string, value: typeof connections) => {
      writes += 1;
      if (writes === 1) await firstWriteGate;
      connections = value;
    });
    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn(() => connections),
          inspect: vi.fn(() => ({ globalValue: connections })),
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
    }));
    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );
    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: { get: vi.fn(), update: vi.fn() },
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    } as never);

    const first = store.mutateConnections((current) => ({
      connections: [
        ...current,
        { id: "a", name: "A", type: "sqlite" as const },
      ],
      result: undefined,
    }));
    const second = store.mutateConnections((current) => ({
      connections: [
        ...current,
        { id: "b", name: "B", type: "sqlite" as const },
      ],
      result: undefined,
    }));
    releaseFirst();
    await Promise.all([first, second]);

    expect(connections.map(({ id }) => id)).toEqual(["a", "b"]);
  });

  it("repairs and persists missing and duplicate connection ids", async () => {
    vi.resetModules();
    let connections = [
      { name: "Missing", type: "sqlite" },
      { id: "duplicate", name: "First", type: "sqlite" },
      { id: "duplicate", name: "Second", type: "sqlite" },
    ] as Array<{ id?: string; name: string; type: "sqlite" }>;
    const update = vi.fn(async (_key: string, value: typeof connections) => {
      connections = value;
    });
    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn(() => connections),
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: { Global: 1 },
    }));
    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );
    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: { get: vi.fn(), update: vi.fn() },
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    } as never);

    const firstRead = store.getConnections();
    const firstIds = firstRead.map((connection) => connection.id);
    expect(new Set(firstIds).size).toBe(3);
    expect(firstIds).toContain("duplicate");
    await vi.waitFor(() => expect(update).toHaveBeenCalledTimes(1));
    expect(store.getConnections().map((connection) => connection.id)).toEqual(
      firstIds,
    );
  });

  it("does not roll back when VS Code applies a config update before rejecting", async () => {
    vi.resetModules();
    let connections = [
      { id: "conn-1", name: "Before", type: "sqlite" as const },
    ];
    const update = vi.fn(async (_key: string, value: typeof connections) => {
      connections = JSON.parse(JSON.stringify(value));
      throw new Error("acknowledgement failed");
    });
    vi.doMock("vscode", () => ({
      env: {},
      workspace: {
        workspaceFolders: workspaceFolders(),
        getConfiguration: vi.fn(() => ({
          get: vi.fn(() => connections),
          inspect: vi.fn(() => ({ globalValue: connections })),
          update,
        })),
        onDidChangeConfiguration: vi.fn(),
      },
      ConfigurationTarget: { Global: 1 },
    }));
    const { VSCodeConnectionManagerStore } = await import(
      "../../src/extension/connectionManagerStore"
    );
    const store = new VSCodeConnectionManagerStore({
      globalStorageUri: storageUri(),
      globalState: { get: vi.fn(), update: vi.fn() },
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    } as never);
    const rollback = vi.fn(async () => undefined);

    await expect(
      store.mutateConnections((current) => ({
        connections: current.map((connection) => ({
          ...connection,
          name: "After",
          connectionUri: undefined,
          ssh: undefined,
        })),
        result: "saved",
        rollback,
      })),
    ).resolves.toBe("saved");
    expect(rollback).not.toHaveBeenCalled();
    expect(connections[0]?.name).toBe("After");
  });
});
