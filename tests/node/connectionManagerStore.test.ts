import { describe, expect, it, vi } from "vitest";
import type { StoredConnectionConfig } from "../../src/extension/connectionManagerModels";
import { QUERY_LIMIT_POLICY } from "../../src/shared/safetyContracts";

async function createRepairStore() {
  vi.resetModules();
  const state = {
    connections: [
      { id: "duplicate", name: "First", type: "pg" },
      { id: "duplicate", name: "Second", type: "pg" },
    ] as StoredConnectionConfig[],
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
      state.connections = value;
    },
  );
  vi.doMock("vscode", () => ({
    workspace: {
      getConfiguration: () => ({
        get: () => state.connections,
        inspect: () =>
          state.target === 1
            ? { globalValue: state.connections }
            : { workspaceValue: state.connections },
        update,
      }),
      onDidChangeConfiguration: vi.fn(),
    },
    ConfigurationTarget: { Global: 1, Workspace: 2, WorkspaceFolder: 3 },
  }));
  const { VSCodeConnectionManagerStore } = await import(
    "../../src/extension/connectionManagerStore"
  );
  const store = new VSCodeConnectionManagerStore({
    globalState: { get: vi.fn(), update: vi.fn() },
    secrets,
  } as never);
  return { store, state, documents, secrets, update, raw };
}

describe("connection repair transactions", () => {
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
    expect(state.connections).toBe(change === "none" ? replacement : external);
  });

  it("copies the exact duplicate secret before committing IDs and preserves the original", async () => {
    const { store, documents, raw, update } = await createRepairStore();
    update.mockImplementationOnce(async (_key, value) => {
      expect(documents.get(value[1].id)).toBe(raw);
    });
    const repaired = store.getConnections()[1].id;
    await vi.waitFor(() => expect(update).toHaveBeenCalledOnce());
    expect(documents.get(repaired)).toBe(raw);
    expect(documents.get("duplicate")).toBe(raw);
  });

  it.each([
    "read",
    "save",
    "delete",
    "saveConnections",
  ])("retries failed normalization with stable UI IDs on %s", async (action) => {
    const { store, state, documents, raw, update, secrets } =
      await createRepairStore();
    update.mockRejectedValueOnce(new Error("settings unavailable"));
    const first = store.getConnections();
    const id = first[1].id;
    await vi.waitFor(() => expect(secrets.delete).toHaveBeenCalledWith(id));
    expect(documents.has(id)).toBe(false);
    expect(documents.get("duplicate")).toBe(raw);
    if (action === "read") {
      expect(store.getConnections()).toEqual(first);
      await vi.waitFor(() => expect(state.connections[1].id).toBe(id));
    } else if (action === "saveConnections") {
      await store.saveConnections(
        first.map((c) => (c.id === id ? { ...c, name: "Saved" } : c)),
      );
      expect(state.connections[1]).toMatchObject({ id, name: "Saved" });
      expect(documents.get(id)).toBe(raw);
    } else {
      await store.mutateConnections(async (current) => {
        expect(current.map((c) => c.id)).toEqual(first.map((c) => c.id));
        expect(await store.getSecret(id)).toBe(raw);
        if (action === "delete") await store.deleteSecret(id);
        return {
          connections:
            action === "delete"
              ? current.filter((c) => c.id !== id)
              : current.map((c) => (c.id === id ? { ...c, name: "Saved" } : c)),
          result: undefined,
        };
      });
      expect(state.connections.find((c) => c.id === id)?.name).toBe(
        action === "delete" ? undefined : "Saved",
      );
    }
    expect(documents.get("duplicate")).toBe(raw);
  });

  it("rolls back a secret copy that applies before rejecting and retries the same ID", async () => {
    const { store, secrets, documents, update, raw } =
      await createRepairStore();
    secrets.store.mockImplementationOnce(async (id, value) => {
      documents.set(id, value);
      throw new Error("secret write failed");
    });
    const id = store.getConnections()[1].id;
    await vi.waitFor(() => expect(secrets.delete).toHaveBeenCalledWith(id));
    expect(update).not.toHaveBeenCalled();
    expect(documents.has(id)).toBe(false);
    expect(store.getConnections()[1].id).toBe(id);
    await vi.waitFor(() => expect(update).toHaveBeenCalledOnce());
    expect(documents.get(id)).toBe(raw);
  });

  it("rolls back copied secrets when settings change during normalization", async () => {
    const { store, state, secrets, documents, update, raw } =
      await createRepairStore();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    secrets.store.mockImplementationOnce(async (id, value) => {
      documents.set(id, value);
      await gate;
    });
    const id = store.getConnections()[1].id;
    await vi.waitFor(() => expect(documents.get(id)).toBe(raw));
    state.connections = [{ id: "external", name: "External", type: "sqlite" }];
    release();
    await vi.waitFor(() => expect(secrets.delete).toHaveBeenCalledWith(id));
    expect(update).not.toHaveBeenCalled();
    expect(documents.get("duplicate")).toBe(raw);
    expect(store.getConnections()).toEqual(state.connections);
  });

  it("keeps copied secrets when normalization applies before rejecting", async () => {
    const { store, state, documents, update, raw, secrets } =
      await createRepairStore();
    update.mockImplementationOnce(async (_key, value) => {
      state.connections = value;
      throw new Error("acknowledgement failed");
    });
    const first = store.getConnections();
    await store.mutateConnections(() => ({ result: undefined }));
    expect(store.getConnections()).toEqual(first);
    expect(documents.get(first[1].id)).toBe(raw);
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
      workspace: {
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
      workspace: {
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
      workspace: {
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

  it("reads skipTableMutationPreview from the rapidb configuration", async () => {
    vi.resetModules();

    vi.doMock("vscode", () => ({
      workspace: {
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
      workspace: {
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
      workspace: {
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
      workspace: {
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
      workspace: {
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
      workspace: {
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
      workspace: {
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
    // No update should be triggered
    expect(update).not.toHaveBeenCalled();
  });

  it("writes connection changes back to the workspace scope they came from", async () => {
    vi.resetModules();
    const workspaceConnections = [
      { id: "conn-1", name: "Workspace", type: "sqlite" },
    ];
    const update = vi.fn(async () => undefined);
    vi.doMock("vscode", () => ({
      workspace: {
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
      globalState: { get: vi.fn(), update: vi.fn() },
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    } as never);

    await store.saveConnections([
      { id: "conn-1", name: "Updated", type: "sqlite" },
    ]);

    expect(update).toHaveBeenCalledWith("connections", expect.any(Array), 2);
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
      workspace: {
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
      workspace: {
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
      connections = value;
      throw new Error("acknowledgement failed");
    });
    vi.doMock("vscode", () => ({
      workspace: {
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
      globalState: { get: vi.fn(), update: vi.fn() },
      secrets: { get: vi.fn(), store: vi.fn(), delete: vi.fn() },
    } as never);
    const rollback = vi.fn(async () => undefined);

    await expect(
      store.mutateConnections((current) => ({
        connections: current.map((connection) => ({
          ...connection,
          name: "After",
        })),
        result: "saved",
        rollback,
      })),
    ).resolves.toBe("saved");
    expect(rollback).not.toHaveBeenCalled();
    expect(connections[0]?.name).toBe("After");
  });
});
