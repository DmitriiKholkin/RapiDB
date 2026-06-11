import { describe, expect, it, vi } from "vitest";
import { QUERY_LIMIT_POLICY } from "../../src/shared/safetyContracts";

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
    // Should persist the migration back
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
});
