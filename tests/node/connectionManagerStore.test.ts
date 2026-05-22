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

    expect(store.getQueryRowLimit()).toBe(100);
  });
});
