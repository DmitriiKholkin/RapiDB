import { beforeEach, describe, expect, it, vi } from "vitest";
import type * as vscode from "vscode";

const { getConfiguration, onDidChangeConfiguration, MockEventEmitter } =
  vi.hoisted(() => {
    class HoistedMockEventEmitter<T> {
      private listeners: Array<(event: T) => void> = [];

      readonly event = (listener: (event: T) => void) => {
        this.listeners.push(listener);
        return {
          dispose: () => {
            this.listeners = this.listeners.filter(
              (current) => current !== listener,
            );
          },
        };
      };

      fire(event: T): void {
        for (const listener of this.listeners) {
          listener(event);
        }
      }

      dispose(): void {
        this.listeners = [];
      }
    }

    return {
      MockEventEmitter: HoistedMockEventEmitter,
      onDidChangeConfiguration: vi.fn(
        (
          _listener: unknown,
          _thisArg?: unknown,
          subscriptions?: { push: (value: unknown) => number },
        ) => {
          const disposable = { dispose: vi.fn() };
          subscriptions?.push(disposable);
          return disposable;
        },
      ),
      getConfiguration: vi.fn(() => ({
        get: vi.fn((_key: string, defaultValue?: unknown) => defaultValue),
        update: vi.fn(),
      })),
    };
  });

vi.mock("vscode", () => ({
  ConfigurationTarget: { Global: 1 },
  EventEmitter: MockEventEmitter,
  window: {
    showWarningMessage: vi.fn(),
  },
  workspace: {
    getConfiguration,
    onDidChangeConfiguration,
  },
}));

vi.mock("../../src/extension/dbDrivers/mssql", () => ({
  MSSQLDriver: class {},
}));

vi.mock("../../src/extension/dbDrivers/mysql", () => ({
  MySQLDriver: class {},
}));

vi.mock("../../src/extension/dbDrivers/oracle", () => ({
  OracleDriver: class {},
}));

vi.mock("../../src/extension/dbDrivers/postgres", () => ({
  PostgresDriver: class {},
}));

vi.mock("../../src/extension/dbDrivers/sqlite", () => ({
  SQLiteDriver: class {},
}));

import { ConnectionManager } from "../../src/extension/connectionManager";

function makeContext(): vscode.ExtensionContext {
  return {
    globalState: {
      get: vi.fn(),
      update: vi.fn(),
    },
    secrets: {
      delete: vi.fn(),
      get: vi.fn(),
      store: vi.fn(),
    },
    subscriptions: [],
  } as unknown as vscode.ExtensionContext;
}

describe("ConnectionManager.beginConnect", () => {
  beforeEach(() => {
    getConfiguration.mockClear();
    onDidChangeConfiguration.mockClear();
  });

  it("reuses an existing pending attempt", () => {
    const manager = new ConnectionManager(makeContext());
    const existingPromise = new Promise<void>(() => {});

    (
      manager as unknown as { _connectingMap: Map<string, Promise<void>> }
    )._connectingMap.set("conn-1", existingPromise);

    const attempt = manager.beginConnect("conn-1");

    expect(attempt).toEqual({ promise: existingPromise, isNew: false });
  });

  it("returns a resolved non-new attempt when the driver is already connected", async () => {
    const manager = new ConnectionManager(makeContext());

    (
      manager as unknown as {
        driverMap: Map<string, { isConnected: () => boolean }>;
      }
    ).driverMap.set("conn-1", {
      isConnected: () => true,
    });

    const attempt = manager.beginConnect("conn-1");

    expect(attempt.isNew).toBe(false);
    await expect(attempt.promise).resolves.toBeUndefined();
    expect(
      (
        manager as unknown as { _connectingMap: Map<string, Promise<void>> }
      )._connectingMap.has("conn-1"),
    ).toBe(false);
  });
});
