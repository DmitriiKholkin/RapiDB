import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { MockEventEmitter } from "../support/mockVscode";

const drivers: HangingDriver[] = [];

class HangingDriver {
  disconnectCalls = 0;

  constructor(readonly config: { id: string }) {
    drivers.push(this);
  }

  connect(): Promise<void> {
    return new Promise(() => undefined);
  }

  async disconnect(): Promise<void> {
    this.disconnectCalls += 1;
  }

  isConnected(): boolean {
    return false;
  }
}

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10, Notification: 15 },
  window: {},
}));
vi.mock("../../src/extension/dbDrivers/postgres", () => ({
  PostgresDriver: HangingDriver,
}));

beforeEach(() => {
  drivers.splice(0, drivers.length);
  vi.resetModules();
});

describe("connect cancellation", () => {
  it("disconnectFrom aborts a connect whose driver never settles", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "10.255.255.1",
        database: "app",
        username: "postgres",
      },
    ]);
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    const connect = manager.connectTo("conn-1", "explicit");
    await vi.waitFor(() => expect(drivers).toHaveLength(1));
    expect(manager.isConnecting("conn-1")).toBe(true);

    await manager.disconnectFrom("conn-1");
    await expect(connect).resolves.toBeUndefined();

    expect(manager.isConnecting("conn-1")).toBe(false);
    expect(manager.isConnected("conn-1")).toBe(false);
    expect(drivers[0]?.disconnectCalls).toBeGreaterThanOrEqual(1);
  });

  it("makes the connect progress cancellable and cancels through disconnectFrom", async () => {
    const { connectWithProgress } = await import(
      "../../src/extension/utils/connectOrchestration"
    );
    let resolveAttempt!: () => void;
    const attempt = new Promise<void>((resolve) => {
      resolveAttempt = resolve;
    });
    const disconnectFrom = vi.fn(async () => resolveAttempt());
    let cancel: (() => void) | undefined;
    const withProgress = vi.fn(
      async (
        options: { cancellable?: boolean },
        task: (
          progress: unknown,
          token: {
            onCancellationRequested(listener: () => void): {
              dispose(): void;
            };
          },
        ) => Promise<unknown>,
      ) => {
        expect(options.cancellable).toBe(true);
        return task(undefined, {
          onCancellationRequested: (listener) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        });
      },
    );

    const result = connectWithProgress(
      {
        isConnected: () => false,
        beginConnect: () => ({ isNew: true, promise: attempt }),
        disconnectFrom,
      },
      "conn-1",
      "Connecting",
      false,
      withProgress as never,
    );
    await vi.waitFor(() => expect(cancel).toBeDefined());
    cancel?.();

    await expect(result).resolves.toBe(false);
    expect(disconnectFrom).toHaveBeenCalledWith("conn-1");
  });

  it("offers Disconnect on a connection node that is still connecting", () => {
    const manifest = JSON.parse(
      readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
    ) as {
      contributes: {
        menus: { "view/item/context": { command: string; when: string }[] };
      };
    };

    expect(
      manifest.contributes.menus["view/item/context"].some(
        (item) =>
          item.command === "rapidb.disconnect" &&
          /viewItem == connectionNode_connecting\b/.test(item.when),
      ),
    ).toBe(true);
  });
});
