import { describe, expect, it, vi } from "vitest";
import type {
  ConnectAttempt,
  ConnectionManager,
} from "../../src/extension/connectionManager";

vi.mock("vscode", () => ({
  ProgressLocation: { Window: 1 },
  window: {
    withProgress: vi.fn(),
  },
}));

import { connectWithProgress } from "../../src/extension/utils/connectOrchestration";

type ConnectManager = Pick<ConnectionManager, "beginConnect" | "isConnected">;

function makeManager(
  attempt: ConnectAttempt,
  isConnected: () => boolean,
): ConnectManager {
  return {
    beginConnect: vi.fn(() => attempt),
    isConnected: vi.fn(() => isConnected()),
  };
}

describe("connectWithProgress", () => {
  it("returns true immediately when the connection is already established", async () => {
    const manager = {
      beginConnect: vi.fn(),
      isConnected: vi.fn(() => true),
    } as unknown as ConnectManager;
    const withProgress = vi.fn();

    await expect(
      connectWithProgress(
        manager,
        "conn-1",
        "RapiDB: Connecting…",
        true,
        withProgress as unknown as typeof import("vscode").window.withProgress,
      ),
    ).resolves.toBe(true);

    expect(manager.isConnected).toHaveBeenCalledWith("conn-1");
    expect(manager.beginConnect).not.toHaveBeenCalled();
    expect(withProgress).not.toHaveBeenCalled();
  });

  it("returns false without waiting when a connect attempt already exists and waitForExisting is false", async () => {
    const existingAttempt: ConnectAttempt = {
      promise: new Promise<void>(() => {}),
      isNew: false,
    };
    const manager = makeManager(existingAttempt, () => false);
    const withProgress = vi.fn();

    await expect(
      connectWithProgress(
        manager,
        "conn-1",
        "RapiDB: Connecting…",
        false,
        withProgress as unknown as typeof import("vscode").window.withProgress,
      ),
    ).resolves.toBe(false);

    expect(manager.beginConnect).toHaveBeenCalledWith("conn-1");
    expect(withProgress).not.toHaveBeenCalled();
  });

  it("waits for an existing attempt when waitForExisting is true", async () => {
    let connected = false;
    let resolveAttempt!: () => void;
    const existingAttempt: ConnectAttempt = {
      promise: new Promise<void>((resolve) => {
        resolveAttempt = () => {
          connected = true;
          resolve();
        };
      }),
      isNew: false,
    };
    const manager = makeManager(existingAttempt, () => connected);
    const withProgress = vi.fn();

    const pending = connectWithProgress(
      manager,
      "conn-1",
      "RapiDB: Connecting…",
      true,
      withProgress as unknown as typeof import("vscode").window.withProgress,
    );

    expect(withProgress).not.toHaveBeenCalled();
    resolveAttempt();

    await expect(pending).resolves.toBe(true);
  });

  it("wraps only new attempts in progress UI", async () => {
    let connected = false;
    const newAttempt: ConnectAttempt = {
      promise: Promise.resolve().then(() => {
        connected = true;
      }),
      isNew: true,
    };
    const manager = makeManager(newAttempt, () => connected);
    const withProgress = vi.fn(async (_options, task) => task());

    await expect(
      connectWithProgress(
        manager,
        "conn-1",
        "RapiDB: Connecting…",
        true,
        withProgress as unknown as typeof import("vscode").window.withProgress,
      ),
    ).resolves.toBe(true);

    expect(withProgress).toHaveBeenCalledTimes(1);
    expect(withProgress).toHaveBeenCalledWith(
      expect.objectContaining({ title: "RapiDB: Connecting…" }),
      expect.any(Function),
    );
  });

  it("propagates errors from new connect attempts", async () => {
    const error = new Error("connect failed");
    const attemptPromise = Promise.reject(error);
    void attemptPromise.catch(() => {});

    const manager = makeManager(
      {
        promise: attemptPromise,
        isNew: true,
      },
      () => false,
    );
    const withProgress = vi.fn(async (_options, task) => task());

    await expect(
      connectWithProgress(
        manager,
        "conn-1",
        "RapiDB: Connecting…",
        true,
        withProgress as unknown as typeof import("vscode").window.withProgress,
      ),
    ).rejects.toThrow("connect failed");

    expect(withProgress).toHaveBeenCalledTimes(1);
  });
});