import { describe, expect, it, vi } from "vitest";

const defaultWithProgress = vi.fn();

vi.mock("vscode", () => ({
  ProgressLocation: { Window: 15 },
  window: {
    withProgress: defaultWithProgress,
  },
}));

describe("connectWithProgress", () => {
  it("short-circuits when the connection is already live", async () => {
    const { connectWithProgress } = await import(
      "../../src/extension/utils/connectOrchestration"
    );

    const result = await connectWithProgress(
      {
        isConnected: () => true,
        beginConnect: vi.fn(),
      } as never,
      "conn-1",
      "Connecting",
      false,
      vi.fn(),
    );

    expect(result).toBe(true);
  });

  it("returns false when another connect attempt already exists and the caller does not want to wait", async () => {
    const { connectWithProgress } = await import(
      "../../src/extension/utils/connectOrchestration"
    );

    const result = await connectWithProgress(
      {
        isConnected: () => false,
        beginConnect: () => ({
          isNew: false,
          promise: Promise.resolve(),
        }),
      } as never,
      "conn-1",
      "Connecting",
      false,
      vi.fn(),
    );

    expect(result).toBe(false);
  });

  it("waits for an in-flight connect when requested", async () => {
    const { connectWithProgress } = await import(
      "../../src/extension/utils/connectOrchestration"
    );

    let connected = false;
    const result = await connectWithProgress(
      {
        isConnected: () => connected,
        beginConnect: () => ({
          isNew: false,
          promise: Promise.resolve().then(() => {
            connected = true;
          }),
        }),
      } as never,
      "conn-1",
      "Connecting",
      true,
      vi.fn(),
    );

    expect(result).toBe(true);
  });

  it("shows progress for a fresh connection attempt", async () => {
    const { connectWithProgress } = await import(
      "../../src/extension/utils/connectOrchestration"
    );

    let connected = false;
    const withProgress = (async <R>(
      _options: unknown,
      runner: (
        progress: {
          report(value: { message?: string; increment?: number }): void;
        },
        token: unknown,
      ) => Promise<R>,
    ): Promise<R> => {
      return runner({ report: () => undefined }, undefined);
    }) as unknown as typeof import("../../src/extension/utils/connectOrchestration").connectWithProgress extends (
      _connectionManager: never,
      _connectionId: string,
      _title: string,
      _waitForExisting: boolean,
      withProgress: infer T,
    ) => Promise<boolean>
      ? NonNullable<T>
      : never;

    const result = await connectWithProgress(
      {
        isConnected: () => connected,
        beginConnect: () => ({
          isNew: true,
          promise: Promise.resolve().then(() => {
            connected = true;
          }),
        }),
      } as never,
      "conn-1",
      "Connecting",
      false,
      withProgress,
    );

    expect(result).toBe(true);
  });
});
