import { afterEach, describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
} from "../../src/extension/dbDrivers/timeout";

const harness = vi.hoisted(() => ({ createPool: vi.fn() }));
vi.mock("oracledb", () => ({ default: { createPool: harness.createPool } }));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

function createPool() {
  const connection = {
    callTimeout: 0,
    ping: vi.fn(async (): Promise<void> => undefined),
    close: vi.fn(async (): Promise<void> => undefined),
  };
  const pool = {
    connectionsOpen: 1,
    connectionsInUse: 0,
    getConnection: vi.fn(async () => connection),
    close: vi.fn(async (): Promise<void> => undefined),
  };
  harness.createPool.mockResolvedValueOnce(pool);
  return { pool, connection };
}

function createDriver() {
  return new OracleDriver({ id: "oracle", name: "Oracle", type: "oracle" });
}

async function flushMicrotasks() {
  for (let i = 0; i < 30; i++) await Promise.resolve();
}

afterEach(() => {
  harness.createPool.mockReset();
  vi.useRealTimers();
});

describe("Oracle pending connection lifecycle", () => {
  it("disconnect before pool creation prevents a late connect", async () => {
    const driver = createDriver();
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow(/cancelled/i);
    await driver.disconnect();
    await rejected;
    expect(harness.createPool).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(false);
  });

  it("can cancel a published attempt once without cancelling its replacement", async () => {
    const old = createPool();
    const driver = createDriver();
    const attempt = driver.connect();
    await attempt;
    await driver.cancelConnectionAttempt(attempt);
    expect(driver.isConnected()).toBe(false);
    const current = createPool();
    await driver.connect();
    await driver.cancelConnectionAttempt(attempt);
    expect(driver.isConnected()).toBe(true);
    expect(old.pool.close).toHaveBeenCalledExactlyOnceWith(0);
    expect(current.pool.close).not.toHaveBeenCalled();
    await driver.disconnect();
  });

  it("deduplicates concurrent pool creation and probes", async () => {
    const { pool } = createPool();
    const driver = createDriver();
    const first = driver.connect();
    const second = driver.connect();
    await flushMicrotasks();
    expect(harness.createPool).toHaveBeenCalledOnce();
    expect(second).toBe(first);
    await first;
    await driver.disconnect();
    expect(pool.close).toHaveBeenCalledExactlyOnceWith(0);
  });

  it.each([
    "createPool",
    "getConnection",
    "ping",
    "release",
  ] as const)("disconnect fences pending %s and late cleanup cannot close a new pool", async (stage) => {
    const old = createPool();
    const creation = deferred<typeof old.pool>();
    const acquisition = deferred<typeof old.connection>();
    const probe = deferred<void>();
    const release = deferred<void>();
    if (stage === "createPool") {
      harness.createPool.mockReset().mockReturnValueOnce(creation.promise);
    } else if (stage === "getConnection") {
      old.pool.getConnection.mockReturnValue(acquisition.promise);
    } else if (stage === "ping") {
      old.connection.ping.mockReturnValue(probe.promise);
    } else {
      old.connection.close.mockReturnValue(release.promise);
    }
    const driver = createDriver();
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow(/cancelled/i);
    await flushMicrotasks();
    await driver.disconnect();
    if (stage !== "createPool") expect(old.pool.close).toHaveBeenCalledOnce();
    const current = createPool();
    await driver.connect();
    creation.resolve(old.pool);
    acquisition.resolve(old.connection);
    probe.resolve(undefined);
    release.resolve(undefined);
    await rejected;
    await driver.cancelConnectionAttempt(pending);
    expect(driver.isConnected()).toBe(true);
    expect(current.pool.close).not.toHaveBeenCalled();
    expect(old.pool.close).toHaveBeenCalledExactlyOnceWith(0);
    expect(old.connection.close).toHaveBeenCalledTimes(
      stage === "createPool" ? 0 : 1,
    );
    if (stage === "getConnection")
      expect(old.connection.ping).not.toHaveBeenCalled();
    await driver.disconnect();
    expect(current.pool.close).toHaveBeenCalledOnce();
  });

  it.each([
    "getConnection",
    "ping",
  ] as const)("timeout closes the pool while %s never settles, and late failure preserves reconnect", async (stage) => {
    vi.useFakeTimers();
    const old = createPool();
    const stalled = deferred<never>();
    if (stage === "getConnection")
      old.pool.getConnection.mockReturnValue(stalled.promise);
    else old.connection.ping.mockReturnValue(stalled.promise);
    const driver = createDriver();
    const wrapped = createTimeoutAwareDriver(driver, () =>
      createDriverTimeoutSettingsSnapshot({ connectionTimeoutSeconds: 1 }),
    );
    const rejected = expect(wrapped.connect()).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(1000);
    await rejected;
    expect(old.pool.close).toHaveBeenCalledExactlyOnceWith(0);
    const current = createPool();
    await wrapped.connect();
    stalled.reject(new Error("cancelled socket"));
    await flushMicrotasks();
    expect(driver.isConnected()).toBe(true);
    expect(current.pool.close).not.toHaveBeenCalled();
    expect(old.pool.close).toHaveBeenCalledOnce();
    await driver.disconnect();
  });

  it("wrapper eventually cleans a successful late pool creation without reconnect", async () => {
    vi.useFakeTimers();
    const { pool } = createPool();
    const creation = deferred<typeof pool>();
    harness.createPool.mockReset().mockReturnValueOnce(creation.promise);
    const driver = createDriver();
    const wrapped = createTimeoutAwareDriver(driver, () =>
      createDriverTimeoutSettingsSnapshot({ connectionTimeoutSeconds: 1 }),
    );
    const rejected = expect(wrapped.connect()).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(1000);
    await rejected;
    creation.resolve(pool);
    await flushMicrotasks();
    expect(driver.isConnected()).toBe(false);
    expect(pool.close).toHaveBeenCalledExactlyOnceWith(0);
  });

  it("disconnect detaches a published pool before awaiting its close", async () => {
    const old = createPool();
    const driver = createDriver();
    await driver.connect();
    const closing = deferred<void>();
    old.pool.close.mockReturnValueOnce(closing.promise);
    const disconnect = driver.disconnect();
    const current = createPool();
    await driver.connect();
    closing.resolve(undefined);
    await disconnect;
    expect(driver.isConnected()).toBe(true);
    expect(old.pool.close).toHaveBeenCalledOnce();
    expect(current.pool.close).not.toHaveBeenCalled();
    await driver.disconnect();
  });
});
