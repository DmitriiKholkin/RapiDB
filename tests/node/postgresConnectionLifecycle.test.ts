import { afterEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";

const harness = vi.hoisted(() => ({
  pools: [] as Array<{
    on: ReturnType<typeof vi.fn>;
    connect: ReturnType<typeof vi.fn>;
    end: ReturnType<typeof vi.fn>;
  }>,
}));

vi.mock("pg", async (importOriginal) => {
  const actual = await importOriginal<typeof import("pg")>();
  class MockPool {
    constructor() {
      const pool = harness.pools.shift();
      if (!pool) throw new Error("Unexpected PostgreSQL pool creation");
      // biome-ignore lint/correctness/noConstructorReturn: a pg Pool test double
      return pool as never;
    }
  }
  return { ...actual, Pool: MockPool };
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

function createPool(name = "main") {
  const client = {
    query: vi.fn(async () => ({ rows: [{ name }] })),
    release: vi.fn(),
  };
  const pool = {
    on: vi.fn(),
    connect: vi.fn(async () => client),
    end: vi.fn(async (): Promise<void> => undefined),
  };
  harness.pools.push(pool);
  return { pool, client };
}

const drivers: PostgresDriver[] = [];
function createDriver() {
  const driver = new PostgresDriver({
    id: "lifecycle",
    name: "PG",
    type: "pg",
    database: "main",
  });
  drivers.push(driver);
  return driver;
}

async function flushMicrotasks() {
  for (let i = 0; i < 20; i++) await Promise.resolve();
}

afterEach(async () => {
  await Promise.all(
    drivers
      .splice(0)
      .map((driver) => driver.disconnect().catch(() => undefined)),
  );
  harness.pools.length = 0;
  vi.useRealTimers();
});

describe("B01 PostgreSQL pending connection lifecycle", () => {
  it.each([
    "connect",
    "probe",
  ] as const)("deduplicates parallel connect calls while %s is pending", async (stage) => {
    const { pool, client } = createPool();
    const acquisition = deferred<typeof client>();
    const probe = deferred<{ rows: Array<{ name: string }> }>();
    if (stage === "connect") pool.connect.mockReturnValue(acquisition.promise);
    else client.query.mockReturnValue(probe.promise);
    const driver = createDriver();
    const first = driver.connect();
    const second = driver.connect();
    expect(second).toBe(first);
    await flushMicrotasks();
    expect(pool.connect).toHaveBeenCalledOnce();
    expect(driver.isConnected()).toBe(false);
    acquisition.resolve(client);
    probe.resolve({ rows: [{ name: "main" }] });
    await Promise.all([first, second]);
    expect(driver.isConnected()).toBe(true);
    await driver.disconnect();
    expect(pool.end).toHaveBeenCalledOnce();
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
  });

  it("disconnect invalidates a connect before pool creation", async () => {
    const { pool } = createPool();
    const driver = createDriver();
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow(
      "connection attempt cancelled",
    );
    await driver.disconnect();
    await rejected;
    expect(pool.connect).not.toHaveBeenCalled();
    expect(pool.end).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(false);
  });

  it.each([
    "connect",
    "probe",
  ] as const)("disconnect closes a pending %s pool and fences its late success", async (stage) => {
    const { pool, client } = createPool();
    const acquisition = deferred<typeof client>();
    const probe = deferred<{ rows: Array<{ name: string }> }>();
    if (stage === "connect") pool.connect.mockReturnValue(acquisition.promise);
    else client.query.mockReturnValue(probe.promise);
    const driver = createDriver();
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow(
      "connection attempt cancelled",
    );
    await flushMicrotasks();
    expect(pool.connect).toHaveBeenCalledOnce();
    expect(client.query).toHaveBeenCalledTimes(stage === "probe" ? 1 : 0);
    await driver.disconnect();
    expect(pool.end).toHaveBeenCalledOnce();
    acquisition.resolve(client);
    probe.resolve({ rows: [{ name: "stale_database" }] });
    await rejected;
    expect(driver.isConnected()).toBe(false);
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(pool.end).toHaveBeenCalledOnce();
    if (stage === "connect") expect(client.query).not.toHaveBeenCalled();
    await expect(
      driver.describeTable("stale_database", "public", "items"),
    ).rejects.toThrow("not open");
  });

  it("initiates pending pool cleanup even when end waits for the probe client to be released", async () => {
    const { pool, client } = createPool();
    const probe = deferred<{ rows: Array<{ name: string }> }>();
    const ended = deferred<void>();
    client.query.mockReturnValue(probe.promise);
    pool.end.mockReturnValue(ended.promise);
    client.release.mockImplementation(() => ended.resolve(undefined));
    const driver = createDriver();
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow(
      "connection attempt cancelled",
    );
    await flushMicrotasks();
    const disconnect = driver.disconnect();
    await flushMicrotasks();
    expect(pool.end).toHaveBeenCalledOnce();
    expect(driver.isConnected()).toBe(false);
    probe.resolve({ rows: [{ name: "late" }] });
    await Promise.all([disconnect, rejected]);
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(pool.end).toHaveBeenCalledOnce();
  });

  it.each([
    "success",
    "failure",
  ] as const)("late %s from a stale probe cannot reset or close a newer connection", async (settlement) => {
    const old = createPool("old");
    const probe = deferred<{ rows: Array<{ name: string }> }>();
    old.client.query.mockReturnValue(probe.promise);
    const driver = createDriver();
    const stale = driver.connect();
    const rejected = expect(stale).rejects.toThrow(
      settlement === "success"
        ? "connection attempt cancelled"
        : "old probe failed",
    );
    await flushMicrotasks();
    await driver.disconnect();
    const fresh = createPool("new");
    await driver.connect();
    if (settlement === "success") probe.resolve({ rows: [{ name: "old" }] });
    else probe.reject(new Error("old probe failed"));
    await rejected;
    expect(driver.isConnected()).toBe(true);
    expect(fresh.pool.end).not.toHaveBeenCalled();
    // Old pool error callbacks also must not mutate the new connection state.
    old.pool.on.mock.calls[0][1](new Error("old idle socket error"));
    expect(driver.isConnected()).toBe(true);
    await driver.disconnect();
    expect([
      old.pool.end.mock.calls.length,
      fresh.pool.end.mock.calls.length,
    ]).toEqual([1, 1]);
  });

  it("disconnect invalidates a reconnect waiting for old-pool cleanup, before a new pool is created", async () => {
    const old = createPool();
    const driver = createDriver();
    await driver.connect();
    const ended = deferred<void>();
    old.pool.end.mockReturnValue(ended.promise);
    const reconnect = driver.connect();
    const rejected = expect(reconnect).rejects.toThrow(
      "connection attempt cancelled",
    );
    await flushMicrotasks();
    await driver.disconnect();
    ended.resolve(undefined);
    await rejected;
    expect(driver.isConnected()).toBe(false);
    expect(old.pool.end).toHaveBeenCalledOnce();
  });

  it.each([
    "connect",
    "probe",
  ] as const)("timeout cleanup closes pending %s and late-disconnect leaves a newer connection intact", async (stage) => {
    vi.useFakeTimers();
    const old = createPool("old");
    const acquisition = deferred<typeof old.client>();
    const probe = deferred<{ rows: Array<{ name: string }> }>();
    if (stage === "connect")
      old.pool.connect.mockReturnValue(acquisition.promise);
    else old.client.query.mockReturnValue(probe.promise);
    const driver = createDriver();
    const cancelAttempt = vi.spyOn(driver, "cancelConnectionAttempt");
    const disconnect = vi.spyOn(driver, "disconnect");
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutSeconds: 1,
      connectionTimeoutMs: 10,
      dbOperationTimeoutMs: 1000,
    }));
    const pending = wrapped.connect();
    const rejected = expect(pending).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(11);
    await rejected;
    expect(old.pool.end).toHaveBeenCalledOnce();
    expect(driver.isConnected()).toBe(false);
    const fresh = createPool("new");
    await wrapped.connect();
    acquisition.resolve(old.client);
    probe.resolve({ rows: [{ name: "old" }] });
    await flushMicrotasks();
    expect(driver.isConnected()).toBe(true);
    expect(fresh.pool.end).not.toHaveBeenCalled();
    expect(old.pool.end).toHaveBeenCalledOnce();
    expect(old.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(cancelAttempt).toHaveBeenCalledTimes(2);
    expect(cancelAttempt.mock.calls[1][0]).toBe(cancelAttempt.mock.calls[0][0]);
    expect(disconnect).toHaveBeenCalledOnce();
    await wrapped.disconnect();
    expect(fresh.pool.end).toHaveBeenCalledOnce();
  });

  it("connect failure cleanup preserves the error and parallel callers share the rejection", async () => {
    const { pool } = createPool();
    const acquisition = deferred<never>();
    pool.connect.mockReturnValue(acquisition.promise);
    pool.end.mockRejectedValue(new Error("cleanup failed"));
    const driver = createDriver();
    const first = driver.connect();
    const second = driver.connect();
    const rejected = Promise.all([
      expect(first).rejects.toThrow("connect failed"),
      expect(second).rejects.toThrow("connect failed"),
    ]);
    await flushMicrotasks();
    acquisition.reject(new Error("connect failed"));
    await rejected;
    expect(pool.connect).toHaveBeenCalledOnce();
    expect(pool.end).toHaveBeenCalledOnce();
    expect(driver.isConnected()).toBe(false);
    const fresh = createPool();
    await driver.connect();
    await driver.disconnect();
    expect(fresh.pool.end).toHaveBeenCalledOnce();
  });
});
