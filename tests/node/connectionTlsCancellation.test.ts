import type { Stats } from "node:fs";
import type { FileHandle } from "node:fs/promises";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  open: vi.fn(),
  stat: vi.fn(),
  mysqlCreatePool: vi.fn(),
  mongoConstruct: vi.fn(),
  mongoConnect: vi.fn(),
  mongoClose: vi.fn(),
  elasticConstruct: vi.fn(),
  elasticPing: vi.fn(),
  elasticClose: vi.fn(),
  pgConstruct: vi.fn(),
  redisCreateClient: vi.fn(),
}));

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return {
    ...actual,
    open: mocks.open,
    stat: mocks.stat,
  };
});

vi.mock("mysql2/promise", async (importOriginal) => {
  const actual = await importOriginal<typeof import("mysql2/promise")>();
  return { ...actual, createPool: mocks.mysqlCreatePool };
});

vi.mock("pg", async (importOriginal) => {
  const actual = await importOriginal<typeof import("pg")>();
  return {
    ...actual,
    Pool: class MockPool {
      constructor(options: unknown) {
        Object.assign(this, mocks.pgConstruct(options));
      }
    },
  };
});

vi.mock("redis", () => ({ createClient: mocks.redisCreateClient }));

vi.mock("mongodb", async (importOriginal) => {
  const actual = await importOriginal<typeof import("mongodb")>();
  return {
    ...actual,
    MongoClient: class MockMongoClient {
      constructor() {
        mocks.mongoConstruct();
      }
      async connect() {
        mocks.mongoConnect();
        return this;
      }
      async close() {
        mocks.mongoClose();
      }
    },
  };
});

vi.mock("@elastic/elasticsearch", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@elastic/elasticsearch")>();
  return {
    ...actual,
    Client: class MockElasticsearchClient {
      constructor() {
        mocks.elasticConstruct();
      }
      async ping() {
        mocks.elasticPing();
      }
      async close() {
        mocks.elasticClose();
      }
    },
  };
});

import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
  DriverTimeoutError,
} from "../../src/extension/dbDrivers/timeout";
import { resolveConnectionTlsSettings } from "../../src/extension/services/connectionTls";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

type Engine = "mysql" | "mongodb" | "elasticsearch" | "postgres" | "redis";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function waitForOpen(): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt++) {
    if (mocks.open.mock.calls.length === 1) return;
    await Promise.resolve();
  }
  expect(mocks.open).toHaveBeenCalledTimes(1);
}

async function expectNoUnhandledRejections(action: () => void): Promise<void> {
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);
  try {
    action();
    await new Promise<void>((resolve) => setImmediate(resolve));
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
  expect(unhandled).toEqual([]);
}

function createDriver(engine: Engine) {
  const config: ConnectionConfig = {
    id: `tls-cancel-${engine}`,
    name: `TLS cancellation ${engine}`,
    type: engine === "postgres" ? "pg" : engine,
    host: "127.0.0.1",
    port: {
      mongodb: 27017,
      mysql: 3306,
      elasticsearch: 9200,
      postgres: 5432,
      redis: 6379,
    }[engine],
    connectionUri:
      engine === "mongodb" ? "mongodb://127.0.0.1:27017/test" : undefined,
    tls: { mode: "requireVerifyCa", caFilePath: "/slow/ca.pem" },
  };
  switch (engine) {
    case "mysql":
      return new MySQLDriver(config);
    case "mongodb":
      return new MongoDBDriver(config);
    case "elasticsearch":
      return new ElasticsearchDriver(config);
    case "postgres":
      return new PostgresDriver(config);
    case "redis":
      return new RedisDriver(config);
  }
}

function expectNoClientOrNetwork(): void {
  expect(mocks.mysqlCreatePool).not.toHaveBeenCalled();
  expect(mocks.mongoConstruct).not.toHaveBeenCalled();
  expect(mocks.mongoConnect).not.toHaveBeenCalled();
  expect(mocks.mongoClose).not.toHaveBeenCalled();
  expect(mocks.elasticConstruct).not.toHaveBeenCalled();
  expect(mocks.elasticPing).not.toHaveBeenCalled();
  expect(mocks.elasticClose).not.toHaveBeenCalled();
  expect(mocks.pgConstruct).not.toHaveBeenCalled();
  expect(mocks.redisCreateClient).not.toHaveBeenCalled();
}

describe.each([
  "mysql",
  "mongodb",
  "elasticsearch",
  "postgres",
  "redis",
] as const)("%s TLS resolution cancellation", (engine) => {
  let pendingRead: ReturnType<typeof deferred<{ bytesRead: number }>>;
  let rejectPendingRead: (error: unknown) => void;
  let handle: FileHandle & {
    close: ReturnType<typeof vi.fn>;
    read: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    pendingRead = deferred<{ bytesRead: number }>();
    rejectPendingRead = pendingRead.reject;
    const pathStats = {
      isFile: () => true,
      size: 1,
      dev: 1,
      ino: 1,
    } as Stats;
    handle = {
      stat: vi.fn(async () => pathStats),
      read: vi.fn(() => pendingRead.promise),
      close: vi.fn(async () => undefined),
    } as unknown as FileHandle & {
      close: ReturnType<typeof vi.fn>;
      read: ReturnType<typeof vi.fn>;
    };
    mocks.stat.mockResolvedValue(pathStats);
    mocks.open.mockResolvedValue(handle);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function waitForRead(): Promise<void> {
    for (let attempt = 0; attempt < 20; attempt++) {
      if (handle.read.mock.calls.length === 1) return;
      await Promise.resolve();
    }
    expect(handle.read).toHaveBeenCalledTimes(1);
  }

  async function verifyLateReadRejectionIsHandled(): Promise<void> {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    rejectPendingRead(new Error("late filesystem read failure"));
    await new Promise<void>((resolve) => setImmediate(resolve));
    process.off("unhandledRejection", onUnhandled);
    expect(unhandled).toEqual([]);
  }

  it("closes the file handle and prevents client creation after disconnect", async () => {
    const driver = createDriver(engine);
    const connecting = driver.connect();
    await waitForRead();

    await driver.disconnect();

    await expect(connecting).rejects.toMatchObject({ name: "AbortError" });
    expect(handle.close).toHaveBeenCalledTimes(1);
    expectNoClientOrNetwork();
    await verifyLateReadRejectionIsHandled();
  });

  it("rejects before a pending open settles and closes its late handle", async () => {
    const pendingOpen = deferred<FileHandle>();
    mocks.open.mockReturnValue(pendingOpen.promise);
    const driver = createDriver(engine);
    const connecting = driver.connect();
    await waitForOpen();

    await driver.disconnect();

    // Cancellation must settle the driver connection without waiting for open().
    await expect(connecting).rejects.toMatchObject({ name: "AbortError" });
    handle.close.mockRejectedValueOnce(new Error("late close failure"));
    await expectNoUnhandledRejections(() => pendingOpen.resolve(handle));

    expect(handle.close).toHaveBeenCalledTimes(1);
    expect(handle.read).not.toHaveBeenCalled();
    expectNoClientOrNetwork();
  });

  it.each([
    "path",
    "handle",
  ] as const)("aborts pending %s stat without waiting for late rejection", async (stage) => {
    const pendingStat = deferred<Stats>();
    if (stage === "path") mocks.stat.mockReturnValue(pendingStat.promise);
    else vi.mocked(handle.stat).mockReturnValue(pendingStat.promise);
    const driver = createDriver(engine);
    const connecting = driver.connect();
    await vi.waitFor(() => {
      expect(stage === "path" ? mocks.stat : handle.stat).toHaveBeenCalledTimes(
        1,
      );
    });

    await driver.disconnect();

    await expect(connecting).rejects.toMatchObject({ name: "AbortError" });
    expect(handle.close).toHaveBeenCalledTimes(stage === "path" ? 0 : 1);
    expect(handle.read).not.toHaveBeenCalled();
    await expectNoUnhandledRejections(() =>
      pendingStat.reject(new Error("late stat failure")),
    );
    expectNoClientOrNetwork();
  });

  it("does not wait for pending file close after read cancellation", async () => {
    const pendingClose = deferred<void>();
    handle.close.mockReturnValue(pendingClose.promise);
    const driver = createDriver(engine);
    const connecting = driver.connect();
    await waitForRead();

    await driver.disconnect();

    await expect(connecting).rejects.toMatchObject({ name: "AbortError" });
    expect(handle.close).toHaveBeenCalledTimes(1);
    expectNoClientOrNetwork();
    await expectNoUnhandledRejections(() => {
      pendingClose.reject(new Error("late close failure"));
      rejectPendingRead(new Error("late read failure"));
    });
  });

  it.each([
    "stat",
    "open",
    "read",
  ] as const)("aborts pending %s and prevents client creation on connect timeout", async (stage) => {
    const pendingStat = deferred<Stats>();
    const pendingOpen = deferred<FileHandle>();
    if (stage === "stat") mocks.stat.mockReturnValue(pendingStat.promise);
    if (stage === "open") mocks.open.mockReturnValue(pendingOpen.promise);
    const driver = createTimeoutAwareDriver(createDriver(engine), () =>
      createDriverTimeoutSettingsSnapshot({
        connectionTimeoutSeconds: 1,
        dbOperationTimeoutSeconds: 1,
      }),
    );
    vi.useFakeTimers();
    const connecting = driver.connect();
    const timeoutOutcome =
      expect(connecting).rejects.toBeInstanceOf(DriverTimeoutError);
    if (stage === "read") await waitForRead();
    else if (stage === "open") await waitForOpen();
    else {
      for (let attempt = 0; attempt < 20; attempt++) {
        if (mocks.stat.mock.calls.length === 1) break;
        await Promise.resolve();
      }
      expect(mocks.stat).toHaveBeenCalledTimes(1);
    }
    await vi.advanceTimersByTimeAsync(1000);

    await timeoutOutcome;
    expect(handle.close).toHaveBeenCalledTimes(stage === "read" ? 1 : 0);
    expectNoClientOrNetwork();
    vi.useRealTimers();
    await expectNoUnhandledRejections(() => {
      if (stage === "stat") pendingStat.reject(new Error("late stat failure"));
      if (stage === "open") pendingOpen.resolve(handle);
      if (stage === "read") rejectPendingRead(new Error("late read failure"));
    });
    expect(handle.close).toHaveBeenCalledTimes(stage === "stat" ? 0 : 1);
    expectNoClientOrNetwork();
  });
});

describe.each([
  "postgres",
  "redis",
] as const)("%s addressed connection cancellation", (engine) => {
  const pathStats = { isFile: () => true, size: 1, dev: 1, ino: 1 } as Stats;

  function fileHandle(read = vi.fn(async () => ({ bytesRead: 0 }))) {
    return {
      stat: vi.fn(async () => pathStats),
      read,
      close: vi.fn(async () => undefined),
    } as unknown as FileHandle & {
      read: ReturnType<typeof vi.fn>;
      close: ReturnType<typeof vi.fn>;
    };
  }

  function nativeFixture() {
    const client = {
      query: vi.fn(async () => ({ rows: [{ name: "test" }] })),
      release: vi.fn(),
    };
    const connect = vi.fn(async () => client);
    const cleanup = vi.fn(async (): Promise<void> => undefined);
    return {
      client,
      connect,
      cleanup,
      native:
        engine === "postgres"
          ? { on: vi.fn(), connect, end: cleanup }
          : {
              on: vi.fn(),
              connect,
              destroy: cleanup,
              isOpen: true,
              isReady: true,
            },
    };
  }

  function driver() {
    return createDriver(engine) as PostgresDriver | RedisDriver;
  }

  beforeEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    mocks.stat.mockResolvedValue(pathStats);
    mocks.open.mockImplementation(async () => fileHandle());
  });

  afterEach(() => vi.useRealTimers());

  it.each([
    ["disconnect", "resolve"],
    ["disconnect", "reject"],
    ["addressed cancel", "resolve"],
    ["addressed cancel", "reject"],
    ["timeout", "resolve"],
    ["timeout", "reject"],
  ] as const)("rejects raw connect after file close starts on %s, isolating late close %s from reconnect", async (cancellation, outcome) => {
    const oldClosing = deferred<void>();
    const oldHandle = fileHandle();
    oldHandle.close.mockReturnValue(oldClosing.promise);
    mocks.open.mockResolvedValueOnce(oldHandle);
    const raw = driver();
    const oldAttempt = raw.connect();
    let oldError: unknown;
    const oldSettlement = oldAttempt.catch((error: unknown) => {
      oldError = error;
    });
    await vi.waitFor(() => expect(oldHandle.close).toHaveBeenCalledTimes(1));
    expect(oldHandle.read).toHaveBeenCalledTimes(1);

    if (cancellation === "timeout") {
      const wrapped = createTimeoutAwareDriver(raw, () =>
        createDriverTimeoutSettingsSnapshot({
          connectionTimeoutSeconds: 1,
          dbOperationTimeoutSeconds: 1,
        }),
      );
      vi.useFakeTimers();
      const timedOut = expect(wrapped.connect()).rejects.toBeInstanceOf(
        DriverTimeoutError,
      );
      await vi.advanceTimersByTimeAsync(1000);
      await timedOut;
      vi.useRealTimers();
    } else if (cancellation === "disconnect") {
      await raw.disconnect();
    } else {
      await raw.cancelConnectionAttempt(oldAttempt);
    }

    // The underlying raw attempt, not just its timeout wrapper, must reject
    // while oldClosing is still pending. No native client may have been created.
    await vi.waitFor(
      () => expect(oldError).toMatchObject({ name: "AbortError" }),
      { timeout: 100 },
    );
    await oldSettlement;
    expect(oldHandle.close).toHaveBeenCalledTimes(1);
    expectNoClientOrNetwork();

    const newClosing = deferred<void>();
    const newHandle = fileHandle();
    newHandle.close.mockReturnValue(newClosing.promise);
    mocks.open.mockResolvedValueOnce(newHandle);
    const fixture = nativeFixture();
    mocks.pgConstruct.mockReturnValue(fixture.native);
    mocks.redisCreateClient.mockReturnValue(fixture.native);
    let newSettled = false;
    const reconnecting = raw.connect();
    const newSettlement = reconnecting.then(
      () => {
        newSettled = true;
      },
      () => {
        newSettled = true;
      },
    );
    await vi.waitFor(() => expect(newHandle.close).toHaveBeenCalledTimes(1));

    await expectNoUnhandledRejections(() => {
      if (outcome === "resolve") oldClosing.resolve();
      else oldClosing.reject(new Error("late old file close failure"));
    });
    await raw.cancelConnectionAttempt(oldAttempt);
    expect(newSettled).toBe(false);
    expect(oldHandle.close).toHaveBeenCalledTimes(1);
    expect(newHandle.close).toHaveBeenCalledTimes(1);
    expectNoClientOrNetwork();

    newClosing.resolve();
    await reconnecting;
    await newSettlement;
    await raw.cancelConnectionAttempt(oldAttempt);
    expect(raw.isConnected()).toBe(true);
    expect(fixture.connect).toHaveBeenCalledTimes(1);
    expect(fixture.cleanup).not.toHaveBeenCalled();
    if (engine === "redis") {
      const signal = mocks.redisCreateClient.mock.calls[0][0].socket
        .signal as AbortSignal;
      expect(signal.aborted).toBe(false);
    }
    await raw.disconnect();
  });

  it.each([
    "stat",
    "open",
    "read",
  ] as const)("cancels only the addressed attempt during pending %s, including after reconnect", async (stage) => {
    const oldStat = deferred<Stats>();
    const oldOpen = deferred<FileHandle>();
    const oldRead = deferred<{ bytesRead: number }>();
    const oldHandle = fileHandle(vi.fn(() => oldRead.promise));
    if (stage === "stat") mocks.stat.mockReturnValueOnce(oldStat.promise);
    if (stage === "open") mocks.open.mockReturnValueOnce(oldOpen.promise);
    if (stage === "read") mocks.open.mockResolvedValueOnce(oldHandle);
    const raw = driver();
    const oldAttempt = raw.connect();
    const oldOutcome = expect(oldAttempt).rejects.toMatchObject({
      name: "AbortError",
    });
    await vi.waitFor(() =>
      expect(
        stage === "stat"
          ? mocks.stat
          : stage === "open"
            ? mocks.open
            : oldHandle.read,
      ).toHaveBeenCalledTimes(1),
    );

    await raw.cancelConnectionAttempt(oldAttempt);
    await oldOutcome;
    expectNoClientOrNetwork();

    const newRead = deferred<{ bytesRead: number }>();
    const newHandle = fileHandle(vi.fn(() => newRead.promise));
    mocks.open.mockResolvedValueOnce(newHandle);
    const fixture = nativeFixture();
    mocks.pgConstruct.mockReturnValue(fixture.native);
    mocks.redisCreateClient.mockReturnValue(fixture.native);
    const newAttempt = raw.connect();
    await vi.waitFor(() => expect(newHandle.read).toHaveBeenCalledTimes(1));
    await raw.cancelConnectionAttempt(oldAttempt);
    expect(newHandle.close).not.toHaveBeenCalled();

    await expectNoUnhandledRejections(() => {
      if (stage === "stat") oldStat.reject(new Error("late old stat failure"));
      if (stage === "open") oldOpen.resolve(oldHandle);
      if (stage === "read") oldRead.reject(new Error("late old read failure"));
    });
    expect(oldHandle.close).toHaveBeenCalledTimes(stage === "stat" ? 0 : 1);
    expect(newHandle.close).not.toHaveBeenCalled();
    newRead.resolve({ bytesRead: 0 });
    await newAttempt;
    await raw.cancelConnectionAttempt(oldAttempt);
    expect(raw.isConnected()).toBe(true);
    expect(fixture.cleanup).not.toHaveBeenCalled();
    await raw.disconnect();
  });

  it.each([
    ["timeout", "resolve"],
    ["timeout", "reject"],
    ["disconnect", "resolve"],
    ["disconnect", "reject"],
  ] as const)("keeps reconnect alive after %s when native connect settles late (%s)", async (cancellation, outcome) => {
    const oldFixture = nativeFixture();
    const oldOpening = deferred<typeof oldFixture.client>();
    const oldClosing = deferred<void>();
    oldFixture.connect.mockReturnValue(oldOpening.promise);
    // pg.end() may wait for a checkout; late catch/finally must own only that pool.
    if (engine === "postgres")
      oldFixture.cleanup.mockReturnValue(oldClosing.promise);
    const newFixture = nativeFixture();
    const construct =
      engine === "postgres" ? mocks.pgConstruct : mocks.redisCreateClient;
    construct
      .mockReturnValueOnce(oldFixture.native)
      .mockReturnValueOnce(newFixture.native);
    const raw = driver();
    const cancel = vi.spyOn(raw, "cancelConnectionAttempt");
    const wrapped = createTimeoutAwareDriver(raw, () =>
      createDriverTimeoutSettingsSnapshot({
        connectionTimeoutSeconds: 1,
        dbOperationTimeoutSeconds: 1,
      }),
    );
    vi.useFakeTimers();
    const connecting =
      cancellation === "timeout" ? wrapped.connect() : raw.connect();
    const cancelledOutcome =
      cancellation === "timeout"
        ? expect(connecting).rejects.toBeInstanceOf(DriverTimeoutError)
        : expect(connecting).rejects.toThrow();
    await vi.advanceTimersByTimeAsync(0);
    expect(oldFixture.connect).toHaveBeenCalledTimes(1);
    let disconnecting: Promise<void> | undefined;
    if (cancellation === "timeout") {
      await vi.advanceTimersByTimeAsync(1000);
      await cancelledOutcome;
      expect(cancel).toHaveBeenCalled();
    } else {
      disconnecting = raw.disconnect();
      await vi.advanceTimersByTimeAsync(0);
    }
    const oldAttempt =
      cancellation === "timeout" ? cancel.mock.calls[0][0] : connecting;

    const pendingRead = deferred<{ bytesRead: number }>();
    const newHandle = fileHandle(vi.fn(() => pendingRead.promise));
    mocks.open.mockResolvedValueOnce(newHandle);
    const reconnecting = wrapped.connect();
    await vi.advanceTimersByTimeAsync(0);
    expect(newHandle.read).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
    await expectNoUnhandledRejections(() => {
      if (outcome === "resolve") oldOpening.resolve(oldFixture.client);
      else oldOpening.reject(new Error("late native connect failure"));
      oldClosing.resolve();
    });
    await disconnecting;
    await cancelledOutcome;
    await raw.cancelConnectionAttempt(oldAttempt);
    expect(newHandle.close).not.toHaveBeenCalled();
    if (engine === "postgres") {
      expect(oldFixture.cleanup).toHaveBeenCalledTimes(1);
      if (cancellation === "timeout")
        expect(cancel.mock.calls.length).toBeGreaterThanOrEqual(2);
      if (outcome === "resolve")
        expect(oldFixture.client.release).toHaveBeenCalledWith(true);
    }
    pendingRead.resolve({ bytesRead: 0 });
    await reconnecting;
    await raw.cancelConnectionAttempt(oldAttempt);
    expect(raw.isConnected()).toBe(true);
    expect(newFixture.cleanup).not.toHaveBeenCalled();
    if (engine === "redis") {
      const signal = construct.mock.calls[1][0].socket.signal as AbortSignal;
      expect(signal.aborted).toBe(false);
    }
    await raw.disconnect();
  });
});

describe("TLS file resolution cancellation during open", () => {
  let handle: FileHandle & {
    close: ReturnType<typeof vi.fn>;
    read: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    vi.clearAllMocks();
    const pathStats = {
      isFile: () => true,
      size: 1,
      dev: 1,
      ino: 1,
    } as Stats;
    handle = {
      stat: vi.fn(async () => pathStats),
      read: vi.fn(async () => ({ bytesRead: 0 })),
      close: vi.fn(async () => undefined),
    } as unknown as FileHandle & {
      close: ReturnType<typeof vi.fn>;
      read: ReturnType<typeof vi.fn>;
    };
    mocks.stat.mockResolvedValue(pathStats);
  });

  it("rejects promptly and closes a FileHandle returned after abort", async () => {
    const pendingOpen = deferred<FileHandle>();
    mocks.open.mockReturnValue(pendingOpen.promise);
    const controller = new AbortController();
    const resolution = resolveConnectionTlsSettings(
      {
        id: "tls-open-cancel",
        name: "TLS open cancellation",
        type: "mysql",
        host: "127.0.0.1",
        port: 3306,
        tls: { mode: "requireVerifyCa", caFilePath: "/slow/ca.pem" },
      },
      controller.signal,
    );
    await waitForOpen();

    controller.abort();
    await expect(resolution).rejects.toMatchObject({ name: "AbortError" });
    handle.close.mockRejectedValueOnce(new Error("late close failure"));
    await expectNoUnhandledRejections(() => pendingOpen.resolve(handle));

    expect(handle.close).toHaveBeenCalledTimes(1);
    expect(handle.read).not.toHaveBeenCalled();
  });
});
