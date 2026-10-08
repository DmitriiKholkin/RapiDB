import { EventEmitter } from "node:events";
import { Duplex } from "node:stream";
import * as mssql from "mssql";
import type { PoolOptions } from "mysql2/promise";
import { afterEach, describe, expect, it, vi } from "vitest";
import { closeDriverResource } from "../../src/extension/dbDrivers/driverCleanup";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";

const mysqlMocks = vi.hoisted(() => ({ createPool: vi.fn() }));
vi.mock("mysql2/promise", async (importOriginal) => ({
  ...(await importOriginal<typeof import("mysql2/promise")>()),
  createPool: mysqlMocks.createPool,
}));

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  mysqlMocks.createPool.mockReset();
});

describe("bounded driver cleanup", () => {
  it("leaves successfully closed resources alone and removes its timer", async () => {
    vi.useFakeTimers();
    const discard = vi.fn();
    await closeDriverResource(async () => undefined, discard);
    expect(discard).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("discards failed cleanup and preserves the original error", async () => {
    vi.useFakeTimers();
    const error = new Error("close failed");
    const discard = vi.fn();
    await expect(
      closeDriverResource(async () => {
        throw error;
      }, discard),
    ).rejects.toBe(error);
    expect(discard).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds cleanup and observes a rejection arriving after its deadline", async () => {
    vi.useFakeTimers();
    let reject!: (error: Error) => void;
    const close = new Promise<void>((_, rejectPromise) => {
      reject = rejectPromise;
    });
    const discard = vi.fn();
    const cleanup = closeDriverResource(() => close, discard);
    await vi.advanceTimersByTimeAsync(1000);
    await cleanup;
    expect(discard).toHaveBeenCalledOnce();
    reject(new Error("late close failure"));
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
  });
});

class BlackholedStream extends Duplex {
  _read(): void {}
  _write(
    _chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error) => void,
  ): void {
    callback();
  }
}

describe("MySQL native shutdown", () => {
  it.each([
    "idle",
    0,
    10,
  ] as const)("destroys a blackholed mysql2 transport, including successful Quit (query state %s)", async (state) => {
    vi.useFakeTimers();
    const mysql =
      await vi.importActual<typeof import("mysql2/promise")>("mysql2/promise");
    const stream = new BlackholedStream();
    let nativeConnection:
      | { _command?: { constructor: { name: string } } }
      | undefined;
    mysqlMocks.createPool.mockImplementation((options: PoolOptions) => {
      const pool = mysql.createPool({ ...options, stream: () => stream });
      // Replace only the wire handshake. All query, timeout, pool and Quit
      // behavior below uses the pinned mysql2 implementation.
      queueMicrotask(() => {
        const core = Reflect.get(pool, "pool") as unknown as {
          _allConnections: {
            get(index: number): EventEmitter & {
              connectTimeout?: ReturnType<typeof setTimeout>;
              _command?: { constructor: { name: string } };
            };
          };
        };
        const connection = core._allConnections.get(0);
        clearTimeout(connection.connectTimeout);
        Reflect.set(connection, "connectTimeout", null);
        Reflect.set(connection, "_command", null);
        Reflect.set(connection, "_handshakePacket", {});
        nativeConnection = connection;
        connection.emit("connect", connection);
      });
      return pool;
    });
    const driver = new MySQLDriver(
      { id: "mysql-close", name: "MySQL", type: "mysql", host: "unused" },
      () => ({
        connectionTimeoutSeconds: 1,
        connectionTimeoutMs: 1000,
        dbOperationTimeoutSeconds: 1,
        dbOperationTimeoutMs: state === "idle" ? 10 : state,
      }),
    );
    try {
      await driver.connect();
      const metadata =
        state === "idle"
          ? undefined
          : driver.listDatabases().catch((error: Error) => error);
      await vi.advanceTimersByTimeAsync(state === "idle" ? 0 : state);
      if (state === "idle") expect(nativeConnection?._command).toBeNull();
      else expect(nativeConnection?._command?.constructor.name).toBe("Query");
      if (state === 10)
        expect(await metadata).toMatchObject({
          code: "PROTOCOL_SEQUENCE_TIMEOUT",
        });
      let finished = false;
      const closing = driver.disconnect().then(() => {
        finished = true;
      });
      expect(driver.isConnected()).toBe(false);
      if (state === "idle") {
        await vi.advanceTimersByTimeAsync(0);
        expect(finished).toBe(true);
      } else {
        await vi.advanceTimersByTimeAsync(999);
        expect(finished).toBe(false);
        expect(stream.destroyed).toBe(false);
        await vi.advanceTimersByTimeAsync(1);
      }
      await closing;
      expect(stream.destroyed).toBe(true);
      if (state === 0) expect(await metadata).toBeInstanceOf(Error);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      stream.destroy();
      await driver.disconnect();
    }
  });
});

class MssqlSession extends EventEmitter {
  closed = false;
  hasError = false;
  beginError?: Error;
  beginTransaction(callback: (error?: Error) => void): void {
    callback(this.beginError);
  }
  commitTransaction(callback: (error?: Error) => void): void {
    callback();
  }
  execSql(request: {
    callback(error: null, rowCount: number, rows: unknown[]): void;
  }): void {
    request.callback(null, 1, []);
  }
  close = vi.fn(() => {
    this.closed = true;
    this.emit("end");
  });
}

function mssqlHarness() {
  const sessions: MssqlSession[] = [];
  let beginError: Error | undefined;
  const prototype = mssql.ConnectionPool.prototype as unknown as {
    _poolCreate(): Promise<MssqlSession>;
    _poolDestroy(connection: MssqlSession): Promise<void>;
  };
  vi.spyOn(prototype, "_poolCreate").mockImplementation(async function (
    this: mssql.ConnectionPool,
  ) {
    const session = new MssqlSession();
    session.beginError = beginError;
    const config = Reflect.get(this, "config") as mssql.config;
    config.beforeConnect?.(session as unknown as mssql.Connection);
    sessions.push(session);
    return session;
  });
  vi.spyOn(prototype, "_poolDestroy").mockImplementation(async (session) => {
    session.close();
  });
  const driver = new MSSQLDriver({
    id: "mssql-close",
    name: "MSSQL",
    type: "mssql",
    host: "unused",
  });
  return {
    driver,
    sessions,
    setBeginError: (error?: Error) => {
      beginError = error;
    },
  };
}

describe("MSSQL native transaction cleanup", () => {
  it("does not publish a connection whose probe finishes after disconnect", async () => {
    const { driver } = mssqlHarness();
    const prototype = mssql.ConnectionPool.prototype as unknown as {
      _poolCreate(): Promise<MssqlSession>;
    };
    const session = new MssqlSession();
    let finishProbe!: () => void;
    const probe = new Promise<void>((resolve) => {
      finishProbe = resolve;
    });
    vi.mocked(prototype._poolCreate).mockImplementationOnce(async function (
      this: mssql.ConnectionPool,
    ) {
      const config = Reflect.get(this, "config") as mssql.config;
      config.beforeConnect?.(session as unknown as mssql.Connection);
      await probe;
      return session;
    });
    const connecting = driver.connect();
    const rejected = expect(connecting).rejects.toMatchObject({
      name: "AbortError",
    });
    await driver.disconnect();
    finishProbe();
    await rejected;
    expect(driver.isConnected()).toBe(false);
    expect(Reflect.get(driver, "pool")).toBeNull();
    expect(session.closed).toBe(true);
  });

  it("discards and releases the node-mssql/Tarn session after failed BEGIN", async () => {
    const { driver, sessions, setBeginError } = mssqlHarness();
    const beginError = new Error("BEGIN failed");
    setBeginError(beginError);
    await driver.connect();
    const pool = Reflect.get(driver, "pool") as mssql.ConnectionPool;
    try {
      await expect(driver.runTransaction([])).rejects.toThrow(
        beginError.message,
      );
      const failed = sessions[1];
      expect(failed.hasError).toBe(true);
      expect(failed.closed).toBe(true);
      expect(failed.listenerCount("rollbackTransaction")).toBe(0);
      expect(pool.borrowed).toBe(0);
      setBeginError(undefined);
      await driver.runTransaction([]);
      expect(sessions).toHaveLength(3);
      expect(pool.borrowed).toBe(0);
    } finally {
      await driver.disconnect();
    }
  });

  it("bounds pool shutdown and closes the native transport even when a borrower never releases", async () => {
    vi.useFakeTimers();
    const { driver } = mssqlHarness();
    const connecting = driver.connect();
    await vi.advanceTimersByTimeAsync(0);
    await connecting;
    const pool = Reflect.get(driver, "pool") as mssql.ConnectionPool & {
      acquire(request: object): Promise<MssqlSession>;
      release(connection: MssqlSession): void;
    };
    const session = await pool.acquire({});
    try {
      let finished = false;
      const closing = driver.disconnect().then(() => {
        finished = true;
      });
      expect(driver.isConnected()).toBe(false);
      await vi.advanceTimersByTimeAsync(999);
      expect(finished).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      await closing;
      expect(session.closed).toBe(true);
    } finally {
      // Release the intentionally stuck test borrower so Tarn itself can drain.
      pool.release(session);
      const finalClose = pool.close();
      await vi.advanceTimersByTimeAsync(0);
      await finalClose;
    }
  });
});
