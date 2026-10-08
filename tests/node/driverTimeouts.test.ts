import { afterEach, describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import {
  CONNECTION_TIMEOUT_SECONDS_DEFAULT,
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
  DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
  DriverTimeoutError,
} from "../../src/extension/dbDrivers/timeout";

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>["resolve"];
  let reject!: Deferred<T>["reject"];
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("driver timeout helpers", () => {
  it("adds per-query cancellation without mutating or losing caller options", async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const options = Object.freeze({
      requestToken: 123,
      readOnly: true,
      database: "selected",
      hardCap: 100,
      signal: controller.signal,
      deadline: Date.now() + 10,
    });
    const query = vi.fn(
      async (_sql: string, _params?: unknown[], context?: typeof options) =>
        context,
    );
    const wrapped = createTimeoutAwareDriver({ query }, () => ({
      connectionTimeoutSeconds: 15,
      dbOperationTimeoutSeconds: 1,
      connectionTimeoutMs: 15000,
      dbOperationTimeoutMs: 25,
    }));
    const captured = await wrapped.query("select 1", undefined, options);
    expect(captured).toMatchObject({
      ...options,
      signal: expect.any(AbortSignal),
    });
    expect(captured).not.toBe(options);
    expect(captured?.signal).not.toBe(options.signal);
    controller.abort();
    expect(captured?.signal.aborted).toBe(true);
  });

  it("cancels transaction connections without recursively recycling pools", async () => {
    const postgres = new PostgresDriver({
      id: "pg-timeout",
      name: "Postgres timeout",
      type: "pg",
    });
    const pgClient = { release: vi.fn() };
    (
      postgres as unknown as {
        activeTransactionClients: Set<typeof pgClient>;
      }
    ).activeTransactionClients.add(pgClient);
    const pgRecycle = vi.spyOn(postgres, "recycleConnectionAfterTimeout");

    const mysql = new MySQLDriver({
      id: "mysql-timeout",
      name: "MySQL timeout",
      type: "mysql",
    });
    const mysqlConnection = { destroy: vi.fn() };
    (
      mysql as unknown as {
        activeTransactionConnections: Set<typeof mysqlConnection>;
      }
    ).activeTransactionConnections.add(mysqlConnection);
    const mysqlRecycle = vi.spyOn(mysql, "recycleConnectionAfterTimeout");

    await postgres.cancelCurrentOperation();
    await mysql.cancelCurrentOperation();

    expect(pgClient.release).toHaveBeenCalledWith(true);
    expect(mysqlConnection.destroy).toHaveBeenCalledOnce();
    expect(pgRecycle).not.toHaveBeenCalled();
    expect(mysqlRecycle).not.toHaveBeenCalled();
  });

  it("cancels only the active superseded query connection", async () => {
    const postgres = new PostgresDriver({
      id: "pg-superseded-query",
      name: "Postgres superseded query",
      type: "pg",
    });
    const pgRecycle = vi
      .spyOn(postgres, "recycleConnectionAfterTimeout")
      .mockResolvedValue(undefined);
    const pgClient = { release: vi.fn() };
    const pgQueryState = postgres as unknown as {
      activeQueryOperations: Set<{
        cancelled: boolean;
        requestToken?: number;
        client?: typeof pgClient;
      }>;
      activeQueryClients: Set<typeof pgClient>;
    };
    pgQueryState.activeQueryClients.add(pgClient);
    pgQueryState.activeQueryOperations.add({
      cancelled: false,
      requestToken: 41,
      client: pgClient,
    });

    const mysql = new MySQLDriver({
      id: "mysql-superseded-query",
      name: "MySQL superseded query",
      type: "mysql",
    });
    const mysqlRecycle = vi
      .spyOn(mysql, "recycleConnectionAfterTimeout")
      .mockResolvedValue(undefined);
    const mysqlConnection = { destroy: vi.fn() };
    (
      mysql as unknown as {
        activeQueryOperations: Set<{
          cancelled: boolean;
          requestToken?: number;
          connection?: typeof mysqlConnection;
        }>;
      }
    ).activeQueryOperations.add({
      cancelled: false,
      requestToken: 41,
      connection: mysqlConnection,
    });
    (
      mysql as unknown as {
        activeQueryConnections: Set<typeof mysqlConnection>;
      }
    ).activeQueryConnections.add(mysqlConnection);

    const context = {
      reason: "superseded" as const,
      operationName: "query",
      requestToken: 41,
    };
    await postgres.cancelCurrentOperation(context);
    await mysql.cancelCurrentOperation(context);

    expect(pgClient.release).toHaveBeenCalledWith(true);
    expect(pgClient.release).toHaveBeenCalledOnce();
    expect(pgQueryState.activeQueryClients.size).toBe(0);
    expect([...pgQueryState.activeQueryOperations]).toEqual([
      { cancelled: true, requestToken: 41, client: undefined },
    ]);
    expect(mysqlConnection.destroy).toHaveBeenCalledOnce();
    expect(
      (
        mysql as unknown as {
          activeQueryConnections: Set<typeof mysqlConnection>;
        }
      ).activeQueryConnections.size,
    ).toBe(0);

    // The real query finally removes the operation entry. Even while it is
    // still registered, cancellation must not release the owned client twice.
    await postgres.cancelCurrentOperation(context);
    await mysql.cancelCurrentOperation(context);
    expect(pgClient.release).toHaveBeenCalledOnce();
    expect(pgQueryState.activeQueryClients.size).toBe(0);
    expect(mysqlConnection.destroy).toHaveBeenCalledOnce();
    expect(pgRecycle).not.toHaveBeenCalled();
    expect(mysqlRecycle).not.toHaveBeenCalled();
  });

  it("normalizes timeout settings to defaults and allowed bounds", () => {
    expect(createDriverTimeoutSettingsSnapshot()).toEqual({
      connectionTimeoutSeconds: CONNECTION_TIMEOUT_SECONDS_DEFAULT,
      dbOperationTimeoutSeconds: DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
      connectionTimeoutMs: CONNECTION_TIMEOUT_SECONDS_DEFAULT * 1000,
      dbOperationTimeoutMs: DB_OPERATION_TIMEOUT_SECONDS_DEFAULT * 1000,
    });

    expect(
      createDriverTimeoutSettingsSnapshot({
        connectionTimeoutSeconds: 0.4,
        dbOperationTimeoutSeconds: 999999,
      }),
    ).toEqual({
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutSeconds: 86400,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutMs: 86400000,
    });

    expect(
      createDriverTimeoutSettingsSnapshot({
        connectionTimeoutSeconds: Number.NaN,
        dbOperationTimeoutSeconds: Number.NaN,
      }),
    ).toEqual({
      connectionTimeoutSeconds: CONNECTION_TIMEOUT_SECONDS_DEFAULT,
      dbOperationTimeoutSeconds: DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
      connectionTimeoutMs: CONNECTION_TIMEOUT_SECONDS_DEFAULT * 1000,
      dbOperationTimeoutMs: DB_OPERATION_TIMEOUT_SECONDS_DEFAULT * 1000,
    });
  });

  it("times out long-running database operations", async () => {
    vi.useFakeTimers();

    const deferred = createDeferred<string[]>();
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {},
        async listDatabases(): Promise<string[]> {
          return deferred.promise;
        },
        quoteIdentifier(name: string): string {
          return name;
        },
      },
      () => ({
        connectionTimeoutSeconds: 15,
        dbOperationTimeoutSeconds: 1,
        connectionTimeoutMs: 15000,
        dbOperationTimeoutMs: 25,
      }),
    );

    const pending = driver.listDatabases();
    const assertion = expect(pending).rejects.toMatchObject({
      name: "DriverTimeoutError",
      timeoutKind: "dbOperation",
      operationName: "listDatabases",
      timeoutMs: 25,
    });

    await vi.advanceTimersByTimeAsync(25);

    await assertion;
  });

  it("times out connection attempts independently", async () => {
    vi.useFakeTimers();

    const deferred = createDeferred<void>();
    const disconnect = vi.fn(async () => undefined);
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {
          return deferred.promise;
        },
        disconnect,
        async listDatabases(): Promise<string[]> {
          return [];
        },
        quoteIdentifier(name: string): string {
          return name;
        },
      },
      () => ({
        connectionTimeoutSeconds: 1,
        dbOperationTimeoutSeconds: 180,
        connectionTimeoutMs: 10,
        dbOperationTimeoutMs: 180000,
      }),
    );

    const pending = driver.connect();
    const assertion = expect(pending).rejects.toMatchObject({
      name: "DriverTimeoutError",
      timeoutKind: "connection",
      operationName: "connect",
      timeoutMs: 10,
    });

    await vi.advanceTimersByTimeAsync(10);

    await assertion;
    expect(disconnect).toHaveBeenCalledTimes(1);

    deferred.resolve();
    await Promise.resolve();
    await Promise.resolve();
    expect(disconnect).toHaveBeenCalledTimes(2);
  });

  it("cancels the current operation when a database timeout fires", async () => {
    vi.useFakeTimers();

    const deferred = createDeferred<string[]>();
    const cancelCurrentOperation = vi.fn();
    const recycleConnectionAfterTimeout = vi.fn();
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {},
        async query(
          _sql: string,
          _params?: unknown[],
          _context?: { requestToken?: number },
        ): Promise<string[]> {
          return deferred.promise;
        },
        async cancelCurrentOperation(context: {
          timeoutKind: "connection" | "dbOperation";
          operationName: string;
        }): Promise<void> {
          cancelCurrentOperation(context);
        },
        async recycleConnectionAfterTimeout(context: {
          timeoutKind: "connection" | "dbOperation";
          operationName: string;
        }): Promise<void> {
          recycleConnectionAfterTimeout(context);
        },
        quoteIdentifier(name: string): string {
          return name;
        },
      },
      () => ({
        connectionTimeoutSeconds: 15,
        dbOperationTimeoutSeconds: 1,
        connectionTimeoutMs: 15000,
        dbOperationTimeoutMs: 25,
      }),
    );

    const pending = driver.query("select 1");
    const rejection =
      expect(pending).rejects.toBeInstanceOf(DriverTimeoutError);

    await vi.advanceTimersByTimeAsync(25);

    await rejection;
    expect(cancelCurrentOperation).toHaveBeenCalledTimes(1);
    expect(recycleConnectionAfterTimeout).not.toHaveBeenCalled();
    expect(cancelCurrentOperation).toHaveBeenCalledWith(
      expect.objectContaining({
        timeoutKind: "dbOperation",
        operationName: "query",
      }),
    );
  });

  it.each([
    "updateRows",
    "insertRow",
    "deleteRows",
  ] as const)("aborts the operation context when %s times out", async (method) => {
    vi.useFakeTimers();

    const deferred = createDeferred<{ affectedRows: number }>();
    let capturedSignal: AbortSignal | undefined;
    const mutation = vi.fn(
      async (_request: unknown, context?: { signal: AbortSignal }) => {
        capturedSignal = context?.signal;
        return deferred.promise;
      },
    );
    const driver = createTimeoutAwareDriver({ [method]: mutation }, () => ({
      connectionTimeoutSeconds: 15,
      dbOperationTimeoutSeconds: 1,
      connectionTimeoutMs: 15000,
      dbOperationTimeoutMs: 25,
    }));

    const pending = driver[method]({});
    const rejection =
      expect(pending).rejects.toBeInstanceOf(DriverTimeoutError);
    expect(capturedSignal?.aborted).toBe(false);

    await vi.advanceTimersByTimeAsync(25);

    await rejection;
    expect(capturedSignal?.aborted).toBe(true);
  });

  it("surfaces timeout errors without waiting for hanging cleanup", async () => {
    vi.useFakeTimers();

    const deferred = createDeferred<string[]>();
    const cleanupStarted = vi.fn();
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {},
        async query(
          _sql: string,
          _params?: unknown[],
          _context?: { requestToken?: number },
        ): Promise<string[]> {
          return deferred.promise;
        },
        async cancelCurrentOperation(): Promise<void> {
          cleanupStarted();
          await new Promise<void>(() => undefined);
        },
        quoteIdentifier(name: string): string {
          return name;
        },
      },
      () => ({
        connectionTimeoutSeconds: 15,
        dbOperationTimeoutSeconds: 1,
        connectionTimeoutMs: 15000,
        dbOperationTimeoutMs: 25,
      }),
    );

    const pending = driver.query("select 1");
    const assertion =
      expect(pending).rejects.toBeInstanceOf(DriverTimeoutError);

    await vi.advanceTimersByTimeAsync(25);
    expect(cleanupStarted).toHaveBeenCalledTimes(1);

    await assertion;
  });

  it("does not recycle the pool when a timed-out operation settles late", async () => {
    vi.useFakeTimers();

    const deferred = createDeferred<string[]>();
    const recycleConnectionAfterTimeout = vi.fn();
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {},
        async listDatabases(): Promise<string[]> {
          return deferred.promise;
        },
        async recycleConnectionAfterTimeout(): Promise<void> {
          recycleConnectionAfterTimeout();
        },
        quoteIdentifier(name: string): string {
          return name;
        },
      },
      () => ({
        connectionTimeoutSeconds: 15,
        dbOperationTimeoutSeconds: 1,
        connectionTimeoutMs: 15000,
        dbOperationTimeoutMs: 25,
      }),
    );

    const pending = driver.listDatabases();
    const rejection =
      expect(pending).rejects.toBeInstanceOf(DriverTimeoutError);
    await vi.advanceTimersByTimeAsync(25);
    await rejection;

    deferred.resolve([]);
    await vi.runAllTimersAsync();
    await Promise.resolve();

    expect(recycleConnectionAfterTimeout).not.toHaveBeenCalled();
  });

  it("passes the query request token to targeted timeout cancellation", async () => {
    vi.useFakeTimers();
    const cancelCurrentOperation = vi.fn(async () => undefined);
    const driver = createTimeoutAwareDriver(
      {
        async query(
          _sql: string,
          _params?: unknown[],
          _context?: { requestToken?: number },
        ): Promise<never> {
          return new Promise(() => undefined);
        },
        cancelCurrentOperation,
      },
      () => ({
        connectionTimeoutSeconds: 15,
        dbOperationTimeoutSeconds: 1,
        connectionTimeoutMs: 15000,
        dbOperationTimeoutMs: 25,
      }),
    );

    const pending = driver.query("select 1", undefined, { requestToken: 73 });
    const rejection =
      expect(pending).rejects.toBeInstanceOf(DriverTimeoutError);
    await vi.advanceTimersByTimeAsync(25);
    await rejection;

    expect(cancelCurrentOperation).toHaveBeenCalledWith(
      expect.objectContaining({ operationName: "query", requestToken: 73 }),
    );
  });

  it("clears the timeout timer when a wrapped method fails synchronously", async () => {
    vi.useFakeTimers();

    const cancelCurrentOperation = vi.fn();
    const recycleConnectionAfterTimeout = vi.fn();
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {},
        listDatabases(): Promise<string[]> {
          throw new Error("sync failure");
        },
        async cancelCurrentOperation(): Promise<void> {
          cancelCurrentOperation();
        },
        async recycleConnectionAfterTimeout(): Promise<void> {
          recycleConnectionAfterTimeout();
        },
        quoteIdentifier(name: string): string {
          return name;
        },
      },
      () => ({
        connectionTimeoutSeconds: 15,
        dbOperationTimeoutSeconds: 1,
        connectionTimeoutMs: 15000,
        dbOperationTimeoutMs: 25,
      }),
    );

    await expect(driver.listDatabases()).rejects.toThrow("sync failure");

    await vi.advanceTimersByTimeAsync(25);

    expect(cancelCurrentOperation).not.toHaveBeenCalled();
    expect(recycleConnectionAfterTimeout).not.toHaveBeenCalled();
  });
});
