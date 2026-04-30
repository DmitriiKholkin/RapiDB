import { afterEach, describe, expect, it, vi } from "vitest";
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
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {
          return deferred.promise;
        },
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
  });

  it("cancels the current operation when a database timeout fires", async () => {
    vi.useFakeTimers();

    const deferred = createDeferred<string[]>();
    const cancelCurrentOperation = vi.fn();
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {},
        async listDatabases(): Promise<string[]> {
          return deferred.promise;
        },
        async cancelCurrentOperation(): Promise<void> {
          cancelCurrentOperation();
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
    expect(cancelCurrentOperation).toHaveBeenCalledTimes(1);
  });

  it("clears the timeout timer when a wrapped method fails synchronously", async () => {
    vi.useFakeTimers();

    const cancelCurrentOperation = vi.fn();
    const driver = createTimeoutAwareDriver(
      {
        async connect(): Promise<void> {},
        listDatabases(): Promise<string[]> {
          throw new Error("sync failure");
        },
        async cancelCurrentOperation(): Promise<void> {
          cancelCurrentOperation();
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
  });
});
