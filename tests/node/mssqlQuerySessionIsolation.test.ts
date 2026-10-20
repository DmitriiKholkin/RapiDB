import type mssql from "mssql";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MssqlQuerySession } from "../../src/extension/dbDrivers/mssqlQuerySession";
import {
  MockMssqlRequest,
  mockMssqlSessionPool,
} from "../helpers/mssqlQuerySessionHarness";

afterEach(() => vi.useRealTimers());

function harness(requests = [new MockMssqlRequest()]) {
  const pool = mockMssqlSessionPool(requests);
  const driver = new MSSQLDriver({
    id: "isolation",
    name: "isolation",
    type: "mssql",
  });
  (driver as unknown as { pool: unknown }).pool = pool;
  return { pool, driver, requests, connection: pool.connections[0] };
}
const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

describe("MSSQL query session isolation", () => {
  it.each([
    undefined,
    1,
  ])("fences a queued borrower until failed query reset finishes (cap=%s)", async (hardCap) => {
    const { pool, driver, requests, connection } = harness();
    let completeReset!: () => void;
    connection.reset.mockImplementation((callback) => {
      completeReset = () => {
        connection.inTransaction = false;
        callback();
      };
    });
    requests[0].execute.mockImplementation(async (_sql, physical) => {
      physical.inTransaction = true;
      throw new Error("SQL error 266");
    });
    const failed = expect(
      driver.query("BEGIN TRAN; UPDATE t SET v=?", [41], { hardCap }),
    ).rejects.toThrow("SQL error 266");
    while (!connection.reset.mock.calls.length) await tick();
    let observed: boolean | undefined;
    pool.acquire({}, (_error, next) => {
      observed = next.inTransaction;
      pool.release(next);
    });
    expect(observed).toBeUndefined();
    expect(pool.release).not.toHaveBeenCalled();
    completeReset();
    await failed;
    expect(observed).toBe(false);
    expect(connection.close).not.toHaveBeenCalled();
    expect(connection.reset).toHaveBeenCalledOnce();
  });

  it("discards BEFORE a queued borrower on reset failure, preserving original error", async () => {
    const { pool, driver, requests, connection } = harness();
    let finish!: () => void;
    connection.reset.mockImplementation((callback) => {
      finish = () => callback(new Error("reset failed"));
    });
    requests[0].execute.mockRejectedValue(new Error("original failure"));
    const failure = driver.query("broken");
    const rejected = expect(failure).rejects.toMatchObject({
      errors: [
        expect.objectContaining({ message: "original failure" }),
        expect.objectContaining({ message: "reset failed" }),
      ],
    });
    while (!connection.reset.mock.calls.length) await tick();
    let nextConnection: unknown;
    pool.acquire({}, (_error, next) => {
      nextConnection = next;
      pool.release(next);
    });
    finish();
    await rejected;
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledBefore(pool.release);
    expect(nextConnection).not.toBe(connection);
  });

  it("bounds a hung reset and releases only a discarded connection", async () => {
    vi.useFakeTimers();
    const { pool, connection } = harness();
    let lateReset!: (error?: unknown) => void;
    connection.reset.mockImplementation((callback) => {
      lateReset = callback;
    });
    const session = new MssqlQuerySession(
      pool as unknown as mssql.ConnectionPool,
    );
    const failure = expect(session.run(async () => 1)).rejects.toThrow(
      "reset timed out",
    );
    let nextConnection: unknown;
    pool.acquire({}, (_error, next) => {
      nextConnection = next;
      pool.release(next);
    });
    expect(nextConnection).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1001);
    await failure;
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledBefore(pool.release);
    expect(nextConnection).not.toBe(connection);
    expect(pool.release).toHaveBeenCalledTimes(2);
    lateReset();
    await Promise.resolve();
    expect(connection.hasError).toBe(true);
    expect(pool.release).toHaveBeenCalledTimes(2);
  });

  it("rejects unfinished successful SQL and resets before release", async () => {
    const { driver, requests, connection, pool } = harness();
    requests[0].execute.mockImplementation(async () => {
      connection.inTransaction = true;
      return { recordsets: [], recordset: [], rowsAffected: [1], output: {} };
    });
    await expect(driver.query("BEGIN TRAN; UPDATE t SET v=1")).rejects.toThrow(
      "open transaction",
    );
    expect(connection.inTransaction).toBe(false);
    expect(connection.reset).toHaveBeenCalledBefore(pool.release);
  });

  it("pins GO/repeats to one lease and uses batch for parameterless SQL", async () => {
    const requests = Array.from({ length: 4 }, () => new MockMssqlRequest());
    const { driver, pool, connection } = harness(requests);
    requests[0].execute.mockImplementation(async () => {
      connection.inTransaction = true;
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });
    requests[3].execute.mockImplementation(async () => {
      connection.inTransaction = false;
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });
    await driver.query("BEGIN TRAN\nGO\nUPDATE t SET v=1\nGO 2\nCOMMIT");
    expect(pool.acquire).toHaveBeenCalledOnce();
    expect(pool.release).toHaveBeenCalledOnce();
    expect(requests.map((request) => request.batch.mock.calls[0][0])).toEqual([
      "BEGIN TRAN",
      "UPDATE t SET v=1",
      "UPDATE t SET v=1",
      "COMMIT",
    ]);
    expect(
      requests.every((request) => request.query.mock.calls.length === 0),
    ).toBe(true);
  });

  it("normalizes nonstreaming batch metadata and retains first-recordset/last-batch semantics", async () => {
    const { driver, requests } = harness();
    const columns = [{ index: 0, name: "", type: undefined }];
    requests[0].execute.mockResolvedValue({
      recordsets: [],
      recordset: Object.assign([[9]], { columns }),
      rowsAffected: [2, 3],
      output: {},
    });
    expect(await driver.query("SELECT 9")).toMatchObject({
      columns: [" "],
      rows: [{ __col_0: 9 }],
      affectedRows: 3,
    });
  });

  it("keeps typed parameters on query rather than batch", async () => {
    const { driver, requests } = harness();
    await driver.query("SELECT ?", ["x'; --"]);
    expect(requests[0].query).toHaveBeenCalledWith("SELECT @p1");
    expect(requests[0].batch).not.toHaveBeenCalled();
    expect(requests[0].input.mock.calls[0][2]).toBe("x'; --");
  });

  it("cancellation while queued returns promptly and fences the late arriving lease", async () => {
    const { pool, driver, connection } = harness();
    pool.acquire({}, () => {}); // another operation owns the only session
    const pending = driver.query("SELECT 1", undefined, { requestToken: 81 });
    const rejected = expect(pending).rejects.toThrow("cancelled");
    await driver.cancelCurrentOperation({
      operationName: "query",
      reason: "manual",
      requestToken: 81,
    });
    await rejected;
    expect(connection.close).not.toHaveBeenCalled();
    pool.release(connection);
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledOnce();
    expect(pool.release).toHaveBeenCalledTimes(2);
    expect(pool.request).not.toHaveBeenCalled();
  });

  it("cancellation is sticky between GO repeats and stale tokens cannot cancel a retry", async () => {
    const requests = [new MockMssqlRequest(), new MockMssqlRequest()];
    const { driver, connection } = harness(requests);
    requests[0].execute.mockImplementation(async () => {
      await driver.cancelCurrentOperation({
        operationName: "query",
        reason: "manual",
        requestToken: 3,
      });
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });
    await expect(
      driver.query("SELECT 1\nGO 5", undefined, { requestToken: 3 }),
    ).rejects.toThrow("cancelled");
    expect(connection.hasError).toBe(true);
    await driver.cancelCurrentOperation({
      operationName: "query",
      reason: "manual",
      requestToken: 3,
    });
    await driver.query("SELECT 2", undefined, { requestToken: 4 });
    expect(requests[1].batch).toHaveBeenCalledOnce();
    expect(requests[1].cancel).not.toHaveBeenCalled();
  });

  it("aborts a hung streaming request without shutting down other pool sessions", async () => {
    const { driver, pool, requests, connection } = harness();
    requests[0].execute.mockImplementation(() => new Promise(() => {}));
    const controller = new AbortController();
    const pending = driver.query("WAITFOR", undefined, {
      signal: controller.signal,
      hardCap: 1,
    });
    const rejected = expect(pending).rejects.toThrow("abort");
    await tick();
    controller.abort();
    await rejected;
    expect(requests[0].cancel).toHaveBeenCalledOnce();
    expect(connection.close).toHaveBeenCalledBefore(pool.release);
    expect(pool.close).not.toHaveBeenCalled();
    // A late native streaming error has a sink even after the caller returned.
    expect(() =>
      requests[0].emit("error", new Error("late native error")),
    ).not.toThrow();
  });

  it("honors an already expired deadline without borrowing and a deadline during cleanup", async () => {
    const { driver, pool } = harness();
    await expect(
      driver.query("SELECT 1", undefined, { deadline: Date.now() - 1 }),
    ).rejects.toThrow("deadline");
    expect(pool.acquire).not.toHaveBeenCalled();
    vi.useFakeTimers();
    const session = new MssqlQuerySession(
      pool as unknown as mssql.ConnectionPool,
      { deadline: Date.now() + 20 },
    );
    pool.connections[0].reset.mockImplementation((callback) => {
      setTimeout(() => callback(), 50);
    });
    const failed = expect(session.run(async () => 1)).rejects.toThrow(
      "deadline",
    );
    await vi.advanceTimersByTimeAsync(51);
    await failed;
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("rejects a pre-aborted empty script without acquiring a session", async () => {
    const { driver, pool } = harness();
    const controller = new AbortController();
    controller.abort();
    await expect(
      driver.query("", undefined, { signal: controller.signal }),
    ).rejects.toThrow();
    expect(pool.acquire).not.toHaveBeenCalled();
  });

  it("fences a contaminated session at acquisition without executing user SQL", async () => {
    const { driver, pool, connection } = harness();
    connection.inTransaction = true;
    await expect(driver.query("COMMIT TRAN")).rejects.toThrow(
      "acquired a session with an open transaction",
    );
    expect(pool.request).not.toHaveBeenCalled();
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledBefore(pool.release);
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("preserves the native timeout diagnostic after successful cleanup", async () => {
    const { driver, pool, requests, connection } = harness();
    const failure = Object.assign(new Error("request timed out"), {
      code: "ETIMEOUT",
    });
    requests[0].execute.mockRejectedValue(failure);
    await expect(driver.query("WAITFOR")).rejects.toBe(failure);
    expect(connection.reset).toHaveBeenCalledBefore(pool.release);
    expect(connection.close).not.toHaveBeenCalled();
  });

  it("discards before release even when reset and close both throw", async () => {
    const { driver, pool, connection } = harness();
    connection.reset.mockImplementation(() => {
      throw new Error("reset threw");
    });
    connection.close.mockImplementation(() => {
      throw new Error("close threw");
    });
    await expect(driver.query("SELECT 1")).rejects.toMatchObject({
      errors: [
        expect.objectContaining({ message: "reset threw" }),
        expect.objectContaining({ message: "close threw" }),
      ],
    });
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledBefore(pool.release);
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("discards a session when reset falsely reports success with transaction still open", async () => {
    const { driver, requests, pool, connection } = harness();
    requests[0].execute.mockImplementation(async () => {
      connection.inTransaction = true;
      throw new Error("operation failed");
    });
    connection.reset.mockImplementation((callback) => callback());
    await expect(driver.query("BEGIN TRAN")).rejects.toMatchObject({
      errors: [
        expect.objectContaining({ message: "operation failed" }),
        expect.objectContaining({
          message:
            "MSSQL query session reset did not restore a healthy session.",
        }),
      ],
    });
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledBefore(pool.release);
  });

  it("disconnect aborts only owned operations and a late lease cannot resurrect the old session", async () => {
    const { driver, pool, connection } = harness();
    pool.acquire({}, () => {});
    const pending = driver.query("SELECT 1", undefined, { requestToken: 92 });
    const rejected = expect(pending).rejects.toThrow("cancelled");
    await driver.disconnect();
    await rejected;
    expect(driver.isConnected()).toBe(false);
    expect(pool.request).not.toHaveBeenCalled();
    pool.release(connection);
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledOnce();
    expect(pool.release).toHaveBeenCalledTimes(2);
  });

  it("keeps cancellation active during cleanup without cancelling the reset request", async () => {
    const { driver, requests, connection, pool } = harness();
    let completeReset!: () => void;
    connection.reset.mockImplementation((callback) => {
      completeReset = () => callback();
    });
    const pending = driver.query("SELECT 1", undefined, { requestToken: 12 });
    const rejected = expect(pending).rejects.toThrow("cancelled");
    while (!connection.reset.mock.calls.length) await tick();
    await driver.cancelCurrentOperation({
      operationName: "query",
      requestToken: 12,
      reason: "manual",
    });
    expect(requests[0].cancel).not.toHaveBeenCalled();
    expect(pool.release).not.toHaveBeenCalled();
    completeReset();
    await rejected;
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("shares a decreasing deadline across GO and never dispatches the expired next batch", async () => {
    const requests = Array.from({ length: 3 }, () => new MockMssqlRequest());
    const { driver, connection } = harness(requests);
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    for (const request of requests) {
      request.execute.mockImplementation(async () => {
        now += 600;
        return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
      });
    }
    try {
      await expect(
        driver.query("SELECT 1\nGO\nSELECT 2\nGO\nSELECT 3", undefined, {
          deadline: now + 1000,
        }),
      ).rejects.toThrow("deadline");
      expect(requests[0].overrides).toEqual({ requestTimeout: 1000 });
      expect(requests[1].overrides).toEqual({ requestTimeout: 400 });
      expect(requests[2].batch).not.toHaveBeenCalled();
      expect(connection.reset).toHaveBeenCalledOnce();
    } finally {
      clock.mockRestore();
    }
  });
});
