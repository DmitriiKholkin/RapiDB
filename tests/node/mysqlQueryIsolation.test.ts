import type { Query } from "mysql2";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";

function harness(status: unknown = { serverStatus: 2 }) {
  const sql: string[] = [];
  const connection = {
    connection: {
      config: {},
      query: vi.fn((command: Query) => {
        sql.push(Reflect.get(command, "sql"));
        queueMicrotask(() => command.emit("end"));
        return command;
      }),
    },
    query: vi.fn(
      async (
        options: { sql: string } | string,
      ): Promise<[unknown, unknown[]]> => {
        const text = typeof options === "string" ? options : options.sql;
        sql.push(text);
        return [text === "DO 0" ? status : [], []];
      },
    ),
    destroy: vi.fn(),
    release: vi.fn(),
  };
  const driver = new MySQLDriver(
    { id: "isolation", name: "MySQL", type: "mysql", host: "localhost" },
    () => ({
      connectionTimeoutMs: 1000,
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutMs: 1000,
      dbOperationTimeoutSeconds: 1,
    }),
  );
  const tracked = driver as unknown as {
    pool: unknown;
    activeQueryOperations: Set<unknown>;
    activeQueryConnections: Set<unknown>;
    activeQueryConnectionSlots: number;
  };
  tracked.pool = { getConnection: vi.fn(async () => connection) };
  return { driver, connection, sql, tracked };
}

afterEach(() => vi.useRealTimers());

describe("MySQL arbitrary-query transaction and session isolation", () => {
  it.each([
    [undefined, undefined],
    [1, undefined],
    [undefined, [1]],
    [1, [1]],
  ])("rejects unfinished transactions in all query paths (cap=%s, params=%s)", async (hardCap, params) => {
    const { driver, connection, sql, tracked } = harness({ serverStatus: 3 });
    await expect(
      driver.query("START TRANSACTION; UPDATE accounts SET balance=0", params, {
        hardCap,
      }),
    ).rejects.toThrow(/Unfinished.*rolled back/);
    expect(sql.slice(-2)).toEqual(["DO 0", "ROLLBACK"]);
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(connection.release).not.toHaveBeenCalled();
    expect(tracked.activeQueryConnectionSlots).toBe(0);
    expect(tracked.activeQueryOperations.size).toBe(0);
  });

  it.each([
    "SELECT 1",
    "START TRANSACTION; UPDATE accounts SET balance=0; COMMIT",
    "START TRANSACTION; UPDATE accounts SET balance=0; ROLLBACK",
    "USE other_db; SET autocommit=0; SET sql_mode=''",
  ])("discards session state even after success: %s", async (script) => {
    const { driver, connection, sql } = harness();
    await driver.query(script);
    expect(sql.at(-1)).toBe("DO 0");
    expect(connection.release).not.toHaveBeenCalled();
    expect(connection.destroy).toHaveBeenCalledOnce();
  });

  it.each([
    {},
    [],
    { serverStatus: -1 },
    { serverStatus: 1.5 },
    { serverStatus: 65536 },
  ])("fails closed on invalid status %j", async (status) => {
    const { driver, connection } = harness(status);
    await expect(driver.query("SELECT 1")).rejects.toThrow(/Cannot verify/);
    expect(connection.destroy).toHaveBeenCalledOnce();
  });

  it.each([
    "SELECT 1",
    "DO 0",
    "ROLLBACK",
  ])("preserves failures and discards connection when %s fails", async (failAt) => {
    const { driver, connection } = harness({ serverStatus: 3 });
    connection.query.mockImplementation(async (options) => {
      const sql = typeof options === "string" ? options : options.sql;
      if (sql === failAt) throw new Error(`failed ${failAt}`);
      return [{ serverStatus: 3 }, []];
    });
    await expect(driver.query("SELECT 1")).rejects.toThrow(`failed ${failAt}`);
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(connection.release).not.toHaveBeenCalled();
  });

  it.each([
    "DO 0",
    "ROLLBACK",
  ])("settles cancellation/timeout while %s is hung", async (hangAt) => {
    for (const cancel of [true, false]) {
      vi.useFakeTimers();
      const { driver, connection, tracked } = harness({ serverStatus: 3 });
      connection.query.mockImplementation(async (options) => {
        const sql = typeof options === "string" ? options : options.sql;
        if (sql === hangAt) return new Promise(() => {});
        return [{ serverStatus: 3 }, []];
      });
      const outcome = driver
        .query("SELECT 1", undefined, { requestToken: 9 })
        .catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(0);
      if (cancel)
        await driver.cancelCurrentOperation({
          operationName: "query",
          requestToken: 9,
          reason: "manual",
        });
      else await vi.advanceTimersByTimeAsync(1000);
      expect(String(await outcome)).toMatch(cancel ? /cancelled/ : /timed out/);
      expect(connection.destroy).toHaveBeenCalledOnce();
      expect(connection.release).not.toHaveBeenCalled();
      expect(tracked.activeQueryConnectionSlots).toBe(0);
      expect(vi.getTimerCount()).toBe(0);
      vi.useRealTimers();
    }
  });
});
