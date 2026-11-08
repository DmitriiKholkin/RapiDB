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

function failedTransactionHarness(failCleanup?: "DO 0" | "ROLLBACK") {
  const h = harness({ serverStatus: 3 });
  const nativeError = Object.assign(
    new Error("Table 'qa.missing' doesn't exist"),
    {
      code: "ER_NO_SUCH_TABLE",
      errno: 1146,
      sqlState: "42S02",
    },
  );
  h.connection.connection.query.mockImplementation((command) => {
    const text = Reflect.get(command, "sql") as string;
    h.sql.push(text);
    queueMicrotask(() => {
      if (text.includes("missing")) {
        command.emit("error", nativeError);
        return;
      }
      if (text.startsWith("SELECT")) {
        command.emit("fields", [{ name: "first_result", columnType: 3 }]);
        command.emit("result", [1]);
      } else command.emit("result", { affectedRows: 0 });
      command.emit("end");
    });
    return command;
  });
  h.connection.query.mockImplementation(async (input) => {
    const text = typeof input === "string" ? input : input.sql;
    h.sql.push(text);
    if (text === failCleanup) throw new Error(`failed ${failCleanup}`);
    if (text.includes("missing")) throw nativeError;
    if (text.startsWith("SELECT"))
      return [[[1]], [{ name: "first_result", columnType: 3 }]];
    return [
      { serverStatus: text === "ROLLBACK" ? 2 : 3, warningStatus: 0 },
      [],
    ];
  });
  return { ...h, nativeError };
}

describe("MySQL arbitrary-query transaction and session isolation", () => {
  it.each([
    undefined,
    1,
  ])("does not turn cancellation during cleanup into a recovered result (cap=%s)", async (hardCap) => {
    vi.useFakeTimers();
    const h = failedTransactionHarness();
    const query = h.connection.query.getMockImplementation();
    if (!query) throw new Error("Expected transaction query fixture");
    h.connection.query.mockImplementation(async (input) =>
      (typeof input === "string" ? input : input.sql) === "ROLLBACK"
        ? new Promise(() => {})
        : query(input),
    );
    const outcome = h.driver
      .query(
        "BEGIN; SELECT 1 AS first_result; SELECT * FROM missing",
        undefined,
        {
          hardCap,
          requestToken: 9,
        },
      )
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    await h.driver.cancelCurrentOperation({
      operationName: "query",
      requestToken: 9,
      reason: "manual",
    });
    expect(String(await outcome)).toMatch(/cancelled/);
    expect(h.connection.destroy).toHaveBeenCalledOnce();
    expect(h.tracked.activeQueryOperations.size).toBe(0);
    expect(h.tracked.activeQueryConnectionSlots).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retains the native error alongside a cleanup timeout and closes its lease", async () => {
    vi.useFakeTimers();
    const h = failedTransactionHarness();
    const query = h.connection.query.getMockImplementation();
    if (!query) throw new Error("Expected transaction query fixture");
    h.connection.query.mockImplementation(async (input) =>
      (typeof input === "string" ? input : input.sql) === "ROLLBACK"
        ? new Promise(() => {})
        : query(input),
    );
    const pending = h.driver.query(
      "BEGIN; SELECT 1 AS first_result; SELECT * FROM missing",
      undefined,
      { hardCap: 1 },
    );
    await vi.advanceTimersByTimeAsync(1000);
    const result = await pending;
    expect(result.rows).toEqual([{ __col_0: 1 }]);
    expect(result.error).toContain(h.nativeError.message);
    expect(result.error).toMatch(/Query cleanup:.*timed out/);
    expect(h.connection.destroy).toHaveBeenCalledOnce();
    expect(h.tracked.activeQueryConnectionSlots).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([
    undefined,
    1,
  ])("retains the native error and completed SELECT when cleanup rolls back an unfinished transaction (cap=%s)", async (hardCap) => {
    const h = failedTransactionHarness();
    const result = await h.driver.query(
      "BEGIN; SELECT 1 AS first_result; SELECT * FROM missing; COMMIT",
      undefined,
      { hardCap },
    );
    expect(result.resultSets).toMatchObject([
      { rows: [{ __col_0: 1 }], rowCount: 1, truncated: false },
    ]);
    expect(result.resultSets?.[0]).not.toHaveProperty("partial", true);
    expect(result.error).toContain(h.nativeError.message);
    expect(result.error).toContain(
      "Unfinished MySQL transaction was rolled back",
    );
    expect(h.sql).toEqual([
      "BEGIN",
      "SELECT 1 AS first_result",
      "SELECT * FROM missing",
      "DO 0",
      "ROLLBACK",
    ]);
    expect(h.connection.destroy).toHaveBeenCalledOnce();
    expect(h.connection.release).not.toHaveBeenCalled();
    expect(h.tracked.activeQueryConnectionSlots).toBe(0);
    expect(h.tracked.activeQueryOperations.size).toBe(0);
  });

  it.each([
    [undefined, "DO 0"],
    [1, "DO 0"],
    [undefined, "ROLLBACK"],
    [1, "ROLLBACK"],
  ] as const)("retains both diagnostics and rows when %s-capped query cleanup fails at %s", async (hardCap, failCleanup) => {
    const h = failedTransactionHarness(failCleanup);
    const result = await h.driver.query(
      "BEGIN; SELECT 1 AS first_result; SELECT * FROM missing",
      undefined,
      { hardCap },
    );
    expect(result.rows).toEqual([{ __col_0: 1 }]);
    expect(result.error).toContain(h.nativeError.message);
    expect(result.error).toContain(`Query cleanup: failed ${failCleanup}`);
    expect(h.connection.destroy).toHaveBeenCalledOnce();
    expect(h.connection.release).not.toHaveBeenCalled();
    expect(h.tracked.activeQueryConnectionSlots).toBe(0);
  });

  it("keeps multi-result rows when read-only rollback fails", async () => {
    const { driver, connection, sql } = harness();
    connection.query.mockImplementation(async (input) => {
      const text = typeof input === "string" ? input : input.sql;
      sql.push(text);
      if (text === "ROLLBACK") throw new Error("rollback refused");
      if (text.startsWith("SELECT 1 AS first_result"))
        return [[[1]], [{ name: "first_result", columnType: 3 }]];
      if (text.startsWith("SELECT 2 AS second_result"))
        return [[[2]], [{ name: "second_result", columnType: 3 }]];
      return [[], []];
    });

    const result = await driver.query(
      "SELECT 1 AS first_result; SELECT 2 AS second_result",
      undefined,
      { readOnly: true },
    );

    expect(result.resultSets?.map((set) => set.rows)).toEqual([
      [{ __col_0: 1 }],
      [{ __col_0: 2 }],
    ]);
    expect(result.error).toContain("Query returned results");
    expect(result.error).toContain("rollback refused");
    expect(result.error).toContain("connection was discarded");
    expect(result.error).not.toContain("MySQL");
    expect(sql).toEqual([
      "START TRANSACTION READ ONLY",
      "SELECT 1 AS first_result",
      "SELECT 2 AS second_result",
      "ROLLBACK",
    ]);
    expect(connection.destroy).toHaveBeenCalledOnce();
  });

  it("keeps the native query error when read-only rollback also fails", async () => {
    const { driver, connection, sql } = harness();
    connection.query.mockImplementation(async (input) => {
      const text = typeof input === "string" ? input : input.sql;
      sql.push(text);
      if (text === "ROLLBACK") throw new Error("rollback refused");
      if (text === "SELECT FROM")
        throw new Error("ER_PARSE_ERROR: SELECT FROM");
      return [[], []];
    });

    await expect(
      driver.query("SELECT FROM", undefined, { readOnly: true }),
    ).rejects.toMatchObject({
      name: "AggregateError",
      message: expect.stringContaining(
        "ER_PARSE_ERROR: SELECT FROM; Read-only transaction rollback failed: rollback refused",
      ),
    });
    expect(sql).toEqual([
      "START TRANSACTION READ ONLY",
      "SELECT FROM",
      "ROLLBACK",
    ]);
    expect(connection.destroy).toHaveBeenCalledOnce();
  });

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
