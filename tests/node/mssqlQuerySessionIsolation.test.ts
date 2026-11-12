import mssql from "mssql";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MssqlQuerySession } from "../../src/extension/dbDrivers/mssqlQuerySession";
import { mssqlCompletionTokenHarness } from "../helpers/mssqlCompletionTokenHarness";
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
  it("preserves prior all-omitted metadata when a later GO batch fails", async () => {
    const requests = Array.from({ length: 3 }, () => new MockMssqlRequest());
    const { driver, pool } = harness(requests);
    requests[0].execute.mockImplementation(async () => {
      requests[0].emit(
        "recordset",
        Array.from({ length: 4097 }, (_, n) => ({
          name: `c${n}`,
          type: mssql.Int,
        })),
      );
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });
    requests[1].execute.mockRejectedValue(new Error("late GO failure"));
    const result = await driver.query(
      "SELECT omitted\nGO\nTHROW\nGO\nSELECT not_run",
    );
    expect(result.columns).toEqual([]);
    expect(result.resultSets).toBeUndefined();
    expect(result.resultSetsTruncated).toBe(true);
    expect(result.error).toBe("late GO failure");
    expect(requests[2].execute).not.toHaveBeenCalled();
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("shares the column budget across GO batches", async () => {
    const requests = Array.from({ length: 3 }, () => new MockMssqlRequest());
    const { driver } = harness(requests);
    requests.forEach((request, n) => {
      request.execute.mockImplementation(async () => {
        const width = n < 2 ? 2048 : 1;
        request.emit(
          "recordset",
          Array.from({ length: width }, (_, i) => ({
            name: `c${i}`,
            type: mssql.Int,
          })),
        );
        request.emit("row", Array(width).fill(n));
        return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
      });
    });
    const result = await driver.query(
      "SELECT first\nGO\nSELECT second\nGO\nSELECT omitted",
    );
    expect(result.resultSets).toHaveLength(2);
    expect(
      result.resultSets?.reduce((n, set) => n + set.columns.length, 0),
    ).toBe(4096);
    expect(result.resultSetsTruncated).toBe(true);
    expect(requests[2].execute).toHaveBeenCalledOnce();
  });

  it("reports a late error even if every result set was omitted", async () => {
    const { driver, requests } = harness();
    requests[0].execute.mockImplementation(async () => {
      requests[0].emit(
        "recordset",
        Array.from({ length: 4097 }, (_, n) => ({
          name: `c${n}`,
          type: mssql.Int,
        })),
      );
      throw new Error("error after omitted results");
    });
    const result = await driver.query("SELECT omitted; THROW");
    expect(result.columns).toEqual([]);
    expect(result.resultSets).toBeUndefined();
    expect(result.resultSetsTruncated).toBe(true);
    expect(result.error).toBe("error after omitted results");
  });

  it("still honors cancellation after the metadata budget is exhausted", async () => {
    const { driver, requests, pool } = harness();
    requests[0].execute.mockImplementation(() => {
      for (let n = 0; n < 101; n++) {
        requests[0].emit("recordset", [{ name: "v", type: mssql.Int }]);
        requests[0].emit("row", [n]);
      }
      return new Promise(() => {});
    });
    const controller = new AbortController();
    const pending = driver.query("SELECT many; WAITFOR", undefined, {
      signal: controller.signal,
      hardCap: 1,
    });
    const rejected = expect(pending).rejects.toThrow("abort");
    await tick();
    controller.abort();
    await rejected;
    expect(requests[0].cancel).toHaveBeenCalledOnce();
    expect(pool.release).toHaveBeenCalledOnce();
    expect(pool.close).not.toHaveBeenCalled();
  });

  it("bounds retained metadata independently of rows and drains later statements", async () => {
    const { driver, requests, pool } = harness();
    const request = requests[0];
    let completedStatements = 0;
    const tokens = mssqlCompletionTokenHarness(pool.connections[0], request);
    request.execute.mockImplementation(async () => {
      for (let n = 0; n < 1000; n++) {
        request.emit("recordset", [{ name: `v${n}`, type: mssql.Int }]);
        request.emit("row", [n]);
        await tokens.complete(0xc1, 1);
        completedStatements++;
      }
      await tokens.complete(0xc5, 1000);
      return {
        recordsets: [],
        recordset: [],
        rowsAffected: [1000],
        output: {},
      };
    });
    const result = await driver.query(
      "SELECT many results; UPDATE later SET x=1",
      undefined,
      {
        hardCap: 1,
      },
    );
    expect(completedStatements).toBe(1000);
    expect(result.resultSets).toHaveLength(100);
    expect(result.resultSets?.reduce((n, set) => n + set.rows.length, 0)).toBe(
      1,
    );
    expect(result.resultSets?.at(-1)?.columns).toEqual(["v99"]);
    expect(result.resultSetsTruncated).toBe(true);
    expect(result.rowCount).toBe(100);
    expect(result.affectedRows).toBe(1000);
    expect(request.cancel).not.toHaveBeenCalled();
    expect(request.eventNames()).toEqual([]);
    expect(Object.hasOwn(tokens.native, "emit")).toBe(false);
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("shares set and row budgets across every GO repeat and keeps executing", async () => {
    const requests = Array.from({ length: 120 }, () => new MockMssqlRequest());
    const { driver, pool } = harness(requests);
    requests.forEach((request, n) => {
      request.execute.mockImplementation(async () => {
        request.emit("recordset", [{ name: "v", type: mssql.Int }]);
        request.emit("row", [n]);
        return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
      });
    });
    const result = await driver.query("SELECT 1\nGO 120", undefined, {
      hardCap: 2,
    });
    expect(
      requests.every((request) => request.execute.mock.calls.length === 1),
    ).toBe(true);
    expect(result.resultSets).toHaveLength(100);
    expect(result.resultSets?.reduce((n, set) => n + set.rows.length, 0)).toBe(
      2,
    );
    expect(result.resultSetsTruncated).toBe(true);
    expect(pool.acquire).toHaveBeenCalledOnce();
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("limits aggregate columns and does not attach omitted rows to a previous set", async () => {
    const { driver, requests } = harness();
    const request = requests[0];
    request.execute.mockImplementation(async () => {
      for (let set = 0; set < 5; set++) {
        request.emit(
          "recordset",
          Array.from({ length: 1024 }, (_, n) => ({
            name: `c${n}`,
            type: mssql.Int,
          })),
        );
        request.emit("row", Array(1024).fill(set));
      }
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });
    const result = await driver.query("SELECT wide sets", undefined, {
      hardCap: 10,
    });
    expect(result.resultSets).toHaveLength(4);
    expect(
      result.resultSets?.reduce((n, set) => n + set.columns.length, 0),
    ).toBe(4096);
    expect(result.resultSets?.every((set) => set.rowCount === 1)).toBe(true);
    expect(result.resultSets?.at(-1)?.rows[0].__col_0).toBe(3);
    expect(result.resultSetsTruncated).toBe(true);
  });

  it("retains a later small set after omitting an oversized first set and preserves its error", async () => {
    const { driver, requests } = harness();
    const request = requests[0];
    request.execute.mockImplementation(async () => {
      request.emit(
        "recordset",
        Array.from({ length: 4097 }, (_, n) => ({
          name: `c${n}`,
          type: mssql.Int,
        })),
      );
      request.emit("row", Array(4097).fill(0));
      request.emit("recordset", [{ name: "small", type: mssql.Int }]);
      request.emit("row", [7]);
      request.emit("rowsaffected", 1);
      request.emit("error", new Error("late failure"));
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });
    const result = await driver.query(
      "SELECT oversized; SELECT small; THROW",
      undefined,
      {
        hardCap: 1,
      },
    );
    expect(result.resultSets).toHaveLength(1);
    expect(result.rows).toEqual([{ __col_0: 7 }]);
    expect(result.resultSetsTruncated).toBe(true);
    expect(result.error).toBe("late failure");
  });

  it("maps SQL Server numeric result metadata and leaves character data untyped", async () => {
    const { driver, requests } = harness();
    requests[0].execute.mockImplementation(async () => {
      requests[0].emit("recordset", [
        {
          index: 0,
          name: "integer_value",
          type: mssql.BigInt,
          nullable: false,
          identity: false,
          readOnly: false,
        },
        {
          index: 1,
          name: "decimal_value",
          type: mssql.Decimal(10, 2),
          nullable: false,
          identity: false,
          readOnly: false,
        },
        {
          index: 2,
          name: "numeric_text",
          type: mssql.VarChar(20),
          nullable: false,
          identity: false,
          readOnly: false,
        },
      ]);
      requests[0].emit("row", ["9007199254740993", "10.25", "10"]);
      return { recordsets: [], recordset: [], rowsAffected: [0], output: {} };
    });

    const result = await driver.query("SELECT typed numeric values");

    expect(result.columnMeta).toEqual([
      { category: "integer" },
      { category: "decimal" },
      { category: null },
    ]);
  });

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
    const completed = expect(session.run(async () => 1)).resolves.toBe(1);
    let nextConnection: unknown;
    pool.acquire({}, (_error, next) => {
      nextConnection = next;
      pool.release(next);
    });
    expect(nextConnection).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1001);
    await completed;
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

  it.each([
    "completed",
    "partial",
    "GO",
    "omitted",
  ] as const)("preserves a %s error result after resetting an open transaction", async (shape) => {
    const requests = Array.from({ length: 3 }, () => new MockMssqlRequest());
    const { driver, connection, pool } = harness(requests);
    const failure = Object.assign(new Error("forced query failure"), {
      lineNumber: 3,
    });
    let completeReset!: () => void;
    connection.reset.mockImplementation((callback) => {
      completeReset = () => {
        connection.inTransaction = false;
        callback();
      };
    });
    requests[0].execute.mockImplementation(async () => {
      connection.inTransaction = true;
      requests[0].emit(
        "recordset",
        shape === "omitted"
          ? Array.from({ length: 4097 }, (_, index) => ({
              name: `c${index}`,
              type: mssql.Int,
            }))
          : [{ name: "v", type: mssql.Int }],
      );
      requests[0].emit("row", [1]);
      if (shape !== "partial") requests[0].emit("rowsaffected", 1);
      if (shape === "GO")
        return { recordsets: [], recordset: [], rowsAffected: [1], output: {} };
      requests[0].emit("error", failure);
      throw failure;
    });
    requests[1].execute.mockRejectedValue(failure);
    const pending = driver
      .query(
        shape === "GO"
          ? "BEGIN TRAN; SELECT 1 AS v\nGO\nRAISERROR('forced query failure',16,1)\nGO\nSELECT not_run"
          : "BEGIN TRAN; SELECT 1 AS v; RAISERROR('forced query failure',16,1)",
      )
      .then(
        (result) => ({ result }),
        (error: unknown) => ({ error }),
      );
    while (!connection.reset.mock.calls.length) await tick();
    let observedTransaction: boolean | undefined;
    pool.acquire({}, (_error, next) => {
      observedTransaction = next.inTransaction;
      pool.release(next);
    });
    expect(observedTransaction).toBeUndefined();
    expect(pool.release).not.toHaveBeenCalled();
    completeReset();
    const outcome = await pending;
    expect(outcome).toMatchObject({
      result: {
        error: "forced query failure (SQL Server line 3)",
        rows: shape === "omitted" ? [] : [{ __col_0: 1 }],
      },
    });
    if ("result" in outcome) {
      if (shape === "omitted") {
        expect(outcome.result.resultSetsTruncated).toBe(true);
      } else {
        expect(outcome.result.resultSets?.[0].partial === true).toBe(
          shape === "partial",
        );
      }
    }
    expect(requests[2].execute).not.toHaveBeenCalled();
    expect(connection.reset).toHaveBeenCalledOnce();
    expect(connection.close).not.toHaveBeenCalled();
    expect(observedTransaction).toBe(false);
    expect(pool.release).toHaveBeenCalledTimes(2);
    expect(connection.reset).toHaveBeenCalledBefore(pool.release);
  });

  it("preserves SQL diagnostics when an error result's session reset fails", async () => {
    const { driver, requests, connection, pool } = harness();
    requests[0].execute.mockImplementation(async () => {
      connection.inTransaction = true;
      requests[0].emit("recordset", [{ name: "v", type: mssql.Int }]);
      requests[0].emit("row", [1]);
      throw new Error("original SQL failure");
    });
    connection.reset.mockImplementation((callback) =>
      callback(new Error("reset failed")),
    );
    let failure: unknown;
    try {
      await driver.query("BEGIN TRAN; SELECT 1; RAISERROR");
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      message: expect.stringContaining("original SQL failure"),
      cause: {
        errors: [
          expect.objectContaining({ message: "original SQL failure" }),
          expect.objectContaining({ message: "reset failed" }),
        ],
      },
    });
    expect(connection.hasError).toBe(true);
    expect(connection.close).toHaveBeenCalledBefore(pool.release);
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it.each([
    "abort",
    "epoch",
  ] as const)("rejects an error result invalidated by %s during session reset", async (invalidation) => {
    const { driver, requests, connection, pool } = harness();
    const controller = new AbortController();
    let completeReset!: () => void;
    connection.reset.mockImplementation((callback) => {
      completeReset = () => {
        connection.inTransaction = false;
        callback();
      };
    });
    requests[0].execute.mockImplementation(async () => {
      connection.inTransaction = true;
      requests[0].emit("recordset", [{ name: "v", type: mssql.Int }]);
      requests[0].emit("row", [1]);
      throw new Error("original SQL failure");
    });
    const pending = driver.query("BEGIN TRAN; SELECT 1; RAISERROR", undefined, {
      signal: controller.signal,
    });
    const rejected = expect(pending).rejects.toThrow(/abort|cancelled/i);
    while (!connection.reset.mock.calls.length) await tick();
    if (invalidation === "abort") controller.abort();
    else Reflect.set(driver, "connectionEpoch", 1);
    completeReset();
    await rejected;
    expect(pool.release).toHaveBeenCalledOnce();
    expect(connection.inTransaction).toBe(false);
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

  it("retains earlier GO results and stops later batches after an error", async () => {
    const requests = Array.from({ length: 3 }, () => new MockMssqlRequest());
    const { driver } = harness(requests);
    requests[0].execute.mockImplementation(async () => {
      requests[0].emit("recordset", [
        { index: 0, name: "first_result", type: undefined },
      ]);
      requests[0].emit("row", [1]);
      return { recordsets: [], recordset: [], rowsAffected: [1], output: {} };
    });
    requests[1].execute.mockRejectedValue(
      Object.assign(new Error("Divide by zero error encountered."), {
        lineNumber: 1,
      }),
    );

    const result = await driver.query(
      "SELECT 1 AS first_result\nGO\nSELECT 1 / 0\nGO 2\nUPDATE t SET v=3",
    );

    expect(result.error).toBe(
      "Divide by zero error encountered. (SQL Server line 1)",
    );
    expect(result.resultSets).toMatchObject([
      {
        id: "result-1",
        columns: ["first_result"],
        rows: [{ __col_0: 1 }],
        rowCount: 1,
      },
    ]);
    expect(requests[0].batch).toHaveBeenCalledOnce();
    expect(requests[1].batch).toHaveBeenCalledOnce();
    expect(requests[2].batch).not.toHaveBeenCalled();
  });

  it.each([
    { counts: [2, 3], affectedRows: undefined },
    { counts: [2, 0], affectedRows: undefined },
    { counts: [0], affectedRows: 0 },
    { counts: [], affectedRows: 0 },
  ])("normalizes buffered metadata without inventing a DML total from $counts", async ({
    counts,
    affectedRows,
  }) => {
    const { driver, requests, pool } = harness();
    const columns = [{ index: 0, name: "", type: undefined }];
    requests[0].execute.mockResolvedValue({
      recordsets: [],
      recordset: Object.assign([[9]], { columns }),
      rowsAffected: counts,
      output: {},
    });
    expect(await driver.query("SELECT 9")).toMatchObject({
      columns: [" "],
      rows: [{ __col_0: 9 }],
      rowCount: 1,
      affectedRows,
    });
    expect(requests[0].cancel).not.toHaveBeenCalled();
    expect(requests[0].eventNames()).toEqual([]);
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("retains every SQL Server result set from a successful batch", async () => {
    const { driver, requests } = harness();
    const request = requests[0];
    requests[0].execute.mockImplementation(async () => {
      request.emit("recordset", [
        { index: 0, name: "first_result", type: undefined },
      ]);
      request.emit("row", [1]);
      request.emit("recordset", [
        { index: 0, name: "second_result", type: undefined },
      ]);
      request.emit("row", [2]);
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });

    const result = await driver.query(
      "SELECT 1 AS first_result; SELECT 2 AS second_result;",
    );

    expect(request.stream).toBe(true);
    expect(result).toMatchObject({
      columns: ["first_result"],
      rows: [{ __col_0: 1 }],
      rowCount: 2,
      resultSets: [
        {
          columns: ["first_result"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
        },
        {
          columns: ["second_result"],
          rows: [{ __col_0: 2 }],
          rowCount: 1,
        },
      ],
    });
  });

  it("preserves completed sets, marks the failing set partial, and reports its line", async () => {
    const { driver, requests } = harness();
    const request = requests[0];
    const failure = Object.assign(
      new Error("Divide by zero error encountered."),
      {
        lineNumber: 2,
      },
    );
    requests[0].execute.mockImplementation(async () => {
      request.emit("recordset", [
        { index: 0, name: "first_result", type: undefined },
      ]);
      request.emit("row", [1]);
      request.emit("rowsaffected", 1);
      request.emit("recordset", [
        { index: 0, name: "second_result", type: undefined },
      ]);
      request.emit("row", [2]);
      request.emit("error", failure);
      request.emit("rowsaffected", 0);
      request.emit("recordset", [
        { index: 0, name: "third_result", type: undefined },
      ]);
      request.emit("row", [3]);
      request.emit("rowsaffected", 1);
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });

    const result = await driver.query(
      "SELECT 1 AS first_result;\nSELECT 1 / 0 AS second_result;",
    );

    expect(result.error).toBe(
      "Divide by zero error encountered. (SQL Server line 2)",
    );
    expect(result.resultSets).toMatchObject([
      {
        id: "result-1",
        columns: ["first_result"],
        rows: [{ __col_0: 1 }],
        rowCount: 1,
      },
      {
        id: "result-2",
        columns: ["second_result"],
        rows: [{ __col_0: 2 }],
        rowCount: 1,
        partial: true,
      },
      {
        id: "result-3",
        columns: ["third_result"],
        rows: [{ __col_0: 3 }],
        rowCount: 1,
      },
    ]);
    expect(result.resultSets?.[0]).not.toHaveProperty("partial");
  });

  it("applies the hard cap across result sets while retaining each set's metadata", async () => {
    const { driver, requests } = harness();
    const request = requests[0];
    requests[0].execute.mockImplementation(async () => {
      request.emit("recordset", [
        { index: 0, name: "first_result", type: undefined },
      ]);
      request.emit("row", [1]);
      request.emit("row", [2]);
      request.emit("recordset", [
        { index: 0, name: "second_result", type: undefined },
      ]);
      request.emit("row", [3]);
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });

    const result = await driver.query("SELECT 1; SELECT 2;", undefined, {
      hardCap: 2,
    });

    expect(result.resultSets).toMatchObject([
      { rows: [{ __col_0: 1 }, { __col_0: 2 }], rowCount: 2 },
      { rows: [], rowCount: 1, truncated: true },
    ]);
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
    pool.acquire({}, () => {});
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
    expect(() =>
      requests[0].emit("error", new Error("late native error")),
    ).not.toThrow();
  });

  it("honors an already expired deadline without borrowing and ignores a deadline during cleanup", async () => {
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
    const completed = expect(session.run(async () => 1)).resolves.toBe(1);
    await vi.advanceTimersByTimeAsync(51);
    await completed;
    expect(pool.release).toHaveBeenCalledOnce();
    expect(pool.connections[0].close).not.toHaveBeenCalled();
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
    await expect(driver.query("SELECT 1")).resolves.toMatchObject({
      rowCount: 0,
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

  it("ignores cancellation during cleanup of a successful query without cancelling the reset request", async () => {
    const { driver, requests, connection, pool } = harness();
    let completeReset!: () => void;
    connection.reset.mockImplementation((callback) => {
      completeReset = () => callback();
    });
    const pending = driver.query("SELECT 1", undefined, { requestToken: 12 });
    const completed = expect(pending).resolves.toMatchObject({ rowCount: 0 });
    while (!connection.reset.mock.calls.length) await tick();
    await driver.cancelCurrentOperation({
      operationName: "query",
      requestToken: 12,
      reason: "manual",
    });
    expect(requests[0].cancel).not.toHaveBeenCalled();
    expect(pool.release).not.toHaveBeenCalled();
    completeReset();
    await completed;
    expect(pool.release).toHaveBeenCalledOnce();
    expect(connection.close).not.toHaveBeenCalled();
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
