import {
  Connection,
  type FieldPacket,
  type Query,
  type QueryOptions,
} from "mysql2";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BoundedQueryRows } from "../../src/extension/dbDrivers/boundedQueryRows";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";

// Exercise the pinned mysql2 command's real row handler. A promise/callback
// query would collect into _currentRows even if the collector itself was bounded.
type WireQuery = Query & {
  onResult?: unknown;
  _currentRows: unknown[][];
  _currentFields: FieldPacket[];
  _rows: unknown[][][];
  _fields: (FieldPacket[] | undefined)[];
  _rowParser: { next(): unknown[] };
  options: object;
  row(packet: { isEOF(): boolean }, connection: object): void;
  doneInsert(header: object): void;
};
const field = (name: string, columnType = 3, columnLength = 11) =>
  ({ name, columnType, columnLength }) as FieldPacket;

function resultSet(command: WireQuery, count: number, first = 0) {
  const fields = [field("same"), field("same")];
  command._currentFields = fields;
  command._currentRows = [];
  command._rows.push(command._currentRows);
  command.options = {};
  command.emit("fields", fields);
  for (let i = 0; i < count; i++) {
    command._rowParser = { next: () => [first + i, i + 1] };
    command.row({ isEOF: () => false }, {});
  }
  expect(command._currentRows).toHaveLength(0);
  expect(command._rows).toHaveLength(0);
  expect(command._fields).toHaveLength(1);
}

function harness(run: (command: WireQuery, options: QueryOptions) => void) {
  const commands: WireQuery[] = [];
  const rawQuery = vi.fn((options: QueryOptions, ...rest: unknown[]) => {
    expect(rest).toEqual([]);
    expect(options.rowsAsArray).toBe(true);
    const createQuery = Connection.createQuery as unknown as (
      options: QueryOptions,
      values: undefined,
      callback: undefined,
      config: object,
    ) => WireQuery;
    const command = createQuery(options, undefined, undefined, {});
    commands.push(command);
    expect(command.onResult).toBeUndefined();
    queueMicrotask(() => run(command, options));
    return command;
  });
  const connection = {
    connection: { query: rawQuery },
    query: vi.fn(async () => [[], []]),
    release: vi.fn(),
    destroy: vi.fn(),
  };
  const driver = new MySQLDriver(
    {
      id: "mysql-budget",
      name: "MySQL",
      type: "mysql",
      host: "localhost",
    },
    () => ({
      connectionTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutSeconds: 1,
      dbOperationTimeoutMs: 1000,
    }),
  );
  const pool = { getConnection: vi.fn(async () => connection) };
  const tracked = driver as unknown as {
    pool: unknown;
    activeQueryOperations: Set<unknown>;
    activeQueryConnections: Set<unknown>;
    activeQueryConnectionSlots: number;
  };
  tracked.pool = pool;
  return { driver, connection, rawQuery, commands, pool, tracked };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("MySQL bounded callback-free queries", () => {
  it("drains large multi-result CALLs and later mutations with one retained budget and no mysql2 row buffering", async () => {
    let peak = 0;
    let mutated = false;
    const add = BoundedQueryRows.prototype.add;
    vi.spyOn(BoundedQueryRows.prototype, "add").mockImplementation(function (
      this: BoundedQueryRows<unknown>,
      row,
    ) {
      add.call(this, row);
      peak = Math.max(peak, this.rows.length);
    });
    const { driver, connection, tracked } = harness((command, options) => {
      if (options.sql === "CALL report()") {
        for (let set = 0; set < 80; set++)
          resultSet(command, 2000, set * 10000);
        command.doneInsert({ affectedRows: 2000, serverStatus: 0 });
      } else {
        mutated = true;
        command.doneInsert({ affectedRows: 999, serverStatus: 0 });
      }
      command.emit("end");
    });
    const result = await driver.query(
      "CALL report(); UPDATE items SET done = 1",
      undefined,
      { hardCap: 3 },
    );
    expect(peak).toBe(3);
    expect(mutated).toBe(true);
    expect(result).toMatchObject({
      columns: ["same", "same"],
      rows: [
        { __col_0: 790000, __col_1: 1 },
        { __col_0: 790001, __col_1: 2 },
        { __col_0: 790002, __col_1: 3 },
      ],
      rowCount: 2000,
      truncated: true,
    });
    expect(connection.release).toHaveBeenCalledOnce();
    expect(connection.destroy).not.toHaveBeenCalled();
    expect(tracked.activeQueryOperations.size).toBe(0);
    expect(tracked.activeQueryConnectionSlots).toBe(0);
  });

  it("selects the last rowset including an empty rowset, rather than a CALL's final OK packet", async () => {
    const { driver } = harness((command) => {
      resultSet(command, 100);
      resultSet(command, 0);
      command.doneInsert({ affectedRows: 0, serverStatus: 0 });
      command.emit("end");
    });
    expect(
      await driver.query("CALL report()", undefined, { hardCap: 2 }),
    ).toMatchObject({
      columns: ["same", "same"],
      rows: [],
      rowCount: 0,
      truncated: false,
    });
  });

  it("sums mutation-only scripts without turning the budget into an affected-row limit", async () => {
    const { driver } = harness((command) => {
      command.doneInsert({ affectedRows: 7000, serverStatus: 0 });
      command.emit("end");
    });
    expect(
      await driver.query("UPDATE a SET x=1; UPDATE b SET x=2", undefined, {
        hardCap: 1,
      }),
    ).toMatchObject({
      columns: [],
      rows: [],
      rowCount: 14000,
      affectedRows: 14000,
      truncated: false,
    });
  });

  it("preserves native value formatting and the existing values-query semantics", async () => {
    const values = ["O'Reilly", Buffer.from([1, 2]), null];
    const sql = "SELECT ?, ?, ?";
    const { driver, rawQuery } = harness((command) => {
      command.emit("fields", [
        field("b", 1, 1),
        field("bits", 16, 8),
        field("f", 4),
      ]);
      command.emit("result", [
        true,
        Buffer.from([255]),
        Number.POSITIVE_INFINITY,
      ]);
      command.emit("end");
    });
    const result = await driver.query(sql, values, { hardCap: 1 });
    expect(rawQuery).toHaveBeenCalledWith(
      expect.objectContaining({ sql, values, rowsAsArray: true }),
    );
    expect(result).toMatchObject({
      rows: [{ __col_0: 1, __col_1: 255, __col_2: null }],
      rowCount: 1,
      truncated: false,
    });
  });

  it("rejects errors after discarded rows, destroys the undrained connection, and releases tracking", async () => {
    let fail = true;
    const { driver, connection, tracked, commands } = harness((command) => {
      resultSet(command, 10000);
      if (fail) command.emit("error", new Error("late procedure failure"));
      command.emit("end");
    });
    await expect(
      driver.query("CALL broken()", undefined, { hardCap: 2 }),
    ).rejects.toThrow("late procedure failure");
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(connection.release).not.toHaveBeenCalled();
    expect(commands[0].listenerCount("result")).toBe(0);
    expect(tracked.activeQueryConnections.size).toBe(0);
    expect(tracked.activeQueryOperations.size).toBe(0);
    expect(tracked.activeQueryConnectionSlots).toBe(0);
    fail = false;
    expect(
      (await driver.query("SELECT 1", undefined, { hardCap: 2 })).rowCount,
    ).toBe(10000);
    expect(connection.release).toHaveBeenCalledOnce();
  });

  it.each([
    "timeout",
    "cancel",
  ])("settles %s even when destroy emits nothing, clears timers, and recovers", async (mode) => {
    vi.useFakeTimers();
    let hang = true;
    const { driver, connection, tracked, commands } = harness((command) => {
      resultSet(command, 20);
      if (!hang) command.emit("end");
    });
    const pending = driver.query("CALL slow()", undefined, {
      hardCap: 2,
      requestToken: 42,
    });
    const outcome = pending.catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    if (mode === "cancel") {
      await driver.cancelCurrentOperation({
        operationName: "query",
        requestToken: 41,
        reason: "manual",
      });
      expect(connection.destroy).not.toHaveBeenCalled();
      const cancellation = {
        operationName: "query",
        requestToken: 42,
        reason: "manual" as const,
      };
      // Both cancellations run before the rejected query's finally microtask.
      await Promise.all([
        driver.cancelCurrentOperation(cancellation),
        driver.cancelCurrentOperation(cancellation),
      ]);
    } else {
      await vi.advanceTimersByTimeAsync(1000);
    }
    expect(await outcome).toBeInstanceOf(Error);
    expect(String(await outcome)).toMatch(
      mode === "cancel" ? /cancelled/ : /timed out/,
    );
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
    expect(tracked.activeQueryOperations.size).toBe(0);
    expect(tracked.activeQueryConnectionSlots).toBe(0);
    expect(() =>
      commands[0].emit("error", new Error("late transport error")),
    ).not.toThrow();
    hang = false;
    expect(
      (await driver.query("SELECT 1", undefined, { hardCap: 2 })).rowCount,
    ).toBe(20);
  });

  it("cancels while acquiring a connection without executing or leaking the acquired connection", async () => {
    const { driver, pool, connection, rawQuery, tracked } = harness(
      () => undefined,
    );
    let acquire!: (value: typeof connection) => void;
    pool.getConnection.mockImplementation(
      () =>
        new Promise((resolve) => {
          acquire = resolve;
        }),
    );
    const pending = driver.query("SELECT 1", undefined, {
      hardCap: 2,
      requestToken: 7,
    });
    const rejected = expect(pending).rejects.toThrow(
      /cancelled before execution/,
    );
    await vi.waitFor(() => expect(pool.getConnection).toHaveBeenCalledOnce());
    await driver.cancelCurrentOperation({
      operationName: "query",
      requestToken: 7,
      reason: "manual",
    });
    acquire(connection);
    await rejected;
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(rawQuery).not.toHaveBeenCalled();
    expect(tracked.activeQueryOperations.size).toBe(0);
    expect(tracked.activeQueryConnectionSlots).toBe(0);
  });

  it("validates and clamps budgets before execution and avoids acquiring for empty scripts", async () => {
    const { driver, pool } = harness((command) => {
      resultSet(command, 10002);
      command.emit("end");
    });
    await expect(
      driver.query("SELECT 1", undefined, { hardCap: 0 }),
    ).rejects.toThrow(/positive integer/);
    expect(pool.getConnection).not.toHaveBeenCalled();
    expect(
      (await driver.query("-- empty", undefined, { hardCap: 1 })).rows,
    ).toEqual([]);
    expect(pool.getConnection).not.toHaveBeenCalled();
    const result = await driver.query("SELECT 1", undefined, {
      hardCap: 100000,
    });
    expect(result.rows).toHaveLength(10001);
    expect(result.rowCount).toBe(10002);
    expect(result.truncated).toBe(true);
    expect(driver.getCapabilities().boundedQueryResults).toBe(true);
  });
});
