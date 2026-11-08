import type { FieldPacket, Query, QueryOptions } from "mysql2";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";

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
  const rawQuery = vi.fn((query: Query, ...rest: unknown[]) => {
    expect(rest).toEqual([]);
    const command = query as WireQuery;
    const options = Reflect.get(command, "_queryOptions") as QueryOptions;
    expect(options.rowsAsArray).toBe(true);
    commands.push(command);
    expect(command.onResult).toBeUndefined();
    queueMicrotask(() => run(command, options));
    return command;
  });
  const connection = {
    connection: { query: rawQuery, config: {} },
    query: vi.fn(
      async (_input?: unknown): Promise<unknown[]> => [{ serverStatus: 2 }, []],
    ),
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
    let mutated = false;
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
    expect(mutated).toBe(true);
    expect(result).toMatchObject({
      columns: ["same", "same"],
      rows: [
        { __col_0: 0, __col_1: 1 },
        { __col_0: 1, __col_1: 2 },
        { __col_0: 2, __col_1: 3 },
      ],
      rowCount: 160000,
      truncated: true,
    });
    expect(result.resultSets).toHaveLength(80);
    expect(
      result.resultSets?.reduce(
        (total, resultSet) => total + resultSet.rows.length,
        0,
      ),
    ).toBe(3);
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 2000,
      truncated: true,
    });
    expect(result.resultSets?.[1]).toMatchObject({
      rowCount: 2000,
      rows: [],
      truncated: true,
    });
    expect(connection.release).not.toHaveBeenCalled();
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(tracked.activeQueryOperations.size).toBe(0);
    expect(tracked.activeQueryConnectionSlots).toBe(0);
  });

  it("retains every CALL result set, including an empty rowset before the final OK packet", async () => {
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
      rows: [
        { __col_0: 0, __col_1: 1 },
        { __col_0: 1, __col_1: 2 },
      ],
      rowCount: 100,
      resultSets: [
        { id: "result-1", rowCount: 100, truncated: true },
        { id: "result-2", rowCount: 0, rows: [], truncated: false },
      ],
      truncated: true,
    });
  });

  it("returns each SELECT rowset from a bounded multi-statement script", async () => {
    const { driver, commands } = harness((command, options) => {
      const first = options.sql === "SELECT 1 AS first_result";
      command.emit("fields", [field(first ? "first_result" : "second_result")]);
      command.emit("result", [first ? 1 : 2]);
      command.emit("end");
    });

    const result = await driver.query(
      "SELECT 1 AS first_result; SELECT 2 AS second_result;",
      undefined,
      { hardCap: 10 },
    );

    expect(commands).toHaveLength(2);
    expect(result).toMatchObject({
      columns: ["first_result"],
      rows: [{ __col_0: 1 }],
      rowCount: 2,
      resultSets: [
        {
          id: "result-1",
          columns: ["first_result"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          truncated: false,
        },
        {
          id: "result-2",
          columns: ["second_result"],
          rows: [{ __col_0: 2 }],
          rowCount: 1,
          truncated: false,
        },
      ],
    });
  });

  it("retains native JSON column metadata on a bounded result set", async () => {
    const { driver } = harness((command) => {
      command.emit("fields", [field("json_value", 245)]);
      command.emit("result", ['{"nested":[1,null]}']);
      command.emit("end");
    });

    const result = await driver.query("SELECT json_value", undefined, {
      hardCap: 10,
    });

    expect(result).toMatchObject({
      columnMeta: [{ category: "json" }],
      resultSets: [
        {
          columns: ["json_value"],
          columnMeta: [{ category: "json" }],
          rows: [{ __col_0: '{"nested":[1,null]}' }],
        },
      ],
    });
  });

  it("retains exact numeric metadata and leaves numeric-looking text untyped", async () => {
    const { driver } = harness((command) => {
      command.emit("fields", [
        field("decimal_value", 246),
        field("integer_value", 8),
        field("numeric_text", 253),
      ]);
      command.emit("result", ["10.25", "9007199254740993", "10"]);
      command.emit("end");
    });

    const result = await driver.query("SELECT numeric values", undefined, {
      hardCap: 10,
    });

    expect(result.columnMeta).toEqual([
      { category: "decimal" },
      { category: "integer" },
      { category: null },
    ]);
  });

  it("marks DATETIME and TIMESTAMP results temporal and leaves text untyped", async () => {
    const { driver } = harness((command) => {
      command.emit("fields", [
        field("datetime_value", 12),
        field("timestamp_value", 7),
        field("text_value", 253),
      ]);
      command.emit("result", [
        "2024-01-01 10:00:00",
        "2024-01-01 10:00:00",
        "2024-01-01T10:00:00.000Z",
      ]);
      command.emit("end");
    });

    const result = await driver.query("SELECT temporal values", undefined, {
      hardCap: 10,
    });

    expect(result.columnMeta).toEqual([
      { category: "datetime" },
      { category: "datetime" },
      { category: null },
    ]);
  });

  it("keeps earlier SELECT rowsets and native server diagnostics after a later statement fails", async () => {
    const serverError = Object.assign(
      new Error("Table 'missing' doesn't exist"),
      {
        code: "ER_NO_SUCH_TABLE",
        errno: 1146,
        sqlState: "42S02",
        fatal: false,
      },
    );
    const { driver, connection } = harness((command, options) => {
      if (options.sql === "SELECT 1 AS first_result") {
        command.emit("fields", [field("first_result")]);
        command.emit("result", [1]);
        command.emit("end");
      } else {
        command.emit("error", serverError);
      }
    });

    const result = await driver.query(
      "SELECT 1 AS first_result; SELECT * FROM missing;",
      undefined,
      { hardCap: 10 },
    );

    expect(result).toMatchObject({
      columns: ["first_result"],
      rows: [{ __col_0: 1 }],
      rowCount: 1,
      error: "Table 'missing' doesn't exist",
      resultSets: [
        {
          id: "result-1",
          columns: ["first_result"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
        },
      ],
    });
    expect(connection.query).toHaveBeenCalledOnce();
    expect(connection.destroy).toHaveBeenCalledOnce();
  });

  it("keeps a rowset complete when a later CALL statement fails after its protocol boundary", async () => {
    const serverError = Object.assign(new Error("procedure statement failed"), {
      code: "ER_SIGNAL_EXCEPTION",
      errno: 1644,
      sqlState: "45000",
      fatal: false,
    });
    const { driver } = harness((command) => {
      resultSet(command, 2);
      Reflect.set(command, "_resultIndex", 1);
      command.emit("error", serverError);
    });

    const result = await driver.query("CALL report()", undefined, {
      hardCap: 10,
    });

    expect(result).toMatchObject({
      error: "procedure statement failed",
      resultSets: [
        {
          id: "result-1",
          rowCount: 2,
        },
      ],
    });
    expect(result.resultSets?.[0]).not.toHaveProperty("partial");
  });

  it("marks only the active CALL rowset partial when a later result fails before its own boundary", async () => {
    const serverError = Object.assign(new Error("second rowset failed"), {
      code: "ER_SIGNAL_EXCEPTION",
      errno: 1644,
      sqlState: "45000",
      fatal: false,
    });
    const { driver } = harness((command) => {
      resultSet(command, 2);
      Reflect.set(command, "_resultIndex", 1);
      resultSet(command, 1, 10);
      command.emit("error", serverError);
    });

    const result = await driver.query("CALL report()", undefined, {
      hardCap: 10,
    });

    expect(result.resultSets).toMatchObject([
      { id: "result-1", rowCount: 2 },
      { id: "result-2", rowCount: 1, partial: true },
    ]);
    expect(result.resultSets?.[0]).not.toHaveProperty("partial");
  });

  it("parses every rowset from an unbounded CALL before later script statements", async () => {
    const { driver, connection } = harness(() => undefined);
    connection.query.mockImplementation(async (input?: unknown) => {
      if (
        typeof input === "string" ||
        (typeof input === "object" &&
          input !== null &&
          Reflect.get(input, "sql") === "DO 0")
      ) {
        return [{ serverStatus: 2 }, []];
      }
      const options = input as QueryOptions;
      if (options.sql === "CALL report()") {
        return [
          [[[11]], [[22]]],
          [[field("first")], [field("second")]],
        ];
      }
      return [[[33]], [field("later")]];
    });

    const result = await driver.query("CALL report(); SELECT 33 AS later");

    expect(
      connection.query.mock.calls.filter(
        ([input]) =>
          typeof input === "object" &&
          input !== null &&
          Reflect.get(input, "sql") !== "DO 0",
      ),
    ).toHaveLength(2);
    expect(result).toMatchObject({
      columns: ["first"],
      rows: [{ __col_0: 11 }],
      rowCount: 3,
      resultSets: [
        { id: "result-1", columns: ["first"], rows: [{ __col_0: 11 }] },
        { id: "result-2", columns: ["second"], rows: [{ __col_0: 22 }] },
        { id: "result-3", columns: ["later"], rows: [{ __col_0: 33 }] },
      ],
    });
  });

  it("caps retained MySQL result-set metadata and marks omitted sets", async () => {
    const { driver } = harness((command, options) => {
      const match = options.sql.match(/result_(\d+)/);
      const value = Number(match?.[1] ?? 0);
      command.emit("fields", [field(`result_${value}`)]);
      command.emit("result", [value]);
      command.emit("end");
    });

    const sql = Array.from(
      { length: 101 },
      (_, index) => `SELECT ${index + 1} AS result_${index + 1}`,
    ).join(";");
    const result = await driver.query(sql, undefined, { hardCap: 200 });

    expect(result.resultSets).toHaveLength(100);
    expect(result.resultSetsTruncated).toBe(true);
    expect(result.resultSets?.[99]).toMatchObject({
      columns: ["result_100"],
      rows: [{ __col_0: 100 }],
    });
  });

  it.each([
    "bounded CALL",
    "bounded script",
    "unbounded CALL",
    "unbounded parameterized CALL",
    "unbounded script",
  ])("shares the retained column budget across every rowset in a %s", async (mode) => {
    const widths = [3000, 1097, 1096, 1];
    const sets = widths.map((width, index) => ({
      fields: Array.from({ length: width }, (_, col) =>
        field(`set_${index}_${col}`),
      ),
      rows: [Array.from({ length: width }, () => index + 1)],
    }));
    const visited: string[] = [];
    const isScript = mode.endsWith("script");
    const isBounded = mode.startsWith("bounded");
    const selectedSets = (sql: string) =>
      sql.startsWith("CALL")
        ? sets
        : sql.startsWith("SELECT")
          ? [sets[Number(sql.slice("SELECT ".length))]]
          : [];
    const { driver, connection } = harness((command, options) => {
      visited.push(options.sql);
      for (const set of selectedSets(options.sql)) {
        command.emit("fields", set.fields);
        for (const row of set.rows) command.emit("result", row);
      }
      command.doneInsert({ affectedRows: 0, serverStatus: 0 });
      command.emit("end");
    });
    connection.query.mockImplementation(async (input) => {
      const sql =
        typeof input === "string" ? input : (input as QueryOptions).sql;
      visited.push(sql);
      const selected = selectedSets(sql);
      if (selected.length === 1) return [selected[0].rows, selected[0].fields];
      if (selected.length > 1)
        return [
          selected.map((set) => set.rows),
          selected.map((set) => set.fields),
        ];
      return [{ affectedRows: 0, serverStatus: 2 }, []];
    });
    const sql = isScript
      ? "SELECT 0; SELECT 1; SELECT 2; SELECT 3; UPDATE items SET done=1"
      : "CALL report()";
    const result = await driver.query(
      sql,
      mode === "unbounded parameterized CALL" ? [1] : undefined,
      isBounded ? { hardCap: 10 } : undefined,
    );

    expect(result.resultSetsTruncated).toBe(true);
    expect(result.resultSets?.map((set) => set.columns.length)).toEqual([
      3000, 1096,
    ]);
    expect(
      result.resultSets?.map((set) => Object.values(set.rows[0])[0]),
    ).toEqual([1, 3]);
    expect(result.rowCount).toBe(2);
    if (isScript) expect(visited).toContain("UPDATE items SET done=1");
  });

  it.each([
    undefined,
    10,
  ])("omits a single rowset exceeding the total column budget (cap=%s)", async (hardCap) => {
    const fields = Array.from({ length: 4097 }, (_, col) =>
      field(`wide_${col}`),
    );
    const { driver, connection } = harness((command) => {
      command.emit("fields", fields);
      command.emit(
        "result",
        Array.from({ length: fields.length }, () => 1),
      );
      command.emit("end");
    });
    connection.query.mockImplementation(async (input) => {
      const sql =
        typeof input === "string" ? input : (input as QueryOptions).sql;
      return sql === "SELECT wide"
        ? [[Array.from({ length: fields.length }, () => 1)], fields]
        : [{ affectedRows: 0, serverStatus: 2 }, []];
    });
    const result = await driver.query("SELECT wide", undefined, { hardCap });
    expect(result).toMatchObject({
      columns: [],
      rows: [],
      resultSetsTruncated: true,
    });
    expect(result.resultSets).toBeUndefined();
  });

  it.each([
    undefined,
    1,
  ])("keeps omitted-result diagnostics when a later statement fails (cap=%s)", async (hardCap) => {
    const fields = Array.from({ length: 4097 }, (_, col) =>
      field(`wide_${col}`),
    );
    const error = Object.assign(new Error("missing table"), {
      code: "ER_NO_SUCH_TABLE",
      errno: 1146,
      sqlState: "42S02",
    });
    const visited: string[] = [];
    const { driver, connection } = harness((command, options) => {
      visited.push(options.sql);
      if (options.sql === "SELECT missing") command.emit("error", error);
      else {
        command.emit("fields", fields);
        command.emit("end");
      }
    });
    connection.query.mockImplementation(async (input) => {
      const sql =
        typeof input === "string" ? input : (input as QueryOptions).sql;
      visited.push(sql);
      if (sql === "SELECT missing") throw error;
      return sql === "SELECT wide"
        ? [[], fields]
        : [{ affectedRows: 0, serverStatus: 2 }, []];
    });
    const result = await driver.query(
      "SELECT wide; SELECT missing; UPDATE items SET done=1",
      undefined,
      { hardCap },
    );
    expect(result).toMatchObject({
      columns: [],
      rows: [],
      resultSetsTruncated: true,
      error: "missing table",
    });
    expect(visited).not.toContain("UPDATE items SET done=1");
    expect(connection.destroy).toHaveBeenCalledOnce();
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
    const { driver, rawQuery, commands } = harness((command) => {
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
    expect(rawQuery).toHaveBeenCalledExactlyOnceWith(commands[0]);
    expect(Reflect.get(commands[0], "_queryOptions")).toMatchObject({
      sql,
      values,
      rowsAsArray: true,
    });
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
    expect(connection.release).not.toHaveBeenCalled();
    expect(connection.destroy).toHaveBeenCalledTimes(2);
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
