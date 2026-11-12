import { EventEmitter } from "node:events";
import * as mssql from "mssql";
import { Client } from "pg";
import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import {
  type BoundedPostgresQuery,
  POSTGRES_MAX_RETAINED_RESULT_COLUMNS,
  POSTGRES_MAX_RETAINED_RESULT_SETS,
} from "../../src/extension/dbDrivers/postgresBoundedQuery";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";
import { mssqlCompletionTokenHarness } from "../helpers/mssqlCompletionTokenHarness";
import { mockMssqlSessionPool } from "../helpers/mssqlQuerySessionHarness";

type WireQuery = BoundedPostgresQuery & {
  handleDataRow(message: { fields: string[] }): void;
  handleCommandComplete(message: { text: string }, connection: object): void;
  handleReadyForQuery(connection: object): void;
  handleError(error: Error, connection: object): void;
};
const field = (name = "id") => ({
  name,
  tableID: 0,
  columnID: 0,
  dataTypeID: 23,
  dataTypeSize: 4,
  dataTypeModifier: -1,
  format: "text",
});

function postgresHarness(run: (query: WireQuery) => void) {
  let active: WireQuery | undefined;
  const client = {
    query: vi.fn((query: WireQuery | string) => {
      if (typeof query === "string") return Promise.resolve({ rows: [] });
      active = query;
      queueMicrotask(() => run(query));
      return query;
    }),
    release: vi.fn((destroy?: boolean) => {
      if (destroy)
        queueMicrotask(() =>
          active?.handleError(new Error("connection terminated"), {}),
        );
    }),
  };
  const driver = new PostgresDriver({
    id: "pg-budget",
    name: "PG",
    type: "pg",
    host: "localhost",
  });
  (driver as unknown as { pool: unknown }).pool = {
    connect: vi.fn(async () => client),
    totalCount: 0,
    idleCount: 1,
    waitingCount: 0,
  };
  return { driver, client };
}

describe("driver-owned result budgets", () => {
  it("PG waits for rollback after ErrorResponse before releasing a previously idle client", async () => {
    const client = new Client();
    Reflect.set(client, "readyForQuery", true);
    Reflect.set(client, "_txStatus", "I");
    const getTransactionStatus = () =>
      Reflect.get(client, "getTransactionStatus").call(client);
    const release = vi.fn();
    Reflect.set(client, "release", release);
    const deliver = (method: string, message: unknown) =>
      Reflect.get(client, method).call(client, message);
    let errorDelivered!: () => void;
    const failed = new Promise<void>((resolve) => {
      errorDelivered = resolve;
    });
    const sent: string[] = [];
    Reflect.get(client, "connection").query = (sql: string) => {
      sent.push(sql);
      if (sql === "ROLLBACK") return;
      queueMicrotask(() => {
        deliver("_handleCommandComplete", { text: "BEGIN" });
        deliver("_handleRowDescription", { fields: [field()] });
        deliver("_handleDataRow", { fields: ["1"] });
        deliver("_handleCommandComplete", { text: "SELECT 1" });
        deliver(
          "_handleErrorMessage",
          Object.assign(new Error("division by zero"), { code: "22012" }),
        );
        errorDelivered();
      });
    };
    const query = vi.spyOn(client, "query");
    const driver = new PostgresDriver({
      id: "ready-race",
      name: "PG",
      type: "pg",
    });
    Reflect.set(driver, "pool", {
      connect: async () => client,
      totalCount: 0,
      idleCount: 1,
      waitingCount: 0,
    });
    const sql = "BEGIN; SELECT 1; SELECT 1/0";
    const pending = driver.query(sql, undefined, { hardCap: 2 });
    await failed;
    await vi.waitFor(() => expect(query).toHaveBeenCalledWith("ROLLBACK"));
    expect(getTransactionStatus()).toBe("I");
    expect(release).not.toHaveBeenCalled();
    expect(sent).toEqual([sql]);
    deliver("_handleReadyForQuery", { status: "E" });
    expect(sent).toEqual([sql, "ROLLBACK"]);
    expect(release).not.toHaveBeenCalled();
    deliver("_handleCommandComplete", { text: "ROLLBACK" });
    deliver("_handleReadyForQuery", { status: "I" });
    expect(await pending).toMatchObject({
      rows: [{ __col_0: 1 }],
      error: "division by zero",
    });
    expect(getTransactionStatus()).toBe("I");
    expect(release).toHaveBeenCalledExactlyOnceWith();
  });

  it("PG exposes every Query protocol result without pg buffering rows or accumulating result builders", async () => {
    let peakRetained = 0;
    let peakInternal = 0;
    let peakBuilders = 0;
    let mutationCompleted = false;
    const { driver, client } = postgresHarness((query) => {
      for (let set = 0; set < 80; set++) {
        query.handleRowDescription({ fields: [field("same"), field("same")] });
        for (let row = 0; row < 120; row++) {
          query.handleDataRow({
            fields: [String(set * 1000 + row), String(row + 1)],
          });
          peakRetained = Math.max(peakRetained, query.retained.rows.length);
          peakInternal = Math.max(peakInternal, query._result.rows.length);
          peakBuilders = Math.max(
            peakBuilders,
            Array.isArray(query._results) ? query._results.length : 1,
          );
        }
        query.handleCommandComplete(
          { text: set === 79 ? "UPDATE 120" : "SELECT 120" },
          {},
        );
      }
      mutationCompleted = true;
      query.handleReadyForQuery({});
    });
    const sql =
      "SELECT * FROM huge; UPDATE items SET active = true RETURNING id, id";
    const result = await driver.query(sql, undefined, {
      hardCap: 3,
      requestToken: 21,
    });
    expect(mutationCompleted).toBe(true);
    expect(peakRetained).toBe(3);
    expect(peakInternal).toBe(0);
    expect(peakBuilders).toBe(1);
    expect(result).toMatchObject({
      columns: ["same", "same"],
      rowCount: 9600,
      affectedRows: 120,
      truncated: true,
    });
    expect(result.rows).toEqual([
      { __col_0: 0, __col_1: 1 },
      { __col_0: 1, __col_1: 2 },
      { __col_0: 2, __col_1: 3 },
    ]);
    expect(result.resultSets).toHaveLength(80);
    expect(result.resultSets?.[0]).toMatchObject({
      id: "result-1",
      rowCount: 120,
      truncated: true,
      rows: [
        { __col_0: 0, __col_1: 1 },
        { __col_0: 1, __col_1: 2 },
        { __col_0: 2, __col_1: 3 },
      ],
    });
    expect(result.resultSets?.[79]).toMatchObject({
      id: "result-80",
      rowCount: 120,
      truncated: true,
      rows: [],
    });
    expect(
      result.resultSets?.reduce(
        (total, resultSet) => total + resultSet.rows.length,
        0,
      ),
    ).toBeLessThanOrEqual(3);
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
    expect(formatQueryResult(result, 10)).toMatchObject({
      truncated: true,
      rowCount: 9600,
      affectedRows: 120,
    });
  });

  it("PG bounds retained result-set metadata by count and total column count", async () => {
    const { driver } = postgresHarness((query) => {
      for (let set = 0; set < POSTGRES_MAX_RETAINED_RESULT_SETS + 1; set++) {
        query.handleRowDescription({ fields: [field(`set_${set}`)] });
        query.handleCommandComplete({ text: "SELECT 0" }, {});
      }
      query.handleReadyForQuery({});
    });

    const manySets = await driver.query("many result sets", undefined, {
      hardCap: 10,
    });
    expect(manySets.resultSets).toHaveLength(POSTGRES_MAX_RETAINED_RESULT_SETS);
    expect(manySets.resultSetsTruncated).toBe(true);

    const wideDriverHarness = postgresHarness((query) => {
      query.handleRowDescription({
        fields: Array.from(
          { length: POSTGRES_MAX_RETAINED_RESULT_COLUMNS + 1 },
          (_, index) => field(`column_${index}`),
        ),
      });
      query.handleCommandComplete({ text: "SELECT 0" }, {});
      query.handleReadyForQuery({});
    });
    const wideSet = await wideDriverHarness.driver.query(
      "one very wide result set",
      undefined,
      { hardCap: 10 },
    );
    expect(wideSet).toMatchObject({
      columns: [],
      rows: [],
      resultSetsTruncated: true,
    });
    expect(wideSet.columnMeta).toBeUndefined();
  });

  it.each([
    "empty SELECT",
    "final DML",
  ])("PG keeps earlier rowsets available after a %s", async (last) => {
    const { driver } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      for (let row = 0; row < 20; row++)
        query.handleDataRow({ fields: [String(row)] });
      query.handleCommandComplete({ text: "SELECT 20" }, {});
      if (last === "empty SELECT")
        query.handleRowDescription({ fields: [field("empty")] });
      query.handleCommandComplete(
        { text: last === "empty SELECT" ? "SELECT 0" : "DELETE 42" },
        {},
      );
      query.handleReadyForQuery({});
    });
    const result = await driver.query("script", undefined, { hardCap: 2 });
    expect(result.rows).toEqual([{ __col_0: 0 }, { __col_0: 1 }]);
    expect(result.truncated).toBe(true);
    expect(result.columns).toEqual(["id"]);
    expect(result.rowCount).toBe(20);
    expect(result.resultSets).toHaveLength(last === "empty SELECT" ? 2 : 1);
    expect(result.resultSets?.[0]).toMatchObject({
      columns: ["id"],
      rowCount: 20,
      truncated: true,
    });
    if (last === "empty SELECT") {
      expect(result.resultSets?.[1]).toMatchObject({
        columns: ["empty"],
        rowCount: 0,
      });
    } else {
      expect(result.affectedRows).toBe(42);
    }
  });

  it("PG maps every rowset returned by the unbounded pg client path", async () => {
    const driver = new PostgresDriver({
      id: "pg-unbounded",
      name: "PG",
      type: "pg",
      host: "localhost",
    });
    const client = {
      query: vi.fn(async () => [
        {
          command: "SELECT",
          fields: [field("first_result")],
          rows: [[1]],
          rowCount: 1,
        },
        {
          command: "SELECT",
          fields: [field("second_result")],
          rows: [[2]],
          rowCount: 1,
        },
      ]),
      release: vi.fn(),
    };
    (driver as unknown as { pool: unknown }).pool = {
      connect: vi.fn(async () => client),
      totalCount: 0,
      idleCount: 1,
      waitingCount: 0,
    };

    const result = await driver.query(
      "SELECT 1 AS first_result; SELECT 2 AS second_result",
    );

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
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
  });

  it("PG marks timestamp results temporal and leaves text untyped", async () => {
    const driver = new PostgresDriver({
      id: "pg-temporal-meta",
      name: "PG",
      type: "pg",
      host: "localhost",
    });
    const client = {
      query: vi.fn(async () => ({
        command: "SELECT",
        fields: [
          { ...field("ts"), dataTypeID: 1114 },
          { ...field("tstz"), dataTypeID: 1184 },
          { ...field("label"), dataTypeID: 25 },
        ],
        rows: [
          [
            "2024-01-01 10:00:00",
            "2024-01-01 10:00:00+00",
            "2024-01-01T10:00:00.000Z",
          ],
        ],
        rowCount: 1,
      })),
      release: vi.fn(),
    };
    (driver as unknown as { pool: unknown }).pool = {
      connect: vi.fn(async () => client),
      totalCount: 0,
      idleCount: 1,
      waitingCount: 0,
    };

    const result = await driver.query("SELECT ts, tstz, label FROM t");

    expect(result.columnMeta).toEqual([
      { category: "datetime" },
      { category: "datetime" },
      { category: null },
    ]);
  });

  it("PG preserves completed rowsets and reports a later statement error", async () => {
    const { driver } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field("first_result")] });
      query.handleDataRow({ fields: ["1"] });
      query.handleCommandComplete({ text: "SELECT 1" }, {});
      query.handleRowDescription({ fields: [field("second_result")] });
      query.handleError(new Error("division by zero"), {});
    });

    const result = await driver.query(
      "SELECT 1 AS first_result; SELECT 1/0 AS second_result",
      undefined,
      { hardCap: 10 },
    );

    expect(result).toMatchObject({
      columns: ["first_result"],
      rows: [{ __col_0: 1 }],
      rowCount: 1,
      error: "division by zero",
      resultSets: [
        {
          id: "result-1",
          columns: ["first_result"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
        },
        {
          id: "result-2",
          columns: ["second_result"],
          rows: [],
          rowCount: 0,
          partial: true,
        },
      ],
    });
  });

  it.each([
    "SHOW",
    "EXPLAIN",
  ])("PG counts drained rows for %s tags without a native row count", async (command) => {
    const { driver } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      for (let row = 0; row < 20; row++)
        query.handleDataRow({ fields: [String(row)] });
      query.handleCommandComplete({ text: command }, {});
      query.handleReadyForQuery({});
    });
    const result = await driver.query(command, undefined, { hardCap: 2 });
    expect(result).toMatchObject({ rowCount: 20, truncated: true });
    expect(result.rows).toHaveLength(2);
    expect(result.affectedRows).toBeUndefined();
  });

  it.each([
    1, 3, 4,
  ])("PG enforces a shared retained-row budget across result sets with a %i-row final set", async (finalCount) => {
    const { driver } = postgresHarness((query) => {
      for (const [name, count] of [
        ["earlier", 20],
        ["final", finalCount],
      ] as const) {
        query.handleRowDescription({ fields: [field(name)] });
        for (let row = 0; row < count; row++) {
          query.handleDataRow({ fields: [String(row)] });
          expect(query._result.rows).toHaveLength(0);
          expect(query.retained.rows.length).toBeLessThanOrEqual(3);
          expect(Array.isArray(query._results)).toBe(false);
        }
        query.handleCommandComplete({ text: `SELECT ${count}` }, {});
      }
      query.handleReadyForQuery({});
    });
    const result = await driver.query(
      "SELECT earlier; SELECT final",
      undefined,
      {
        hardCap: 3,
      },
    );
    expect(result).toMatchObject({
      columns: ["earlier"],
      rowCount: 20 + finalCount,
      truncated: true,
    });
    expect(result.rows).toEqual(
      Array.from({ length: 3 }, (_, index) => ({
        __col_0: index,
      })),
    );
    expect(result.resultSets).toHaveLength(2);
    expect(result.resultSets?.[1]).toMatchObject({
      columns: ["final"],
      rowCount: finalCount,
      rows: [],
      truncated: true,
    });
    expect(
      result.resultSets?.reduce(
        (total, resultSet) => total + resultSet.rows.length,
        0,
      ),
    ).toBe(3);
  });

  it("PG keeps draining a bounded writable query until real pg ReadyForQuery completion", async () => {
    let finish!: () => void;
    let started!: () => void;
    const ready = new Promise<void>((resolve) => {
      started = resolve;
    });
    const { driver, client } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      for (let row = 0; row < 20; row++)
        query.handleDataRow({ fields: [String(row)] });
      query.handleCommandComplete({ text: "UPDATE 20" }, {});
      finish = () => query.handleReadyForQuery({});
      started();
    });
    let settled = false;
    const pending = driver
      .query("UPDATE items SET active = true RETURNING id", undefined, {
        hardCap: 2,
      })
      .then((result) => {
        settled = true;
        return result;
      });
    await ready;
    expect(settled).toBe(false);
    expect(client.release).not.toHaveBeenCalled();
    finish();
    expect(await pending).toMatchObject({
      rowCount: 20,
      affectedRows: 20,
      truncated: true,
    });
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
  });

  it("PG preserves partial rows from a single SELECT before a late server error", async () => {
    const { driver, client } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      for (let row = 0; row < 20; row++)
        query.handleDataRow({ fields: [String(row)] });
      query.handleError(new Error("late failure"), {});
    });
    const result = await driver.query(
      "SELECT id FROM rows_that_fail_late",
      undefined,
      { hardCap: 2, readOnly: true },
    );
    expect(result).toMatchObject({
      columns: ["id"],
      rows: [{ __col_0: 0 }, { __col_0: 1 }],
      rowCount: 20,
      truncated: true,
      error: "late failure",
      resultSets: [
        {
          id: "result-1",
          columns: ["id"],
          rowCount: 20,
          truncated: true,
          partial: true,
        },
      ],
    });
    expect(client.query).toHaveBeenCalledWith("ROLLBACK");
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
  });

  it("PG marks streamed rows partial when a row parser fails before ReadyForQuery", async () => {
    const { driver } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      query.handleDataRow({ fields: ["17"] });
      const resultBuilder = Reflect.get(query, "_result") as unknown as {
        _parsers: Array<(value: unknown) => unknown>;
      };
      resultBuilder._parsers[0] = () => {
        throw new Error("row parser failed");
      };
      query.handleDataRow({ fields: ["malformed"] });
      query.handleCommandComplete({ text: "SELECT 2" }, {});
      query.handleReadyForQuery({});
    });

    const result = await driver.query(
      "SELECT value FROM malformed_rows",
      undefined,
      { hardCap: 10 },
    );

    expect(result).toMatchObject({
      columns: ["id"],
      rows: [{ __col_0: 17 }],
      rowCount: 1,
      error: "row parser failed",
      resultSets: [
        {
          partial: true,
          rowCount: 1,
        },
      ],
    });
  });

  it("PG keeps earlier complete sets and marks sets skipped after parser failure partial", async () => {
    const { driver } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field("before")] });
      query.handleDataRow({ fields: ["1"] });
      query.handleCommandComplete({ text: "SELECT 1" }, {});
      query.handleRowDescription({ fields: [field("failing")] });
      query.handleDataRow({ fields: ["10"] });
      const builder = Reflect.get(query, "_result") as unknown as {
        _parsers: Array<(value: unknown) => unknown>;
      };
      builder._parsers[0] = () => {
        throw new Error("parser failed");
      };
      query.handleDataRow({ fields: ["bad"] });
      query.handleCommandComplete({ text: "SELECT 2" }, {});
      query.handleRowDescription({ fields: [field("after")] });
      query.handleDataRow({ fields: ["100"] });
      query.handleCommandComplete({ text: "SELECT 1" }, {});
      query.handleReadyForQuery({});
    });
    const result = await driver.query(
      "SELECT before; SELECT failing; SELECT after",
      undefined,
      { hardCap: 10 },
    );
    expect(result.error).toBe("parser failed");
    expect(result.resultSets).toMatchObject([
      { rows: [{ __col_0: 1 }], rowCount: 1 },
      { rows: [{ __col_0: 10 }], rowCount: 1, partial: true },
      { rows: [], rowCount: 0, partial: true },
    ]);
    expect(result.resultSets?.[0]).not.toHaveProperty("partial", true);
    const formatted = formatQueryResult(result, 10);
    expect(formatted.resultSets?.[1].partial).toBe(true);
    expect(formatted.resultSets?.[2].partial).toBe(true);
  });

  it("PG retains a partial set after a later command without RowDescription", async () => {
    const { driver } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      query.handleDataRow({ fields: ["17"] });
      const builder = Reflect.get(query, "_result") as unknown as {
        _parsers: Array<(value: unknown) => unknown>;
      };
      builder._parsers[0] = () => {
        throw new Error("parser failed");
      };
      query.handleDataRow({ fields: ["bad"] });
      query.handleCommandComplete({ text: "SELECT 2" }, {});
      query.handleCommandComplete({ text: "UPDATE 1" }, {});
      query.handleReadyForQuery({});
    });
    const result = await driver.query(
      "SELECT failing; UPDATE mock",
      undefined,
      { hardCap: 10 },
    );
    expect(result.resultSets).toMatchObject([
      { rows: [{ __col_0: 17 }], rowCount: 1, partial: true },
    ]);
    expect(result.error).toBe("parser failed");
  });

  it("PG does not label a parser failure before any decoded row as a completed result", async () => {
    const { driver, client } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      const builder = Reflect.get(query, "_result") as unknown as {
        _parsers: Array<(value: unknown) => unknown>;
      };
      builder._parsers[0] = () => {
        throw new Error("first row parser failed");
      };
      query.handleDataRow({ fields: ["bad"] });
      query.handleCommandComplete({ text: "SELECT 1" }, {});
      query.handleReadyForQuery({});
    });
    await expect(
      driver.query("SELECT failing", undefined, { hardCap: 10 }),
    ).rejects.toThrow("first row parser failed");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("PG still supports targeted cancellation of a draining bounded query", async () => {
    let started!: () => void;
    const ready = new Promise<void>((resolve) => {
      started = resolve;
    });
    const { driver, client } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      for (let row = 0; row < 20; row++)
        query.handleDataRow({ fields: [String(row)] });
      started();
    });
    const pending = driver.query("SELECT * FROM huge", undefined, {
      hardCap: 2,
      requestToken: 31,
    });
    const rejected = expect(pending).rejects.toThrow("connection terminated");
    await ready;
    expect(client.release).not.toHaveBeenCalled();
    await driver.cancelCurrentOperation({
      operationName: "query",
      reason: "timeout",
      requestToken: 31,
    });
    await rejected;
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
  });

  const column = (name: string, index = 0) => ({
    name,
    index,
    type: mssql.Int,
    nullable: false,
    identity: false,
    readOnly: true,
  });
  class Request extends EventEmitter {
    overrides = {};
    stream = false;
    arrayRowMode = false;
    cancel = vi.fn();
    query = vi.fn(async (_sql: string) => ({
      rowsAffected: [1000],
      recordsets: [],
    }));
    batch = vi.fn((sql: string) => this.query(sql));
  }
  function mssqlHarness(
    requests: Request[],
    setup?: (connection: object) => void,
  ) {
    const driver = new MSSQLDriver({
      id: "ms-budget",
      name: "MSSQL",
      type: "mssql",
      host: "localhost",
    });
    const pool = mockMssqlSessionPool(requests);
    setup?.(pool.connections[0]);
    (driver as unknown as { pool: unknown }).pool = pool;
    return driver;
  }

  it("MSSQL streams every trigger/plan result, retains the existing first recordset, and waits for mutation completion", async () => {
    const request = new Request();
    let complete!: () => void;
    const finished = new Promise<void>((resolve) => {
      complete = resolve;
    });
    let drained = 0;
    let tokens!: ReturnType<typeof mssqlCompletionTokenHarness>;
    const driver = mssqlHarness([request], (connection) => {
      tokens = mssqlCompletionTokenHarness(connection, request);
    });
    request.query.mockImplementation(async () => {
      for (let set = 0; set < 60; set++) {
        request.emit("recordset", [column("same"), column("same", 1)]);
        for (let row = 0; row < 1000; row++) {
          request.emit("row", [set * 1000 + row, -row]);
          drained++;
        }
        await tokens.complete(0xc1, 1000); // SELECT rowset, not DML.
      }
      await finished;
      await tokens.complete(0xc5, 60000); // Parent UPDATE completion.
      return { rowsAffected: [60000], recordsets: [] };
    });
    let settled = false;
    const pending = driver
      .query("UPDATE items SET x = 1", undefined, {
        hardCap: 3,
        requestToken: 22,
      })
      .then((result) => {
        settled = true;
        return result;
      });
    await vi.waitFor(() => {
      expect(request.stream).toBe(true);
      expect(drained).toBe(60000);
    });
    expect(request.arrayRowMode).toBe(true);
    expect(drained).toBe(60000);
    expect(settled).toBe(false);
    expect(request.cancel).not.toHaveBeenCalled();
    complete();
    const result = await pending;
    expect(result).toMatchObject({
      columns: ["same", "same"],
      rowCount: 60000,
      affectedRows: 60000,
    });
    expect(result.resultSets).toHaveLength(60);
    expect(
      result.resultSets?.every(
        (set) => set.rowCount === 1000 && set.truncated === true,
      ),
    ).toBe(true);
    expect(
      result.resultSets?.reduce((total, set) => total + set.rows.length, 0),
    ).toBe(3);
    expect(result.rows).toEqual([
      { __col_0: 0, __col_1: -0 },
      { __col_0: 1, __col_1: -1 },
      { __col_0: 2, __col_1: -2 },
    ]);
    expect(request.eventNames()).toEqual([]);
    expect(Object.hasOwn(tokens.native, "emit")).toBe(false);
    expect(request.query).toHaveBeenCalledWith("UPDATE items SET x = 1");
  });

  it("MSSQL preserves first-GO selection and every later empty result set", async () => {
    const requests = [new Request(), new Request()];
    requests[0].query.mockImplementation(async () => {
      requests[0].emit("recordset", [column("before")]);
      for (let row = 0; row < 100; row++) requests[0].emit("row", [row]);
      return { rowsAffected: [100], recordsets: [] };
    });
    requests[1].query.mockImplementation(async () => {
      requests[1].emit("recordset", [column("empty")]);
      return { rowsAffected: [0], recordsets: [] };
    });
    const result = await mssqlHarness(requests).query(
      "SELECT 1\nGO\nSELECT 2 WHERE 0=1",
      undefined,
      { hardCap: 2 },
    );
    expect(result).toMatchObject({
      columns: ["before"],
      rows: [{ __col_0: 0 }, { __col_0: 1 }],
      rowCount: 100,
    });
    expect(result.resultSets).toMatchObject([
      {
        id: "result-1",
        columns: ["before"],
        rows: [{ __col_0: 0 }, { __col_0: 1 }],
        rowCount: 100,
        truncated: true,
      },
      {
        id: "result-2",
        columns: ["empty"],
        rows: [],
        rowCount: 0,
        truncated: false,
      },
    ]);
    expect(
      requests.every(
        (request) => request.stream && request.eventNames().length === 0,
      ),
    ).toBe(true);
  });

  it("MSSQL handles chunked XML/JSON objects emitted in array-row streaming mode", async () => {
    const request = new Request();
    request.query.mockImplementation(async () => {
      request.emit("recordset", [column("JSON_result")]);
      request.emit("row", { "0": '[{"id":1},{"id":2}]' });
      return { rowsAffected: [1], recordsets: [] };
    });
    const result = await mssqlHarness([request]).query(
      "SELECT id FROM items FOR JSON PATH",
      undefined,
      { hardCap: 1 },
    );
    expect(result).toMatchObject({
      columns: ["JSON_result"],
      rows: [{ __col_0: '[{"id":1},{"id":2}]' }],
      rowCount: 1,
    });
    expect(result.resultSets?.[0]).toMatchObject({
      truncated: false,
      rowCount: 1,
    });
  });

  it("MSSQL propagates streaming errors only after completion and removes listeners", async () => {
    const request = new Request();
    let drained = false;
    request.query.mockImplementation(async () => {
      request.emit("recordset", [column("id")]);
      for (let row = 0; row < 100; row++) request.emit("row", [row]);
      request.emit("error", new Error("late SQL failure"));
      await Promise.resolve();
      drained = true;
      return { rowsAffected: [100], recordsets: [] };
    });
    const result = await mssqlHarness([request]).query(
      "UPDATE items SET x=1",
      undefined,
      {
        hardCap: 2,
      },
    );
    expect(result.error).toBe("late SQL failure");
    expect(result.rows).toEqual([{ __col_0: 0 }, { __col_0: 1 }]);
    expect(result.resultSets?.[0]).toMatchObject({
      partial: true,
      truncated: true,
      rowCount: 100,
    });
    expect(drained).toBe(true);
    expect(request.cancel).not.toHaveBeenCalled();
    expect(request.eventNames()).toEqual([]);
  });

  it("MSSQL explicit cancellation still terminates a bounded draining request", async () => {
    const request = new Request();
    let finish!: () => void;
    request.query.mockImplementation(
      () =>
        new Promise((resolve) => {
          request.emit("recordset", [column("id")]);
          for (let row = 0; row < 20; row++) request.emit("row", [row]);
          finish = () => {
            request.emit("error", new Error("cancelled"));
            resolve({ rowsAffected: [], recordsets: [] });
          };
        }),
    );
    request.cancel.mockImplementation(() => finish());
    const driver = mssqlHarness([request]);
    const pending = driver.query("SELECT * FROM huge", undefined, {
      hardCap: 2,
      requestToken: 10,
    });
    const rejected = expect(pending).rejects.toThrow("cancelled");
    await vi.waitFor(() => expect(request.query).toHaveBeenCalledOnce());
    expect(request.cancel).not.toHaveBeenCalled();
    await driver.cancelCurrentOperation({
      operationName: "query",
      reason: "manual",
      requestToken: 10,
    });
    await rejected;
    expect(request.cancel).toHaveBeenCalledOnce();
    expect(request.eventNames()).toEqual([]);
  });
});
