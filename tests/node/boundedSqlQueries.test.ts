import { EventEmitter } from "node:events";
import * as mssql from "mssql";
import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { BoundedPostgresQuery } from "../../src/extension/dbDrivers/postgresBoundedQuery";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";

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
  it("PG drains many real Query protocol results without pg buffering rows or accumulating result builders", async () => {
    let peakRetained = 0;
    let peakInternal = 0;
    let peakBuilders = 0;
    let mutationCompleted = false;
    const { driver, client } = postgresHarness((query) => {
      // Exercise pg's real parsing and command handling, not a fake row emitter.
      // A callback is installed (as with query_timeout); native pg would buffer
      // rows despite a row listener without the bounded adapter override.
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
      rowCount: 120,
      affectedRows: 120,
      truncated: true,
    });
    expect(result.rows).toEqual([
      { __col_0: 79000, __col_1: 1 },
      { __col_0: 79001, __col_1: 2 },
      { __col_0: 79002, __col_1: 3 },
    ]);
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
    expect(formatQueryResult(result, 10)).toMatchObject({
      truncated: true,
      rowCount: 120,
      affectedRows: 120,
    });
  });

  it.each([
    "empty SELECT",
    "final DML",
  ])("PG keeps last-command semantics for %s after a large earlier result", async (last) => {
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
    expect(result.rows).toEqual([]);
    expect(result.truncated).toBe(false);
    expect(result.columns).toEqual(last === "empty SELECT" ? ["empty"] : []);
    expect(result.rowCount).toBe(last === "empty SELECT" ? 0 : 42);
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
  ])("PG reports truncation only for the final %i-row result, not discarded earlier sets", async (finalCount) => {
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
      columns: ["final"],
      rowCount: finalCount,
      truncated: finalCount > 3,
    });
    expect(result.rows).toEqual(
      Array.from({ length: Math.min(finalCount, 3) }, (_, index) => ({
        __col_0: index,
      })),
    );
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

  it("PG propagates a late server error and destroys the incomplete connection exactly once", async () => {
    const { driver, client } = postgresHarness((query) => {
      query.handleRowDescription({ fields: [field()] });
      for (let row = 0; row < 20; row++)
        query.handleDataRow({ fields: [String(row)] });
      query.handleError(new Error("late failure"), {});
    });
    await expect(
      driver.query("script", undefined, { hardCap: 2, readOnly: true }),
    ).rejects.toThrow("late failure");
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(client.query).not.toHaveBeenCalledWith("ROLLBACK");
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
    stream = false;
    arrayRowMode = false;
    cancel = vi.fn();
    query = vi.fn(async (_sql: string) => ({
      rowsAffected: [1000],
      recordsets: [],
    }));
  }
  function mssqlHarness(requests: Request[]) {
    let next = 0;
    const driver = new MSSQLDriver({
      id: "ms-budget",
      name: "MSSQL",
      type: "mssql",
      host: "localhost",
    });
    (driver as unknown as { pool: unknown }).pool = {
      connected: true,
      request: () => requests[next++],
    };
    return driver;
  }

  it("MSSQL streams every trigger/plan result, retains the existing first recordset, and waits for mutation completion", async () => {
    const request = new Request();
    let complete!: () => void;
    const finished = new Promise<void>((resolve) => {
      complete = resolve;
    });
    let drained = 0;
    request.query.mockImplementation(async () => {
      for (let set = 0; set < 60; set++) {
        request.emit("recordset", [column("same"), column("same", 1)]);
        for (let row = 0; row < 1000; row++) {
          request.emit("row", [set * 1000 + row, -row]);
          drained++;
        }
        request.emit("rowsaffected", 1000);
      }
      await finished;
      return { rowsAffected: [60000], recordsets: [] };
    });
    const driver = mssqlHarness([request]);
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
    await Promise.resolve();
    expect(request.stream).toBe(true);
    expect(request.arrayRowMode).toBe(true);
    expect(drained).toBe(60000);
    expect(settled).toBe(false);
    expect(request.cancel).not.toHaveBeenCalled();
    complete();
    const result = await pending;
    expect(result).toMatchObject({
      columns: ["same", "same"],
      rowCount: 1000,
      affectedRows: 60000,
      truncated: true,
    });
    expect(result.rows).toEqual([
      { __col_0: 0, __col_1: -0 },
      { __col_0: 1, __col_1: -1 },
      { __col_0: 2, __col_1: -2 },
    ]);
    expect(request.eventNames()).toEqual([]);
    expect(request.query).toHaveBeenCalledWith("UPDATE items SET x = 1");
  });

  it("MSSQL preserves last-GO-batch selection and empty final result metadata", async () => {
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
    expect(
      await mssqlHarness(requests).query(
        "SELECT 1\nGO\nSELECT 2 WHERE 0=1",
        undefined,
        { hardCap: 2 },
      ),
    ).toMatchObject({
      columns: ["empty"],
      rows: [],
      rowCount: 0,
      truncated: false,
    });
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
      truncated: false,
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
    await expect(
      mssqlHarness([request]).query("UPDATE items SET x=1", undefined, {
        hardCap: 2,
      }),
    ).rejects.toThrow("late SQL failure");
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
