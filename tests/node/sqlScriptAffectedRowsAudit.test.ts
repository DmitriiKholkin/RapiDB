import { EventEmitter } from "node:events";
import { createRequire } from "node:module";
import mssql from "mssql";
import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { BoundedPostgresQuery } from "../../src/extension/dbDrivers/postgresBoundedQuery";
import {
  MockMssqlRequest,
  mockMssqlSessionPool,
} from "../helpers/mssqlQuerySessionHarness";

type WireQuery = BoundedPostgresQuery & {
  handleRowDescription(message: unknown): void;
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

function postgresDriver(client: object) {
  const driver = new PostgresDriver({
    id: "pg-affected-rows",
    name: "PG",
    type: "pg",
    host: "localhost",
  });
  Reflect.set(driver, "pool", {
    connect: vi.fn(async () => client),
    totalCount: 0,
    idleCount: 1,
    waitingCount: 0,
  });
  return driver;
}

function boundedPostgres(run: (query: WireQuery) => void) {
  const client = {
    query: vi.fn((query: WireQuery | string) => {
      if (typeof query === "string") return Promise.resolve({ rows: [] });
      queueMicrotask(() => run(query));
      return query;
    }),
    release: vi.fn(),
  };
  return postgresDriver(client);
}

describe("PostgreSQL script affectedRows", () => {
  it("sums every DML command tag instead of reporting the last statement", async () => {
    const driver = boundedPostgres((query) => {
      query.handleCommandComplete({ text: "UPDATE 5" }, {});
      query.handleCommandComplete({ text: "UPDATE 0" }, {});
      query.handleReadyForQuery({});
    });
    const result = await driver.query("UPDATE a ...; UPDATE b ...", undefined, {
      hardCap: 10,
    });
    expect(result).toMatchObject({ rowCount: 5, affectedRows: 5 });
  });

  it("keeps the DML total when the script ends with DDL", async () => {
    const driver = boundedPostgres((query) => {
      query.handleCommandComplete({ text: "INSERT 0 3" }, {});
      query.handleCommandComplete({ text: "CREATE TABLE" }, {});
      query.handleReadyForQuery({});
    });
    const result = await driver.query(
      "INSERT ...; CREATE TABLE t()",
      undefined,
      {
        hardCap: 10,
      },
    );
    expect(result).toMatchObject({ rowCount: 3, affectedRows: 3 });
  });

  it("does not count SELECT rows as affected nor replace result-set row counts", async () => {
    const driver = boundedPostgres((query) => {
      query.handleCommandComplete({ text: "INSERT 0 3" }, {});
      query.handleRowDescription({ fields: [field()] });
      query.handleDataRow({ fields: ["1"] });
      query.handleDataRow({ fields: ["2"] });
      query.handleCommandComplete({ text: "SELECT 2" }, {});
      query.handleCommandComplete({ text: "SELECT 9" }, {});
      query.handleCommandComplete({ text: "DELETE 4" }, {});
      query.handleReadyForQuery({});
    });
    const result = await driver.query(
      "INSERT ...; SELECT id FROM t; CREATE TABLE c AS SELECT ...; DELETE ...",
      undefined,
      { hardCap: 10 },
    );
    expect(result).toMatchObject({ rowCount: 2, affectedRows: 7 });
    expect(result.resultSets).toHaveLength(1);
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 2,
      rows: [{ __col_0: 1 }, { __col_0: 2 }],
    });
  });

  it("counts RETURNING DML once, by its command tag", async () => {
    const driver = boundedPostgres((query) => {
      query.handleCommandComplete({ text: "UPDATE 5" }, {});
      query.handleRowDescription({ fields: [field()] });
      query.handleDataRow({ fields: ["7"] });
      query.handleCommandComplete({ text: "DELETE 1" }, {});
      query.handleReadyForQuery({});
    });
    const result = await driver.query(
      "UPDATE a ...; DELETE FROM b ... RETURNING id",
      undefined,
      { hardCap: 10 },
    );
    expect(result).toMatchObject({ rowCount: 1, affectedRows: 6 });
  });

  it("reports no affectedRows when a later statement fails", async () => {
    const driver = boundedPostgres((query) => {
      query.handleCommandComplete({ text: "UPDATE 5" }, {});
      query.handleRowDescription({ fields: [field()] });
      query.handleDataRow({ fields: ["1"] });
      query.handleCommandComplete({ text: "SELECT 1" }, {});
      query.handleRowDescription({ fields: [field("boom")] });
      query.handleError(new Error("division by zero"), {});
    });
    const result = await driver.query(
      "UPDATE a ...; SELECT 1; SELECT 1/0",
      undefined,
      { hardCap: 10 },
    );
    expect(result.error).toBe("division by zero");
    expect(result.affectedRows).toBeUndefined();
  });

  it("sums DML row counts on the unbounded multi-statement path", async () => {
    const client = {
      query: vi.fn(async () => [
        { command: "UPDATE", fields: [], rows: [], rowCount: 5 },
        { command: "UPDATE", fields: [], rows: [], rowCount: 0 },
      ]),
      release: vi.fn(),
    };
    const result = await postgresDriver(client).query(
      "UPDATE a ...; UPDATE b ...",
    );
    expect(result.rowCount).toBe(5);
  });
});

type CompletionEvent = "done" | "doneInProc" | "doneProc";
const require = createRequire(import.meta.url);
const { RequestTokenHandler } = require("tedious/lib/token/handler") as {
  RequestTokenHandler: new (
    connection: object,
    request: object,
  ) => Record<string, (token: unknown) => void>;
};
const doneParsers = require("tedious/lib/token/done-token-parser") as Record<
  string,
  (buffer: Buffer, offset: number, options: object) => { value: unknown }
>;

function mssqlHarness(count = 1) {
  const requests = Array.from({ length: count }, () => {
    const request = new MockMssqlRequest();
    Object.assign(request, {
      _setCurrentRequest: vi.fn((_native: EventEmitter) => request),
    });
    return request;
  });
  const driver = new MSSQLDriver({
    id: "mssql-affected-rows",
    name: "MSSQL",
    type: "mssql",
  });
  const pool = mockMssqlSessionPool(requests);
  // The completion contract now needs the metadata that tedious drops from
  // native emit. Expose the parser boundary in this fixture, not fake extra
  // emit arguments absent from the real SDK. Assertions remain unchanged.
  Object.assign(pool.connections[0], {
    config: { options: {} },
    createTokenStreamParser: (_message: unknown, handler: object) => handler,
  });
  Object.assign(driver, { pool });
  return { driver, requests };
}

// Mirrors node-mssql: its DONE/DONEINPROC handler emits `rowsaffected` for
// every counted token (SELECTs and trigger statements included) before the
// driver's completion hook observes the same native event.
function streamTokens(
  request: MockMssqlRequest,
  script: (
    token: (event: CompletionEvent, count?: number, curCmd?: number) => void,
  ) => void,
) {
  request.execute.mockImplementation(async (_sql, connection) => {
    const native = new EventEmitter();
    Object.assign(native, { rowCount: 0, rst: [] });
    Reflect.get(request, "_setCurrentRequest").call(request, native);
    const handler = new RequestTokenHandler(connection, native);
    Reflect.get(connection, "createTokenStreamParser").call(
      connection,
      [],
      handler,
    );
    const rowsAffected: number[] = [];
    const sdkDone = (rowCount?: number) => {
      if (rowCount == null) return;
      rowsAffected.push(rowCount);
      request.emit("rowsaffected", rowCount);
    };
    native.on("done", sdkDone);
    native.on("doneInProc", sdkDone);
    script((event, rowCount, curCmd = 0xc5) => {
      const bytes = Buffer.alloc(12);
      bytes.writeUInt16LE(1 | (rowCount === undefined ? 0 : 16), 0);
      bytes.writeUInt16LE(curCmd, 2);
      bytes.writeBigUInt64LE(BigInt(rowCount ?? 0), 4);
      const token = doneParsers[`${event}Parser`](bytes, 0, {
        tdsVersion: "7_4",
      }).value;
      handler[`on${event[0].toUpperCase()}${event.slice(1)}`](token);
    });
    return { recordsets: [], recordset: [], rowsAffected, output: {} };
  });
}

describe("MSSQL script affectedRows", () => {
  it("sums top-level DML counts instead of reporting the last statement", async () => {
    const { driver, requests } = mssqlHarness();
    streamTokens(requests[0], (token) => {
      token("done", 5);
      token("done", 0);
    });
    const result = await driver.query("UPDATE a ...; UPDATE b ...", undefined, {
      hardCap: 10,
    });
    expect(result).toMatchObject({ rowCount: 5, affectedRows: 5 });
  });

  it("does not add trigger DONEINPROC counts to the statement count", async () => {
    const { driver, requests } = mssqlHarness();
    streamTokens(requests[0], (token) => {
      token("doneInProc", 7);
      token("done", 5);
      token("done", 0);
    });
    const result = await driver.query(
      "UPDATE audited ...; UPDATE b ...",
      undefined,
      { hardCap: 10 },
    );
    expect(result).toMatchObject({ rowCount: 5, affectedRows: 5 });
  });

  it("excludes SELECT rowset counts and keeps each result set's own counts", async () => {
    const { driver, requests } = mssqlHarness();
    const request = requests[0];
    streamTokens(request, (token) => {
      token("done", 5);
      request.emit("recordset", [{ name: "id", type: mssql.Int }]);
      request.emit("row", [1]);
      request.emit("row", [2]);
      token("done", 2, 0xc1);
      token("done", 1, 0xc4);
    });
    const result = await driver.query(
      "UPDATE a ...; SELECT id FROM t; DELETE c ...",
      undefined,
      { hardCap: 10 },
    );
    expect(result).toMatchObject({ rowCount: 2, affectedRows: 6 });
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 2,
      affectedRows: 2,
      rows: [{ __col_0: 1 }, { __col_0: 2 }],
    });
  });

  it("does not claim an exact sum across ambiguous procedure boundaries", async () => {
    const { driver, requests } = mssqlHarness();
    streamTokens(requests[0], (token) => {
      token("doneInProc", 3);
      token("doneProc");
      token("done", 4);
    });
    const result = await driver.query("EXEC p; UPDATE b ...", undefined, {
      hardCap: 10,
    });
    // The same DONEINPROC -> DONEPROC -> DONE stream can come from a
    // procedure inside an UPDATE trigger. DONEPROC is not a top-level
    // boundary, so promoting its body count would double-count that case.
    expect(result).toMatchObject({
      rowCount: 0,
      affectedRows: undefined,
      affectedRowsUnknown: true,
    });
  });

  it("reports zero when NOCOUNT suppresses every count", async () => {
    const { driver, requests } = mssqlHarness();
    streamTokens(requests[0], (token) => {
      token("done");
      token("done");
    });
    const result = await driver.query(
      "SET NOCOUNT ON; UPDATE a ...",
      undefined,
      { hardCap: 10 },
    );
    expect(result).toMatchObject({ rowCount: 0, affectedRows: 0 });
  });

  it("sums DML counts across GO batches and repeats", async () => {
    const { driver, requests } = mssqlHarness(4);
    streamTokens(requests[0], (token) => token("done", 5));
    streamTokens(requests[1], (token) => token("done", 2, 0xc3));
    streamTokens(requests[2], (token) => token("done", 2, 0xc3));
    streamTokens(requests[3], (token) => token("done", 0));
    const result = await driver.query(
      "UPDATE a ...\nGO\nINSERT b ...\nGO 2\nUPDATE c ...",
      undefined,
      { hardCap: 10 },
    );
    expect(
      requests.every((request) => request.batch.mock.calls.length === 1),
    ).toBe(true);
    expect(result).toMatchObject({ rowCount: 9, affectedRows: 9 });
  });

  it("keeps result-set row counts while summing DML across GO batches", async () => {
    const { driver, requests } = mssqlHarness(2);
    streamTokens(requests[0], (token) => token("done", 5));
    streamTokens(requests[1], (token) => {
      requests[1].emit("recordset", [{ name: "id", type: mssql.Int }]);
      requests[1].emit("row", [1]);
      token("done", 1, 0xc1);
    });
    const result = await driver.query(
      "UPDATE a ...\nGO\nSELECT id FROM t",
      undefined,
      { hardCap: 10 },
    );
    expect(result).toMatchObject({ rowCount: 1, affectedRows: 5 });
    expect(result.resultSets?.[0]).toMatchObject({ rowCount: 1 });
  });
});
