import { EventEmitter, once } from "node:events";
import { createRequire } from "node:module";
import mssql from "mssql";
import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";

const require = createRequire(import.meta.url);
const { Connection } = require("tedious") as {
  Connection: {
    prototype: {
      createTokenStreamParser: (
        message: Buffer[],
        handler: object,
      ) => EventEmitter;
    };
  };
};
const { RequestTokenHandler } = require("tedious/lib/token/handler") as {
  RequestTokenHandler: new (connection: object, request: object) => object;
};

// Microsoft SqlClient TdsEnums, not inferred from the submitted SQL text:
// https://github.com/dotnet/SqlClient/blob/main/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/Parser/TdsEnums.cs
const SELECT = 0xc1;
const INSERT = 0xc3;
const DELETE = 0xc4;
const UPDATE = 0xc5;
const MERGE = 0x117;

function done(curCmd: number, count?: number, event = 0xfd) {
  const bytes = Buffer.alloc(13);
  bytes[0] = event;
  bytes.writeUInt16LE(1 | (count === undefined ? 0 : 16), 1);
  bytes.writeUInt16LE(curCmd, 3);
  bytes.writeBigUInt64LE(BigInt(count ?? 0), 5);
  return bytes;
}

function rowset(count: number, curCmd = SELECT) {
  // COLMETADATA(Int column 'id'), ROWs, DONE; parsed by the real tedious parser.
  const metadata = Buffer.from([
    0x81, 1, 0, 0, 0, 0, 0, 0, 0, 0x38, 2, 0x69, 0, 0x64, 0,
  ]);
  const rows = Array.from({ length: count }, (_, i) => {
    const row = Buffer.alloc(5);
    row[0] = 0xd1;
    row.writeInt32LE(i + 1, 1);
    return row;
  });
  return Buffer.concat([metadata, ...rows, done(curCmd, count)]);
}

interface NativeRequest extends EventEmitter {
  rowCount: number;
  rst: unknown[];
  error?: Error;
  canceled?: boolean;
  callback(error: unknown): void;
}

function harness(
  batches: Buffer[][],
  bridgeAvailable = true,
  onDispatch?: () => void,
) {
  let batch = 0;
  const sdkCounts: number[] = [];
  const natives: NativeRequest[] = [];
  const handlers: object[] = [];
  const parsedCommands: number[] = [];
  const nativeEvents: unknown[][] = [];
  const nativeProcedureEvents: unknown[][] = [];
  const cancellations: NativeRequest[] = [];
  const finished = new WeakSet<NativeRequest>();
  const finish = (request: NativeRequest, error: unknown) => {
    if (finished.has(request)) return;
    finished.add(request);
    request.callback(error);
  };
  const connection = Object.assign(new EventEmitter(), {
    config: { options: { tdsVersion: "7_4" } },
    debug: {
      token: (token: { curCmd?: number }) => {
        if (token.curCmd !== undefined) parsedCommands.push(token.curCmd);
      },
    },
    inTransaction: false,
    closed: false,
    reset: (callback: () => void) => callback(),
    close: () => {
      connection.closed = true;
    },
    cancel: () => {
      const request = natives.at(-1);
      if (request) {
        cancellations.push(request);
        request.canceled = true;
        // Like tedious, finish the callback only after the response parser
        // ends; it must not clear native.canceled while tokens still drain.
        request.error = new mssql.RequestError("Canceled.", "ECANCEL");
      }
    },
    createTokenStreamParser: Connection.prototype.createTokenStreamParser,
    execSqlBatch: (request: NativeRequest) => dispatch(request),
    execSql: (request: NativeRequest) => dispatch(request),
  });
  const originalParser = connection.createTokenStreamParser;
  function dispatch(request: NativeRequest) {
    natives.push(request);
    request.rowCount = 0;
    request.rst = [];
    request.on("done", (...args) => nativeEvents.push(args));
    request.on("doneProc", (...args) => nativeProcedureEvents.push(args));
    const handler = new RequestTokenHandler(connection, request);
    handlers.push(handler);
    // Use the real Connection parser factory and RequestTokenHandler, not a
    // fabricated native emit carrying metadata that tedious actually drops.
    const parser = (
      bridgeAvailable ? connection.createTokenStreamParser : originalParser
    ).call(connection, batches[batch++], handler) as EventEmitter;
    void once(parser, "end").then(
      () => finish(request, request.error ?? null),
      (error) => finish(request, error),
    );
    onDispatch?.();
  }
  if (!bridgeAvailable)
    Reflect.deleteProperty(connection, "createTokenStreamParser");
  const pool = {
    config: { requestTimeout: 5000 },
    connected: true,
    acquire: (
      _request: unknown,
      callback: (error: unknown, connection: object) => void,
    ) => callback(null, connection),
    release: () => {},
    request: () => {
      const request = new mssql.Request(pool as never);
      request.on("rowsaffected", (count) => sdkCounts.push(count));
      return request;
    },
  };
  const driver = new MSSQLDriver({
    id: "select-count",
    name: "MSSQL",
    type: "mssql",
  });
  Object.assign(driver, { pool });
  return {
    driver,
    connection,
    originalParser,
    natives,
    handlers,
    sdkCounts,
    parsedCommands,
    nativeEvents,
    nativeProcedureEvents,
    cancellations,
  };
}

describe("MSSQL SELECT assignment counts through the real SDK/token parser", () => {
  it.each([
    0, 1, 7,
  ])("excludes SELECT assignment (%i rows) before and after UPDATE", async (count) => {
    for (const first of [true, false]) {
      const tokens = [done(SELECT, count), done(UPDATE, 5)];
      if (!first) tokens.reverse();
      const { driver, sdkCounts, parsedCommands, nativeEvents } = harness([
        tokens,
      ]);
      const select = "DECLARE @x int; SELECT @x=id FROM b";
      const result = await driver.query(
        first ? `${select}; UPDATE a SET x=1` : `UPDATE a SET x=1; ${select}`,
        undefined,
        { hardCap: 1 },
      );
      expect(sdkCounts).toEqual(first ? [count, 5] : [5, count]);
      expect(parsedCommands).toEqual(
        first ? [SELECT, UPDATE] : [UPDATE, SELECT],
      );
      // Actual tedious emits only rowCount, more, rows; CurCmd is lost here.
      expect(nativeEvents).toEqual(
        (first ? [count, 5] : [5, count]).map((n) => [n, true, []]),
      );
      expect(result).toMatchObject({ affectedRows: 5, rowCount: 5 });
    }
  });

  it("excludes assignment-only counts without guessing from comments, strings, CTE or EXEC text", async () => {
    for (const sql of [
      "DECLARE @x int; /* UPDATE */ SELECT @x=id FROM b",
      "DECLARE @x nvarchar(20); SELECT @x='DELETE FROM b' FROM b",
      "DECLARE @x int; WITH c AS (SELECT id FROM b) SELECT @x=id FROM c",
      "EXEC(N'DECLARE @x int; SELECT @x=id FROM b')",
    ]) {
      const { driver } = harness([[done(SELECT, 7)]]);
      expect(await driver.query(sql)).toMatchObject({
        affectedRows: 0,
        rowCount: 0,
      });
    }
  });

  it("preserves UPDATE5;UPDATE0 and ordinary INSERT/DELETE/MERGE totals", async () => {
    const { driver } = harness([
      [
        done(UPDATE, 5),
        done(UPDATE, 0),
        done(INSERT, 2),
        done(DELETE, 3),
        done(MERGE, 4),
      ],
    ]);
    expect(
      await driver.query(
        "UPDATE a SET x=1; UPDATE b SET x=1; INSERT c VALUES(1); DELETE d; MERGE e ...",
      ),
    ).toMatchObject({ affectedRows: 14, rowCount: 14 });
  });

  it("does not treat DDL or counted transaction control commands as DML", async () => {
    const { driver } = harness([
      [done(0xc6), done(0xd4, 8), done(0xd5, 6), done(UPDATE, 5)],
    ]);
    expect(
      await driver.query(
        "CREATE TABLE t(id int); SET XACT_ABORT ON; UPDATE a SET x=1",
      ),
    ).toMatchObject({ affectedRows: 5 });
  });

  it("does not claim an exact DML total for unknown counted CurCmd values", async () => {
    const { driver } = harness([[done(0xffff, 7), done(UPDATE, 5)]]);
    expect(
      (await driver.query("EXEC custom; UPDATE a SET x=1")).affectedRows,
    ).toBeUndefined();
  });

  it.each([
    undefined,
    1,
    100,
  ])("counts OUTPUT DML once without counting SELECT rows (hardCap=%s)", async (hardCap) => {
    const { driver } = harness([[rowset(5, UPDATE)]]);
    const result = await driver.query(
      "UPDATE t SET x=1 OUTPUT inserted.id",
      undefined,
      { hardCap },
    );
    expect(result).toMatchObject({ affectedRows: 5, rowCount: 5 });
    expect(result.affectedRowsUnknown).not.toBe(true);
    expect(result.rows).toHaveLength(hardCap === 1 ? 1 : 5);
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 5,
      affectedRows: 5,
    });
  });

  it("preserves SELECT/OUTPUT rowset counts, display caps and completion semantics", async () => {
    const { driver } = harness([
      [done(UPDATE, 5), rowset(3), rowset(2, UPDATE), done(SELECT, 7)],
    ]);
    const result = await driver.query(
      "UPDATE a SET x=1; SELECT id FROM b; UPDATE b SET x=2 OUTPUT inserted.id; DECLARE @x int; SELECT @x=id FROM b",
      undefined,
      { hardCap: 1 },
    );
    expect(result).toMatchObject({ affectedRows: 7, rowCount: 5 });
    expect(result.resultSets).toMatchObject([
      { rowCount: 3, affectedRows: 3, rows: [{ __col_0: 1 }], truncated: true },
      { rowCount: 2, affectedRows: 2, rows: [], truncated: true },
    ]);
    expect(result.resultSets?.some((set) => set.partial)).toBe(false);
  });

  it("keeps SELECT rowsets out of totals and does not infer zero without command metadata", async () => {
    const known = await harness([[rowset(5, SELECT)]]).driver.query(
      "SELECT id FROM t",
    );
    expect(known).toMatchObject({ affectedRows: 0, rowCount: 5 });
    expect(known.affectedRowsUnknown).not.toBe(true);
    for (const curCmd of [SELECT, UPDATE]) {
      const { driver } = harness([[rowset(5, curCmd)]], false);
      const unknown = await driver.query(
        curCmd === SELECT
          ? "SELECT id FROM t"
          : "UPDATE t SET x=1 OUTPUT inserted.id",
      );
      expect(unknown).toMatchObject({ affectedRowsUnknown: true, rowCount: 5 });
      expect(unknown.affectedRows).toBeUndefined();
    }
  });

  it("counts OUTPUT completions once across GO repeats without changing displayed row counts", async () => {
    const { driver } = harness([[rowset(5, UPDATE)], [rowset(5, UPDATE)]]);
    const result = await driver.query(
      "UPDATE t SET x=1 OUTPUT inserted.id\nGO 2",
      undefined,
      { hardCap: 1 },
    );
    expect(result).toMatchObject({ affectedRows: 10, rowCount: 10 });
    expect(result.resultSets?.map((set) => set.rowCount)).toEqual([5, 5]);
    expect(result.resultSets?.map((set) => set.rows.length)).toEqual([1, 0]);
  });

  it("keeps counts across GO and repeated batches", async () => {
    const { driver } = harness([
      [done(SELECT, 7), done(UPDATE, 5)],
      [done(SELECT, 2)],
      [done(SELECT, 2)],
      [done(UPDATE, 0)],
    ]);
    expect(
      await driver.query(
        "DECLARE @x int; SELECT @x=id FROM b; UPDATE a SET x=1\nGO\nDECLARE @x int; SELECT @x=id FROM b\nGO 2\nUPDATE b SET x=1",
      ),
    ).toMatchObject({ affectedRows: 5, rowCount: 5 });
  });

  it("classifies parameterized RPC DONEINPROC before/after DML and keeps RETURN status separate", async () => {
    for (const first of [true, false]) {
      const status = Buffer.from([0x79, 42, 0, 0, 0]);
      const commands = [done(SELECT, 7, 0xff), done(UPDATE, 5, 0xff)];
      if (!first) commands.reverse();
      const { driver, sdkCounts, parsedCommands, nativeProcedureEvents } =
        harness([[...commands, status, done(0xe0, undefined, 0xfe)]]);
      const select = "DECLARE @x int; SELECT @x=id FROM b";
      expect(
        await driver.query(
          first
            ? `${select}; UPDATE a SET x=?; RETURN 42`
            : `UPDATE a SET x=?; ${select}; RETURN 42`,
          [1],
        ),
      ).toMatchObject({
        affectedRows: undefined,
        affectedRowsUnknown: true,
        rowCount: 0,
      });
      // Classification still reaches the raw SDK tokens. DONEPROC cannot
      // establish a top-level mutation total, even when RETURN is available.
      expect(parsedCommands).toEqual(
        first ? [SELECT, UPDATE, 0xe0] : [UPDATE, SELECT, 0xe0],
      );
      expect(sdkCounts).toEqual(first ? [7, 5] : [5, 7]);
      expect(nativeProcedureEvents).toEqual([[undefined, true, 42, []]]);
    }
  });

  it.each([
    [done(SELECT, 7), done(UPDATE, 5)],
    [done(UPDATE, 5), done(SELECT, 7)],
    [done(UPDATE, 5), done(UPDATE, 0)],
  ])("omits affectedRows if the SDK parser bridge is unavailable", async (...commands) => {
    const known = harness([commands]);
    const unknown = harness([commands], false);
    const sql = "SELECT assignment or DML";
    expect((await known.driver.query(sql)).affectedRows).toBe(5);
    expect((await unknown.driver.query(sql)).affectedRows).toBeUndefined();
    // Identical SDK counts do not identify command types without CurCmd.
    expect(unknown.sdkCounts).toEqual(known.sdkCounts);
  });

  it("keeps reported zero counts but does not classify a positive rowset without metadata", async () => {
    const zeros = harness([[done(UPDATE, 0), done(SELECT, 0)]], false);
    expect(
      await zeros.driver.query("UPDATE a SET x=1 WHERE 1=0; SELECT assignment"),
    ).toMatchObject({ affectedRows: 0, rowCount: 0 });
    const selected = harness([[rowset(2)]], false);
    expect(await selected.driver.query("SELECT id FROM b")).toMatchObject({
      affectedRows: undefined,
      affectedRowsUnknown: true,
      rowCount: 2,
      rows: [{ __col_0: 1 }, { __col_0: 2 }],
    });
  });

  it("does not turn an unavailable GO subtotal into a falsely exact total", async () => {
    const { driver } = harness([[done(SELECT, 7)], [done(UPDATE, 0)]], false);
    expect(
      (await driver.query("SELECT assignment\nGO\nUPDATE a SET x=1"))
        .affectedRows,
    ).toBeUndefined();
  });

  it("preserves rowsets when command metadata is unavailable", async () => {
    const { driver } = harness([[rowset(2), done(UPDATE, 5)]], false);
    expect(
      await driver.query("SELECT id FROM b; UPDATE a SET x=1"),
    ).toMatchObject({
      rowCount: 2,
      rows: [{ __col_0: 1 }, { __col_0: 2 }],
      affectedRows: undefined,
    });
  });

  it("restores connection/native/handler hooks after actual SDK completion", async () => {
    const { driver, connection, originalParser, natives, handlers } = harness([
      [done(UPDATE, 5)],
    ]);
    await driver.query("UPDATE a SET x=1");
    expect(connection.createTokenStreamParser).toBe(originalParser);
    expect(natives.every((native) => !Object.hasOwn(native, "emit"))).toBe(
      true,
    );
    expect(handlers.every((handler) => !Object.hasOwn(handler, "onDone"))).toBe(
      true,
    );
  });

  it("restores hooks after a real tedious DONE_ERROR token", async () => {
    const token = done(UPDATE);
    token.writeUInt16LE(2, 1);
    const { driver, connection, originalParser, natives, handlers } = harness([
      [token],
    ]);
    await expect(driver.query("UPDATE a SET x=1")).rejects.toThrow(
      "An unknown error has occurred.",
    );
    expect(connection.createTokenStreamParser).toBe(originalParser);
    expect(natives.every((native) => !Object.hasOwn(native, "emit"))).toBe(
      true,
    );
    expect(handlers.every((handler) => !Object.hasOwn(handler, "onDone"))).toBe(
      true,
    );
  });

  it("restores the real SDK parser/handler hooks when cancelled after parser registration", async () => {
    const controller = new AbortController();
    const reason = new Error("cancel parser bridge");
    const {
      driver,
      connection,
      originalParser,
      natives,
      handlers,
      sdkCounts,
      cancellations,
    } = harness([[done(UPDATE, 5)]], true, () => controller.abort(reason));
    await expect(
      driver.query("UPDATE a SET x=1", undefined, {
        signal: controller.signal,
      }),
    ).rejects.toThrow("cancel parser bridge");
    await vi.waitFor(() => {
      expect(connection.createTokenStreamParser).toBe(originalParser);
      expect(natives.every((native) => !Object.hasOwn(native, "emit"))).toBe(
        true,
      );
      expect(
        handlers.every((handler) => !Object.hasOwn(handler, "onDone")),
      ).toBe(true);
    });
    expect(connection.closed).toBe(true);
    expect(natives).toHaveLength(1);
    expect(cancellations).toEqual(natives);
    // The real tedious Request.callback clears canceled before returning.
    expect(natives[0].canceled).toBe(false);
    expect(sdkCounts).toEqual([]);
  });
});
