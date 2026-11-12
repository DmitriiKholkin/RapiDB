import { EventEmitter, once } from "node:events";
import { createRequire } from "node:module";
import mssql from "mssql";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { MssqlStatementCompletionMetadata } from "../../src/extension/dbDrivers/mssqlCompletionMetadata";
import { MssqlQuerySession } from "../../src/extension/dbDrivers/mssqlQuerySession";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";
import {
  MockMssqlRequest,
  mockMssqlSessionPool,
} from "../helpers/mssqlQuerySessionHarness";

// Resolve the installed SDK through THIS worktree, not the main checkout.
const require = createRequire(new URL("../../package.json", import.meta.url));
type Completion = "done" | "doneInProc" | "doneProc";
const SELECT = 0xc1;
const INSERT = 0xc3;
const UPDATE = 0xc5;
const EXEC = 0xe0;
interface NativeRequest extends EventEmitter {
  rowCount: number;
  rst: unknown[];
  canceled?: boolean;
  error?: Error;
  callback(error?: Error): void;
}
const { Connection } = require("tedious") as {
  Connection: {
    prototype: {
      createTokenStreamParser(message: Buffer[], handler: object): EventEmitter;
    };
  };
};
const { RequestTokenHandler } = require("tedious/lib/token/handler") as {
  RequestTokenHandler: new (
    connection: object,
    request: NativeRequest,
  ) => object;
};

function done(
  event: Completion,
  count?: number,
  more = true,
  curCmd = event === "doneProc" ? EXEC : UPDATE,
): Buffer {
  const data = Buffer.alloc(13);
  data[0] = { done: 0xfd, doneInProc: 0xff, doneProc: 0xfe }[event];
  data.writeUInt16LE((more ? 1 : 0) | (count === undefined ? 0 : 16), 1);
  data.writeUInt16LE(curCmd, 3);
  data.writeBigUInt64LE(BigInt(count ?? 0), 5);
  return data;
}

function errorToken(): Buffer {
  const message = Buffer.from("later failure", "utf16le");
  const body = Buffer.alloc(14 + message.length);
  body.writeUInt32LE(50000, 0);
  body[4] = 1;
  body[5] = 16;
  body.writeUInt16LE(message.length / 2, 6);
  message.copy(body, 8);
  // Empty server/procedure names, then the TDS 7.4 four-byte line number.
  body.writeUInt32LE(1, 10 + message.length);
  const header = Buffer.alloc(3);
  header[0] = 0xaa;
  header.writeUInt16LE(body.length, 1);
  return Buffer.concat([header, body]);
}

function sdkHarness(
  scripts: Array<
    (wire: {
      token(
        event: Completion,
        count?: number,
        more?: boolean,
        curCmd?: number,
      ): void;
      returnStatus(value: number): void;
      rowset(): void;
      fail(): void;
      native: NativeRequest;
    }) => void
  >,
  bridgeAvailable = true,
) {
  const counts: number[] = [];
  const parsedCommands: number[] = [];
  const nativeEvents: Array<{ event: Completion; args: unknown[] }> = [];
  const nativeRequests: NativeRequest[] = [];
  const handlers: object[] = [];
  let index = 0;
  const connection = Object.assign(new EventEmitter(), {
    config: { options: { tdsVersion: "7_4" } },
    debug: {
      token: (token: { curCmd?: number }) => {
        if (token.curCmd !== undefined) parsedCommands.push(token.curCmd);
      },
    },
    createTokenStreamParser: Connection.prototype.createTokenStreamParser,
    inTransaction: false,
    closed: false,
    reset: vi.fn((callback: () => void) => callback()),
    close: vi.fn(),
    cancel: vi.fn(() => {
      const native = nativeRequests.at(-1);
      if (native) {
        native.canceled = true;
        native.error = new mssql.RequestError("Canceled.", "ECANCEL");
      }
    }),
    execSqlBatch: (request: NativeRequest) => execute(request),
    execSql: (request: NativeRequest) => execute(request),
  });
  const originalParser = connection.createTokenStreamParser;
  if (!bridgeAvailable)
    Reflect.deleteProperty(connection, "createTokenStreamParser");
  const execute = (native: NativeRequest) => {
    native.rowCount = 0;
    native.rst = [];
    nativeRequests.push(native);
    for (const event of ["done", "doneInProc", "doneProc"] as const) {
      native.on(event, (...args) => nativeEvents.push({ event, args }));
    }
    const handler = new RequestTokenHandler(connection, native);
    handlers.push(handler);
    const bytes: Buffer[] = [];
    scripts[index++]({
      native,
      token: (event, count, more, curCmd) => {
        bytes.push(done(event, count, more, curCmd));
      },
      returnStatus: (value) => {
        const status = Buffer.alloc(5);
        status[0] = 0x79;
        status.writeInt32LE(value, 1);
        bytes.push(status);
      },
      rowset: () => {
        // Real COLMETADATA (Int column 'v') and ROW(1), not public SDK emits.
        bytes.push(
          Buffer.from([
            0x81, 1, 0, 0, 0, 0, 0, 0, 0, 0x38, 1, 0x76, 0, 0xd1, 1, 0, 0, 0,
          ]),
        );
      },
      fail: () => {
        bytes.push(errorToken());
      },
    });
    // Exercise the same factory -> token parser -> RequestTokenHandler path
    // as tedious.Connection. In particular, only this factory lets the driver
    // bridge observe raw CurCmd; native done event arguments remain untouched.
    const parser = (
      bridgeAvailable ? connection.createTokenStreamParser : originalParser
    ).call(connection, bytes, handler);
    void once(parser, "end").then(
      () => native.callback(native.error),
      (error: Error) => native.callback(error),
    );
  };
  const pool = {
    config: { requestTimeout: 5000 },
    connected: true,
    acquire: vi.fn(
      (_request: unknown, callback: (error: null, conn: object) => void) =>
        callback(null, connection),
    ),
    release: vi.fn(),
    request: () => {
      const request = new mssql.Request(pool as never);
      request.on("rowsaffected", (count: number) => counts.push(count));
      return request;
    },
  };
  const driver = new MSSQLDriver({
    id: "nested-proc",
    name: "MSSQL",
    type: "mssql",
  });
  Object.assign(driver, { pool });
  return {
    driver,
    counts,
    parsedCommands,
    nativeEvents,
    connection,
    originalParser,
    nativeRequests,
    handlers,
  };
}

describe("MSSQL nested procedure affected rows: installed SDK and TDS parser", () => {
  it("does not claim 12 for UPDATE(5) whose trigger invokes proc(7)", async () => {
    const completions: Array<{
      event: Completion;
      metadata?: MssqlStatementCompletionMetadata;
    }> = [];
    const track = MssqlQuerySession.prototype.trackStatementCompletions;
    vi.spyOn(
      MssqlQuerySession.prototype,
      "trackStatementCompletions",
    ).mockImplementation(function (
      this: MssqlQuerySession,
      request,
      onComplete,
    ) {
      return track.call(this, request, (event, metadata) => {
        completions.push({ event, metadata });
        onComplete(event, metadata);
      });
    });
    const {
      driver,
      counts,
      parsedCommands,
      nativeEvents,
      connection,
      originalParser,
      handlers,
    } = sdkHarness([
      ({ token, returnStatus }) => {
        token("doneInProc", 7);
        returnStatus(0);
        token("doneProc");
        token("done", 5, false);
      },
    ]);
    const result = await driver.query("UPDATE audited SET x=1");
    expect(counts).toEqual([7, 5]);
    expect(parsedCommands).toEqual([UPDATE, EXEC, UPDATE]);
    // Observe the production callback, without adding arguments to native
    // events. All metadata is present: nesting, not a missing bridge, makes
    // the total unknown.
    expect(completions).toEqual([
      {
        event: "doneInProc",
        metadata: { curCmd: UPDATE, rowCount: 7, more: true },
      },
      {
        event: "doneProc",
        metadata: { curCmd: EXEC, rowCount: undefined, more: true },
      },
      { event: "done", metadata: { curCmd: UPDATE, rowCount: 5, more: false } },
    ]);
    expect(nativeEvents).toEqual([
      { event: "doneInProc", args: [7, true, []] },
      { event: "doneProc", args: [undefined, true, 0, []] },
      { event: "done", args: [5, false, []] },
    ]);
    expect(result.affectedRows).toBeUndefined();
    expect(result).toMatchObject({ affectedRowsUnknown: true, rowCount: 0 });
    expect(connection.createTokenStreamParser).toBe(originalParser);
    expect(
      handlers.every((handler) => !Object.hasOwn(handler, "onDoneProc")),
    ).toBe(true);
  });

  it("preserves UPDATE(5); UPDATE(0) as exactly 5", async () => {
    const { driver, counts, parsedCommands } = sdkHarness([
      ({ token }) => {
        token("done", 5);
        token("done", 0, false);
      },
    ]);
    expect(
      await driver.query("UPDATE a SET x=1; UPDATE b SET x=1"),
    ).toMatchObject({ rowCount: 5, affectedRows: 5 });
    expect(counts).toEqual([5, 0]);
    expect(parsedCommands).toEqual([UPDATE, UPDATE]);
  });

  it.each([
    "UPDATE audited SET x=1",
    "EXEC p; UPDATE b SET x=1",
  ])("does not guess a parent from the indistinguishable completion stream: %s", async (sql) => {
    const { driver } = sdkHarness([
      ({ token }) => {
        token("doneInProc", 7);
        token("doneProc");
        token("done", 5, false);
      },
    ]);
    const result = await driver.query(sql, undefined, { hardCap: 1 });
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it("does not invent a total for multiple standalone/nested procedures", async () => {
    const { driver } = sdkHarness([
      ({ token }) => {
        token("doneInProc", 7);
        token("doneProc");
        token("doneInProc", 3);
        token("doneProc");
        token("doneProc", undefined, false);
      },
    ]);
    const result = await driver.query("EXEC p; EXEC q");
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it("does not mistake count suppression in a trigger for a parent boundary", async () => {
    const { driver } = sdkHarness([
      ({ token }) => {
        token("doneInProc", 7);
        token("doneProc");
        token("doneProc");
        token("done", undefined, false);
      },
    ]);
    const result = await driver.query("SET NOCOUNT ON; UPDATE audited SET x=1");
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it("keeps uncertainty sticky across GO repetitions and later known counts", async () => {
    const nested: Parameters<typeof sdkHarness>[0][number] = ({ token }) => {
      token("doneInProc", 7);
      token("doneProc");
      token("done", 5, false);
    };
    const { driver, nativeRequests, parsedCommands, counts } = sdkHarness([
      nested,
      nested,
      ({ token }) => token("done", 0, false),
    ]);
    const result = await driver.query(
      "UPDATE audited SET x=1\nGO 2\nUPDATE b SET x=1",
    );
    expect(nativeRequests).toHaveLength(3);
    expect(parsedCommands).toEqual([
      UPDATE,
      EXEC,
      UPDATE,
      UPDATE,
      EXEC,
      UPDATE,
      UPDATE,
    ]);
    expect(counts).toEqual([7, 5, 7, 5, 0]);
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it("preserves closed rowsets and uncertainty on partial failure", async () => {
    const { driver, nativeRequests } = sdkHarness([
      ({ token, rowset, fail }) => {
        rowset();
        token("doneInProc", undefined, true, SELECT);
        token("doneInProc", 7);
        token("doneProc");
        token("done", 5);
        fail();
      },
    ]);
    const result = await driver.query(
      "EXEC p; THROW 50000, 'later failure', 1",
      undefined,
      { hardCap: 1 },
    );
    expect(result.error).toContain("later failure");
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 1,
      rows: [{ __col_0: 1 }],
    });
    expect(result.resultSets?.[0].partial).not.toBe(true);
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
    expect(Object.hasOwn(nativeRequests[0], "emit")).toBe(false);
  });

  it("treats the parameterized sp_executesql path conservatively too", async () => {
    const { driver } = sdkHarness([
      ({ token }) => {
        token("doneInProc", 7);
        token("doneProc");
        token("doneInProc", 5);
        token("doneProc", undefined, false);
      },
    ]);
    const result = await driver.query("UPDATE audited SET x=?", [1]);
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it("does not let a zero parent count make a nested total exact", async () => {
    const { driver } = sdkHarness([
      ({ token }) => {
        token("doneInProc", 7);
        token("doneProc");
        token("done", 0, false);
      },
    ]);
    const result = await driver.query("UPDATE audited SET x=1 WHERE 1=0");
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it("keeps a genuinely partial rowset partial when nested completion follows an error", async () => {
    const { driver } = sdkHarness([
      ({ token, rowset, fail }) => {
        rowset();
        fail();
        token("doneInProc", undefined, true, SELECT);
        token("doneProc", undefined, false);
      },
    ]);
    const result = await driver.query("EXEC failing_proc");
    expect(result.error).toContain("later failure");
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 1,
      partial: true,
    });
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it("rejects cancellation rather than returning an ambiguous successful result", async () => {
    const controller = new AbortController();
    const reason = new Error("cancel nested request");
    const { driver, connection, nativeRequests } = sdkHarness([
      ({ token, native }) => {
        native.once("doneProc", () => controller.abort(reason));
        token("doneInProc", 7);
        token("doneProc");
        token("done", 5, false);
      },
    ]);
    await expect(
      driver.query("UPDATE audited SET x=1", undefined, {
        signal: controller.signal,
      }),
    ).rejects.toBe(reason);
    expect(connection.cancel).toHaveBeenCalledOnce();
    expect(connection.close).toHaveBeenCalledOnce();
    await vi.waitFor(() => {
      expect(Object.hasOwn(nativeRequests[0], "emit")).toBe(false);
    });
  });

  it("does not flush a procedure-body count when execution fails before DONEPROC", async () => {
    const { driver } = sdkHarness([
      ({ token, rowset, fail }) => {
        rowset();
        token("done", 1, true, SELECT);
        token("doneInProc", 7);
        fail();
      },
    ]);
    const result = await driver.query("SELECT 1; EXEC failing_proc");
    expect(result.error).toContain("later failure");
    expect(result.rowCount).toBe(1);
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });

  it.each([
    true,
    false,
  ])("keeps nested assignment/DML/rowset counts separate (metadata bridge=%s)", async (bridgeAvailable) => {
    const { driver, parsedCommands, counts } = sdkHarness(
      [
        ({ token, rowset, returnStatus }) => {
          token("doneInProc", 11, true, SELECT);
          token("doneInProc", 7);
          rowset();
          token("doneInProc", 1, true, SELECT);
          returnStatus(0);
          token("doneProc");
          token("doneInProc", 2, true, SELECT);
          returnStatus(0);
          token("doneProc");
          token("done", 5, false);
        },
      ],
      bridgeAvailable,
    );
    const result = await driver.query("UPDATE audited SET x=1", undefined, {
      hardCap: 1,
    });
    expect(parsedCommands).toEqual([
      SELECT,
      UPDATE,
      SELECT,
      EXEC,
      SELECT,
      EXEC,
      UPDATE,
    ]);
    expect(counts).toEqual([11, 7, 1, 2, 5]);
    expect(result).toMatchObject({
      affectedRows: undefined,
      affectedRowsUnknown: true,
      rowCount: 1,
    });
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 1,
      affectedRows: 1,
      rows: [{ __col_0: 1 }],
    });
    expect(result.resultSets?.[0].partial).not.toBe(true);
  });

  it.each([
    true,
    false,
  ])("proves positive DML classification depends on genuine parser metadata, not native arguments (%s)", async (bridgeAvailable) => {
    const { driver, parsedCommands, counts } = sdkHarness(
      [
        ({ token }) => {
          token("done", 7, true, SELECT);
          token("done", 5, false);
        },
      ],
      bridgeAvailable,
    );
    const result = await driver.query(
      "DECLARE @x int; SELECT @x=id FROM b; UPDATE a SET x=1",
    );
    expect(parsedCommands).toEqual([SELECT, UPDATE]);
    expect(counts).toEqual([7, 5]);
    if (bridgeAvailable) {
      expect(result).toMatchObject({ affectedRows: 5, rowCount: 5 });
      expect(result.affectedRowsUnknown).not.toBe(true);
    } else {
      expect(result).toMatchObject({
        affectedRows: undefined,
        affectedRowsUnknown: true,
        rowCount: 0,
      });
    }
  });

  it.each([
    0, 7,
  ])("does not treat an unknown CurCmd as DML or lose a known sum when its count is zero (%s)", async (count) => {
    const { driver, parsedCommands } = sdkHarness([
      ({ token }) => {
        token("done", count, true, 0xffff);
        token("done", 5, false);
      },
    ]);
    const result = await driver.query("EXEC custom; UPDATE a SET x=1");
    expect(parsedCommands).toEqual([0xffff, UPDATE]);
    if (count === 0) {
      expect(result).toMatchObject({ affectedRows: 5, rowCount: 5 });
    } else {
      expect(result).toMatchObject({
        affectedRows: undefined,
        affectedRowsUnknown: true,
        rowCount: 0,
      });
    }
  });

  it.each([
    0,
    undefined,
  ])("does not mistake counted zero or NOCOUNT for a nesting boundary (%s)", async (count) => {
    const { driver, parsedCommands, counts } = sdkHarness([
      ({ token }) => {
        token("doneInProc", count, true, SELECT);
        token("doneInProc", count);
        token("doneProc");
        token("done", count, false);
      },
    ]);
    const result = await driver.query("UPDATE audited SET x=1 WHERE 1=0");
    expect(parsedCommands).toEqual([SELECT, UPDATE, EXEC, UPDATE]);
    expect(counts).toEqual(count === 0 ? [0, 0, 0] : []);
    expect(result).toMatchObject({
      affectedRows: undefined,
      affectedRowsUnknown: true,
      rowCount: 0,
    });
  });

  it.each([
    true,
    false,
  ])("keeps SELECT assignments excluded across repeated DML batches without clearing uncertainty (%s)", async (bridgeAvailable) => {
    const repeated: Parameters<typeof sdkHarness>[0][number] = ({ token }) => {
      token("done", 7, true, SELECT);
      token("done", 5, false);
    };
    const { driver, parsedCommands, counts } = sdkHarness(
      [repeated, repeated, ({ token }) => token("done", 0, false)],
      bridgeAvailable,
    );
    const result = await driver.query(
      "DECLARE @x int; SELECT @x=id FROM b; UPDATE a SET x=1\nGO 2\nUPDATE b SET x=1",
    );
    expect(parsedCommands).toEqual([SELECT, UPDATE, SELECT, UPDATE, UPDATE]);
    expect(counts).toEqual([7, 5, 7, 5, 0]);
    if (bridgeAvailable) {
      expect(result).toMatchObject({ affectedRows: 10, rowCount: 10 });
    } else {
      expect(result).toMatchObject({
        affectedRows: undefined,
        affectedRowsUnknown: true,
        rowCount: 0,
      });
    }
  });
});

describe("MSSQL nested procedure integration stream", () => {
  it("propagates ambiguity without changing completed rowsets on mock streams", async () => {
    const request = new MockMssqlRequest();
    Object.assign(request, {
      _setCurrentRequest: (_native: EventEmitter) => request,
    });
    const driver = new MSSQLDriver({
      id: "mock-nested",
      name: "MSSQL",
      type: "mssql",
    });
    Object.assign(driver, { pool: mockMssqlSessionPool([request]) });
    request.execute.mockImplementation(async () => {
      const native = new EventEmitter();
      Reflect.get(request, "_setCurrentRequest").call(request, native);
      for (const event of ["done", "doneInProc"]) {
        native.on(event, (count?: number) => {
          if (count !== undefined) request.emit("rowsaffected", count);
        });
      }
      request.emit("recordset", [{ name: "v", type: mssql.Int }]);
      request.emit("row", [1]);
      native.emit("done", 1, true);
      native.emit("doneInProc", 7, true);
      native.emit("doneProc", undefined, true);
      native.emit("done", 5, false);
      return {
        recordsets: [],
        recordset: [],
        rowsAffected: [1, 7, 5],
        output: {},
      };
    });
    const result = await driver.query("SELECT 1; UPDATE audited SET x=1");
    expect(result.rowCount).toBe(1);
    expect(result.resultSets?.[0].affectedRows).toBe(1);
    expect(result.affectedRows).toBeUndefined();
    expect(result).toHaveProperty("affectedRowsUnknown", true);
  });
});

describe("MSSQL query result consumers", () => {
  const columns: ColumnTypeMeta[] = [
    {
      name: "id",
      type: "int",
      nativeType: "int",
      category: "integer",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    },
  ];
  const config = { id: "nested-proc", name: "MSSQL", type: "mssql" as const };
  const settings = () => ({
    connectionTimeoutSeconds: 5,
    dbOperationTimeoutSeconds: 5,
    connectionTimeoutMs: 5000,
    dbOperationTimeoutMs: 5000,
  });

  it("does not report a successful DEFAULT VALUES INSERT as failed through the timeout wrapper", async () => {
    const { driver, counts } = sdkHarness([
      ({ token }) => {
        token("doneInProc", 7);
        token("doneProc");
        token("done", 1, false, INSERT);
      },
    ]);
    const transaction = vi.spyOn(driver, "runTransaction");
    const wrapped = createTimeoutAwareDriver(driver, settings);
    const manager = {
      getConnection: () => config,
      getDriver: () => wrapped,
    } as unknown as ConnectionManager;
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    const plan = await service.prepareInsertRow(
      config.id,
      "db",
      "dbo",
      "audited",
      {},
    );
    // DEFAULT VALUES supplies nothing to verify: this genuinely reaches the
    // query() consumer, not the guarded runTransaction() path.
    expect(plan.verification).toBeUndefined();
    expect(plan.operation.sql).toContain("DEFAULT VALUES");
    await expect(
      service.executePreparedInsertPlan(plan),
    ).resolves.toBeUndefined();
    expect(counts).toEqual([7, 1]);
    expect(transaction).not.toHaveBeenCalled();
  });

  it("still rejects a definite zero-row DEFAULT VALUES INSERT", async () => {
    const { driver } = sdkHarness([
      ({ token }) => token("done", 0, false, INSERT),
    ]);
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    await expect(
      service.insertRow(config.id, "db", "dbo", "audited", {}),
    ).rejects.toThrow("Insert failed: the database reported 0 rows affected");
  });

  it("preserves uncertainty, rows, and partial errors through timeout and formatting across GO", async () => {
    const { driver } = sdkHarness([
      ({ token, rowset }) => {
        rowset();
        token("done", 1, false, SELECT);
      },
      ({ token, rowset, fail }) => {
        token("doneInProc", 7);
        token("doneProc");
        token("done", 5);
        rowset();
        fail();
      },
    ]);
    const wrapped = createTimeoutAwareDriver(driver, settings);
    const result = formatQueryResult(
      await wrapped.query(
        "SELECT 1\nGO\nUPDATE audited SET x=1; SELECT failing",
        undefined,
        { hardCap: 1 },
      ),
      1,
    );
    expect(result.affectedRows).toBeUndefined();
    expect(result.affectedRowsUnknown).toBe(true);
    expect(result.error).toContain("later failure");
    expect(result.rowCount).toBe(2);
    expect(result.resultSets?.[0]).toMatchObject({
      rowCount: 1,
      rows: [{ __col_0: 1 }],
    });
    expect(result.resultSets?.[1]).toMatchObject({
      rowCount: 1,
      rows: [],
      partial: true,
      truncated: true,
    });
  });

  it("routes verified INSERT and both UPDATE consumers through guarded transactions; DELETE reads row existence only", async () => {
    const { driver } = sdkHarness([]);
    const transaction = vi.spyOn(driver, "runTransaction").mockResolvedValue();
    const query = vi.spyOn(driver, "query").mockResolvedValue({
      columns: [],
      rows: [],
      rowCount: 0,
      affectedRowsUnknown: true,
      executionTimeMs: 0,
    });
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    const insert = await service.prepareInsertRow(
      config.id,
      "db",
      "dbo",
      "audited",
      { id: 1 },
    );
    expect(insert.verification).toBeDefined();
    await service.executePreparedInsertPlan(insert);
    await service.updateRow(
      config.id,
      "db",
      "dbo",
      "audited",
      { id: 1 },
      { id: 2 },
    );
    const prepared = prepareApplyChangesPlan(
      manager,
      config.id,
      "db",
      "dbo",
      "audited",
      [{ primaryKeys: { id: 1 }, changes: { id: 2 } }],
      columns,
    );
    expect(prepared.executable).toBe(true);
    if (!prepared.executable) throw new Error("Expected executable UPDATE");
    expect(
      await executePreparedApplyPlan(manager, prepared.plan),
    ).toMatchObject({ success: true });
    expect(query).not.toHaveBeenCalled();
    await service.deleteRows(config.id, "db", "dbo", "audited", [{ id: 1 }]);
    expect(transaction).toHaveBeenCalledTimes(4);
    for (const [operations] of transaction.mock.calls) {
      expect(operations).toHaveLength(1);
      expect(
        Boolean(
          operations[0].checkAffectedRows ||
            operations[0].expectedAffectedRows !== undefined,
        ),
      ).toBe(true);
    }
    expect(query).toHaveBeenCalledExactlyOnceWith(
      expect.stringMatching(/^SELECT 1 FROM /),
      [1],
      { database: "db" },
    );
  });

  it.each([
    0, 1,
  ])("guarded transactions use explicit @@ROWCOUNT output (%s), not streamed query counts", async (parentCount) => {
    const { driver } = sdkHarness([]);
    vi.spyOn(mssql.Transaction.prototype, "begin").mockResolvedValue(
      undefined as never,
    );
    const commit = vi
      .spyOn(mssql.Transaction.prototype, "commit")
      .mockResolvedValue(undefined as never);
    const rollback = vi
      .spyOn(mssql.Transaction.prototype, "rollback")
      .mockResolvedValue(undefined as never);
    const request = new mssql.Request();
    const execute = vi.spyOn(request, "query").mockResolvedValue({
      recordsets: [],
      recordset: [],
      rowsAffected: [7, 0],
      output: { __rapidb_affected_rows: parentCount },
    } as never);
    vi.spyOn(mssql.Transaction.prototype, "request").mockReturnValue(request);
    const outsideQuery = vi.spyOn(driver, "query");
    const pending = driver.runTransaction([
      {
        sql: "UPDATE audited SET x=1",
        checkAffectedRows: true,
        expectedAffectedRows: 1,
      },
    ]);
    if (parentCount === 1) {
      await expect(pending).resolves.toBeUndefined();
      expect(commit).toHaveBeenCalledOnce();
      expect(rollback).not.toHaveBeenCalled();
    } else {
      await expect(pending).rejects.toThrow();
      expect(rollback).toHaveBeenCalledOnce();
      expect(commit).not.toHaveBeenCalled();
    }
    expect(execute).toHaveBeenCalledExactlyOnceWith(
      "UPDATE audited SET x=1\n;SET @__rapidb_affected_rows = @@ROWCOUNT;",
    );
    expect(request.parameters.__rapidb_affected_rows.io).toBe(2);
    expect(outsideQuery).not.toHaveBeenCalled();
  });
});
