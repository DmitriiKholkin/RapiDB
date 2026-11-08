import type mssql from "mssql";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MssqlQuerySession } from "../../src/extension/dbDrivers/mssqlQuerySession";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { logger } from "../../src/extension/utils/logger";
import { mockMssqlSessionPool } from "../helpers/mssqlQuerySessionHarness";

afterEach(() => vi.useRealTimers());

const tediousValueParser = require("tedious/lib/value-parser") as {
  readValue: (
    buf: Buffer,
    offset: number,
    metadata: { type?: { name?: string } },
    options: unknown,
  ) => { value: unknown; offset: number };
};

function variant(baseType: number, props: number[], value: Buffer): Buffer {
  const header = Buffer.alloc(6);
  header.writeUInt32LE(2 + props.length + value.length, 0);
  header.writeUInt8(baseType, 4);
  header.writeUInt8(props.length, 5);
  return Buffer.concat([header, Buffer.from(props), value]);
}

const { NotEnoughDataError } = require("tedious/lib/token/helpers") as {
  NotEnoughDataError: new (byteCount: number) => Error;
};

function readVariant(buf: Buffer, offset = 0) {
  return tediousValueParser.readValue(
    buf,
    offset,
    { type: { name: "Variant" } },
    { useUTC: false },
  );
}

function expectSplitVariant(encoded: Buffer, value: string) {
  const prefix = Buffer.from([0xaa, 0xbb, 0xcc]);
  const full = Buffer.concat([prefix, encoded]);
  for (const length of [prefix.length + 2, prefix.length + 5, full.length - 1])
    expect(() => readVariant(full.subarray(0, length), prefix.length)).toThrow(
      NotEnoughDataError,
    );
  expect(readVariant(full, prefix.length)).toEqual({
    value,
    offset: full.length,
  });
}

describe("MSSQL query session completion", () => {
  it("does not turn a successful query into an abort when cancelled during cleanup", async () => {
    const pool = mockMssqlSessionPool([]);
    const connection = pool.connections[0];
    const controller = new AbortController();
    let completeReset!: () => void;
    connection.reset.mockImplementation((callback) => {
      completeReset = () => callback();
    });
    const session = new MssqlQuerySession(
      pool as unknown as mssql.ConnectionPool,
      { signal: controller.signal },
    );
    const pending = session.run(async () => "committed");
    while (!connection.reset.mock.calls.length)
      await new Promise<void>((resolve) => setImmediate(resolve));
    controller.abort();
    session.cancel();
    expect(session.hasStopped()).toBe(false);
    completeReset();
    await expect(pending).resolves.toBe("committed");
    expect(connection.close).not.toHaveBeenCalled();
    expect(pool.release).toHaveBeenCalledOnce();
  });

  it("discards the session but keeps the result when reset fails after success", async () => {
    const pool = mockMssqlSessionPool([]);
    const connection = pool.connections[0];
    connection.reset.mockImplementation((callback) =>
      callback(new Error("reset failed")),
    );
    const logged = vi
      .spyOn(logger, "error")
      .mockImplementation((_, error) =>
        error instanceof Error ? error : new Error(String(error)),
      );
    try {
      const session = new MssqlQuerySession(
        pool as unknown as mssql.ConnectionPool,
      );
      await expect(session.run(async () => 7)).resolves.toBe(7);
      expect(connection.hasError).toBe(true);
      expect(connection.close).toHaveBeenCalledBefore(pool.release);
      expect(logged).toHaveBeenCalledOnce();
      expect(logged).toHaveBeenCalledWith(
        "MSSQL session cleanup failed after a successful query",
        expect.objectContaining({ message: "reset failed" }),
      );
    } finally {
      logged.mockRestore();
    }
  });
});

describe("MSSQL exact numeric values", () => {
  it("emits sequence DDL with its type and exact sql_variant bounds", async () => {
    const driver = new MSSQLDriver({ id: "seq", name: "seq", type: "mssql" });
    const query = vi.fn(async (sql: string) => {
      expect(sql).toContain("CONVERT(nvarchar(100), seq.maximum_value)");
      expect(sql).toContain("TYPE_NAME(seq.system_type_id)");
      return {
        recordset: [
          {
            start_value: "1",
            increment: "1",
            minimum_value: "-99999999999999999999999999999999999999",
            maximum_value: "99999999999999999999999999999999999999",
            is_cycling: 1,
            is_cached: 1,
            cache_size: 50,
            type_name: "decimal",
            precision: 38,
            scale: 0,
          },
        ],
      };
    });
    const request = vi.fn(() => ({ query, input: vi.fn().mockReturnThis() }));
    (driver as unknown as { pool: unknown }).pool = { request };
    await expect(
      driver.getObjectDefinition("db", "dbo", "big_seq", "sequence"),
    ).resolves.toBe(
      "CREATE SEQUENCE [dbo].[big_seq] AS decimal(38,0) START WITH 1 INCREMENT BY 1 " +
        "MINVALUE -99999999999999999999999999999999999999 " +
        "MAXVALUE 99999999999999999999999999999999999999 CYCLE CACHE 50;",
    );
  });

  it("emits a plain AS clause for integer sequences", async () => {
    const driver = new MSSQLDriver({ id: "seq", name: "seq", type: "mssql" });
    const query = vi.fn(async () => ({
      recordset: [
        {
          start_value: "1",
          increment: "1",
          minimum_value: "-9223372036854775808",
          maximum_value: "9223372036854775807",
          is_cycling: 0,
          is_cached: 0,
          cache_size: null,
          type_name: "bigint",
          precision: 19,
          scale: 0,
        },
      ],
    }));
    const request = vi.fn(() => ({ query, input: vi.fn().mockReturnThis() }));
    (driver as unknown as { pool: unknown }).pool = { request };
    await expect(
      driver.getObjectDefinition("db", "dbo", "s", "sequence"),
    ).resolves.toBe(
      "CREATE SEQUENCE [dbo].[s] AS bigint START WITH 1 INCREMENT BY 1 " +
        "MINVALUE -9223372036854775808 MAXVALUE 9223372036854775807 NO CYCLE NO CACHE;",
    );
  });

  it("decodes decimal sql_variant values exactly", () => {
    const magnitude = Buffer.alloc(16);
    let remaining = 1234567890123456789012n;
    for (let index = 0; index < 16; index++) {
      magnitude[index] = Number(remaining & 0xffn);
      remaining >>= 8n;
    }
    const positive = variant(
      0x6a,
      [38, 2],
      Buffer.concat([Buffer.from([1]), magnitude]),
    );
    expect(readVariant(positive)).toEqual({
      value: "12345678901234567890.12",
      offset: positive.length,
    });
    const negative = variant(
      0x6c,
      [38, 2],
      Buffer.concat([Buffer.from([0]), magnitude]),
    );
    expect(readVariant(negative).value).toBe("-12345678901234567890.12");
  });

  it("decodes money sql_variant values exactly", () => {
    const money = Buffer.alloc(8);
    money.writeInt32LE(0x7fffffff, 0);
    money.writeUInt32LE(0xffffffff, 4);
    const encoded = variant(0x3c, [], money);
    expect(readVariant(encoded)).toEqual({
      value: "922337203685477.5807",
      offset: encoded.length,
    });
    const small = Buffer.alloc(4);
    small.writeInt32LE(-12345, 0);
    expect(readVariant(variant(0x7a, [], small)).value).toBe("-1.2345");
  });

  it("waits for more data on split decimal and money sql_variant packets", () => {
    const magnitude = Buffer.alloc(16);
    magnitude.writeUInt32LE(123456789, 0);
    expectSplitVariant(
      variant(0x6a, [38, 4], Buffer.concat([Buffer.from([0]), magnitude])),
      "-12345.6789",
    );
    const money = Buffer.alloc(8);
    money.writeInt32LE(0, 0);
    money.writeUInt32LE(1234567, 4);
    expectSplitVariant(variant(0x3c, [], money), "123.4567");
    const small = Buffer.alloc(4);
    small.writeInt32LE(50000, 0);
    expectSplitVariant(variant(0x7a, [], small), "5.0000");
  });

  it("delegates other sql_variant base types and NULL to tedious", () => {
    const int = Buffer.alloc(4);
    int.writeInt32LE(42, 0);
    const encoded = variant(0x38, [], int);
    expect(readVariant(encoded)).toEqual({ value: 42, offset: encoded.length });
    expect(readVariant(Buffer.alloc(4))).toEqual({ value: null, offset: 4 });
  });
});

function oracleHarness(execute: (sql: string, params?: unknown[]) => unknown) {
  const connection = {
    execute: vi.fn(async (sql: string, params?: unknown[]) =>
      sql.startsWith("ALTER SESSION SET NLS_") ? {} : execute(sql, params),
    ),
    commit: vi.fn(async () => {}),
    rollback: vi.fn(async () => {}),
    close: vi.fn(async (_options?: unknown) => {}),
    break: vi.fn(async () => {}),
  };
  const driver = new OracleDriver({
    id: "oracle-cancel",
    name: "oracle-cancel",
    type: "oracle",
  });
  const getConnection = vi.fn(async () => connection);
  (driver as unknown as { pool: unknown }).pool = { getConnection };
  return { driver, connection, getConnection };
}

function userCalls(connection: { execute: { mock: { calls: unknown[][] } } }) {
  return connection.execute.mock.calls.filter(
    ([sql]) => !String(sql).startsWith("ALTER SESSION SET NLS_"),
  );
}

describe("Oracle NUMBER(*,s) types", () => {
  it("classifies star-precision NUMBER by scale", () => {
    const driver = new OracleDriver({ id: "t", name: "t", type: "oracle" });
    expect(driver.mapTypeCategory("NUMBER(*,0)")).toBe("integer");
    expect(driver.mapTypeCategory("NUMBER(*,2)")).toBe("decimal");
    expect(driver.mapTypeCategory("NUMBER")).toBe("decimal");
    expect(driver.mapTypeCategory("NUMBER(10,0)")).toBe("integer");
  });

  it("keeps the integer constraint in fallback DDL", async () => {
    const { driver } = oracleHarness((sql) => {
      if (sql.includes("DBMS_METADATA")) throw new Error("unavailable");
      if (sql.includes("FROM all_tab_cols")) {
        return {
          rows: [
            {
              COLUMN_NAME: "ID",
              DATA_TYPE: "NUMBER",
              DATA_PRECISION: null,
              DATA_SCALE: 0,
              DATA_LENGTH: 22,
              CHAR_LENGTH: null,
              CHAR_USED: null,
              NULLABLE: "N",
              DATA_DEFAULT: null,
              VIRTUAL_COLUMN: "NO",
              COLUMN_ID: 1,
            },
          ],
        };
      }
      return { rows: [] };
    });
    const ddl = await driver.getCreateTableDDL("", "APP", "T");
    expect(ddl).toContain('"ID" NUMBER(*,0) NOT NULL');
  });

  it("verifies star-precision decimals at their declared scale", () => {
    const driver = new OracleDriver({ id: "t", name: "t", type: "oracle" });
    const column = {
      name: "AMOUNT",
      type: "NUMBER(*,2)",
      nativeType: "NUMBER(*,2)",
      category: "decimal" as const,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain" as const,
    };
    expect(
      driver.checkPersistedEdit(column, "1.234", { persistedValue: "1.23" })
        ?.ok,
    ).toBe(true);
  });
});

const indexRows = () => ({
  rows: [
    {
      INDEX_OWNER: "APP",
      INDEX_NAME: "T_IDX",
      COLUMN_NAME: "B",
      UNIQUENESS: "UNIQUE",
      INDEX_TYPE: "NORMAL",
    },
    {
      INDEX_OWNER: "OTHER",
      INDEX_NAME: "T_IDX",
      COLUMN_NAME: "A",
      UNIQUENESS: "NONUNIQUE",
      INDEX_TYPE: "NORMAL",
    },
  ],
});

describe("Oracle getIndexes", () => {
  it("fetches DDL of the index owned by the matching schema", async () => {
    const { driver, connection } = oracleHarness((sql) => {
      if (sql.includes("FROM all_indexes")) return indexRows();
      if (sql.includes("DBMS_METADATA.GET_DDL('INDEX'"))
        return { rows: [{ DDL: "CREATE INDEX metadata" }] };
      return {};
    });
    await expect(
      driver.getIndexDDL("", "APP", "T", "OTHER.T_IDX"),
    ).resolves.toBe("CREATE INDEX metadata");
    const metadataCall = userCalls(connection).find(([sql]) =>
      String(sql).includes("GET_DDL('INDEX'"),
    );
    expect(metadataCall?.[1]).toEqual(["T_IDX", "OTHER"]);
    await driver.getIndexDDL("", "APP", "T", "T_IDX");
    expect(
      userCalls(connection)
        .filter(([sql]) => String(sql).includes("GET_DDL('INDEX'"))
        .at(-1)?.[1],
    ).toEqual(["T_IDX", "APP"]);
    expect(connection.close).toHaveBeenCalledTimes(2);
  });

  it("qualifies the fallback index DDL with its owner", async () => {
    const { driver } = oracleHarness((sql) => {
      if (sql.includes("FROM all_indexes")) return indexRows();
      if (sql.includes("DBMS_METADATA")) throw new Error("unavailable");
      return {};
    });
    await expect(
      driver.getIndexDDL("", "APP", "T", "OTHER.T_IDX"),
    ).resolves.toBe('CREATE INDEX "OTHER"."T_IDX" ON "APP"."T" ("A");');
    await expect(driver.getIndexDDL("", "APP", "T", "T_IDX")).resolves.toBe(
      'CREATE UNIQUE INDEX "APP"."T_IDX" ON "APP"."T" ("B");',
    );
    await expect(driver.getIndexDDL("", "APP", "T", "MISSING")).rejects.toThrow(
      'Index "MISSING" not found',
    );
  });

  it("filters by table owner and detects primary indexes through constraints", async () => {
    const { driver, connection } = oracleHarness(() => ({
      rows: [
        {
          INDEX_OWNER: "APP",
          INDEX_NAME: "T_PK",
          COLUMN_NAME: "ID",
          UNIQUENESS: "UNIQUE",
          INDEX_TYPE: "PRIMARY",
        },
        {
          INDEX_OWNER: "OTHER",
          INDEX_NAME: "T_IDX",
          COLUMN_NAME: "A",
          UNIQUENESS: "NONUNIQUE",
          INDEX_TYPE: "NORMAL",
        },
        {
          INDEX_OWNER: "APP",
          INDEX_NAME: "T_IDX",
          COLUMN_NAME: "B",
          UNIQUENESS: "NONUNIQUE",
          INDEX_TYPE: "NORMAL",
        },
      ],
    }));
    const indexes = await driver.getIndexes("", "APP", "T");
    const [sql, params] = userCalls(connection)[0] as [string, unknown[]];
    expect(sql).toContain("i.table_owner = :1");
    expect(sql).not.toContain("i.owner = :1");
    expect(sql).toMatch(/EXISTS[\s\S]*pk\.index_name = i\.index_name/);
    expect(params).toEqual(["APP", "T"]);
    expect(indexes).toEqual([
      { name: "T_PK", columns: ["ID"], unique: true, primary: true },
      { name: "OTHER.T_IDX", columns: ["A"], unique: false, primary: false },
      { name: "T_IDX", columns: ["B"], unique: false, primary: false },
    ]);
  });
});

describe("Oracle query AbortSignal", () => {
  it("does not borrow a connection when already aborted", async () => {
    const { driver, getConnection } = oracleHarness(() => ({}));
    const controller = new AbortController();
    controller.abort(new Error("panel closed"));
    await expect(
      driver.query("SELECT 1 FROM dual", undefined, {
        signal: controller.signal,
      }),
    ).rejects.toThrow("panel closed");
    expect(getConnection).not.toHaveBeenCalled();
  });

  it("releases the connection without executing when aborted during checkout", async () => {
    const controller = new AbortController();
    const { driver, connection, getConnection } = oracleHarness(() => ({}));
    getConnection.mockImplementation(async () => {
      controller.abort(new Error("export cancelled"));
      return connection;
    });
    await expect(
      driver.query("SELECT 1 FROM dual", undefined, {
        signal: controller.signal,
      }),
    ).rejects.toThrow("export cancelled");
    expect(userCalls(connection)).toEqual([]);
    expect(connection.close).toHaveBeenCalledOnce();
    expect(connection.commit).not.toHaveBeenCalled();
  });

  it("breaks the running statement on abort and removes the listener", async () => {
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    let fail!: (error: Error) => void;
    const { driver, connection } = oracleHarness(
      () =>
        new Promise((_, reject) => {
          fail = reject;
        }),
    );
    connection.break.mockImplementation(async () => {
      fail(new Error("ORA-01013: user requested cancel"));
    });
    const pending = driver.query("SELECT 1 FROM dual", undefined, {
      signal: controller.signal,
    });
    while (!userCalls(connection).length)
      await new Promise<void>((resolve) => setImmediate(resolve));
    controller.abort();
    const rejected = await pending.then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(rejected).toBe(controller.signal.reason);
    expect((rejected as Error).name).toBe("AbortError");
    expect(connection.break).toHaveBeenCalledOnce();
    expect(connection.commit).not.toHaveBeenCalled();
    expect(connection.rollback).toHaveBeenCalled();
    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
  });

  it("rolls back and skips later statements when aborted between statements", async () => {
    const controller = new AbortController();
    const reason = new DOMException("export cancelled", "AbortError");
    const { driver, connection } = oracleHarness((sql) => {
      if (sql.includes("SET a = 1")) controller.abort(reason);
      return { rowsAffected: 1 };
    });
    await expect(
      driver.query("UPDATE t SET a = 1; UPDATE t SET b = 2", undefined, {
        signal: controller.signal,
      }),
    ).rejects.toBe(reason);
    const executed = userCalls(connection).map(([sql]) => String(sql));
    expect(executed.some((sql) => sql.includes("SET a = 1"))).toBe(true);
    expect(executed.some((sql) => sql.includes("SET b = 2"))).toBe(false);
    expect(connection.rollback).toHaveBeenCalledOnce();
    expect(connection.commit).not.toHaveBeenCalled();
    expect(connection.close).toHaveBeenCalledOnce();
  });
});

describe("Oracle session-changing detection", () => {
  it.each([
    "ALTER/**/SESSION SET CURRENT_SCHEMA = other",
    "ALTER -- comment\nSESSION SET NLS_DATE_FORMAT = 'YYYY'",
    "BEGIN SYS.\"DBMS_SESSION\".SET_ROLE('ALL'); END;",
    "SET/* x */ROLE ALL",
    "BEGIN EXECUTE IMMEDIATE 'ALTER /* x */ SESSION SET CURRENT_SCHEMA = other'; END;",
    "BEGIN EXECUTE IMMEDIATE q'[ALTER--x\nSESSION SET CURRENT_SCHEMA = other]'; END;",
  ])("drops the connection after %s", async (sql) => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(sql);
    expect(connection.close).toHaveBeenCalledWith({ drop: true });
  });

  it.each([
    "SELECT 1 FROM dual",
    "UPDATE t SET /* c */ role = 'x'",
  ])("returns the connection to the pool after %s", async (sql) => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(sql);
    expect(connection.close).toHaveBeenCalledWith();
  });
});
