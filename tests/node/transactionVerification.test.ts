import mssql from "mssql";
import oracledb from "oracledb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import type {
  ColumnTypeMeta,
  TransactionOptions,
} from "../../src/extension/dbDrivers/types";

const engines = ["pg", "mysql", "mssql", "oracle", "sqlite"] as const;
type Engine = (typeof engines)[number];
const column: ColumnTypeMeta = {
  name: "amount",
  type: "INTEGER",
  nativeType: "INTEGER",
  category: "integer",
  nullable: false,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};
const options: TransactionOptions = {
  verifications: [
    {
      rowIndex: 1,
      sql: "SELECT amount AS __col_0 FROM edits WHERE id = ?",
      params: [2],
      values: [{ column, expectedValue: "9" }],
    },
  ],
};

function harness(
  engine: Engine,
  read: () => Promise<unknown[][]>,
  affected = 1,
) {
  const config = { id: "verification", name: "Verification", type: engine };
  const driver =
    engine === "pg"
      ? new PostgresDriver(config)
      : engine === "mysql"
        ? new MySQLDriver(config)
        : engine === "mssql"
          ? new MSSQLDriver(config)
          : engine === "oracle"
            ? new OracleDriver(config)
            : new SQLiteCoreDriver(config);
  const events: string[] = [];
  const commit = vi.fn(async () => {
    events.push("COMMIT");
  });
  const rollback = vi.fn(async () => {
    events.push("ROLLBACK");
  });
  const cancel = vi.fn(async () => undefined);
  const release = vi.fn();
  const metadata = [{ name: "__col_0", dbType: oracledb.DB_TYPE_NUMBER }];
  const execute = vi.fn(
    async (
      input: string | { sql: string },
      _params?: unknown,
      executeOptions?: oracledb.ExecuteOptions,
    ) => {
      const sql = typeof input === "string" ? input : input.sql;
      if (sql.startsWith("ALTER SESSION")) return {};
      if (sql === "BEGIN") {
        events.push("BEGIN");
        return { rowCount: 0 };
      }
      if (sql === "COMMIT") {
        await commit();
        return {};
      }
      if (sql === "ROLLBACK") {
        await rollback();
        return {};
      }
      if (!sql.startsWith("SELECT")) {
        events.push("DML");
        return engine === "mysql"
          ? [{ affectedRows: affected }]
          : {
              rowCount: affected,
              rowsAffected: engine === "oracle" ? affected : [affected],
              output: { __rapidb_affected_rows: affected },
            };
      }
      events.push("READ");
      const rows = await read();
      if (engine === "mysql") return [rows.map((row) => ({ __col_0: row[0] }))];
      if (engine === "pg")
        return {
          rows: rows.map((row) => ({ __col_0: row[0] })),
          rowCount: rows.length,
        };
      if (engine === "mssql")
        return { recordset: rows, columns: [metadata], rowsAffected: [] };
      expect(executeOptions?.autoCommit).toBe(false);
      expect(executeOptions?.fetchTypeHandler?.(metadata[0])).toMatchObject({
        type: oracledb.STRING,
      });
      let fetched = false;
      return {
        resultSet: {
          metaData: metadata,
          getRows: vi.fn(async () => {
            const batch = fetched ? [] : rows;
            fetched = true;
            return batch;
          }),
          close: vi.fn(async () => {
            events.push("CURSOR_CLOSE");
          }),
        },
      };
    },
  );
  const connection = {
    query: execute,
    execute,
    release,
    destroy: cancel,
    break: cancel,
    beginTransaction: async () => {
      events.push("BEGIN");
    },
    commit,
    rollback,
    close: release,
  };
  const lease = vi.fn(async () => connection);
  if (engine === "sqlite") {
    (driver as unknown as { db: unknown }).db = {
      isOpen: true,
      inTransaction: true,
      exec: (sql: string) => {
        events.push(sql === "BEGIN TRANSACTION" ? "BEGIN" : sql);
      },
      run: () => {
        events.push("DML");
        return { changes: affected };
      },
      queryBounded: () => {
        events.push("READ");
        return { columns: ["__col_0"], rows: [[9]], rowCount: 1 };
      },
    };
  } else if (engine === "mssql") {
    (driver as unknown as { pool: unknown }).pool = { config: { options: {} } };
    vi.spyOn(mssql.Transaction.prototype, "begin").mockImplementation(
      (async () => {
        events.push("BEGIN");
      }) as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "request").mockReturnValue({
      query: execute,
      input: vi.fn(),
      output: vi.fn(),
      cancel,
    } as never);
    vi.spyOn(mssql.Transaction.prototype, "commit").mockImplementation(
      commit as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "rollback").mockImplementation(
      rollback as never,
    );
  } else {
    (driver as unknown as { pool: unknown }).pool = {
      connect: lease,
      getConnection: lease,
    };
  }
  const outsideQuery = vi
    .spyOn(driver, "query")
    .mockRejectedValue(new Error("Outside transaction read forbidden"));
  return { driver, events, lease, execute, outsideQuery, cancel };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("transaction-scoped UPDATE verification", () => {
  it.each(
    engines,
  )("%s verifies on the same lease before commit", async (engine) => {
    const { driver, events, lease, outsideQuery } = harness(
      engine,
      async () => [[9]],
    );
    await driver.runTransaction(
      [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
      undefined,
      options,
    );
    expect(events.filter((event) => event !== "CURSOR_CLOSE")).toEqual([
      ...(engine === "oracle" ? [] : ["BEGIN"]),
      "DML",
      "READ",
      "COMMIT",
    ]);
    if (engine !== "sqlite" && engine !== "mssql")
      expect(lease).toHaveBeenCalledOnce();
    expect(outsideQuery).not.toHaveBeenCalled();
  });

  it.each(
    engines.flatMap((engine) =>
      ["mismatch", "missing", "ambiguous", "read error", "affected zero"].map(
        (failure) => ({ engine, failure }),
      ),
    ),
  )("$engine aborts on $failure", async ({ engine, failure }) => {
    const read = async () => {
      if (failure === "read error") throw new Error("read failed");
      return failure === "missing"
        ? []
        : failure === "ambiguous"
          ? [[9], [9]]
          : [[10]];
    };
    const { driver, events, outsideQuery } = harness(
      engine,
      read,
      failure === "affected zero" ? 0 : 1,
    );
    if (engine === "sqlite") {
      (driver as unknown as { db: { queryBounded: unknown } }).db.queryBounded =
        () => {
          events.push("READ");
          if (failure === "read error") throw new Error("read failed");
          const rows =
            failure === "missing"
              ? []
              : failure === "ambiguous"
                ? [[9], [9]]
                : [[10]];
          return { columns: ["__col_0"], rows, rowCount: rows.length };
        };
    }
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      ),
    ).rejects.toThrow(
      failure === "affected zero"
        ? "Mutation affected 0"
        : "verification failed",
    );
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
    expect(events.includes("READ")).toBe(failure !== "affected zero");
    expect(outsideQuery).not.toHaveBeenCalled();
  });

  it.each([
    "pg",
    "mysql",
    "mssql",
    "oracle",
  ] as const)("%s fences late verification settlement after timeout", async (engine) => {
    vi.useFakeTimers();
    let finish!: () => void;
    const wait = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const {
      driver: raw,
      events,
      cancel,
    } = harness(engine, async () => {
      await wait;
      return [[9]];
    });
    const driver = createTimeoutAwareDriver(raw, () => ({
      connectionTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutSeconds: 1,
      dbOperationTimeoutMs: 25,
    }));
    const pending = driver.runTransaction(
      [{ sql: "UPDATE edits SET amount = 9" }],
      undefined,
      options,
    );
    const rejected = expect(pending).rejects.toThrow("outcome may be unknown");
    await vi.advanceTimersByTimeAsync(25);
    await rejected;
    expect(events).toContain("READ");
    finish();
    await vi.runAllTimersAsync();
    expect(events).not.toContain("COMMIT");
    expect(events).toContain("ROLLBACK");
    if (engine !== "pg") expect(cancel).toHaveBeenCalled();
  });

  it("SQLite core checks a wall-clock deadline after native read-back and rolls back", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1000);
    const { driver, events } = harness("sqlite", async () => [[9]]);
    (driver as unknown as { db: { queryBounded: unknown } }).db.queryBounded =
      () => {
        events.push("READ");
        vi.setSystemTime(1026);
        return { columns: ["__col_0"], rows: [[9]], rowCount: 1 };
      };
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9" }],
        {
          signal: new AbortController().signal,
          deadline: 1025,
        },
        options,
      ),
    ).rejects.toThrow("deadline exceeded");
    expect(events).toEqual(["BEGIN", "DML", "READ", "ROLLBACK"]);
  });

  it.each(
    engines,
  )("%s honors cancellation between read-back and commit", async (engine) => {
    const controller = new AbortController();
    const { driver, events } = harness(engine, async () => {
      controller.abort(new Error("cancelled during verification"));
      return [[9]];
    });
    if (engine === "sqlite") {
      (driver as unknown as { db: { queryBounded: unknown } }).db.queryBounded =
        () => {
          events.push("READ");
          controller.abort(new Error("cancelled during verification"));
          return { columns: ["__col_0"], rows: [[9]], rowCount: 1 };
        };
    }
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9" }],
        {
          signal: controller.signal,
          deadline: Date.now() + 10000,
        },
        options,
      ),
    ).rejects.toThrow("cancelled");
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });
});
