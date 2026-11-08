import mssql from "mssql";
import oracledb from "oracledb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import {
  isMutationNotExecutedError,
  MutationNotExecutedError,
} from "../../src/extension/dbDrivers/mutationExecutionState";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import { TransactionVerificationError } from "../../src/extension/dbDrivers/transactionVerification";
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
  let released = false;
  const release = vi.fn(() => {
    if (engine === "pg") {
      expect(released).toBe(false);
      released = true;
    }
  });
  const metadata = [{ name: "__col_0", dbType: oracledb.DB_TYPE_NUMBER }];
  const execute = vi.fn(
    async (
      input: string | { sql: string },
      _params?: unknown,
      executeOptions?: oracledb.ExecuteOptions,
    ) => {
      if (engine === "pg" && released)
        throw new Error("SQL on a released PostgreSQL client");
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
        return engine === "mysql" ? [{ warningStatus: 0 }] : {};
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
  return {
    driver,
    events,
    lease,
    execute,
    outsideQuery,
    cancel,
    release,
    commit,
    rollback,
  };
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
    const { driver, events, outsideQuery, release } = harness(
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
    if (engine === "pg") expect(release).toHaveBeenCalledExactlyOnceWith();
  });

  it("confirms an MSSQL verification failure only after rollback succeeds", async () => {
    const { driver, events } = harness("mssql", async () => [[10]]);
    let transactionError: unknown;

    try {
      await driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      );
    } catch (error) {
      transactionError = error;
    }

    expect(transactionError).toBeInstanceOf(TransactionVerificationError);
    expect(transactionError).toMatchObject({ rollbackConfirmed: true });
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });

  it.each([
    "pg",
    "mysql",
    "oracle",
    "sqlite",
  ] as const)("confirms %s verification rollback", async (engine) => {
    const { driver } = harness(engine, async () => [[10]]);
    if (engine === "sqlite") {
      (driver as unknown as { db: { queryBounded: unknown } }).db.queryBounded =
        () => ({ columns: ["__col_0"], rows: [[10]], rowCount: 1 });
    }
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9" }],
        undefined,
        options,
      ),
    ).rejects.toMatchObject({ rollbackConfirmed: true });
  });

  it("preserves Oracle verification evidence when close fails after rollback", async () => {
    const { driver, rollback, release, events } = harness(
      "oracle",
      async () => {
        throw new Error("network read failed");
      },
    );
    release.mockImplementation(() => {
      events.push("CLOSE");
      throw new Error("close failed");
    });
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9" }],
        undefined,
        options,
      ),
    ).rejects.toMatchObject({
      rollbackConfirmed: true,
      message: expect.stringContaining("network read failed"),
    });
    expect(rollback).toHaveBeenCalledOnce();
    expect(events.indexOf("ROLLBACK")).toBeLessThan(events.indexOf("CLOSE"));
  });

  it("preserves an acknowledged Oracle commit when close fails", async () => {
    const { driver, commit, release, events } = harness("oracle", async () => [
      [9],
    ]);
    release.mockImplementation(() => {
      events.push("CLOSE");
      throw new Error("close failed");
    });
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9" }],
        undefined,
        options,
      ),
    ).resolves.toBeUndefined();
    expect(commit).toHaveBeenCalledOnce();
    expect(events.indexOf("COMMIT")).toBeLessThan(events.indexOf("CLOSE"));
  });

  it("does not confirm Oracle rollback when rollback and close fail", async () => {
    const { driver, rollback, release } = harness("oracle", async () => {
      throw new Error("network read failed");
    });
    rollback.mockRejectedValue(new Error("rollback failed"));
    release.mockImplementation(() => {
      throw new Error("close failed");
    });
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9" }],
        undefined,
        options,
      ),
    ).rejects.toMatchObject({
      message: expect.stringContaining("rollback failed"),
      errors: [
        expect.objectContaining({ rollbackConfirmed: false }),
        expect.any(Error),
      ],
    });
    expect(release).toHaveBeenCalledOnce();
  });

  it("does not confirm an MSSQL verification failure when rollback fails", async () => {
    const { driver, events } = harness("mssql", async () => [[10]]);
    vi.spyOn(mssql.Transaction.prototype, "rollback").mockRejectedValue(
      new Error("rollback failed"),
    );

    let transactionError: unknown;
    try {
      await driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      );
    } catch (error) {
      transactionError = error;
    }

    expect(transactionError).toBeInstanceOf(AggregateError);
    expect(transactionError).toMatchObject({
      message: expect.stringContaining("transaction and rollback failed"),
    });
    expect(events).not.toContain("COMMIT");
  });

  it("marks an MSSQL pre-commit statement error not executed after rollback", async () => {
    const { driver, events, execute } = harness("mssql", async () => [[9]]);
    execute.mockRejectedValueOnce(new Error("CLR is disabled"));

    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      ),
    ).rejects.toMatchObject({
      name: "MutationNotExecutedError",
      executionState: "not-executed",
      message: "CLR is disabled",
    });
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });

  it.each([
    "statement",
    "verification",
  ] as const)("keeps an MSSQL server-aborted %s failure outcome unknown", async (failure) => {
    const { driver, execute, rollback, commit } = harness("mssql", async () => [
      [10],
    ]);
    const statementError = new Error("The transaction ended in the trigger.");
    if (failure === "statement") execute.mockRejectedValueOnce(statementError);
    const abortedError = Object.assign(
      new Error("Transaction has been aborted."),
      { code: "EABORT" },
    );
    rollback.mockRejectedValueOnce(abortedError);

    let transactionError: unknown;
    try {
      await driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      );
    } catch (error) {
      transactionError = error;
    }

    expect(transactionError).toBeInstanceOf(AggregateError);
    expect(isMutationNotExecutedError(transactionError)).toBe(false);
    expect(transactionError).not.toBeInstanceOf(TransactionVerificationError);
    const errors = (transactionError as AggregateError).errors;
    expect(errors[1]).toBe(abortedError);
    if (failure === "statement") {
      expect(errors[0]).toBe(statementError);
    } else {
      expect(errors[0]).toBeInstanceOf(TransactionVerificationError);
      expect(errors[0]).toMatchObject({ rollbackConfirmed: false });
    }
    expect(rollback).toHaveBeenCalledExactlyOnceWith();
    expect(commit).not.toHaveBeenCalled();
  });

  it("keeps an MSSQL XACT_ABORT outcome unknown when session close fails", async () => {
    const { driver, execute, commit } = harness("mssql", async () => [[9]]);
    execute.mockRejectedValueOnce(new Error("CLR is disabled"));
    const closeError = new Error("session close failed");
    const abortedError = Object.assign(
      new Error("Transaction has been aborted."),
      { code: "EABORT" },
    );
    const connection = {
      rollbackTransaction: vi.fn((callback: (error?: unknown) => void) => {
        callback(abortedError);
      }),
      close: vi.fn(() => {
        throw closeError;
      }),
      removeListener: vi.fn(),
    };

    vi.spyOn(mssql.Transaction.prototype, "begin").mockImplementation(
      async function (this: mssql.Transaction) {
        (
          this as unknown as { _acquiredConnection: typeof connection }
        )._acquiredConnection = connection;
      } as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "rollback").mockImplementation(
      async function (this: mssql.Transaction) {
        const acquired = (
          this as unknown as { _acquiredConnection: typeof connection }
        )._acquiredConnection;
        await new Promise<void>((resolve, reject) => {
          acquired.rollbackTransaction((error) =>
            error ? reject(error) : resolve(),
          );
        });
      } as never,
    );

    let transactionError: unknown;
    try {
      await driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      );
    } catch (error) {
      transactionError = error;
    }

    expect(transactionError).toBeInstanceOf(AggregateError);
    expect(transactionError).toMatchObject({
      message: expect.stringContaining(
        "session close error: session close failed",
      ),
    });
    expect(isMutationNotExecutedError(transactionError)).toBe(false);
    expect(connection.rollbackTransaction).toHaveBeenCalledOnce();
    expect(connection.close).toHaveBeenCalledOnce();
    expect(commit).not.toHaveBeenCalled();
  });

  it("keeps MSSQL commit failures outcome-unknown after a later rollback", async () => {
    const { driver, events, commit } = harness("mssql", async () => [[9]]);
    commit.mockRejectedValueOnce(new Error("connection lost during commit"));

    let transactionError: unknown;
    try {
      await driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      );
    } catch (error) {
      transactionError = error;
    }

    expect(transactionError).toMatchObject({
      message: expect.stringContaining("connection lost during commit"),
    });
    expect(isMutationNotExecutedError(transactionError)).toBe(false);
    expect(transactionError).not.toBeInstanceOf(MutationNotExecutedError);
    expect(events).toContain("ROLLBACK");
    expect(commit).toHaveBeenCalledExactlyOnceWith();
  });

  it("labels an Oracle verification failure as rolled back only after rollback succeeds", async () => {
    const { driver, events } = harness("oracle", async () => [[10]]);

    let transactionError: unknown;
    try {
      await driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9", expectedAffectedRows: 1 }],
        undefined,
        options,
      );
    } catch (error) {
      transactionError = error;
    }

    expect(transactionError).toBeInstanceOf(TransactionVerificationError);
    expect(transactionError).toMatchObject({ rollbackConfirmed: true });
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
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
      execute,
      release,
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
    const sqlCountAtTimeout = execute.mock.calls.length;
    if (engine === "pg") expect(release).toHaveBeenCalledExactlyOnceWith(true);
    finish();
    await vi.runAllTimersAsync();
    expect(events).not.toContain("COMMIT");
    if (engine === "pg") {
      expect(events).not.toContain("ROLLBACK");
      expect(execute).toHaveBeenCalledTimes(sqlCountAtTimeout);
      expect(release).toHaveBeenCalledExactlyOnceWith(true);
    } else {
      expect(events).toContain("ROLLBACK");
      expect(cancel).toHaveBeenCalled();
    }
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

  it("pg rolls back a still-live client when verification crosses the deadline without abort", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1000);
    const { driver, events, release } = harness("pg", async () => {
      vi.setSystemTime(1026);
      return [[9]];
    });
    const controller = new AbortController();
    await expect(
      driver.runTransaction(
        [{ sql: "UPDATE edits SET amount = 9" }],
        { signal: controller.signal, deadline: 1025 },
        options,
      ),
    ).rejects.toThrow("deadline exceeded");
    expect(controller.signal.aborted).toBe(false);
    expect(events).toEqual(["BEGIN", "DML", "READ", "ROLLBACK"]);
    expect(release).toHaveBeenCalledExactlyOnceWith();
  });

  it.each(
    engines,
  )("%s honors cancellation between read-back and commit", async (engine) => {
    const controller = new AbortController();
    const { driver, events, execute, release } = harness(engine, async () => {
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
    if (engine === "pg") {
      expect(release).toHaveBeenCalledExactlyOnceWith(true);
      expect(events).toEqual(["BEGIN", "DML", "READ"]);
      expect(execute).toHaveBeenCalledTimes(3);
    } else expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });
});
