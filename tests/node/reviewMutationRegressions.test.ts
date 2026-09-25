import mssql from "mssql";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";

const config = { id: "regression", name: "Regression", type: "pg" as const };
const column = (
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta => ({
  name: "value",
  type: nativeType,
  nativeType,
  category,
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("review mutation regressions", () => {
  it.each([
    1, 0,
  ] as const)("validates MSSQL DML count %s independently of trigger row counts", async (affectedRows) => {
    const driver = new MSSQLDriver({ ...config, type: "mssql" });
    (driver as unknown as { pool: unknown }).pool = { config: { options: {} } };
    const request = {
      output: vi.fn(),
      query: vi.fn(async () => ({
        rowsAffected: [1, affectedRows],
        output: { __rapidb_affected_rows: affectedRows },
      })),
    };
    vi.spyOn(mssql.Transaction.prototype, "begin").mockImplementation(
      (async () => undefined) as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "request").mockReturnValue(
      request as never,
    );
    const commit = vi
      .spyOn(mssql.Transaction.prototype, "commit")
      .mockImplementation((async () => undefined) as never);
    const rollback = vi
      .spyOn(mssql.Transaction.prototype, "rollback")
      .mockImplementation((async () => undefined) as never);

    const pending = driver.runTransaction([
      { sql: "DELETE FROM [items] WHERE [id] = 1", expectedAffectedRows: 1 },
    ]);
    if (affectedRows === 1) {
      await expect(pending).resolves.toBeUndefined();
      expect(commit).toHaveBeenCalledOnce();
      expect(rollback).not.toHaveBeenCalled();
    } else {
      await expect(pending).rejects.toThrow("Mutation affected 0 row(s)");
      expect(rollback).toHaveBeenCalledOnce();
      expect(commit).not.toHaveBeenCalled();
    }
    expect(request.output).toHaveBeenCalledWith(
      "__rapidb_affected_rows",
      mssql.Int,
    );
    expect(request.query).toHaveBeenCalledWith(
      "DELETE FROM [items] WHERE [id] = 1\n;SET @__rapidb_affected_rows = @@ROWCOUNT;",
    );
  });

  it.each([
    ["pg", false],
    ["pg", true],
    ["mysql", false],
    ["mysql", true],
  ] as const)("fences late %s transaction statements and commit after timeout (more statements: %s)", async (type, moreStatements) => {
    vi.useFakeTimers();
    let finish: (() => void) | undefined;
    const slow = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const query = vi.fn(async (sql: string | { sql: string }) => {
      if ((typeof sql === "string" ? sql : sql.sql) === "slow") await slow;
      return type === "pg" ? { rowCount: 1 } : [{ affectedRows: 1 }];
    });
    const connection = {
      query,
      release: vi.fn(),
      destroy: vi.fn(),
      beginTransaction: vi.fn(),
      commit: vi.fn(),
      rollback: vi.fn(async () => undefined),
    };
    const raw =
      type === "pg"
        ? new PostgresDriver(config)
        : new MySQLDriver({ ...config, type: "mysql" });
    (raw as unknown as { pool: unknown }).pool = {
      connect: async () => connection,
      getConnection: async () => connection,
    };
    const driver = createTimeoutAwareDriver(raw, () => ({
      connectionTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutSeconds: 1,
      dbOperationTimeoutMs: 25,
    }));
    const pending = driver.runTransaction([
      { sql: "slow" },
      ...(moreStatements ? [{ sql: "must-not-run" }] : []),
    ]);
    const rejection = expect(pending).rejects.toThrow("outcome may be unknown");
    await vi.advanceTimersByTimeAsync(25);
    await rejection;
    finish?.();
    await vi.runAllTimersAsync();
    const statements = query.mock.calls.map(([sql]) =>
      typeof sql === "string" ? sql : sql.sql,
    );
    expect(statements).toContain("slow");
    expect(statements).not.toContain("must-not-run");
    expect(statements).not.toContain("COMMIT");
    expect(connection.commit).not.toHaveBeenCalled();
    expect(
      type === "pg" ? connection.release : connection.destroy,
    ).toHaveBeenCalledOnce();
  });

  it.each([
    "oracle",
    "mssql",
  ] as const)("fences %s commit even when request cancellation settles late", async (type) => {
    vi.useFakeTimers();
    let finish: (() => void) | undefined;
    const slow = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const execute = vi.fn(async (sql: string) => {
      if (sql === "slow") await slow;
      return { rowsAffected: [1] };
    });
    const cancel = vi.fn(async () => undefined);
    const commit = vi.fn(async () => undefined);
    const rollback = vi.fn(async () => undefined);
    const raw =
      type === "oracle"
        ? new OracleDriver({ ...config, type })
        : new MSSQLDriver({ ...config, type });
    if (type === "oracle") {
      (raw as unknown as { pool: unknown }).pool = {
        getConnection: async () => ({
          execute,
          break: cancel,
          commit,
          rollback,
          close: vi.fn(),
        }),
      };
    } else {
      (raw as unknown as { pool: unknown }).pool = { config: { options: {} } };
      vi.spyOn(mssql.Transaction.prototype, "begin").mockImplementation(
        (async () => undefined) as never,
      );
      vi.spyOn(mssql.Transaction.prototype, "request").mockReturnValue({
        query: execute,
        cancel,
      } as never);
      vi.spyOn(mssql.Transaction.prototype, "commit").mockImplementation(
        commit as never,
      );
      vi.spyOn(mssql.Transaction.prototype, "rollback").mockImplementation(
        rollback as never,
      );
    }
    const driver = createTimeoutAwareDriver(raw, () => ({
      connectionTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutSeconds: 1,
      dbOperationTimeoutMs: 25,
    }));
    const pending = driver.runTransaction([{ sql: "slow" }]);
    const rejection = expect(pending).rejects.toThrow("outcome may be unknown");
    await vi.advanceTimersByTimeAsync(25);
    await rejection;
    expect(cancel).toHaveBeenCalledOnce();
    finish?.();
    await vi.runAllTimersAsync();
    expect(commit).not.toHaveBeenCalled();
    expect(rollback).toHaveBeenCalledOnce();
  });

  it("checks the SQLite deadline before commit even while the timer is blocked", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1000);
    const exec = vi.fn();
    const driver = new SQLiteDriver({ ...config, type: "sqlite" });
    (driver as unknown as { db: unknown }).db = {
      isOpen: true,
      inTransaction: true,
      exec,
      run: () => {
        vi.setSystemTime(1026);
        return { changes: 1 };
      },
    };
    await expect(
      driver.runTransaction([{ sql: "slow" }], {
        signal: new AbortController().signal,
        deadline: 1025,
      }),
    ).rejects.toThrow("deadline exceeded");
    expect(exec.mock.calls).toEqual([["BEGIN TRANSACTION"], ["ROLLBACK"]]);
  });

  it("uses type-aware SQL OCC predicates for JSON, LOBs, text and XML", () => {
    const pg = new PostgresDriver(config);
    const oracle = new OracleDriver({ ...config, type: "oracle" });
    const mssql = new MSSQLDriver({ ...config, type: "mssql" });
    expect(pg.buildOriginalValueComparison(column("json", "json"), 3)).toBe(
      '"value"::text = $3::text',
    );
    expect(oracle.buildOriginalValueComparison(column("CLOB", "text"), 3)).toBe(
      'DBMS_LOB.COMPARE("value", TO_CLOB(:3)) = 0',
    );
    expect(
      oracle.buildOriginalValueComparison(column("NCLOB", "text"), 3),
    ).toContain("TO_NCLOB(:3)");
    for (const type of ["text", "ntext", "xml"]) {
      expect(
        mssql.buildOriginalValueComparison(column(type, "text"), 3),
      ).toContain("CONVERT(varbinary(max), CONVERT(nvarchar(max), [value]))");
    }
  });

  it.each([
    "  padded  ",
    '"quoted"',
    "'quoted'",
    "NULL",
  ])("preserves Mongo OCC string snapshot %s through plan preparation", (original) => {
    const driver = new MongoDBDriver({ ...config, type: "mongodb" });
    const prepared = prepareApplyChangesPlan(
      { getDriver: () => driver } as never,
      config.id,
      "db",
      "db",
      "rows",
      [
        {
          primaryKeys: { _id: "key" },
          changes: { value: "new" },
          originalValues: { value: original },
        },
      ],
      [
        { ...column("string", "text"), name: "_id", isPrimaryKey: true },
        column("string", "text"),
      ],
    );
    if (!prepared.executable) throw new Error("Expected executable plan");
    expect(prepared.plan.updates[0].originalValues).toEqual({
      value: original,
    });
  });
});
