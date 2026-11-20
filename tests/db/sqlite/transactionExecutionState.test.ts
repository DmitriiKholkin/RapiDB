import { afterEach, describe, expect, it, vi } from "vitest";
import {
  isMutationNotExecutedError,
  MutationNotExecutedError,
} from "../../../src/extension/dbDrivers/mutationExecutionState";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import type { SQLiteDatabase } from "../../../src/extension/dbDrivers/sqliteRuntime";

describe("SQLite transaction execution state", () => {
  const driver = new SQLiteCoreDriver({
    id: "transaction-execution-state",
    name: "Transaction execution state",
    type: "sqlite",
    filePath: ":memory:",
  });
  const operations = [{ sql: "INSERT INTO items VALUES (2)" }];

  afterEach(async () => {
    await driver.disconnect();
  });

  async function connect() {
    await driver.connect();
    const { db } = driver as unknown as { db: SQLiteDatabase | null };
    if (!db) throw new Error("Expected an open SQLite test database");
    db.exec("CREATE TABLE items (value INTEGER)");
    return db;
  }

  it("marks a failed BEGIN as not executed without touching the outer transaction", async () => {
    const db = await connect();
    db.exec("BEGIN; INSERT INTO items VALUES (1)");
    const exec = vi.spyOn(db, "exec");
    const run = vi.spyOn(db, "run");

    const error = await driver
      .runTransaction(operations)
      .catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(MutationNotExecutedError);
    expect(error).toMatchObject({
      executionState: "not-executed",
      message: expect.stringMatching(/did not start.*within a transaction/i),
      cause: { code: "SQLITE_ERROR" },
    });
    expect(exec.mock.calls).toEqual([["BEGIN TRANSACTION"]]);
    expect(run).not.toHaveBeenCalled();
    expect(db.inTransaction).toBe(true);
    db.exec("COMMIT");
    expect(db.all("SELECT value FROM items")).toEqual([{ value: 1 }]);
  });

  it("marks a missing SQLite connection as not executed", async () => {
    await expect(driver.runTransaction(operations)).rejects.toMatchObject({
      name: "MutationNotExecutedError",
      executionState: "not-executed",
      cause: { message: "[RapiDB] SQLite connection is not open" },
    });
  });

  it("marks cancellation before BEGIN as not executed", async () => {
    const db = await connect();
    const exec = vi.spyOn(db, "exec");
    const cause = new Error("Cancelled before starting");

    await expect(
      driver.runTransaction(operations, {
        signal: AbortSignal.abort(cause),
        deadline: Date.now() + 10_000,
      }),
    ).rejects.toMatchObject({ executionState: "not-executed", cause });
    expect(exec).not.toHaveBeenCalled();
  });

  it("recognizes a schema conflict that rolls back the owned transaction", async () => {
    const db = await connect();
    db.exec(`
      CREATE TABLE guarded (value INTEGER UNIQUE ON CONFLICT ROLLBACK);
      INSERT INTO guarded VALUES (1);
    `);
    const exec = vi.spyOn(db, "exec");

    await expect(
      driver.runTransaction([
        ...operations,
        { sql: "INSERT INTO guarded VALUES (1)" },
      ]),
    ).rejects.toMatchObject({
      executionState: "not-executed",
      cause: { code: "SQLITE_CONSTRAINT_UNIQUE" },
    });
    expect(exec.mock.calls).toEqual([["BEGIN TRANSACTION"]]);
    expect(db.inTransaction).toBe(false);
    expect(db.all("SELECT value FROM items")).toEqual([]);
    expect(db.all("SELECT value FROM guarded")).toEqual([{ value: 1 }]);
  });

  it.each([
    { boundary: "END TRANSACTION", afterCommit: [] },
    {
      boundary: "COMMIT",
      afterCommit: [{ sql: "BEGIN" }, { sql: "INSERT INTO items VALUES (3)" }],
    },
  ])("keeps failures after a raw $boundary uncertain even if a later transaction rolls back", async ({
    boundary,
    afterCommit,
  }) => {
    const db = await connect();
    const error = await driver
      .runTransaction([
        ...operations,
        { sql: boundary },
        ...afterCommit,
        { sql: "INSERT INTO missing VALUES (4)" },
      ])
      .catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(Error);
    expect(isMutationNotExecutedError(error)).toBe(false);
    expect(db.inTransaction).toBe(false);
    expect(db.all("SELECT value FROM items")).toEqual([{ value: 2 }]);
  });

  it("does not confirm rollback when SQLite transaction state is unreadable", async () => {
    const db = await connect();
    const stateFailure = new Error("Transaction state is unavailable");
    vi.spyOn(db, "inTransaction", "get").mockImplementation(() => {
      throw stateFailure;
    });

    const error = await driver
      .runTransaction([
        ...operations,
        { sql: "INSERT INTO missing VALUES (3)" },
      ])
      .catch((failure: unknown) => failure);

    expect(isMutationNotExecutedError(error)).toBe(false);
    expect(error).toMatchObject({
      name: "SQLiteRollbackFailure",
      errors: [
        expect.objectContaining({
          message: expect.stringContaining("no such table: missing"),
        }),
        stateFailure,
      ],
    });
  });

  it("does not report not-executed if COMMIT took effect before throwing", async () => {
    const db = await connect();
    const commitFailure = new Error("Commit acknowledgement failed");
    const exec = db.exec.bind(db);
    const execSpy = vi.spyOn(db, "exec").mockImplementation((sql) => {
      exec(sql);
      if (sql === "COMMIT") throw commitFailure;
    });

    await expect(driver.runTransaction(operations)).rejects.toBe(commitFailure);
    expect(isMutationNotExecutedError(commitFailure)).toBe(false);
    expect(execSpy.mock.calls).toEqual([["BEGIN TRANSACTION"], ["COMMIT"]]);
    expect(db.all("SELECT value FROM items")).toEqual([{ value: 2 }]);
  });

  it("confirms rollback when a deferred foreign key rejects COMMIT and leaves the transaction active", async () => {
    const db = await connect();
    db.exec(`
      CREATE TABLE parent (id INTEGER PRIMARY KEY);
      CREATE TABLE child (
        parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
      );
    `);
    const exec = vi.spyOn(db, "exec");

    await expect(
      driver.runTransaction([
        ...operations,
        { sql: "INSERT INTO child VALUES (1)" },
      ]),
    ).rejects.toMatchObject({
      executionState: "not-executed",
      cause: { code: "SQLITE_CONSTRAINT_FOREIGNKEY" },
    });
    expect(exec.mock.calls).toEqual([
      ["BEGIN TRANSACTION"],
      ["COMMIT"],
      ["ROLLBACK"],
    ]);
    expect(db.inTransaction).toBe(false);
    expect(db.all("SELECT value FROM items")).toEqual([]);
    expect(db.all("SELECT parent_id FROM child")).toEqual([]);
  });

  it("keeps rollback failure unsafe and preserves both errors", async () => {
    const db = await connect();
    const rollbackFailure = new Error("Rollback failed");
    const exec = db.exec.bind(db);
    vi.spyOn(db, "exec").mockImplementation((sql) => {
      if (sql === "ROLLBACK") throw rollbackFailure;
      exec(sql);
    });

    const error = await driver
      .runTransaction([
        ...operations,
        { sql: "INSERT INTO missing VALUES (3)" },
      ])
      .catch((failure: unknown) => failure);

    expect(isMutationNotExecutedError(error)).toBe(false);
    expect(error).toMatchObject({
      name: "SQLiteRollbackFailure",
      errors: [
        expect.objectContaining({
          message: expect.stringContaining("no such table: missing"),
        }),
        rollbackFailure,
      ],
    });
    expect(db.inTransaction).toBe(true);
  });
});
