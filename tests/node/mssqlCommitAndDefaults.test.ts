import { EventEmitter } from "node:events";
import mssql from "mssql";
import { describe, expect, it, vi } from "vitest";
import {
  cleanMssqlDefault,
  MSSQLDriver,
} from "../../src/extension/dbDrivers/mssql";
import { MutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";

function harness() {
  const driver = new MSSQLDriver({
    id: "commit-failure",
    name: "QA",
    type: "mssql",
  });
  const order: string[] = [];
  const connection = Object.assign(new EventEmitter(), {
    config: { options: {} },
    hasError: false,
    beginTransaction(callback: (error?: Error) => void) {
      callback();
    },
    commitTransaction: vi.fn((callback: (error?: Error) => void) => {
      callback(new Error("Connection lost during COMMIT"));
    }),
    rollbackTransaction: vi.fn((callback: (error?: Error) => void) =>
      callback(),
    ),
    close: vi.fn(() => {
      order.push("close");
    }),
  });
  const pool = {
    config: connection.config,
    connected: true,
    acquire(
      _requester: unknown,
      callback: (
        error: null,
        physical: typeof connection,
        config: typeof connection.config,
      ) => void,
    ) {
      callback(null, connection, connection.config);
    },
    release: vi.fn((released: typeof connection) => {
      order.push(released.hasError ? "release:poisoned" : "release:healthy");
    }),
  };
  Object.assign(driver, { pool });
  return { driver, connection, pool, order };
}

describe("MSSQL commit failure", () => {
  it("closes and poisons the connection before mssql returns it to the pool", async () => {
    const h = harness();
    const commitTransaction = h.connection.commitTransaction;

    const failure = await h.driver.runTransaction([]).catch((error) => error);

    expect(failure).toBeInstanceOf(AggregateError);
    expect(failure).not.toBeInstanceOf(MutationNotExecutedError);
    expect(String(failure.message)).toContain(
      "MSSQL COMMIT failed; the transaction outcome is unknown",
    );
    expect(String(failure.message)).toContain("Connection lost during COMMIT");
    expect(String(failure.message)).not.toContain("rollback");
    expect(failure.cause).toBe(failure.errors[0]);
    expect(failure.errors).toHaveLength(1);
    expect(h.connection.hasError).toBe(true);
    expect(h.order).toEqual(["close", "release:poisoned"]);
    expect(h.pool.release).toHaveBeenCalledExactlyOnceWith(h.connection);
    expect(h.connection.rollbackTransaction).not.toHaveBeenCalled();
    expect(h.connection.commitTransaction).toBe(commitTransaction);
  });

  it("includes a session close error raised while fencing a failed commit", async () => {
    const h = harness();
    const closeError = new Error("socket already destroyed");
    h.connection.close.mockImplementation(() => {
      h.order.push("close");
      throw closeError;
    });

    const failure = await h.driver.runTransaction([]).catch((error) => error);

    expect(failure).toBeInstanceOf(AggregateError);
    expect(failure.errors).toHaveLength(2);
    expect(failure.errors[1]).toBe(closeError);
    expect(String(failure.message)).toContain(
      "session close error: socket already destroyed",
    );
    expect(h.connection.hasError).toBe(true);
    expect(h.connection.rollbackTransaction).not.toHaveBeenCalled();
  });

  it("rolls back when mssql rejects COMMIT before sending it", async () => {
    const h = harness();
    const commitError = new Error(
      "Can't commit transaction. There is a request in progress.",
    );
    const commitSpy = vi
      .spyOn(mssql.Transaction.prototype, "commit")
      .mockRejectedValueOnce(commitError);

    try {
      const failure = await h.driver.runTransaction([]).catch((error) => error);

      expect(failure).toBe(commitError);
      expect(h.connection.commitTransaction).not.toHaveBeenCalled();
      expect(h.connection.rollbackTransaction).toHaveBeenCalledOnce();
      expect(h.connection.hasError).toBe(false);
      expect(h.order).toEqual(["release:healthy"]);
    } finally {
      commitSpy.mockRestore();
    }
  });

  describe("when the server rolls back during COMMIT", () => {
    async function runWithServerRollback(
      h: ReturnType<typeof harness>,
      respond: (callback: (error?: Error) => void) => void,
    ) {
      const uncaught: unknown[] = [];
      const onUncaught = (error: unknown) => {
        uncaught.push(error);
      };
      process.on("uncaughtException", onUncaught);
      const commitTransaction = h.connection.commitTransaction;
      h.connection.commitTransaction.mockImplementation((callback) => {
        h.connection.emit("rollbackTransaction");
        respond(callback);
      });
      try {
        const failure = await h.driver
          .runTransaction([])
          .catch((error) => error);
        await new Promise((resolve) => setTimeout(resolve, 20));
        expect(uncaught).toEqual([]);
        expect(h.connection.commitTransaction).toBe(commitTransaction);
        expect(h.pool.release).toHaveBeenCalledExactlyOnceWith(h.connection);
        expect(h.connection.rollbackTransaction).not.toHaveBeenCalled();
        return failure;
      } finally {
        process.off("uncaughtException", onUncaught);
      }
    }

    it("reports a definite rollback with the COMMIT error", async () => {
      const h = harness();
      const commitError = new Error(
        "The current transaction failed to commit due to a serializable validation failure.",
      );

      const failure = await runWithServerRollback(h, (callback) => {
        process.nextTick(callback, commitError);
      });

      expect(failure).toBeInstanceOf(MutationNotExecutedError);
      expect(String(failure.message)).toContain(
        "[RapiDB] SQL Server rolled back the transaction during COMMIT; no changes were applied.",
      );
      expect(String(failure.message)).toContain(
        "serializable validation failure",
      );
      expect(failure.cause).toBe(commitError);
      expect(h.connection.close).not.toHaveBeenCalled();
    });

    it("reports a definite rollback when COMMIT returns without an error", async () => {
      const h = harness();

      const failure = await runWithServerRollback(h, (callback) => {
        process.nextTick(callback);
      });

      expect(failure).toBeInstanceOf(MutationNotExecutedError);
      expect(String(failure.message)).toContain(
        "Transaction was rolled back by the server",
      );
    });

    it("does not wait indefinitely for a late COMMIT response", async () => {
      const h = harness();

      const failure = await runWithServerRollback(h, (callback) => {
        setTimeout(callback, 10, new Error("late COMMIT error"));
      });

      expect(failure).toBeInstanceOf(MutationNotExecutedError);
      expect(String(failure.message)).toContain(
        "Transaction was rolled back by the server",
      );
    });
  });

  it("keeps the connection pooled after a successful commit", async () => {
    const h = harness();
    const commitTransaction = h.connection.commitTransaction;
    h.connection.commitTransaction.mockImplementation((callback) => callback());

    await h.driver.runTransaction([]);

    expect(h.connection.hasError).toBe(false);
    expect(h.connection.close).not.toHaveBeenCalled();
    expect(h.order).toEqual(["release:healthy"]);
    expect(h.connection.commitTransaction).toBe(commitTransaction);
    expect(
      Object.getOwnPropertyDescriptor(h.connection, "commitTransaction")?.value,
    ).toBe(commitTransaction);
  });
});

describe("cleanMssqlDefault", () => {
  it.each([
    ["((0))", "0"],
    ["((1)+(2))", "(1)+(2)"],
    ["(N'abc')", "'abc'"],
    ["('a)(b')", "'a)(b'"],
    ["('it''s (x')", "'it''s (x'"],
    ["([dbo].[f)]())", "[dbo].[f)]()"],
    ["(getdate())", "getdate()"],
    ["(1)+(2)", "(1)+(2)"],
  ])("cleans %s", (raw, expected) => {
    expect(cleanMssqlDefault(raw)).toBe(expected);
  });
});
