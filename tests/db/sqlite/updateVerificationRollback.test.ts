import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import type { SQLiteWorkerRequest } from "../../../src/extension/dbDrivers/sqliteWorkerProtocol";
import {
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";

const drivers: SQLiteDriver[] = [];
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

// Observe, but never alter, the real IPC client's active request and FIFO.
function workerRequests(driver: SQLiteDriver) {
  return (
    driver as unknown as {
      worker: {
        active?: { request: SQLiteWorkerRequest };
        queue: Array<{ request: SQLiteWorkerRequest }>;
      };
    }
  ).worker;
}

afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
  vi.restoreAllMocks();
});

describe("UPDATE verification transaction rollback", () => {
  it.each([
    { path: "prepared", preflight: "default" },
    { path: "atomic", preflight: "default" },
    { path: "prepared", preflight: "deferred" },
    { path: "atomic", preflight: "deferred" },
  ] as const)("$path with $preflight preflight verifies a changed primary key before reads queued behind the transaction", async ({
    path,
    preflight,
  }) => {
    const driver = new SQLiteDriver({
      id: "compatible",
      name: "Compatible",
      type: "sqlite",
      filePath: ":memory:",
    });
    drivers.push(driver);
    await driver.connect();
    await driver.query(
      "CREATE TABLE edits (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO edits VALUES (1, 0)",
    );
    const manager = {
      getDriver: () => driver,
      getConnection: () => ({ id: "compatible", type: "sqlite" }),
    } as never;
    const columns = await new TableReadService(manager).getColumns(
      "compatible",
      "",
      "main",
      "edits",
    );
    const prepared = prepareApplyChangesPlan(
      manager,
      "compatible",
      "",
      "main",
      "edits",
      [{ primaryKeys: { id: 1 }, changes: { id: "2", amount: "9" } }],
      columns,
    );
    if (!prepared.executable) throw new Error("Expected an apply plan");
    const risk = deferred<string | null>();
    const riskCheck =
      preflight === "deferred"
        ? vi
            .spyOn(driver, "getMutationAtomicityRisk")
            .mockReturnValue(risk.promise)
        : undefined;
    const enqueued = deferred<void>();
    const original = driver.runTransaction.bind(driver);
    const transaction = vi
      .spyOn(driver, "runTransaction")
      .mockImplementation((...args) => {
        // Observe the real transaction enqueue, not its completion.
        const pending = original(...args);
        enqueued.resolve();
        return pending;
      });
    let applySettled = false;
    const pending =
      path === "prepared"
        ? executePreparedApplyPlan(manager, prepared.plan)
        : executeAtomicSqlApplyPlan(manager, prepared.plan, []);
    void pending.then(() => {
      applySettled = true;
    });
    if (riskCheck) {
      expect(riskCheck).toHaveBeenCalledExactlyOnceWith("", "main", "edits");
      expect(transaction).not.toHaveBeenCalled();
      // Before metadata resolves, apply is not queued and a read may see old data.
      expect((await driver.query("SELECT id, amount FROM edits")).rows).toEqual(
        [{ __col_0: 1, __col_1: 0 }],
      );
      expect(transaction).not.toHaveBeenCalled();
      expect(applySettled).toBe(false);
      risk.resolve(null);
    }
    await enqueued.promise;
    const queuedRead = driver.query("SELECT id, amount FROM edits");
    // Inspect the IPC FIFO before a reply can complete the transaction.
    const worker = workerRequests(driver);
    const queueAtRead = {
      applySettled,
      active: worker.active?.request,
      queuedMethods: worker.queue.map(({ request }) => request.method),
    };
    // Drain both promises before assertions to avoid an unhandled read rejection.
    const [result, selected] = await Promise.all([pending, queuedRead]);
    expect(queueAtRead.applySettled).toBe(false);
    expect(queueAtRead.active?.method).toBe("runTransaction");
    expect(queueAtRead.queuedMethods).toEqual(["query"]);
    expect(queueAtRead.active?.args[2]).toMatchObject({
      verifications: [
        {
          rowIndex: 0,
          sql: expect.stringContaining('WHERE "id" = ?'),
          params: ["2"],
          values: [
            { column: { name: "id" }, expectedValue: "2" },
            { column: { name: "amount" }, expectedValue: "9" },
          ],
        },
      ],
    });
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ status: "applied" }],
    });
    expect(selected.rows).toEqual([{ __col_0: 2, __col_1: 9 }]);
    expect(transaction).toHaveBeenCalledTimes(1);
  });

  it.each([
    "prepared",
    "atomic",
  ] as const)("rolls back the entire %s batch on a trigger mismatch", async (mode) => {
    const driver = new SQLiteDriver({
      id: "b01",
      name: "B01",
      type: "sqlite",
      filePath: ":memory:",
    });
    drivers.push(driver);
    await driver.connect();
    await driver.query(`
      CREATE TABLE edits (id INTEGER PRIMARY KEY, amount INTEGER);
      INSERT INTO edits VALUES (1, 0), (2, 0);
      CREATE TRIGGER alter_amount AFTER UPDATE ON edits
      WHEN NEW.amount = 9 BEGIN
        UPDATE edits SET amount = 10 WHERE id = NEW.id;
      END;
    `);
    const manager = {
      getConnection: () => ({ id: "b01", type: "sqlite" }),
      getDriver: () => driver,
    } as never;
    const read = new TableReadService(manager);
    const columns = await read.getColumns("b01", "", "main", "edits");
    const prepared = prepareApplyChangesPlan(
      manager,
      "b01",
      "",
      "main",
      "edits",
      [
        { primaryKeys: { id: 1 }, changes: { amount: "4" } },
        { primaryKeys: { id: 2 }, changes: { amount: "9" } },
      ],
      columns,
    );
    if (!prepared.executable) throw new Error("Expected an apply plan");
    const insert = await new TableMutationService(
      manager,
      read,
    ).prepareInsertRow("b01", "", "main", "edits", { id: 3, amount: 5 });
    const result =
      mode === "prepared"
        ? await executePreparedApplyPlan(manager, prepared.plan)
        : await executeAtomicSqlApplyPlan(manager, prepared.plan, [insert]);
    expect(result.success).toBe(false);
    expect(result.warning).toBeUndefined();
    expect(result.insertApplied).not.toBe(true);
    expect(result.rowOutcomes?.[0]).toMatchObject({
      success: false,
      status: "skipped",
    });
    expect(result.rowOutcomes?.[1].status).toBe("verification_failed");
    expect(
      (await driver.query("SELECT id, amount FROM edits ORDER BY id")).rows,
    ).toEqual([
      { __col_0: 1, __col_1: 0 },
      { __col_0: 2, __col_1: 0 },
    ]);
  });
});
