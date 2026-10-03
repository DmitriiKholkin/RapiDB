import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
} from "../../../src/extension/dbDrivers/timeout";
import { TableMutationPreviewController } from "../../../src/extension/panels/tableMutationPreviewController";
import {
  executeAtomicSqlApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { createProjectTempDir } from "../../runtime/tempDirectories";

const drivers: SQLiteDriver[] = [];
afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
  vi.restoreAllMocks();
});

async function fixture(
  ddl = "CREATE TABLE edits (id INTEGER PRIMARY KEY, amount DECIMAL)",
) {
  const driver = new SQLiteDriver({
    id: "b02",
    name: "B02",
    type: "sqlite",
    filePath: ":memory:",
  });
  drivers.push(driver);
  await driver.connect();
  await driver.query(ddl);
  const manager = {
    getConnection: () => ({ id: "b02", type: "sqlite" }),
    getDriver: () => driver,
    getQueryEditorPresentation: () => ({ editorLanguage: "sql" }),
  } as never;
  const read = new TableReadService(manager);
  const service = new TableMutationService(manager, read);
  const prepare = (values: Record<string, unknown>) =>
    service.prepareInsertRow("b02", "", "main", "edits", values);
  return { driver, manager, read, service, prepare };
}

describe("INSERT verification transaction rollback (B02)", () => {
  it.each([
    false,
    true,
  ])("rejects DECIMAL precision loss (generated key: %s)", async (generated) => {
    const { driver, read, service, prepare } = await fixture();
    const requested = "1.234567890123456789";
    const columns = await read.getColumns("b02", "", "main", "edits");
    const amount = columns.find((column) => column.name === "amount");
    if (!amount) throw new Error("Missing DECIMAL metadata");
    expect(driver.checkPersistedEdit(amount, requested)).toMatchObject({
      ok: true,
      shouldVerify: true,
    });
    const plan = await prepare({
      ...(generated ? {} : { id: 1 }),
      amount: requested,
    });
    expect(
      plan.verification?.values.find(({ column }) => column.name === "amount")
        ?.expectedValue,
    ).toBe(requested);
    await expect(service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /verification/i,
    );
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([]);
  });

  it("demonstrates the SQLite storage incompatibility independently of the service", async () => {
    const { driver, read } = await fixture();
    const requested = "1.234567890123456789";
    await driver.query("INSERT INTO edits VALUES (1, ?)", [requested]);
    const persistedValue = (await driver.query("SELECT amount FROM edits"))
      .rows[0].__col_0;
    expect(String(persistedValue)).not.toBe(requested);
    const amount = (await read.getColumns("b02", "", "main", "edits")).find(
      (column) => column.name === "amount",
    );
    if (!amount) throw new Error("Missing DECIMAL metadata");
    expect(
      driver.checkPersistedEdit(amount, requested, { persistedValue })?.ok,
    ).toBe(false);
  });

  it("captures distinct generated identities and keeps queued reads outside the transaction", async () => {
    const { driver, manager, prepare } = await fixture();
    const inserts = await Promise.all([
      prepare({ amount: "1.25" }),
      prepare({ amount: "2.5" }),
    ]);
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const original = driver.runTransaction.bind(driver);
    vi.spyOn(driver, "runTransaction").mockImplementation((...args) => {
      const transaction = original(...args);
      entered();
      return transaction;
    });
    const pending = executeAtomicSqlApplyPlan(manager, null, inserts);
    await started;
    const read = driver.query("SELECT id, amount FROM edits ORDER BY id");
    expect(await pending).toMatchObject({ success: true, insertApplied: true });
    expect((await read).rows).toEqual([
      { __col_0: 1, __col_1: 1.25 },
      { __col_0: 2, __col_1: 2.5 },
    ]);
  });

  it.each([
    false,
    true,
  ])("rolls back all inserts and any update (mixed: %s)", async (mixed) => {
    const { driver, manager, read, prepare } = await fixture();
    await driver.query("INSERT INTO edits VALUES (1, 0)");
    const columns = await read.getColumns("b02", "", "main", "edits");
    const update = prepareApplyChangesPlan(
      manager,
      "b02",
      "",
      "main",
      "edits",
      [{ primaryKeys: { id: 1 }, changes: { amount: "4.5" } }],
      columns,
    );
    if (!update.executable) throw new Error("Expected update plan");
    const inserts = await Promise.all([
      prepare({ amount: "2.5" }),
      prepare({ amount: "1.234567890123456789" }),
    ]);
    const result = await executeAtomicSqlApplyPlan(
      manager,
      mixed ? update.plan : null,
      inserts,
    );
    expect(result).toMatchObject({
      success: false,
      error: expect.stringMatching(/INSERT verification failed/),
    });
    expect(result.insertApplied).not.toBe(true);
    expect(result.failedRows).toBeUndefined();
    if (mixed)
      expect(result.rowOutcomes).toMatchObject([
        { success: false, status: "skipped" },
      ]);
    expect((await driver.query("SELECT id, amount FROM edits")).rows).toEqual([
      { __col_0: 1, __col_1: 0 },
    ]);
  });

  it("verifies inserts after later UPDATE triggers, not only immediately after INSERT", async () => {
    const { driver, manager, read, prepare } = await fixture(`
      CREATE TABLE edits (id INTEGER PRIMARY KEY, amount INTEGER);
      INSERT INTO edits VALUES (1, 0);
      CREATE TRIGGER alter_insert AFTER UPDATE ON edits WHEN NEW.id = 1 BEGIN
        UPDATE edits SET amount = 99 WHERE id = 2;
      END;
    `);
    const columns = await read.getColumns("b02", "", "main", "edits");
    const update = prepareApplyChangesPlan(
      manager,
      "b02",
      "",
      "main",
      "edits",
      [{ primaryKeys: { id: 1 }, changes: { amount: "4" } }],
      columns,
    );
    if (!update.executable) throw new Error("Expected update plan");
    const insert = await prepare({ amount: "9" });
    const result = await executeAtomicSqlApplyPlan(manager, update.plan, [
      insert,
    ]);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/INSERT verification failed/);
    expect((await driver.query("SELECT id, amount FROM edits")).rows).toEqual([
      { __col_0: 1, __col_1: 0 },
    ]);
  });

  it.each([
    false,
    true,
  ])("verifies composite primary keys (default component: %s)", async (generated) => {
    const { driver, service, prepare } = await fixture(
      "CREATE TABLE edits (tenant TEXT, code TEXT DEFAULT 'generated', amount DECIMAL, PRIMARY KEY (tenant, code))",
    );
    await service.executePreparedInsertPlan(
      await prepare({
        tenant: "a",
        ...(generated ? {} : { code: "x" }),
        amount: "1.25",
      }),
    );
    await expect(
      service.executePreparedInsertPlan(
        await prepare({
          tenant: "b",
          ...(generated ? {} : { code: "x" }),
          amount: "1.234567890123456789",
        }),
      ),
    ).rejects.toThrow(/verification/);
    expect(
      (await driver.query("SELECT tenant, code, amount FROM edits")).rows,
    ).toEqual([
      { __col_0: "a", __col_1: generated ? "generated" : "x", __col_2: 1.25 },
    ]);
  });

  it("does not change default-only auto-generated key inserts", async () => {
    const { driver, service, prepare } = await fixture(
      "CREATE TABLE edits (id INTEGER PRIMARY KEY AUTOINCREMENT, amount INTEGER DEFAULT 7)",
    );
    await service.executePreparedInsertPlan(await prepare({}));
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([
      { __col_0: 1, __col_1: 7 },
    ]);
  });

  it("supports explicit and default-generated keys on WITHOUT ROWID tables", async () => {
    const { driver, service, prepare } = await fixture(
      "CREATE TABLE edits (tenant TEXT, code TEXT DEFAULT 'generated', amount DECIMAL, PRIMARY KEY (tenant, code)) WITHOUT ROWID",
    );
    await service.executePreparedInsertPlan(
      await prepare({ tenant: "a", code: "x", amount: "1.25" }),
    );
    await service.executePreparedInsertPlan(
      await prepare({ tenant: "b", amount: "2.5" }),
    );
    expect((await driver.query("SELECT tenant FROM edits")).rows).toEqual([
      { __col_0: "a" },
      { __col_0: "b" },
    ]);
  });

  it("rolls back incompatible values with a default-generated WITHOUT ROWID primary key", async () => {
    const { driver, service, prepare } = await fixture(
      "CREATE TABLE edits (id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))), amount DECIMAL) WITHOUT ROWID",
    );
    await expect(
      service.executePreparedInsertPlan(
        await prepare({ amount: "1.234567890123456789" }),
      ),
    ).rejects.toThrow(/INSERT verification failed/);
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([]);
  });

  it("verifies a nullable composite key without treating NULL as a missing identity", async () => {
    const { driver, service, prepare } = await fixture(
      "CREATE TABLE edits (tenant TEXT, code TEXT, amount DECIMAL, PRIMARY KEY (tenant, code))",
    );
    await service.executePreparedInsertPlan(
      await prepare({ tenant: "a", code: null, amount: "1.25" }),
    );
    expect(
      (await driver.query("SELECT tenant, code, amount FROM edits")).rows,
    ).toEqual([{ __col_0: "a", __col_1: null, __col_2: 1.25 }]);
  });

  it("fails closed on ambiguous nullable composite keys and retains only the old row", async () => {
    const { driver, service, prepare } = await fixture(
      "CREATE TABLE edits (tenant TEXT, code TEXT, amount DECIMAL, PRIMARY KEY (tenant, code)); INSERT INTO edits VALUES ('a', NULL, 7)",
    );
    await expect(
      service.executePreparedInsertPlan(
        await prepare({ tenant: "a", code: null, amount: "1.25" }),
      ),
    ).rejects.toThrow(/could not be read back uniquely/);
    expect((await driver.query("SELECT amount FROM edits")).rows).toEqual([
      { __col_0: 7 },
    ]);
  });

  it("rolls back precision-loss insert batches with nullable/default SQLite keys", async () => {
    const { driver, manager, prepare } = await fixture(
      "CREATE TABLE edits (tenant TEXT, code TEXT, amount DECIMAL, PRIMARY KEY (tenant, code))",
    );
    const inserts = await Promise.all([
      prepare({ tenant: "a", code: null, amount: "1.25" }),
      prepare({ tenant: "b", amount: "1.234567890123456789" }),
    ]);
    expect(
      (await executeAtomicSqlApplyPlan(manager, null, inserts)).success,
    ).toBe(false);
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([]);
  });

  it("preserves explicit NULL as an auto-generated INTEGER key request", async () => {
    const { driver, service, prepare } = await fixture();
    await service.executePreparedInsertPlan(
      await prepare({ id: null, amount: "1.25" }),
    );
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([
      { __col_0: 1, __col_1: 1.25 },
    ]);
  });

  it("rolls back a rejected INSERT with zero affected rows", async () => {
    const { driver, service, prepare } = await fixture(`
      CREATE TABLE edits (id INTEGER PRIMARY KEY, amount INTEGER);
      CREATE TRIGGER reject_insert BEFORE INSERT ON edits BEGIN SELECT RAISE(IGNORE); END;
    `);
    await expect(
      service.executePreparedInsertPlan(await prepare({ amount: "9" })),
    ).rejects.toThrow(/Row not found|Mutation affected 0/);
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([]);
  });

  it("fails closed when a verifying insert has no reliable primary key", async () => {
    const { driver, prepare } = await fixture(
      "CREATE TABLE edits (amount DECIMAL)",
    );
    await expect(prepare({ amount: "1.25" })).rejects.toThrow(
      /verification requires a reliable primary key/,
    );
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([]);
  });

  it.each([
    "insert",
    "single apply",
    "batch apply",
  ])("UI %s confirmation never acknowledges an incompatible insert", async (mode) => {
    const { driver, manager, service, prepare } = await fixture();
    const controller = new TableMutationPreviewController({
      connectionId: "b02",
      tableName: "edits",
      connectionManager: manager,
      tableDataService: service,
      notifyWarning: () => {},
    });
    const insert = await prepare({ amount: "1.234567890123456789" });
    const preview =
      mode === "insert"
        ? controller.createInsertPreview("operation", insert)
        : controller.createApplyChangesPreview("operation", {
            apply: null,
            applyResultWhenEmpty: null,
            inserts:
              mode === "batch apply"
                ? [await prepare({ amount: "1.25" }), insert]
                : [insert],
          });
    const result = await controller.confirm(preview.previewToken, "operation");
    expect(result?.payload).toMatchObject({
      success: false,
      error: expect.stringMatching(/verification failed/),
    });
    expect(result?.payload).not.toHaveProperty("insertApplied", true);
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([]);
  });

  it.each([
    "timeout",
    "abort",
  ])("SQLite IPC fences generated INSERT verification interrupted by %s", async (mode) => {
    const directory = await createProjectTempDir("b02-insert-timeout");
    const settings = () =>
      createDriverTimeoutSettingsSnapshot({
        connectionTimeoutSeconds: 1,
        dbOperationTimeoutSeconds: 1,
      });
    const driver = createTimeoutAwareDriver(
      new SQLiteDriver(
        {
          id: "b02",
          name: "B02",
          type: "sqlite",
          filePath: join(directory, "insert.sqlite"),
        },
        settings,
      ),
      settings,
    );
    drivers.push(driver);
    await driver.connect();
    await driver.query(
      "CREATE TABLE edits (id INTEGER PRIMARY KEY, amount INTEGER)",
    );
    const manager = {
      getConnection: () => ({ id: "b02", type: "sqlite" }),
      getDriver: () => driver,
    } as never;
    const service = new TableMutationService(
      manager,
      new TableReadService(manager),
    );
    const plan = await service.prepareInsertRow("b02", "", "main", "edits", {
      amount: "9",
    });
    if (!plan.verification) throw new Error("Expected verification");
    const controller = new AbortController();
    const pending = driver.runTransaction(
      [plan.operation],
      { signal: controller.signal, deadline: Date.now() + 10000 },
      {
        verifications: [
          {
            ...plan.verification,
            sql: "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n WHERE ? IS NOT NULL",
          },
        ],
      },
    );
    const rejected = expect(pending).rejects.toThrow(
      mode === "timeout" ? /timed out.*rolled back/ : /cancelled.*rolled back/,
    );
    if (mode === "abort") {
      await new Promise((resolve) => setTimeout(resolve, 100));
      controller.abort();
    }
    await rejected;
    expect(driver.isConnected()).toBe(false);
    await driver.connect();
    expect((await driver.query("SELECT * FROM edits")).rows).toEqual([]);
  });
});
