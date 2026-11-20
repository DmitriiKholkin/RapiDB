import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import {
  applyChangesTransactional,
  executeAtomicSqlApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";

const drivers: SQLiteCoreDriver[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function setup(ddl: string) {
  const driver = new SQLiteCoreDriver({
    id: "identity",
    name: "Identity",
    type: "sqlite",
    filePath: ":memory:",
  });
  drivers.push(driver);
  await driver.connect();
  await driver.query(ddl);
  const manager = {
    getConnection: () => ({ id: "identity", type: "sqlite" }),
    getDriver: () => driver,
  } as never;
  const read = new TableReadService(manager);
  const service = new TableMutationService(manager, read);
  const columns = await read.getColumns("identity", "", "main", "items");
  return { driver, manager, service, columns };
}

describe("SQLite nullable primary-key row identity", () => {
  it.each([
    [
      "id TEXT PRIMARY KEY, value TEXT",
      { id: null },
      { id: "safe" },
      "(NULL, 'first'), (NULL, 'second'), ('safe', 'third')",
    ],
    [
      "tenant TEXT, id TEXT, value TEXT, PRIMARY KEY (tenant, id)",
      { tenant: "a", id: null },
      { tenant: "a", id: "safe" },
      "('a', NULL, 'first'), ('a', NULL, 'second'), ('a', 'safe', 'third')",
    ],
  ])("rejects ambiguous identities before preview or SQL (%s)", async (definition, invalid, valid, values) => {
    const { driver, manager, service, columns } = await setup(
      `CREATE TABLE items (${definition}); INSERT INTO items VALUES ${values}`,
    );
    const before = (await driver.query("SELECT * FROM items ORDER BY rowid"))
      .rows;
    expect(before).toHaveLength(3);
    const query = vi.spyOn(driver, "query");
    const transaction = vi.spyOn(driver, "runTransaction");
    const preview = vi.spyOn(driver, "materializePreviewSql");
    const updates = [valid, invalid].map((primaryKeys) => ({
      primaryKeys,
      changes: { value: "changed" },
    }));
    expect(() =>
      prepareApplyChangesPlan(
        manager,
        "identity",
        "",
        "main",
        "items",
        updates,
        columns,
      ),
    ).toThrow(/NULL or missing/);
    await expect(
      applyChangesTransactional(
        manager,
        "identity",
        "",
        "main",
        "items",
        updates,
        columns,
      ),
    ).rejects.toThrow(/NULL or missing/);
    await expect(
      service.updateRow("identity", "", "main", "items", invalid, {
        value: "changed",
      }),
    ).rejects.toThrow(/NULL or missing/);
    await expect(
      service.prepareDeleteRowsPlan("identity", "", "main", "items", [
        valid,
        invalid,
      ]),
    ).rejects.toThrow(/NULL or missing/);
    await expect(
      service.deleteRows("identity", "", "main", "items", [valid, invalid]),
    ).rejects.toThrow(/NULL or missing/);
    expect(preview).not.toHaveBeenCalled();
    expect(transaction).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
    expect(
      (await driver.query("SELECT * FROM items ORDER BY rowid")).rows,
    ).toEqual(before);
    await service.updateRow("identity", "", "main", "items", valid, {
      value: "changed",
    });
    await service.deleteRows("identity", "", "main", "items", [valid]);
    expect(
      (await driver.query("SELECT * FROM items ORDER BY rowid")).rows,
    ).toEqual(before.slice(0, 2));
  });

  it.each([
    ["id INTEGER PRIMARY KEY, value TEXT", "", 1],
    ["id TEXT PRIMARY KEY NOT NULL, value TEXT", "", "safe"],
    ["id TEXT PRIMARY KEY, value TEXT", " STRICT", "safe"],
    ["id TEXT PRIMARY KEY, value TEXT", " WITHOUT ROWID", "safe"],
  ])("updates and deletes valid rows (%s%s)", async (definition, suffix, id) => {
    const { driver, manager, service, columns } = await setup(
      `CREATE TABLE items (${definition})${suffix}`,
    );
    await service.insertRow("identity", "", "main", "items", {
      id,
      value: "before",
    });
    const result = await applyChangesTransactional(
      manager,
      "identity",
      "",
      "main",
      "items",
      [{ primaryKeys: { id }, changes: { value: "after" } }],
      columns,
    );
    expect(result.success).toBe(true);
    expect((await driver.query("SELECT value FROM items")).rows).toEqual([
      { __col_0: "after" },
    ]);
    await service.deleteRows("identity", "", "main", "items", [{ id }]);
    expect((await driver.query("SELECT * FROM items")).rows).toEqual([]);
  });

  it("allows database-controlled PK edits and defaults, then blocks the newly unaddressable row", async () => {
    const { driver, service } = await setup(
      "CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT DEFAULT 'default'); INSERT INTO items VALUES ('safe', 'before')",
    );
    await service.updateRow(
      "identity",
      "",
      "main",
      "items",
      { id: "safe" },
      { id: null },
    );
    await service.insertRow("identity", "", "main", "items", {});
    expect((await driver.query("SELECT * FROM items")).rows).toEqual([
      { __col_0: null, __col_1: "before" },
      { __col_0: null, __col_1: "default" },
    ]);
    await expect(
      service.updateRow(
        "identity",
        "",
        "main",
        "items",
        { id: null },
        { id: "restored" },
      ),
    ).rejects.toThrow(/NULL or missing/);
  });

  it.each([
    [
      "id TEXT PRIMARY KEY, value TEXT",
      " WITHOUT ROWID",
      "'safe'",
      "WITHOUT ROWID",
    ],
    ["id TEXT PRIMARY KEY, value TEXT", " STRICT", "'safe'", "cannot be set"],
    ["id INTEGER PRIMARY KEY, value TEXT", "", "1", "cannot be set"],
    [
      "id TEXT PRIMARY KEY, value TEXT, ROWID TEXT, _RowID_ TEXT, OID TEXT",
      "",
      "'safe'",
      "all rowid aliases are shadowed",
    ],
  ])("refuses unsafe NULL key writes before executing any DML (%s%s)", async (definition, suffix, id, reason) => {
    const { driver, manager, columns } = await setup(`
      CREATE TABLE items (${definition})${suffix};
      INSERT INTO items (id, value) VALUES (${id}, 'before');
      CREATE TRIGGER reject_update BEFORE UPDATE ON items
      BEGIN SELECT RAISE(ABORT, 'UPDATE must not execute'); END;
    `);
    const result = await applyChangesTransactional(
      manager,
      "identity",
      "",
      "main",
      "items",
      [
        {
          primaryKeys: { id: id === "1" ? 1 : "safe" },
          changes: { id: null, value: "after" },
        },
      ],
      columns,
    );
    expect(result).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
      error: expect.stringMatching(
        new RegExp(`SQLite primary key "id".*${reason}`),
      ),
    });
    expect((await driver.query("SELECT value FROM items")).rows).toEqual([
      { __col_0: "before" },
    ]);
  });

  it.each([
    "during",
    "after",
  ] as const)("rolls back a mixed Apply if a trigger replaces the captured row %s the NULL key update", async (timing) => {
    const { driver, manager, service, columns } = await setup(`
      CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT);
      INSERT INTO items VALUES (NULL, 'unrelated'), ('safe', 'before'), ('later', 'before');
      CREATE TRIGGER replace_row AFTER UPDATE ON items
      WHEN ${timing === "during" ? "NEW.id IS NULL" : "NEW.id = 'later'"}
      BEGIN
        DELETE FROM items WHERE rowid = 2;
        INSERT INTO items (rowid, id, value) VALUES (2, NULL, 'after');
      END;
    `);
    const plan = prepareApplyChangesPlan(
      manager,
      "identity",
      "",
      "main",
      "items",
      [
        { primaryKeys: { id: "safe" }, changes: { id: null, value: "after" } },
        { primaryKeys: { id: "later" }, changes: { value: "after" } },
      ],
      columns,
    );
    if (!plan.executable) throw new Error("Expected an Apply plan");
    const insert = await service.prepareInsertRow(
      "identity",
      "",
      "main",
      "items",
      { id: "draft", value: "draft" },
    );
    const result = await executeAtomicSqlApplyPlan(manager, plan.plan, [
      insert,
    ]);
    expect(result).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
      failedRows: [0],
      error: expect.stringContaining("A trigger or cascading action"),
    });
    expect(
      (await driver.query("SELECT id, value FROM items ORDER BY rowid")).rows,
    ).toEqual([
      { __col_0: null, __col_1: "unrelated" },
      { __col_0: "safe", __col_1: "before" },
      { __col_0: "later", __col_1: "before" },
    ]);
  });

  it("commits a mixed Apply with an INTEGER PRIMARY KEY DESC that is not a rowid alias", async () => {
    const { driver, manager, service, columns } = await setup(`
      CREATE TABLE items (id INTEGER PRIMARY KEY DESC, value TEXT);
      INSERT INTO items VALUES (NULL, 'unrelated'), (4, 'before');
    `);
    const plan = prepareApplyChangesPlan(
      manager,
      "identity",
      "",
      "main",
      "items",
      [{ primaryKeys: { id: 4 }, changes: { id: null, value: "after" } }],
      columns,
    );
    if (!plan.executable) throw new Error("Expected an Apply plan");
    const insert = await service.prepareInsertRow(
      "identity",
      "",
      "main",
      "items",
      { id: 7, value: "draft" },
    );
    expect(
      await executeAtomicSqlApplyPlan(manager, plan.plan, [insert]),
    ).toMatchObject({
      success: true,
      insertApplied: true,
      rowOutcomes: [{ status: "applied" }],
    });
    expect(
      (await driver.query("SELECT id, value FROM items ORDER BY rowid")).rows,
    ).toEqual([
      { __col_0: null, __col_1: "unrelated" },
      { __col_0: null, __col_1: "after" },
      { __col_0: 7, __col_1: "draft" },
    ]);
  });
});
