import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";

const location = ["conflicts", "", "main", "items"] as const;
const drivers: SQLiteDriver[] = [];

afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function fixture() {
  const driver = new SQLiteDriver({
    id: location[0],
    name: "Conflict policy",
    type: "sqlite",
    filePath: ":memory:",
  });
  drivers.push(driver);
  await driver.connect();
  await driver.query(`
    CREATE TABLE items (
      id TEXT PRIMARY KEY ON CONFLICT REPLACE DEFAULT 'new',
      code DATE UNIQUE ON CONFLICT REPLACE DEFAULT '2024-01-01',
      amount INTEGER DEFAULT 0
    );
    INSERT INTO items VALUES
      ('left', '2024-01-01', 10), ('right', '2024-01-02', 20), ('other', '2024-01-03', 30);
  `);
  const manager = {
    getConnection: () => ({ id: location[0], type: "sqlite" }),
    getDriver: () => driver,
  } as never;
  const reads = new TableReadService(manager);
  const mutations = new TableMutationService(manager, reads);
  const page = () => reads.getPage(...location, 1, 25, []);
  const initial = await page();
  const key = (id: string) => {
    const index = initial.rows.findIndex((row) => row.id === id);
    const identity = initial.sqliteRowIdentities?.[index];
    if (!identity) throw new Error(`Missing SQLite identity for ${id}`);
    return identity;
  };
  const expectUnchanged = async () => {
    expect((await page()).rows).toEqual(initial.rows);
  };
  return { driver, manager, mutations, initial, key, expectUnchanged };
}

describe("SQLite Table Viewer conflict policy", () => {
  it.each([
    {
      path: "ordinary",
      values: { code: "2024-01-01" },
    },
    {
      path: "identity-capturing",
      values: { id: "new", code: "2024-01-01", amount: "30" },
    },
    {
      path: "default-only",
      values: {},
    },
  ])("rejects a conflicting $path INSERT without replacing the existing row", async ({
    path,
    values,
  }) => {
    const { mutations, expectUnchanged } = await fixture();
    const plan = await mutations.prepareInsertRow(...location, values);
    await expect(mutations.insertRow(...location, values)).rejects.toThrow(
      /UNIQUE constraint failed: items\.code/,
    );
    await expectUnchanged();
    expect(plan.previewStatements).toEqual([
      expect.stringMatching(/^INSERT OR ABORT INTO /),
    ]);
    expect(Boolean(plan.operation.captureIdentity)).toBe(
      path === "identity-capturing",
    );
    if (path === "default-only")
      expect(plan.previewStatements[0]).toMatch(/ DEFAULT VALUES$/);
  });

  it("rejects a conflicting single-row UPDATE without replacing the other row", async () => {
    const { mutations, key, expectUnchanged } = await fixture();
    await expect(
      mutations.updateRow(...location, key("right"), { code: "2024-01-01" }),
    ).rejects.toThrow(/UNIQUE constraint failed: items\.code/);
    await expectUnchanged();
  });

  it("rejects an identity-capturing UPDATE on a primary key conflict and rolls back the batch", async () => {
    const { manager, initial, key, expectUnchanged } = await fixture();
    const prepared = prepareApplyChangesPlan(
      manager,
      ...location,
      [
        { primaryKeys: key("other"), changes: { amount: "31" } },
        { primaryKeys: key("right"), changes: { id: "left" } },
      ],
      initial.columns,
    );
    if (!prepared.executable) throw new Error("Expected an update plan");
    expect(
      await executePreparedApplyPlan(manager, prepared.plan),
    ).toMatchObject({
      success: false,
      error: expect.stringMatching(/UNIQUE constraint failed: items\.id/),
    });
    await expectUnchanged();
    expect(prepared.plan.operations[1].captureIdentity).toBeDefined();
    expect(prepared.plan.previewStatements).toEqual([
      expect.stringMatching(/^UPDATE OR ABORT /),
      expect.stringMatching(/^UPDATE OR ABORT .* RETURNING /),
    ]);
  });

  it("reports no changes when an UPDATE trigger rolls back an earlier grid edit", async () => {
    const { driver, manager, initial, key, expectUnchanged } = await fixture();
    await driver.query(`
      CREATE TRIGGER reject_edit BEFORE UPDATE ON items
      WHEN OLD.id = 'right'
      BEGIN
        SELECT RAISE(ROLLBACK, 'edit rejected by trigger');
      END;
    `);
    const prepared = prepareApplyChangesPlan(
      manager,
      ...location,
      [
        { primaryKeys: key("other"), changes: { amount: "31" } },
        { primaryKeys: key("right"), changes: { id: "updated" } },
      ],
      initial.columns,
    );
    if (!prepared.executable) throw new Error("Expected an update plan");
    expect(prepared.plan.operations[1].sql).toMatch(
      /^UPDATE OR ABORT .* RETURNING /,
    );

    expect(
      await executePreparedApplyPlan(manager, prepared.plan),
    ).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
      error: expect.stringMatching(
        /no changes were made.*rejected by trigger/i,
      ),
    });
    await expectUnchanged();
  });

  it("reports no changes when a DELETE trigger hits a schema ROLLBACK conflict", async () => {
    const { driver, mutations, key, expectUnchanged } = await fixture();
    await driver.query(`
      CREATE TABLE guarded (id TEXT UNIQUE ON CONFLICT ROLLBACK);
      INSERT INTO guarded VALUES ('right');
      CREATE TRIGGER guard_delete BEFORE DELETE ON items
      BEGIN
        INSERT INTO guarded VALUES (OLD.id);
      END;
    `);

    await expect(
      mutations.deleteRows(...location, [key("other"), key("right")]),
    ).rejects.toMatchObject({
      deleteResult: {
        success: false,
        affectedRows: 0,
        changesPossible: false,
        outcomeUnknown: false,
        error: expect.stringMatching(/UNIQUE constraint failed: guarded\.id/),
      },
    });
    await expectUnchanged();
    expect((await driver.query("SELECT id FROM guarded")).rows).toEqual([
      { __col_0: "right" },
    ]);
  });

  it("keeps schema and explicit REPLACE behavior for user-authored SQL", async () => {
    const { driver } = await fixture();
    await driver.query(
      "INSERT INTO items (id, code, amount) VALUES ('new', '2024-01-01', 30)",
    );
    const rows = () => driver.query("SELECT id, amount FROM items ORDER BY id");
    expect((await rows()).rows).toEqual([
      { __col_0: "new", __col_1: 30 },
      { __col_0: "other", __col_1: 30 },
      { __col_0: "right", __col_1: 20 },
    ]);
    await driver.query(
      "UPDATE OR REPLACE items SET id = 'new' WHERE id = 'right'",
    );
    expect((await rows()).rows).toEqual([
      { __col_0: "new", __col_1: 20 },
      { __col_0: "other", __col_1: 30 },
    ]);
  });
});
