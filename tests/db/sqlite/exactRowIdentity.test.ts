import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import {
  applyChangesTransactional,
  executeAtomicSqlApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";

const location = ["identity", "", "main", "items"] as const;
const drivers: SQLiteCoreDriver[] = [];

afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function setup(sql: string, mode: "native" | "worker" = "worker") {
  const Driver = mode === "worker" ? SQLiteDriver : SQLiteCoreDriver;
  const driver = new Driver({
    id: location[0],
    name: "Identity",
    type: "sqlite",
    filePath: ":memory:",
  });
  drivers.push(driver);
  await driver.connect();
  await driver.query(sql);
  const manager = {
    getConnection: () => ({ id: location[0], type: "sqlite" }),
    getDriver: () => driver,
  } as never;
  const reads = new TableReadService(manager);
  const mutations = new TableMutationService(manager, reads);
  const page = () => reads.getPage(...location, 1, 25, []);
  return { driver, manager, mutations, page };
}

function wireIdentity(
  page: Awaited<ReturnType<TableReadService["getPage"]>>,
  label: string,
) {
  const index = page.rows.findIndex((row) => row.label === label);
  const identity = page.sqliteRowIdentities?.[index];
  if (!identity) throw new Error(`Missing primary key for ${label}`);
  return JSON.parse(JSON.stringify(identity)) as typeof identity;
}

describe("SQLite stored primary keys", () => {
  it.each([
    "native",
    "worker",
  ] as const)("reads, updates, verifies and deletes text and numeric BOOLEAN keys independently (%s)", async (mode) => {
    const { manager, mutations, page } = await setup(
      `CREATE TABLE items (id BOOLEAN PRIMARY KEY, label TEXT, amount INTEGER);
         INSERT INTO items VALUES (1, 'numeric', 3), ('true', 'text', 4), (0, 'zero', 5)`,
      mode,
    );
    const initial = await page();
    expect(initial.rows).toEqual([
      { id: false, label: "zero", amount: 5 },
      { id: true, label: "numeric", amount: 3 },
      { id: "true", label: "text", amount: 4 },
    ]);
    const textKey = wireIdentity(initial, "text");
    const numericKey = wireIdentity(initial, "numeric");
    expect(textKey).not.toEqual(numericKey);
    const result = await applyChangesTransactional(
      manager,
      ...location,
      [
        {
          primaryKeys: textKey,
          changes: { amount: "8" },
          originalValues: { amount: 4 },
        },
      ],
      initial.columns,
    );
    expect(result).toMatchObject({ success: true });
    expect((await page()).rows.map((row) => row.amount)).toEqual([5, 3, 8]);
    await mutations.deleteRows(...location, [textKey]);
    expect((await page()).rows.map((row) => row.label)).toEqual([
      "zero",
      "numeric",
    ]);
    await mutations.deleteRows(...location, [numericKey]);
    expect((await page()).rows.map((row) => row.label)).toEqual(["zero"]);
  });

  it("keeps composite binary, text, REAL, and large INTEGER identities distinct through the worker", async () => {
    const { mutations, page, driver } = await setup(
      `CREATE TABLE items (tenant TEXT, id BLOB, label TEXT, PRIMARY KEY (tenant, id)) WITHOUT ROWID;
       INSERT INTO items VALUES
         ('team', X'', 'empty blob'), ('team', '0x', 'blob display text'),
         ('team', X'AB', 'blob'), ('team', '0xab', 'hex text'),
         ('team', 9007199254740993, 'integer'), ('team', '9007199254740993', 'integer display text'),
         ('team', 9007199254740992.0, 'real'), ('team', 1.0000000000000002, 'precise real')`,
    );
    const initial = await page();
    const getKey = (label: string) => wireIdentity(initial, label);
    for (const [first, second] of [
      ["empty blob", "blob display text"],
      ["blob", "hex text"],
      ["integer", "integer display text"],
    ]) {
      expect(initial.rows.find((row) => row.label === first)?.id).toBe(
        initial.rows.find((row) => row.label === second)?.id,
      );
      expect(getKey(first)).not.toEqual(getKey(second));
    }
    await mutations.updateRow(...location, getKey("integer"), {
      label: "exact integer",
    });
    await mutations.updateRow(...location, getKey("precise real"), {
      label: "exact real",
    });
    await mutations.deleteRows(...location, [
      getKey("empty blob"),
      getKey("hex text"),
      getKey("integer display text"),
    ]);
    const remaining = await driver.query(
      "SELECT typeof(id), label FROM items ORDER BY label",
    );
    expect(remaining.rows.map(Object.values)).toEqual([
      ["blob", "blob"],
      ["text", "blob display text"],
      ["integer", "exact integer"],
      ["real", "exact real"],
      ["real", "real"],
    ]);
  });

  it("verifies a changed key by its stored class in an atomic insert and update", async () => {
    const { manager, page, mutations, driver } = await setup(
      `CREATE TABLE items (id BOOLEAN PRIMARY KEY, label TEXT, amount INTEGER);
       INSERT INTO items VALUES ('true', 'text', 1), (0, 'zero', 2)`,
    );
    const initial = await page();
    const prepared = prepareApplyChangesPlan(
      manager,
      ...location,
      [
        {
          primaryKeys: wireIdentity(initial, "text"),
          changes: { id: true, amount: "7" },
          originalValues: { id: "true", amount: 1 },
        },
      ],
      initial.columns,
    );
    if (!prepared.executable) throw new Error("Expected an update");
    const insert = await mutations.prepareInsertRow(...location, {
      id: null,
      label: "inserted",
      amount: "9",
    });
    expect(insert.operation.captureIdentity).toBeDefined();
    expect(
      await executeAtomicSqlApplyPlan(manager, prepared.plan, [insert]),
    ).toMatchObject({ success: true, insertApplied: true });
    const result = await driver.query(
      "SELECT typeof(id), label, amount FROM items ORDER BY label",
    );
    expect(result.rows.map(Object.values)).toEqual([
      ["null", "inserted", 9],
      ["integer", "text", 7],
      ["integer", "zero", 2],
    ]);
  });

  it("retains exact text bytes and guards against a stale collated key", async () => {
    const { page, mutations } = await setup(
      `PRAGMA encoding = 'UTF-16le';
       CREATE TABLE items (id TEXT COLLATE NOCASE PRIMARY KEY, label TEXT);
       INSERT INTO items VALUES ('Alpha', 'original'), ('é', 'non-ascii')`,
    );
    const initial = await page();
    await mutations.updateRow(...location, wireIdentity(initial, "non-ascii"), {
      label: "updated",
    });
    await mutations.updateRow(...location, wireIdentity(initial, "original"), {
      id: "alpha",
    });
    await expect(
      mutations.deleteRows(...location, [wireIdentity(initial, "original")]),
    ).rejects.toThrow();
    expect((await page()).rows).toEqual([
      { id: "alpha", label: "original" },
      { id: "é", label: "updated" },
    ]);
  });
});
