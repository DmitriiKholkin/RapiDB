import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import {
  applyChangesTransactional,
  TableDataService,
} from "../../../src/extension/tableDataService";

const drivers: SQLiteDriver[] = [];
afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function setup(strict = false) {
  const config = {
    id: "stage4-native",
    name: "Native",
    type: "sqlite" as const,
    filePath: ":memory:",
  };
  const driver = new SQLiteDriver(config);
  drivers.push(driver);
  await driver.connect();
  await driver.query(
    `CREATE TABLE edits (id INTEGER PRIMARY KEY, amount INTEGER, bytes BLOB)${strict ? " STRICT" : ""}; INSERT INTO edits VALUES(1,0,NULL)`,
  );
  const manager = { getDriver: () => driver, getConnection: () => config };
  const service = new TableDataService(manager as never);
  const columns = await service.getColumns(config.id, "", "main", "edits");
  return { config, driver, manager, service, columns };
}

describe("stage 4 review real SQLite affinity and binary persisted edits", () => {
  it.each([
    ["amount", "1.25", "real", 1.25],
    ["amount", "-0.125", "real", -0.125],
    ["bytes", "[222,173]", "blob", "DEAD"],
    ["bytes", '{"0":222,"1":173}', "blob", "DEAD"],
  ])("stores %s %s losslessly through public update/insert and atomic apply", async (name, value, storage, expected) => {
    const s = await setup();
    const meta = s.columns.find((column) => column.name === name);
    if (!meta) throw new Error("Missing actual metadata");
    expect(meta.category).toBe(name === "amount" ? "integer" : "binary");
    await s.service.updateRow(
      s.config.id,
      "",
      "main",
      "edits",
      { id: 1 },
      { [name]: value },
    );
    await s.service.insertRow(s.config.id, "", "main", "edits", {
      id: 2,
      [name]: value,
    });
    const expr = name === "bytes" ? "hex(bytes)" : "amount";
    const rows = (
      await s.driver.query(
        `SELECT ${expr},typeof(${name}) FROM edits ORDER BY id`,
      )
    ).rows;
    expect(rows).toEqual([
      { __col_0: expected, __col_1: storage },
      { __col_0: expected, __col_1: storage },
    ]);
    const result = await applyChangesTransactional(
      s.manager as never,
      s.config.id,
      "",
      "main",
      "edits",
      [{ primaryKeys: { id: 1 }, changes: { [name]: value } }],
      s.columns,
    );
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ success: true, status: "applied" }],
    });
    expect(result.warning).toBeUndefined();
  });

  it("leaves STRICT integer enforcement to the actual SQLite table", async () => {
    const s = await setup(true);
    const meta = s.columns.find((column) => column.name === "amount");
    if (!meta) throw new Error("Missing actual metadata");
    expect(s.driver.checkPersistedEdit(meta, "1.25")?.ok).not.toBe(false);
    await expect(
      s.service.updateRow(
        s.config.id,
        "",
        "main",
        "edits",
        { id: 1 },
        { amount: "1.25" },
      ),
    ).rejects.toThrow(/REAL value.*INTEGER column/);
    await expect(
      s.service.insertRow(s.config.id, "", "main", "edits", {
        id: 2,
        amount: "1.25",
      }),
    ).rejects.toThrow(/REAL value.*INTEGER column/);
    expect((await s.driver.query("SELECT amount FROM edits")).rows).toEqual([
      { __col_0: 0 },
    ]);
  });

  it.each([
    "[256]",
    "[-1]",
    "[1.5]",
    "[true]",
  ])("does not relax existing byte-array range validation for %s", async (input) => {
    const s = await setup();
    const meta = s.columns.find((column) => column.name === "bytes");
    if (!meta) throw new Error("Missing actual metadata");
    expect(s.driver.coerceInputValue(input, meta)).toBe(input);
    await expect(
      s.service.insertRow(s.config.id, "", "main", "edits", {
        id: 2,
        bytes: input,
      }),
    ).rejects.toMatchObject({
      status: "prevalidation_failed",
      columns: ["bytes"],
    });
  });
});
