import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import {
  applyChangesTransactional,
  TableDataService,
} from "../../../src/extension/tableDataService";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { rowsFromQuery } from "../../support/liveDbHarness";

describe("stage 4 review PostgreSQL native persisted-edit compatibility", () => {
  let config: ConnectionConfig;
  let driver: PostgresDriver;
  let service: TableDataService;
  let id = 0;
  const schema = `stage4_native_${process.pid}_${Date.now().toString(36)}`;
  const table = `"${schema}".edits`;
  const manager = { getDriver: () => driver, getConnection: () => config };
  const database = () => config.database ?? "";

  beforeAll(async () => {
    config = await resolveConnectionSeed("postgres");
    driver = new PostgresDriver(config);
    await driver.connect();
    await driver.query(`CREATE SCHEMA "${schema}"`);
    await driver.query(`CREATE TABLE ${table} (
      id integer PRIMARY KEY, u uuid, b boolean, a boolean[], f double precision, r real
    )`);
    service = new TableDataService(manager as never);
  });
  afterAll(async () => {
    if (!driver) return;
    try {
      await driver.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    } finally {
      await driver.disconnect();
    }
  });

  it.each([
    ["a", "{1,0}", [true, false]],
    ["a", "{TRUE,FALSE}", [true, false]],
    ["a", "{YeS,oF}", [true, false]],
    [
      "a",
      '[0:1][-1:0]={{ON,OFF},{NULL," yes "}}',
      [
        [true, false],
        [null, true],
      ],
    ],
    ["a", '["on","off",null]', [true, false, null]],
    ["a", [1, 0, "YES", "no", null], [true, false, true, false, null]],
    [
      "u",
      "01890f476a427cc398c4dc0c0c07398f",
      "01890f47-6a42-7cc3-98c4-dc0c0c07398f",
    ],
    [
      "u",
      "{01890F47-6A42-7CC3-98C4-DC0C0C07398F}",
      "01890f47-6a42-7cc3-98c4-dc0c0c07398f",
    ],
    [
      "u",
      "0189-0f47-6a42-7cc3-98c4-dc0c-0c07-398f",
      "01890f47-6a42-7cc3-98c4-dc0c0c07398f",
    ],
    ["b", "yes", true],
    ["b", "on", true],
    ["b", " YES ", true],
    ["b", "TrU", true],
    ["b", "y", true],
    ["b", "1", true],
    ["b", "no", false],
    ["b", "off", false],
    ["b", "OF", false],
    ["b", "fa", false],
    ["b", "n", false],
    ["b", "0", false],
    ["f", "Infinity", Infinity],
    ["f", "+INF", Infinity],
    ["f", "inf", Infinity],
    ["f", "-Infinity", -Infinity],
    ["f", "-iNf", -Infinity],
    ["f", "NaN", NaN],
    ["f", "nan", NaN],
    ["f", "+NaN", NaN],
    ["f", "-NaN", NaN],
    ["f", "nan()", NaN],
    ["f", "nan(123)", NaN],
    ["f", "-nan(foo)", NaN],
    ["r", "+NaN", NaN],
    ["r", "-NaN", NaN],
    ["r", "nan()", NaN],
    ["r", "nan(123)", NaN],
    ["r", "-nan(foo)", NaN],
    ["f", Infinity, Infinity],
    ["f", -Infinity, -Infinity],
    ["f", NaN, NaN],
  ])("native %s input %s survives public update/insert and verified atomic apply", async (name, input, expected) => {
    const columns = await service.getColumns(
      config.id,
      database(),
      schema,
      "edits",
    );
    const meta = columns.find((column) => column.name === name);
    if (!meta) throw new Error("Missing actual metadata");
    // Read-only server cast is the independent acceptance/value control.
    const controlSql =
      name === "a" && typeof input === "string" && input.startsWith('["')
        ? "SELECT ARRAY(SELECT value::boolean FROM jsonb_array_elements_text($1::jsonb) AS e(value)) AS value"
        : `SELECT $1::${meta.nativeType} AS value`;
    const [control] = rowsFromQuery(await driver.query(controlSql, [input]));
    expect(control.value).toEqual(expected);
    const updateId = ++id;
    const insertId = ++id;
    await driver.query(`INSERT INTO ${table}(id) VALUES ($1)`, [updateId]);
    await service.updateRow(
      config.id,
      database(),
      schema,
      "edits",
      { id: updateId },
      { [name]: input },
    );
    await service.insertRow(config.id, database(), schema, "edits", {
      id: insertId,
      [name]: input,
    });
    const persisted = rowsFromQuery(
      await driver.query(
        `SELECT ${name} AS value FROM ${table} WHERE id IN ($1,$2) ORDER BY id`,
        [updateId, insertId],
      ),
    );
    expect(persisted.map((row) => row.value)).toEqual([expected, expected]);
    expect(driver.checkPersistedEdit(meta, input)).toEqual({
      ok: true,
      shouldVerify: true,
    });
    expect(
      driver.checkPersistedEdit(meta, input, { persistedValue: control.value }),
    ).toEqual({ ok: true, shouldVerify: true });
    const result = await applyChangesTransactional(
      manager as never,
      config.id,
      database(),
      schema,
      "edits",
      [{ primaryKeys: { id: updateId }, changes: { [name]: input } }],
      columns,
    );
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ success: true, status: "applied" }],
    });
    expect(result.warning).toBeUndefined();
  });

  it.each([
    ["b", "o"],
    ["b", "truth"],
    ["b", "2"],
    ["u", "01890f476a427cc398c4dc0c0c07398g"],
    ["a", "{truth,0}"],
    ["f", "infinite"],
  ])("still rejects invalid native %s input %s", async (name, input) => {
    const columns = await service.getColumns(
      config.id,
      database(),
      schema,
      "edits",
    );
    const meta = columns.find((column) => column.name === name);
    if (!meta) throw new Error("Missing actual metadata");
    await expect(
      driver.query(`SELECT $1::${meta.nativeType}`, [input]),
    ).rejects.toThrow();
    await expect(
      service.insertRow(config.id, database(), schema, "edits", {
        id: ++id,
        [name]: input,
      }),
    ).rejects.toThrow();
  });

  it("detects special-float mismatches instead of treating all nonfinite values alike", async () => {
    const meta = (
      await service.getColumns(config.id, database(), schema, "edits")
    ).find((column) => column.name === "f");
    if (!meta) throw new Error("Missing actual metadata");
    for (const [input, actual] of [
      ["Inf", -Infinity],
      ["NaN", Infinity],
      ["-Inf", 0],
    ] as const) {
      expect(
        driver.checkPersistedEdit(meta, input, { persistedValue: actual })?.ok,
      ).toBe(false);
    }
  });
});
