import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import {
  applyChangesTransactional,
  TableDataService,
} from "../../../src/extension/tableDataService";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { rowsFromQuery } from "../../support/liveDbHarness";

describe("stage 4 C01/C02/B01 PostgreSQL persisted edits (live)", () => {
  let config: ConnectionConfig;
  let driver: PostgresDriver;
  let service: TableDataService;
  const schema = `stage4_${process.pid}_${Date.now().toString(36)}`;
  const table = `"${schema}"."edits"`;
  const manager = { getDriver: () => driver, getConnection: () => config };
  const database = () => config.database ?? "";
  const columns = () =>
    service.getColumns(config.id, database(), schema, "edits");
  const read = async (id: number) =>
    rowsFromQuery(
      await driver.query(`SELECT * FROM ${table} WHERE id=$1`, [id]),
    )[0];

  beforeAll(async () => {
    config = await resolveConnectionSeed("postgres");
    driver = new PostgresDriver(config);
    await driver.connect();
    await driver.query(`CREATE SCHEMA "${schema}"`);
    await driver.query(`CREATE TABLE ${table} (
      id integer PRIMARY KEY, u uuid, equal_scale numeric(2,2),
      fractional numeric(2,4), ordinary numeric(4,2), implicit_scale numeric(2),
      big numeric(30,10), unlimited numeric, negative_scale numeric(2,-3), a text[]
    )`);
    await driver.query(
      `INSERT INTO ${table}(id,ordinary) SELECT i,0 FROM generate_series(1,20) i`,
    );
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
    "01890f47-6a42-7cc3-98c4-dc0c0c07398f",
    "00000000-0000-0000-0000-000000000000",
    "ffffffff-ffff-ffff-ffff-ffffffffffff",
  ])("saves and verifies UUID %s together with numeric(2,2) 0.12", async (uuid) => {
    const result = await applyChangesTransactional(
      manager as never,
      config.id,
      database(),
      schema,
      "edits",
      [
        {
          primaryKeys: { id: 1 },
          changes: { u: uuid.toUpperCase(), equal_scale: "0.12" },
        },
      ],
      await columns(),
    );
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ status: "applied", success: true }],
    });
    expect(result.warning).toBeUndefined();
    expect(await read(1)).toMatchObject({ u: uuid, equal_scale: "0.12" });
  });

  it.each([
    ["equal_scale", "0.12", true],
    ["equal_scale", "-0.00", true],
    ["equal_scale", "0.001", false],
    ["equal_scale", "1", false],
    ["fractional", "0.0012", true],
    ["fractional", "-0.0099", true],
    ["fractional", "0", true],
    ["fractional", "0.0123", false],
    ["big", "12345678901234567890.1234567890", true],
    ["big", "123456789012345678901.1234567890", false],
  ])("cross-checks actual metadata/server precision: %s %s", async (name, value, ok) => {
    const meta = (await columns()).find((column) => column.name === name);
    if (!meta) throw new Error("Missing numeric metadata");
    expect(driver.checkPersistedEdit(meta, value)?.ok).toBe(ok);
    if (ok) {
      const [row] = rowsFromQuery(
        await driver.query(`SELECT $1::${meta.nativeType} AS value`, [value]),
      );
      expect(
        driver.checkPersistedEdit(meta, value, { persistedValue: row.value })
          ?.ok,
      ).toBe(true);
    } else if (name === "equal_scale" && value === "0.001") {
      // Native SQL rounds; the table-edit policy intentionally rejects loss.
      expect(
        rowsFromQuery(
          await driver.query(`SELECT $1::${meta.nativeType} AS value`, [value]),
        )[0].value,
      ).toBe("0.00");
    } else {
      await expect(
        driver.query(`SELECT $1::${meta.nativeType}`, [value]),
      ).rejects.toThrow("numeric field overflow");
    }
  });

  it("uses format_type's implicit scale and preserves native numeric fallbacks", async () => {
    const byName = new Map(
      (await columns()).map((column) => [column.name, column]),
    );
    const implicit = byName.get("implicit_scale");
    if (!implicit) throw new Error("Missing numeric(p) metadata");
    expect(implicit.nativeType).toBe("numeric(2,0)");
    expect(driver.checkPersistedEdit(implicit, "12.1")?.ok).toBe(false);
    expect(
      rowsFromQuery(await driver.query("SELECT 12.1::numeric(2) AS value"))[0]
        .value,
    ).toBe("12");
    const negative = byName.get("negative_scale");
    const unlimited = byName.get("unlimited");
    if (!negative || !unlimited) throw new Error("Missing numeric metadata");
    expect(driver.checkPersistedEdit(negative, "12001")).toBeNull();
    for (const value of ["NaN", "Infinity", "-Infinity"]) {
      expect(driver.checkPersistedEdit(unlimited, value)).toBeNull();
      expect(
        rowsFromQuery(
          await driver.query("SELECT $1::numeric AS value", [value]),
        )[0].value,
      ).toBe(value);
    }
  });

  it("rejects actual numeric(4,2) 1.239 and performs zero batch writes", async () => {
    const transaction = vi.spyOn(driver, "runTransaction");
    const preview = vi.spyOn(driver, "materializePreviewColumnSql");
    try {
      const result = await applyChangesTransactional(
        manager as never,
        config.id,
        database(),
        schema,
        "edits",
        [
          { primaryKeys: { id: 2 }, changes: { ordinary: "2.34" } },
          { primaryKeys: { id: 3 }, changes: { ordinary: "1.239" } },
        ],
        await columns(),
      );
      expect(result).toMatchObject({
        success: false,
        failedRows: [1],
        rowOutcomes: [
          { rowIndex: 0, status: "skipped", success: false },
          {
            rowIndex: 1,
            status: "prevalidation_failed",
            success: false,
            columns: ["ordinary"],
            message: expect.stringContaining("2 fractional digits"),
          },
        ],
      });
      expect(transaction).not.toHaveBeenCalled();
      expect(preview).not.toHaveBeenCalled();
      expect(await read(2)).toMatchObject({ ordinary: "0.00" });
      expect(await read(3)).toMatchObject({ ordinary: "0.00" });
      await driver.query(`UPDATE ${table} SET ordinary=1.239 WHERE id=4`);
      expect(await read(4)).toMatchObject({ ordinary: "1.24" });
    } finally {
      transaction.mockRestore();
      preview.mockRestore();
    }
  });

  it("rejects public insert/update before writes with structured column errors", async () => {
    for (const run of [
      () =>
        service.updateRow(
          config.id,
          database(),
          schema,
          "edits",
          { id: 5 },
          { ordinary: "1.239" },
        ),
      () =>
        service.insertRow(config.id, database(), schema, "edits", {
          id: 99,
          ordinary: "1.239",
        }),
    ])
      await expect(run()).rejects.toMatchObject({
        status: "prevalidation_failed",
        columns: ["ordinary"],
      });
    expect(await read(5)).toMatchObject({ ordinary: "0.00" });
    expect(await read(99)).toBeUndefined();
  });

  it("keeps legacy native bounded array edits valid under prevalidation", async () => {
    const result = await applyChangesTransactional(
      manager as never,
      config.id,
      database(),
      schema,
      "edits",
      [
        {
          primaryKeys: { id: 6 },
          changes: { a: '[-1:1]={"01",NULL,"NULL"}' },
        },
      ],
      await columns(),
    );
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ status: "applied", success: true }],
    });
    expect(result.warning).toBeUndefined();
    const [row] = rowsFromQuery(
      await driver.query(
        `SELECT a,array_dims(a) AS dims FROM ${table} WHERE id=6`,
      ),
    );
    expect(row).toEqual({ a: ["01", null, "NULL"], dims: "[-1:1]" });
  });
});
