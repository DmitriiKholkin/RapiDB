import { expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import { buildUpdateRowSql } from "../../../src/extension/table/updateSql";
import {
  applyChangesTransactional,
  TableDataService,
} from "../../../src/extension/tableDataService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { rowsFromQuery } from "../../support/liveDbHarness";

it("targets the exact PostgreSQL interval key across different session styles", async () => {
  const config = await resolveConnectionSeed("postgres");
  const driver = new PostgresDriver(config);
  const database = config.database ?? "";
  const schema = `interval_identity_${process.pid}`;
  const qualified = `${schema}.items`;
  const manager = { getDriver: () => driver, getConnection: () => config };
  const service = new TableDataService(manager as never);
  await driver.connect();
  try {
    await driver.query(`CREATE SCHEMA ${schema}`);
    await driver.query(`
      CREATE DOMAIN ${schema}.duration_domain AS interval;
      CREATE DOMAIN ${schema}.duration_array AS ${schema}.duration_domain[];
      CREATE TABLE ${qualified} (
        duration ${schema}.duration_domain,
        sequence ${schema}.duration_array DEFAULT '[0:0]={"4 days"}',
        amount integer NOT NULL,
        PRIMARY KEY (duration, sequence)
      );
      INSERT INTO ${qualified} (duration, amount) VALUES ('0 days', 1), ('1 day', 1);
    `);
    await driver.query("SET IntervalStyle = iso_8601");
    expect(rowsFromQuery(await driver.query("SHOW IntervalStyle"))).toEqual([
      { IntervalStyle: "iso_8601" },
    ]);
    const page = await service.getPage(
      config.id,
      database,
      schema,
      "items",
      1,
      10,
      [],
    );
    const before = page.rows[1];
    expect(before).toMatchObject({ duration: "P1D", amount: 1 });
    const intervalKey = page.postgresIntervalRowIdentities?.[1];
    expect(intervalKey).toEqual({
      duration: { $rapidbPgIntervalKey: "00000000000000000000000100000000" },
      sequence: { $rapidbPgIntervalKey: expect.stringMatching(/^[0-9a-f]+$/) },
    });
    expect(page.postgresArrayRowIdentities).toBeUndefined();
    expect(() =>
      buildUpdateRowSql(
        driver,
        database,
        schema,
        "items",
        { ...intervalKey, duration: before.duration },
        { amount: 2 },
        page.columns,
      ),
    ).toThrow(/missing its exact row identity/);

    await driver.query("SET IntervalStyle = sql_standard");
    expect(
      await applyChangesTransactional(
        manager as never,
        config.id,
        database,
        schema,
        "items",
        [
          {
            primaryKeys: { ...intervalKey },
            originalValues: before,
            changes: { duration: "P2D", amount: 2 },
          },
        ],
        page.columns,
      ),
    ).toMatchObject({ success: true, rowOutcomes: [{ status: "applied" }] });
    await service.insertRow(config.id, database, schema, "items", {
      duration: "P3D",
      amount: 3,
    });
    expect(
      rowsFromQuery(
        await driver.query(
          `SELECT extract(day FROM duration)::int AS days, array_lower(sequence, 1) AS lower, amount FROM ${qualified} ORDER BY duration`,
        ),
      ),
    ).toEqual([
      { days: 0, lower: 0, amount: 1 },
      { days: 2, lower: 0, amount: 2 },
      { days: 3, lower: 0, amount: 3 },
    ]);
    await driver.query("SET IntervalStyle = postgres_verbose");
    const current = await service.getPage(
      config.id,
      database,
      schema,
      "items",
      1,
      10,
      [],
    );
    expect(current.rows[1]?.duration).toBe("@ 2 days");
    await driver.query("SET IntervalStyle = postgres");
    await service.deleteRows(config.id, database, schema, "items", [
      { ...current.postgresIntervalRowIdentities?.[1] },
    ]);
    expect(
      rowsFromQuery(
        await driver.query(`SELECT amount FROM ${qualified} ORDER BY duration`),
      ),
    ).toEqual([{ amount: 1 }, { amount: 3 }]);
  } finally {
    try {
      await driver.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    } finally {
      await driver.disconnect();
    }
  }
});

it.each([
  [
    "interval",
    "'-1 mons +3 days -0:01:02.123456'",
    "'2562047788:00:54.775807'",
  ],
  [
    "interval[]",
    "'[0:1]={\"-1 mons +3 days -0:01:02.123456\",NULL}'",
    "'[2:2]={\"2562047788:00:54.775807\"}'",
  ],
])("uses the native %s index for point updates and retains exact microseconds", async (type, first, second) => {
  const config = await resolveConnectionSeed("postgres");
  const driver = new PostgresDriver(config);
  const database = config.database ?? "";
  const schema = `interval_index_${process.pid}`;
  const qualified = `${schema}.items`;
  const service = new TableDataService({
    getDriver: () => driver,
    getConnection: () => config,
  } as never);
  await driver.connect();
  try {
    await driver.query(`CREATE SCHEMA ${schema}`);
    await driver.query(`
      CREATE TABLE ${qualified} (duration ${type} PRIMARY KEY, amount integer NOT NULL);
      INSERT INTO ${qualified} VALUES (${first}, 1), (${second}, 2);
      SET enable_seqscan = off;
    `);
    const page = await service.getPage(
      config.id,
      database,
      schema,
      "items",
      1,
      10,
      [],
    );
    for (const [index, key] of (
      page.postgresIntervalRowIdentities ?? []
    ).entries()) {
      const operation = buildUpdateRowSql(
        driver,
        database,
        schema,
        "items",
        key,
        { amount: 10 },
        page.columns,
      );
      if (!operation) throw new Error("Missing interval update");
      if (index === 0) {
        const plan = await driver.query(
          `EXPLAIN ${operation.sql}`,
          operation.params,
        );
        expect(plan.rows.map((row) => row.__col_0).join("\n")).toMatch(
          /Index Cond/,
        );
        const prepared = await service.prepareDeleteRowsPlan(
          config.id,
          database,
          schema,
          "items",
          [key],
        );
        const preview = prepared?.previewStatements[0];
        expect(preview).toMatch(/= pg_catalog.decode\('[0-9a-f]+', 'hex'\)/);
        expect(
          (await driver.query(`EXPLAIN ${preview}`)).rows.length,
        ).toBeGreaterThan(0);
      }
      await driver.runTransaction(
        [{ ...operation, expectedAffectedRows: 1 }],
        undefined,
        { database },
      );
    }
    expect(
      rowsFromQuery(
        await driver.query(`SELECT amount FROM ${qualified} ORDER BY duration`),
      ),
    ).toEqual([{ amount: 10 }, { amount: 10 }]);
  } finally {
    try {
      await driver.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    } finally {
      await driver.disconnect();
    }
  }
});
