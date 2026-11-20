import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import { buildUpdateRowSql } from "../../../src/extension/table/updateSql";
import {
  applyChangesTransactional,
  TableDataService,
} from "../../../src/extension/tableDataService";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { rowsFromQuery } from "../../support/liveDbHarness";

describe("PostgreSQL native array primary-key identity (live)", () => {
  let config: ConnectionConfig;
  let driver: PostgresDriver;
  let service: TableDataService;
  let database: string;
  const schema = `array_identity_${process.pid}_${Date.now().toString(36)}`;
  const manager = { getDriver: () => driver, getConnection: () => config };

  beforeAll(async () => {
    config = await resolveConnectionSeed("postgres");
    database = config.database ?? "";
    driver = new PostgresDriver(config);
    await driver.connect();
    service = new TableDataService(manager as never);
    await driver.query(`CREATE SCHEMA ${schema}`);
  });

  afterAll(async () => {
    if (!driver) return;
    try {
      await driver.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    } finally {
      await driver.disconnect();
    }
  });

  it("verifies explicit and default inserts at their stored bounds", async () => {
    const table = "inserts";
    const qualified = `${schema}.${table}`;
    await driver.query(
      `CREATE TABLE ${qualified} (
        id integer[] PRIMARY KEY DEFAULT '[0:1]={4,5}', amount integer
      )`,
    );
    await service.insertRow(config.id, database, schema, table, {
      id: "[4,5]",
      amount: "1",
    });
    await service.insertRow(config.id, database, schema, table, {
      amount: "2",
    });
    await service.insertRow(config.id, database, schema, table, {
      id: "[-1:0][2:3]={{8,9},{10,11}}",
      amount: "3",
    });
    expect(
      rowsFromQuery(
        await driver.query(
          `SELECT id::text AS key, amount FROM ${qualified} ORDER BY amount`,
        ),
      ),
    ).toEqual([
      { key: "{4,5}", amount: 1 },
      { key: "[0:1]={4,5}", amount: 2 },
      { key: "[-1:0][2:3]={{8,9},{10,11}}", amount: 3 },
    ]);
  });

  it("verifies a key-changing update on the changed row, not an identical-element decoy", async () => {
    const table = "updates";
    const qualified = `${schema}.${table}`;
    await driver.query(
      `CREATE TABLE ${qualified} (id integer[] PRIMARY KEY, amount integer);
       INSERT INTO ${qualified} VALUES ('[0:1]={4,5}', 3), ('{8,9}', 8);
       CREATE FUNCTION ${schema}.change_amount() RETURNS trigger
       LANGUAGE plpgsql AS $$ BEGIN NEW.amount := 99; RETURN NEW; END $$;
       CREATE TRIGGER change_amount BEFORE UPDATE ON ${qualified}
       FOR EACH ROW EXECUTE FUNCTION ${schema}.change_amount()`,
    );
    const page = await service.getPage(
      config.id,
      database,
      schema,
      table,
      1,
      10,
      [],
    );
    expect(page.rows[0]).toEqual({ id: "[4,5]", amount: 3 });
    const key = page.postgresArrayRowIdentities?.[0];
    if (!key) throw new Error("Missing row identity");
    const changeKey = () =>
      applyChangesTransactional(
        manager as never,
        config.id,
        database,
        schema,
        table,
        [
          {
            primaryKeys: key,
            changes: { id: "[0:1]={8,9}", amount: "8" },
            originalValues: page.rows[0],
          },
        ],
        page.columns,
      );
    expect(await changeKey()).toMatchObject({
      success: false,
      changesPossible: false,
      rowOutcomes: [{ status: "verification_failed", columns: ["amount"] }],
    });
    await driver.query(`DROP TRIGGER change_amount ON ${qualified}`);
    expect(await changeKey()).toMatchObject({
      success: true,
      rowOutcomes: [{ status: "applied" }],
    });
    expect(
      rowsFromQuery(
        await driver.query(
          `SELECT id::text AS key FROM ${qualified} ORDER BY id`,
        ),
      ),
    ).toEqual([{ key: "[0:1]={8,9}" }, { key: "{8,9}" }]);
  });

  it("retains different bounds in composite domain keys and leaves export rows unchanged", async () => {
    const table = "domains";
    const qualified = `${schema}.${table}`;
    await driver.query(
      `CREATE DOMAIN ${schema}.array_domain AS integer[];
       CREATE TABLE ${qualified} (scope integer, id ${schema}.array_domain, PRIMARY KEY(scope, id));
       INSERT INTO ${qualified} VALUES
         (1, '[0:1]={4,5}'), (1, '{4,5}'), (2, '[0:1]={4,5}')`,
    );
    const page = await service.getPage(
      config.id,
      database,
      schema,
      table,
      1,
      10,
      [],
    );
    const identities = page.postgresArrayRowIdentities;
    expect(identities).toEqual([
      { id: { $rapidbPgArrayKey: "[0:1]={4,5}" } },
      { id: { $rapidbPgArrayKey: "{4,5}" } },
      { id: { $rapidbPgArrayKey: "[0:1]={4,5}" } },
    ]);
    const exported: unknown[] = [];
    for await (const chunk of service.exportAll(
      config.id,
      database,
      schema,
      table,
      1,
    )) {
      exported.push(...chunk.rows);
    }
    expect(exported).toEqual(page.rows);
    await service.deleteRows(
      config.id,
      database,
      schema,
      table,
      page.rows.slice(0, 2).map(({ scope }, index) => ({
        scope,
        ...identities?.[index],
      })),
    );
    expect(
      rowsFromQuery(
        await driver.query(`SELECT scope, id::text AS key FROM ${qualified}`),
      ),
    ).toEqual([{ scope: 2, key: "[0:1]={4,5}" }]);
  });

  it("does not mutate a different row if a session reinterprets an array key", async () => {
    const table = "reinterpretation";
    const qualified = `${schema}.${table}`;
    await driver.query(
      `CREATE TABLE ${qualified} (id text[] PRIMARY KEY, amount integer);
       INSERT INTO ${qualified} VALUES ('[0:0]={NULL}', 1), ('[0:0]={"NULL"}', 2)`,
    );
    const page = await service.getPage(
      config.id,
      database,
      schema,
      table,
      1,
      10,
      [],
    );
    const key = page.postgresArrayRowIdentities?.find(
      ({ id }) => id?.$rapidbPgArrayKey === "[0:0]={NULL}",
    );
    if (!key) throw new Error("Missing NULL element key");
    const update = buildUpdateRowSql(
      driver,
      database,
      schema,
      table,
      key,
      { amount: 99 },
      page.columns,
    );
    const deletion = await service.prepareDeleteRowsPlan(
      config.id,
      database,
      schema,
      table,
      [key],
    );
    if (!update || !deletion) throw new Error("Missing mutation plan");
    for (const operation of [update, ...deletion.operations]) {
      await expect(
        driver.runTransaction(
          [
            { sql: "SET LOCAL array_nulls = off" },
            { ...operation, expectedAffectedRows: 1 },
          ],
          undefined,
          { database },
        ),
      ).rejects.toThrow(/affected 0 row/);
    }
    expect(
      rowsFromQuery(
        await driver.query(
          `SELECT id::text AS key, amount FROM ${qualified} ORDER BY amount`,
        ),
      ),
    ).toEqual([
      { key: "[0:0]={NULL}", amount: 1 },
      { key: '[0:0]={"NULL"}', amount: 2 },
    ]);
  });
});
