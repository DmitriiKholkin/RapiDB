import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../../src/extension/connectionManager";
import { OracleDriver } from "../../../src/extension/dbDrivers/oracle";
import type { TablePage } from "../../../src/extension/table/tableDataContracts";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableDataService } from "../../../src/extension/tableDataService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

const sameDisplay = "0001-02-03 04:05:06";
const dateValue = (era: "AD" | "BC", text = sameDisplay) =>
  `TO_DATE('${text} ${era}', 'YYYY-MM-DD HH24:MI:SS BC')`;

async function withTable(
  definition: string,
  test: (fixture: {
    driver: OracleDriver;
    service: TableDataService;
    scope: [string, string, string, string];
    table: string;
    apply: (
      page: TablePage,
      keys: Record<string, unknown>,
      changes: Record<string, unknown>,
      originals: Record<string, unknown>,
    ) => ReturnType<typeof executePreparedApplyPlan>;
  }) => Promise<void>,
) {
  const config = await resolveConnectionSeed("oracle");
  const driver = new OracleDriver(config);
  const database = config.database ?? "";
  const schema = (config.username ?? "").toUpperCase();
  const name = `H6_TEMPORAL_${randomUUID().replaceAll("-", "").slice(0, 16)}`;
  const table = driver.qualifiedTableName(database, schema, name);
  const scope: [string, string, string, string] = [
    config.id,
    database,
    schema,
    name,
  ];
  await driver.connect();
  let created = false;
  try {
    await driver.query(`CREATE TABLE ${table} (${definition})`);
    created = true;
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    await test({
      driver,
      service: new TableDataService(manager),
      scope,
      table,
      apply: (page, primaryKeys, changes, originalValues) => {
        const prepared = prepareApplyChangesPlan(
          manager,
          ...scope,
          [{ primaryKeys, changes, originalValues }],
          page.columns,
        );
        if (!prepared.executable) throw new Error("No prepared UPDATE");
        return executePreparedApplyPlan(manager, prepared.plan);
      },
    });
  } finally {
    try {
      if (created) await driver.query(`DROP TABLE ${table} PURGE`);
    } finally {
      await driver.disconnect();
    }
  }
}

describe("Oracle exact temporal primary keys (live)", () => {
  it("keeps DATE display unchanged and targets, verifies and pages by era", async () => {
    await withTable(
      'ID DATE PRIMARY KEY, AMOUNT NUMBER(10), "__rapidb_oracle_temporal_key_0" NUMBER(10) DEFAULT 4',
      async ({ driver, service, scope, table, apply }) => {
        for (const era of ["BC", "AD"] as const) {
          await driver.query(
            `INSERT INTO ${table}(ID, AMOUNT) VALUES (${dateValue(era)}, 1)`,
          );
        }
        const stored = async () =>
          (
            await driver.query(
              `SELECT TO_CHAR(ID, 'YYYY-MM-DD HH24:MI:SS BC'), AMOUNT FROM ${table} ORDER BY ID`,
            )
          ).rows.map(({ __col_0, __col_1 }) => [__col_0, __col_1]);
        const page = await service.getPage(...scope, 1, 10, []);
        expect(page.rows).toEqual(
          Array(2).fill({
            ID: sameDisplay,
            AMOUNT: 1,
            __rapidb_oracle_temporal_key_0: 4,
          }),
        );
        const keys = page.oracleTemporalRowIdentities;
        expect(keys).toEqual([
          { ID: { $rapidbOracleTemporalKey: `-${sameDisplay}` } },
          { ID: { $rapidbOracleTemporalKey: ` ${sameDisplay}` } },
        ]);
        if (!keys) throw new Error("Missing projected Oracle keys");
        expect(() =>
          apply(page, { ID: sameDisplay }, { AMOUNT: 7 }, { AMOUNT: 1 }),
        ).toThrow(/Refresh the table.*exact stored identity/);
        await expect(
          service.prepareDeleteRowsPlan(...scope, [{ ID: sameDisplay }]),
        ).rejects.toThrow(/Refresh the table.*exact stored identity/);
        expect(await stored()).toEqual([
          [`${sameDisplay} BC`, 1],
          [`${sameDisplay} AD`, 1],
        ]);

        await driver.query("ALTER SESSION SET NLS_CALENDAR = 'Thai Buddha'");
        expect(
          (await service.getPage(...scope, 1, 10, []))
            .oracleTemporalRowIdentities,
        ).toEqual(keys);
        expect(
          await apply(page, keys[0], { AMOUNT: 2 }, page.rows[0]),
        ).toMatchObject({ success: true });
        await driver.query("ALTER SESSION SET NLS_CALENDAR = 'Gregorian'");
        expect(
          await apply(page, keys[0], { AMOUNT: 3 }, { AMOUNT: 1 }),
        ).toMatchObject({ success: false });
        expect(await stored()).toEqual([
          [`${sameDisplay} BC`, 2],
          [`${sameDisplay} AD`, 1],
        ]);
        for (const direction of ["asc", "desc"] as const) {
          const exported = [];
          for await (const chunk of service.exportAll(...scope, 1, {
            column: "ID",
            direction,
          })) {
            exported.push(...chunk.rows);
          }
          expect(exported.map((row) => [row.ID, row.AMOUNT])).toEqual(
            direction === "asc"
              ? [
                  [sameDisplay, 2],
                  [sameDisplay, 1],
                ]
              : [
                  [sameDisplay, 1],
                  [sameDisplay, 2],
                ],
          );
          expect(exported.every((row) => Object.keys(row).length === 3)).toBe(
            true,
          );
        }
        const deletion = await service.prepareDeleteRowsPlan(...scope, [
          keys[0],
        ]);
        if (!deletion) throw new Error("No prepared DELETE");
        expect(deletion.previewStatements[0]).toContain(`'-${sameDisplay}'`);
        await service.executePreparedDeletePlan(deletion);
        expect(await stored()).toEqual([[`${sameDisplay} AD`, 1]]);
      },
    );
  });

  it("uses the returned new DATE key and detects a trigger assigning the opposite era", async () => {
    await withTable(
      "ID DATE PRIMARY KEY, AMOUNT NUMBER(10)",
      async ({ driver, service, scope, table, apply }) => {
        const next = "0001-02-04 04:05:06";
        await driver.query(
          `INSERT INTO ${table} VALUES (${dateValue("BC")}, 1)`,
        );
        const page = await service.getPage(...scope, 1, 10, []);
        const key = page.oracleTemporalRowIdentities?.[0];
        if (!key) throw new Error("Missing projected Oracle key");
        const trigger = driver.qualifiedTableName(
          scope[1],
          scope[2],
          `${scope[3]}_TRG`,
        );
        await driver.query(
          `CREATE TRIGGER ${trigger} BEFORE UPDATE OF ID ON ${table} FOR EACH ROW
           BEGIN :NEW.ID := ${dateValue("BC", next)}; END;`,
        );
        expect(
          await apply(page, key, { ID: next, AMOUNT: 7 }, page.rows[0]),
        ).toMatchObject({
          success: false,
          changesPossible: false,
          rowOutcomes: [expect.objectContaining({ columns: ["ID"] })],
        });
        expect((await service.getPage(...scope, 1, 10, [])).rows).toEqual(
          page.rows,
        );
        await driver.query(`DROP TRIGGER ${trigger}`);
        expect(
          await apply(page, key, { ID: next, AMOUNT: 7 }, page.rows[0]),
        ).toMatchObject({ success: true });
        expect(await service.getPage(...scope, 1, 10, [])).toMatchObject({
          rows: [{ ID: next, AMOUNT: 7 }],
          oracleTemporalRowIdentities: [
            { ID: { $rapidbOracleTemporalKey: ` ${next}` } },
          ],
        });
      },
    );
  });

  it("uses the generated BC DATE via RETURNING even when an AD twin exists", async () => {
    await withTable(
      `ID DATE DEFAULT ${dateValue("BC")} PRIMARY KEY, AMOUNT NUMBER(10)`,
      async ({ driver, service, scope, table }) => {
        await driver.query(
          `INSERT INTO ${table} VALUES (${dateValue("AD")}, 1)`,
        );
        await service.insertRow(...scope, { AMOUNT: 9 });
        expect((await service.getPage(...scope, 1, 10, [])).rows).toEqual([
          { ID: sameDisplay, AMOUNT: 9 },
          { ID: sameDisplay, AMOUNT: 1 },
        ]);
      },
    );
  });

  it.each([
    "TIMESTAMP(9)",
    "TIMESTAMP(9) WITH LOCAL TIME ZONE",
  ])("keeps the era and nanoseconds for %s composite keys", async (type) => {
    await withTable(
      `BUCKET NUMBER(10), ID ${type}, AMOUNT NUMBER(10), PRIMARY KEY (BUCKET, ID)`,
      async ({ driver, service, scope, table, apply }) => {
        const local = type.includes("LOCAL");
        const ts = `${sameDisplay}.123456789`;
        for (const era of ["BC", "AD"]) {
          const literal = local
            ? `TO_TIMESTAMP_TZ('${ts} ${era} +00:00', 'YYYY-MM-DD HH24:MI:SS.FF9 BC TZH:TZM')`
            : `TO_TIMESTAMP('${ts} ${era}', 'YYYY-MM-DD HH24:MI:SS.FF9 BC')`;
          await driver.query(`INSERT INTO ${table} VALUES (1, ${literal}, 1)`);
        }
        const page = await service.getPage(...scope, 1, 10, []);
        expect(page.rows[0]).toEqual(page.rows[1]);
        const keys = page.oracleTemporalRowIdentities;
        expect(keys).toEqual([
          { ID: { $rapidbOracleTemporalKey: `-${ts}` } },
          { ID: { $rapidbOracleTemporalKey: ` ${ts}` } },
        ]);
        if (!keys) throw new Error("Missing projected Oracle keys");
        const bcKey = { ...keys[0], BUCKET: 1 };
        const adKey = { ...keys[1], BUCKET: 1 };
        await driver.query("ALTER SESSION SET TIME_ZONE = 'America/New_York'");
        expect(
          await apply(page, bcKey, { AMOUNT: 8 }, page.rows[0]),
        ).toMatchObject({ success: true });
        const exported = [];
        for await (const chunk of service.exportAll(...scope, 1)) {
          exported.push(...chunk.rows);
        }
        expect(exported.map((row) => row.AMOUNT)).toEqual([8, 1]);
        const deletion = await service.prepareDeleteRowsPlan(...scope, [adKey]);
        if (!deletion) throw new Error("No prepared DELETE");
        await service.executePreparedDeletePlan(deletion);
        const remaining = await service.getPage(...scope, 1, 10, []);
        expect(remaining.rows).toHaveLength(1);
        const newKey = "0001-02-04 04:05:06.123456788";
        expect(
          await apply(remaining, bcKey, { ID: newKey }, remaining.rows[0]),
        ).toMatchObject({ success: true });
        expect(
          (await service.getPage(...scope, 1, 10, []))
            .oracleTemporalRowIdentities,
        ).toEqual([{ ID: { $rapidbOracleTemporalKey: ` ${newKey}` } }]);
      },
    );
  });
});
