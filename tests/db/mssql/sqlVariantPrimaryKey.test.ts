import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../../src/extension/connectionManager";
import { MSSQLDriver } from "../../../src/extension/dbDrivers/mssql";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableDataService } from "../../../src/extension/tableDataService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

async function withTable(
  definition: string,
  test: (fixture: {
    driver: MSSQLDriver;
    manager: ConnectionManager;
    service: TableDataService;
    scope: [string, string, string, string];
    table: string;
  }) => Promise<void>,
  useVariantAlias = false,
) {
  const config = await resolveConnectionSeed("mssql");
  const driver = new MSSQLDriver(config);
  const name = `variant_key_${randomUUID().replaceAll("-", "")}`;
  const scope = [config.id, config.database ?? "", "dbo", name] as const;
  const table = driver.qualifiedTableName(scope[1], scope[2], scope[3]);
  const alias = `dbo.[${name}_type]`;
  await driver.connect();
  try {
    if (useVariantAlias) {
      await driver.query(`CREATE TYPE ${alias} FROM sql_variant NOT NULL`);
    }
    await driver.query(
      `CREATE TABLE ${table} (${useVariantAlias ? definition.replace("sql_variant", alias) : definition})`,
    );
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    await test({
      driver,
      manager,
      service: new TableDataService(manager),
      scope: [...scope],
      table,
    });
  } finally {
    try {
      await driver.query(`DROP TABLE IF EXISTS ${table}`);
      if (useVariantAlias) {
        await driver.query(`DROP TYPE IF EXISTS ${alias}`);
      }
    } finally {
      await driver.disconnect();
    }
  }
}

const value = "12345678901234567890.12";

describe("SQL Server sql_variant primary key (live)", () => {
  it("updates and deletes the decimal row, never the identically displayed text row", async () => {
    await withTable(
      "id sql_variant PRIMARY KEY, amount decimal(12,2), payload sql_variant",
      async ({ driver, manager, service, scope, table }) => {
        await driver.query(
          `INSERT INTO ${table} VALUES
           (CONVERT(sql_variant, CONVERT(decimal(26,2), '${value}')), 1.25, CONVERT(sql_variant, CONVERT(decimal(7,2), '8.25'))),
           (CONVERT(sql_variant, CONVERT(nvarchar(40), N'${value}')), 1.25, CONVERT(sql_variant, CONVERT(decimal(7,2), '8.25')))`,
        );
        const stored = async () =>
          (
            await driver.query(
              `SELECT CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(id, 'BaseType')), amount, CONVERT(decimal(7,2), payload) FROM ${table} ORDER BY id`,
            )
          ).rows;
        expect(
          (
            await driver.query(
              `SELECT CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(id, 'BaseType')) FROM ${table} WHERE id = ?`,
              [value],
            )
          ).rows,
        ).toEqual([{ __col_0: "nvarchar" }]);

        const page = await service.getPage(...scope, 1, 25, []);
        expect(page.rows.map(({ id }) => id)).toEqual([value, value]);
        expect(page.columns[0].mssqlSqlVariantPrimaryKey).toBe(true);
        const keys = page.mssqlSqlVariantRowIdentities;
        expect(
          keys?.map(({ id }) => id?.$rapidbMssqlVariantKey.split(":")[1]),
        ).toEqual(["nvarchar", "decimal"]);
        if (!keys) throw new Error("Missing exact variant keys");
        const decimalRow = page.rows[1];
        const exactKey = keys[1];

        const plan = (
          primaryKeys: Record<string, unknown>,
          changes: Record<string, unknown>,
        ) =>
          prepareApplyChangesPlan(
            manager,
            ...scope,
            [{ primaryKeys, changes, originalValues: decimalRow }],
            page.columns,
          );
        expect(() => plan({ id: value }, { amount: "2.25" })).toThrow(
          /Refresh the table.*exact stored identity/,
        );
        await expect(
          service.prepareDeleteRowsPlan(...scope, [{ id: value }]),
        ).rejects.toThrow(/Refresh the table.*exact stored identity/);
        expect(() => plan(exactKey, { id: `${value}0` })).toThrow(
          /cannot safely change SQL Server sql_variant primary key.*Query Editor/,
        );
        expect(await stored()).toEqual([
          { __col_0: "nvarchar", __col_1: "1.25", __col_2: "8.25" },
          { __col_0: "decimal", __col_1: "1.25", __col_2: "8.25" },
        ]);

        const prepared = plan(JSON.parse(JSON.stringify(exactKey)), {
          amount: "2.25",
          payload: "9.25",
        });
        if (!prepared.executable) throw new Error("No prepared UPDATE");
        expect(
          await executePreparedApplyPlan(manager, prepared.plan),
        ).toMatchObject({
          success: true,
        });
        expect(await stored()).toEqual([
          { __col_0: "nvarchar", __col_1: "1.25", __col_2: "8.25" },
          { __col_0: "decimal", __col_1: "2.25", __col_2: "9.25" },
        ]);

        const deletion = await service.prepareDeleteRowsPlan(...scope, [
          exactKey,
        ]);
        if (!deletion) throw new Error("No prepared DELETE");
        await service.executePreparedDeletePlan(deletion);
        expect(await stored()).toEqual([
          { __col_0: "nvarchar", __col_1: "1.25", __col_2: "8.25" },
        ]);
      },
    );
  });

  it("uses the same exact identity for composite keys, collation, temporal precision and binary data", async () => {
    await withTable(
      "tenant int, id sql_variant, label varchar(20), amount decimal(12,2), PRIMARY KEY (tenant, id)",
      async ({ driver, manager, service, scope, table }) => {
        await driver.query(
          `INSERT INTO ${table} VALUES
           (1, CONVERT(sql_variant, CAST(N'same' COLLATE Latin1_General_100_BIN2 AS nvarchar(20))), 'bin2', 1.25),
           (1, CONVERT(sql_variant, CAST(N'same' COLLATE Japanese_CI_AS AS nvarchar(20))), 'japanese', 1.25),
           (1, CONVERT(sql_variant, CAST('2026-01-02T11:22:33.1234567+01:30' AS datetimeoffset(7))), 'offset', 1.25),
           (1, CONVERT(sql_variant, CAST('2026-01-02T11:22:33.1234568+01:30' AS datetimeoffset(7))), 'offset sibling', 1.25),
           (1, CONVERT(sql_variant, CAST(0xAB00 AS varbinary(8))), 'binary', 1.25),
           (2, CONVERT(sql_variant, CAST(0xAB AS varbinary(8))), 'binary sibling', 1.25)`,
        );
        const page = await service.getPage(...scope, 1, 25, []);
        const key = (label: string) => {
          const index = page.rows.findIndex((row) => row.label === label);
          const identity = page.mssqlSqlVariantRowIdentities?.[index]?.id;
          if (!identity)
            throw new Error(`Missing exact variant key for ${label}`);
          return { tenant: page.rows[index].tenant, id: identity };
        };
        expect(key("bin2").id.$rapidbMssqlVariantKey).toContain(
          ":Latin1_General_100_BIN2:",
        );
        expect(key("japanese").id.$rapidbMssqlVariantKey).toContain(
          ":Japanese_CI_AS:",
        );
        expect(key("offset").id.$rapidbMssqlVariantKey).not.toBe(
          key("offset sibling").id.$rapidbMssqlVariantKey,
        );
        const offsetHex = key("offset").id.$rapidbMssqlVariantKey.split(":")[6];
        expect(Buffer.from(offsetHex, "hex").toString("utf16le")).toBe(
          "2026-01-02T11:22:33.1234567+01:30",
        );
        expect(key("binary").id.$rapidbMssqlVariantKey).toMatch(/:AB00$/);
        expect(key("binary sibling").id.$rapidbMssqlVariantKey).toMatch(/:AB$/);
        const prepared = prepareApplyChangesPlan(
          manager,
          ...scope,
          [{ primaryKeys: key("offset"), changes: { amount: "2.25" } }],
          page.columns,
        );
        if (!prepared.executable) throw new Error("No prepared UPDATE");
        expect(
          await executePreparedApplyPlan(manager, prepared.plan),
        ).toMatchObject({
          success: true,
        });
        const deletion = await service.prepareDeleteRowsPlan(...scope, [
          key("bin2"),
          key("binary"),
        ]);
        if (!deletion) throw new Error("No prepared DELETE");
        await service.executePreparedDeletePlan(deletion);
        expect(
          (
            await driver.query(
              `SELECT label, amount FROM ${table} ORDER BY label`,
            )
          ).rows,
        ).toEqual([
          { __col_0: "binary sibling", __col_1: "1.25" },
          { __col_0: "japanese", __col_1: "1.25" },
          { __col_0: "offset", __col_1: "2.25" },
          { __col_0: "offset sibling", __col_1: "1.25" },
        ]);
      },
    );
  });

  it.each([
    false,
    true,
  ])("verifies explicit and default keys against their own rows (alias=%s)", async (useVariantAlias) => {
    await withTable(
      `id sql_variant NOT NULL DEFAULT (CAST('${value}' AS decimal(26,2))) PRIMARY KEY, amount decimal(12,2)`,
      async ({ driver, service, scope, table }) => {
        await service.insertRow(...scope, { id: value, amount: "1.25" });
        await service.insertRow(...scope, { amount: "2.50" });
        expect(
          (
            await driver.query(
              `SELECT CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(id, 'BaseType')), amount FROM ${table} ORDER BY id`,
            )
          ).rows,
        ).toEqual([
          { __col_0: "nvarchar", __col_1: "1.25" },
          { __col_0: "decimal", __col_1: "2.50" },
        ]);
        const page = await service.getPage(...scope, 1, 25, []);
        expect(page.columns[0].mssqlSqlVariantPrimaryKey).toBe(true);
        const keys = page.mssqlSqlVariantRowIdentities;
        if (!keys) throw new Error("Missing exact variant keys");
        const deletion = await service.prepareDeleteRowsPlan(...scope, [
          keys[1],
        ]);
        if (!deletion) throw new Error("No prepared DELETE");
        await service.executePreparedDeletePlan(deletion);
        expect((await service.getPage(...scope, 1, 25, [])).rows).toEqual([
          { id: value, amount: "1.25" },
        ]);
      },
      useVariantAlias,
    );
  });
});
