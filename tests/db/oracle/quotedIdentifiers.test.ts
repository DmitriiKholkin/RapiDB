import { describe, expect, it } from "vitest";
import { OracleDriver } from "../../../src/extension/dbDrivers/oracle";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

describe("Oracle quoted object identity", () => {
  it("uses metadata from the exact table, not its uppercase sibling", async () => {
    const config = await resolveConnectionSeed("oracle");
    const driver = new OracleDriver(config);
    const schema = (config.username ?? "").toUpperCase();
    const table = `Quoted_${process.pid}`;
    const sibling = table.toUpperCase();
    const qualified = driver.qualifiedTableName("", schema, table);
    const siblingQualified = driver.qualifiedTableName("", schema, sibling);
    const created: string[] = [];
    await driver.connect();
    try {
      await driver.query(
        `CREATE TABLE ${siblingQualified} (ID BINARY_DOUBLE, VALUE NUMBER PRIMARY KEY)`,
      );
      created.push(siblingQualified);
      await driver.query(
        `CREATE TABLE ${qualified} (ID VARCHAR2(20) PRIMARY KEY, VALUE VARCHAR2(40))`,
      );
      created.push(qualified);
      await driver.query(`INSERT INTO ${qualified} VALUES ('01', 'unchanged')`);

      const columns = await driver.describeColumns("", schema, table);
      expect(columns.map(({ name, nativeType }) => [name, nativeType])).toEqual(
        [
          ["ID", "VARCHAR2(20 BYTE)"],
          ["VALUE", "VARCHAR2(40 BYTE)"],
        ],
      );
      expect(
        columns
          .filter((column) => column.isPrimaryKey)
          .map((column) => column.name),
      ).toEqual(["ID"]);
      expect(
        (await driver.getIndexes("", schema, table))
          .filter((index) => index.primary)
          .map((index) => index.columns),
      ).toEqual([["ID"]]);
      expect(
        (await driver.getConstraints("", schema, table))
          .filter((constraint) => constraint.kind === "primary_key")
          .map((constraint) => constraint.columns),
      ).toEqual([["ID"]]);
      const service = new TableReadService({
        getDriver: () => driver,
        getConnection: () => config,
      } as never);
      const page = await service.getPage(
        config.id,
        "",
        schema,
        table,
        1,
        25,
        [],
      );
      expect(page.rows).toEqual([{ ID: "01", VALUE: "unchanged" }]);
      const ddl = await driver.getCreateTableDDL("", schema, table);
      expect(ddl).toContain(`"${table}"`);
      expect(ddl).toContain("VARCHAR2");
      expect(ddl).not.toContain("BINARY_DOUBLE");
    } finally {
      try {
        for (const name of created.reverse())
          await driver.query(`DROP TABLE ${name} PURGE`);
      } finally {
        await driver.disconnect();
      }
    }
  });
});
