import { randomUUID } from "node:crypto";
import type { Pool } from "mysql2/promise";
import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../../src/extension/dbDrivers/mysql";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

describe("MySQL parameter safety across SQL modes", () => {
  it.each([
    "",
    "NO_BACKSLASH_ESCAPES",
  ])("preserves strings and row boundaries with sql_mode=%j", async (mode) => {
    const driver = new MySQLDriver(await resolveConnectionSeed("mysql"));
    const table = `parameter_safety_${randomUUID().replaceAll("-", "")}`;
    await driver.connect();
    const pool = (driver as unknown as { pool: Pool }).pool;
    pool.on("connection", (connection) => {
      connection.query(`SET SESSION sql_mode='${mode}'`);
    });
    const connection = await pool.getConnection();
    await connection.query(`SET SESSION sql_mode='${mode}'`);
    connection.release();
    try {
      await driver.query(
        `CREATE TABLE ${table} (id INT PRIMARY KEY, payload TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)`,
      );
      await driver.query(
        `INSERT INTO ${table} VALUES (1,'before'),(2,'untouched')`,
      );
      const attack = "x' WHERE 1=1 -- ";
      await driver.runTransaction([
        {
          sql: `UPDATE ${table} SET payload=? WHERE id=?`,
          params: [attack, 1],
          checkAffectedRows: true,
        },
      ]);
      expect(
        (await driver.query(`SELECT payload FROM ${table} ORDER BY id`)).rows,
      ).toEqual([{ __col_0: attack }, { __col_0: "untouched" }]);
      const value = "O'Reilly; \\ quoted 😀\u0000";
      expect(
        (await driver.query("SELECT COERCIBILITY(?)", [value])).rows,
      ).toEqual([{ __col_0: "4" }]);
      for (const hardCap of [undefined, 1]) {
        expect(
          (await driver.query("SELECT ? AS payload", [value], { hardCap }))
            .rows,
        ).toEqual([{ __col_0: value }]);
      }
      const preview = driver.materializePreviewSql(
        `UPDATE ${table} SET payload=? WHERE id=?`,
        [value, 1],
      );
      await driver.query(preview);
      expect(
        (await driver.query(`SELECT payload FROM ${table} ORDER BY id`)).rows,
      ).toEqual([{ __col_0: value }, { __col_0: "untouched" }]);
      expect(
        (
          await driver.query(`SELECT COUNT(*) FROM ${table} WHERE payload=?`, [
            value.toLowerCase(),
          ])
        ).rows,
      ).toEqual([{ __col_0: "0" }]);
    } finally {
      await driver.query(`DROP TABLE IF EXISTS ${table}`);
      await driver.disconnect();
    }
  });
});
