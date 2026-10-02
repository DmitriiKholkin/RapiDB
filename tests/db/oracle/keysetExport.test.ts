import { describe, expect, it } from "vitest";
import { OracleDriver } from "../../../src/extension/dbDrivers/oracle";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

describe("Oracle timezone-aware keyset export", () => {
  it.each([
    1, 2,
  ])("preserves winter/summer instants across session timezones, chunk %i", async (chunkSize) => {
    const config = await resolveConnectionSeed("oracle");
    const driver = new OracleDriver(config);
    const table = `B02_TZ_${process.pid}_${chunkSize}`;
    const schema = (config.username ?? "").toUpperCase();
    const qualified = driver.qualifiedTableName(
      config.database ?? "",
      schema,
      table,
    );
    await driver.connect();
    let created = false;
    try {
      await driver.query(`CREATE TABLE ${qualified}(ID NUMBER(10) PRIMARY KEY,
        TSTZ TIMESTAMP(3) WITH TIME ZONE NOT NULL,
        TSLTZ TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL, BUCKET NUMBER(10) NOT NULL)`);
      created = true;
      const values = [
        [6, "2024-01-15 12:00:00.003 +01:00"],
        [2, "2024-01-15 11:00:00.003 +00:00"],
        [5, "2024-01-15 12:00:00.007 +01:00"],
        [1, "2024-07-15 12:00:00.003 +02:00"],
        [4, "2024-07-15 10:00:00.003 +00:00"],
        [3, "2024-07-15 12:00:00.007 +02:00"],
      ] as const;
      for (const [id, value] of values) {
        await driver.query(
          `INSERT INTO ${qualified} VALUES (:1,
          TO_TIMESTAMP_TZ(:2, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'),
          TO_TIMESTAMP_TZ(:3, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'), 1)`,
          [id, value, value],
        );
      }
      await driver.query(
        `INSERT INTO ${qualified} SELECT 7, TSTZ, TSLTZ, 2 FROM ${qualified} WHERE ID = 6`,
      );
      const service = new TableReadService({
        getDriver: () => driver,
        getConnection: () => config,
      } as never);
      for (const sessionTimezone of ["-08:00", "+05:30", "Europe/Berlin"]) {
        await driver.query(
          `ALTER SESSION SET TIME_ZONE = '${sessionTimezone}'`,
        );
        const session = await driver.query("SELECT SESSIONTIMEZONE FROM dual");
        expect(session.rows[0].__col_0).toBe(sessionTimezone);
        for (const column of ["TSTZ", "TSLTZ"]) {
          for (const direction of ["asc", "desc"] as const) {
            const ids: unknown[] = [];
            let batches = 0;
            for await (const chunk of service.exportAll(
              config.id,
              config.database ?? "",
              schema,
              table,
              chunkSize,
              { column, direction },
              [{ column: "BUCKET", operator: "eq", value: "1" }],
            )) {
              ids.push(...chunk.rows.map((row) => row.ID));
              if (++batches > 6) throw new Error("Export did not terminate");
              // Change timezone while the generator is suspended: the next
              // cursor must still denote the same instant as the previous row.
              const nextTimezone =
                batches % 2 === 0 ? sessionTimezone : "+09:00";
              await driver.query(
                `ALTER SESSION SET TIME_ZONE = '${nextTimezone}'`,
              );
              const currentSession = await driver.query(
                "SELECT SESSIONTIMEZONE FROM dual",
              );
              expect(currentSession.rows[0].__col_0).toBe(nextTimezone);
            }
            expect(ids).toEqual(
              direction === "asc" ? [2, 6, 5, 1, 4, 3] : [3, 1, 4, 5, 2, 6],
            );
          }
        }
      }
    } finally {
      try {
        if (created) await driver.query(`DROP TABLE ${qualified} PURGE`);
      } finally {
        await driver.disconnect();
      }
    }
  });

  it("compares precise UTC cursor text natively without FF6 rounding", async () => {
    const config = await resolveConnectionSeed("oracle");
    const driver = new OracleDriver(config);
    const schema = (config.username ?? "").toUpperCase();
    const table = `B02_PRECISE_${process.pid}`;
    const qualified = driver.qualifiedTableName(
      config.database ?? "",
      schema,
      table,
    );
    await driver.connect();
    let created = false;
    try {
      await driver.query(`CREATE TABLE ${qualified}(ID NUMBER(10) PRIMARY KEY,
        TSTZ TIMESTAMP(9) WITH TIME ZONE NOT NULL,
        TSLTZ TIMESTAMP(9) WITH LOCAL TIME ZONE NOT NULL)`);
      created = true;
      for (const [id, value] of [
        [1, "2024-07-15 12:00:00.123456789 +02:00"],
        [2, "2024-07-15 12:00:00.123456790 +02:00"],
        [3, "2024-07-15 12:00:01.000000000 +02:00"],
      ]) {
        await driver.query(
          `INSERT INTO ${qualified} VALUES (:1,
          TO_TIMESTAMP_TZ(:2, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'),
          TO_TIMESTAMP_TZ(:3, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'))`,
          [id, value, value],
        );
      }
      const columns = await driver.describeColumns(
        config.database ?? "",
        schema,
        table,
      );
      for (const timezone of ["-08:00", "+05:30"]) {
        await driver.query(`ALTER SESSION SET TIME_ZONE = '${timezone}'`);
        for (const name of ["TSTZ", "TSLTZ"]) {
          const column = columns.find((entry) => entry.name === name);
          if (!column) throw new Error(`Missing column ${name}`);
          for (const [operator, expected] of [
            ["eq", [1]],
            ["gt", [2, 3]],
            ["lt", []],
          ] as const) {
            const condition = driver.buildCursorComparison(
              column,
              operator,
              "2024-07-15 10:00:00.123456789",
              1,
            );
            const result = await driver.query(
              `SELECT ID FROM ${qualified} WHERE ${condition.sql} ORDER BY ID`,
              condition.params,
            );
            expect(result.rows.map((row) => row.__col_0)).toEqual(expected);
          }
          const condition = driver.buildCursorComparison(
            column,
            "eq",
            "2024-07-15 10:00:01",
            1,
          );
          const result = await driver.query(
            `SELECT ID FROM ${qualified} WHERE ${condition.sql}`,
            condition.params,
          );
          expect(result.rows.map((row) => row.__col_0)).toEqual([3]);
        }
      }
    } finally {
      try {
        if (created) await driver.query(`DROP TABLE ${qualified} PURGE`);
      } finally {
        await driver.disconnect();
      }
    }
  });
});
