import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../../src/extension/dbDrivers/mssql";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

describe("MSSQL exact temporal keyset export", () => {
  it.each([
    1, 2,
  ])("preserves datetime ticks and ties, chunk %i", async (chunkSize) => {
    const config = await resolveConnectionSeed("mssql");
    const driver = new MSSQLDriver(config);
    const table = `b02_datetime_${process.pid}_${chunkSize}`;
    const qualified = driver.qualifiedTableName(
      config.database ?? "",
      "dbo",
      table,
    );
    await driver.connect();
    try {
      await driver.query(
        `CREATE TABLE ${qualified}(id int PRIMARY KEY, ts datetime NOT NULL, bucket int NOT NULL)`,
      );
      await driver.query(`INSERT INTO ${qualified} VALUES
        (6, '2024-01-15T12:00:00.003', 1),
        (2, '2024-01-15T12:00:00.003', 1),
        (5, '2024-01-15T12:00:00.007', 1),
        (1, '2024-01-15T12:00:00.007', 1),
        (4, '2024-01-15T12:00:00.010', 1),
        (3, '2024-01-15T12:00:00.010', 1),
        (7, '2024-01-15T12:00:00.003', 2)`);
      const service = new TableReadService({
        getDriver: () => driver,
        getConnection: () => config,
      } as never);
      for (const direction of ["asc", "desc"] as const) {
        const ids: unknown[] = [];
        let batches = 0;
        for await (const chunk of service.exportAll(
          config.id,
          config.database ?? "",
          "dbo",
          table,
          chunkSize,
          { column: "ts", direction },
          [{ column: "bucket", operator: "eq", value: "1" }],
        )) {
          ids.push(...chunk.rows.map((row) => row.id));
          if (++batches > 6) throw new Error("Export did not terminate");
        }
        expect(ids).toEqual(
          direction === "asc" ? [2, 6, 1, 5, 3, 4] : [3, 4, 1, 5, 2, 6],
        );
      }
    } finally {
      try {
        await driver.query(`DROP TABLE IF EXISTS ${qualified}`);
      } finally {
        await driver.disconnect();
      }
    }
  });
});
