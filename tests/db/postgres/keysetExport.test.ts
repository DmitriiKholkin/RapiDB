import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

describe("exact keyset export", () => {
  it.each([
    2, 500,
  ])("terminates without duplicates for close floats, chunk %i", async (chunkSize) => {
    const config = await resolveConnectionSeed("postgres");
    const driver = new PostgresDriver(config);
    const schema = `keyset_${process.pid}_${chunkSize}`;
    await driver.connect();
    try {
      await driver.query(`CREATE SCHEMA ${driver.quoteIdentifier(schema)}`);
      await driver.query(
        `CREATE TABLE "${schema}".items(id int PRIMARY KEY, f float8 NOT NULL, bucket int NOT NULL)`,
      );
      await driver.query(
        `INSERT INTO "${schema}".items SELECT 1002-i, 1::float8+i*2.220446049250313e-16::float8, 1 FROM generate_series(1,1001) i`,
      );
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
          schema,
          "items",
          chunkSize,
          { column: "f", direction },
          [{ column: "bucket", operator: "eq", value: "1" }],
        )) {
          ids.push(...chunk.rows.map((row) => row.id));
          if (++batches > Math.ceil(1001 / chunkSize) + 1)
            throw new Error("Export did not terminate");
        }
        const expected = Array.from({ length: 1001 }, (_, index) =>
          direction === "asc" ? 1001 - index : index + 1,
        );
        expect(ids).toEqual(expected);
      }
    } finally {
      await driver.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      await driver.disconnect();
    }
  });
});
