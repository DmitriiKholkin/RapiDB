import { describe, expect, it } from "vitest";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import type { FilterExpression } from "../../../src/shared/tableTypes";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { createDriver } from "../../support/liveDbHarness";

export function registerNumericFilterPrecisionTests(
  engine: "postgres" | "mysql" | "mssql" | "oracle",
) {
  describe(`${engine} active numeric filter precision`, () => {
    it("keeps adjacent bigint and decimal values distinct in rows and counts", async () => {
      const config = await resolveConnectionSeed(engine);
      const driver = createDriver(engine, config);
      const schema =
        engine === "postgres"
          ? "public"
          : engine === "mssql"
            ? "dbo"
            : engine === "oracle"
              ? (config.username ?? "").toUpperCase()
              : "";
      const table = `M1_NUMERIC_${process.pid}`;
      const qualified = driver.qualifiedTableName(
        config.database ?? "",
        schema,
        table,
      );
      await driver.connect();
      try {
        await driver.query(
          `CREATE TABLE ${qualified} (${driver.quoteIdentifier("id")} int PRIMARY KEY, ${driver.quoteIdentifier("whole")} ${engine === "oracle" ? "NUMBER(19,0)" : "bigint"}, ${driver.quoteIdentifier("amount")} decimal(38,18))`,
        );
        for (const [id, value] of [
          [1, "9007199254740992"],
          [2, "9007199254740993"],
          [3, "9007199254740994"],
          [4, "-9007199254740993"],
        ]) {
          await driver.query(
            `INSERT INTO ${qualified} VALUES (${id}, ${value}, ${value}.123456789012345678)`,
          );
        }
        const service = new TableReadService({
          getDriver: () => driver,
          getConnection: () => config,
        } as never);
        const columns = await service.getColumns(
          config.id,
          config.database ?? "",
          schema,
          table,
        );
        expect(
          columns.find((column) => column.name === "whole")?.category,
        ).toBe("integer");
        expect(
          columns.find((column) => column.name === "amount")?.category,
        ).toBe("decimal");
        const check = async (filter: FilterExpression, expected: number[]) => {
          const page = await service.getPage(
            config.id,
            config.database ?? "",
            schema,
            table,
            1,
            100,
            [filter],
            { column: "id", direction: "asc" },
          );
          expect(
            page.rows.map((row) => Number(row.id)),
            JSON.stringify(filter),
          ).toEqual(expected);
          expect(page.totalCount, JSON.stringify(filter)).toBe(expected.length);
        };
        for (const value of [
          "9007199254740993",
          "+9007199254740993",
          "9007199254740993.0",
          "9.007199254740993e15",
        ]) {
          await check({ column: "whole", operator: "eq", value }, [2]);
          await check({ column: "whole", operator: "neq", value }, [1, 3, 4]);
          await check({ column: "whole", operator: "gt", value }, [3]);
          await check({ column: "whole", operator: "lt", value }, [1, 4]);
          await check(
            {
              column: "whole",
              operator: "in",
              value: `${value}, -9007199254740993`,
            },
            [2, 4],
          );
          await check(
            {
              column: "whole",
              operator: "between",
              value: [value, "9007199254740994"],
            },
            [2, 3],
          );
        }
        await check(
          { column: "whole", operator: "gt", value: "9007199254740993.5" },
          [3],
        );
        await check(
          { column: "whole", operator: "lt", value: "9007199254740993.5" },
          [1, 2, 4],
        );
        await check(
          { column: "whole", operator: "in", value: "9007199254740993.5" },
          [],
        );
        await check(
          {
            column: "whole",
            operator: "between",
            value: ["9007199254740993.5", "9007199254740994"],
          },
          [3],
        );
        const value = "9007199254740993.123456789012345678";
        await check({ column: "amount", operator: "eq", value }, [2]);
        await check({ column: "amount", operator: "gt", value }, [3]);
        await check({ column: "amount", operator: "lt", value }, [1, 4]);
        await check({ column: "amount", operator: "in", value }, [2]);
        await check(
          {
            column: "amount",
            operator: "between",
            value: [value, "9007199254740994.123456789012345678"],
          },
          [2, 3],
        );
        // A one-row page must still receive the full count, not a fallback
        // derived from the number of fetched rows.
        const first = await service.getPage(
          config.id,
          config.database ?? "",
          schema,
          table,
          1,
          1,
          [{ column: "whole", operator: "neq", value: "+9007199254740993" }],
          { column: "id", direction: "asc" },
        );
        expect(first.rows.map((row) => Number(row.id))).toEqual([1]);
        expect(first.totalCount).toBe(3);
        await driver.query(
          `INSERT INTO ${qualified} VALUES (5, 9223372036854775807, NULL)`,
        );
        await driver.query(
          `INSERT INTO ${qualified} VALUES (6, -9223372036854775808, NULL)`,
        );
        await check(
          { column: "whole", operator: "eq", value: "+9223372036854775807" },
          [5],
        );
        await check(
          { column: "whole", operator: "eq", value: "-9223372036854775808" },
          [6],
        );
      } finally {
        try {
          await driver.query(`DROP TABLE ${qualified}`);
        } finally {
          await driver.disconnect();
        }
      }
    });
  });
}
