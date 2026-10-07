import { EventEmitter } from "node:events";
import mssql from "mssql";
import { describe, expect, it, vi } from "vitest";
import { exactNumericFilterLiteral } from "../../src/extension/dbDrivers/exactNumericFilter";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import {
  type ColumnTypeMeta,
  resolveFilterOperators,
} from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";

const factories = [
  [
    "PostgreSQL",
    () => new PostgresDriver({ id: "m1", name: "m1", type: "pg" }),
    "bigint",
    "double precision",
  ],
  [
    "MySQL",
    () => new MySQLDriver({ id: "m1", name: "m1", type: "mysql" }),
    "bigint",
    "double",
  ],
  [
    "MSSQL",
    () => new MSSQLDriver({ id: "m1", name: "m1", type: "mssql" }),
    "bigint",
    "float",
  ],
  [
    "Oracle",
    () => new OracleDriver({ id: "m1", name: "m1", type: "oracle" }),
    "NUMBER(19,0)",
    "BINARY_DOUBLE",
  ],
  [
    "SQLite",
    () => new SQLiteCoreDriver({ id: "m1", name: "m1", type: "sqlite" }),
    "BIGINT",
    "REAL",
  ],
] as const;

function meta(
  driver: ReturnType<(typeof factories)[number][1]>,
  nativeType: string,
): ColumnTypeMeta {
  const category = driver.mapTypeCategory(nativeType);
  return {
    name: "value",
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    valueSemantics: "plain",
    filterOperators: resolveFilterOperators(category, {
      filterable: true,
      nullable: true,
    }),
  };
}

describe.each(
  factories,
)("%s numeric precision contracts", (_name, create, integerType, floatType) => {
  it("retains exact signed bigint text and existing approximate float behavior", () => {
    const driver = create();
    const condition = buildWhere(
      driver,
      [{ column: "value", operator: "eq", value: "+9007199254740993" }],
      [meta(driver, integerType)],
    );
    expect(String(condition.params[0])).toMatch(/^\+?9007199254740993$/);
    const approximate = buildWhere(
      driver,
      [{ column: "value", operator: "eq", value: "1.234567890123456789" }],
      [meta(driver, floatType)],
    );
    expect(approximate.params[0]).toBe(Number("1.234567890123456789"));
  });

  it("rejects malformed/non-finite integer filters before building SQL", () => {
    const driver = create();
    const column = meta(driver, integerType);
    for (const value of [
      "NaN",
      "Infinity",
      "1e999",
      "0x10",
      "1); DROP TABLE items; --",
    ]) {
      for (const filter of [
        { column: "value", operator: "gt" as const, value },
        ...(_name === "SQLite"
          ? []
          : [
              { column: "value", operator: "eq" as const, value },
              {
                column: "value",
                operator: "in" as const,
                value: `1, ${value}`,
              },
            ]),
        {
          column: "value",
          operator: "between" as const,
          value: ["1", value] as [string, string],
        },
      ])
        expect(() => buildWhere(driver, [filter], [column])).toThrow(
          /RapiDB Filter/,
        );
    }
    if (_name === "SQLite") {
      // SQLite deliberately supports text stored in INTEGER-affinity columns.
      expect(
        buildWhere(
          driver,
          [{ column: "value", operator: "eq", value: "raw text" }],
          [column],
        ).params,
      ).toEqual(["raw text"]);
    }
  });
});

describe("bounded exact DECIMAL filter transport", () => {
  it.each([
    ["+9.007199254740993e15", "9007199254740993", 0],
    ["-9007199254740993.5000", "-9007199254740993.5", 1],
    [".0012500", "0.00125", 5],
    ["1.25e-3", "0.00125", 5],
    ["000001.25e2", "125", 0],
    ["-0e-99999999999999999999", "0", 0],
  ] as const)("expands %s without rounding", (input, value, scale) => {
    expect(exactNumericFilterLiteral(input, "value", 38, 38)).toEqual({
      value,
      scale,
    });
  });

  it.each([
    "1e99999999999999999999",
    "1e-99999999999999999999",
    "1e38",
    "1e-39",
    "1.2 OR 1=1",
  ])("rejects unrepresentable input %s without expanding it", (value) => {
    expect(() => exactNumericFilterLiteral(value, "value", 38, 38)).toThrow(
      /RapiDB Filter/,
    );
  });

  it("binds MSSQL bigint as BigInt and decimal/scientific operands as exact NVarChar", async () => {
    const driver = new MSSQLDriver({ id: "m1", name: "m1", type: "mssql" });
    const request = Object.assign(new EventEmitter(), {
      input: vi.fn().mockReturnThis(),
      query: vi.fn().mockResolvedValue({ rowsAffected: [0], columns: [[]] }),
    });
    (driver as unknown as { pool: unknown }).pool = { request: () => request };
    for (const [value, expected, expectedType] of [
      ["+9007199254740993", 9007199254740993n, mssql.BigInt],
      ["9.007199254740993e15", "9007199254740993", mssql.NVarChar(16)],
      ["9007199254740993.5", "9007199254740993.5", mssql.NVarChar(18)],
    ] as const) {
      request.input.mockClear();
      const condition = buildWhere(
        driver,
        [{ column: "value", operator: "gt", value }],
        [meta(driver, "bigint")],
      );
      await driver.query(
        `SELECT 1 FROM items ${condition.clause}`,
        condition.params,
      );
      expect(request.input).toHaveBeenCalledExactlyOnceWith(
        "p1",
        expectedType,
        expected,
      );
    }
  });
});
