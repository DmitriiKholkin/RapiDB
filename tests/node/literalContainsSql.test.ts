import mysql from "mysql2";
import { describe, expect, it, vi } from "vitest";
import { BaseDBDriver } from "../../src/extension/dbDrivers/BaseDBDriver";
import { FilterBuilder } from "../../src/extension/dbDrivers/filterBuilder";
import { literalContainsPattern } from "../../src/extension/dbDrivers/literalContains";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import { resolveFilterOperators } from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { defaultFilterOperator } from "../../src/shared/tableTypes";

const config = { id: "h1", name: "H1", database: "db" };
const drivers = [
  [
    "SQLite",
    () =>
      new SQLiteCoreDriver({ ...config, type: "sqlite", filePath: ":memory:" }),
  ],
  ["PostgreSQL", () => new PostgresDriver({ ...config, type: "pg" })],
  ["MySQL", () => new MySQLDriver({ ...config, type: "mysql" })],
  ["Oracle", () => new OracleDriver({ ...config, type: "oracle" })],
  ["MSSQL", () => new MSSQLDriver({ ...config, type: "mssql" })],
] as const;

function column(
  category: ColumnTypeMeta["category"] = "text",
  nativeType = "text",
): ColumnTypeMeta {
  return {
    name: "value",
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: resolveFilterOperators(category, {
      filterable: true,
      nullable: true,
    }),
    valueSemantics: "plain",
  };
}

const values = [
  "%",
  "_",
  "\\",
  "!",
  String.raw`!%_\!_%%\\`,
  "ordinary string",
  "雪😀",
  "x' OR 1=1 --",
  "2026-10-05T10:20:30Z",
];

describe.each(drivers)("H1 %s active SQL table read", (name, createDriver) => {
  it.each(
    values,
  )("keeps Contains input %j literal in both count and data queries", async (value) => {
    const driver = createDriver();
    const meta = column();
    expect(defaultFilterOperator(meta)).toBe("like");
    expect(meta.filterOperators).toContain("like");
    vi.spyOn(driver, "describeColumns").mockResolvedValue([meta]);
    const query = vi.spyOn(driver, "query").mockResolvedValue({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    });
    const service = new TableReadService({
      getConnection: () => config,
      getDriver: () => driver,
    } as never);
    await service.getPage(config.id, "db", "main", "items", 1, 25, [
      { column: "value", operator: "like", value },
    ]);
    expect(query).toHaveBeenCalledTimes(2);
    for (const [sql, params] of query.mock.calls) {
      if (name === "MSSQL") {
        expect(sql).toContain("CHARINDEX(");
        expect(params?.[0]).toBe(value);
      } else {
        expect(sql).toContain("ESCAPE '!'");
        const pattern =
          name === "MySQL"
            ? Buffer.from(String(params?.[0]), "hex").toString("utf8")
            : params?.[0];
        expect(pattern).toBe(literalContainsPattern(value));
        if (value.length > 1) expect(sql).not.toContain(value);
      }
    }
  });

  it.each([
    "text",
    "json",
    "array",
    "enum",
    "uuid",
    "other",
  ] as const)("escapes advertised %s searches and ilike without changing placeholder indices", (category) => {
    const driver = createDriver();
    const meta = column(
      category,
      category === "array" && name === "PostgreSQL" ? "text[]" : category,
    );
    const value = String.raw`%_\!`;
    for (const operator of ["like", "ilike"] as const) {
      const result = driver.buildFilterCondition(meta, operator, value, 7);
      expect(result).not.toBeNull();
      const param = result?.params[0];
      expect(
        name === "MySQL"
          ? Buffer.from(String(param), "hex").toString("utf8")
          : param,
      ).toBe(name === "MSSQL" ? value : literalContainsPattern(value));
      if (name === "PostgreSQL") expect(result?.sql).toContain("$7 ESCAPE '!'");
      if (name === "Oracle") expect(result?.sql).toContain(":7) ESCAPE '!'");
    }
  });
});

describe("H1 shared builders and dialect edge cases", () => {
  it("escapes %, _, and the chosen escape character, but not backslash", () => {
    expect(literalContainsPattern("!%_\\")).toBe("%!!!%!_\\%");
  });

  it.each([
    "text",
    "array",
  ] as const)("keeps BaseDBDriver and standalone FilterBuilder %s Contains consistent", (category) => {
    const driver = new MySQLDriver({ ...config, type: "mysql" });
    const meta = column(category);
    const value = "!%_\\";
    const expected = {
      sql: "CAST(`value` AS CHAR) LIKE ? ESCAPE '!'",
      params: [literalContainsPattern(value)],
    };
    expect(
      BaseDBDriver.prototype.buildFilterCondition.call(
        driver,
        meta,
        "like",
        value,
        1,
      ),
    ).toEqual(expected);
    expect(
      new FilterBuilder().buildFilterCondition(meta, "like", value, 1, (id) =>
        driver.quoteIdentifier(id),
      ),
    ).toEqual(expected);
  });

  it.each([
    "eq",
    "neq",
  ] as const)("treats percent, underscore, and the escape character literally for BaseDBDriver %s filters", (operator) => {
    const driver = new MySQLDriver({ ...config, type: "mysql" });
    const value = "!%_";
    expect(
      BaseDBDriver.prototype.buildFilterCondition.call(
        driver,
        column(),
        operator,
        value,
        1,
      ),
    ).toEqual({
      sql: `CAST(${driver.quoteIdentifier("value")} AS CHAR) ${operator === "eq" ? "LIKE" : "NOT LIKE"} ? ESCAPE '!'`,
      params: [literalContainsPattern(value)],
    });
  });

  it("keeps text IN values unchanged, including %, _, and backslashes", () => {
    for (const [, createDriver] of drivers) {
      const meta = column();
      meta.filterOperators.push("in");
      const result = buildWhere(
        createDriver() as IDBDriver,
        [{ column: "value", operator: "in", value: "%, _, \\" }],
        [meta],
      );
      expect(result.params).toEqual(["%", "_", "\\"]);
      expect(result.clause).not.toContain("ESCAPE");
    }
  });

  it.each([
    "CLOB",
    "NCLOB",
    "XMLTYPE",
  ])("retains Oracle %s full-LOB search without truncating or casting to VARCHAR", (nativeType) => {
    const driver = new OracleDriver({ ...config, type: "oracle" });
    const meta = column("text", nativeType);
    const value = `<r>${"a".repeat(5000)}%_\\!</r>`;
    const result = buildWhere(
      driver,
      [{ column: "value", operator: "like", value }],
      [meta],
    );
    expect(result.clause).toContain("ESCAPE '!'");
    expect(result.clause).not.toMatch(/SUBSTR|VARCHAR/i);
    expect(result.params).toEqual([literalContainsPattern(value)]);
  });

  it("transports MySQL Contains parameters independently of NO_BACKSLASH_ESCAPES", () => {
    const driver = new MySQLDriver({ ...config, type: "mysql" });
    const value = String.raw`雪😀\' OR 1=1 -- %_!`;
    const result = buildWhere(
      driver,
      [{ column: "value", operator: "like", value }],
      [column()],
    );
    expect(result.clause).toContain("LIKE CAST(UNHEX(?) AS CHAR) ESCAPE '!'");
    expect(String(result.params[0])).toMatch(/^[0-9a-f]+$/);
    // Exercise mysql2's real text-protocol formatter: no backslash escapes or
    // user-controlled SQL quotes can reach either SQL-mode parser.
    const formatted = mysql.format(result.clause, result.params.map(String));
    expect(formatted).not.toContain("\\");
    expect(formatted).not.toContain("OR 1=1");
    expect(Buffer.from(String(result.params[0]), "hex").toString("utf8")).toBe(
      literalContainsPattern(value),
    );
  });
});
