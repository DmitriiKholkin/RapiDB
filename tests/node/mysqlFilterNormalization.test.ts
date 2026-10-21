import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const driver = new MySQLDriver({
  id: "mysql-filter-normalization",
  name: "MySQL Filter Normalization",
  type: "mysql",
  host: "127.0.0.1",
  port: 3306,
  database: "test",
  username: "root",
  password: "root",
} as ConnectionConfig);

function buildColumn(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    defaultValue: undefined,
    isPrimaryKey: false,
    primaryKeyOrdinal: undefined,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

describe("mysql filter normalization", () => {
  it("preserves exact decimal precision in equality filters", () => {
    const condition = driver.buildFilterCondition(
      buildColumn("amount", "decimal(18,10)", "decimal"),
      "eq",
      "9999999.1234567890",
      1,
    );

    expect(condition).toEqual({
      sql: "`amount` = ?",
      params: ["9999999.1234567890"],
    });
  });

  it("matches text operands using the column charset and collation", () => {
    const varcharColumn = {
      ...buildColumn("label", "varchar(80)", "text"),
      characterSet: "utf8mb4",
      collation: "utf8mb4_0900_ai_ci",
    };

    expect(
      driver.buildFilterCondition(varcharColumn, "like", "Привет", 1),
    ).toEqual({
      sql: "`label` LIKE CONVERT(CONVERT(UNHEX(?) USING utf8mb4) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci ESCAPE '!'",
      params: [Buffer.from("%Привет%", "utf8").toString("hex")],
    });
  });

  it("transcodes UTF-8 filter values to the column charset", () => {
    const latin1Column = {
      ...buildColumn("label", "varchar(80)", "text"),
      characterSet: "latin1",
      collation: "latin1_swedish_ci",
    };

    expect(
      driver.buildFilterCondition(latin1Column, "like", "café", 1),
    ).toEqual({
      sql: "(`label` LIKE CONVERT(CONVERT(UNHEX(?) USING utf8mb4) USING latin1) COLLATE latin1_swedish_ci ESCAPE '!' AND CAST(CONVERT(CONVERT(CONVERT(UNHEX(?) USING utf8mb4) USING latin1) USING utf8mb4) AS BINARY) = UNHEX(?))",
      params: Array(3).fill(Buffer.from("%café%", "utf8").toString("hex")),
    });
  });

  it("uses catalog charset for collations without a charset prefix", () => {
    const mariaDbColumn = {
      ...buildColumn("label", "varchar(80)", "text"),
      characterSet: "utf8mb4",
      collation: "uca1400_ai_ci",
    };

    expect(
      driver.buildFilterCondition(mariaDbColumn, "like", "needle", 1),
    ).toEqual({
      sql: "`label` LIKE CONVERT(CONVERT(UNHEX(?) USING utf8mb4) USING utf8mb4) COLLATE uca1400_ai_ci ESCAPE '!'",
      params: [Buffer.from("%needle%", "utf8").toString("hex")],
    });
  });

  it("does not interpolate unsupported MySQL collation tokens", () => {
    const varcharColumn = {
      ...buildColumn("label", "varchar(80)", "text"),
      characterSet: "utf8mb4",
      collation: "utf8mb4_0900_ai_ci` OR 1=1 --",
    };

    expect(
      driver.buildFilterCondition(varcharColumn, "like", "needle", 1),
    ).toEqual({
      sql: "`label` LIKE CAST(UNHEX(?) AS CHAR) ESCAPE '!'",
      params: [Buffer.from("%needle%", "utf8").toString("hex")],
    });
  });

  it("uses text contains matching for valid JSON filter input", () => {
    const condition = driver.buildFilterCondition(
      buildColumn("payload", "json", "json"),
      "like",
      '{"arr":[1,2,3],"key":"value","num":42,"bool":true,"nested":{"a":1}}',
      1,
    );

    expect(condition).toEqual({
      sql: "CAST(`payload` AS CHAR) LIKE CAST(UNHEX(?) AS CHAR) ESCAPE '!'",
      params: [
        Buffer.from(
          '%{"arr":[1,2,3],"key":"value","num":42,"bool":true,"nested":{"a":1}}%',
        ).toString("hex"),
      ],
    });
  });

  it("formats MySQL spatial values as WKT", () => {
    const spatialColumn = buildColumn("geom", "geometry", "spatial");

    expect(
      driver.formatOutputValue({ x: 55.7558, y: 37.6171 }, spatialColumn),
    ).toBe("POINT(55.7558 37.6171)");
    expect(
      driver.formatOutputValue(
        [
          [
            { x: 0, y: 0 },
            { x: 4, y: 0 },
            { x: 4, y: 4 },
            { x: 0, y: 4 },
            { x: 0, y: 0 },
          ],
        ],
        buildColumn("shape", "polygon", "spatial"),
      ),
    ).toBe("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))");
  });

  it("formats MySQL boolean values as numeric 0 or 1", () => {
    const booleanColumn = {
      ...buildColumn("is_active", "tinyint(1)", "boolean"),
      valueSemantics: "boolean" as const,
    };
    const parsed = (
      driver as unknown as {
        _parseQueryResult: (
          rawRows: unknown[][],
          fields: Array<{
            name: string;
            type?: number;
            columnType?: number;
            length?: number;
            columnLength?: number;
          }>,
          executionTimeMs: number,
        ) => { rows: Array<Record<string, unknown>> };
      }
    )._parseQueryResult(
      [[true], [false], [1], [0]],
      [
        {
          name: booleanColumn.name,
          type: 1,
          columnType: 1,
          length: 1,
          columnLength: 1,
        },
      ],
      0,
    );

    expect(parsed.rows.map((row) => row.__col_0)).toEqual([1, 0, 1, 0]);
  });

  it("rejects legacy JSON spatial input", () => {
    const spatialColumn = buildColumn("geom", "geometry", "spatial");

    expect(() =>
      driver.coerceInputValue('{"x":55.7558,"y":37.6171}', spatialColumn),
    ).toThrow(/expects WKT text/);
  });

  it("builds spatial filters from WKT text", () => {
    const spatialColumn = buildColumn("geom", "geometry", "spatial");

    expect(
      driver.buildFilterCondition(
        spatialColumn,
        "eq",
        "POINT(55.7558 37.6171)",
        1,
      ),
    ).toEqual({
      sql: "ST_Equals(`geom`, ST_GeomFromText(?, ST_SRID(`geom`))) = 1",
      params: ["POINT(55.7558 37.6171)"],
    });
  });

  it("builds boolean filters from numeric text", () => {
    const booleanColumn = {
      ...buildColumn("is_active", "tinyint(1)", "boolean"),
      valueSemantics: "boolean" as const,
    };

    expect(driver.buildFilterCondition(booleanColumn, "eq", "1", 1)).toEqual({
      sql: "`is_active` = ?",
      params: [1],
    });
    expect(driver.buildFilterCondition(booleanColumn, "neq", "0", 1)).toEqual({
      sql: "`is_active` != ?",
      params: [0],
    });
  });

  it("rejects true/false boolean filter values for MySQL", () => {
    const booleanColumn = {
      ...buildColumn("is_active", "tinyint(1)", "boolean"),
      valueSemantics: "boolean" as const,
    };

    expect(() =>
      driver.normalizeFilterValue(booleanColumn, "eq", "true"),
    ).toThrow(/0 or 1/);
    expect(() =>
      driver.normalizeFilterValue(booleanColumn, "eq", "false"),
    ).toThrow(/0 or 1/);
  });

  it("skips strict persisted verification for temporal on-update columns", () => {
    const updatedAtColumn: ColumnTypeMeta = {
      ...buildColumn("updated_at", "timestamp(6)", "datetime"),
      onUpdateExpression: "CURRENT_TIMESTAMP(6)",
    };

    expect(
      driver.checkPersistedEdit(updatedAtColumn, "2026-05-29 13:45:36.917354", {
        persistedValue: "2026-05-29 14:57:54.510808",
      }),
    ).toEqual({ ok: true, shouldVerify: false });
  });

  it("builds spatial filters for polygon and linestring values", () => {
    const spatialColumn = buildColumn("geom", "geometry", "spatial");
    const samples = [
      "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
      "LINESTRING(0 0, 5 5, 10 0, 15 5)",
      "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))",
    ];

    for (const value of samples) {
      expect(
        driver.buildFilterCondition(spatialColumn, "eq", value, 1),
      ).toEqual({
        sql: "ST_Equals(`geom`, ST_GeomFromText(?, ST_SRID(`geom`))) = 1",
        params: [value],
      });
    }
  });

  it("uses SRID-preserving WKT in table page reads for spatial columns", async () => {
    const pageDriver = new MySQLDriver({
      id: "mysql-filter-normalization-read-page",
      name: "MySQL Filter Normalization Read Page",
      type: "mysql",
      host: "127.0.0.1",
      port: 3306,
      database: "test",
      username: "root",
      password: "root",
    } as ConnectionConfig);

    const spatialColumn: ColumnTypeMeta = {
      ...buildColumn("geometry_column", "geometry", "spatial"),
      nullable: false,
      filterOperators: ["eq", "neq"],
    };
    const idColumn: ColumnTypeMeta = {
      ...buildColumn("id", "int", "integer"),
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      filterOperators: ["eq", "neq", "gt", "gte", "lt", "lte"],
    };

    const sqlCalls: string[] = [];
    (
      pageDriver as unknown as {
        describeColumns: typeof pageDriver.describeColumns;
      }
    ).describeColumns = async () => [idColumn, spatialColumn];
    (pageDriver as unknown as { query: typeof pageDriver.query }).query =
      async (sql) => {
        sqlCalls.push(sql);
        return {
          columns: ["id", "geometry_column"],
          rows: [
            {
              __col_0: 1,
              __col_1: "MULTIPOINT((0 0),(1 1),(2 2))",
            },
          ],
          rowCount: 1,
          executionTimeMs: 0,
        };
      };

    const page = await pageDriver.readTablePage({
      database: "test",
      schema: "",
      table: "geo_table",
      page: 1,
      pageSize: 50,
      filters: [],
      sort: null,
      skipCount: true,
    });

    expect(
      sqlCalls.some((sql) =>
        /CASE WHEN ST_SRID\(`geometry_column`\) = 0 THEN ST_AsText\(`geometry_column`\) ELSE CONCAT\('SRID=', ST_SRID\(`geometry_column`\), ';', ST_AsText\(`geometry_column`\)\) END AS `__rapidb_col_\d+`/.test(
          sql,
        ),
      ),
    ).toBe(true);
    expect(page.rows[0]?.geometry_column).toBe("MULTIPOINT((0 0),(1 1),(2 2))");
  });

  it("uses a one-row lookahead when the count query fails", async () => {
    const pageDriver = new MySQLDriver({
      id: "mysql-filter-normalization-count-fallback",
      name: "MySQL Filter Normalization Count Fallback",
      type: "mysql",
      host: "127.0.0.1",
      port: 3306,
      database: "test",
      username: "root",
      password: "root",
    } as ConnectionConfig);
    const idColumn: ColumnTypeMeta = {
      ...buildColumn("id", "int", "integer"),
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      filterOperators: ["eq", "neq", "gt", "gte", "lt", "lte"],
    };
    const titleColumn = buildColumn("title", "varchar(255)", "text");
    const sqlCalls: Array<{ sql: string; params?: unknown[] }> = [];

    (
      pageDriver as unknown as {
        describeColumns: typeof pageDriver.describeColumns;
      }
    ).describeColumns = async () => [idColumn, titleColumn];
    (
      pageDriver as unknown as {
        query: typeof pageDriver.query;
      }
    ).query = async (sql, params) => {
      sqlCalls.push({ sql, params });
      if (/count\(\*\)/i.test(sql)) {
        throw new Error("Count failed");
      }
      return {
        columns: ["id", "title"],
        rows: Array.from({ length: 6 }, (_, index) => ({
          __col_0: index + 1,
          __col_1: `row-${index + 1}`,
        })),
        rowCount: 6,
        executionTimeMs: 0,
      };
    };

    const page = await pageDriver.readTablePage({
      database: "test",
      schema: "",
      table: "users",
      page: 1,
      pageSize: 5,
      filters: [],
      sort: null,
      skipCount: false,
    });

    expect(page.rows).toHaveLength(5);
    expect(page.totalCount).toBe(6);
    const dataCall = sqlCalls.find((call) => /SELECT `id`/i.test(call.sql));
    expect(dataCall?.params).toEqual([6, 0]);
  });

  it("preserves page offsets, filters, and sorting during lookahead", async () => {
    const pageDriver = new MySQLDriver({
      id: "mysql-filter-normalization-count-fallback-page-two",
      name: "MySQL Filter Normalization Count Fallback Page Two",
      type: "mysql",
      host: "127.0.0.1",
      port: 3306,
      database: "test",
      username: "root",
      password: "root",
    } as ConnectionConfig);
    const idColumn: ColumnTypeMeta = {
      ...buildColumn("id", "int", "integer"),
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      filterOperators: ["eq", "neq", "gt", "gte", "lt", "lte"],
    };
    const titleColumn = buildColumn("title", "varchar(255)", "text");
    const sqlCalls: Array<{ sql: string; params?: unknown[] }> = [];

    (
      pageDriver as unknown as {
        describeColumns: typeof pageDriver.describeColumns;
      }
    ).describeColumns = async () => [idColumn, titleColumn];
    (
      pageDriver as unknown as {
        query: typeof pageDriver.query;
      }
    ).query = async (sql, params) => {
      sqlCalls.push({ sql, params });
      if (/count\(\*\)/i.test(sql)) {
        throw new Error("Count failed");
      }
      return {
        columns: ["id", "title"],
        rows: Array.from({ length: 6 }, (_, index) => ({
          __col_0: index + 6,
          __col_1: `row-${index + 6}`,
        })),
        rowCount: 6,
        executionTimeMs: 0,
      };
    };

    const page = await pageDriver.readTablePage({
      database: "test",
      schema: "",
      table: "users",
      page: 2,
      pageSize: 5,
      filters: [{ column: "title", operator: "like", value: "row" }],
      sort: { column: "title", direction: "desc" },
      skipCount: false,
    });

    expect(page.rows).toHaveLength(5);
    expect(page.totalCount).toBe(11);
    const dataCall = sqlCalls.find((call) => /SELECT `id`/i.test(call.sql));
    expect(dataCall?.sql).toContain(
      "WHERE `title` LIKE CAST(UNHEX(?) AS CHAR) ESCAPE '!'",
    );
    expect(dataCall?.sql).toContain("ORDER BY `test`.`users`.`title` DESC");
    expect(dataCall?.params).toEqual([
      Buffer.from("%row%").toString("hex"),
      6,
      5,
    ]);
  });

  it("does not invent a next row when count fails at the end", async () => {
    const pageDriver = new MySQLDriver({
      id: "mysql-filter-normalization-count-fallback-end",
      name: "MySQL Filter Normalization Count Fallback End",
      type: "mysql",
      host: "127.0.0.1",
      port: 3306,
      database: "test",
      username: "root",
      password: "root",
    } as ConnectionConfig);
    const idColumn: ColumnTypeMeta = {
      ...buildColumn("id", "int", "integer"),
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      filterOperators: ["eq", "neq", "gt", "gte", "lt", "lte"],
    };
    const titleColumn = buildColumn("title", "varchar(255)", "text");

    (
      pageDriver as unknown as {
        describeColumns: typeof pageDriver.describeColumns;
      }
    ).describeColumns = async () => [idColumn, titleColumn];
    (
      pageDriver as unknown as {
        query: typeof pageDriver.query;
      }
    ).query = async (sql) => {
      if (/count\(\*\)/i.test(sql)) {
        throw new Error("Count failed");
      }
      return {
        columns: ["id", "title"],
        rows: Array.from({ length: 5 }, (_, index) => ({
          __col_0: index + 1,
          __col_1: `row-${index + 1}`,
        })),
        rowCount: 5,
        executionTimeMs: 0,
      };
    };

    const page = await pageDriver.readTablePage({
      database: "test",
      schema: "",
      table: "users",
      page: 1,
      pageSize: 5,
      filters: [],
      sort: null,
      skipCount: false,
    });

    expect(page.rows).toHaveLength(5);
    expect(page.totalCount).toBe(5);
  });

  it("keeps skipCount reads to a single page", async () => {
    const pageDriver = new MySQLDriver({
      id: "mysql-filter-normalization-skip-count",
      name: "MySQL Filter Normalization Skip Count",
      type: "mysql",
      host: "127.0.0.1",
      port: 3306,
      database: "test",
      username: "root",
      password: "root",
    } as ConnectionConfig);
    const idColumn: ColumnTypeMeta = {
      ...buildColumn("id", "int", "integer"),
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      filterOperators: ["eq", "neq", "gt", "gte", "lt", "lte"],
    };
    const titleColumn = buildColumn("title", "varchar(255)", "text");
    const sqlCalls: Array<{ sql: string; params?: unknown[] }> = [];

    (
      pageDriver as unknown as {
        describeColumns: typeof pageDriver.describeColumns;
      }
    ).describeColumns = async () => [idColumn, titleColumn];
    (
      pageDriver as unknown as {
        query: typeof pageDriver.query;
      }
    ).query = async (sql, params) => {
      sqlCalls.push({ sql, params });
      return {
        columns: ["id", "title"],
        rows: Array.from({ length: 5 }, (_, index) => ({
          __col_0: index + 1,
          __col_1: `row-${index + 1}`,
        })),
        rowCount: 5,
        executionTimeMs: 0,
      };
    };

    const page = await pageDriver.readTablePage({
      database: "test",
      schema: "",
      table: "users",
      page: 2,
      pageSize: 5,
      filters: [],
      sort: null,
      skipCount: true,
    });

    expect(page.rows).toHaveLength(5);
    expect(page.totalCount).toBe(0);
    expect(sqlCalls).toHaveLength(1);
    expect(sqlCalls[0]?.params).toEqual([5, 5]);
  });
});
