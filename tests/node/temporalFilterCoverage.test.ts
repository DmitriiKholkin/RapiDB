import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnTypeMeta,
  FilterOperator,
} from "../../src/extension/dbDrivers/types";
import {
  filterOperatorsForCategory,
  resolveFilterOperators,
} from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { defaultFilterOperator } from "../../src/shared/tableTypes";

const scalarTemporalOperators: FilterOperator[] = [
  "eq",
  "neq",
  "gt",
  "gte",
  "lt",
  "lte",
  "between",
  "in",
];

const expandedTemporalOperators: FilterOperator[] = [
  ...scalarTemporalOperators,
  "is_null",
  "is_not_null",
];

function column(
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
    filterOperators: resolveFilterOperators(category, {
      filterable: true,
      nullable: true,
    }),
    valueSemantics: "plain",
  };
}

const baseConfig = {
  id: "temporal-filter-coverage",
  name: "Temporal Filter Coverage",
  host: "127.0.0.1",
  port: 0,
  database: "db",
  username: "user",
  password: "pass",
};

describe("temporal filter operator coverage", () => {
  it("exposes scalar operators by category and adds null operators only for nullable columns", () => {
    expect(filterOperatorsForCategory("date")).toEqual(scalarTemporalOperators);
    expect(filterOperatorsForCategory("time")).toEqual(scalarTemporalOperators);
    expect(filterOperatorsForCategory("datetime")).toEqual(
      scalarTemporalOperators,
    );
    expect(
      resolveFilterOperators("date", { filterable: true, nullable: true }),
    ).toEqual(expandedTemporalOperators);
    expect(
      resolveFilterOperators("time", { filterable: true, nullable: true }),
    ).toEqual(expandedTemporalOperators);
    expect(
      resolveFilterOperators("datetime", {
        filterable: true,
        nullable: true,
      }),
    ).toEqual(expandedTemporalOperators);
    expect(
      resolveFilterOperators("datetime", {
        filterable: true,
        nullable: false,
      }),
    ).toEqual(scalarTemporalOperators);
  });

  it("describes SQLite explicit temporal columns with nullable-aware scalar operators", async () => {
    const driver = new SQLiteDriver({
      ...baseConfig,
      type: "sqlite",
      filePath: ":memory:",
    } as ConnectionConfig);

    await driver.connect();

    try {
      await driver.query(
        "CREATE TABLE temporal_probe (event_time TIME NOT NULL, created_at DATETIME)",
      );

      const describedColumns = await driver.describeColumns(
        "main",
        "",
        "temporal_probe",
      );
      const eventTimeColumn = describedColumns.find(
        (describedColumn) => describedColumn.name === "event_time",
      );
      const createdAtColumn = describedColumns.find(
        (describedColumn) => describedColumn.name === "created_at",
      );

      expect(eventTimeColumn?.category).toBe("time");
      expect(eventTimeColumn?.filterOperators).toEqual(scalarTemporalOperators);
      expect(createdAtColumn?.category).toBe("datetime");
      expect(createdAtColumn?.filterOperators).toEqual(
        expandedTemporalOperators,
      );
    } finally {
      await driver.disconnect();
    }
  });

  it("uses eq as the default operator for date, time, datetime, and binary columns", () => {
    expect(defaultFilterOperator({ category: "date" })).toBe("eq");
    expect(defaultFilterOperator({ category: "time" })).toBe("eq");
    expect(defaultFilterOperator({ category: "datetime" })).toBe("eq");
    expect(defaultFilterOperator({ category: "binary" })).toBe("eq");
  });

  it("builds PostgreSQL temporal gt and in filters", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);
    const timeColumn = column("event_time", "time with time zone", "time");
    const datetimeColumn = column(
      "event_at",
      "timestamp with time zone",
      "datetime",
    );

    const gtCondition = driver.buildFilterCondition(
      timeColumn,
      "gt",
      driver.normalizeFilterValue(timeColumn, "gt", "12:34:56+00"),
      1,
    );
    const inCondition = driver.buildFilterCondition(
      datetimeColumn,
      "in",
      driver.normalizeFilterValue(
        datetimeColumn,
        "in",
        "2026-04-23 12:34:56+00, 2026-04-24 08:00:00+00",
      ),
      1,
    );

    expect(gtCondition).toEqual({
      sql: '"event_time" > $1::timetz',
      params: ["12:34:56+00"],
    });
    expect(inCondition).toEqual({
      sql: '"event_at" IN ($1::timestamptz, $2::timestamptz)',
      params: ["2026-04-23 12:34:56+00:00", "2026-04-24 08:00:00+00:00"],
    });
  });

  it("builds MySQL temporal comparison and IN filters", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
    } as ConnectionConfig);
    const timeColumn = column("event_time", "time", "time");
    const datetimeColumn = column("event_at", "datetime", "datetime");

    const gtCondition = driver.buildFilterCondition(
      timeColumn,
      "gt",
      "12:34:56",
      1,
    );
    const inCondition = driver.buildFilterCondition(
      datetimeColumn,
      "in",
      "2026-04-23 12:34:56, 2026-04-24 08:00:00",
      1,
    );

    expect(gtCondition).toEqual({
      sql: "CAST(`event_time` AS TIME) > CAST(? AS TIME)",
      params: ["12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: "CAST(`event_at` AS DATETIME) IN (CAST(? AS DATETIME), CAST(? AS DATETIME))",
      params: ["2026-04-23 12:34:56", "2026-04-24 08:00:00"],
    });
  });

  it("builds SQLite temporal comparison and IN filters", () => {
    const driver = new SQLiteDriver({
      ...baseConfig,
      type: "sqlite",
      filePath: ":memory:",
    } as ConnectionConfig);
    const timeColumn = column("event_time", "TIME", "time");
    const datetimeColumn = column("event_at", "DATETIME", "datetime");

    const gtValue = driver.normalizeFilterValue(timeColumn, "gt", "12:34:56");
    const inValue = driver.normalizeFilterValue(
      datetimeColumn,
      "in",
      "2026-04-23 12:34:56, 2026-04-24 08:00:00",
    );

    const gtCondition = driver.buildFilterCondition(
      timeColumn,
      "gt",
      gtValue,
      1,
    );
    const inCondition = driver.buildFilterCondition(
      datetimeColumn,
      "in",
      inValue,
      1,
    );

    expect(gtCondition).toEqual({
      sql: 'TIME("event_time") > TIME(?)',
      params: ["12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: 'DATETIME("event_at") IN (DATETIME(?), DATETIME(?))',
      params: ["2026-04-23 12:34:56", "2026-04-24 08:00:00"],
    });
  });

  it("builds MSSQL temporal comparison and IN filters", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);
    const timeColumn = column("event_time", "time", "time");
    const datetimeColumn = column("event_at", "datetimeoffset", "datetime");

    const gtCondition = driver.buildFilterCondition(
      timeColumn,
      "gt",
      "12:34:56",
      1,
    );
    const inCondition = driver.buildFilterCondition(
      datetimeColumn,
      "in",
      "2026-04-23 12:34:56+00, 2026-04-24 08:00:00+00",
      1,
    );

    expect(gtCondition).toEqual({
      sql: "CAST([event_time] AS time) > CAST(? AS time)",
      params: ["12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: "[event_at] IN (?, ?)",
      params: ["2026-04-23 12:34:56+00:00", "2026-04-24 08:00:00+00:00"],
    });
  });

  describe.each([
    ["TIME", "time", "12", "12:34", "12:34:00", "TIME"],
    [
      "DATETIME",
      "datetime",
      "2026",
      "2026-10-01T12:34:56",
      "2026-10-01 12:34:56",
      "DATETIME",
    ],
  ] as const)("SQLite %s equality", (nativeType, category, raw, valid, normalized, sqlFunction) => {
    const driver = new SQLiteDriver({
      ...baseConfig,
      type: "sqlite",
      filePath: ":memory:",
    } as ConnectionConfig);
    const temporalColumn = column("event", nativeType, category);

    it.each([
      "eq",
      "neq",
    ] as const)("uses exact parameterized %s for raw values and literal pattern tokens", (operator) => {
      for (const value of [
        raw,
        "raw%_value",
        "%",
        "_",
        "x' OR 1=1 --",
        "25:99:99",
      ]) {
        const normalizedValue = driver.normalizeFilterValue(
          temporalColumn,
          operator,
          value,
        );
        expect(normalizedValue).toBe(value);
        expect(
          driver.buildFilterCondition(
            temporalColumn,
            operator,
            normalizedValue,
            1,
          ),
        ).toEqual({
          sql: `"event" ${operator === "eq" ? "=" : "<>"} ?`,
          params: [value],
        });
      }
    });

    it("keeps valid temporal normalization and SQL null predicates", () => {
      for (const operator of ["eq", "neq"] as const) {
        expect(
          driver.normalizeFilterValue(temporalColumn, operator, valid),
        ).toBe(normalized);
        expect(
          driver.buildFilterCondition(temporalColumn, operator, valid, 1),
        ).toEqual({
          sql: `${sqlFunction}("event") ${operator === "eq" ? "=" : "!="} ${sqlFunction}(?)`,
          params: [normalized],
        });
      }
      expect(
        driver.buildFilterCondition(temporalColumn, "is_null", undefined, 1),
      ).toEqual({ sql: '"event" IS NULL', params: [] });
      expect(
        driver.buildFilterCondition(
          temporalColumn,
          "is_not_null",
          undefined,
          1,
        ),
      ).toEqual({ sql: '"event" IS NOT NULL', params: [] });
    });

    it("keeps Contains wildcard semantics", () => {
      const value = driver.normalizeFilterValue(
        temporalColumn,
        "like",
        "raw%_value",
      );
      expect(
        driver.buildFilterCondition(temporalColumn, "like", value, 1),
      ).toEqual({ sql: '"event" LIKE ?', params: ["%raw%_value%"] });
    });

    it("rejects invalid ranges in normalization and direct builders", () => {
      const inputs: [FilterOperator, string | [string, string]][] = [
        ["gt", raw],
        ["gte", raw],
        ["lt", raw],
        ["lte", raw],
        ["between", [valid, raw]],
        ["between", [raw, valid]],
      ];
      for (const [operator, value] of inputs) {
        expect(() =>
          driver.normalizeFilterValue(temporalColumn, operator, value),
        ).toThrow(/expects a valid/);
        expect(() =>
          driver.buildFilterCondition(temporalColumn, operator, value, 1),
        ).toThrow(/expects a valid/);
      }
    });
  });

  describe("SQLite datetime component validation", () => {
    const driver = new SQLiteDriver({
      ...baseConfig,
      type: "sqlite",
      filePath: ":memory:",
    } as ConnectionConfig);
    const datetimeColumn = column("event", "DATETIME", "datetime");

    it.each([
      "2026-00-01 12:34:56",
      "2026-13-01 12:34:56",
      "2026-13-01T12:34:56",
      "2026-10-00 12:34:56",
      "2026-10-32 12:34:56",
      "2026-04-31 12:34:56",
      "2026-02-29 12:34:56",
      "2024-02-30 12:34:56",
      "1900-02-29 12:34:56",
      "2100-02-29 12:34:56",
      "2026-10-01 12:34:56+99:99",
      "2026-10-01T12:34:56 +99:99",
      "2026-10-01 12:34:56-99:99",
      "2026-10-01 12:34:56+15:00",
      "2026-10-01 12:34:56-15:00",
      "2026-10-01 12:34:56+00:60",
      "2026-10-01 12:34:56-14:60",
    ])("uses raw equality and rejects ranges/lists for %s", (value) => {
      for (const operator of ["eq", "neq"] as const) {
        expect(
          driver.normalizeFilterValue(datetimeColumn, operator, value),
        ).toBe(value);
        expect(
          driver.buildFilterCondition(datetimeColumn, operator, value, 1),
        ).toEqual({
          sql: `"event" ${operator === "eq" ? "=" : "<>"} ?`,
          params: [value],
        });
      }
      const valid = "2026-10-01 12:34:56";
      const inputs: [FilterOperator, string | [string, string]][] = [
        ["gt", value],
        ["gte", value],
        ["lt", value],
        ["lte", value],
        ["between", [valid, value]],
        ["between", [value, valid]],
        ["in", `${valid}, ${value}`],
      ];
      for (const [operator, input] of inputs) {
        expect(() =>
          driver.normalizeFilterValue(datetimeColumn, operator, input),
        ).toThrow(/expects a valid datetime/);
        expect(() =>
          driver.buildFilterCondition(datetimeColumn, operator, input, 1),
        ).toThrow(/expects a valid datetime/);
      }
    });

    it.each([
      ["0000-02-29 00:00", "0000-02-29 00:00"],
      ["0001-01-01 00:00:00", "0001-01-01 00:00:00"],
      ["1900-02-28 23:59:59", "1900-02-28 23:59:59"],
      ["2000-02-29 12:34:56", "2000-02-29 12:34:56"],
      ["2024-02-29T12:34:56.123Z", "2024-02-29 12:34:56.123Z"],
      ["2026-04-30 12:34:56", "2026-04-30 12:34:56"],
      ["9999-12-31 23:59:59", "9999-12-31 23:59:59"],
      ["2026-10-01T12:34z", "2026-10-01 12:34z"],
      ["2026-10-01 12:34:56 Z", "2026-10-01 12:34:56 Z"],
      ["2026-10-01T12:34:56 +02:00", "2026-10-01 12:34:56+02:00"],
      ["2026-10-01 12:34:56-00:00", "2026-10-01 12:34:56-00:00"],
      ["2026-10-01 12:34:56+14:59", "2026-10-01 12:34:56+14:59"],
      ["2026-10-01 12:34:56-14:59", "2026-10-01 12:34:56-14:59"],
    ])("preserves supported datetime normalization for %s", (value, normalized) => {
      for (const operator of ["eq", "neq", "gt"] as const) {
        expect(
          driver.normalizeFilterValue(datetimeColumn, operator, value),
        ).toBe(normalized);
        expect(
          driver.buildFilterCondition(datetimeColumn, operator, value, 1),
        ).toEqual({
          sql: `DATETIME("event") ${operator === "eq" ? "=" : operator === "neq" ? "!=" : ">"} DATETIME(?)`,
          params: [normalized],
        });
      }
    });
  });

  it("matches MSSQL temporal equality against the millisecond-precision values shown in the table viewer", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);
    const timeColumn = column("event_time", "time(7)", "time");
    const datetime2Column = column("event_at", "datetime2(7)", "datetime");
    const datetimeOffsetColumn = column(
      "event_offset",
      "datetimeoffset(7)",
      "datetime",
    );

    const timeCondition = driver.buildFilterCondition(
      timeColumn,
      "eq",
      "14:30:00.123",
      1,
    );
    const datetime2Condition = driver.buildFilterCondition(
      datetime2Column,
      "eq",
      "2024-06-15 14:30:00.123",
      1,
    );
    const datetimeOffsetCondition = driver.buildFilterCondition(
      datetimeOffsetColumn,
      "eq",
      "2024-06-15 11:30:00.123 +00:00",
      1,
    );

    expect(timeCondition).toEqual({
      sql: "DATEDIFF(millisecond, CAST(? AS time), CAST([event_time] AS time)) BETWEEN 0 AND ?",
      params: ["14:30:00.123", 0],
    });
    expect(datetime2Condition).toEqual({
      sql: "DATEDIFF(millisecond, CAST(? AS datetime2(7)), [event_at]) BETWEEN 0 AND ?",
      params: ["2024-06-15 14:30:00.123", 0],
    });
    expect(datetimeOffsetCondition).toEqual({
      sql: "DATEDIFF(millisecond, CAST(? AS datetimeoffset(7)), [event_offset]) BETWEEN 0 AND ?",
      params: ["2024-06-15 11:30:00.123+00:00", 0],
    });
  });

  it("builds Oracle temporal comparison and IN filters", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const datetimeColumn = column(
      "event_at",
      "TIMESTAMP WITH TIME ZONE",
      "datetime",
    );

    const gtCondition = driver.buildFilterCondition(
      datetimeColumn,
      "gt",
      "2026-04-23 12:34:56+00:00",
      1,
    );
    const inCondition = driver.buildFilterCondition(
      datetimeColumn,
      "in",
      "2026-04-23 12:34:56+00:00, 2026-04-24 08:00:00+00:00",
      1,
    );

    expect(gtCondition).toEqual({
      sql: `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST("event_at" AS TIMESTAMP WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF6'), '0'), '.') > :1`,
      params: ["2026-04-23 12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST("event_at" AS TIMESTAMP WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF6'), '0'), '.') IN (:1, :2)`,
      params: ["2026-04-23 12:34:56", "2026-04-24 08:00:00"],
    });
  });
});
