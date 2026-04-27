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
import { filterOperatorsForCategory } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { defaultFilterOperator } from "../../src/shared/tableTypes";

const expandedTemporalOperators: FilterOperator[] = [
  "eq",
  "neq",
  "gt",
  "gte",
  "lt",
  "lte",
  "between",
  "in",
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
    isAutoIncrement: false,
    filterable: true,
    filterOperators: filterOperatorsForCategory(category),
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
  it("exposes numeric-like operators for date, time, and datetime categories", () => {
    expect(filterOperatorsForCategory("date")).toEqual(
      expandedTemporalOperators,
    );
    expect(filterOperatorsForCategory("time")).toEqual(
      expandedTemporalOperators,
    );
    expect(filterOperatorsForCategory("datetime")).toEqual(
      expandedTemporalOperators,
    );
  });

  it("uses eq as the default operator for date, time, and datetime columns", () => {
    expect(defaultFilterOperator({ category: "date" })).toBe("eq");
    expect(defaultFilterOperator({ category: "time" })).toBe("eq");
    expect(defaultFilterOperator({ category: "datetime" })).toBe("eq");
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
      sql: `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST("event_at" AS TIMESTAMP WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF3'), '0'), '.') > :1`,
      params: ["2026-04-23 12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST("event_at" AS TIMESTAMP WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF3'), '0'), '.') IN (:1, :2)`,
      params: ["2026-04-23 12:34:56", "2026-04-24 08:00:00"],
    });
  });
});
