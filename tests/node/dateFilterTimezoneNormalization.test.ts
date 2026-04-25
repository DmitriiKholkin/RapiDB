import { describe, expect, it } from "vitest";
import {
  hasExplicitTimezone,
  normalizeSqlDatetimeOffsetSpacing,
} from "../../src/extension/dbDrivers/BaseDBDriver";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const sqliteDriver = new SQLiteDriver({
  id: "date-filter-timezone-test",
  name: "Date Filter Timezone Test",
  type: "sqlite",
  filePath: ":memory:",
} as ConnectionConfig);

const postgresDriver = new PostgresDriver({
  id: "date-filter-timezone-test-pg",
  name: "Date Filter Timezone Test PG",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
} as ConnectionConfig);

const dateColumn: ColumnTypeMeta = {
  name: "created_at",
  type: "date",
  nativeType: "date",
  category: "date",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  isAutoIncrement: false,
  filterable: true,
  filterOperators: ["eq", "like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

const datetimeColumn: ColumnTypeMeta = {
  name: "created_at",
  type: "timestamp with time zone",
  nativeType: "timestamp with time zone",
  category: "datetime",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  isAutoIncrement: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

describe("date filter timezone normalization", () => {
  it("treats +HH and +HHMM as explicit timezone", () => {
    expect(hasExplicitTimezone("2019-07-24 22:24:19.395+00")).toBe(true);
    expect(hasExplicitTimezone("2019-07-24 22:24:19.395+0000")).toBe(true);
    expect(hasExplicitTimezone("2019-07-24 22:24:19.395+00:00")).toBe(true);
  });

  it("normalizes SQL datetime suffixes to +HH:MM", () => {
    expect(
      normalizeSqlDatetimeOffsetSpacing("2019-07-24 22:24:19.395 +00"),
    ).toBe("2019-07-24 22:24:19.395+00:00");
    expect(
      normalizeSqlDatetimeOffsetSpacing("2019-07-24 22:24:19.395+0000"),
    ).toBe("2019-07-24 22:24:19.395+00:00");
  });

  it("normalizes date filter value when datetime has +HH timezone", () => {
    const normalized = sqliteDriver.normalizeFilterValue(
      dateColumn,
      "eq",
      "2019-07-24 22:24:19.395+00",
    );

    expect(normalized).toBe("2019-07-24");
  });

  it("builds PostgreSQL LIKE filter for date from copied datetime+timezone", () => {
    const condition = postgresDriver.buildFilterCondition(
      dateColumn,
      "like",
      "2019-07-24 22:24:19.395+00",
      1,
    );

    expect(condition).toEqual({
      sql: 'CAST("created_at" AS TEXT) ILIKE $1',
      params: ["%2019-07-24%"],
    });
  });

  it("builds PostgreSQL LIKE filter for datetime from copied +00 value", () => {
    const condition = postgresDriver.buildFilterCondition(
      datetimeColumn,
      "like",
      "2016-12-21 16:50:38.528+00",
      1,
    );

    expect(condition).toEqual({
      sql: 'CAST("created_at" AS TEXT) ILIKE $1',
      params: ["%2016-12-21%16:50:38.528%"],
    });
  });
});
