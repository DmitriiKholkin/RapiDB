import { describe, expect, it } from "vitest";
import {
  formatDatetimeForDisplay,
  hasExplicitTimezone,
  isoToLocalDateStr,
  normalizeSqlDatetimeOffsetSpacing,
} from "../../src/extension/dbDrivers/BaseDBDriver";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { isoToLocalDateStr as isoToSharedDateStr } from "../../src/extension/utils/dateUtils";
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
const mysqlDriver = new MySQLDriver({
  id: "date-filter-timezone-test-mysql",
  name: "Date Filter Timezone Test MySQL",
  type: "mysql",
} as ConnectionConfig);
const mssqlDriver = new MSSQLDriver({
  id: "date-filter-timezone-test-mssql",
  name: "Date Filter Timezone Test MSSQL",
  type: "mssql",
} as ConnectionConfig);
const oracleDriver = new OracleDriver({
  id: "date-filter-timezone-test-oracle",
  name: "Date Filter Timezone Test Oracle",
  type: "oracle",
} as ConnectionConfig);

const dateColumn: ColumnTypeMeta = {
  name: "created_at",
  type: "date",
  nativeType: "date",
  category: "date",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
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
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

describe("date filter timezone normalization", () => {
  const sqlDrivers = [
    ["SQLite", sqliteDriver],
    ["PostgreSQL", postgresDriver],
    ["MySQL", mysqlDriver],
    ["MSSQL", mssqlDriver],
  ] as const;

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

  it("formats ISO datetime strings with T separator into the shared display form", () => {
    expect(formatDatetimeForDisplay("2026-04-01T09:00:00Z")).toBe(
      "2026-04-01 09:00:00Z",
    );
    expect(formatDatetimeForDisplay("2026-04-01T09:00:00.123456+0000")).toBe(
      "2026-04-01 09:00:00.123456+00:00",
    );
  });

  it("preserves microsecond precision in timestamp display", () => {
    expect(formatDatetimeForDisplay("2026-05-28T13:08:29.999999+00:00")).toBe(
      "2026-05-28 13:08:29.999999+00:00",
    );
    expect(formatDatetimeForDisplay("2026-05-28 13:08:29.123456+00:00")).toBe(
      "2026-05-28 13:08:29.123456+00:00",
    );
  });

  it("handles mixed precision fractional seconds", () => {
    expect(formatDatetimeForDisplay("2026-05-28 13:08:29.123+00:00")).toBe(
      "2026-05-28 13:08:29.123+00:00",
    );
    expect(formatDatetimeForDisplay("2026-05-28 13:08:29.1234+00:00")).toBe(
      "2026-05-28 13:08:29.1234+00:00",
    );
    expect(formatDatetimeForDisplay("2026-05-28 13:08:29.12345+00:00")).toBe(
      "2026-05-28 13:08:29.12345+00:00",
    );
  });

  it("strips trailing zeros from fractional seconds", () => {
    expect(formatDatetimeForDisplay("2026-05-28 13:08:29.100000+00:00")).toBe(
      "2026-05-28 13:08:29.1+00:00",
    );
    expect(formatDatetimeForDisplay("2026-05-28 13:08:29.120000+00:00")).toBe(
      "2026-05-28 13:08:29.12+00:00",
    );
  });

  it("normalizes date filter value when datetime has +HH timezone", () => {
    const normalized = sqliteDriver.normalizeFilterValue(
      dateColumn,
      "eq",
      "2019-07-24 22:24:19.395+00",
    );

    expect(normalized).toBe("2019-07-24");
  });

  it.each([
    ["+HH", "2024-03-10T00:30:00+14", "2024-03-09"],
    ["+HHMM", "2024-03-10T00:30:00+1400", "2024-03-09"],
    ["+HH:MM", "2024-03-10T00:30:00+14:00", "2024-03-09"],
    ["DST boundary", "2024-03-10T23:30:00-04:00", "2024-03-11"],
  ])("normalizes %s offsets consistently for each SQL driver's DATE filter", (_label, value, expected) => {
    expect(isoToLocalDateStr(value)).toBe(expected);
    expect(isoToSharedDateStr(value)).toBe(expected);
    for (const [dialect, driver] of sqlDrivers) {
      expect(
        driver.normalizeFilterValue(dateColumn, "eq", value),
        dialect,
      ).toBe(expected);
    }
  });

  it("keeps timezone-free wall dates stable across host TZ and DST", () => {
    const originalTz = process.env.TZ;
    try {
      for (const timezone of [
        "UTC",
        "Pacific/Kiritimati",
        "America/Los_Angeles",
        "America/New_York",
      ]) {
        process.env.TZ = timezone;
        const wallDatetime = "2024-03-10T02:30:00";
        expect(isoToLocalDateStr(wallDatetime), timezone).toBe("2024-03-10");
        expect(isoToSharedDateStr(wallDatetime), timezone).toBe("2024-03-10");
        for (const [dialect, driver] of sqlDrivers) {
          expect(
            driver.normalizeFilterValue(dateColumn, "eq", wallDatetime),
            `${dialect} in ${timezone}`,
          ).toBe("2024-03-10");
        }
        expect(
          postgresDriver.coerceInputValue(wallDatetime, dateColumn),
          timezone,
        ).toBe("2024-03-10");
        expect(isoToLocalDateStr("2024-03-10T23:30:00-04:00"), timezone).toBe(
          "2024-03-11",
        );
      }
    } finally {
      if (originalTz === undefined) {
        delete process.env.TZ;
      } else {
        process.env.TZ = originalTz;
      }
    }
  });

  it("preserves Oracle DATE wall time while accepting all numeric offset forms", () => {
    for (const suffix of ["+14", "+1400", "+14:00", " +1400"]) {
      const input = `2024-03-10T00:30:00${suffix}`;
      const value = oracleDriver.normalizeFilterValue(dateColumn, "eq", input);
      expect(value).toBe("2024-03-10 00:30:00");
      expect(
        oracleDriver.buildFilterCondition(dateColumn, "eq", value, 1)?.params,
      ).toEqual(["2024-03-10 00:30:00"]);
    }
    expect(
      oracleDriver.normalizeFilterValue(
        dateColumn,
        "eq",
        "2024-03-10T02:30:00",
      ),
    ).toBe("2024-03-10 02:30:00");
  });

  it("normalizes compact Oracle offsets for comparisons, ranges, IN and temporal binds", () => {
    for (const suffix of ["Z", "z"]) {
      expect(
        oracleDriver.buildFilterCondition(
          datetimeColumn,
          "eq",
          `2024-03-10T00:30:00${suffix}`,
          1,
        )?.params,
      ).toEqual(["2024-03-10 00:30:00"]);
    }
    for (const [nativeType, category] of [
      ["DATE", "date"],
      ["TIMESTAMP(9)", "datetime"],
      ["TIMESTAMP(9) WITH TIME ZONE", "datetime"],
      ["TIMESTAMP(9) WITH LOCAL TIME ZONE", "datetime"],
    ] as const) {
      const meta = { ...datetimeColumn, nativeType, category };
      for (const [compact, canonical] of [
        ["+1400", "+14:00"],
        ["-0330", "-03:30"],
        ["+0545", "+05:45"],
        ["+0000", "+00:00"],
      ]) {
        const raw = `2024-03-10T00:30:00.123456789${compact}`;
        const full = `2024-03-10T00:30:00.123456789${canonical}`;
        for (const op of [
          "eq",
          "neq",
          "gt",
          "gte",
          "lt",
          "lte",
          "in",
          "between",
        ] as const) {
          const input =
            op === "between"
              ? ([raw, raw] as [string, string])
              : op === "in"
                ? `${raw},${raw}`
                : raw;
          const expectedInput =
            op === "between"
              ? ([full, full] as [string, string])
              : op === "in"
                ? `${full},${full}`
                : full;
          const normalized = oracleDriver.normalizeFilterValue(meta, op, input);
          const expected = oracleDriver.normalizeFilterValue(
            meta,
            op,
            expectedInput,
          );
          expect(normalized).toEqual(expected);
          expect(
            oracleDriver.buildFilterCondition(meta, op, normalized, 1),
          ).toEqual(oracleDriver.buildFilterCondition(meta, op, expected, 1));
          expect(oracleDriver.buildFilterCondition(meta, op, input, 1)).toEqual(
            oracleDriver.buildFilterCondition(meta, op, expectedInput, 1),
          );
        }
        expect(oracleDriver.coerceInputValue(raw, meta)).toBe(
          oracleDriver.coerceInputValue(full, meta),
        );
      }
      for (const offset of [
        "+1460",
        "+2400",
        "+9999",
        "+123",
        "+1:30",
        "+12345",
      ]) {
        expect(() =>
          oracleDriver.normalizeFilterValue(
            meta,
            "eq",
            `2024-03-10T00:30:00${offset}`,
          ),
        ).toThrow("valid Oracle temporal value");
      }
    }
    expect(
      oracleDriver.buildFilterCondition(
        datetimeColumn,
        "eq",
        "2024-03-10T00:30:00.123456789+1400",
        1,
      )?.params,
    ).toEqual(["2024-03-09 10:30:00.123456789"]);
    expect(
      oracleDriver.buildFilterCondition(
        datetimeColumn,
        "eq",
        "2024-03-10T00:30:00.123456789-0330",
        1,
      )?.params,
    ).toEqual(["2024-03-10 04:00:00.123456789"]);
  });

  it("builds PostgreSQL LIKE filter for date from copied datetime+timezone", () => {
    const condition = postgresDriver.buildFilterCondition(
      dateColumn,
      "like",
      "2019-07-24 22:24:19.395+00",
      1,
    );

    expect(condition).toEqual({
      sql: `CAST("created_at" AS TEXT) ILIKE $1 ESCAPE '!'`,
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
      sql: `CAST("created_at" AS TEXT) ILIKE $1 ESCAPE '!'`,
      params: ["%2016-12-21%16:50:38.528%"],
    });
  });
});
