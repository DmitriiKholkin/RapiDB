import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  FilterOperator,
} from "../../src/extension/dbDrivers/types";
import {
  filterOperatorsForCategory,
  resolveFilterOperators,
} from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";
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
  it("classifies Oracle inconsistent datatypes as a filter error", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
    } as ConnectionConfig);
    const isTableFilterError = driver.getCapabilities().isTableFilterError;

    expect(
      isTableFilterError?.(
        "ORA-00932: inconsistent datatypes: expected CHAR got RAPIDB.OBJECT_TYPE",
      ),
    ).toBe(true);
    expect(
      isTableFilterError?.("ORA-00942: table or view does not exist"),
    ).toBe(false);
  });

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
      sql: "`event_time` > CAST(? AS TIME(6))",
      params: ["12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: "`event_at` IN (?, ?)",
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
      sql: 'COALESCE(RAPIDB_TIME_KEY("event_time"), RAPIDB_TIME_KEY(TIME("event_time"))) > RAPIDB_TIME_KEY(?)',
      params: ["12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: 'COALESCE(RAPIDB_DATETIME_KEY("event_at"), RAPIDB_DATETIME_KEY(DATETIME("event_at"))) IN (RAPIDB_DATETIME_KEY(?), RAPIDB_DATETIME_KEY(?))',
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

  it("casts SQL Server date filter inputs so invalid values raise filter errors", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);
    const dateColumn = column("event_date", "date", "date");
    const invalidDate = "not-a-date";

    expect(
      driver.buildFilterCondition(dateColumn, "eq", invalidDate, 1),
    ).toEqual({
      sql: "[event_date] = CAST(? AS date)",
      params: [invalidDate],
    });
    expect(
      driver.buildFilterCondition(
        dateColumn,
        "between",
        ["2024-02-29", invalidDate],
        1,
      ),
    ).toEqual({
      sql: "[event_date] BETWEEN CAST(? AS date) AND CAST(? AS date)",
      params: ["2024-02-29", invalidDate],
    });
    expect(
      driver
        .getCapabilities()
        .isTableFilterError?.(
          "Conversion failed when converting date and/or time from character string.",
        ),
    ).toBe(true);
  });

  describe.each([
    ["TIME", "time", "12", "12:34", "12:34:00"],
    [
      "DATETIME",
      "datetime",
      "2026",
      "2026-10-01T12:34:56",
      "2026-10-01 12:34:56",
    ],
  ] as const)("SQLite %s equality", (nativeType, category, raw, valid, normalized) => {
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
          sql:
            category === "datetime"
              ? `COALESCE(RAPIDB_DATETIME_KEY("event"), RAPIDB_DATETIME_KEY(DATETIME("event"))) ${operator === "eq" ? "=" : "!="} RAPIDB_DATETIME_KEY(?)`
              : `COALESCE(RAPIDB_TIME_KEY("event"), RAPIDB_TIME_KEY(TIME("event"))) ${operator === "eq" ? "=" : "!="} RAPIDB_TIME_KEY(?)`,
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

    it("keeps Contains input literal", () => {
      const value = driver.normalizeFilterValue(
        temporalColumn,
        "like",
        "raw%_value",
      );
      expect(
        driver.buildFilterCondition(temporalColumn, "like", value, 1),
      ).toEqual({
        sql: `"event" LIKE ? ESCAPE '!'`,
        params: ["%raw!%!_value%"],
      });
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

    it("uses precision-preserving keys for datetime comparisons", () => {
      const fractionalThreshold = "2024-02-29 12:34:56.123455";
      const eventKey =
        'COALESCE(RAPIDB_DATETIME_KEY("event"), RAPIDB_DATETIME_KEY(DATETIME("event")))';
      expect(
        driver.buildFilterCondition(
          datetimeColumn,
          "gt",
          fractionalThreshold,
          1,
        ),
      ).toEqual({
        sql: `${eventKey} > RAPIDB_DATETIME_KEY(?)`,
        params: [fractionalThreshold],
      });
      expect(
        driver.buildFilterCondition(
          datetimeColumn,
          "between",
          [fractionalThreshold, "2024-02-29 12:34:56.123457"],
          1,
        ),
      ).toEqual({
        sql: `${eventKey} BETWEEN RAPIDB_DATETIME_KEY(?) AND RAPIDB_DATETIME_KEY(?)`,
        params: [fractionalThreshold, "2024-02-29 12:34:56.123457"],
      });
      expect(
        driver.buildFilterCondition(
          datetimeColumn,
          "in",
          "2024-02-29 12:34:56.123455, 2024-02-29 12:34:56.123456",
          1,
        ),
      ).toEqual({
        sql: `${eventKey} IN (RAPIDB_DATETIME_KEY(?), RAPIDB_DATETIME_KEY(?))`,
        params: [fractionalThreshold, "2024-02-29 12:34:56.123456"],
      });
    });

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
          sql: `COALESCE(RAPIDB_DATETIME_KEY("event"), RAPIDB_DATETIME_KEY(DATETIME("event"))) ${operator === "eq" ? "=" : operator === "neq" ? "!=" : ">"} RAPIDB_DATETIME_KEY(?)`,
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
      sql: "[event_at] >= CAST(? AS datetime2(7)) AND [event_at] <= DATEADD(nanosecond, ?, CAST(? AS datetime2(7)))",
      params: ["2024-06-15 14:30:00.123", 999_900, "2024-06-15 14:30:00.123"],
    });
    expect(datetimeOffsetCondition).toEqual({
      sql: "[event_offset] >= CAST(? AS datetimeoffset(7)) AND [event_offset] <= DATEADD(nanosecond, ?, CAST(? AS datetimeoffset(7)))",
      params: [
        "2024-06-15 11:30:00.123+00:00",
        999_900,
        "2024-06-15 11:30:00.123+00:00",
      ],
    });
  });

  it("builds MSSQL datetime2 and datetimeoffset equality as a bounded range instead of an overflowing millisecond DATEDIFF", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);
    const datetime2Column = column("created_at", "datetime2(7)", "datetime");
    const datetimeOffsetColumn = column(
      "created_offset",
      "datetimeoffset(7)",
      "datetime",
    );

    const conditions = [
      driver.buildFilterCondition(
        datetime2Column,
        "eq",
        "2024-06-15 14:30:00",
        1,
      ),
      driver.buildFilterCondition(
        datetime2Column,
        "neq",
        "2024-06-15 14:30:00.1",
        1,
      ),
      driver.buildFilterCondition(
        datetimeOffsetColumn,
        "eq",
        "2024-06-15 14:30:00.12 +02:00",
        1,
      ),
      driver.buildFilterCondition(
        datetimeOffsetColumn,
        "neq",
        "2024-06-15 14:30:00 +02:00",
        1,
      ),
    ];

    for (const condition of conditions) {
      expect(condition?.sql).not.toContain("DATEDIFF");
    }
    expect(conditions).toEqual([
      {
        sql: "[created_at] >= CAST(? AS datetime2(7)) AND [created_at] <= DATEADD(nanosecond, ?, CAST(? AS datetime2(7)))",
        params: ["2024-06-15 14:30:00", 999_999_900, "2024-06-15 14:30:00"],
      },
      {
        sql: "NOT ([created_at] >= CAST(? AS datetime2(7)) AND [created_at] <= DATEADD(nanosecond, ?, CAST(? AS datetime2(7))))",
        params: ["2024-06-15 14:30:00.1", 99_999_900, "2024-06-15 14:30:00.1"],
      },
      {
        sql: "[created_offset] >= CAST(? AS datetimeoffset(7)) AND [created_offset] <= DATEADD(nanosecond, ?, CAST(? AS datetimeoffset(7)))",
        params: [
          "2024-06-15 14:30:00.12+02:00",
          9_999_900,
          "2024-06-15 14:30:00.12+02:00",
        ],
      },
      {
        sql: "NOT ([created_offset] >= CAST(? AS datetimeoffset(7)) AND [created_offset] <= DATEADD(nanosecond, ?, CAST(? AS datetimeoffset(7))))",
        params: [
          "2024-06-15 14:30:00+02:00",
          999_999_900,
          "2024-06-15 14:30:00+02:00",
        ],
      },
    ]);
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
      sql: `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST("event_at" AS TIMESTAMP(9) WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF9'), '0'), '.') > :1`,
      params: ["2026-04-23 12:34:56"],
    });
    expect(inCondition).toEqual({
      sql: `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST("event_at" AS TIMESTAMP(9) WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF9'), '0'), '.') IN (:1, :2)`,
      params: ["2026-04-23 12:34:56", "2026-04-24 08:00:00"],
    });
  });

  it("exposes Oracle temporal Contains through discovered metadata and validated table filters", () => {
    class MetadataOracleDriver extends OracleDriver {
      enrichForTest(meta: ColumnMeta) {
        return this.enrichColumn(meta);
      }
    }
    const driver = new MetadataOracleDriver({
      ...baseConfig,
      type: "oracle",
    } as ConnectionConfig);
    for (const type of [
      "DATE",
      "TIMESTAMP(9)",
      "TIMESTAMP WITH TIME ZONE",
      "TIMESTAMP WITH LOCAL TIME ZONE",
    ]) {
      for (const nullable of [false, true]) {
        const meta = driver.enrichForTest({
          name: "event_at",
          type,
          nullable,
          isPrimaryKey: false,
          isForeignKey: false,
        });
        expect(meta.filterable).toBe(true);
        expect(meta.filterOperators).toEqual([
          ...scalarTemporalOperators,
          "like",
          ...(nullable ? ["is_null", "is_not_null"] : []),
        ]);
        expect(
          buildWhere(
            driver,
            [{ column: "event_at", operator: "like", value: "2024-03-10" }],
            [meta],
          ),
        ).toEqual({
          clause: expect.stringMatching(/^WHERE .* LIKE :1 ESCAPE '!'$/),
          params: ["%2024-03-10%"],
        });
        expect(() =>
          buildWhere(
            driver,
            [{ column: "event_at", operator: "eq", value: "2024" }],
            [meta],
          ),
        ).toThrow("valid Oracle temporal value");
      }
    }
  });

  it("preserves literal Oracle temporal Contains fragments without adding midnight", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    for (const nativeType of [
      "DATE",
      "TIMESTAMP(9)",
      "TIMESTAMP WITH TIME ZONE",
      "TIMESTAMP WITH LOCAL TIME ZONE",
    ]) {
      const temporalColumn = column(
        "event_at",
        nativeType,
        nativeType === "DATE" ? "date" : "datetime",
      );
      for (const operator of ["like", "ilike"] as const) {
        for (const [input, pattern] of [
          ["2024", "%2024%"],
          ["2024-03-10", "%2024-03-10%"],
          ["12:34", "%12:34%"],
          [".123456789", "%.123456789%"],
          [" 2024%_! ", "%2024!%!_!!%"],
        ]) {
          const normalized = driver.normalizeFilterValue(
            temporalColumn,
            operator,
            input,
          );
          expect(normalized).toBe(input.trim());
          const direct = driver.buildFilterCondition(
            temporalColumn,
            operator,
            input,
            3,
          );
          expect(direct).toEqual({
            sql: expect.stringMatching(/ LIKE :3 ESCAPE '!'$/),
            params: [pattern],
          });
          expect(
            driver.buildFilterCondition(
              temporalColumn,
              operator,
              normalized,
              3,
            ),
          ).toEqual(direct);
          if (nativeType.includes("TIME ZONE"))
            expect(direct?.sql).toContain("SYS_EXTRACT_UTC");
        }
      }
    }
  });

  it("rejects invalid Oracle temporal filter inputs before sending a query", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const dateColumn = column("event_date", "DATE", "date");
    const message =
      "[RapiDB Filter] Column event_date expects a valid Oracle temporal value.";

    expect(() =>
      driver.buildFilterCondition(dateColumn, "eq", "not-a-date", 1),
    ).toThrow(message);
    expect(() =>
      driver.buildFilterCondition(
        dateColumn,
        "between",
        ["2024-02-29", "not-a-date"],
        1,
      ),
    ).toThrow(message);
    expect(() =>
      driver.buildFilterCondition(dateColumn, "in", "2024-02-29,not-a-date", 1),
    ).toThrow(message);
    expect(() =>
      driver.buildFilterCondition(
        dateColumn,
        "in",
        "2024-02-29 00:00:00 +99:99",
        1,
      ),
    ).toThrow(message);
    expect(() =>
      driver.buildFilterCondition(dateColumn, "eq", "2024-02-29", 1),
    ).not.toThrow();
    const normalizedDate = driver.normalizeFilterValue(
      dateColumn,
      "eq",
      "2024-02-29",
    );
    expect(normalizedDate).toBe("2024-02-29 00:00:00");
    expect(
      driver.buildFilterCondition(dateColumn, "eq", normalizedDate, 1),
    ).toEqual({
      sql: `TO_CHAR("event_date", 'YYYY-MM-DD HH24:MI:SS') = :1`,
      params: ["2024-02-29 00:00:00"],
    });
    expect(
      driver.normalizeFilterValue(dateColumn, "eq", "2024-02-29 12:34:56"),
    ).toBe("2024-02-29 12:34:56");
    expect(driver.getCapabilities().isTableFilterError?.(message)).toBe(true);
  });
});
