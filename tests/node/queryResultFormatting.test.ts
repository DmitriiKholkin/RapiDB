import oracledb from "oracledb";
import { types as pgTypes } from "pg";
import { describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";

describe("formatQueryResult", () => {
  it.each([
    [10_000, 1, 0],
    [6_000, 4_001, 4_000],
    [2, 2, 2],
  ])("applies one aggregate row budget to sets of %i and %i", (firstCount, secondCount, retainedSecond) => {
    const first = Array.from({ length: firstCount }, (_, i) => ({
      __col_0: i,
    }));
    const second = Array.from({ length: secondCount }, (_, i) => ({
      __col_0: i + 10000,
    }));
    const input = {
      columns: ["first"],
      rows: first,
      rowCount: firstCount + secondCount,
      executionTimeMs: 1,
      error: "late error",
      resultSets: [
        { id: "one", columns: ["first"], rows: first, rowCount: firstCount },
        {
          id: "two",
          columns: ["second"],
          rows: second,
          rowCount: secondCount,
          partial: true,
        },
        { id: "empty", columns: ["empty"], rows: [], rowCount: 0 },
      ],
    };
    const result = formatQueryResult(input, 10_000);
    expect(result.resultSets?.map((s) => s.rows.length)).toEqual([
      firstCount,
      retainedSecond,
      0,
    ]);
    expect(result.resultSets?.[1]).toMatchObject({
      rowCount: secondCount,
      truncated: secondCount > retainedSecond,
      truncatedAt: retainedSecond,
      partial: true,
    });
    expect(result.resultSets?.[2]).toMatchObject({
      rows: [],
      truncated: false,
      truncatedAt: 0,
    });
    expect(result.error).toBe("late error");
    expect(input.resultSets[1].rows.length).toBe(secondCount);
  });

  it("shares a configured display budget across all result sets", () => {
    const rows = [{ __col_0: 1 }, { __col_0: 2 }];
    const result = formatQueryResult(
      {
        columns: ["n"],
        rows,
        rowCount: 4,
        executionTimeMs: 1,
        resultSets: [
          { id: "one", columns: ["n"], rows, rowCount: 2 },
          { id: "two", columns: ["n"], rows, rowCount: 2 },
          { id: "three", columns: ["n"], rows: [{ __col_0: 3 }], rowCount: 1 },
        ],
      },
      3,
    );
    expect(
      result.resultSets?.map((s) => ({
        rows: s.rows.length,
        at: s.truncatedAt,
        truncated: s.truncated,
      })),
    ).toEqual([
      { rows: 2, at: 2, truncated: false },
      { rows: 1, at: 1, truncated: true },
      { rows: 0, at: 0, truncated: true },
    ]);
  });

  it("reports the actual displayed row count when a driver already truncated below the UI limit", () => {
    const formatted = formatQueryResult(
      {
        columns: ["id"],
        rows: [{ __col_0: 1 }, { __col_0: 2 }],
        rowCount: 120,
        executionTimeMs: 12,
        truncated: true,
      },
      100,
    );

    expect(formatted).toMatchObject({
      truncated: true,
      truncatedAt: 2,
      rows: [{ __col_0: 1 }, { __col_0: 2 }],
    });
  });

  it("keeps export-only native rows aligned with the displayed row limit", () => {
    const formatted = formatQueryResult(
      {
        columns: ["value"],
        rows: [{ __col_0: "first" }, { __col_0: "second" }],
        exportRows: [{ __col_0: { S: "first" } }, { __col_0: { S: "second" } }],
        rowCount: 2,
        executionTimeMs: 3,
      },
      1,
    );

    expect(formatted.rows).toEqual([{ __col_0: "first" }]);
    expect(formatted.exportRows).toEqual([{ __col_0: { S: "first" } }]);
    expect(formatted.truncated).toBe(true);
  });

  it("normalizes binary query values to the 0x display prefix", () => {
    const formatted = formatQueryResult(
      {
        columns: ["payload", "legacy_payload", "plain_text"],
        columnMeta: [
          { category: "binary" },
          { category: "binary" },
          { category: "text" },
        ],
        rows: [
          {
            __col_0: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
            __col_1: "\\xBEEF",
            __col_2: "\\xdeadbeef",
          },
        ],
        rowCount: 1,
        executionTimeMs: 12,
      },
      100,
    );

    expect(formatted.rows).toEqual([
      {
        __col_0: "0xdeadbeef",
        __col_1: "0xbeef",
        __col_2: "\\xdeadbeef",
      },
    ]);
  });

  it("stringifies non-finite numeric values before webview serialization", () => {
    const formatted = formatQueryResult(
      {
        columns: ["col_real"],
        columnMeta: [{ category: "float" }],
        rows: [{ __col_0: Number.POSITIVE_INFINITY }],
        rowCount: 1,
        executionTimeMs: 3,
      },
      100,
    );

    expect(formatted.rows).toEqual([{ __col_0: "Infinity" }]);
  });

  it("formats PostgreSQL circle and interval-like objects without legacy JSON output", () => {
    const formatted = formatQueryResult(
      {
        columns: ["col_circle", "col_interval"],
        columnMeta: [{ category: "spatial" }, { category: "interval" }],
        rows: [
          {
            __col_0: { x: 1, y: 2, radius: 3 },
            __col_1: {
              years: 1,
              months: 2,
              days: 3,
              hours: 4,
              minutes: 5,
              seconds: 6,
              milliseconds: 700,
            },
          },
        ],
        rowCount: 1,
        executionTimeMs: 5,
      },
      100,
    );

    expect(formatted.rows).toEqual([
      {
        __col_0: "<(1,2),3>",
        __col_1: "P1Y2M3DT4H5M6.7S",
      },
    ]);
  });

  it("formats a zero PostgreSQL interval as P0D without touching plain empty objects", () => {
    const zero = pgTypes.getTypeParser(1186)("00:00:00");
    expect(Object.keys(zero)).toEqual([]);
    const formatted = formatQueryResult(
      {
        columns: ["zero", "json_empty"],
        columnMeta: [{ category: "interval" }, { category: "json" }],
        rows: [{ __col_0: zero, __col_1: {} }],
        rowCount: 1,
        executionTimeMs: 1,
      },
      100,
    );
    expect(formatted.rows).toEqual([{ __col_0: "P0D", __col_1: "{}" }]);

    const pg = new PostgresDriver({ id: "pg", name: "PG", type: "pg" });
    expect(
      pg.formatOutputValue(zero, {
        name: "i",
        nativeType: "interval",
        category: "interval",
      } as never),
    ).toBe("P0D");
    expect(
      pg.formatOutputValue({}, {
        name: "j",
        nativeType: "jsonb",
        category: "json",
      } as never),
    ).toBe("{}");
    expect(
      formatQueryResult(
        {
          columns: ["empty"],
          columnMeta: [{ category: "interval" }],
          rows: [{ __col_0: {} }],
          rowCount: 1,
          executionTimeMs: 1,
        },
        100,
      ).rows,
    ).toEqual([{ __col_0: "{}" }]);
  });

  it.each([
    ["json", "json"],
    ["untyped", null],
  ] as const)("keeps point-, circle- and interval-shaped %s objects as JSON", (_label, category) => {
    const formatted = formatQueryResult(
      {
        columns: ["point_like", "circle_like", "interval_like"],
        columnMeta: [{ category }, { category }, { category }],
        rows: [
          {
            __col_0: { x: 1, y: 2 },
            __col_1: { x: 1, y: 2, radius: 3 },
            __col_2: { days: 3, hours: 4 },
          },
        ],
        rowCount: 1,
        executionTimeMs: 1,
      },
      100,
    );

    expect(formatted.rows).toEqual([
      {
        __col_0: '{"x":1,"y":2}',
        __col_1: '{"x":1,"y":2,"radius":3}',
        __col_2: '{"days":3,"hours":4}',
      },
    ]);
  });

  it("trims trailing zeros from datetime fractions in query results", () => {
    const formatted = formatQueryResult(
      {
        columns: ["col_dt_1", "col_dt_2", "col_dt_3"],
        rows: [
          {
            __col_0: "2026-04-23 12:34:56.120000+00",
            __col_1: "2026-04-23 12:34:56.000000+00",
            __col_2: "2026-04-23 12:34:56.100000",
          },
        ],
        columnMeta: [
          { category: "datetime" },
          { category: "datetime" },
          { category: "datetime" },
        ],
        rowCount: 1,
        executionTimeMs: 6,
      },
      100,
    );

    expect(formatted.rows).toEqual([
      {
        __col_0: "2026-04-23 12:34:56.12+00:00",
        __col_1: "2026-04-23 12:34:56+00:00",
        __col_2: "2026-04-23 12:34:56.1",
      },
    ]);
  });

  it("marks Oracle TO_CHAR temporal projections as datetime and trims FF9 fractions", async () => {
    const driver = new OracleDriver({
      id: "oracle-ff9",
      name: "oracle-ff9",
      type: "oracle",
    });
    const connection = {
      getStatementInfo: vi
        .fn()
        .mockResolvedValueOnce({
          statementType: oracledb.STMT_TYPE_SELECT,
          metaData: [
            { name: "D", dbType: oracledb.DB_TYPE_DATE },
            { name: "TS", dbType: oracledb.DB_TYPE_TIMESTAMP },
            { name: "TSTZ", dbType: oracledb.DB_TYPE_TIMESTAMP_TZ },
            { name: "TSLTZ", dbType: oracledb.DB_TYPE_TIMESTAMP_LTZ },
            { name: "LABEL", dbType: oracledb.DB_TYPE_VARCHAR },
          ],
        })
        .mockResolvedValueOnce({
          statementType: oracledb.STMT_TYPE_SELECT,
          metaData: [0, 1, 2, 3, 4].map((index) => ({
            name: `__RAPIDB_Q_COL_${index}`,
          })),
        }),
    };
    const projection = await (
      driver as unknown as {
        _buildTemporalQueryProjection: (
          connection: unknown,
          sql: string,
        ) => Promise<{ datetimeColumns: Set<string> } | undefined>;
      }
    )._buildTemporalQueryProjection(
      connection,
      "SELECT D, TS, TSTZ, TSLTZ, LABEL FROM T",
    );
    expect([...(projection?.datetimeColumns ?? [])]).toEqual([
      "__RAPIDB_Q_COL_0",
      "__RAPIDB_Q_COL_1",
      "__RAPIDB_Q_COL_3",
    ]);

    const formatted = formatQueryResult(
      {
        columns: ["TS", "TSLTZ", "LABEL"],
        rows: [
          {
            __col_0: "2024-01-15 10:00:00.000000000",
            __col_1: "2024-01-15 10:00:00.120000000Z",
            __col_2: "2024-01-15 10:00:00.000000000",
          },
        ],
        columnMeta: [
          { category: "datetime" },
          { category: "datetime" },
          { category: null },
        ],
        rowCount: 1,
        executionTimeMs: 1,
      },
      100,
    );
    expect(formatted.rows).toEqual([
      {
        __col_0: "2024-01-15 10:00:00",
        __col_1: "2024-01-15 10:00:00.12Z",
        __col_2: "2024-01-15 10:00:00.000000000",
      },
    ]);
  });

  it("keeps datetime-looking text verbatim outside temporal columns", () => {
    const formatted = formatQueryResult(
      {
        columns: ["text_value", "unknown_value", "date_value"],
        rows: [
          {
            __col_0: "2024-01-01T10:00:00.000Z",
            __col_1: "2024-01-01 10:00:00.500000",
            __col_2: new Date("2024-01-01T10:00:00.000Z"),
          },
        ],
        columnMeta: [
          { category: "text" },
          { category: null },
          { category: null },
        ],
        rowCount: 1,
        executionTimeMs: 1,
      },
      100,
    );

    expect(formatted.rows).toEqual([
      {
        __col_0: "2024-01-01T10:00:00.000Z",
        __col_1: "2024-01-01 10:00:00.500000",
        __col_2: "2024-01-01 10:00:00",
      },
    ]);
  });

  it("formats every rowset and keeps an error beside earlier SQL Server results", () => {
    const result = formatQueryResult(
      {
        columns: ["first_value"],
        rows: [{ __col_0: 1 }],
        rowCount: 2,
        executionTimeMs: 12,
        error: "Divide by zero error encountered. (SQL Server line 2)",
        resultSets: [
          {
            id: "result-1",
            columns: ["first_value"],
            rows: [{ __col_0: 1 }],
            rowCount: 1,
          },
          {
            id: "result-2",
            columns: ["second_value"],
            rows: [{ __col_0: 2 }],
            rowCount: 1,
          },
        ],
      },
      10,
    );

    expect(result.error).toContain("SQL Server line 2");
    expect(result.resultSets).toMatchObject([
      {
        columns: ["first_value"],
        columnMeta: [{ category: null }],
        rows: [{ __col_0: 1 }],
        rowCount: 1,
      },
      {
        columns: ["second_value"],
        columnMeta: [{ category: null }],
        rows: [{ __col_0: 2 }],
        rowCount: 1,
      },
    ]);
  });
});
