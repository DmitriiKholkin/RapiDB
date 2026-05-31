import { describe, expect, it } from "vitest";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";

describe("formatQueryResult", () => {
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
});
