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
});
