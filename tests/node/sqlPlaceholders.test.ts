import { describe, expect, it } from "vitest";
import {
  indexedPlaceholderOffsets,
  questionMarkPlaceholderOffsets,
} from "../../src/extension/dbDrivers/sqlPlaceholders";
import { SqlPreviewMaterializer } from "../../src/extension/dbDrivers/sqlPreviewMaterializer";

describe("default placeholder scanner compatibility", () => {
  it.each([
    "$",
    ":",
  ] as const)("protects bracket identifiers while scanning %s markers", (marker) => {
    const sql = `SELECT [x]]${marker}1], ${marker}1`;
    expect(indexedPlaceholderOffsets(sql, marker)).toEqual([
      {
        start: sql.lastIndexOf(marker),
        end: sql.length,
        index: 0,
        text: `${marker}1`,
      },
    ]);
    expect(new SqlPreviewMaterializer().materializePreviewSql(sql, [7])).toBe(
      `SELECT [x]]${marker}1], 7`,
    );
  });

  it("keeps the question-mark scanner's bracket escaping contract", () => {
    const sql = "SELECT [x]]?], ?";
    expect(questionMarkPlaceholderOffsets(sql)).toEqual([sql.lastIndexOf("?")]);
    expect(new SqlPreviewMaterializer().materializePreviewSql(sql, [7])).toBe(
      "SELECT [x]]?], 7",
    );
  });
});
