import { describe, expect, it } from "vitest";
import {
  indexedPlaceholderOffsets,
  questionMarkPlaceholderOffsets,
} from "../../src/extension/dbDrivers/sqlPlaceholders";
import { SqlPreviewMaterializer } from "../../src/extension/dbDrivers/sqlPreviewMaterializer";

describe("default placeholder scanner compatibility", () => {
  it("retains MySQL-style backslash escapes in generic scanners and previews", () => {
    const literal = "'it\\'s ? $1 :1'";
    const sql = `SELECT ${literal}, ?`;
    expect(questionMarkPlaceholderOffsets(sql)).toEqual([sql.lastIndexOf("?")]);
    expect(new SqlPreviewMaterializer().materializePreviewSql(sql, [7])).toBe(
      `SELECT ${literal}, 7`,
    );
    for (const marker of ["$", ":"] as const) {
      const indexedSql = `SELECT ${literal}, ${marker}1`;
      expect(
        indexedPlaceholderOffsets(indexedSql, marker).map(({ start }) => start),
      ).toEqual([indexedSql.lastIndexOf(marker)]);
      expect(
        new SqlPreviewMaterializer().materializePreviewSql(indexedSql, [7]),
      ).toBe(`SELECT ${literal}, 7`);
    }
  });

  it("retains PostgreSQL dollar-quote protection in generic indexed previews", () => {
    const sql = "SELECT $body$ ' ? $1 :1 $body$, $1";
    expect(new SqlPreviewMaterializer().materializePreviewSql(sql, [7])).toBe(
      "SELECT $body$ ' ? $1 :1 $body$, 7",
    );
  });

  it("retains explicit Oracle alternative-quote scanning", () => {
    const sql = "SELECT q'[it's :1 \\]', :1 FROM dual";
    expect(
      indexedPlaceholderOffsets(sql, ":", { dialect: "oracle" }).map(
        ({ start }) => start,
      ),
    ).toEqual([sql.lastIndexOf(":1")]);
  });

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

describe("MSSQL scanner lexical alignment (indexed markers are not T-SQL binds)", () => {
  it.each([
    "$",
    ":",
  ] as const)("uses ordinary backslashes for %s and question marks", (marker) => {
    const prefix = `SELECT N'C:\\', 'it\\''s ? ${marker}1', [x]]?${marker}1], "x""?${marker}1" /* ? ${marker}1 */ -- ? ${marker}1\n`;
    const sql = `${prefix}?, ${marker}1`;
    const options = { dialect: "mssql" } as const;
    expect(questionMarkPlaceholderOffsets(sql, options)).toEqual([
      prefix.length,
    ]);
    expect(indexedPlaceholderOffsets(sql, marker, options)).toEqual([
      {
        start: prefix.length + 3,
        end: sql.length,
        index: 0,
        text: `${marker}1`,
      },
    ]);
  });
});
