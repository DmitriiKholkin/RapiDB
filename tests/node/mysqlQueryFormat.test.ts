import { describe, expect, it } from "vitest";
import {
  formatMysqlQuery,
  formatMysqlStringLiteral,
} from "../../src/extension/dbDrivers/mysqlQueryFormat";

describe("SQL-mode-independent MySQL bindings", () => {
  it.each([
    "",
    "O'Reilly",
    "x' WHERE 1=1 -- ",
    "\\' OR 1=1 -- ",
    "😀\u0000\r\n\u001a",
  ])("encodes %j as a UTF-8 literal with no escape sequences", (value) => {
    const literal = formatMysqlStringLiteral(value);
    expect(literal).toBe(`_utf8mb4 X'${Buffer.from(value).toString("hex")}'`);
    expect(formatMysqlQuery("SELECT ?", [value])).toBe(`SELECT ${literal}`);
  });
  it("preserves exact scalar, temporal, binary and list binding types", () => {
    expect(
      formatMysqlQuery(
        "SELECT ?, ?, ?, ?, ?, ?, ?",
        [
          9007199254740993n,
          42,
          false,
          null,
          undefined,
          Buffer.from([0, 255]),
          new Date("2026-01-02T03:04:05Z"),
        ],
        "Z",
      ),
    ).toBe(
      "SELECT 9007199254740993, 42, false, NULL, NULL, X'00ff', '2026-01-02 03:04:05.000'",
    );
    expect(formatMysqlQuery("SELECT ?", [["one", ["two", "three"]]])).toBe(
      "SELECT _utf8mb4 X'6f6e65', (_utf8mb4 X'74776f', _utf8mb4 X'7468726565')",
    );
    expect(formatMysqlQuery("SELECT ?", [new Uint8Array([0, 255])])).toBe(
      "SELECT X'00ff'",
    );
  });
  it("does not reformat SQL without bindings", () => {
    const sql = "SELECT 'literal ?'";
    expect(formatMysqlQuery(sql)).toBe(sql);
  });
  it("rejects object SQL expansion and caller-provided raw SQL hooks", () => {
    for (const value of [{ value: "x" }, { toSqlString: () => "1 OR 1=1" }]) {
      expect(() => formatMysqlQuery("SELECT ?", [value])).toThrow(
        /serialize JSON/,
      );
    }
  });
});
