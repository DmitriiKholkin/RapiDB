import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import {
  indexedPlaceholderOffsets,
  questionMarkPlaceholderOffsets,
} from "../../src/extension/dbDrivers/sqlPlaceholders";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const previewDriver = new SQLiteDriver({
  id: "sql-placeholders-preview",
  name: "SQL Placeholders Preview",
  type: "sqlite",
  filePath: ":memory:",
});

describe("default placeholder scanner compatibility", () => {
  it("retains MySQL-style backslash escapes in generic scanners", () => {
    const literal = "'it\\'s ? $1 :1'";
    const sql = `SELECT ${literal}, ?`;
    expect(questionMarkPlaceholderOffsets(sql)).toEqual([sql.lastIndexOf("?")]);
    for (const marker of ["$", ":"] as const) {
      const indexedSql = `SELECT ${literal}, ${marker}1`;
      expect(
        indexedPlaceholderOffsets(indexedSql, marker).map(({ start }) => start),
      ).toEqual([indexedSql.lastIndexOf(marker)]);
    }
  });

  it("retains PostgreSQL dollar-quote protection in generic indexed previews", () => {
    const sql = "SELECT $body$ ' ? $1 :1 $body$, $1";
    expect(previewDriver.materializePreviewSql(sql, [7])).toBe(
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
    expect(previewDriver.materializePreviewSql(sql, [7])).toBe(
      `SELECT [x]]${marker}1], 7`,
    );
  });

  it("keeps the question-mark scanner's bracket escaping contract", () => {
    const sql = "SELECT [x]]?], ?";
    expect(questionMarkPlaceholderOffsets(sql)).toEqual([sql.lastIndexOf("?")]);
    expect(previewDriver.materializePreviewSql(sql, [7])).toBe(
      "SELECT [x]]?], 7",
    );
  });
});

describe("preview backslash policy", () => {
  const oracle = new OracleDriver({
    id: "oracle-preview",
    name: "Oracle Preview",
    type: "oracle",
  });
  const mysql = new MySQLDriver({
    id: "mysql-preview",
    name: "MySQL Preview",
    type: "mysql",
  });

  it.each([
    { driver: previewDriver, from: "" },
    { driver: oracle, from: " FROM dual" },
  ])("ends ordinary literals at the quote after a backslash ($from)", ({
    driver,
    from,
  }) => {
    // Question marks are native SQLite binds; Oracle uses :n in generated SQL.
    const literals = String.raw`'notes\', 'today''s folder ?\'`;
    const sql = `SELECT ${literals}, ?, ?${from}`;
    expect(driver.materializePreviewSql(sql, [7, 8])).toBe(
      `SELECT ${literals}, 7, 8${from}`,
    );
    expect(() => driver.materializePreviewSql(sql, [7])).toThrow(
      "Preview parameter mismatch: SQL has 2 placeholder(s)",
    );
  });

  it("preserves Oracle alternative quotes in the sequential fallback", () => {
    const literal = String.raw`q'[today's folder ? :1\]'`;
    expect(
      oracle.materializePreviewSql(`SELECT ${literal}, ? FROM dual`, [7]),
    ).toBe(`SELECT ${literal}, 7 FROM dual`);
  });

  it("preserves PostgreSQL ordinary, doubled and E-string quote boundaries", () => {
    const literals = [
      String.raw`'today''s notes ? $1\'`,
      `E'first part'\n${String.raw`'today\'s notes ? $1'`}`,
    ].join(", ");
    const sql = `SELECT ${literals}, ?, $1`;
    const options = { dialect: "pg" } as const;
    expect(questionMarkPlaceholderOffsets(sql, options)).toEqual([
      sql.lastIndexOf("?"),
    ]);
    expect(
      indexedPlaceholderOffsets(sql, "$", options).map((p) => p.start),
    ).toEqual([sql.lastIndexOf("$1")]);
  });

  it.each([
    { backend: "sqlite", driver: previewDriver, bind: "?" },
    { backend: "mysql", driver: mysql, bind: "?" },
    { backend: "oracle", driver: oracle, bind: ":1" },
  ] as const)("renders a generated $backend delete preview after a quoted identifier ending in a backslash", async ({
    backend,
    driver,
    bind,
  }) => {
    const column: ColumnTypeMeta = {
      name: "id",
      type: "INTEGER",
      nativeType: "INTEGER",
      category: "integer",
      valueSemantics: "plain",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
    };
    const service = new TableMutationService(
      {
        getDriver: () => driver,
        getConnection: () => ({ id: "preview", type: backend }),
      } as never,
      { getColumns: async () => [column] },
    );
    const plan = await service.prepareDeleteRowsPlan(
      "preview",
      "",
      "main",
      "notes\\",
      [{ id: 7 }],
    );
    expect(plan?.operations[0]).toMatchObject({ params: [7] });
    const sql = plan?.operations[0].sql ?? "";
    expect(sql).toContain(
      backend === "mysql"
        ? "`id` = ?"
        : backend === "sqlite"
          ? '"id" = ?'
          : `"id" IN (${bind})`,
    );
    expect(plan?.previewStatements).toEqual([sql.replace(bind, "7")]);
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
