import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";

const driver = new PostgresDriver({
  id: "preview-test",
  name: "preview-test",
  type: "pg",
  host: "localhost",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
});

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
    filterOperators: ["is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

describe("postgres preview SQL materialization", () => {
  describe.each([
    "generic",
    "column-aware",
  ] as const)("%s lexical boundaries", (mode) => {
    function materialize(sql: string, params: readonly unknown[]): string {
      return mode === "generic"
        ? driver.materializePreviewSql(sql, params)
        : driver.materializePreviewColumnSql(sql, params, [
            column("id", "integer", "integer"),
          ]);
    }

    it.each([
      "\n",
      " \t\r\n\f ",
      " -- ignored ' $1\n",
      "\n -- another comment $1\r\n",
    ])("preserves E-string escape mode across continuation %j", (separator) => {
      const literal = `E'foo'${separator}'it\\'s $1'`;
      expect(materialize(`SELECT ${literal}, $1`, [7])).toBe(
        `SELECT ${literal}, 7`,
      );
    });

    it("preserves escape mode across multiple continued segments", () => {
      const literal = "E'foo'\n'bar' -- comment\r'it\\'s $1'";
      expect(materialize(`SELECT ${literal}, $1`, [7])).toBe(
        `SELECT ${literal}, 7`,
      );
    });

    it.each([
      " ",
      "\n/* block comment $1 */\n",
      ",\n",
    ])("ends escape mode when %j is not a PG quote continuation", (separator) => {
      const literals = `E'foo'${separator}'plain\\'`;
      expect(materialize(`SELECT ${literals}, $1`, [7])).toBe(
        `SELECT ${literals}, 7`,
      );
    });

    it("substitutes placeholders in ARRAY expressions", () => {
      expect(materialize("SELECT ARRAY[$1,$2],$1", [7, 8])).toBe(
        "SELECT ARRAY[7,8],7",
      );
    });

    it("substitutes placeholders in update subscripts", () => {
      expect(materialize("UPDATE t SET a[$1]=$2 WHERE id=$3", [1, 8, 7])).toBe(
        "UPDATE t SET a[1]=8 WHERE id=7",
      );
    });

    it("protects placeholders inside Unicode dollar quotes", () => {
      expect(materialize("SELECT $тег$ $1 $тег$, $1", [7])).toBe(
        "SELECT $тег$ $1 $тег$, 7",
      );
    });

    it("protects Unicode dollar tags with exact, case-sensitive closing delimiters", () => {
      const literal = "$тег_2$ '\\' $1 $Тег_2$ $тег_2$";
      expect(materialize(`SELECT ${literal}, $1`, [7])).toBe(
        `SELECT ${literal}, 7`,
      );
    });

    it("does not recognize dollar tags embedded in Unicode identifiers", () => {
      expect(materialize('SELECT имя$тег$, "имя""$тег$", $1', [7])).toBe(
        'SELECT имя$тег$, "имя""$тег$", 7',
      );
    });
  });

  it("protects quoted identifiers, strings, comments and dollar quotes in column-aware previews", () => {
    const protectedSql = `SELECT "x$1", "x""$2", "trailing\\", '$1 '' $2', E'it\\'s $1', $$ '$2 $$, $body$ $1 $body$ /* $1 /* $2 */ $1 */ -- $2\n`;
    expect(
      driver.materializePreviewColumnSql(
        `${protectedSql}$2, $1, $2`,
        [["a$2", null], Buffer.from([0, 255])],
        [column("x$1", "text[]", "array"), column("x$2", "bytea", "binary")],
      ),
    ).toBe(
      `${protectedSql}'\\x00ff'::bytea, CAST(ARRAY['a$2', NULL] AS text[]), '\\x00ff'::bytea`,
    );
  });

  it("keeps indexed marker boundaries, repeats and multi-digit indices", () => {
    const params = ["9007199254740993.1234567890", ...Array(8).fill(null), 10];
    expect(
      driver.materializePreviewColumnSql(
        "SELECT x$1, $1suffix, $10, $1, $10, $2, $0, $11",
        params,
        [column("amount", "numeric(30,10)", "decimal")],
      ),
    ).toBe(
      "SELECT x$1, $1suffix, 10, '9007199254740993.1234567890', 10, NULL, $0, $11",
    );
  });

  it("prioritizes indexed parameters over the JSON existence operator", () => {
    expect(
      driver.materializePreviewSql(
        `SELECT payload ? 'enabled' FROM probe WHERE id = $1`,
        [7],
      ),
    ).toBe(`SELECT payload ? 'enabled' FROM probe WHERE id = 7`);
  });

  it("does not replace indexed markers inside literals or dollar-quoted SQL", () => {
    expect(
      driver.materializePreviewSql(
        `SELECT '$1' AS text_value, $$ $2 $$ AS body WHERE id = $1`,
        [7],
      ),
    ).toBe(`SELECT '$1' AS text_value, $$ $2 $$ AS body WHERE id = 7`);
  });

  it("does not replace colon markers inside dollar-quoted SQL", () => {
    expect(
      driver.materializePreviewSql("SELECT $$ :1 $$ AS body, :1", [7]),
    ).toBe("SELECT $$ :1 $$ AS body, 7");
  });

  it("renders executable PostgreSQL array and bytea literals", () => {
    const sql =
      'UPDATE "public"."probe" SET "tags" = $1, "payload" = $2 WHERE "id" = $3';
    const preview = driver.materializePreviewSql(sql, [
      ["alpha", "b'et'a", null],
      Buffer.from([0x00, 0xff, 0x10]),
      7,
    ]);

    expect(preview).toContain("\"tags\" = ARRAY['alpha', 'b''et''a', NULL]");
    expect(preview).toContain("\"payload\" = '\\x00ff10'::bytea");
    expect(preview).toContain('WHERE "id" = 7');
  });

  it("renders empty arrays as PostgreSQL empty array literals", () => {
    const sql = "INSERT INTO probe(payload, note) VALUES (?, ?)";
    const preview = driver.materializePreviewSql(sql, [[], "ok"]);

    expect(preview).toBe(
      "INSERT INTO probe(payload, note) VALUES ('{}', 'ok')",
    );
  });

  it("casts typed insert array previews to the target PostgreSQL array type", () => {
    const sql =
      'INSERT INTO "public"."probe" ("col_jsonb_array", "col_text_array") VALUES ($1, $2)';
    const preview = driver.materializePreviewInsertSql(
      sql,
      [
        ['{"a":1}', '{"b":2}'],
        ["one", "two"],
      ],
      [
        column("col_jsonb_array", "jsonb[]", "array"),
        column("col_text_array", "text[]", "array"),
      ],
    );

    expect(preview).toContain(`CAST(ARRAY['{"a":1}', '{"b":2}'] AS jsonb[])`);
    expect(preview).toContain(`CAST(ARRAY['one', 'two'] AS text[])`);
  });

  it("casts typed update array previews to the target PostgreSQL array type", () => {
    const sql =
      'UPDATE "public"."probe" SET "col_jsonb_array" = $1, "col_text_array" = $2 WHERE "id" = $3';
    const preview = driver.materializePreviewColumnSql(
      sql,
      [['{"a":1}', '{"b":3}'], ["one", "two", "three"], 1],
      [
        column("col_jsonb_array", "jsonb[]", "array"),
        column("col_text_array", "text[]", "array"),
        column("id", "integer", "integer"),
      ],
    );

    expect(preview).toContain(
      `"col_jsonb_array" = CAST(ARRAY['{"a":1}', '{"b":3}'] AS jsonb[])`,
    );
    expect(preview).toContain(
      `"col_text_array" = CAST(ARRAY['one', 'two', 'three'] AS text[])`,
    );
    expect(preview).toContain('WHERE "id" = 1');
  });

  it("preserves full exact numeric literals in insert previews", () => {
    const operation = buildInsertRowOperation(
      driver,
      "main",
      "public",
      "probe",
      { amount: "9999999999.1234567890" },
      [column("amount", "numeric(28,10)", "decimal")],
    );

    expect(operation.params).toEqual(["9999999999.1234567890"]);
    expect(driver.materializePreviewSql(operation.sql, operation.params)).toBe(
      'INSERT INTO "public"."probe" ("amount") VALUES (\'9999999999.1234567890\')',
    );
  });
});
