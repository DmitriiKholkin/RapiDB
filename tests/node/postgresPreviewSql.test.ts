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
    isAutoIncrement: false,
    filterable: true,
    filterOperators: ["is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

describe("postgres preview SQL materialization", () => {
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
