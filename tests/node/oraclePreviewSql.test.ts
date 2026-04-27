import { describe, expect, it } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const baseConfig = {
  id: "oracle-preview-sql",
  name: "Oracle Preview SQL",
  type: "oracle",
  host: "127.0.0.1",
  port: 1521,
  database: "db",
  serviceName: "FREEPDB1",
  username: "user",
  password: "pass",
} as const satisfies Partial<ConnectionConfig>;

function buildColumn(
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

describe("Oracle preview SQL literals", () => {
  it("materializes JS Date values as ANSI TIMESTAMP literals", () => {
    const driver = new OracleDriver(baseConfig as ConnectionConfig);
    const preview = driver.materializePreviewSql(
      'INSERT INTO "T" ("d", "ts") VALUES (:1, :2)',
      [
        new Date(Date.UTC(2024, 5, 15, 12, 30, 0, 0)),
        new Date(Date.UTC(2024, 5, 15, 12, 30, 0, 123)),
      ],
    );

    expect(preview).toContain("TIMESTAMP '2024-06-15 12:30:00'");
    expect(preview).toContain("TIMESTAMP '2024-06-15 12:30:00.123'");
  });

  it("materializes INSERT preview SQL with Oracle temporal type-safe literals", () => {
    const driver = new OracleDriver(baseConfig as ConnectionConfig);
    const columns: ColumnTypeMeta[] = [
      {
        name: "COL_DATE",
        type: "DATE",
        nativeType: "DATE",
        category: "date",
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        isAutoIncrement: false,
        filterable: true,
        filterOperators: ["eq", "like", "is_null", "is_not_null"],
        valueSemantics: "plain",
      },
      {
        name: "COL_TS",
        type: "TIMESTAMP",
        nativeType: "TIMESTAMP",
        category: "datetime",
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        isAutoIncrement: false,
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
        valueSemantics: "plain",
      },
      {
        name: "COL_TS_TZ",
        type: "TIMESTAMP WITH TIME ZONE",
        nativeType: "TIMESTAMP WITH TIME ZONE",
        category: "datetime",
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        isAutoIncrement: false,
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
        valueSemantics: "plain",
      },
    ];

    const preview = driver.materializePreviewInsertSql(
      'INSERT INTO "T" ("COL_DATE", "COL_TS", "COL_TS_TZ") VALUES (:1, :2, :3)',
      [
        new Date(2024, 5, 15, 12, 30, 0, 0),
        new Date(2024, 5, 15, 12, 30, 0, 123),
        new Date(Date.UTC(2024, 5, 15, 11, 30, 0, 123)),
      ],
      columns,
    );

    expect(preview).toContain("TO_DATE('");
    expect(preview).toContain("TO_TIMESTAMP('");
    expect(preview).toContain("TO_TIMESTAMP_TZ('");
    expect(preview).toContain("+00:00");
  });

  it("preserves full exact numeric literals in insert previews", () => {
    const driver = new OracleDriver(baseConfig as ConnectionConfig);
    const operation = buildInsertRowOperation(
      driver,
      "db",
      "APP",
      "T",
      { amount: "9999999999.1234567890" },
      [
        {
          name: "amount",
          type: "NUMBER(28,10)",
          nativeType: "NUMBER(28,10)",
          category: "decimal",
          nullable: true,
          defaultValue: undefined,
          isPrimaryKey: false,
          primaryKeyOrdinal: undefined,
          isForeignKey: false,
          isAutoIncrement: false,
          filterable: true,
          filterOperators: ["is_null", "is_not_null"],
          valueSemantics: "plain",
        },
      ],
    );

    expect(operation.params).toEqual(["9999999999.1234567890"]);
    expect(
      driver.materializePreviewInsertSql(operation.sql, operation.params, [
        {
          name: "amount",
          type: "NUMBER(28,10)",
          nativeType: "NUMBER(28,10)",
          category: "decimal",
          nullable: true,
          defaultValue: undefined,
          isPrimaryKey: false,
          primaryKeyOrdinal: undefined,
          isForeignKey: false,
          isAutoIncrement: false,
          filterable: true,
          filterOperators: ["is_null", "is_not_null"],
          valueSemantics: "plain",
        },
      ]),
    ).toBe(
      'INSERT INTO "APP"."T" ("amount") VALUES (\'9999999999.1234567890\')',
    );
  });

  it("materializes Oracle-specific literals for binary, LOB, and XML columns", () => {
    const driver = new OracleDriver(baseConfig as ConnectionConfig);
    const longClob = `Very long CLOB: ${"x".repeat(1200)}`;
    const preview = driver.materializePreviewInsertSql(
      'INSERT INTO "T" ("COL_BLOB", "COL_RAW", "COL_CLOB", "COL_NCLOB", "COL_XMLTYPE") VALUES (:1, :2, :3, :4, :5)',
      [
        Buffer.from("deadbeefcafe0102030405060708090a", "hex"),
        Buffer.from("48656c6c6f", "hex"),
        longClob,
        "Unicode NCLOB: Привет мир 你好 😀",
        '<root><child id="1">Text</child></root>',
      ],
      [
        buildColumn("COL_BLOB", "BLOB", "binary"),
        buildColumn("COL_RAW", "RAW(16)", "binary"),
        buildColumn("COL_CLOB", "CLOB", "text"),
        buildColumn("COL_NCLOB", "NCLOB", "text"),
        buildColumn("COL_XMLTYPE", "XMLTYPE", "text"),
      ],
    );

    expect(preview).toContain("HEXTORAW('deadbeefcafe0102030405060708090a')");
    expect(preview).toContain("HEXTORAW('48656c6c6f')");
    expect(preview).not.toContain("X '");
    expect(preview).not.toContain("X'");
    expect(preview).toContain("TO_CLOB('Very long CLOB: ");
    expect(preview).toContain(" || TO_CLOB('");
    expect(preview).toContain("TO_NCLOB(N'Unicode NCLOB: Привет мир 你好 😀')");
    expect(preview).toContain(
      `XMLTYPE(TO_CLOB('<root><child id="1">Text</child></root>'))`,
    );
  });

  it("normalizes Oracle XMLTYPE whitespace for display and editing", () => {
    const driver = new OracleDriver(baseConfig as ConnectionConfig);
    const column = buildColumn("COL_XMLTYPE", "XMLTYPE", "text");

    const formatted = driver.formatOutputValue(
      '<root>    <child id="1">Text</child>   </root>',
      column,
    );

    expect(formatted).toBe('<root><child id="1">Text</child></root>');
    expect(String(formatted)).toBe('<root><child id="1">Text</child></root>');
  });

  it("normalizes Oracle XMLTYPE filters to the compact XML form", () => {
    const driver = new OracleDriver(baseConfig as ConnectionConfig);
    const column = buildColumn("COL_XMLTYPE", "XMLTYPE", "text");

    expect(
      driver.normalizeFilterValue(
        column,
        "like",
        '<root>    <child id="1">Text</child>   </root>',
      ),
    ).toBe('<root><child id="1">Text</child></root>');

    const condition = driver.buildFilterCondition(
      column,
      "like",
      '<root><child id="1">Text</child></root>',
      1,
    );

    expect(condition).toBeTruthy();
    expect(condition).toMatchObject({
      sql: `UPPER(REGEXP_REPLACE(XMLSERIALIZE(CONTENT "COL_XMLTYPE" AS CLOB), '>\\s+<', '><')) LIKE UPPER(:1)`,
      params: ['%<root><child id="1">Text</child></root>%'],
    });
  });
});
