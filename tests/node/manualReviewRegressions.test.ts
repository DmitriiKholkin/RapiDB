import {
  Binary,
  Decimal128,
  Double,
  Int32,
  Long,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp,
} from "mongodb";
import oracledb from "oracledb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { splitInFilterValues } from "../../src/extension/dbDrivers/nosqlUtils";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { verifyTransaction } from "../../src/extension/dbDrivers/transactionVerification";
import {
  type ColumnTypeMeta,
  type FilterExpression,
  NULL_SENTINEL,
} from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { buildUpdateRowSql } from "../../src/extension/table/updateSql";
import {
  formatTableCsvExportValue,
  serializeJsonExportRecord,
} from "../../src/extension/utils/exportValueFormat";

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta {
  return {
    name,
    nativeType,
    type: nativeType,
    category,
    nullable: true,
    isPrimaryKey: name === "ID",
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  };
}
const oracle = new OracleDriver({
  id: "offline-review",
  name: "Review",
  type: "oracle",
});
const mssql = new MSSQLDriver({
  id: "offline-review",
  name: "Review",
  type: "mssql",
});
const id = column("ID", "NUMBER", "integer");
const objectColumn = {
  ...column("OBJ", "REVIEW_OBJ", "other"),
  objectTypeOwner: "QA",
  objectTypeName: "REVIEW_OBJ",
  objectAttributes: [
    { name: "N", nativeType: "NUMBER" },
    { name: "TXT", nativeType: "VARCHAR2" },
  ],
};
const variant = column("V", "sql_variant", "other");
function snapshot(overrides: Record<string, unknown> = {}) {
  return {
    __rapidbSqlVariant: 1,
    displayValue: "abc",
    baseType: "nvarchar",
    precision: 0,
    scale: 0,
    maxLength: 6,
    collation: "Latin1_General_CI_AS",
    binaryHex: "610062006300",
    ...overrides,
  };
}
function jsonExport(
  value: unknown,
  category: "json" | "other" = "json",
  nativeType?: string,
) {
  return serializeJsonExportRecord([
    { key: "payload", sourceKey: "payload", category, nativeType, value },
  ]);
}

describe("manual review: exact exports and user documents", () => {
  it("exports nested numbers exactly in JSON and CSV, including exponents", () => {
    const text =
      '{"n":9007199254740993,"d":0.123456789012345678901,"a":[1e400,-0,1.2300]}';
    expect(jsonExport(text)).toBe(`{"payload":${text}}`);
    expect(formatTableCsvExportValue(text, "json")).toBe(text);
  });
  it("preserves marker-like user data and literal keys at every depth", () => {
    const value = {
      literal: "user text",
      nested: { __rapidbSpatial: 1, displayValue: "visible", userData: 123 },
      a: [{ __rapidbSqlVariant: 1, displayValue: "x", literal: 7 }],
    };
    expect(jsonExport(JSON.stringify(value))).toBe(
      JSON.stringify({ payload: value }),
    );
    expect(jsonExport(value)).toBe(JSON.stringify({ payload: value }));
    expect(formatTableCsvExportValue(value, "json")).toBe(
      JSON.stringify(value),
    );
  });
  it("unwraps real table snapshots only with native column metadata", () => {
    const value = {
      __rapidbSpatial: 1,
      displayValue: "SRID=0;POINT (1 2)",
      srid: 0,
      binaryHex: "abcd",
    };
    expect(jsonExport(value, "other", "geometry")).toBe(
      '{"payload":"SRID=0;POINT (1 2)"}',
    );
    expect(formatTableCsvExportValue(value, "spatial", "geometry")).toBe(
      value.displayValue,
    );
    expect(JSON.parse(jsonExport(value))).toEqual({ payload: value });
  });
});

describe("manual review: Oracle exact objects and temporal writes", () => {
  it("reads native JSON through text to preserve exact numeric tokens", () => {
    const jsonColumn = column("JSON_DOC", "JSON", "json");
    expect(oracle.buildVerificationValueExpr(jsonColumn)).toBe(
      'JSON_SERIALIZE("JSON_DOC" RETURNING CLOB)',
    );
  });
  it("uses Oracle JSON_EQUAL for exact optimistic edit guards", () => {
    const jsonColumn = column("JSON_DOC", "JSON", "json");
    const previous = '{"ok":true,"n":9007199254740993}';
    const next = '{"ok":false,"n":9007199254740994}';
    const update = buildUpdateRowSql(
      oracle,
      "",
      "QA",
      "T",
      { ID: 1 },
      { JSON_DOC: next },
      [id, jsonColumn],
      { JSON_DOC: previous },
    );

    expect(update?.sql).toContain(
      'JSON_EQUAL("RAPIDB_UPDATE_ROW"."JSON_DOC", :3 ERROR ON ERROR)',
    );
    expect(update?.params).toEqual([next, 1, previous]);
  });
  it("verifies native JSON edits without rounding numbers and distinguishes SQL NULL", () => {
    const jsonColumn = column("JSON_DOC", "JSON", "json");
    const requested = '{"ok":false,"n":9007199254740994}';

    expect(
      oracle.checkPersistedEdit(jsonColumn, requested, {
        persistedValue: '{ "n": 9007199254740994, "ok": false }',
      })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(jsonColumn, requested, {
        persistedValue: '{"ok":false,"n":9007199254740992}',
      })?.ok,
    ).toBe(false);
    expect(
      oracle.checkPersistedEdit(jsonColumn, null, { persistedValue: null })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(jsonColumn, "null", { persistedValue: null })
        ?.ok,
    ).toBe(false);
    expect(
      oracle.checkPersistedEdit(jsonColumn, "null", {
        persistedValue: "null",
      })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(jsonColumn, '{"n":1.0}', {
        persistedValue: '{"n":1}',
      })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(jsonColumn, '{"n":1e0}', {
        persistedValue: '{"n":1.0}',
      })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(jsonColumn, '{"x":1,"x":2}')?.shouldVerify,
    ).toBe(false);
    expect(
      buildUpdateRowSql(
        oracle,
        "",
        "QA",
        "T",
        { ID: 1 },
        { JSON_DOC: requested },
        [id, jsonColumn],
        { JSON_DOC: "null" },
      )?.sql,
    ).toContain(
      'JSON_EQUAL("RAPIDB_UPDATE_ROW"."JSON_DOC", :3 ERROR ON ERROR)',
    );
  });

  it("aligns preview metadata with every expanded object bind and skips inline NULLs", () => {
    const xml = column("XML_DOC", "XMLTYPE", "lob");
    const next = '{"N":1,"TXT":"new"}';
    const old = '{"N":2,"TXT":"old"}';
    let updateColumns: Array<ColumnTypeMeta | undefined> = [];
    const update = buildUpdateRowSql(
      oracle,
      "",
      "QA",
      "T",
      { ID: 1 },
      { OBJ: next, XML_DOC: "<new/>" },
      [id, objectColumn, xml],
      { OBJ: old, XML_DOC: "<old/>" },
      (columns) => {
        updateColumns = columns;
      },
    );
    expect(updateColumns.map((col) => col?.name)).toEqual([
      "OBJ",
      "OBJ",
      "XML_DOC",
      "ID",
      "OBJ",
      "OBJ",
      "XML_DOC",
    ]);
    if (!update) throw new Error("Expected an update operation");
    const preview = oracle.materializePreviewColumnSql(
      update.sql,
      update.params,
      updateColumns,
    );
    expect(preview).not.toContain("JSON_VALUE(XMLTYPE");
    expect(preview).toContain("XMLTYPE(TO_CLOB('<new/>'))");
    expect(preview).toContain("XMLTYPE(TO_CLOB('<old/>')) AS CLOB NO INDENT");
    expect(preview).not.toContain("XMLTYPE(XMLTYPE(");
    expect(update.sql).toContain('"XML_DOC" = XMLTYPE(:3)');
    expect(oracle.buildVerificationValueExpr(xml)).toBe(
      'XMLSERIALIZE(CONTENT "XML_DOC" AS CLOB NO INDENT)',
    );
    let insertColumns: ColumnTypeMeta[] = [];
    const insert = buildInsertRowOperation(
      oracle,
      "",
      "QA",
      "T",
      { ID: null, OBJ: next, XML_DOC: "<new/>" },
      [id, objectColumn, xml],
      undefined,
      (columns) => {
        insertColumns = columns;
      },
    );
    expect(insertColumns.map((col) => col.name)).toEqual([
      "OBJ",
      "OBJ",
      "XML_DOC",
    ]);
    const insertPreview = oracle.materializePreviewInsertSql(
      insert.sql,
      insert.params,
      insertColumns,
    );
    expect(insertPreview).not.toContain("JSON_VALUE(XMLTYPE");
    expect(insertPreview).toContain("XMLTYPE(TO_CLOB('<new/>'))");
  });
  it("describes DbObject SELECTs without executing the user's SQL when Thin metadata fails", async () => {
    const original = "SELECT OBJ, TS FROM QA_REVIEW";
    const connection = {
      getStatementInfo: vi
        .fn()
        .mockRejectedValueOnce(
          new TypeError(
            "options.connection._getDbObjectClass is not a function",
          ),
        )
        .mockResolvedValueOnce({
          statementType: oracledb.STMT_TYPE_SELECT,
          metaData: [
            { name: "__RAPIDB_Q_COL_0" },
            { name: "__RAPIDB_Q_COL_1" },
          ],
        }),
      execute: vi.fn().mockResolvedValue({
        outBinds: {
          columns: JSON.stringify([
            { name: "OBJ", type: 109 },
            { name: "TS", type: 180 },
          ]),
        },
      }),
    };
    const state = oracle as unknown as {
      _buildTemporalQueryProjection: (
        connection: unknown,
        sql: string,
      ) => Promise<{ sql: string } | undefined>;
    };
    const projection = await state._buildTemporalQueryProjection(
      connection,
      original,
    );
    expect(projection?.sql).toContain("JSON_OBJECT(");
    expect(projection?.sql).toContain("HH24:MI:SS.FF9");
    expect(connection.execute).toHaveBeenCalledOnce();
    expect(connection.execute.mock.calls[0][0]).toContain(
      "DBMS_SQL.DESCRIBE_COLUMNS3",
    );
    expect(connection.execute.mock.calls[0][0]).not.toContain(
      "DBMS_SQL.EXECUTE",
    );
    expect(connection.execute.mock.calls[0][1].sql_text.val).toBe(original);
  });
  it("escapes SQL apostrophes in Oracle object JSON paths for mutations and guards", () => {
    const col = {
      ...objectColumn,
      objectAttributes: [{ name: "O'Reilly", nativeType: "VARCHAR2" }],
    };
    const update = buildUpdateRowSql(
      oracle,
      "",
      "QA",
      "T",
      { ID: 1 },
      { OBJ: `{"O'Reilly":"new"}` },
      [id, col],
      { OBJ: `{"O'Reilly":"old"}` },
    );
    expect(update?.sql.match(/\$\."O''Reilly"/g)).toHaveLength(2);
    const insert = buildInsertRowOperation(
      oracle,
      "",
      "QA",
      "T",
      { ID: 1, OBJ: `{"O'Reilly":"new"}` },
      [id, col],
    );
    expect(insert.sql).toContain(`'$."O''Reilly"'`);
  });
  it("keeps an Oracle __proto__ attribute as an own JSON field", () => {
    const col = {
      ...objectColumn,
      objectAttributes: [{ name: "__proto__", nativeType: "VARCHAR2" }],
    };
    const operation = buildUpdateRowSql(
      oracle,
      "",
      "QA",
      "T",
      { ID: 1 },
      { OBJ: '{"__proto__":"hello"}' },
      [id, col],
      { OBJ: '{"__proto__":"old"}' },
    );
    expect(operation?.params[0]).toBe('{"__proto__":"hello"}');
    expect(operation?.params.at(-1)).toBe('{"__proto__":"old"}');
  });
  it("preserves exact NUMBER tokens in SQL parameters and detects a rounded readback", () => {
    const requested = '{"N":9007199254740993,"TXT":"updated"}';
    const sql = buildUpdateRowSql(
      oracle,
      "",
      "QA",
      "T",
      { ID: 1 },
      { OBJ: requested },
      [id, objectColumn],
      { OBJ: '{"N":9007199254740991,"TXT":"old"}' },
    );
    expect(sql?.params[0]).toContain('"9007199254740993"');
    expect(
      oracle.checkPersistedEdit(objectColumn, requested, {
        persistedValue: requested,
      })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(objectColumn, requested, {
        persistedValue: '{"N":9007199254740992,"TXT":"updated"}',
      })?.ok,
    ).toBe(false);
    expect(oracle.buildVerificationValueExpr(objectColumn)).toContain(
      "JSON_OBJECT",
    );
  });
  it.each([
    null,
    NULL_SENTINEL,
  ])("supports object SQL NULL and verifies atomic null (%s)", (value) => {
    expect(
      oracle.checkPersistedEdit(objectColumn, value, { persistedValue: null })
        ?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(objectColumn, value, {
        persistedValue: '{"N":null,"TXT":null}',
      })?.ok,
    ).toBe(false);
    expect(
      buildUpdateRowSql(
        oracle,
        "",
        "QA",
        "T",
        { ID: 1 },
        { OBJ: value },
        [id, objectColumn],
        { OBJ: null },
      )?.sql,
    ).toContain('SET "OBJ" = NULL');
  });
  it("keeps nanoseconds in both timestamp binds and rejects actual stale values", () => {
    const ts = column("TS", "TIMESTAMP(9)", "datetime");
    const old = "2026-10-14 12:34:56.123456789";
    const next = "2026-10-15 12:34:56.987654321";
    const sql = buildUpdateRowSql(
      oracle,
      "",
      "QA",
      "T",
      { ID: 1 },
      { TS: next },
      [id, ts],
      { TS: old },
    );
    expect(sql?.params).toEqual([next, 1, old]);
    expect(sql?.sql).toContain('"TS" = TO_TIMESTAMP(:3');
    expect(
      oracle.checkPersistedEdit(ts, next, { persistedValue: next })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(ts, next, {
        persistedValue: next.replace("321", "322"),
      })?.ok,
    ).toBe(false);
  });
  it("preserves timezone offsets and nano precision in inserts and verification", () => {
    const ts = column("TS", "TIMESTAMP(9) WITH TIME ZONE", "datetime");
    const requested = "2026-10-14 12:34:56.123456789 +02:00";
    const op = buildInsertRowOperation(
      oracle,
      "",
      "QA",
      "T",
      { TS: requested },
      [ts],
    );
    expect(op.params).toEqual([requested]);
    expect(op.sql).toContain("TO_TIMESTAMP_TZ");
    expect(
      oracle.checkPersistedEdit(ts, requested, {
        persistedValue: "2026-10-14 10:34:56.123456789 +00:00",
      })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(
        column("TS", "TIMESTAMP(6)", "datetime"),
        requested,
      )?.ok,
    ).toBe(false);
  });
});

describe("manual review: sql_variant writes", () => {
  it("carries the original subtype into verification and rejects a trigger changing it", async () => {
    const original = snapshot({
      baseType: "int",
      displayValue: "1",
      precision: 10,
      scale: 0,
      maxLength: 4,
      collation: null,
    });
    const manager = {
      getDriver: () => mssql,
      getConnection: () => ({ id: "qa", type: "mssql" }),
    } as unknown as ConnectionManager;
    const plan = prepareApplyChangesPlan(
      manager,
      "qa",
      "qa",
      "dbo",
      "t",
      [
        {
          primaryKeys: { ID: 1 },
          changes: { V: "42" },
          originalValues: { V: original },
        },
      ],
      [id, variant],
    );
    expect(plan.executable).toBe(true);
    if (!plan.executable) return;
    const values = plan.plan.verificationTargets[0].values;
    expect(values[0].originalValue).toEqual(original);
    await expect(
      verifyTransaction(
        mssql,
        [{ rowIndex: 0, sql: "SELECT", params: [], values }],
        async () => [
          {
            __col_0: JSON.stringify(
              snapshot({ displayValue: "42", maxLength: 4 }),
            ),
          },
        ],
      ),
    ).rejects.toThrow(/subtype/);
    await expect(
      verifyTransaction(
        mssql,
        [{ rowIndex: 0, sql: "SELECT", params: [], values }],
        async () => [
          { __col_0: JSON.stringify({ ...original, displayValue: "42" }) },
        ],
      ),
    ).resolves.toBeUndefined();
  });
  it("preserves Unicode in sql_variant update, NULL-update and insert previews", () => {
    for (const original of [snapshot(), null]) {
      let columns: Array<ColumnTypeMeta | undefined> = [];
      const operation = buildUpdateRowSql(
        mssql,
        "qa",
        "dbo",
        "t",
        { ID: 1 },
        { V: "雪'" },
        [id, variant],
        { V: original },
        (value) => {
          columns = value;
        },
      );
      if (!operation) throw new Error("Expected update");
      expect(
        mssql.materializePreviewColumnSql(
          operation.sql,
          operation.params,
          columns,
        ),
      ).toContain("N'雪'''");
    }
    let columns: ColumnTypeMeta[] = [];
    const operation = buildInsertRowOperation(
      mssql,
      "qa",
      "dbo",
      "t",
      { ID: 1, V: "雪'" },
      [id, variant],
      undefined,
      (value) => {
        columns = value;
      },
    );
    expect(
      mssql.materializePreviewColumnSql(
        operation.sql,
        operation.params,
        columns,
      ),
    ).toContain("N'雪'''");
  });
  it("refuses nvarchar truncation and decimal rounding before SQL executes", () => {
    expect(() =>
      mssql.buildMutationValueExpr(variant, "abcdef", "update", snapshot()),
    ).toThrow(/length/);
    expect(() =>
      mssql.buildMutationValueExpr(
        variant,
        "12.345",
        "update",
        snapshot({ baseType: "decimal", precision: 5, scale: 2 }),
      ),
    ).toThrow();
    expect(
      mssql.buildMutationValueExpr(variant, "xyz", "update", snapshot())
        ?.params,
    ).toEqual(["xyz"]);
  });
  it("requires readback and fails transaction verification for truncated data", async () => {
    expect(mssql.checkPersistedEdit(variant, "abcdef")?.shouldVerify).toBe(
      true,
    );
    expect(mssql.buildVerificationValueExpr(variant)).toContain(
      "FOR JSON PATH",
    );
    await expect(
      verifyTransaction(
        mssql,
        [
          {
            rowIndex: 0,
            sql: "SELECT",
            params: [],
            values: [{ column: variant, expectedValue: "abcdef" }],
          },
        ],
        async () => [{ __col_0: JSON.stringify(snapshot()) }],
      ),
    ).rejects.toThrow("not committed");
    expect(
      mssql.checkPersistedEdit(variant, "abc", {
        persistedValue: JSON.stringify(snapshot()),
      })?.ok,
    ).toBe(true);
  });
});

describe("manual review: catalog and projection collisions", () => {
  it("never confuses Redis original binary bytes with a literal marker string", async () => {
    const driver = new RedisDriver({
      id: "redis-marker",
      name: "QA",
      type: "redis",
    });
    const current = Buffer.from("RedisBinary(0xff)");
    const sendCommand = vi.fn(async (args: unknown[]) =>
      Buffer.isBuffer(args[7]) && args[7].equals(current) ? 1 : -3,
    );
    Object.assign(driver, {
      getDatabaseClient: async () => ({
        type: async () => "string",
        sendCommand,
      }),
    });
    await expect(
      driver.updateRows({
        database: "0",
        schema: "",
        table: "",
        updates: [
          {
            primaryKeys: { key: "qa" },
            changes: { ttl: "60" },
            originalValues: {
              value: {
                __rapidbRedisBinary: 1,
                displayValue: "RedisBinary(0xff)",
                binaryHex: "ff",
              },
            },
          },
        ],
      }),
    ).rejects.toThrow(/changed/);
    expect(sendCommand.mock.calls[0][0][7]).toEqual(Buffer.from([255]));
    const snapshotValue = {
      __rapidbRedisBinary: 1,
      displayValue: "RedisBinary(0xff)",
      binaryHex: "ff",
    };
    expect(jsonExport(snapshotValue, "other", "string")).toBe(
      '{"payload":"RedisBinary(0xff)"}',
    );
    expect(jsonExport(snapshotValue, "json", "jsonb")).toBe(
      JSON.stringify({ payload: snapshotValue }),
    );
  });
  it("keeps MSSQL companion aliases distinct from real columns without shared mutable state", () => {
    const cols = [
      variant,
      column("__rapidb_sql_variant_0_base_type", "nvarchar(20)", "text"),
      column("__rapidb_sql_variant_0_base_type_1", "nvarchar(20)", "text"),
      column("G", "geometry", "spatial"),
      column("__rapidb_spatial_3_binary_hex", "nvarchar(20)", "text"),
    ];
    const sql = mssql.buildTableDataProjection(cols);
    const names = [...sql.matchAll(/ AS \[([^\]]+)\]/g)].map(
      (match) => match[1],
    );
    expect(new Set(names).size).toBe(names.length);
    expect(names).toContain("__rapidb_sql_variant_0_base_type_2");
    expect(names).toContain("__rapidb_spatial_3_binary_hex_1");
    mssql.buildTableDataProjection([variant]);
    const data: Record<string, unknown> = {
      V: "41",
      G: "SRID=0;POINT (1 2)",
      __rapidb_sql_variant_0_base_type: "user base",
      __rapidb_sql_variant_0_base_type_1: "user suffix",
      __rapidb_spatial_3_binary_hex: "user spatial",
      __rapidb_sql_variant_0_base_type_2: "int",
      __rapidb_sql_variant_0_precision: 10,
      __rapidb_sql_variant_0_scale: 0,
      __rapidb_sql_variant_0_max_length: 4,
      __rapidb_sql_variant_0_collation: null,
      __rapidb_sql_variant_0_binary_hex: "29000000",
      __rapidb_spatial_3_binary_hex_1: "0102",
    };
    const service = new TableReadService(
      {} as ConnectionManager,
    ) as unknown as {
      formatQueryRows: (
        driver: MSSQLDriver,
        columns: ColumnTypeMeta[],
        result: unknown,
      ) => Record<string, unknown>[];
    };
    const [row] = service.formatQueryRows(mssql, cols, {
      columns: names,
      rows: [
        Object.fromEntries(
          names.map((name, index) => [`__col_${index}`, data[name]]),
        ),
      ],
    });
    expect(row.V).toMatchObject({ baseType: "int", displayValue: "41" });
    expect(row.G).toMatchObject({ binaryHex: "0102" });
    expect(row.__rapidb_sql_variant_0_base_type).toBe("user base");
    expect(row.__rapidb_spatial_3_binary_hex).toBe("user spatial");
  });
  it("preserves a custom PostgreSQL multirange companion name in range DDL", async () => {
    const driver = new PostgresDriver({ id: "pg-ddl", name: "QA", type: "pg" });
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ type_oid: "99", typtype: "m" }] })
      .mockResolvedValueOnce({
        rows: [
          {
            range_schema: "qa",
            range_name: "price_range",
            multirange_name: "price_bands",
            subtype: "numeric",
          },
        ],
      });
    Object.assign(driver, { requirePool: () => ({ query }) });
    const ddl = await driver.getObjectDefinition(
      "qa",
      "qa",
      "price_bands",
      "type",
    );
    expect(ddl).toContain('MULTIRANGE_TYPE_NAME = "price_bands"');
    expect(query.mock.calls[1][0]).toContain("r.rngmultitypid");
  });
  it("allows Redis binary TTL/rename while retaining raw optimistic bytes and refusing payload edits", async () => {
    const driver = new RedisDriver({
      id: "redis-binary",
      name: "QA",
      type: "redis",
    });
    const sendCommand = vi.fn().mockResolvedValue(1);
    const client = {
      type: async () => "string",
      get: async () => Buffer.from([255]),
      withTypeMapping: () => client,
      sendCommand,
    };
    Object.assign(driver, { getDatabaseClient: async () => client });
    for (const changes of [{ ttl: "60" }, { key: "renamed" }]) {
      const result = await driver.updateRows({
        database: "0",
        schema: "",
        table: "",
        updates: [
          {
            primaryKeys: { key: "qa" },
            changes,
            originalValues: {
              value: {
                __rapidbRedisBinary: 1,
                displayValue: "RedisBinary(0xff)",
                binaryHex: "ff",
              },
            },
          },
        ],
      });
      expect(result.affectedRows).toBe(1);
      expect(sendCommand.mock.calls.at(-1)?.[0][7]).toEqual(Buffer.from([255]));
    }
    await expect(
      driver.updateRows({
        database: "0",
        schema: "",
        table: "",
        updates: [{ primaryKeys: { key: "qa" }, changes: { value: "text" } }],
      }),
    ).rejects.toThrow(/read-only/);
    expect(sendCommand).toHaveBeenCalledTimes(2);
  });
});

describe("manual review: unsigned BSON timestamps", () => {
  it("does not match a literal Timestamp string in typed equality or IN fallback", async () => {
    const driver = new MongoDBDriver({
      id: "timestamp-evidence",
      name: "QA",
      type: "mongodb",
    });
    const stamp = new Timestamp({ t: 1720094400, i: 2 });
    const docs = [
      { _id: "bson", stamp, obj: { k: "needle" } },
      {
        _id: "literal",
        stamp: "Timestamp(1720094400, 2)",
        obj: { k: "needle" },
      },
    ];
    const state = driver as unknown as {
      toRow: (doc: Record<string, unknown>) => Record<string, unknown>;
    };
    Object.assign(driver, {
      describeSchemaColumns: async () => [
        column("stamp", "timestamp", "datetime"),
        column("obj", "object", "json"),
      ],
      readRows: async () => docs.map((doc) => state.toRow(doc)),
    });
    for (const operator of ["eq", "in"] as const) {
      const page = await driver.readTablePage({
        database: "qa",
        schema: "",
        table: "t",
        page: 1,
        pageSize: 100,
        skipCount: false,
        sort: null,
        filters: [
          { column: "stamp", operator, value: "Timestamp(1720094400, 2)" },
          { column: "obj", operator: "like", value: "needle" },
        ],
      });
      expect(page.rows.map((row) => row._id)).toEqual(["bson"]);
    }
  });
  it("serializes maximum Timestamp components as unsigned preview values", async () => {
    const driver = new MongoDBDriver({
      id: "preview",
      name: "QA",
      type: "mongodb",
    });
    const stamp = new Timestamp({ t: 4294967295, i: 2147483648 });
    const preview = driver.buildMutationPreviewStatement(
      "insert",
      "qa",
      "",
      "t",
      { values: { stamp } },
    );
    expect(preview).toContain("new Timestamp(4294967295, 2147483648)");
    const insertOne = vi
      .fn()
      .mockResolvedValue({ acknowledged: true, insertedId: "qa" });
    Object.assign(driver, {
      connected: true,
      client: { db: () => ({ collection: () => ({ insertOne }) }) },
    });
    await driver.query(preview);
    expect(insertOne.mock.calls[0][0].stamp.equals(stamp)).toBe(true);
  });
  it.each([
    [1770900000, 2147483648],
    [2147483648, 1],
    [4294967295, 4294967295],
  ])("round-trips t=%i and i=%i", (t, i) => {
    const driver = new MongoDBDriver({
      id: "offline-review",
      name: "Review",
      type: "mongodb",
    });
    const col = column("TS", "timestamp", "datetime");
    const source = new Timestamp({ t, i });
    const display = driver.formatOutputValue(source, col);
    expect(display).toBe(`Timestamp(${t}, ${i})`);
    const restored = driver.coerceInputValue(display, col);
    expect(restored).toBeInstanceOf(Timestamp);
    expect((restored as Timestamp).equals(source)).toBe(true);
  });
  it("keeps BSON timestamp ranges, IN and sorting correct when a JSON filter forces client evaluation", async () => {
    const driver = new MongoDBDriver({
      id: "timestamp-fallback",
      name: "QA",
      type: "mongodb",
    });
    const documents = [2, 10, 2147483648].map((i) => ({
      _id: String(i),
      stamp: new Timestamp({ t: 1720094400, i }),
      obj: { k: "needle" },
    }));
    const state = driver as unknown as {
      toRow: (doc: Record<string, unknown>) => Record<string, unknown>;
      buildMongoFilterCriteria: (
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => Record<string, unknown>;
    };
    Object.assign(driver, {
      readSchemaDocuments: async () => documents,
      readRows: async () => documents.map((doc) => state.toRow(doc)),
    });
    const run = async (filter?: FilterExpression) =>
      driver.readTablePage({
        database: "qa",
        schema: "",
        table: "t",
        page: 1,
        pageSize: 100,
        skipCount: false,
        sort: { column: "stamp", direction: "asc" },
        filters: [
          { column: "obj", operator: "like", value: "needle" },
          ...(filter ? [filter] : []),
        ],
      });
    expect(
      (
        await run({
          column: "stamp",
          operator: "gt",
          value: "Timestamp(1720094400, 1)",
        })
      ).rows.map((row) => row._id),
    ).toEqual(["2", "10", "2147483648"]);
    expect(
      (
        await run({
          column: "stamp",
          operator: "between",
          value: ["Timestamp(1720094400, 2)", "Timestamp(1720094400, 10)"],
        })
      ).rows.map((row) => row._id),
    ).toEqual(["2", "10"]);
    const inFilter = {
      column: "stamp",
      operator: "in",
      value: "Timestamp(1720094400,2), Timestamp(1720094400, 10)",
    } as const;
    expect((await run(inFilter)).rows.map((row) => row._id)).toEqual([
      "2",
      "10",
    ]);
    expect(
      state.buildMongoFilterCriteria(
        [inFilter],
        [column("stamp", "timestamp", "datetime")],
      ),
    ).toEqual({ stamp: { $in: [documents[0].stamp, documents[1].stamp] } });
  });
});

describe("manual review: Oracle exact temporal filters and export cursors", () => {
  it.each([
    "asc",
    "desc",
  ] as const)("uses native TSTZ ordering for every keyset export chunk (%s)", async (direction) => {
    const driver = new OracleDriver({
      id: "native-order",
      name: "QA",
      type: "oracle",
    });
    const columns = [
      { ...id, nullable: false, filterOperators: ["eq", "gt", "lt"] as const },
      {
        ...column("TS", "TIMESTAMP(9) WITH TIME ZONE", "datetime"),
        nullable: false,
        filterOperators: ["eq", "gt", "lt"] as const,
      },
    ];
    const source = [
      {
        ID: 1,
        TS: "2026-10-16 01:00:00.123456789 +02:00",
        utc: "2026-10-15 23:00:00.123456789",
      },
      {
        ID: 2,
        TS: "2026-10-16 00:30:00.123456790 +00:00",
        utc: "2026-10-16 00:30:00.12345679",
      },
    ];
    const query = vi.fn(async (sql: string, params: unknown[] = []) => {
      const cursor = typeof params[0] === "string" ? params[0] : null;
      const ordered = [...source]
        .filter(
          (row) =>
            !cursor ||
            (direction === "asc" ? row.utc > cursor : row.utc < cursor),
        )
        .sort((a, b) => {
          const native = sql.includes('ORDER BY "QA"."T"."TS"');
          const order = (native ? a.utc : a.TS).localeCompare(
            native ? b.utc : b.TS,
          );
          return direction === "asc" ? order : -order;
        })
        .slice(0, 1);
      return {
        columns: ["ID", "TS"],
        rows: ordered.map((row) => ({ __col_0: row.ID, __col_1: row.TS })),
        rowCount: ordered.length,
        executionTimeMs: 0,
      };
    });
    Object.assign(driver, { describeColumns: async () => columns, query });
    const manager = {
      getConnection: () => ({ type: "oracle" }),
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const service = new TableReadService(manager);
    const ids: unknown[] = [];
    for await (const chunk of service.exportAll("qa", "", "QA", "T", 1, {
      column: "TS",
      direction,
    }))
      ids.push(...chunk.rows.map((row) => row.ID));
    expect(ids).toEqual(direction === "asc" ? [1, 2] : [2, 1]);
    expect(
      query.mock.calls.every(([sql]) =>
        sql.includes(`ORDER BY "QA"."T"."TS" ${direction.toUpperCase()}`),
      ),
    ).toBe(true);
  });
  it("qualifies MSSQL variant and default PK order expressions", () => {
    expect(mssql.buildTableDataOrderExpression("V", "[qa].[dbo].[t]")).toBe(
      "[qa].[dbo].[t].[V]",
    );
    expect(mssql.buildOrderByDefault([id], "[qa].[dbo].[t]")).toBe(
      "ORDER BY [qa].[dbo].[t].[ID]",
    );
    expect(oracle.buildOrderByDefault([id], '"QA"."T"')).toBe(
      'ORDER BY "QA"."T"."ID"',
    );
  });
  it.each([
    "TIMESTAMP(9)",
    "TIMESTAMP(9) WITH TIME ZONE",
    "TIMESTAMP(9) WITH LOCAL TIME ZONE",
  ])("keeps FF9 boundaries in %s", (nativeType) => {
    const col = column("TS", nativeType, "datetime");
    const zoned = nativeType.includes("TIME ZONE");
    const input = `2026-10-16 00:34:56.123456789${zoned ? " +02:00" : ""}`;
    const exact = zoned ? "2026-10-15 22:34:56.123456789" : input;
    for (const operator of ["eq", "gt", "lt"] as const) {
      expect(
        oracle.buildCursorComparison(col, operator, input, 1).params,
      ).toEqual([exact]);
    }
    for (const operator of [
      "eq",
      "neq",
      "gt",
      "gte",
      "lt",
      "lte",
      "in",
    ] as const) {
      const condition = oracle.buildFilterCondition(col, operator, input, 1);
      expect(condition?.sql).toContain("FF9");
      expect(condition?.params).toEqual([exact]);
      if (zoned)
        expect(condition?.sql).toContain("TIMESTAMP(9) WITH TIME ZONE");
    }
    expect(
      oracle.buildFilterCondition(col, "between", [input, input], 1)?.params,
    ).toEqual([exact, exact]);
    expect(oracle.buildFilterCondition(col, "like", input, 1)?.params).toEqual([
      `%${exact}%`,
    ]);
    if (zoned)
      expect(
        oracle.buildCursorComparison(
          col,
          "gt",
          "2026-10-15 22:34:56.123456789Z",
          1,
        ).params,
      ).toEqual([exact]);
  });
});

describe("manual review: MongoDB explicit sentinel edits", () => {
  it("parses unquoted sentinel literals independently of field type and preserves quoted strings", () => {
    const driver = new MongoDBDriver({
      id: "sentinel-edit",
      name: "Review",
      type: "mongodb",
    });

    expect(
      driver.coerceInputValue("MaxKey()", column("min", "minKey", "text")),
    ).toBeInstanceOf(MaxKey);
    expect(
      driver.coerceInputValue("MinKey()", column("max", "maxKey", "text")),
    ).toBeInstanceOf(MinKey);
    expect(
      driver.coerceInputValue('"MaxKey()"', column("min", "minKey", "text")),
    ).toBe("MaxKey()");
    expect(
      driver.coerceInputValue('"MinKey()"', column("min", "minKey", "text")),
    ).toBe("MinKey()");
    expect(
      driver.coerceInputValue('"MaxKey()"', column("max", "maxKey", "text")),
    ).toBe("MaxKey()");
  });
});

describe("manual review: IN value boundaries", () => {
  it.each([
    [
      "objectId",
      "text",
      new ObjectId("507f1f77bcf86cd799439011"),
      "507f1f77bcf86cd799439011",
      "507f1f77bcf86cd799439011",
    ],
    ["binData", "binary", new Binary(Buffer.from([255]), 0), "0xff", "0xff"],
    ["bool", "boolean", true, "TRUE", "true"],
    ["bool", "boolean", true, "1", "1"],
    ["bool", "boolean", false, "0", "0"],
    [
      "objectId",
      "text",
      new ObjectId("123456789012345678901234"),
      "123456789012345678901234",
      "123456789012345678901234",
    ],
    [
      "date",
      "datetime",
      new Date("2026-10-16T00:00:00Z"),
      "2026-10-16T00:00:00.000Z",
      "2026-10-16 00:00:00",
    ],
  ] as const)("retains BSON %s identity in scalar native/client filters", async (nativeType, category, bson, input, literal) => {
    const driver = new MongoDBDriver({
      id: "scalar-evidence",
      name: "QA",
      type: "mongodb",
    });
    const col = column("v", nativeType, category);
    const docs = [
      { _id: "bson", v: bson, obj: { k: "needle" } },
      { _id: "literal", v: literal, obj: { k: "needle" } },
    ];
    const state = driver as unknown as {
      toRow: (doc: Record<string, unknown>) => Record<string, unknown>;
      buildMongoFilterCriteria: (
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => Record<string, unknown>;
    };
    Object.assign(driver, {
      describeSchemaColumns: async () => [col, column("obj", "object", "json")],
      readRows: async () => docs.map((doc) => state.toRow(doc)),
    });
    for (const operator of ["eq", "in", "neq"] as const) {
      const filter = { column: "v", operator, value: input };
      expect(state.buildMongoFilterCriteria([filter], [col])).toEqual(
        operator === "eq"
          ? { v: bson }
          : operator === "in"
            ? { v: { $in: [bson] } }
            : { $and: [{ v: { $ne: bson } }] },
      );
      const page = await driver.readTablePage({
        database: "qa",
        schema: "",
        table: "t",
        page: 1,
        pageSize: 100,
        skipCount: false,
        sort: null,
        filters: [filter, { column: "obj", operator: "like", value: "needle" }],
      });
      expect(page.rows.map((row) => row._id)).toEqual([
        operator === "neq" ? "literal" : "bson",
      ]);
    }
  });
  it("preserves decimal bounds beyond Double precision", () => {
    const driver = new MongoDBDriver({
      id: "double-filter",
      name: "QA",
      type: "mongodb",
    });
    const state = driver as unknown as {
      buildMongoFilterCriteria: (
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => { n: Decimal128 };
    };
    const criteria = state.buildMongoFilterCriteria(
      [{ column: "n", operator: "eq", value: "0.1000000000000000000001" }],
      [column("n", "double", "float")],
    );
    expect(criteria.n).toBeInstanceOf(Decimal128);
    expect(criteria.n.toString()).toBe("0.1000000000000000000001");
  });
  it("matches Decimal128 bounds beyond Double range in native and client filters", async () => {
    const driver = new MongoDBDriver({
      id: "double-overflow",
      name: "QA",
      type: "mongodb",
    });
    const col = column("n", "double", "float");
    const state = driver as unknown as {
      toRow: (doc: Record<string, unknown>) => Record<string, unknown>;
      buildMongoFilterCriteria: (
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => { n: Decimal128 | { $in: Decimal128[] } };
    };
    const docs = [
      { _id: "double", n: new Double(1) },
      { _id: "decimal", n: Decimal128.fromString("1e400") },
      { _id: "literal", n: "1e400" },
    ].map((doc) => ({ ...doc, obj: { k: "needle" } }));
    Object.assign(driver, {
      describeSchemaColumns: async () => [col, column("obj", "object", "json")],
      readRows: async () => docs.map((doc) => state.toRow(doc)),
    });
    for (const operator of ["eq", "in"] as const) {
      const filter = { column: "n", operator, value: "1e400" };
      const criteria = state.buildMongoFilterCriteria([filter], [col]);
      const bound =
        operator === "eq"
          ? criteria.n
          : (criteria.n as { $in: Decimal128[] }).$in[0];
      expect(bound).toBeInstanceOf(Decimal128);
      expect((bound as Decimal128).toString()).toBe("1E+400");
      const page = await driver.readTablePage({
        database: "qa",
        schema: "",
        table: "t",
        page: 1,
        pageSize: 100,
        skipCount: false,
        sort: null,
        filters: [filter, { column: "obj", operator: "like", value: "needle" }],
      });
      expect(page.rows.map((row) => row._id)).toEqual(["decimal"]);
    }
  });
  it("keeps sampled Int32 overflow bounds on the native numeric path", () => {
    const driver = new MongoDBDriver({
      id: "int32-filter",
      name: "QA",
      type: "mongodb",
    });
    const state = driver as unknown as {
      buildMongoFilterCriteria: (
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => Record<string, unknown>;
    };
    const col = column("n", "int32", "integer");
    for (const operator of ["eq", "in", "gt"] as const) {
      const criteria = state.buildMongoFilterCriteria(
        [{ column: "n", operator, value: "2147483648" }],
        [col],
      ) as { n: Decimal128 | { $in?: Decimal128[]; $gt?: Decimal128 } };
      const bound =
        operator === "eq"
          ? criteria.n
          : operator === "in"
            ? (criteria.n as { $in: Decimal128[] }).$in[0]
            : (criteria.n as { $gt: Decimal128 }).$gt;
      expect(bound).toBeInstanceOf(Decimal128);
      expect((bound as Decimal128).toString()).toBe("2147483648");
    }
  });
  it("preserves exact Mongo numeric filters and matches equivalent numeric spellings only against BSON numbers", async () => {
    const driver = new MongoDBDriver({
      id: "numeric-filters",
      name: "QA",
      type: "mongodb",
    });
    const col = column("n", "long", "integer");
    const docs = [
      { _id: "zero", n: new Int32(0) },
      { _id: "fraction", n: new Double(1.5) },
      { _id: "big", n: Long.fromString("9007199254740993") },
      { _id: "neighbor", n: Long.fromString("9007199254740992") },
      { _id: "text", n: "1.50" },
      { _id: "decimal", n: Decimal128.fromString("0.1000000000000000000001") },
    ].map((doc) => ({ ...doc, obj: { k: "needle" } }));
    const state = driver as unknown as {
      toRow: (doc: Record<string, unknown>) => Record<string, unknown>;
      buildMongoFilterCriteria: (
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => Record<string, unknown>;
    };
    Object.assign(driver, {
      describeSchemaColumns: async () => [col, column("obj", "object", "json")],
      readRows: async () => docs.map((doc) => state.toRow(doc)),
    });
    for (const [input, id] of [
      ["1.50", "fraction"],
      ["9007199254740993.0", "big"],
      ["9.007199254740993e15", "big"],
      ["0.1000000000000000000001", "decimal"],
    ]) {
      for (const operator of ["eq", "in"] as const) {
        const filter = { column: "n", operator, value: input };
        const criteria = state.buildMongoFilterCriteria([filter], [col]) as {
          n: Decimal128 | { $in: Decimal128[] };
        };
        const bound =
          operator === "in"
            ? (criteria.n as { $in: Decimal128[] }).$in[0]
            : criteria.n;
        expect(bound).toBeInstanceOf(Decimal128);
        expect((bound as Decimal128).toString()).toBe(
          Decimal128.fromString(input).toString(),
        );
        const page = await driver.readTablePage({
          database: "qa",
          schema: "",
          table: "t",
          page: 1,
          pageSize: 100,
          skipCount: false,
          sort: null,
          filters: [
            filter,
            { column: "obj", operator: "like", value: "needle" },
          ],
        });
        expect(page.rows.map((row) => row._id)).toEqual([id]);
      }
    }
  });
  it("uses identical Mongo string candidates in native and client filtering", async () => {
    const driver = new MongoDBDriver({
      id: "quoted-in",
      name: "QA",
      type: "mongodb",
    });
    const documents = ["Alice, Smith", "Jane", "O'Reilly", 'D"Angelo'].map(
      (name) => ({ _id: name, name, obj: { k: "needle" } }),
    );
    const state = driver as unknown as {
      toRow: (doc: Record<string, unknown>) => Record<string, unknown>;
      buildMongoFilterCriteria: (
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => Record<string, unknown>;
    };
    Object.assign(driver, {
      readSchemaDocuments: async () => documents,
      readRows: async () => documents.map((doc) => state.toRow(doc)),
    });
    for (const value of [
      `"Alice, Smith",Jane`,
      `O'Reilly,Jane,D"Angelo`,
      `"Alice, Smith","Jane"`,
    ]) {
      const filter = { column: "name", operator: "in", value } as const;
      const expected = value.startsWith('"')
        ? ["Alice, Smith", "Jane"]
        : ["Jane", "O'Reilly", 'D"Angelo'];
      expect(
        state.buildMongoFilterCriteria(
          [filter],
          [column("name", "string", "text")],
        ),
      ).toEqual({ name: { $in: expect.arrayContaining(expected) } });
      const page = await driver.readTablePage({
        database: "qa",
        schema: "",
        table: "t",
        page: 1,
        pageSize: 100,
        skipCount: false,
        sort: null,
        filters: [filter, { column: "obj", operator: "like", value: "needle" }],
      });
      expect(page.rows.map((row) => row.name)).toEqual(expected);
    }
  });
  it("preserves apostrophes and embedded quotes in raw text entries", () => {
    expect(splitInFilterValues(`O'Reilly,Jane,D"Angelo`)).toEqual([
      "O'Reilly",
      "Jane",
      'D"Angelo',
    ]);
    expect(
      splitInFilterValues(
        `Timestamp(1720094400, 2), Timestamp(1720094400, 10)`,
      ),
    ).toEqual(["Timestamp(1720094400, 2)", "Timestamp(1720094400, 10)"]);
    expect(splitInFilterValues(`"a,b",{'k':'c,d'},last`)).toEqual([
      '"a,b"',
      "{'k':'c,d'}",
      "last",
    ]);
  });
});

describe("manual review: DynamoDB collection Contains types", () => {
  it("preserves literal string needles and requires explicit mixed-list numeric/binary needles", () => {
    const driver = new DynamoDBDriver({
      id: "contains-types",
      name: "QA",
      type: "dynamodb",
      awsRegion: "us-east-1",
    });
    const state = driver as unknown as {
      formatDynamoRowForDisplay: (
        row: Record<string, unknown>,
      ) => Record<string, unknown>;
      applyDynamoFilters: (
        rows: Record<string, unknown>[],
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => Record<string, unknown>[];
    };
    const rows = [
      { ID: "string", V: new Set(["0xdead", "123", "0x (empty binary)"]) },
      { ID: "list", V: [Buffer.from("dead", "hex"), 123] },
    ].map((row) => state.formatDynamoRowForDisplay(row));
    const matches = (value: string, nativeType = "list") =>
      state
        .applyDynamoFilters(
          rows,
          [{ column: "V", operator: "like", value }],
          [column("V", nativeType, "array")],
        )
        .map((row) => row.ID);
    expect(matches("0xdead")).toEqual(["string"]);
    expect(matches("123")).toEqual(["string"]);
    expect(matches("0x (empty binary)")).toEqual(["string"]);
    expect(matches("B(0xdead)")).toEqual(["list"]);
    expect(matches("N(123)")).toEqual(["list"]);
  });
});
