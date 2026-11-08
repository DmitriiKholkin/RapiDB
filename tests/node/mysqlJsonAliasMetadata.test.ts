import { describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { serializeJsonExportRecord } from "../../src/extension/utils/exportValueFormat";

async function describeTextWithChecks(
  checks: string[],
  columnName = "payload",
) {
  const driver = new MySQLDriver({
    id: "rev002",
    name: "REV-002",
    type: "mysql",
  });
  Reflect.set(driver, "queryObjectRows", async (sql: string) =>
    sql.includes("CHECK_CONSTRAINTS")
      ? checks.map((CHECK_CLAUSE) => ({ CHECK_CLAUSE }))
      : [{ COLUMN_NAME: columnName, COLUMN_TYPE: "longtext" }],
  );
  return (await driver.describeColumns("qa", "", "text_alias"))[0];
}

describe("MySQL-family JSON text aliases", () => {
  it.each([
    { enforced: "NO", json: false },
    { enforced: "YES", json: true },
    { enforced: null, json: false },
    { enforced: undefined, json: true },
  ])("respects CHECK enforcement metadata: $enforced", async ({
    enforced,
    json,
  }) => {
    const driver = new MySQLDriver({
      id: "check-enforcement",
      name: "QA",
      type: "mysql",
    });
    const queryObjectRows = vi.fn(async (sql: string) =>
      sql.includes("CHECK_CONSTRAINTS")
        ? [
            {
              CHECK_CLAUSE: "json_valid(`payload`)",
              ...(enforced === undefined ? {} : { ENFORCED: enforced }),
            },
          ]
        : [{ COLUMN_NAME: "payload", COLUMN_TYPE: "longtext" }],
    );
    Reflect.set(driver, "queryObjectRows", queryObjectRows);
    const [column] = await driver.describeColumns("qa", "", "text_alias");
    expect(column.exportCategory).toBe(json ? "json" : undefined);
    const value = '{"ordinary":"text"}';
    const exported = JSON.parse(
      serializeJsonExportRecord([
        {
          key: "payload",
          sourceKey: "payload",
          value,
          category: column.exportCategory ?? column.category,
          nativeType: column.nativeType,
        },
      ]),
    );
    expect(exported).toEqual({ payload: json ? { ordinary: "text" } : value });
    const metadataSql = queryObjectRows.mock.calls[1][0];
    expect(metadataSql).toContain("tc.*");
    expect(metadataSql).not.toContain("tc.ENFORCED");
  });

  it.each([
    "json_valid(`payload`) OR legacy = 1",
    "(json_valid(`payload`) OR legacy = 1)",
    "legacy = 1 OR json_valid(`payload`)",
    "NOT json_valid(`payload`)",
    "json_valid(`payload`) = 0",
    "'json_valid(payload)'",
    'json_valid("payload")',
    "json_valid(`other`)",
  ])("preserves SQL text for a noncanonical CHECK: %s", async (clause) => {
    const column = await describeTextWithChecks([clause]);
    expect(column).not.toHaveProperty("exportCategory");
    for (const value of ['{"old":"text"}', "123", "null"]) {
      const exported = serializeJsonExportRecord([
        {
          key: "payload",
          sourceKey: "payload",
          category: column.exportCategory ?? column.category,
          nativeType: column.nativeType,
          value,
        },
      ]);
      expect(JSON.parse(exported)).toEqual({ payload: value });
    }
  });

  it.each([
    "json_valid(`payload`)",
    "JSON_VALID(payload)",
    " ( ( JSON_VALID ( `payload` ) ) ) ",
  ])("recognizes a canonical JSON alias CHECK: %s", async (clause) => {
    const column = await describeTextWithChecks([
      "legacy = 1 OR json_valid(`payload`)",
      clause,
    ]);
    expect(column).toMatchObject({ category: "text", exportCategory: "json" });
  });

  it.each([
    "payload data",
    "a`b",
    "a.b",
    "payload) OR (1=1",
  ])("preserves the exact quoted column identifier %s", async (name) => {
    const quoted = `\`${name.replace(/`/g, "``")}\``;
    expect(
      await describeTextWithChecks([`json_valid(${quoted})`], name),
    ).toHaveProperty("exportCategory", "json");
    expect(
      await describeTextWithChecks(["json_valid(`payload`)"], name),
    ).not.toHaveProperty("exportCategory");
  });

  it("marks MariaDB JSON_VALID text aliases for JSON export only", async () => {
    const columnRows = [
      {
        COLUMN_NAME: "json_col",
        COLUMN_TYPE: "longtext",
        CHARACTER_SET_NAME: "utf8mb4",
        COLLATION_NAME: "utf8mb4_bin",
        IS_NULLABLE: "YES",
        COLUMN_DEFAULT: null,
        GENERATION_EXPRESSION: null,
        EXTRA: null,
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
      {
        COLUMN_NAME: "plain_text",
        COLUMN_TYPE: "longtext",
        CHARACTER_SET_NAME: "utf8mb4",
        COLLATION_NAME: "utf8mb4_bin",
        IS_NULLABLE: "YES",
        COLUMN_DEFAULT: null,
        GENERATION_EXPRESSION: null,
        EXTRA: null,
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
      {
        COLUMN_NAME: "native_json",
        COLUMN_TYPE: "json",
        CHARACTER_SET_NAME: null,
        COLLATION_NAME: null,
        IS_NULLABLE: "YES",
        COLUMN_DEFAULT: null,
        GENERATION_EXPRESSION: null,
        EXTRA: null,
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
    ];
    const queryObjectRows = vi.fn(async (sql: string, _params?: unknown[]) =>
      sql.includes("CHECK_CONSTRAINTS")
        ? [{ CHECK_CLAUSE: "json_valid(`json_col`)" }]
        : columnRows,
    );
    const driver = new MySQLDriver({
      id: "mariadb-json-alias",
      name: "MariaDB JSON alias",
      type: "mysql",
      host: "localhost",
    });
    (
      driver as unknown as {
        queryObjectRows: (
          sql: string,
          params?: unknown[],
        ) => Promise<unknown[]>;
      }
    ).queryObjectRows = queryObjectRows;

    const columns = await driver.describeColumns(
      "rapidb_mariadb_db",
      "",
      "rapidb_manual_types_20261024",
    );
    const jsonColumn = columns.find((column) => column.name === "json_col");
    const textColumn = columns.find((column) => column.name === "plain_text");
    const nativeJsonColumn = columns.find(
      (column) => column.name === "native_json",
    );
    if (!jsonColumn || !textColumn || !nativeJsonColumn) {
      throw new Error("Expected JSON alias, text, and native JSON columns.");
    }

    expect(queryObjectRows).toHaveBeenCalledTimes(2);
    expect(queryObjectRows.mock.calls[0]?.[0]).not.toContain(
      "CHECK_CONSTRAINTS",
    );
    expect(queryObjectRows.mock.calls[1]?.[0]).toContain(
      "information_schema.CHECK_CONSTRAINTS",
    );
    expect(queryObjectRows.mock.calls[1]?.[0]).toContain("NOT EXISTS");
    expect(queryObjectRows.mock.calls[1]?.[0]).toContain(
      "tc2.TABLE_NAME <> tc.TABLE_NAME",
    );
    expect(jsonColumn).toMatchObject({
      type: "longtext",
      nativeType: "longtext",
      category: "text",
      exportCategory: "json",
    });
    expect(textColumn).toMatchObject({
      category: "text",
    });
    expect(textColumn).not.toHaveProperty("exportCategory");
    expect(nativeJsonColumn).toMatchObject({
      category: "json",
    });
    expect(nativeJsonColumn).not.toHaveProperty("exportCategory");

    const source = '{"nested":{"num":42},"items":[1,true,null]}';
    const displayValue = driver.formatOutputValue(source, jsonColumn);
    expect(
      serializeJsonExportRecord([
        {
          key: "json_col",
          sourceKey: "json_col",
          category: jsonColumn.exportCategory ?? jsonColumn.category,
          nativeType: jsonColumn.nativeType,
          value: displayValue,
        },
      ]),
    ).toBe('{"json_col":{"nested":{"num":42},"items":[1,true,null]}}');
  });

  it("keeps describing columns when older servers lack check metadata", async () => {
    const columnRows = [
      {
        COLUMN_NAME: "json_col",
        COLUMN_TYPE: "longtext",
        CHARACTER_SET_NAME: "utf8mb4",
        COLLATION_NAME: "utf8mb4_bin",
        IS_NULLABLE: "YES",
        COLUMN_DEFAULT: null,
        GENERATION_EXPRESSION: null,
        EXTRA: null,
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
    ];
    const missingCatalogError = Object.assign(
      new Error("Unknown table 'CHECK_CONSTRAINTS' in information_schema"),
      { code: "ER_UNKNOWN_TABLE", errno: 1109 },
    );
    const queryObjectRows = vi.fn(async (sql: string, _params?: unknown[]) => {
      if (sql.includes("CHECK_CONSTRAINTS")) throw missingCatalogError;
      return columnRows;
    });
    const driver = new MySQLDriver({
      id: "mariadb-json-alias-old-server",
      name: "Older MariaDB JSON alias",
      type: "mysql",
      host: "localhost",
    });
    (
      driver as unknown as {
        queryObjectRows: (
          sql: string,
          params?: unknown[],
        ) => Promise<unknown[]>;
      }
    ).queryObjectRows = queryObjectRows;

    const [column] = await driver.describeColumns(
      "rapidb_mariadb_db",
      "",
      "rapidb_manual_types_20261024",
    );

    expect(column).toMatchObject({ type: "longtext", category: "text" });
    expect(column).not.toHaveProperty("exportCategory");
  });

  it("does not use a same-named check from another table", async () => {
    const columnRows = [
      {
        COLUMN_NAME: "json_col",
        COLUMN_TYPE: "longtext",
        CHARACTER_SET_NAME: "utf8mb4",
        COLLATION_NAME: "utf8mb4_bin",
        IS_NULLABLE: "YES",
        COLUMN_DEFAULT: null,
        GENERATION_EXPRESSION: null,
        EXTRA: null,
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
    ];
    const queryObjectRows = vi.fn(async (sql: string, _params?: unknown[]) =>
      sql.includes("CHECK_CONSTRAINTS") ? [] : columnRows,
    );
    const driver = new MySQLDriver({
      id: "mariadb-json-alias-collision",
      name: "MariaDB JSON alias collision",
      type: "mysql",
      host: "localhost",
    });
    (
      driver as unknown as {
        queryObjectRows: (
          sql: string,
          params?: unknown[],
        ) => Promise<unknown[]>;
      }
    ).queryObjectRows = queryObjectRows;

    const [column] = await driver.describeColumns(
      "rapidb_mariadb_db",
      "",
      "rapidb_manual_types_20261024",
    );

    expect(column).toMatchObject({ type: "longtext", category: "text" });
    expect(column).not.toHaveProperty("exportCategory");
    expect(queryObjectRows.mock.calls[1]?.[0]).toContain(
      "tc2.TABLE_SCHEMA <> tc.TABLE_SCHEMA",
    );
    expect(queryObjectRows.mock.calls[1]?.[0]).toContain(
      "tc2.TABLE_NAME <> tc.TABLE_NAME",
    );
  });
});
