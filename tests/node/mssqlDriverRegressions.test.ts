import { describe, expect, it, vi } from "vitest";
import {
  MSSQLDriver,
  parseMssqlBatches,
} from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const splitMssqlBatches = (sql: string) =>
  parseMssqlBatches(sql).map((batch) => batch.sql);

function makeDriver(): MSSQLDriver {
  return new MSSQLDriver({
    id: "mssql-driver-regressions",
    name: "MSSQL Driver Regressions",
    type: "mssql",
    host: "127.0.0.1",
    port: 1433,
    database: "master",
    username: "sa",
    password: "secret",
  } as ConnectionConfig);
}

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
  isPrimaryKey = false,
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "lt", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

function attachPool(
  driver: MSSQLDriver,
  queryHandler: (sql: string) => Array<Record<string, unknown>>,
) {
  const query = vi.fn((sql: string) =>
    Promise.resolve({ recordset: queryHandler(sql) }),
  );
  const input = vi.fn().mockReturnThis();
  const request = vi.fn(() => ({ query, input }));
  (driver as unknown as { pool: unknown }).pool = {
    request,
    close: vi.fn(),
    on: vi.fn(),
  };
  return { query, input };
}

function recordBindings(driver: MSSQLDriver, sql: string, params: unknown[]) {
  const recorded: Array<{ typeName: string; value: unknown }> = [];
  const request = {
    input(_name: string, type: unknown, value: unknown): void {
      const typeName =
        typeof type === "function"
          ? type.name
          : typeof (type as { type?: unknown }).type === "function"
            ? ((type as { type: { name: string } }).type.name ?? "")
            : "";
      recorded.push({ typeName, value });
    },
  };
  (
    driver as unknown as {
      bindPositionalParameters: (
        request: unknown,
        sql: string,
        params: readonly unknown[],
      ) => string;
    }
  ).bindPositionalParameters(request, sql, params);
  return recorded;
}

const tediousValueParser = require("tedious/lib/value-parser") as {
  readValue: (
    buf: Buffer,
    offset: number,
    metadata: { type?: { name?: string } },
    options: unknown,
  ) => { value: unknown; offset: number };
};

function legacyDateTimeBuffer(
  wallClockUtcMs: number,
  kind: "DateTime" | "SmallDateTime",
): Buffer {
  const dayMs = 86_400_000;
  const days = Math.floor((wallClockUtcMs - Date.UTC(1900, 0, 1)) / dayMs);
  const msOfDay = wallClockUtcMs - Date.UTC(1900, 0, 1) - days * dayMs;
  if (kind === "SmallDateTime") {
    const buf = Buffer.alloc(4);
    buf.writeUInt16LE(days, 0);
    buf.writeUInt16LE(msOfDay / 60_000, 2);
    return buf;
  }
  const buf = Buffer.alloc(8);
  buf.writeInt32LE(days, 0);
  buf.writeUInt32LE(Math.round((msOfDay * 3) / 10), 4);
  return buf;
}

describe("MSSQL driver regressions", () => {
  it("keeps GUID-like and date-like text column values as exact NVarChar text", () => {
    const driver = makeDriver();
    const text = column("label", "nvarchar(100)", "text");
    const guid = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    expect(driver.coerceInputValue(guid, text)).toBe(guid);
    expect(driver.coerceInputValue(" 2024-01-01 ", text)).toBe(" 2024-01-01 ");
    expect(driver.buildFilterCondition(text, "eq", guid, 1)?.params).toEqual([
      guid,
    ]);
    expect(
      recordBindings(driver, "SELECT ?, ?", [guid, " 2024-01-01 "]),
    ).toEqual([
      { typeName: "NVarChar", value: guid },
      { typeName: "NVarChar", value: " 2024-01-01 " },
    ]);
  });

  it("splits GO lines followed by comments", () => {
    expect(
      splitMssqlBatches(
        "CREATE PROCEDURE p AS SELECT 1\nGO -- end of proc\nSELECT 2\nGO /* x */\nSELECT 3\ngo 2 -- twice\nSELECT 4",
      ),
    ).toEqual([
      "CREATE PROCEDURE p AS SELECT 1",
      "SELECT 2",
      "SELECT 3",
      "SELECT 4",
    ]);
  });

  it("splits GO lines with long trailing whitespace in linear time", () => {
    const sql = `GO${" ".repeat(100_000)}x`;
    const startedAt = performance.now();
    expect(splitMssqlBatches(sql)).toEqual([sql]);
    expect(performance.now() - startedAt).toBeLessThan(100);
  });

  it.each([
    [
      "DateTime",
      "2024-03-31 02:30:00.997",
      Date.UTC(2024, 2, 31, 2, 30, 0, 997),
    ],
    ["DateTime", "1753-01-01 00:00:00", Date.UTC(1753, 0, 1)],
    ["SmallDateTime", "2024-03-10 02:30:00", Date.UTC(2024, 2, 10, 2, 30)],
  ] as const)("decodes legacy %s values as wall-clock text %s", (kind, expected, ms) => {
    const encoded = legacyDateTimeBuffer(ms, kind);
    expect(
      tediousValueParser.readValue(
        encoded,
        0,
        { type: { name: kind } },
        {
          useUTC: false,
        },
      ),
    ).toEqual({ value: expected, offset: encoded.length });
    const nullable = Buffer.concat([Buffer.from([encoded.length]), encoded]);
    expect(
      tediousValueParser.readValue(
        nullable,
        0,
        { type: { name: "DateTimeN" } },
        { useUTC: false },
      ),
    ).toEqual({ value: expected, offset: nullable.length });
  });

  it("returns null for empty DateTimeN values", () => {
    expect(
      tediousValueParser.readValue(
        Buffer.from([0]),
        0,
        { type: { name: "DateTimeN" } },
        { useUTC: false },
      ),
    ).toEqual({ value: null, offset: 1 });
  });

  it("casts decimal filter literals to their own scale instead of the column type", () => {
    const driver = makeDriver();
    expect(
      driver.buildFilterCondition(
        column("price", "decimal(10,2)", "decimal"),
        "eq",
        "1.005",
        1,
      ),
    ).toEqual({ sql: "[price] = CAST(? AS decimal(4,3))", params: ["1.005"] });
    expect(
      driver.buildFilterCondition(
        column("amount", "decimal(5,2)", "decimal"),
        "lt",
        "1000000",
        1,
      ),
    ).toEqual({
      sql: "[amount] < CAST(? AS decimal(7,0))",
      params: ["1000000"],
    });
    expect(
      driver.buildFilterCondition(
        column("ratio", "numeric(28,10)", "decimal"),
        "eq",
        "1.",
        1,
      ),
    ).toEqual({ sql: "[ratio] = CAST(? AS decimal(1,0))", params: ["1"] });
    expect(
      driver.buildFilterCondition(
        column("ratio", "numeric(28,10)", "decimal"),
        "eq",
        "-0.0001",
        1,
      ),
    ).toEqual({
      sql: "[ratio] = CAST(? AS decimal(4,4))",
      params: ["-0.0001"],
    });
  });

  it("matches time Contains filters against fractional seconds", () => {
    const driver = makeDriver();
    expect(
      driver.buildFilterCondition(
        column("at", "time(7)", "time"),
        "like",
        "10:00:00.5",
        1,
      ),
    ).toEqual({
      sql: "CONVERT(VARCHAR(16), CAST([at] AS time(7))) LIKE ?",
      params: ["%10:00:00.5%"],
    });
  });

  it("orders PK-less tables by all comparable columns", () => {
    const driver = makeDriver();
    expect(
      driver.buildOrderByDefault([
        column("doc", "xml", "other"),
        column("a", "int", "integer"),
        column("shape", "geography", "spatial"),
        column("notes", "ntext", "text"),
        column("b", "nvarchar(20)", "text"),
        column("clr", "Money2", "other"),
        column("vec", "vector(3)", "other"),
        column("js", "json", "other"),
        column("img", "image", "binary"),
        column("h", "hierarchyid", "other"),
        column("v", "sql_variant", "other"),
      ]),
    ).toBe("ORDER BY [a], [b], [h], [v]");
    expect(driver.buildOrderByDefault([column("doc", "xml", "other")])).toBe(
      "ORDER BY (SELECT NULL)",
    );
  });

  it("reads trigger events, defaults, and routine bodies from the target database", async () => {
    const driver = makeDriver();
    const { query } = attachPool(driver, () => []);
    await driver.getTriggers("other_db", "dbo", "t");
    await driver.describeTable("other_db", "dbo", "t");
    await driver.getRoutineDefinition("other_db", "dbo", "p", "procedure");
    const sql = query.mock.calls.map(([text]) => text).join("\n");
    expect(sql).toContain("[other_db].sys.trigger_events");
    expect(sql).toContain("[other_db].sys.default_constraints");
    expect(sql).toContain("[other_db].sys.sql_modules");
    expect(sql).not.toMatch(/OBJECTPROPERTY|OBJECT_DEFINITION/);
  });

  it("lists only index key columns in key order", async () => {
    const driver = makeDriver();
    const { query } = attachPool(driver, () => []);
    await driver.getIndexes("db", "dbo", "t");
    const sql = query.mock.calls[0]?.[0] ?? "";
    expect(sql).toContain("ic.key_ordinal > 0");
    expect(sql).toContain("ORDER BY i.name, ic.key_ordinal");
  });

  it("renders identity seed/increment and non-default collations in table DDL", async () => {
    const driver = makeDriver();
    attachPool(driver, (sql) =>
      sql.includes("INFORMATION_SCHEMA.TABLES")
        ? [{ TABLE_TYPE: "BASE TABLE" }]
        : [
            {
              COLUMN_NAME: "id",
              DATA_TYPE: "int",
              max_length: 4,
              precision: 10,
              scale: 0,
              IS_NULLABLE: 0,
              is_identity: 1,
              is_computed: 0,
              COMPUTED_DEFINITION: null,
              is_persisted: 0,
              COLUMN_DEFAULT: null,
              COLLATION_NAME: null,
              IDENTITY_SEED: "100",
              IDENTITY_INCREMENT: "5",
              IS_PK: 1,
              PK_ORDINAL: 1,
            },
            {
              COLUMN_NAME: "name",
              DATA_TYPE: "nvarchar",
              max_length: 100,
              precision: 0,
              scale: 0,
              IS_NULLABLE: 1,
              is_identity: 0,
              is_computed: 0,
              COMPUTED_DEFINITION: null,
              is_persisted: 0,
              COLUMN_DEFAULT: null,
              COLLATION_NAME: "Latin1_General_CS_AS",
              IDENTITY_SEED: null,
              IDENTITY_INCREMENT: null,
              IS_PK: 0,
              PK_ORDINAL: null,
            },
          ],
    );
    expect(await driver.getCreateTableDDL("db", "dbo", "t")).toBe(
      "CREATE TABLE [dbo].[t] (\n  [id] int IDENTITY(100,5) NOT NULL PRIMARY KEY,\n  [name] nvarchar(50) COLLATE Latin1_General_CS_AS\n);",
    );
  });
});
