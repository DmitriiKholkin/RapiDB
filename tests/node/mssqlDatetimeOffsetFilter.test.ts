import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const datetimeOffsetColumn: ColumnTypeMeta = {
  name: "created_at",
  type: "datetimeoffset",
  nativeType: "datetimeoffset",
  category: "datetime",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

const dateColumn: ColumnTypeMeta = {
  name: "created_on",
  type: "date",
  nativeType: "date",
  category: "date",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "like",
    "in",
    "between",
    "is_null",
    "is_not_null",
  ],
  valueSemantics: "plain",
};

const varcharMaxColumn: ColumnTypeMeta = {
  name: "payload",
  type: "varchar(max)",
  nativeType: "varchar(max)",
  category: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

function makeDriver(): MSSQLDriver {
  return new MSSQLDriver({
    id: "mssql-datetimeoffset-filter",
    name: "MSSQL DatetimeOffset Filter",
    type: "mssql",
    host: "127.0.0.1",
    port: 1433,
    database: "master",
    username: "sa",
    password: "secret",
  } as ConnectionConfig);
}

describe("MSSQL datetimeoffset filter", () => {
  it("matches display-formatted datetimeoffset with timezone", () => {
    const driver = makeDriver();

    const condition = driver.buildFilterCondition(
      datetimeOffsetColumn,
      "like",
      "2024-06-15 11:30:00.123 +00:00",
      1,
    );

    expect(condition).toEqual({
      sql: "REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(40), [created_at], 127), 'Z', '+00:00'), ' +', '+'), ' -', '-') LIKE ?",
      params: ["%2024-06-15T11:30:00.123%+00:00%"],
    });
  });

  it("uses textual date comparison for date equality filters", () => {
    const driver = makeDriver();

    const condition = driver.buildFilterCondition(
      dateColumn,
      "eq",
      "2024-02-28",
      1,
    );

    expect(condition).toEqual({
      sql: "CONVERT(CHAR(10), [created_on], 23) = ?",
      params: ["2024-02-28"],
    });
  });

  it("treats scientific notation float filters as float parameters", () => {
    const driver = makeDriver() as unknown as {
      bindPositionalParameters: (
        request: {
          input: (name: string, type: unknown, value: unknown) => void;
        },
        sql: string,
        params: readonly unknown[],
      ) => string;
    };

    const recorded: Array<{ name: string; typeName: string; value: unknown }> =
      [];
    const fakeRequest = {
      input(name: string, type: unknown, value: unknown): void {
        const typeName =
          typeof type === "function"
            ? type.name
            : typeof (type as { type?: unknown }).type === "function"
              ? ((type as { type: { name: string } }).type.name ?? "")
              : "";
        recorded.push({ name, typeName, value });
      },
    };

    const boundSql = driver.bindPositionalParameters(
      fakeRequest,
      "SELECT 1 WHERE [score] = ?",
      [1.797693134862316e38],
    );

    expect(boundSql).toBe("SELECT 1 WHERE [score] = @p1");
    expect(recorded).toHaveLength(1);
    expect(recorded[0].typeName).toBe("Float");
  });

  it("supports datetimeoffset equality using copied 7-digit fractional values", () => {
    const driver = makeDriver();

    const condition = driver.buildFilterCondition(
      datetimeOffsetColumn,
      "eq",
      "2024-07-04 08:34:56.1230000 +02:00",
      1,
    );

    expect(condition).toEqual({
      sql: "REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(40), [created_at], 127), 'Z', '+00:00'), ' +', '+'), ' -', '-') LIKE ?",
      params: ["%2024-07-04T08:34:56.123%+02:00%"],
    });
  });

  it("accepts copied value wrapped in quotes", () => {
    const driver = makeDriver();

    const condition = driver.buildFilterCondition(
      datetimeOffsetColumn,
      "like",
      '"2024-06-15 11:30:00.123 +00:00"',
      1,
    );

    expect(condition).toEqual({
      sql: "REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(40), [created_at], 127), 'Z', '+00:00'), ' +', '+'), ' -', '-') LIKE ?",
      params: ["%2024-06-15T11:30:00.123%+00:00%"],
    });
  });

  it("uses CHARINDEX with NVARCHAR(MAX) for long varchar(max) filters", () => {
    const driver = makeDriver();
    const longText = "x".repeat(3000);

    const condition = driver.buildFilterCondition(
      varcharMaxColumn,
      "like",
      longText,
      1,
    );

    expect(condition).toEqual({
      sql: "CHARINDEX(CAST(? AS NVARCHAR(MAX)), CAST([payload] AS NVARCHAR(MAX))) > 0",
      params: [longText],
    });
  });

  it("falls back to exact NVARCHAR(MAX) comparison for oversized text filters", () => {
    const driver = makeDriver();
    const hugeText = "x".repeat(12000);

    const condition = driver.buildFilterCondition(
      varcharMaxColumn,
      "like",
      hugeText,
      1,
    );

    expect(condition).toEqual({
      sql: "CAST([payload] AS NVARCHAR(MAX)) = CAST(? AS NVARCHAR(MAX))",
      params: [hugeText],
    });
  });

  it("parses tedious DateTimeOffset values into wall-clock time with the original offset", () => {
    // Encoded payload stores UTC time 04:34:56.1230000 with +02:00 offset.
    // The display value must preserve wall-clock form: 06:34:56.1230000+02:00.
    const encoded = Buffer.from([
      0x0a, // payload length
      0xb0,
      0x9c,
      0x74,
      0x68,
      0x26, // time
      0xfe,
      0x46,
      0x0b, // date
      0x78,
      0x00, // offset minutes (+120)
    ]);

    const tediousValueParser = require("tedious/lib/value-parser") as {
      readValue: (
        buf: Buffer,
        offset: number,
        metadata: { type?: { name?: string }; scale?: number },
        options: unknown,
      ) => { value: unknown; offset: number };
    };

    const parsed = tediousValueParser.readValue(
      encoded,
      0,
      { type: { name: "DateTimeOffset" }, scale: 7 },
      { useUTC: false },
    );

    expect(parsed.value).toBe("2024-07-04 06:34:56.1230000+02:00");
    expect(parsed.offset).toBe(encoded.length);
  });
});
