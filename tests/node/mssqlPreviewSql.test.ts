import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const baseConfig = {
  id: "mssql-preview-sql",
  name: "MSSQL Preview SQL",
  type: "mssql",
  host: "127.0.0.1",
  port: 1433,
  database: "db",
  username: "user",
  password: "pass",
} as const satisfies Partial<ConnectionConfig>;

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

describe("MSSQL preview SQL literals", () => {
  function encodeNumericN(value: string, scale: number): Buffer {
    const negative = value.startsWith("-");
    const normalized = negative ? value.slice(1) : value;
    const [integerPart, fractionPart = ""] = normalized.split(".");
    const scaledDigits = `${integerPart}${fractionPart.padEnd(scale, "0")}`
      .replace(/^0+(?=\d)/, "")
      .replace(/^$/, "0");
    let magnitude = BigInt(scaledDigits);
    const bytes: number[] = [];
    while (magnitude > 0n) {
      bytes.push(Number(magnitude & 0xffn));
      magnitude >>= 8n;
    }
    const payload = bytes.length === 0 ? [0] : bytes;
    return Buffer.from([payload.length + 1, negative ? 0 : 1, ...payload]);
  }

  it("materializes binary and boolean values as valid T-SQL literals", () => {
    const driver = new MSSQLDriver(baseConfig as ConnectionConfig);
    const preview = driver.materializePreviewSql(
      "UPDATE [t] SET [payload] = ?, [flag] = ? WHERE [id] = ?",
      [Buffer.from("48656c6c6f", "hex"), true, "row-1"],
    );

    expect(preview).toContain("[payload] = 0x48656c6c6f");
    expect(preview).toContain("[flag] = 1");
    expect(preview).toContain("[id] = 'row-1'");
    expect(preview).not.toContain("X'");
    expect(preview).not.toContain("TRUE");
  });

  it("materializes INSERT preview SQL with Unicode string prefixes for Unicode columns", () => {
    const driver = new MSSQLDriver(baseConfig as ConnectionConfig);
    const preview = driver.materializePreviewInsertSql(
      "INSERT INTO [t] ([payload], [title], [plain_text]) VALUES (?, ?, ?)",
      [
        Buffer.from("48656c6c6f576f726c64313233340000", "hex"),
        "Привет мир",
        "ascii value",
      ],
      [
        column("payload", "varbinary(16)", "binary"),
        column("title", "nvarchar(100)", "text"),
        column("plain_text", "varchar(100)", "text"),
      ],
    );

    expect(preview).toContain("0x48656c6c6f576f726c64313233340000");
    expect(preview).toContain("N'Привет мир'");
    expect(preview).toContain("'ascii value'");
    expect(preview).not.toContain("X '");
    expect(preview).not.toContain("X'");
  });

  it("materializes UPDATE preview SQL with Unicode prefixes for column-aware placeholders", () => {
    const driver = new MSSQLDriver(
      baseConfig as ConnectionConfig,
    ) as unknown as {
      materializePreviewColumnSql: (
        sql: string,
        params: readonly unknown[] | undefined,
        columns: readonly (ColumnTypeMeta | undefined)[],
      ) => string;
    };

    const preview = driver.materializePreviewColumnSql(
      "UPDATE [db].[dbo].[t] SET [col_nvarchar] = ? WHERE [id] = ?",
      ["NVarChar: Привет мир! 你好世界 😀", "3"],
      [
        column("col_nvarchar", "nvarchar(max)", "text"),
        column("id", "int", "integer"),
      ],
    );

    expect(preview).toContain(
      "[col_nvarchar] = N'NVarChar: Привет мир! 你好世界 😀'",
    );
    expect(preview).toContain("WHERE [id] = '3'");
  });

  it("keeps exact numeric parser values as strings for high-precision MSSQL numerics", () => {
    new MSSQLDriver(baseConfig as ConnectionConfig);
    const tediousValueParser = require("tedious/lib/value-parser") as {
      readValue: (
        buf: Buffer,
        offset: number,
        metadata: {
          type: { name: string };
          precision?: number;
          scale?: number;
        },
        options: unknown,
      ) => { value: unknown; offset: number };
    };

    const parsed = tediousValueParser.readValue(
      encodeNumericN("9999999999.1234567890", 10),
      0,
      { type: { name: "NumericN" }, precision: 20, scale: 10 },
      {},
    );

    expect(parsed.value).toBe("9999999999.1234567890");
  });

  it("materializes insert previews without truncating high-precision numerics", () => {
    const driver = new MSSQLDriver(baseConfig as ConnectionConfig);
    const operation = buildInsertRowOperation(
      driver,
      "db",
      "dbo",
      "t",
      { amount: "9999999999.1234567890" },
      [column("amount", "numeric(28,10)", "decimal")],
    );

    expect(operation.params).toEqual(["9999999999.1234567890"]);
    expect(operation.sql).toBe(
      "INSERT INTO [db].[dbo].[t] ([amount]) VALUES (CAST(? AS numeric(28,10)))",
    );
    expect(driver.materializePreviewSql(operation.sql, operation.params)).toBe(
      "INSERT INTO [db].[dbo].[t] ([amount]) VALUES (CAST('9999999999.1234567890' AS numeric(28,10)))",
    );
  });

  it("inlines NULL insert values instead of binding NVARCHAR NULL parameters", () => {
    const driver = new MSSQLDriver(baseConfig as ConnectionConfig);
    const operation = buildInsertRowOperation(
      driver,
      "db",
      "dbo",
      "t",
      {
        col_nvarchar: "NVarChar: Привет мир! 你好世界 😀",
        col_geography: null,
      },
      [
        column("col_nvarchar", "nvarchar(max)", "text"),
        column("col_geography", "geography", "spatial"),
      ],
    );

    expect(operation.sql).toBe(
      "INSERT INTO [db].[dbo].[t] ([col_nvarchar], [col_geography]) VALUES (?, NULL)",
    );
    expect(operation.params).toEqual(["NVarChar: Привет мир! 你好世界 😀"]);
    expect(
      driver.materializePreviewInsertSql(operation.sql, operation.params, [
        column("col_nvarchar", "nvarchar(max)", "text"),
        column("col_geography", "geography", "spatial"),
      ]),
    ).toBe(
      "INSERT INTO [db].[dbo].[t] ([col_nvarchar], [col_geography]) VALUES (N'NVarChar: Привет мир! 你好世界 😀', NULL)",
    );
  });
});
