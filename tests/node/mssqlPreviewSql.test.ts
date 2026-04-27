import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
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
    isAutoIncrement: false,
    filterable: true,
    filterOperators: ["is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

describe("MSSQL preview SQL literals", () => {
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
});
