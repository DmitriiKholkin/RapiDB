import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const baseConfig = {
  id: "temporal-preview-consistency",
  name: "Temporal Preview Consistency",
  host: "127.0.0.1",
  port: 0,
  database: "db",
  username: "user",
  password: "pass",
} as const;

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

function previewInsertLiteral(
  driver: IDBDriver,
  col: ColumnTypeMeta,
  rawValue: string,
): { coerced: unknown; preview: string } {
  const coerced = driver.coerceInputValue(rawValue, col);
  const sql = `INSERT INTO ${driver.quoteIdentifier("t")} (${driver.quoteIdentifier(col.name)}) VALUES (${driver.buildInsertValueExpr(col, 1)})`;
  const preview = driver.materializePreviewSql(sql, [coerced]);
  return { coerced, preview };
}

describe("temporal preview consistency (non-Oracle drivers)", () => {
  it("MySQL converts timezone input to UTC datetime and preview uses the same value", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
    } as ConnectionConfig);

    const col = column("created_at", "datetime", "datetime");
    const { coerced, preview } = previewInsertLiteral(
      driver,
      col,
      "2024-06-15T12:30:00+03:00",
    );

    expect(coerced).toBe("2024-06-15 09:30:00");
    expect(preview).toContain("'2024-06-15 09:30:00'");
  });

  it("MSSQL keeps datetimeoffset literal normalized and preview reuses it", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);

    const col = column("event_at", "datetimeoffset", "datetime");
    const { coerced, preview } = previewInsertLiteral(
      driver,
      col,
      "2024-06-15 12:30:00.123+03:00",
    );

    expect(coerced).toBe("2024-06-15 12:30:00.123+03:00");
    expect(preview).toContain("'2024-06-15 12:30:00.123+03:00'");
  });

  it("Postgres preserves timestamptz offset text and preview uses the same literal", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);

    const col = column("event_at", "timestamp with time zone", "datetime");
    const { coerced, preview } = previewInsertLiteral(
      driver,
      col,
      "2024-06-15 12:30:00+03:00",
    );

    expect(coerced).toBe("2024-06-15 12:30:00+03:00");
    expect(preview).toContain("'2024-06-15 12:30:00+03:00'");
  });

  it("SQLite keeps datetime text literal unchanged in preview", () => {
    const driver = new SQLiteDriver({
      ...baseConfig,
      type: "sqlite",
      filePath: ":memory:",
    } as ConnectionConfig);

    const col = column("event_at", "DATETIME", "datetime");
    const { coerced, preview } = previewInsertLiteral(
      driver,
      col,
      "2024-06-15 12:30:00+03:00",
    );

    expect(coerced).toBe("2024-06-15 12:30:00+03:00");
    expect(preview).toContain("'2024-06-15 12:30:00+03:00'");
  });
});
