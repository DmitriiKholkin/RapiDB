import { describe, expect, it } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
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
});
