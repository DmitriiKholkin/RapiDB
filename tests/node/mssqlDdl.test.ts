import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const baseConfig = {
  id: "mssql-ddl-test",
  name: "MSSQL DDL Test",
  type: "mssql",
  host: "127.0.0.1",
  port: 1433,
  database: "test_db",
  username: "user",
  password: "pass",
} as const satisfies Partial<ConnectionConfig>;

type QueryResult = { recordset: Array<Record<string, unknown>> };

function makeDriver(): MSSQLDriver {
  return new MSSQLDriver(baseConfig as ConnectionConfig);
}

function attachPool(
  driver: MSSQLDriver,
  queryHandler: (sql: string) => QueryResult,
) {
  const query = vi.fn((sql: string) => Promise.resolve(queryHandler(sql)));
  const request = vi.fn(() => ({ query }));
  const pool = {
    request,
    close: vi.fn(),
    on: vi.fn(),
  };
  (driver as unknown as { pool: typeof pool }).pool = pool;
  return { pool, request, query };
}

describe("MSSQL DDL generation", () => {
  it("returns view definition instead of table DDL for views", async () => {
    const driver = makeDriver();
    const queries: string[] = [];
    const { request } = attachPool(driver, (sql) => {
      queries.push(sql);
      if (sql.includes("INFORMATION_SCHEMA.TABLES")) {
        return { recordset: [{ TABLE_TYPE: "VIEW" }] };
      }
      if (sql.includes("OBJECT_DEFINITION")) {
        return {
          recordset: [
            {
              def: "CREATE VIEW [test_schema].[v_employees] AS SELECT [emp_id], [emp_name] FROM [test_schema].[employees]",
            },
          ],
        };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });

    const ddl = await driver.getCreateTableDDL(
      "test_db",
      "test_schema",
      "v_employees",
    );

    expect(ddl).toBe(
      "CREATE VIEW [test_schema].[v_employees] AS SELECT [emp_id], [emp_name] FROM [test_schema].[employees]",
    );
    expect(request).toHaveBeenCalledTimes(2);
    expect(queries[0]).toContain("INFORMATION_SCHEMA.TABLES");
    expect(queries[1]).toContain("OBJECT_DEFINITION");
  });

  it("continues to render table DDL for base tables", async () => {
    const driver = makeDriver();
    attachPool(driver, (sql) => {
      if (sql.includes("INFORMATION_SCHEMA.TABLES")) {
        return { recordset: [{ TABLE_TYPE: "BASE TABLE" }] };
      }
      if (sql.includes("sys.columns")) {
        return {
          recordset: [
            {
              COLUMN_NAME: "emp_id",
              DATA_TYPE: "int",
              max_length: 4,
              precision: 10,
              scale: 0,
              IS_NULLABLE: 0,
              is_identity: 0,
              is_computed: 0,
              COMPUTED_DEFINITION: null,
              is_persisted: 0,
              COLUMN_DEFAULT: null,
              IS_PK: 1,
              PK_ORDINAL: 1,
            },
          ],
        };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });

    const ddl = await driver.getCreateTableDDL(
      "test_db",
      "test_schema",
      "employees",
    );

    expect(ddl).toBe(
      "CREATE TABLE [test_schema].[employees] (\n  [emp_id] int NOT NULL PRIMARY KEY\n);",
    );
  });
});
