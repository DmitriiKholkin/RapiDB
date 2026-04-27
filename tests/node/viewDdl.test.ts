import { describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const mysqlConfig = {
  id: "mysql-view-ddl-test",
  name: "MySQL View DDL Test",
  type: "mysql",
  host: "127.0.0.1",
  port: 3306,
  database: "test_db",
  username: "user",
  password: "pass",
} as const satisfies Partial<ConnectionConfig>;

const postgresConfig = {
  id: "pg-view-ddl-test",
  name: "Postgres View DDL Test",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "test_db",
  username: "user",
  password: "pass",
} as const satisfies Partial<ConnectionConfig>;

const oracleConfig = {
  id: "oracle-view-ddl-test",
  name: "Oracle View DDL Test",
  type: "oracle",
  host: "127.0.0.1",
  port: 1521,
  serviceName: "test_db",
  username: "user",
  password: "pass",
} as const satisfies Partial<ConnectionConfig>;

const sqliteConfig = {
  id: "sqlite-view-ddl-test",
  name: "SQLite View DDL Test",
  type: "sqlite",
  filePath: "/tmp/test-db.sqlite",
} as const satisfies Partial<ConnectionConfig>;

describe("view DDL generation", () => {
  it("MySQL returns SHOW CREATE VIEW output when the object is a view", async () => {
    const driver = new MySQLDriver(mysqlConfig as ConnectionConfig);
    const query = vi.fn(async (sql: string) => {
      expect(sql).toBe("SHOW CREATE TABLE `test_db`.`v_employees`");
      return [[{ "Create View": "CREATE VIEW `v_employees` AS select 1" }], []];
    });
    (driver as unknown as { pool: { query: typeof query } }).pool = {
      query,
    } as never;

    await expect(
      driver.getCreateTableDDL("test_db", "ignored", "v_employees"),
    ).resolves.toBe("CREATE VIEW `v_employees` AS select 1");
  });

  it("Postgres uses pg_get_viewdef for view DDL", async () => {
    const driver = new PostgresDriver(postgresConfig as ConnectionConfig);
    const query = vi.fn(async (sql: string, params?: unknown[]) => {
      if (sql.includes("information_schema.tables")) {
        expect(params).toEqual(["public", "v_employees"]);
        return { rows: [{ table_type: "VIEW" }] };
      }
      expect(sql).toContain("pg_get_viewdef");
      expect(params).toEqual(["public", "v_employees"]);
      return {
        rows: [{ def: 'CREATE VIEW "public"."v_employees" AS SELECT 1' }],
      };
    });
    (driver as unknown as { pool: { query: typeof query } }).pool = {
      query,
    } as never;

    await expect(
      driver.getCreateTableDDL("test_db", "public", "v_employees"),
    ).resolves.toBe('CREATE VIEW "public"."v_employees" AS SELECT 1');
  });

  it("Oracle asks DBMS_METADATA for VIEW DDL when the object type is a view", async () => {
    const driver = new OracleDriver(oracleConfig as ConnectionConfig);
    const execute = vi.fn(async (sql: string) => {
      if (sql.includes("all_objects")) {
        return { rows: [{ OBJECT_TYPE: "VIEW" }] };
      }
      if (sql.includes("DBMS_METADATA.SET_TRANSFORM_PARAM")) {
        return { rows: [] };
      }
      if (sql.includes("DBMS_METADATA.GET_DDL('VIEW'")) {
        return {
          rows: [
            {
              DDL: 'CREATE OR REPLACE VIEW "TEST_SCHEMA"."V_EMPLOYEES" AS SELECT 1',
            },
          ],
        };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    (
      driver as unknown as {
        getConnection: () => Promise<{
          execute: typeof execute;
          close(): Promise<void>;
        }>;
      }
    ).getConnection = async () => ({
      execute,
      close: async () => undefined,
    });

    await expect(
      driver.getCreateTableDDL("test_db", "test_schema", "v_employees"),
    ).resolves.toBe(
      'CREATE OR REPLACE VIEW "TEST_SCHEMA"."V_EMPLOYEES" AS SELECT 1',
    );
  });

  it("SQLite returns the stored view SQL from sqlite_master", async () => {
    const driver = new SQLiteDriver(sqliteConfig as ConnectionConfig);
    const get = vi.fn((sql: string, params?: unknown[]) => {
      expect(sql).toBe(
        "SELECT sql FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
      );
      expect(params).toEqual(["v_employees"]);
      return { sql: 'CREATE VIEW "v_employees" AS SELECT 1' };
    });
    (driver as unknown as { db: { isOpen: boolean; get: typeof get } }).db = {
      isOpen: true,
      get,
    } as never;

    await expect(
      driver.getCreateTableDDL("test_db", "ignored", "v_employees"),
    ).resolves.toBe('CREATE VIEW "v_employees" AS SELECT 1');
  });
});
