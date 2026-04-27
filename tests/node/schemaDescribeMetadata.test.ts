import { describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const mysqlConfig = {
  id: "mysql-schema-metadata",
  name: "MySQL Schema Metadata",
  type: "mysql",
  host: "127.0.0.1",
  port: 3306,
  database: "rapidb",
  username: "root",
  password: "root",
} as const satisfies Partial<ConnectionConfig>;

const postgresConfig = {
  id: "postgres-schema-metadata",
  name: "Postgres Schema Metadata",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "rapidb",
  username: "postgres",
  password: "postgres",
} as const satisfies Partial<ConnectionConfig>;

const oracleConfig = {
  id: "oracle-schema-metadata",
  name: "Oracle Schema Metadata",
  type: "oracle",
  host: "127.0.0.1",
  port: 1521,
  serviceName: "FREEPDB1",
  username: "system",
  password: "oracle",
} as const satisfies Partial<ConnectionConfig>;

const sqliteConfig = {
  id: "sqlite-schema-metadata",
  name: "SQLite Schema Metadata",
  type: "sqlite",
  filePath: "/tmp/rapidb-schema-metadata.sqlite",
} as const satisfies Partial<ConnectionConfig>;

describe("describeTable schema metadata", () => {
  it("separates MySQL defaults, on-update clauses, and generated metadata", async () => {
    const driver = new MySQLDriver(mysqlConfig as ConnectionConfig);
    const rows = [
      {
        COLUMN_NAME: "created_at",
        COLUMN_TYPE: "timestamp(6)",
        IS_NULLABLE: "NO",
        COLUMN_DEFAULT: "CURRENT_TIMESTAMP(6)",
        GENERATION_EXPRESSION: null,
        EXTRA: "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)",
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
      {
        COLUMN_NAME: "public_id",
        COLUMN_TYPE: "char(36)",
        IS_NULLABLE: "NO",
        COLUMN_DEFAULT: "uuid()",
        GENERATION_EXPRESSION: null,
        EXTRA: "DEFAULT_GENERATED",
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
      {
        COLUMN_NAME: "slug",
        COLUMN_TYPE: "varchar(255)",
        IS_NULLABLE: "YES",
        COLUMN_DEFAULT: null,
        GENERATION_EXPRESSION: "lcase(concat(`public_id`, '-', `created_at`))",
        EXTRA: "STORED GENERATED",
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
      {
        COLUMN_NAME: "slug_preview",
        COLUMN_TYPE: "varchar(255)",
        IS_NULLABLE: "YES",
        COLUMN_DEFAULT: null,
        GENERATION_EXPRESSION: "lcase(`public_id`)",
        EXTRA: "VIRTUAL GENERATED",
        IS_PRIMARY_KEY: 0,
        PRIMARY_KEY_ORDINAL: null,
        IS_FOREIGN_KEY: 0,
      },
    ];
    const query = vi.fn(async () => [rows, []]);
    (driver as unknown as { pool: { query: typeof query } }).pool = {
      query,
    } as never;

    const columns = await driver.describeTable("rapidb", "rapidb", "audit_log");

    expect(columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "created_at",
          defaultValue: "CURRENT_TIMESTAMP(6)",
          defaultKind: "expression",
          onUpdateExpression: "CURRENT_TIMESTAMP(6)",
        }),
        expect.objectContaining({
          name: "public_id",
          defaultValue: "uuid()",
          defaultKind: "expression",
        }),
        expect.objectContaining({
          name: "slug",
          defaultValue: undefined,
          isComputed: true,
          computedExpression: "lcase(concat(`public_id`, '-', `created_at`))",
          generatedKind: "stored",
          isPersisted: true,
        }),
        expect.objectContaining({
          name: "slug_preview",
          defaultValue: undefined,
          isComputed: true,
          computedExpression: "lcase(`public_id`)",
          generatedKind: "virtual",
          isPersisted: false,
        }),
      ]),
    );
  });

  it("surfaces Postgres stored generated columns without reusing defaultValue", async () => {
    const driver = new PostgresDriver(postgresConfig as ConnectionConfig);
    const query = vi.fn(async () => ({
      rows: [
        {
          column_name: "created_at",
          data_type: "timestamp with time zone",
          is_nullable: false,
          column_default: "CURRENT_TIMESTAMP",
          generated_kind: null,
          identity_kind: null,
          is_pk: false,
          pk_ordinal: null,
          is_fk: false,
        },
        {
          column_name: "slug",
          data_type: "text",
          is_nullable: true,
          column_default: "lower(display_name)",
          generated_kind: "s",
          identity_kind: null,
          is_pk: false,
          pk_ordinal: null,
          is_fk: false,
        },
      ],
    }));
    (driver as unknown as { pool: { query: typeof query } }).pool = {
      query,
    } as never;

    const columns = await driver.describeTable("rapidb", "public", "users");

    expect(columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "created_at",
          defaultValue: "CURRENT_TIMESTAMP",
          defaultKind: "expression",
          isComputed: false,
        }),
        expect.objectContaining({
          name: "slug",
          defaultValue: undefined,
          isComputed: true,
          computedExpression: "lower(display_name)",
          generatedKind: "stored",
          isPersisted: true,
        }),
      ]),
    );
  });

  it("surfaces Oracle virtual columns separately from default expressions", async () => {
    const driver = new OracleDriver(oracleConfig as ConnectionConfig);
    const execute = vi.fn(async (sql: string) => {
      if (sql.includes("FROM all_tab_columns")) {
        return {
          rows: [
            {
              COLUMN_NAME: "CREATED_AT",
              DATA_TYPE: "TIMESTAMP",
              DATA_PRECISION: null,
              DATA_SCALE: null,
              DATA_LENGTH: 11,
              NULLABLE: "N",
              DATA_DEFAULT: "CURRENT_TIMESTAMP",
              VIRTUAL_COLUMN: "NO",
              COLUMN_ID: 1,
            },
            {
              COLUMN_NAME: "SLUG",
              DATA_TYPE: "VARCHAR2",
              DATA_PRECISION: null,
              DATA_SCALE: null,
              DATA_LENGTH: 128,
              NULLABLE: "Y",
              DATA_DEFAULT: 'LOWER("NAME")',
              VIRTUAL_COLUMN: "YES",
              COLUMN_ID: 2,
            },
          ],
        };
      }
      if (sql.includes("constraint_type = 'P'")) {
        return { rows: [] };
      }
      if (sql.includes("constraint_type = 'R'")) {
        return { rows: [] };
      }
      if (sql.includes("FROM all_tab_identity_cols")) {
        return { rows: [] };
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

    const columns = await driver.describeTable("rapidb", "app", "users");

    expect(columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "CREATED_AT",
          defaultValue: "CURRENT_TIMESTAMP",
          defaultKind: "expression",
          isComputed: false,
        }),
        expect.objectContaining({
          name: "SLUG",
          defaultValue: undefined,
          isComputed: true,
          computedExpression: 'LOWER("NAME")',
          generatedKind: "virtual",
          isPersisted: false,
        }),
      ]),
    );
  });

  it("extracts SQLite generated expressions and storage mode from CREATE TABLE SQL", async () => {
    const driver = new SQLiteDriver(sqliteConfig as ConnectionConfig);
    const all = vi.fn((sql: string, params?: unknown[]) => {
      if (sql.startsWith('PRAGMA table_xinfo("users")')) {
        return [
          {
            name: "id",
            type: "INTEGER",
            notnull: 0,
            dflt_value: null,
            pk: 1,
            hidden: 0,
          },
          {
            name: "name",
            type: "TEXT",
            notnull: 1,
            dflt_value: null,
            pk: 0,
            hidden: 0,
          },
          {
            name: "slug_virtual",
            type: "TEXT",
            notnull: 0,
            dflt_value: null,
            pk: 0,
            hidden: 2,
          },
          {
            name: "slug_stored",
            type: "TEXT",
            notnull: 0,
            dflt_value: null,
            pk: 0,
            hidden: 3,
          },
          {
            name: "updated_at",
            type: "TEXT",
            notnull: 0,
            dflt_value: "CURRENT_TIMESTAMP",
            pk: 0,
            hidden: 0,
          },
        ];
      }
      if (sql.startsWith('PRAGMA foreign_key_list("users")')) {
        return [];
      }
      if (
        sql === "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
      ) {
        expect(params).toEqual(["users"]);
        return [
          {
            sql: `CREATE TABLE "users" (
              "id" INTEGER PRIMARY KEY AUTOINCREMENT,
              "name" TEXT NOT NULL,
              "slug_virtual" TEXT GENERATED ALWAYS AS (lower("name")) VIRTUAL,
              "slug_stored" TEXT GENERATED ALWAYS AS (trim("name" || '-' || "id")) STORED,
              "updated_at" TEXT DEFAULT CURRENT_TIMESTAMP
            )`,
          },
        ];
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    (driver as unknown as { db: { isOpen: boolean; all: typeof all } }).db = {
      isOpen: true,
      all,
    } as never;

    const columns = await driver.describeTable("main", "main", "users");

    expect(columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "slug_virtual",
          defaultValue: undefined,
          isComputed: true,
          computedExpression: 'lower("name")',
          generatedKind: "virtual",
          isPersisted: false,
        }),
        expect.objectContaining({
          name: "slug_stored",
          defaultValue: undefined,
          isComputed: true,
          computedExpression: 'trim("name" || \'-\' || "id")',
          generatedKind: "stored",
          isPersisted: true,
        }),
        expect.objectContaining({
          name: "updated_at",
          defaultValue: "CURRENT_TIMESTAMP",
          defaultKind: "expression",
        }),
      ]),
    );
  });
});
