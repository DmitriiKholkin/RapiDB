import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const mysqlConfig = {
  id: "mysql-schema-object-definitions",
  name: "MySQL Schema Objects",
  type: "mysql",
  host: "127.0.0.1",
  port: 3306,
  database: "rapidb",
  username: "root",
  password: "root",
} as const satisfies Partial<ConnectionConfig>;

const postgresConfig = {
  id: "pg-schema-object-definitions",
  name: "Postgres Schema Objects",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "rapidb",
  username: "postgres",
  password: "postgres",
} as const satisfies Partial<ConnectionConfig>;

const mssqlConfig = {
  id: "mssql-schema-object-definitions",
  name: "MSSQL Schema Objects",
  type: "mssql",
  host: "127.0.0.1",
  port: 1433,
  database: "rapidb",
  username: "sa",
  password: "Password123!",
} as const satisfies Partial<ConnectionConfig>;

const oracleConfig = {
  id: "oracle-schema-object-definitions",
  name: "Oracle Schema Objects",
  type: "oracle",
  host: "127.0.0.1",
  port: 1521,
  serviceName: "FREEPDB1",
  username: "system",
  password: "oracle",
} as const satisfies Partial<ConnectionConfig>;

const sqliteConfig = {
  id: "sqlite-schema-object-definitions",
  name: "SQLite Schema Objects",
  type: "sqlite",
  filePath: "/tmp/rapidb-schema-object-definitions.sqlite",
} as const satisfies Partial<ConnectionConfig>;

const mongodbConfig = {
  id: "mongodb-schema-object-definitions",
  name: "MongoDB Schema Objects",
  type: "mongodb",
  host: "127.0.0.1",
  port: 27017,
  database: "rapidb",
} as const satisfies Partial<ConnectionConfig>;

const dynamodbConfig = {
  id: "dynamodb-schema-object-definitions",
  name: "DynamoDB Schema Objects",
  type: "dynamodb",
  awsRegion: "us-east-1",
} as const satisfies Partial<ConnectionConfig>;

const elasticsearchConfig = {
  id: "elasticsearch-schema-object-definitions",
  name: "Elasticsearch Schema Objects",
  type: "elasticsearch",
  host: "127.0.0.1",
  port: 9200,
  database: "rapidb",
} as const satisfies Partial<ConnectionConfig>;

const redisConfig = {
  id: "redis-schema-object-definitions",
  name: "Redis Schema Objects",
  type: "redis",
  host: "127.0.0.1",
  port: 6379,
} as const satisfies Partial<ConnectionConfig>;

describe("schema object definitions", () => {
  it("lists Postgres materialized views, sequences, and types", async () => {
    const driver = new PostgresDriver(postgresConfig as ConnectionConfig);
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("information_schema.tables")) {
        return {
          rows: [
            { name: "users", type: "BASE TABLE" },
            { name: "active_users", type: "VIEW" },
          ],
        };
      }
      if (sql.includes("pg_matviews")) {
        return { rows: [{ name: "daily_users" }] };
      }
      if (sql.includes("pg_proc")) {
        return { rows: [{ name: "refresh_users", type: "procedure" }] };
      }
      if (sql.includes("information_schema.sequences")) {
        return { rows: [{ name: "users_id_seq" }] };
      }
      if (sql.includes("FROM pg_type t")) {
        return { rows: [{ name: "user_status" }] };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });

    (
      driver as unknown as {
        withDatabasePool: (
          database: string,
          callback: (pool: { query: typeof query }) => Promise<unknown>,
        ) => Promise<unknown>;
      }
    ).withDatabasePool = async (_database, callback) => callback({ query });

    await expect(driver.listObjects("rapidb", "public")).resolves.toEqual(
      expect.arrayContaining([
        { schema: "public", name: "users", type: "table" },
        { schema: "public", name: "active_users", type: "view" },
        {
          schema: "public",
          name: "daily_users",
          type: "materializedView",
        },
        {
          schema: "public",
          name: "users_id_seq",
          type: "sequence",
        },
        { schema: "public", name: "user_status", type: "type" },
      ]),
    );
  });

  it("renders Postgres materialized view, sequence, and type DDL", async () => {
    const driver = new PostgresDriver(postgresConfig as ConnectionConfig);
    const query = vi.fn(async (sql: string, params?: unknown[]) => {
      if (sql.includes("SELECT c.relkind") && sql.includes("c.relkind IN")) {
        expect(params).toEqual(["public", "daily_users"]);
        return { rows: [{ relkind: "m" }] };
      }
      if (sql.includes("CREATE MATERIALIZED VIEW")) {
        return {
          rows: [
            {
              def: 'CREATE MATERIALIZED VIEW "public"."daily_users" AS\n SELECT 1;',
            },
          ],
        };
      }
      if (sql.includes("FROM pg_sequences")) {
        return {
          rows: [
            {
              data_type: "bigint",
              start_value: "1",
              min_value: "1",
              max_value: "9223372036854775807",
              increment_by: "1",
              cycle: false,
              cache_size: "1",
            },
          ],
        };
      }
      if (sql.includes("format_type(t.typbasetype")) {
        return {
          rows: [
            {
              typtype: "e",
              typnotnull: false,
              typdefault: null,
              base_type: null,
            },
          ],
        };
      }
      if (sql.includes("FROM pg_enum e")) {
        return {
          rows: [{ enumlabel: "active" }, { enumlabel: "archived" }],
        };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });

    (driver as unknown as { pool: { query: typeof query } }).pool = {
      query,
    } as never;

    await expect(
      driver.getCreateTableDDL("rapidb", "public", "daily_users"),
    ).resolves.toBe(
      'CREATE MATERIALIZED VIEW "public"."daily_users" AS\n SELECT 1;',
    );
    await expect(
      driver.getObjectDefinition(
        "rapidb",
        "public",
        "users_id_seq",
        "sequence",
      ),
    ).resolves.toBe(
      'CREATE SEQUENCE "public"."users_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;',
    );
    await expect(
      driver.getObjectDefinition("rapidb", "public", "user_status", "type"),
    ).resolves.toBe(
      "CREATE TYPE \"public\".\"user_status\" AS ENUM ('active', 'archived');",
    );
  });

  it("lists MSSQL sequences and user-defined types and renders their DDL", async () => {
    const driver = new MSSQLDriver(mssqlConfig as ConnectionConfig);
    const query = vi.fn((sql: string) => {
      if (sql.includes("INFORMATION_SCHEMA.TABLES")) {
        return Promise.resolve({
          recordset: [{ name: "users", type: "BASE TABLE" }],
        });
      }
      if (sql.includes("sys.objects o")) {
        return Promise.resolve({
          recordset: [{ name: "refresh_users", type: "procedure" }],
        });
      }
      if (
        sql.includes("sys.sequences seq") &&
        sql.includes("ORDER BY seq.name")
      ) {
        return Promise.resolve({ recordset: [{ name: "users_id_seq" }] });
      }
      if (sql.includes("sys.types t") && sql.includes("ORDER BY t.name")) {
        return Promise.resolve({ recordset: [{ name: "user_status" }] });
      }
      if (
        sql.includes("FROM [rapidb].sys.sequences seq") &&
        sql.includes("seq.name = 'users_id_seq'")
      ) {
        return Promise.resolve({
          recordset: [
            {
              start_value: 10,
              increment: 5,
              minimum_value: 10,
              maximum_value: 1000,
              is_cycling: 0,
              cache_size: 20,
            },
          ],
        });
      }
      if (
        sql.includes("FROM [rapidb].sys.types t") &&
        sql.includes("t.name = 'user_status'")
      ) {
        return Promise.resolve({
          recordset: [
            {
              base_type: "nvarchar",
              max_length: 40,
              precision: 0,
              scale: 0,
              is_nullable: 1,
            },
          ],
        });
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const request = vi.fn(() => ({ query }));
    (driver as unknown as { pool: { request: typeof request } }).pool = {
      request,
      close: vi.fn(),
      on: vi.fn(),
    } as never;

    await expect(driver.listObjects("rapidb", "dbo")).resolves.toEqual(
      expect.arrayContaining([
        { schema: "dbo", name: "users", type: "table" },
        { schema: "dbo", name: "users_id_seq", type: "sequence" },
        { schema: "dbo", name: "user_status", type: "type" },
      ]),
    );
    await expect(
      driver.getObjectDefinition("rapidb", "dbo", "users_id_seq", "sequence"),
    ).resolves.toBe(
      "CREATE SEQUENCE [dbo].[users_id_seq] START WITH 10 INCREMENT BY 5 MINVALUE 10 MAXVALUE 1000 NO CYCLE CACHE 20;",
    );
    await expect(
      driver.getObjectDefinition("rapidb", "dbo", "user_status", "type"),
    ).resolves.toBe("CREATE TYPE [dbo].[user_status] FROM nvarchar(20) NULL;");
  });

  it("keeps MySQL limited to tables, views, and routines", async () => {
    const driver = new MySQLDriver(mysqlConfig as ConnectionConfig);
    const query = vi.fn(
      async (options: string | { sql: string; values?: unknown[] }) => {
        const sql = typeof options === "string" ? options : options.sql;
        const values = typeof options === "string" ? undefined : options.values;

        if (sql.includes("information_schema.TABLES")) {
          expect(values).toEqual(["rapidb"]);
          return [
            [
              { name: "users", type: "BASE TABLE" },
              { name: "active_users", type: "VIEW" },
            ],
            [],
          ];
        }
        if (sql.includes("information_schema.ROUTINES")) {
          expect(values).toEqual(["rapidb"]);
          return [
            [
              { name: "refresh_users", type: "PROCEDURE" },
              { name: "users_search", type: "FUNCTION" },
            ],
            [],
          ];
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      },
    );

    (driver as unknown as { pool: { query: typeof query } }).pool = {
      query,
    } as never;

    const objects = await driver.listObjects("rapidb", "ignored");

    expect(objects).toEqual([
      { schema: "rapidb", name: "users", type: "table" },
      { schema: "rapidb", name: "active_users", type: "view" },
      { schema: "rapidb", name: "refresh_users", type: "procedure" },
      { schema: "rapidb", name: "users_search", type: "function" },
    ]);
    expect(
      objects.some(
        (object) =>
          object.type === "materializedView" ||
          object.type === "sequence" ||
          object.type === "type",
      ),
    ).toBe(false);
    await expect(
      driver.getObjectDefinition("rapidb", "ignored", "user_status", "type"),
    ).resolves.toBeNull();
  });

  it("keeps SQLite limited to tables and views and returns null DDL-only definitions", async () => {
    const driver = new SQLiteDriver(sqliteConfig as ConnectionConfig);
    const all = vi.fn((sql: string) => {
      if (sql === "PRAGMA database_list") {
        return [{ seq: 0, name: "main", file: "/tmp/schema-objects.sqlite" }];
      }
      expect(sql).toContain('FROM "main".sqlite_master');
      expect(sql).toContain("type IN ('table','view')");
      return [
        { name: "users", type: "table" },
        { name: "active_users", type: "view" },
      ];
    });

    (driver as unknown as { db: { isOpen: boolean; all: typeof all } }).db = {
      isOpen: true,
      all,
    } as never;

    await expect(driver.listObjects("rapidb", "main")).resolves.toEqual([
      { schema: "main", name: "users", type: "table" },
      { schema: "main", name: "active_users", type: "view" },
    ]);
    await expect(
      driver.getObjectDefinition("rapidb", "main", "users_id_seq", "sequence"),
    ).resolves.toBeNull();
    await expect(
      driver.getObjectDefinition("rapidb", "main", "user_status", "type"),
    ).resolves.toBeNull();
  });

  it("lists MongoDB collections and views while hiding system namespaces", async () => {
    const driver = new MongoDBDriver(mongodbConfig as ConnectionConfig);
    const toArray = vi.fn().mockResolvedValue([
      { name: "system.views", type: "collection" },
      { name: "users", type: "collection" },
      { name: "active_users", type: "view" },
    ]);
    const listCollections = vi.fn().mockReturnValue({ toArray });

    (
      driver as unknown as {
        client: {
          db: (database?: string) => {
            listCollections: typeof listCollections;
          };
        };
        connected: boolean;
      }
    ).client = {
      db: vi.fn().mockReturnValue({ listCollections }),
    };
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(driver.listObjects("rapidb")).resolves.toEqual([
      { schema: "rapidb", name: "users", type: "table" },
      { schema: "rapidb", name: "active_users", type: "view" },
    ]);
    expect(listCollections).toHaveBeenCalledWith({}, { nameOnly: false });
  });

  it("renders MongoDB collection, view, and index DDL in mongosh syntax", async () => {
    const driver = new MongoDBDriver(mongodbConfig as ConnectionConfig);
    const listCollections = vi.fn(({ name }: { name?: string }) => ({
      toArray: vi.fn().mockResolvedValue(
        name === "active_users"
          ? [
              {
                name: "active_users",
                type: "view",
                options: {
                  viewOn: "users",
                  pipeline: [{ $match: { active: true } }],
                },
              },
            ]
          : [
              {
                name: "users",
                type: "collection",
                options: {
                  validator: { $jsonSchema: { bsonType: "object" } },
                },
              },
            ],
      ),
    }));
    const indexes = vi.fn().mockResolvedValue([
      {
        name: "users_by_email",
        key: { email: 1 },
        unique: true,
      },
    ]);

    (
      driver as unknown as {
        client: {
          db: (database: string) => {
            listCollections: typeof listCollections;
            collection: (name: string) => { indexes: typeof indexes };
          };
        };
        connected: boolean;
      }
    ).client = {
      db: vi.fn((_database: string) => ({
        listCollections,
        collection: vi.fn((_name: string) => ({ indexes })),
      })),
    };
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(
      driver.getCreateTableDDL("rapidb", "ignored", "users"),
    ).resolves.toBe(
      'db.getSiblingDB("rapidb").createCollection(\n  "users",\n  { "validator": { "$jsonSchema": { "bsonType": "object" } } }\n);',
    );
    await expect(
      driver.getCreateTableDDL("rapidb", "ignored", "active_users"),
    ).resolves.toBe(
      'db.getSiblingDB("rapidb").createView(\n  "active_users",\n  "users",\n  [{ "$match": { "active": true } }]\n);',
    );
    await expect(
      driver.getIndexDDL("rapidb", "ignored", "users", "users_by_email"),
    ).resolves.toBe(
      'db.getSiblingDB("rapidb").getCollection("users").createIndex(\n  { "email": 1 },\n  { "name": "users_by_email", "unique": true }\n);',
    );
  });

  it("renders Elasticsearch index DDL with filtered settings, mappings, and aliases", async () => {
    const driver = new ElasticsearchDriver(
      elasticsearchConfig as ConnectionConfig,
    );

    (
      driver as unknown as {
        client: {
          indices: {
            get: typeof vi.fn;
          };
        };
        connected: boolean;
      }
    ).client = {
      indices: {
        get: vi.fn().mockResolvedValue({
          users: {
            settings: {
              index: {
                number_of_shards: "1",
                number_of_replicas: "1",
                provided_name: "users",
                creation_date: "1700000000000",
                uuid: "abc123",
                version: { created: "8500000" },
              },
            },
            mappings: {
              properties: {
                email: { type: "keyword" },
              },
            },
            aliases: {
              users_read: {},
            },
          },
        }),
      },
    };
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(
      driver.getCreateTableDDL("default", "indices", "users"),
    ).resolves.toBe(
      'PUT /users\n{\n  "settings": {\n    "number_of_shards": "1",\n    "number_of_replicas": "1"\n  },\n  "mappings": {\n    "properties": {\n      "email": {\n        "type": "keyword"\n      }\n    }\n  },\n  "aliases": {\n    "users_read": {}\n  }\n}',
    );
  });

  it("lists Oracle materialized views, sequences, and types and renders metadata DDL", async () => {
    const driver = new OracleDriver(oracleConfig as ConnectionConfig);
    const execute = vi.fn(async (sql: string) => {
      if (
        sql.includes("FROM all_objects") &&
        sql.includes("status = 'VALID'")
      ) {
        return {
          rows: [
            { OBJECT_NAME: "USERS", OBJECT_TYPE: "TABLE" },
            { OBJECT_NAME: "DAILY_USERS", OBJECT_TYPE: "MATERIALIZED VIEW" },
            { OBJECT_NAME: "USERS_ID_SEQ", OBJECT_TYPE: "SEQUENCE" },
            { OBJECT_NAME: "USER_STATUS", OBJECT_TYPE: "TYPE" },
          ],
        };
      }
      if (sql.includes("SELECT object_type FROM all_objects")) {
        return { rows: [{ OBJECT_TYPE: "MATERIALIZED VIEW" }] };
      }
      if (sql.includes("DBMS_METADATA.SET_TRANSFORM_PARAM")) {
        return { rows: [] };
      }
      if (sql.includes("DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW'")) {
        return {
          rows: [
            {
              DDL: 'CREATE MATERIALIZED VIEW "APP"."DAILY_USERS" AS SELECT 1;',
            },
          ],
        };
      }
      if (sql.includes("DBMS_METADATA.GET_DDL('SEQUENCE'")) {
        return {
          rows: [{ DDL: 'CREATE SEQUENCE "APP"."USERS_ID_SEQ" START WITH 1;' }],
        };
      }
      if (sql.includes("DBMS_METADATA.GET_DDL('TYPE'")) {
        return {
          rows: [
            {
              DDL: 'CREATE TYPE "APP"."USER_STATUS" AS OBJECT (STATUS VARCHAR2(32));',
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

    await expect(driver.listObjects("rapidb", "app")).resolves.toEqual(
      expect.arrayContaining([
        { schema: "app", name: "USERS", type: "table" },
        {
          schema: "app",
          name: "DAILY_USERS",
          type: "materializedView",
        },
        { schema: "app", name: "USERS_ID_SEQ", type: "sequence" },
        { schema: "app", name: "USER_STATUS", type: "type" },
      ]),
    );
    await expect(
      driver.getCreateTableDDL("rapidb", "app", "daily_users"),
    ).resolves.toBe(
      'CREATE MATERIALIZED VIEW "APP"."DAILY_USERS" AS SELECT 1;',
    );
    await expect(
      driver.getObjectDefinition("rapidb", "app", "users_id_seq", "sequence"),
    ).resolves.toBe('CREATE SEQUENCE "APP"."USERS_ID_SEQ" START WITH 1;');
    await expect(
      driver.getObjectDefinition("rapidb", "app", "user_status", "type"),
    ).resolves.toBe(
      'CREATE TYPE "APP"."USER_STATUS" AS OBJECT (STATUS VARCHAR2(32));',
    );
  });

  it("declares driver-specific entity manifests for all supported drivers", () => {
    const manifests = {
      mssql: new MSSQLDriver(
        mssqlConfig as ConnectionConfig,
      ).getEntityManifest(),
      mysql: new MySQLDriver(
        mysqlConfig as ConnectionConfig,
      ).getEntityManifest(),
      postgres: new PostgresDriver(
        postgresConfig as ConnectionConfig,
      ).getEntityManifest(),
      sqlite: new SQLiteDriver(
        sqliteConfig as ConnectionConfig,
      ).getEntityManifest(),
      oracle: new OracleDriver(
        oracleConfig as ConnectionConfig,
      ).getEntityManifest(),
      mongodb: new MongoDBDriver(
        mongodbConfig as ConnectionConfig,
      ).getEntityManifest(),
      dynamodb: new DynamoDBDriver(
        dynamodbConfig as ConnectionConfig,
      ).getEntityManifest(),
      elasticsearch: new ElasticsearchDriver(
        elasticsearchConfig as ConnectionConfig,
      ).getEntityManifest(),
      redis: new RedisDriver(
        redisConfig as ConnectionConfig,
      ).getEntityManifest(),
    };

    expect(manifests).toEqual({
      mssql: {
        dbObjectKinds: [
          "table",
          "view",
          "function",
          "procedure",
          "sequence",
          "type",
        ],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
        tableSectionOverridesByObjectKind: {
          view: {
            constraints: "not_applicable",
            triggers: "not_applicable",
          },
        },
      },
      mysql: {
        dbObjectKinds: ["table", "view", "function", "procedure"],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
        tableSectionOverridesByObjectKind: {
          view: {
            constraints: "not_applicable",
            indexes: "not_applicable",
            triggers: "not_applicable",
          },
        },
      },
      postgres: {
        dbObjectKinds: [
          "table",
          "view",
          "materializedView",
          "function",
          "procedure",
          "sequence",
          "type",
        ],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
        tableSectionOverridesByObjectKind: {
          view: {
            constraints: "not_applicable",
            indexes: "not_applicable",
          },
          materializedView: {
            constraints: "not_applicable",
            triggers: "not_applicable",
          },
        },
      },
      sqlite: {
        dbObjectKinds: ["table", "view"],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
        tableSectionOverridesByObjectKind: {
          view: {
            constraints: "not_applicable",
            indexes: "not_applicable",
          },
        },
      },
      oracle: {
        dbObjectKinds: [
          "table",
          "view",
          "materializedView",
          "function",
          "procedure",
          "sequence",
          "type",
        ],
        tableSections: {
          columns: "supported",
          constraints: "supported",
          indexes: "supported",
          triggers: "supported",
        },
        tableSectionOverridesByObjectKind: {
          view: {
            constraints: "not_applicable",
            indexes: "not_applicable",
          },
          materializedView: {
            constraints: "not_applicable",
            triggers: "not_applicable",
          },
        },
      },
      mongodb: {
        dbObjectKinds: ["table", "view"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "supported",
          triggers: "not_applicable",
        },
        tableSectionOverridesByObjectKind: {
          view: {
            indexes: "not_applicable",
          },
        },
      },
      dynamodb: {
        dbObjectKinds: ["table"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "supported",
          triggers: "not_applicable",
        },
      },
      elasticsearch: {
        dbObjectKinds: ["table"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "not_applicable",
          triggers: "not_applicable",
        },
      },
      redis: {
        dbObjectKinds: ["table"],
        tableSections: {
          columns: "supported",
          constraints: "not_applicable",
          indexes: "not_applicable",
          triggers: "not_applicable",
        },
      },
    });
  });
});
