import { describe, expect, it } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ReadOnlyQueryGuard } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function requireReadOnlyQueryGuard(guard: ReadOnlyQueryGuard | undefined) {
  if (!guard) {
    throw new Error("Expected driver readOnlyQueryGuard to be defined");
  }
  return guard;
}

describe("readonly query guards", () => {
  it("allows dialect-safe read-only SQL statements", () => {
    const driver = new PostgresDriver({
      id: "pg-readonly-guard",
      name: "pg-readonly-guard",
      type: "pg",
      host: "localhost",
      port: 5432,
      database: "postgres",
      username: "postgres",
      password: "postgres",
    } as ConnectionConfig);
    const guard = requireReadOnlyQueryGuard(
      driver.getCapabilities().readOnlyQueryGuard,
    );

    expect(guard("select 1")).toEqual({ allowed: true });
    expect(guard("show search_path")).toEqual({ allowed: true });
    expect(guard("table users")).toEqual({ allowed: true });
    expect(guard("values (1), (2)")).toEqual({ allowed: true });
    expect(guard("explain select * from users")).toEqual({ allowed: true });
    expect(guard("explain analyze select * from users")).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only SQL connections allow only read-only queries.",
    });
    expect(
      guard(
        "with moved as (delete from users returning *) select * from moved",
      ),
    ).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only SQL connections allow only read-only queries.",
    });
    expect(guard("select * into temp_users from users")).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only SQL connections allow only read-only queries.",
    });
  });

  it("allows dialect-specific metadata commands for MySQL and SQLite", () => {
    const mysqlDriver = new MySQLDriver({
      id: "mysql-readonly-guard",
      name: "mysql-readonly-guard",
      type: "mysql",
      host: "localhost",
      port: 3306,
      database: "mysql",
      username: "root",
      password: "root",
    } as ConnectionConfig);
    const mysqlGuard = requireReadOnlyQueryGuard(
      mysqlDriver.getCapabilities().readOnlyQueryGuard,
    );

    expect(mysqlGuard("show databases")).toEqual({ allowed: true });
    expect(mysqlGuard("describe users")).toEqual({ allowed: true });
    expect(mysqlGuard("desc select * from users")).toEqual({
      allowed: true,
    });
    expect(mysqlGuard("explain format=json select * from users")).toEqual({
      allowed: true,
    });

    const sqliteDriver = new SQLiteDriver({
      id: "sqlite-readonly-guard",
      name: "sqlite-readonly-guard",
      type: "sqlite",
      filePath: "/tmp/test.db",
    } as ConnectionConfig);
    const sqliteGuard = requireReadOnlyQueryGuard(
      sqliteDriver.getCapabilities().readOnlyQueryGuard,
    );

    expect(sqliteGuard('pragma table_xinfo("users")')).toEqual({
      allowed: true,
    });
    expect(sqliteGuard("explain query plan select * from users")).toEqual({
      allowed: true,
    });
    expect(sqliteGuard("pragma foreign_keys = on")).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only SQL connections allow only read-only queries.",
    });
  });

  it("keeps readonly SQL coverage for MSSQL and Oracle dialects", () => {
    const mssqlDriver = new MSSQLDriver({
      id: "mssql-readonly-guard",
      name: "mssql-readonly-guard",
      type: "mssql",
      host: "localhost",
      port: 1433,
      database: "master",
      username: "sa",
      password: "pass",
    } as ConnectionConfig);
    const mssqlGuard = requireReadOnlyQueryGuard(
      mssqlDriver.getCapabilities().readOnlyQueryGuard,
    );

    expect(mssqlGuard("select 1")).toEqual({ allowed: true });
    expect(mssqlGuard("values (1), (2)")).toEqual({ allowed: true });
    expect(mssqlGuard("exec sp_help users")).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only SQL connections allow only read-only queries.",
    });

    const oracleDriver = new OracleDriver({
      id: "oracle-readonly-guard",
      name: "oracle-readonly-guard",
      type: "oracle",
      host: "localhost",
      port: 1521,
      serviceName: "xe",
      username: "system",
      password: "oracle",
    } as ConnectionConfig);
    const oracleGuard = requireReadOnlyQueryGuard(
      oracleDriver.getCapabilities().readOnlyQueryGuard,
    );

    expect(oracleGuard("select 1 from dual")).toEqual({ allowed: true });
    expect(
      oracleGuard("with src as (select 1 from dual) select * from src"),
    ).toEqual({ allowed: true });
    expect(oracleGuard("explain plan for select 1 from dual")).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only SQL connections allow only read-only queries.",
    });
  });

  it("uses mongosh operation parsing to block MongoDB mutations", () => {
    const driver = new MongoDBDriver({
      id: "mongo-readonly-guard",
      name: "mongo-readonly-guard",
      type: "mongodb",
      host: "localhost",
    } as ConnectionConfig);
    const guard = requireReadOnlyQueryGuard(
      driver.getCapabilities().readOnlyQueryGuard,
    );

    expect(guard("db.users.find({ active: true })")).toEqual({
      allowed: true,
    });
    expect(
      guard(
        'db.users.aggregate([{ $match: { active: true } }, { $out: "archive" }])',
      ),
    ).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only MongoDB connections allow only find, findOne, countDocuments, and aggregate queries without $out or $merge.",
    });
    expect(guard("db.users.deleteMany({ active: false })")).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only MongoDB connections allow only find, findOne, countDocuments, and aggregate queries without $out or $merge.",
    });
  });

  it("uses Redis command parsing to block write commands", () => {
    const driver = new RedisDriver({
      id: "redis-readonly-guard",
      name: "redis-readonly-guard",
      type: "redis",
      host: "localhost",
    } as ConnectionConfig);
    const guard = requireReadOnlyQueryGuard(
      driver.getCapabilities().readOnlyQueryGuard,
    );

    expect(guard("GET users:1")).toEqual({ allowed: true });
    expect(guard('SET users:1 "Alice"')).toEqual({
      allowed: false,
      reason: "[RapiDB] Read-only Redis connections allow only read commands.",
    });
  });

  it("uses REST method and path classification for Elasticsearch", () => {
    const driver = new ElasticsearchDriver({
      id: "es-readonly-guard",
      name: "es-readonly-guard",
      type: "elasticsearch",
      host: "localhost",
    } as ConnectionConfig);
    const guard = requireReadOnlyQueryGuard(
      driver.getCapabilities().readOnlyQueryGuard,
    );

    expect(guard('POST /users/_search\n{"query":{"match_all":{}}}')).toEqual({
      allowed: true,
    });
    expect(guard("DELETE /users/_doc/doc-1")).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only Elasticsearch connections allow only GET requests and POST _search requests.",
    });
  });

  it("uses DynamoDB request-shape inference to block mutations", () => {
    const driver = new DynamoDBDriver({
      id: "dynamo-readonly-guard",
      name: "dynamo-readonly-guard",
      type: "dynamodb",
      awsRegion: "us-east-1",
    } as ConnectionConfig);
    const guard = requireReadOnlyQueryGuard(
      driver.getCapabilities().readOnlyQueryGuard,
    );

    expect(guard('{"TableName":"Users","Key":{"id":{"S":"1"}}}')).toEqual({
      allowed: true,
    });
    expect(guard('{"TableName":"Users","Item":{"id":{"S":"1"}}}')).toEqual({
      allowed: false,
      reason:
        "[RapiDB] Read-only DynamoDB connections allow only GetItem, BatchGetItem, Query, Scan, and TransactGetItems requests.",
    });
  });

  it("safe-denies malformed DynamoDB payloads", () => {
    const driver = new DynamoDBDriver({
      id: "dynamo-readonly-malformed",
      name: "dynamo-readonly-malformed",
      type: "dynamodb",
      awsRegion: "us-east-1",
    } as ConnectionConfig);
    const guard = requireReadOnlyQueryGuard(
      driver.getCapabilities().readOnlyQueryGuard,
    );

    expect(guard("{")).toEqual(
      expect.objectContaining({
        allowed: false,
        reason: expect.any(String),
      }),
    );
  });
});
