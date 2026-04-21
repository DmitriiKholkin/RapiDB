import mssql from "mssql";
import mysql from "mysql2/promise";
import sqlite3Wasm from "node-sqlite3-wasm";
import oracledb from "oracledb";
import { Client } from "pg";
import type { ConnectionConfig } from "../../src/shared/connectionConfig.ts";
import type { DbEngineId } from "../contracts/testingContracts.ts";
import { ensureParentDirectory } from "./tempDirectories.ts";

const { Database } = sqlite3Wasm;

export interface SqlExecutor {
  execute(sql: string): Promise<void>;
  queryScalar(sql: string): Promise<unknown>;
  close(): Promise<void>;
}

async function openPostgresExecutor(
  connection: ConnectionConfig,
): Promise<SqlExecutor> {
  const client = new Client({
    host: connection.host,
    port: connection.port,
    database: connection.database,
    user: connection.username,
    password: connection.password,
    connectionTimeoutMillis: 10_000,
  });
  await client.connect();
  return {
    async execute(sql) {
      await client.query(sql);
    },
    async queryScalar(sql) {
      const result = await client.query(sql);
      return Object.values(result.rows[0] ?? {})[0] ?? null;
    },
    async close() {
      await client.end();
    },
  };
}

async function openMySqlExecutor(
  connection: ConnectionConfig,
): Promise<SqlExecutor> {
  const client = await mysql.createConnection({
    host: connection.host,
    port: connection.port,
    database: connection.database,
    user: connection.username,
    password: connection.password,
    connectTimeout: 10_000,
    timezone: "Z",
  });
  return {
    async execute(sql) {
      await client.query(sql);
    },
    async queryScalar(sql) {
      const [rows] = await client.query(sql);
      const firstRow = Array.isArray(rows)
        ? (rows[0] as Record<string, unknown> | undefined)
        : undefined;
      return firstRow ? (Object.values(firstRow)[0] ?? null) : null;
    },
    async close() {
      await client.end();
    },
  };
}

async function openMssqlExecutor(
  connection: ConnectionConfig,
): Promise<SqlExecutor> {
  const pool = new mssql.ConnectionPool({
    server: connection.host ?? "127.0.0.1",
    port: connection.port,
    database: connection.database,
    user: connection.username,
    password: connection.password,
    connectionTimeout: 10_000,
    requestTimeout: 30_000,
    options: {
      encrypt: connection.ssl ?? true,
      trustServerCertificate: !(connection.rejectUnauthorized ?? false),
      enableArithAbort: true,
      useUTC: true,
    },
  });
  await pool.connect();
  return {
    async execute(sql) {
      await pool.request().query(sql);
    },
    async queryScalar(sql) {
      const result = await pool.request().query(sql);
      return Object.values(result.recordset[0] ?? {})[0] ?? null;
    },
    async close() {
      await pool.close();
    },
  };
}

async function openOracleExecutor(
  connection: ConnectionConfig,
): Promise<SqlExecutor> {
  const connectString = connection.serviceName
    ? `${connection.host}:${connection.port}/${connection.serviceName}`
    : `${connection.host}:${connection.port}/${connection.database ?? ""}`;
  const client = await oracledb.getConnection({
    user: connection.username,
    password: connection.password,
    connectString,
  });
  return {
    async execute(sql) {
      await client.execute(sql, [], { autoCommit: true });
    },
    async queryScalar(sql) {
      const result = await client.execute<Record<string, unknown>>(sql, [], {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      });
      const row = result.rows?.[0] as Record<string, unknown> | undefined;
      return row ? (Object.values(row)[0] ?? null) : null;
    },
    async close() {
      await client.close();
    },
  };
}

async function openSqliteExecutor(
  connection: ConnectionConfig,
): Promise<SqlExecutor> {
  if (!connection.filePath) {
    throw new Error("[RapiDB:testdb] SQLite connection requires a file path.");
  }

  await ensureParentDirectory(connection.filePath);
  const db = new Database(connection.filePath);
  db.exec("PRAGMA foreign_keys = ON");
  return {
    async execute(sql) {
      db.exec(sql);
    },
    async queryScalar(sql) {
      const row = db.get(sql) as Record<string, unknown> | undefined;
      return row ? (Object.values(row)[0] ?? null) : null;
    },
    async close() {
      db.close();
    },
  };
}

export async function openSqlExecutor(
  engineId: DbEngineId,
  connection: ConnectionConfig,
): Promise<SqlExecutor> {
  switch (engineId) {
    case "postgres":
      return openPostgresExecutor(connection);
    case "mysql":
      return openMySqlExecutor(connection);
    case "mssql":
      return openMssqlExecutor(connection);
    case "oracle":
      return openOracleExecutor(connection);
    case "sqlite":
      return openSqliteExecutor(connection);
  }
}

export async function withSqlExecutor<T>(
  engineId: DbEngineId,
  connection: ConnectionConfig,
  callback: (executor: SqlExecutor) => Promise<T>,
): Promise<T> {
  const executor = await openSqlExecutor(engineId, connection);
  try {
    return await callback(executor);
  } finally {
    await executor.close();
  }
}
