import { afterEach, describe, expect, it, vi } from "vitest";

const timeoutSettingsProvider = () => ({
  connectionTimeoutSeconds: 7,
  dbOperationTimeoutSeconds: 42,
  connectionTimeoutMs: 7000,
  dbOperationTimeoutMs: 42000,
});

afterEach(() => {
  vi.resetModules();
});

describe("native driver timeout wiring", () => {
  it("passes configured timeouts into the MySQL pool and query options", async () => {
    const pool = {
      getConnection: vi.fn(async () => ({
        release: vi.fn(),
      })),
      end: vi.fn(async () => undefined),
      query: vi.fn(async () => [[{ Database: "app_db" }], []]),
    };
    const createPool = vi.fn(() => pool);

    vi.doMock("mysql2/promise", () => ({
      createPool,
    }));

    const { MySQLDriver } = await import("../../src/extension/dbDrivers/mysql");

    const driver = new MySQLDriver(
      {
        id: "mysql-1",
        name: "MySQL",
        type: "mysql",
        host: "127.0.0.1",
        port: 3306,
        database: "app_db",
        username: "user",
        password: "secret",
      },
      timeoutSettingsProvider,
    );

    await driver.connect();
    await driver.listDatabases();

    expect(createPool).toHaveBeenCalledWith(
      expect.objectContaining({
        connectTimeout: 7000,
      }),
    );
    expect(pool.query).toHaveBeenCalledWith(
      expect.objectContaining({
        sql: "SHOW DATABASES",
        timeout: 42000,
      }),
    );
  });

  it("passes configured timeouts into the PostgreSQL pool", async () => {
    const poolConfigs: unknown[] = [];
    class MockPool {
      readonly connect = vi.fn(async () => ({
        query: vi.fn(async () => ({
          rows: [{ name: "app_db" }],
        })),
        release: vi.fn(),
      }));

      readonly end = vi.fn(async () => undefined);

      readonly on = vi.fn();

      constructor(config: unknown) {
        poolConfigs.push(config);
      }
    }

    vi.doMock("pg", () => ({
      Pool: MockPool,
      types: {
        setTypeParser: vi.fn(),
      },
    }));

    const { PostgresDriver } = await import(
      "../../src/extension/dbDrivers/postgres"
    );

    const driver = new PostgresDriver(
      {
        id: "pg-1",
        name: "Postgres",
        type: "pg",
        host: "127.0.0.1",
        port: 5432,
        database: "app_db",
        username: "user",
        password: "secret",
      },
      timeoutSettingsProvider,
    );

    await driver.connect();

    expect(poolConfigs[0]).toMatchObject({
      connectionTimeoutMillis: 7000,
      query_timeout: 42000,
      statement_timeout: 42000,
    });
  });

  it("passes configured timeouts into the MSSQL pool", async () => {
    const poolConfigs: unknown[] = [];
    class MockConnectionPool {
      readonly connect = vi.fn(async () => this);

      readonly close = vi.fn(async () => undefined);

      readonly on = vi.fn();

      constructor(config: unknown) {
        poolConfigs.push(config);
      }
    }

    vi.doMock("mssql", () => ({
      ConnectionPool: MockConnectionPool,
    }));

    const { MSSQLDriver } = await import("../../src/extension/dbDrivers/mssql");

    const driver = new MSSQLDriver(
      {
        id: "mssql-1",
        name: "MSSQL",
        type: "mssql",
        host: "127.0.0.1",
        port: 1433,
        database: "app_db",
        username: "sa",
        password: "secret",
      },
      timeoutSettingsProvider,
    );

    await driver.connect();

    expect(poolConfigs[0]).toMatchObject({
      connectionTimeout: 7000,
      requestTimeout: 42000,
    });
  });

  it("applies the configured operation timeout to Oracle connections", async () => {
    const bootstrapConnection = {
      callTimeout: 0,
      ping: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const queryConnection = {
      callTimeout: 0,
      execute: vi.fn(async () => ({
        rows: [{ ORA_DATABASE_NAME: "app_db" }],
      })),
      close: vi.fn(async () => undefined),
    };
    const pool = {
      getConnection: vi
        .fn()
        .mockResolvedValueOnce(bootstrapConnection)
        .mockResolvedValueOnce(queryConnection),
      close: vi.fn(async () => undefined),
      connectionsInUse: 1,
      connectionsOpen: 1,
    };
    const createPool = vi.fn(async () => pool);

    vi.doMock("oracledb", () => ({
      default: {
        createPool,
        OUT_FORMAT_OBJECT: 1,
      },
    }));

    const { OracleDriver } = await import(
      "../../src/extension/dbDrivers/oracle"
    );

    const driver = new OracleDriver(
      {
        id: "oracle-1",
        name: "Oracle",
        type: "oracle",
        host: "127.0.0.1",
        port: 1521,
        serviceName: "xe",
        username: "user",
        password: "secret",
      },
      timeoutSettingsProvider,
    );

    await driver.connect();
    await driver.listDatabases();

    expect(createPool).toHaveBeenCalledWith(
      expect.objectContaining({
        connectTimeout: 7,
        transportConnectTimeout: 7,
      }),
    );
    expect(bootstrapConnection.callTimeout).toBe(42000);
    expect(queryConnection.callTimeout).toBe(42000);
  });
});
