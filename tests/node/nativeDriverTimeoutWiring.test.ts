import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

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
        sql: expect.stringContaining("FROM information_schema.SCHEMATA"),
        timeout: 42000,
      }),
    );
  });

  it("routes MySQL SSH connections through the forwarded socket and preserves TLS servername", async () => {
    const pool = {
      getConnection: vi.fn(async () => ({
        release: vi.fn(),
      })),
      end: vi.fn(async () => undefined),
    };
    const createPool = vi.fn(() => pool);

    vi.doMock("mysql2/promise", () => ({
      createPool,
    }));

    const { MySQLDriver } = await import("../../src/extension/dbDrivers/mysql");

    const driver = new MySQLDriver(
      {
        id: "mysql-ssh",
        name: "MySQL SSH",
        type: "mysql",
        host: "127.0.0.1",
        port: 13306,
        database: "app_db",
        username: "user",
        password: "secret",
        ssl: true,
        rejectUnauthorized: true,
        runtimeOverrides: {
          transport: {
            kind: "tcpForward",
            localHost: "127.0.0.1",
            localPort: 13306,
            remoteHost: "mysql.internal",
            remotePort: 3306,
          },
          tlsServername: "mysql.internal",
        },
      } as ConnectionConfig,
      timeoutSettingsProvider,
    );

    await driver.connect();

    expect(createPool).toHaveBeenCalledWith(
      expect.objectContaining({
        host: "127.0.0.1",
        port: 13306,
        ssl: expect.objectContaining({
          rejectUnauthorized: true,
          servername: "mysql.internal",
        }),
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

  it("routes PostgreSQL SSH connections through the forwarded socket and preserves TLS servername", async () => {
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
        id: "pg-ssh",
        name: "Postgres SSH",
        type: "pg",
        host: "127.0.0.1",
        port: 15432,
        database: "app_db",
        username: "user",
        password: "secret",
        ssl: true,
        rejectUnauthorized: true,
        runtimeOverrides: {
          transport: {
            kind: "tcpForward",
            localHost: "127.0.0.1",
            localPort: 15432,
            remoteHost: "pg.internal",
            remotePort: 5432,
          },
          tlsServername: "pg.internal",
        },
      } as ConnectionConfig,
      timeoutSettingsProvider,
    );

    await driver.connect();

    expect(poolConfigs[0]).toMatchObject({
      host: "127.0.0.1",
      port: 15432,
      ssl: expect.objectContaining({
        rejectUnauthorized: true,
        servername: "pg.internal",
      }),
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

  it("routes MSSQL SSH connections through the forwarded socket and preserves serverName for strict TLS", async () => {
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
        id: "mssql-ssh",
        name: "MSSQL SSH",
        type: "mssql",
        host: "127.0.0.1",
        port: 11433,
        database: "app_db",
        username: "sa",
        password: "secret",
        ssl: true,
        rejectUnauthorized: true,
        runtimeOverrides: {
          transport: {
            kind: "tcpForward",
            localHost: "127.0.0.1",
            localPort: 11433,
            remoteHost: "sql.internal",
            remotePort: 1433,
          },
          mssqlServerName: "sql.internal",
        },
      } as ConnectionConfig,
      timeoutSettingsProvider,
    );

    await driver.connect();

    expect(poolConfigs[0]).toMatchObject({
      server: "127.0.0.1",
      port: 11433,
      options: expect.objectContaining({
        encrypt: true,
        trustServerCertificate: false,
        serverName: "sql.internal",
      }),
    });
  });

  it("preserves the forwarded MSSQL serverName when trustServerCertificate is enabled", async () => {
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
        id: "mssql-ssh-trust-cert",
        name: "MSSQL SSH Trust Cert",
        type: "mssql",
        host: "127.0.0.1",
        port: 11433,
        database: "app_db",
        username: "sa",
        password: "secret",
        ssl: true,
        rejectUnauthorized: false,
        runtimeOverrides: {
          transport: {
            kind: "tcpForward",
            localHost: "127.0.0.1",
            localPort: 11433,
            remoteHost: "sql.internal",
            remotePort: 1433,
          },
          mssqlServerName: "sql.internal",
        },
      } as ConnectionConfig,
      timeoutSettingsProvider,
    );

    await driver.connect();

    expect(poolConfigs[0]).toMatchObject({
      server: "127.0.0.1",
      port: 11433,
      options: expect.objectContaining({
        encrypt: true,
        trustServerCertificate: true,
        serverName: "sql.internal",
      }),
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

  it("routes Oracle SSH connections through the forwarded socket", async () => {
    const bootstrapConnection = {
      callTimeout: 0,
      ping: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const pool = {
      getConnection: vi.fn().mockResolvedValue(bootstrapConnection),
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
        id: "oracle-ssh",
        name: "Oracle SSH",
        type: "oracle",
        host: "127.0.0.1",
        port: 11521,
        serviceName: "xe",
        username: "user",
        password: "secret",
        runtimeOverrides: {
          transport: {
            kind: "tcpForward",
            localHost: "127.0.0.1",
            localPort: 11521,
            remoteHost: "oracle.internal",
            remotePort: 1521,
          },
        },
      } as ConnectionConfig,
      timeoutSettingsProvider,
    );

    await driver.connect();

    expect(createPool).toHaveBeenCalledWith(
      expect.objectContaining({
        connectString: "127.0.0.1:11521/xe",
      }),
    );
  });

  it("routes Redis SSH TLS connections through the forwarded socket and preserves TLS servername", async () => {
    const client = {
      on: vi.fn(),
      connect: vi.fn(async () => undefined),
      select: vi.fn(async () => undefined),
      quit: vi.fn(async () => undefined),
    };
    const createClient = vi.fn(() => client);

    vi.doMock("redis", () => ({
      createClient,
    }));

    const { RedisDriver } = await import("../../src/extension/dbDrivers/redis");

    const driver = new RedisDriver({
      id: "redis-ssh",
      name: "Redis SSH",
      type: "redis",
      connectionUri: "rediss://user:pass@127.0.0.1:16379/0",
      host: "127.0.0.1",
      port: 16379,
      ssl: true,
      rejectUnauthorized: true,
      runtimeOverrides: {
        transport: {
          kind: "tcpForward",
          localHost: "127.0.0.1",
          localPort: 16379,
          remoteHost: "redis.internal",
          remotePort: 6379,
        },
        tlsServername: "redis.internal",
      },
    } as ConnectionConfig);

    await driver.connect();

    expect(createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        url: "rediss://user:pass@127.0.0.1:16379/0",
        socket: expect.objectContaining({
          host: "127.0.0.1",
          port: 16379,
          tls: true,
          servername: "redis.internal",
        }),
      }),
    );
  });
});
