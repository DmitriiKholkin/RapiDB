import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

afterEach(() => {
  vi.resetModules();
});

describe("pool leak on failed connect (stage 3)", () => {
  it("postgres: ends probe pool when connect fails", async () => {
    const end = vi.fn(async () => undefined);
    const failingPool = {
      on: vi.fn(),
      connect: vi.fn(async () => {
        throw new Error("connect refused");
      }),
      end,
    };
    vi.doMock("pg", async (importOriginal) => {
      const actual = (await importOriginal()) as Record<string, unknown>;
      class MockPool {
        constructor() {
          // biome-ignore lint/correctness/noConstructorReturn: mock must stay a class for `new Pool()`
          return failingPool as never;
        }
      }
      return {
        ...actual,
        Pool: MockPool,
      };
    });

    const { PostgresDriver } = await import(
      "../../src/extension/dbDrivers/postgres"
    );
    const driver = new PostgresDriver({
      id: "pg-leak",
      name: "pg",
      type: "pg",
      host: "localhost",
      port: 5432,
      database: "db",
      username: "u",
      password: "p",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("connect refused");
    expect(end).toHaveBeenCalledTimes(1);
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });

  it("postgres: ends probe pool when probe query fails and resets state", async () => {
    const end = vi.fn(async () => undefined);
    const release = vi.fn();
    const queryFailingPool = {
      on: vi.fn(),
      connect: vi.fn(async () => ({
        query: vi.fn(async () => {
          throw new Error("db gone");
        }),
        release,
      })),
      end,
    };
    vi.doMock("pg", async (importOriginal) => {
      const actual = (await importOriginal()) as Record<string, unknown>;
      class MockPool {
        constructor() {
          // biome-ignore lint/correctness/noConstructorReturn: mock must stay a class for `new Pool()`
          return queryFailingPool as never;
        }
      }
      return { ...actual, Pool: MockPool };
    });

    const { PostgresDriver } = await import(
      "../../src/extension/dbDrivers/postgres"
    );
    const driver = new PostgresDriver({
      id: "pg-leak-q",
      name: "pg",
      type: "pg",
      host: "localhost",
      port: 5432,
      database: "db",
      username: "u",
      password: "p",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("db gone");
    expect(end).toHaveBeenCalledTimes(1);
    expect(release).toHaveBeenCalledTimes(1);
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });

  it("postgres: original error survives failing cleanup", async () => {
    const end = vi.fn(async () => {
      throw new Error("cleanup boom");
    });
    const failingPool = {
      on: vi.fn(),
      connect: vi.fn(async () => {
        throw new Error("connect refused");
      }),
      end,
    };
    vi.doMock("pg", async (importOriginal) => {
      const actual = (await importOriginal()) as Record<string, unknown>;
      class MockPool {
        constructor() {
          // biome-ignore lint/correctness/noConstructorReturn: mock must stay a class for `new Pool()`
          return failingPool as never;
        }
      }
      return { ...actual, Pool: MockPool };
    });

    const { PostgresDriver } = await import(
      "../../src/extension/dbDrivers/postgres"
    );
    const driver = new PostgresDriver({
      id: "pg-leak-cleanup",
      name: "pg",
      type: "pg",
      host: "localhost",
      port: 5432,
      database: "db",
      username: "u",
      password: "p",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("connect refused");
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
  });

  it("postgres: disconnect nulls pool even when end throws", async () => {
    vi.doMock("pg", async (importOriginal) => {
      const actual = (await importOriginal()) as Record<string, unknown>;
      return actual;
    });
    const { PostgresDriver } = await import(
      "../../src/extension/dbDrivers/postgres"
    );
    const driver = new PostgresDriver({
      id: "pg-disc",
      name: "pg",
      type: "pg",
      host: "localhost",
      port: 5432,
      database: "db",
      username: "u",
      password: "p",
    } as ConnectionConfig);
    (driver as unknown as { pool: { end: () => Promise<void> } }).pool = {
      end: async () => {
        throw new Error("end boom");
      },
    } as never;
    await expect(driver.disconnect()).rejects.toThrow("end boom");
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
  });

  it("mysql: ends pool when probe getConnection fails", async () => {
    const end = vi.fn(async () => undefined);
    const failingPool = {
      getConnection: vi.fn(async () => {
        throw new Error("ECONNREFUSED");
      }),
      end,
    };
    vi.doMock("mysql2/promise", () => ({
      createPool: vi.fn(() => failingPool),
    }));

    const { MySQLDriver } = await import("../../src/extension/dbDrivers/mysql");
    const driver = new MySQLDriver({
      id: "mysql-leak",
      name: "mysql",
      type: "mysql",
      host: "localhost",
      port: 3306,
      database: "db",
      username: "u",
      password: "p",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("ECONNREFUSED");
    expect(end).toHaveBeenCalledTimes(1);
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
  });

  it("oracle: closes pool when ping fails", async () => {
    const connClose = vi.fn(async () => undefined);
    const close = vi.fn(async () => undefined);
    const failingPool = {
      getConnection: vi.fn(async () => ({
        callTimeout: 0,
        ping: vi.fn(async () => {
          throw new Error("ping failed");
        }),
        close: connClose,
      })),
      close,
    };
    vi.doMock("oracledb", () => ({
      default: { createPool: vi.fn(async () => failingPool) },
    }));

    const { OracleDriver } = await import(
      "../../src/extension/dbDrivers/oracle"
    );
    const driver = new OracleDriver({
      id: "oracle-leak",
      name: "oracle",
      type: "oracle",
      host: "localhost",
      port: 1521,
      serviceName: "xe",
      username: "u",
      password: "p",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("ping failed");
    expect(close).toHaveBeenCalledTimes(1);
    expect(connClose).toHaveBeenCalledTimes(1);
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
  });

  it("oracle: closes pool when getConnection fails", async () => {
    const close = vi.fn(async () => undefined);
    vi.doMock("oracledb", () => ({
      default: {
        createPool: vi.fn(async () => ({
          getConnection: vi.fn(async () => {
            throw new Error("no listener");
          }),
          close,
        })),
      },
    }));

    const { OracleDriver } = await import(
      "../../src/extension/dbDrivers/oracle"
    );
    const driver = new OracleDriver({
      id: "oracle-leak-conn",
      name: "oracle",
      type: "oracle",
      host: "localhost",
      port: 1521,
      serviceName: "xe",
      username: "u",
      password: "p",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("no listener");
    expect(close).toHaveBeenCalledTimes(1);
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
  });

  it("oracle: ping error is not masked by conn.close error", async () => {
    const close = vi.fn(async () => undefined);
    vi.doMock("oracledb", () => ({
      default: {
        createPool: vi.fn(async () => ({
          getConnection: vi.fn(async () => ({
            callTimeout: 0,
            ping: vi.fn(async () => {
              throw new Error("ping failed");
            }),
            close: vi.fn(async () => {
              throw new Error("close boom");
            }),
          })),
          close,
        })),
      },
    }));

    const { OracleDriver } = await import(
      "../../src/extension/dbDrivers/oracle"
    );
    const driver = new OracleDriver({
      id: "oracle-mask",
      name: "oracle",
      type: "oracle",
      host: "localhost",
      port: 1521,
      serviceName: "xe",
      username: "u",
      password: "p",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("ping failed");
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("oracle: closes probe pool when releasing a healthy connection fails", async () => {
    const close = vi.fn(async () => undefined);
    vi.doMock("oracledb", () => ({
      default: {
        createPool: vi.fn(async () => ({
          getConnection: vi.fn(async () => ({
            callTimeout: 0,
            ping: vi.fn(async () => undefined),
            close: vi.fn(async () => {
              throw new Error("release failed");
            }),
          })),
          close,
        })),
      },
    }));
    const { OracleDriver } = await import(
      "../../src/extension/dbDrivers/oracle"
    );
    const driver = new OracleDriver({
      id: "oracle-release",
      name: "oracle",
      type: "oracle",
      host: "localhost",
      serviceName: "xe",
    } as ConnectionConfig);

    await expect(driver.connect()).rejects.toThrow("release failed");
    expect(close).toHaveBeenCalledWith(0);
    expect((driver as unknown as { pool: unknown }).pool).toBeNull();
  });
});
