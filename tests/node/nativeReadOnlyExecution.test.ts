import { describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

describe("database-native read-only query execution", () => {
  it("wraps MySQL editor reads in a READ ONLY transaction", async () => {
    const lifecycle: string[] = [];
    const query = vi.fn(async (input: unknown) => {
      if (typeof input === "string") {
        lifecycle.push(input);
        return [[], []];
      }
      lifecycle.push("query");
      return [[[1]], [{ name: "value", type: 3 }]];
    });
    const connection = {
      query,
      release: vi.fn(() => {
        lifecycle.push("release");
      }),
      destroy: vi.fn(),
    };
    const driver = new MySQLDriver({
      id: "mysql-readonly-native",
      name: "MySQL",
      type: "mysql",
      host: "localhost",
      database: "test",
    } as ConnectionConfig);
    (driver as unknown as { pool: unknown }).pool = {
      getConnection: vi.fn(async () => connection),
    };

    await driver.query("select 1; select 2", undefined, {
      requestToken: 1,
      readOnly: true,
    });

    expect(query.mock.calls[0]?.[0]).toBe("START TRANSACTION READ ONLY");
    expect(query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(lifecycle).toEqual([
      "START TRANSACTION READ ONLY",
      "query",
      "query",
      "ROLLBACK",
      "release",
    ]);
  });

  it("wraps PostgreSQL editor reads in a READ ONLY transaction", async () => {
    const query = vi.fn(async (input: unknown) => {
      if (typeof input === "string") {
        return { rows: [] };
      }
      return { fields: [{ name: "value" }], rows: [[1]], rowCount: 1 };
    });
    const client = { query, release: vi.fn() };
    const driver = new PostgresDriver({
      id: "pg-readonly-native",
      name: "Postgres",
      type: "pg",
      host: "localhost",
      database: "test",
    } as ConnectionConfig);
    (driver as unknown as { pool: unknown; _connected: boolean }).pool = {
      connect: vi.fn(async () => client),
      waitingCount: 0,
      idleCount: 1,
      totalCount: 1,
    };
    (driver as unknown as { _connected: boolean })._connected = true;

    await driver.query("select 1; select 2", undefined, {
      requestToken: 2,
      readOnly: true,
    });

    expect(query.mock.calls[0]?.[0]).toBe("BEGIN READ ONLY");
    expect(query.mock.calls[1]?.[0]).toMatchObject({
      text: "select 1; select 2",
    });
    expect(query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
  });

  it("uses an Oracle READ ONLY transaction and never commits it", async () => {
    const execute = vi
      .fn()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({
        metaData: [{ name: "VALUE" }],
        rows: [[1]],
      });
    const connection = {
      callTimeout: 0,
      execute,
      rollback: vi.fn(async () => undefined),
      commit: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const driver = new OracleDriver({
      id: "oracle-readonly-native",
      name: "Oracle",
      type: "oracle",
      host: "localhost",
      serviceName: "xe",
    } as ConnectionConfig);
    (driver as unknown as { pool: unknown }).pool = {
      getConnection: vi.fn(async () => connection),
      connectionsInUse: 0,
      connectionsOpen: 1,
    };

    await driver.query("select 1 from dual; select 2 from dual", undefined, {
      requestToken: 3,
      readOnly: true,
    });

    expect(execute.mock.calls[2]?.[0]).toBe("SET TRANSACTION READ ONLY");
    expect(execute.mock.calls[3]?.[0]).toBe("select 1 from dual");
    expect(execute.mock.calls[4]?.[0]).toBe("select 2 from dual");
    expect(connection.rollback).toHaveBeenCalledOnce();
    expect(connection.commit).not.toHaveBeenCalled();
  });
});
