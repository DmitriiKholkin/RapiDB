import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

describe("targeted query cancellation", () => {
  it("cancels only the MSSQL request with the matching token", async () => {
    const requests = [1, 2].map(() => {
      let rejectQuery: ((error: Error) => void) | undefined;
      return {
        arrayRowMode: false,
        query: vi.fn(
          () =>
            new Promise((_, reject) => {
              rejectQuery = reject;
            }),
        ),
        cancel: vi.fn(() => rejectQuery?.(new Error("cancelled"))),
      };
    });
    let requestIndex = 0;
    const driver = new MSSQLDriver({
      id: "mssql-targeted",
      name: "MSSQL",
      type: "mssql",
      host: "localhost",
      database: "master",
    } as ConnectionConfig);
    (driver as unknown as { pool: unknown }).pool = {
      connected: true,
      request: () => requests[requestIndex++],
    };

    const first = driver.query("select 1", undefined, { requestToken: 11 });
    const second = driver.query("select 2", undefined, { requestToken: 22 });
    await Promise.resolve();
    await driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 11,
    });

    expect(requests[0].cancel).toHaveBeenCalledOnce();
    expect(requests[1].cancel).not.toHaveBeenCalled();
    await expect(first).rejects.toThrow("cancelled");
    requests[1].cancel();
    await expect(second).rejects.toThrow("cancelled");
  });

  it("breaks and rolls back timed-out Oracle DML without a late commit", async () => {
    let rejectDml: ((error: Error) => void) | undefined;
    const connection = {
      callTimeout: 0,
      execute: vi
        .fn()
        .mockResolvedValueOnce({})
        .mockResolvedValueOnce({})
        .mockImplementationOnce(
          () =>
            new Promise((_, reject) => {
              rejectDml = reject;
            }),
        ),
      break: vi.fn(async () => rejectDml?.(new Error("ORA-01013"))),
      rollback: vi.fn(async () => undefined),
      commit: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const driver = new OracleDriver({
      id: "oracle-targeted",
      name: "Oracle",
      type: "oracle",
      host: "localhost",
      serviceName: "xe",
    } as ConnectionConfig);
    (driver as unknown as { pool: unknown }).pool = {
      connectionsInUse: 0,
      connectionsOpen: 1,
      getConnection: vi.fn(async () => connection),
    };

    const pending = driver.query("update users set active = 1", undefined, {
      requestToken: 31,
    });
    while (connection.execute.mock.calls.length < 3) {
      await Promise.resolve();
    }
    await driver.cancelCurrentOperation({
      reason: "timeout",
      operationName: "query",
      requestToken: 31,
    });

    await expect(pending).rejects.toThrow("ORA-01013");
    expect(connection.break).toHaveBeenCalledOnce();
    expect(connection.rollback).toHaveBeenCalledOnce();
    expect(connection.commit).not.toHaveBeenCalled();
  });
});
