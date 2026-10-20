import { EventEmitter } from "node:events";
import type mssql from "mssql";
import { vi } from "vitest";

export class MockMssqlConnection {
  inTransaction = false;
  closed = false;
  hasError = false;
  reset = vi.fn((callback: (error?: unknown) => void) => {
    this.inTransaction = false;
    callback();
  });
  close = vi.fn(() => {
    this.closed = true;
  });
}

// A real exclusive lease/queue, not the old pool.request-only stub. A borrower
// already waiting at release observes exactly the state exposed by production.
export function mockMssqlSessionPool(requests: object[] | (() => object)) {
  let next = 0;
  let borrowed = false;
  let connection = new MockMssqlConnection();
  const connections = [connection];
  const waiting: Array<
    (error: unknown, connection: MockMssqlConnection) => void
  > = [];
  const lend = () => {
    if (borrowed || !waiting.length) return;
    if (connection.closed || connection.hasError) {
      connection = new MockMssqlConnection();
      connections.push(connection);
    }
    borrowed = true;
    waiting.shift()?.(null, connection);
  };
  const pool = {
    config: { requestTimeout: 5000 },
    connected: true,
    collation: undefined,
    connections,
    waiting,
    acquire: vi.fn(
      (
        _requester: unknown,
        callback: (error: unknown, connection: MockMssqlConnection) => void,
      ) => {
        waiting.push(callback);
        lend();
      },
    ),
    release: vi.fn((released: MockMssqlConnection) => {
      if (!borrowed || released !== connection)
        throw new Error("Invalid/double release");
      borrowed = false;
      lend();
    }),
    request: vi.fn(
      () =>
        (typeof requests === "function"
          ? requests()
          : requests[next++]) as mssql.Request,
    ),
    close: vi.fn(async () => {}),
  };
  return pool;
}

export class MockMssqlRequest extends EventEmitter {
  stream = false;
  arrayRowMode = false;
  overrides = {};
  parameters: Record<string, unknown> = {};
  parent!: {
    acquire(
      request: unknown,
      callback: (error: unknown, connection: MockMssqlConnection) => void,
    ): void;
    release(connection: MockMssqlConnection): void;
  };
  input = vi.fn((name: string, _type: unknown, value: unknown) => {
    this.parameters[name] = value;
    return this;
  });
  cancel = vi.fn();
  execute = vi.fn(
    async (
      _sql: string,
      _connection: MockMssqlConnection,
    ): Promise<{
      recordsets: unknown[];
      recordset: unknown[];
      rowsAffected: number[];
      output: Record<string, unknown>;
    }> => ({
      recordsets: [],
      recordset: [],
      rowsAffected: [0],
      output: {},
    }),
  );
  private dispatch(sql: string) {
    return new Promise<Awaited<ReturnType<typeof this.execute>>>(
      (resolve, reject) => {
        this.parent.acquire(this, (error, connection) => {
          if (error) return reject(error);
          void this.execute(sql, connection).then(
            (result) => {
              this.parent.release(connection);
              resolve(result);
            },
            (failure) => {
              this.parent.release(connection);
              reject(failure);
            },
          );
        });
      },
    );
  }
  query = vi.fn((sql: string) => this.dispatch(sql));
  batch = vi.fn((sql: string) => this.dispatch(sql));
}
