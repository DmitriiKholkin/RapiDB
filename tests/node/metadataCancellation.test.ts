import { afterEach, describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { getMetadataContext } from "../../src/extension/dbDrivers/metadataCancellation";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import {
  createTimeoutAwareDriver,
  DriverTimeoutError,
} from "../../src/extension/dbDrivers/timeout";

const settings = () => ({
  connectionTimeoutSeconds: 1,
  dbOperationTimeoutSeconds: 1,
  connectionTimeoutMs: 1000,
  dbOperationTimeoutMs: 25,
});
const config = { id: "metadata", name: "Metadata" };
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
async function expire(operation: Promise<unknown>) {
  const assertion =
    expect(operation).rejects.toBeInstanceOf(DriverTimeoutError);
  await vi.advanceTimersByTimeAsync(25);
  await assertion;
}
afterEach(() => vi.useRealTimers());

describe("request-scoped metadata timeout cancellation", () => {
  it("force-closes the owned MySQL socket even when destroy() does not settle its command", async () => {
    vi.useFakeTimers();
    const driver = new MySQLDriver({ ...config, type: "mysql" }, settings);
    const destroy = vi.fn();
    const stream = { destroy: vi.fn() };
    const release = vi.fn();
    Object.assign(driver, {
      pool: {
        pool: {
          _freeConnections: { length: 1 },
          _allConnections: { length: 1 },
          _connectionQueue: { length: 0 },
          config: { connectionLimit: 5 },
        },
        getConnection: async () => ({
          query: () => new Promise(() => undefined),
          destroy,
          release,
          connection: { stream },
        }),
      },
    });
    await expire(createTimeoutAwareDriver(driver, settings).listDatabases());
    expect(destroy).toHaveBeenCalledOnce();
    expect(stream.destroy).toHaveBeenCalledOnce();
    expect(release).not.toHaveBeenCalled();
  });

  it("never enqueues timed-out MySQL metadata when all pool connections are busy", async () => {
    vi.useFakeTimers();
    const driver = new MySQLDriver({ ...config, type: "mysql" }, settings);
    const core = {
      _freeConnections: { length: 0 },
      _allConnections: { length: 5 },
      _connectionQueue: { length: 0 },
      config: { connectionLimit: 5 },
    };
    const release = vi.fn();
    const getConnection = vi.fn(async () => ({
      query: async () => [[{ SCHEMA_NAME: "db" }], []],
      release,
      destroy: vi.fn(),
    }));
    Object.assign(driver, { pool: { pool: core, getConnection } });
    const wrapped = createTimeoutAwareDriver(driver, settings);
    for (let i = 0; i < 20; i++) await expire(wrapped.listDatabases());
    expect(getConnection).not.toHaveBeenCalled();
    expect(core._connectionQueue.length).toBe(0);
    core._freeConnections.length = 1;
    await vi.advanceTimersByTimeAsync(1000);
    expect(getConnection).not.toHaveBeenCalled();
    expect(await wrapped.listDatabases()).toEqual([
      { name: "db", schemas: [] },
    ]);
    expect(getConnection).toHaveBeenCalledOnce();
    expect(release).toHaveBeenCalledOnce();
  });

  it("cancels a MySQL metadata handshake and discards a late checkout without running SQL", async () => {
    vi.useFakeTimers();
    const driver = new MySQLDriver({ ...config, type: "mysql" }, settings);
    const native = { destroy: vi.fn(), stream: { destroy: vi.fn() } };
    const query = vi.fn();
    const release = vi.fn();
    const connection = {
      connection: native,
      destroy: native.destroy,
      query,
      release,
    };
    const checkout = deferred<typeof connection>();
    const allConnections = { length: 0, get: () => native };
    const getConnection = vi.fn(() => {
      allConnections.length++;
      return checkout.promise;
    });
    Object.assign(driver, {
      pool: {
        pool: {
          _freeConnections: { length: 0 },
          _allConnections: allConnections,
          _connectionQueue: { length: 0 },
          config: { connectionLimit: 5 },
        },
        getConnection,
      },
    });
    await expire(createTimeoutAwareDriver(driver, settings).listDatabases());
    expect(native.destroy).toHaveBeenCalledOnce();
    expect(native.stream.destroy).toHaveBeenCalledOnce();
    checkout.resolve(connection);
    await vi.advanceTimersByTimeAsync(0);
    expect(native.destroy).toHaveBeenCalledOnce();
    expect(query).not.toHaveBeenCalled();
    expect(release).not.toHaveBeenCalled();
  });

  it.each([
    "mysql",
    "postgres",
    "mssql",
    "oracle",
  ])("cancels only the owned %s native request", async (engine) => {
    vi.useFakeTimers();
    const pending = deferred<never>();
    const cancelled = vi.fn(() =>
      pending.reject(new Error("Native request cancelled")),
    );
    const query = vi.fn(() => pending.promise);
    const unrelated = {
      destroy: vi.fn(),
      release: vi.fn(),
      cancel: vi.fn(),
      break: vi.fn(),
    };
    let driver: MySQLDriver | PostgresDriver | MSSQLDriver | OracleDriver;
    if (engine === "mysql") {
      driver = new MySQLDriver({ ...config, type: "mysql" }, settings);
      Object.assign(driver, {
        pool: {
          pool: {
            _freeConnections: { length: 1 },
            _allConnections: { length: 1 },
            _connectionQueue: { length: 0 },
            config: { connectionLimit: 5 },
          },
          getConnection: async () => ({
            query,
            destroy: cancelled,
            release: vi.fn(),
          }),
        },
        activeQueryConnections: new Set([unrelated]),
      });
    } else if (engine === "postgres") {
      driver = new PostgresDriver({ ...config, type: "pg" }, settings);
      const client = { query, release: cancelled };
      Object.assign(driver, {
        pool: {
          connect: async () => client,
          idleCount: 1,
          waitingCount: 0,
          totalCount: 1,
        },
        activeQueryClients: new Set([unrelated]),
      });
    } else if (engine === "mssql") {
      driver = new MSSQLDriver({ ...config, type: "mssql" }, settings);
      Object.assign(driver, {
        pool: { request: () => ({ query, cancel: cancelled }) },
        activeRequests: new Map([[unrelated, 123]]),
      });
    } else {
      driver = new OracleDriver({ ...config, type: "oracle" }, settings);
      const execute = vi.fn((sql: string) =>
        sql.startsWith("ALTER SESSION")
          ? Promise.resolve({ rows: [] })
          : query(),
      );
      Object.assign(driver, {
        pool: {
          getConnection: async () => ({
            execute,
            break: cancelled,
            close: vi.fn(async () => undefined),
          }),
        },
        activeQueryOperations: new Set([
          { requestToken: 123, connection: unrelated },
        ]),
      });
    }
    const disconnect = vi.spyOn(driver, "disconnect");
    const operation = createTimeoutAwareDriver(
      driver,
      settings,
    ).listDatabases();
    await vi.advanceTimersByTimeAsync(0);
    expect(query).toHaveBeenCalledOnce();
    await expire(operation);
    expect(cancelled).toHaveBeenCalledOnce();
    expect(disconnect).not.toHaveBeenCalled();
    for (const fn of Object.values(unrelated))
      expect(fn).not.toHaveBeenCalled();
  });

  it.each([
    "mongodb",
    "dynamodb",
    "elasticsearch",
  ])("passes an abort signal to %s catalog SDK calls", async (engine) => {
    vi.useFakeTimers();
    let signal: AbortSignal | undefined;
    const pending = deferred<never>();
    const capture = (value: AbortSignal | undefined) => {
      signal = value;
      value?.addEventListener("abort", () => pending.reject(value.reason), {
        once: true,
      });
      return pending.promise;
    };
    let driver: MongoDBDriver | DynamoDBDriver | ElasticsearchDriver;
    if (engine === "mongodb") {
      driver = new MongoDBDriver({ ...config, type: "mongodb" });
      Object.assign(driver, {
        connected: true,
        client: {
          db: () => ({
            listCollections: (
              _filter: unknown,
              options: { signal?: AbortSignal },
            ) => ({ toArray: () => capture(options.signal) }),
          }),
        },
      });
    } else if (engine === "dynamodb") {
      driver = new DynamoDBDriver({ ...config, type: "dynamodb" });
      Object.assign(driver, {
        connected: true,
        client: {
          send: (_command: unknown, options: { abortSignal?: AbortSignal }) =>
            capture(options.abortSignal),
        },
      });
    } else {
      driver = new ElasticsearchDriver(
        { ...config, type: "elasticsearch" },
        settings,
      );
      Object.assign(driver, {
        connected: true,
        client: {
          indices: {
            resolveIndex: (
              _params: unknown,
              options: { signal?: AbortSignal },
            ) => capture(options.signal),
          },
        },
      });
    }
    const disconnect = vi.spyOn(driver, "disconnect");
    const operation = createTimeoutAwareDriver(driver, settings).listObjects(
      "db",
    );
    expect(signal?.aborted).toBe(false);
    await expire(operation);
    expect(signal?.aborted).toBe(true);
    expect(disconnect).not.toHaveBeenCalled();
  });

  it("isolates concurrent scopes and prevents fallback work after a swallowed cancellation", async () => {
    vi.useFakeTimers();
    const gate = deferred<void>();
    const signals: AbortSignal[] = [];
    const fallback = vi.fn();
    const wrapped = createTimeoutAwareDriver(
      {
        async listObjects(name: string) {
          const context = getMetadataContext();
          if (!context) throw new Error("Missing context");
          signals.push(context.signal);
          if (name === "slow") {
            await gate.promise;
            getMetadataContext();
            fallback();
          }
          return [];
        },
      },
      settings,
    );
    const slow = wrapped.listObjects("slow");
    await wrapped.listObjects("fast");
    expect(signals[0]).not.toBe(signals[1]);
    await expire(slow);
    gate.resolve();
    await vi.advanceTimersByTimeAsync(0);
    expect(signals[0].aborted).toBe(true);
    expect(signals[1].aborted).toBe(false);
    expect(fallback).not.toHaveBeenCalled();
  });
});
