import type { Query } from "mysql2";
import { afterEach, describe, expect, it, vi } from "vitest";

const mysqlMocks = vi.hoisted(() => ({
  createConnection: vi.fn(),
  createPool: vi.fn(),
}));
vi.mock("mysql2/promise", async (importOriginal) => ({
  ...(await importOriginal<typeof import("mysql2/promise")>()),
  createConnection: mysqlMocks.createConnection,
  createPool: mysqlMocks.createPool,
}));

import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";

afterEach(() => {
  vi.useRealTimers();
  mysqlMocks.createConnection.mockReset();
  mysqlMocks.createPool.mockReset();
});

function harness() {
  const events: string[] = [];
  let rejectPending: ((error: Error) => void) | undefined;
  const connection = {
    threadId: 42,
    connection: {
      config: {},
      query: vi.fn((command: Query) => command),
    },
    query: vi.fn(
      async (_options: unknown): Promise<unknown> =>
        new Promise((_resolve, reject) => {
          rejectPending = reject;
        }),
    ),
    destroy: vi.fn(() => {
      events.push("destroy");
      rejectPending?.(new Error("Connection destroyed"));
    }),
    release: vi.fn(),
  };
  const driver = new MySQLDriver(
    { id: "session", name: "MySQL", type: "mysql", host: "localhost" },
    () => ({
      connectionTimeoutMs: 1000,
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutMs: 60000,
      dbOperationTimeoutSeconds: 60,
    }),
  );
  const tracked = driver as unknown as {
    pool: unknown;
    killQueryConnectionOptions: unknown;
    killTargets: WeakMap<object, unknown>;
    activeQueryOperations: Set<unknown>;
    activeQueryConnectionSlots: number;
  };
  tracked.pool = { getConnection: vi.fn(async () => connection) };
  tracked.killQueryConnectionOptions = { host: "localhost", user: "app" };
  tracked.killTargets.set(connection.connection, {
    threadId: 42,
    identitySql: "SELECT IDENTITY",
    identity: "server-1",
  });
  return { driver, connection, events, tracked };
}

function killer(events: string[], identity = "server-1") {
  return {
    query: vi.fn(async (options: { sql: string }) => {
      if (options.sql === "SELECT IDENTITY") {
        events.push("identity");
        return [[{ connection_id: "7", server_identity: identity }], []];
      }
      events.push(options.sql);
      return [{}, []];
    }),
    destroy: vi.fn(() => events.push("killer destroy")),
  };
}

async function cancelSleepingQuery(
  h: ReturnType<typeof harness>,
  requestToken: number,
) {
  const outcome = h.driver
    .query("SELECT SLEEP(100)", undefined, { hardCap: 10, requestToken })
    .catch((error: unknown) => error);
  await vi.waitFor(() =>
    expect(h.connection.connection.query).toHaveBeenCalled(),
  );
  await h.driver.cancelCurrentOperation({
    operationName: "query",
    requestToken,
    reason: "manual",
  });
  return outcome;
}

describe("MySQL server-side cancellation", () => {
  it("verifies the server and sends KILL QUERY before discarding a cancelled query", async () => {
    const h = harness();
    mysqlMocks.createConnection.mockResolvedValue(killer(h.events));

    expect(String(await cancelSleepingQuery(h, 7))).toMatch(/cancelled/);
    await vi.waitFor(() => expect(h.connection.destroy).toHaveBeenCalled());
    expect(mysqlMocks.createConnection).toHaveBeenCalledWith(
      expect.objectContaining({ host: "localhost", user: "app" }),
    );
    expect(h.events).toEqual([
      "identity",
      "KILL QUERY 42",
      "killer destroy",
      "destroy",
    ]);
    expect(h.tracked.activeQueryConnectionSlots).toBe(0);
  });

  it("does not send KILL QUERY when the cancellation connection reaches another server", async () => {
    const h = harness();
    mysqlMocks.createConnection.mockResolvedValue(killer(h.events, "server-2"));

    await cancelSleepingQuery(h, 9);
    await vi.waitFor(() => expect(h.connection.destroy).toHaveBeenCalled());
    expect(h.events).toEqual(["identity", "killer destroy", "destroy"]);
  });

  it("skips KILL QUERY when the session thread id was not verified", async () => {
    const h = harness();
    h.tracked.killTargets.delete(h.connection.connection);

    await cancelSleepingQuery(h, 10);
    expect(h.connection.destroy).toHaveBeenCalledOnce();
    expect(mysqlMocks.createConnection).not.toHaveBeenCalled();
  });

  it("bounds an unreachable KILL QUERY connection and still discards the query socket", async () => {
    vi.useFakeTimers();
    const h = harness();
    mysqlMocks.createConnection.mockReturnValue(new Promise(() => {}));

    const outcome = h.driver
      .query("SELECT SLEEP(100)", undefined, { hardCap: 10, requestToken: 8 })
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    await h.driver.cancelCurrentOperation({
      operationName: "query",
      requestToken: 8,
      reason: "manual",
    });
    expect(String(await outcome)).toMatch(/cancelled/);
    expect(h.connection.destroy).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(5000);
    expect(h.connection.destroy).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("cancels a parameterized query when its abort signal fires", async () => {
    const h = harness();
    mysqlMocks.createConnection.mockResolvedValue(killer(h.events));
    const abort = new AbortController();

    const outcome = h.driver
      .query("SELECT * FROM t WHERE id = ?", [1], { signal: abort.signal })
      .catch((error: unknown) => error);
    await vi.waitFor(() => expect(h.connection.query).toHaveBeenCalled());
    abort.abort();

    expect(String(await outcome)).toMatch(/destroyed/);
    expect(h.events).toEqual([
      "identity",
      "KILL QUERY 42",
      "killer destroy",
      "destroy",
    ]);
    expect(h.tracked.activeQueryOperations.size).toBe(0);
  });

  it("stops an unbounded script before the next statement after cancellation", async () => {
    const h = harness();
    h.tracked.killTargets.delete(h.connection.connection);
    const sent: string[] = [];
    h.connection.query.mockImplementation(async (options: unknown) => {
      const sql = (options as { sql: string }).sql;
      sent.push(sql);
      if (sql === "SELECT 1") {
        await h.driver.cancelCurrentOperation({
          operationName: "query",
          requestToken: 11,
          reason: "manual",
        });
      }
      return [[[1]], [{ name: "a", columnType: 3 }]];
    });

    const outcome = await h.driver
      .query("SELECT 1; DELETE FROM t", undefined, { requestToken: 11 })
      .catch((error: unknown) => error);

    expect(String(outcome)).toMatch(/SQL query cancelled/);
    expect(sent).toEqual(["SELECT 1"]);
  });

  it("forgets the cancellation endpoint on disconnect", async () => {
    const h = harness();
    h.tracked.pool = { end: vi.fn(async () => undefined) };
    await h.driver.disconnect();
    expect(h.tracked.killQueryConnectionOptions).toBeUndefined();
  });
});

const pageColumn = (isPrimaryKey: boolean) => ({
  name: "id",
  type: "int",
  nativeType: "int",
  category: "integer",
  nullable: false,
  isPrimaryKey,
  primaryKeyOrdinal: isPrimaryKey ? 1 : undefined,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
});

const pageRequest = {
  database: "db",
  schema: "",
  table: "t",
  page: 1,
  pageSize: 10,
  filters: [],
  sort: null,
  skipCount: true,
};

describe("MySQL table page cancellation", () => {
  it("forwards the request signal to COUNT and page queries", async () => {
    const h = harness();
    const abort = new AbortController();
    vi.spyOn(h.driver, "describeColumns").mockResolvedValue([
      pageColumn(true),
    ] as never);
    const signals: Array<AbortSignal | undefined> = [];
    vi.spyOn(h.driver, "query").mockImplementation(
      async (_sql, _params, options) => {
        signals.push(options?.signal);
        abort.abort(new Error("page cancelled"));
        return { columns: [], rows: [{ __col_0: 1 }], rowCount: 1 } as never;
      },
    );

    await expect(
      h.driver.readTablePage({
        ...pageRequest,
        skipCount: false,
        signal: abort.signal,
        deadline: Date.now() + 60000,
      }),
    ).rejects.toThrow("page cancelled");
    expect(signals).toHaveLength(1);
    expect(signals[0]?.aborted).toBe(true);
  });

  it("reports a timeout when the page deadline expires", async () => {
    const h = harness();
    vi.spyOn(h.driver, "describeColumns").mockResolvedValue([
      pageColumn(true),
    ] as never);
    vi.spyOn(h.driver, "query").mockImplementation(
      (_sql, _params, options) =>
        new Promise((_resolve, reject) => {
          options?.signal?.addEventListener("abort", () =>
            reject(new Error("SQL query cancelled.")),
          );
        }),
    );

    await expect(
      h.driver.readTablePage({ ...pageRequest, deadline: Date.now() + 20 }),
    ).rejects.toMatchObject({
      name: "DriverTimeoutError",
      message: expect.stringMatching(/timed out/),
    });
  });

  it("caches unique-key lookups per table and falls back when they fail", async () => {
    const h = harness();
    vi.spyOn(h.driver, "describeColumns").mockResolvedValue([
      pageColumn(false),
    ] as never);
    const sqls: string[] = [];
    vi.spyOn(h.driver, "query").mockImplementation(async (sql) => {
      sqls.push(sql);
      return { columns: [], rows: [], rowCount: 0 } as never;
    });
    const getIndexes = vi
      .spyOn(h.driver, "getIndexes")
      .mockRejectedValueOnce(new Error("no privilege"))
      .mockResolvedValue([
        { name: "uq_id", columns: ["id"], unique: true, primary: false },
      ]);

    await h.driver.readTablePage(pageRequest);
    await h.driver.readTablePage(pageRequest);
    await h.driver.readTablePage(pageRequest);
    expect(getIndexes).toHaveBeenCalledTimes(2);
    expect(sqls).toHaveLength(3);
    expect(sqls.every((sql) => sql.includes("ORDER BY"))).toBe(true);

    h.driver.clearTableReadCache();
    await h.driver.readTablePage(pageRequest);
    expect(getIndexes).toHaveBeenCalledTimes(3);
  });
});

describe("MySQL session initialization", () => {
  const mysqlIdentitySql =
    "SELECT CONNECTION_ID() AS connection_id, @@autocommit AS autocommit, CONCAT_WS(':', @@server_uuid, @@hostname, @@port) AS server_identity";

  function nativeConnection(statusFlags?: number) {
    return {
      threadId: 42,
      _handshakePacket: { serverVersion: "8.0.36", statusFlags },
      once: vi.fn(),
      query: vi.fn(),
      destroy: vi.fn(),
      _handleFatalError: vi.fn(),
    };
  }

  async function connectedPool(
    probeStatusFlags = 2,
    probeIdentity: Record<string, unknown> = {
      connection_id: "42",
      autocommit: 1,
      server_identity: "uuid-1:db-1:3306",
    },
  ) {
    let onConnection: ((connection: unknown) => void) | undefined;
    const probeNative = nativeConnection(probeStatusFlags);
    const probe = {
      connection: probeNative,
      release: vi.fn(),
      destroy: vi.fn(),
    };
    mysqlMocks.createPool.mockReturnValue({
      on: (event: string, listener: (connection: unknown) => void) => {
        if (event === "connection") onConnection = listener;
      },
      getConnection: vi.fn(async () => {
        onConnection?.(probeNative);
        const identity = probeNative.query.mock.calls.find(
          (call) => call[0] === mysqlIdentitySql,
        )?.[1] as (error: Error | null, rows?: unknown) => void;
        identity(null, [probeIdentity]);
        return probe;
      }),
      end: vi.fn(async () => undefined),
    });
    const driver = new MySQLDriver({
      id: "session-init",
      name: "MySQL",
      type: "mysql",
      host: "localhost",
    });
    await driver.connect();
    const open = (statusFlags?: number) => {
      const native = nativeConnection(statusFlags);
      onConnection?.(native);
      const callbacks = native.query.mock.calls.map(
        (call) => call[1] as (error: Error | null, rows?: unknown) => void,
      );
      return { native, callbacks };
    };
    const killTargets = (
      driver as unknown as { killTargets: WeakMap<object, unknown> }
    ).killTargets;
    return { driver, open, probe, probeNative, killTargets };
  }

  it("needs a single identity round trip when the handshake reports autocommit", async () => {
    const { driver, probe, probeNative, killTargets } = await connectedPool();
    expect(probeNative.query.mock.calls.map((call) => call[0])).toEqual([
      mysqlIdentitySql,
    ]);
    expect(probe.release).toHaveBeenCalledOnce();
    expect(killTargets.get(probeNative)).toEqual({
      threadId: 42,
      identitySql: mysqlIdentitySql,
      identity: "uuid-1:db-1:3306",
    });
    await driver.disconnect();
  });

  it("sets autocommit before any user query when the handshake does not report it", async () => {
    const { driver, open } = await connectedPool();
    const { native, callbacks } = open(0);
    expect(native.query.mock.calls.map((call) => call[0])).toEqual([
      "SET SESSION autocommit = 1",
      mysqlIdentitySql,
    ]);
    callbacks[0](null);
    expect(native.destroy).not.toHaveBeenCalled();
    await driver.disconnect();
  });

  it("forces autocommit on later connections when init_connect disabled it", async () => {
    const { driver, open, probe } = await connectedPool(2, {
      connection_id: "42",
      autocommit: 0,
      server_identity: "uuid-1:db-1:3306",
    });
    expect(probe.destroy).toHaveBeenCalledOnce();
    expect(probe.release).not.toHaveBeenCalled();
    const { native } = open(2);
    expect(native.query.mock.calls[0][0]).toBe("SET SESSION autocommit = 1");
    await driver.disconnect();
  });

  it("bounds the probe identity wait and discards an unanswered probe", async () => {
    vi.useFakeTimers();
    let onConnection: ((connection: unknown) => void) | undefined;
    const probeNative = nativeConnection(2);
    const probe = {
      connection: probeNative,
      release: vi.fn(),
      destroy: vi.fn(),
    };
    mysqlMocks.createPool.mockReturnValue({
      on: (event: string, listener: (connection: unknown) => void) => {
        if (event === "connection") onConnection = listener;
      },
      getConnection: vi.fn(async () => {
        onConnection?.(probeNative);
        return probe;
      }),
      end: vi.fn(async () => undefined),
    });
    const driver = new MySQLDriver({
      id: "session-init",
      name: "MySQL",
      type: "mysql",
      host: "localhost",
    });
    let connected = false;
    const connecting = driver.connect().then(() => {
      connected = true;
    });
    await vi.advanceTimersByTimeAsync(1999);
    expect(connected).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await connecting;
    expect(probe.destroy).toHaveBeenCalledOnce();
    expect(probe.release).not.toHaveBeenCalled();
    expect(
      (
        driver as unknown as { killTargets: WeakMap<object, unknown> }
      ).killTargets.has(probeNative),
    ).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
    await driver.disconnect();
  });

  it("disables KILL QUERY when CONNECTION_ID differs from the handshake thread id", async () => {
    const { driver, open, killTargets } = await connectedPool();
    const { native, callbacks } = open(2);
    callbacks[0](null, [
      { connection_id: "77", autocommit: 1, server_identity: "uuid-1" },
    ]);
    expect(killTargets.has(native)).toBe(false);
    await driver.disconnect();
  });

  it("surfaces a session initialization failure to queued commands", async () => {
    const { driver, open } = await connectedPool();
    const { native, callbacks } = open(0);
    callbacks[0](new Error("access denied"));
    expect(native._handleFatalError).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringMatching(
          /session initialization \(SET SESSION autocommit = 1\) failed: access denied/,
        ),
      }),
    );
    expect(native.destroy).toHaveBeenCalledOnce();
    await driver.disconnect();
  });

  it("reports unique indexes when NON_UNIQUE is returned as a string", async () => {
    const h = harness();
    (h.driver as unknown as { pool: unknown }).pool = {
      query: vi.fn(async () => [
        [
          { INDEX_NAME: "uq_email", COLUMN_NAME: "email", NON_UNIQUE: "0" },
          { INDEX_NAME: "ix_name", COLUMN_NAME: "name", NON_UNIQUE: "1" },
        ],
        [],
      ]),
    };

    const indexes = await h.driver.getIndexes("db", "", "users");

    expect(indexes.map(({ name, unique }) => ({ name, unique }))).toEqual([
      { name: "uq_email", unique: true },
      { name: "ix_name", unique: false },
    ]);
  });
});
