import { DatabaseError } from "pg";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";

type MockClient = {
  processID?: number;
  secretKey?: number;
  query: ReturnType<typeof vi.fn>;
  release: ReturnType<typeof vi.fn>;
  getTransactionStatus: () => string;
};

const harness = vi.hoisted(() => ({
  events: [] as string[],
  cancelFailure: undefined as Error | undefined,
  options: [] as Array<{ onConnect?: (client: unknown) => Promise<void> }>,
  pools: [] as Array<{
    on: ReturnType<typeof vi.fn>;
    connect: ReturnType<typeof vi.fn>;
    query: ReturnType<typeof vi.fn>;
    end: ReturnType<typeof vi.fn>;
  }>,
}));

vi.mock("pg", async (importOriginal) => {
  const actual = await importOriginal<typeof import("pg")>();
  const { EventEmitter } = await import("node:events");
  class MockConnection extends EventEmitter {
    readonly stream = {
      end: () => queueMicrotask(() => this.emit("end")),
      destroy: () => undefined,
    };
    connect(port: number, host: string) {
      harness.events.push(`connect ${host}:${port}`);
      queueMicrotask(() => {
        if (harness.cancelFailure) this.emit("error", harness.cancelFailure);
        else this.emit("connect");
      });
    }
    cancel(processID: number, secretKey: number) {
      harness.events.push(`cancel ${processID}/${secretKey}`);
    }
  }
  function MockPool(options: (typeof harness.options)[number]) {
    harness.options.push(options);
    const pool = harness.pools.shift();
    if (!pool) throw new Error("Unexpected PostgreSQL pool creation");
    return pool;
  }
  return { ...actual, Connection: MockConnection, Pool: MockPool };
});

function createPool(client: MockClient) {
  const pool = {
    on: vi.fn(),
    connect: vi.fn(async () => client),
    query: vi.fn(async () => ({ rows: [] })),
    end: vi.fn(async (): Promise<void> => undefined),
  };
  harness.pools.push(pool);
  return pool;
}

function createClient(processID?: number, secretKey?: number): MockClient {
  return {
    processID,
    secretKey,
    query: vi.fn(async () => ({ rows: [{ name: "main" }] })),
    release: vi.fn((destroy?: boolean) => {
      harness.events.push(destroy ? "destroy" : "release");
    }),
    getTransactionStatus: () => "I",
  };
}

const drivers: PostgresDriver[] = [];
async function connectedDriver(client: MockClient) {
  const pool = createPool(client);
  const driver = new PostgresDriver({
    id: "session-safety",
    name: "PG",
    type: "pg",
    host: "db.internal",
    port: 6543,
    database: "main",
  });
  drivers.push(driver);
  await driver.connect();
  client.query.mockReset();
  client.release.mockClear();
  harness.events.length = 0;
  client.query.mockImplementation(async () => ({ rows: [], fields: [] }));
  return { driver, pool };
}

afterEach(async () => {
  await Promise.all(
    drivers
      .splice(0)
      .map((driver) => driver.disconnect().catch(() => undefined)),
  );
  harness.pools.length = 0;
  harness.options.length = 0;
  harness.events.length = 0;
  harness.cancelFailure = undefined;
});

describe("PostgreSQL backend cancellation", () => {
  it("sends a keyed CancelRequest before discarding a cancelled editor query", async () => {
    const client = createClient(4242, 99);
    const { driver } = await connectedDriver(client);
    let rejectRunning!: (error: Error) => void;
    client.query.mockReturnValueOnce(
      new Promise((_resolve, reject) => {
        rejectRunning = reject;
      }),
    );
    const running = driver.query("UPDATE big SET x = 1", undefined, {
      requestToken: 9,
    });
    const rejected = expect(running).rejects.toThrow("canceling statement");
    await vi.waitFor(() => expect(client.query).toHaveBeenCalledOnce());

    await driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 9,
    });

    expect(harness.events).toEqual([
      "connect db.internal:6543",
      "cancel 4242/99",
      "destroy",
    ]);
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    rejectRunning(new Error("canceling statement due to user request"));
    await rejected;
  });

  it("still discards transaction clients on disconnect when the CancelRequest fails", async () => {
    const client = createClient(77, 5);
    const { driver } = await connectedDriver(client);
    harness.cancelFailure = new Error("ECONNREFUSED");
    let rejectRunning!: (error: Error) => void;
    client.query.mockImplementation(async (sql: string) =>
      sql === "BEGIN"
        ? { rows: [] }
        : new Promise((_resolve, reject) => {
            rejectRunning = reject;
          }),
    );
    const transaction = driver.runTransaction([
      { sql: "UPDATE t SET a = 1", params: [] },
    ]);
    const rejected = expect(transaction).rejects.toThrow();
    await vi.waitFor(() => expect(client.query).toHaveBeenCalledTimes(2));

    await expect(driver.disconnect()).resolves.toBeUndefined();

    expect(harness.events).toEqual(["connect db.internal:6543", "destroy"]);
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    rejectRunning(new Error("socket closed"));
    await rejected;
  });

  it("discards clients without backend keys immediately", async () => {
    const client = createClient(12);
    const { driver } = await connectedDriver(client);
    client.query.mockReturnValueOnce(new Promise(() => undefined));
    void driver
      .query("SELECT pg_sleep(60)", undefined, { requestToken: 4 })
      .catch(() => undefined);
    await vi.waitFor(() => expect(client.query).toHaveBeenCalledOnce());
    await driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 4,
    });
    expect(harness.events).toEqual(["destroy"]);
  });
});

describe("PostgreSQL pooled session state", () => {
  it("resets session state after an editor query before returning the client", async () => {
    const client = createClient();
    const { driver } = await connectedDriver(client);
    await driver.query(
      "SET ROLE reporting; SET search_path = other; CREATE TEMP TABLE t(x int)",
      undefined,
      { requestToken: 1 },
    );
    expect(client.query.mock.calls.slice(1).map(([sql]) => sql)).toEqual([
      "DISCARD ALL",
      "SET extra_float_digits = 3",
    ]);
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
  });

  it("discards the client when the session reset fails", async () => {
    const client = createClient();
    const { driver } = await connectedDriver(client);
    client.query.mockImplementation(async (sql: unknown) => {
      if (sql === "DISCARD ALL") throw new Error("reset failed");
      return { rows: [] };
    });
    await driver.query("SET ROLE reporting", undefined, { requestToken: 2 });
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    client.release.mockClear();
    await driver.query("SET ROLE reporting", undefined, { requestToken: 3 });
    expect(
      client.query.mock.calls.filter(([sql]) => sql === "DISCARD ALL"),
    ).toHaveLength(2);
  });

  it("keeps retrying DISCARD ALL after transient rejections and clears the flag on reconnect", async () => {
    const client = createClient();
    const { driver, pool } = await connectedDriver(client);
    const transient = new DatabaseError("canceling statement", 0, "error");
    transient.code = "57014";
    const unsupported = new DatabaseError("not supported", 0, "error");
    unsupported.code = "0A000";
    const failures: DatabaseError[] = [transient, unsupported];
    client.query.mockImplementation(async (sql: unknown) => {
      if (sql === "DISCARD ALL") {
        const failure = failures.shift();
        if (failure) throw failure;
      }
      return {
        rows:
          typeof sql === "string" && sql.includes("current_database")
            ? [{ name: "main" }]
            : [],
        fields: [],
      };
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const discards = () =>
      client.query.mock.calls.filter(([sql]) => sql === "DISCARD ALL").length;
    try {
      await driver.query("SELECT 1", undefined, { requestToken: 1 });
      await driver.query("SELECT 1", undefined, { requestToken: 2 });
      await driver.query("SELECT 1", undefined, { requestToken: 3 });
      expect(discards()).toBe(2);
      harness.pools.push(pool);
      await driver.connect();
      await driver.query("SELECT 1", undefined, { requestToken: 4 });
      expect(discards()).toBe(3);
      expect(client.release.mock.calls.at(-1)).toEqual([]);
    } finally {
      warn.mockRestore();
    }
  });

  it("stops trying DISCARD ALL after the server rejects it", async () => {
    const client = createClient();
    const { driver } = await connectedDriver(client);
    const unsupported = new DatabaseError(
      'syntax error at or near "DISCARD"',
      0,
      "error",
    );
    unsupported.code = "42601";
    client.query.mockImplementation(async (sql: unknown) => {
      if (sql === "DISCARD ALL") throw unsupported;
      return { rows: [], fields: [] };
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      for (const requestToken of [5, 6, 7]) {
        await driver.query("SELECT 1", undefined, { requestToken });
      }
      expect(
        client.query.mock.calls.filter(([sql]) => sql === "DISCARD ALL"),
      ).toHaveLength(1);
      expect(client.release.mock.calls).toEqual([[true], [true], [true]]);
      expect(
        warn.mock.calls.filter(([message]) =>
          String(message).includes("session reset failed"),
        ),
      ).toHaveLength(1);
    } finally {
      warn.mockRestore();
    }
  });

  it("does not reset internal table queries", async () => {
    const client = createClient();
    const { driver } = await connectedDriver(client);
    await driver.query("SELECT 1", undefined, { database: "main" });
    expect(client.query).toHaveBeenCalledOnce();
    expect(client.release).toHaveBeenCalledExactlyOnceWith();
  });

  it("sets extra_float_digits on every new pooled connection before use", async () => {
    await connectedDriver(createClient());
    const fresh = createClient();
    fresh.query.mockRejectedValueOnce(new Error("unsupported"));
    await expect(
      harness.options[0]?.onConnect?.(fresh),
    ).resolves.toBeUndefined();
    expect(fresh.query).toHaveBeenCalledWith("SET extra_float_digits = 3");
  });
});

describe("PostgreSQL rolled-back statement batches", () => {
  type WireQuery = {
    handleRowDescription(message: unknown): void;
    handleDataRow(message: { fields: string[] }): void;
    handleCommandComplete(message: { text: string }, connection: object): void;
    handleError(error: Error, connection: object): void;
  };
  const field = {
    name: "id",
    tableID: 0,
    columnID: 0,
    dataTypeID: 23,
    dataTypeSize: 4,
    dataTypeModifier: -1,
    format: "text",
  };

  async function runFailingBatch(
    sql: string,
    error: Error,
    statusAfterError = "I",
    completeFirst = true,
  ) {
    const client = createClient();
    const { driver } = await connectedDriver(client);
    let status = "I";
    client.getTransactionStatus = () => status;
    client.query.mockImplementation((query: unknown) => {
      if (typeof query === "string") return Promise.resolve({ rows: [] });
      const wire = query as WireQuery;
      queueMicrotask(() => {
        wire.handleRowDescription({ fields: [field] });
        wire.handleDataRow({ fields: ["1"] });
        if (completeFirst)
          wire.handleCommandComplete({ text: "INSERT 0 1" }, {});
        status = statusAfterError;
        wire.handleError(error, {});
      });
      return query;
    });
    return driver.query(sql, undefined, { requestToken: 3, hardCap: 10 });
  }

  function serverError(message: string) {
    const error = new DatabaseError(message, 0, "error");
    error.code = "22012";
    return error;
  }

  it("marks result sets recovered from a server-rejected implicit transaction as partial", async () => {
    const result = await runFailingBatch(
      "INSERT INTO t VALUES (1) RETURNING id; SELECT 1/0",
      serverError("division by zero"),
    );
    expect(result.resultSets).toEqual([
      expect.objectContaining({ rows: [{ __col_0: 1 }], partial: true }),
    ]);
    expect(result.error).toContain("division by zero");
    expect(result.error).toContain("rolled back");
  });

  it("does not mark results partial when the script controls its own transaction", async () => {
    const result = await runFailingBatch(
      "BEGIN; INSERT INTO t VALUES (1) RETURNING id; COMMIT; SELECT 1/0",
      serverError("division by zero"),
    );
    expect(result.resultSets?.[0]).not.toHaveProperty("partial");
    expect(result.error).toBe("division by zero");
  });

  it("marks an explicit transaction left aborted by the error as rolled back", async () => {
    const result = await runFailingBatch(
      "BEGIN; INSERT INTO t VALUES (1) RETURNING id; SELECT 1/0",
      serverError("division by zero"),
      "E",
    );
    expect(result.resultSets?.[0]).toHaveProperty("partial", true);
    expect(result.error).toContain("rolled back");
  });

  it("does not claim a rollback for a single SELECT failing mid-stream", async () => {
    const result = await runFailingBatch(
      "SELECT 1/(5 - id) FROM t",
      serverError("division by zero"),
      "I",
      false,
    );
    expect(result.resultSets?.[0]).toHaveProperty("partial", true);
    expect(result.error).toBe("division by zero");
  });

  it("does not mark results partial for a local client error", async () => {
    const result = await runFailingBatch(
      "INSERT INTO t VALUES (1) RETURNING id; SELECT 2",
      new Error("connection terminated"),
    );
    expect(result.resultSets?.[0]).not.toHaveProperty("partial");
  });
});

describe("PostgreSQL routine listing", () => {
  it("does not list aggregates as functions whose DDL cannot be generated", async () => {
    const query = vi.fn(async (sql: string) => ({
      rows: sql.includes("FROM pg_proc p JOIN pg_namespace")
        ? [{ name: "fn", routine_id: "10", type: "function" }]
        : [],
    }));
    const driver = new PostgresDriver({ id: "pg", name: "PG", type: "pg" });
    Object.assign(driver, { pool: { query } });
    const objects = await driver.listObjects("", "public");
    expect(objects).toEqual([
      {
        schema: "public",
        name: "fn",
        type: "function",
        routineIdentity: "oid:10",
      },
    ]);
    const routineSql = query.mock.calls
      .map(([sql]) => sql)
      .find((sql) => sql.includes("FROM pg_proc p JOIN pg_namespace"));
    expect(routineSql).toContain("p.prokind IN ('f','p','w')");
    expect(routineSql).not.toContain("'a'");

    query.mockClear();
    await driver.getRoutineDefinition("", "public", "sum", "function");
    expect(query.mock.calls[0]?.[0]).toContain("pg_aggregate");
  });

  it("falls back to information_schema without pg_aggregate or prokind", async () => {
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("prokind") || sql.includes("pg_aggregate"))
        throw new Error("relation does not exist");
      return {
        rows: sql.includes("information_schema.routines")
          ? [{ name: "fn", type: "FUNCTION" }]
          : [],
      };
    });
    const driver = new PostgresDriver({ id: "pg", name: "PG", type: "pg" });
    Object.assign(driver, { pool: { query } });
    const objects = await driver.listObjects("", "public");
    expect(objects).toEqual([
      { schema: "public", name: "fn", type: "function" },
    ]);
  });
});
