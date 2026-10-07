import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import { ErdGraphService } from "../../src/extension/services/erdGraphService";
import {
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { TableReadService } from "../../src/extension/table/tableReadService";

const harness = vi.hoisted(() => ({
  pools: [] as Array<{
    options: Record<string, unknown>;
    query: ReturnType<typeof vi.fn>;
    connect: ReturnType<typeof vi.fn>;
    end: ReturnType<typeof vi.fn>;
    clients: Array<{
      query: ReturnType<typeof vi.fn>;
      release: ReturnType<typeof vi.fn>;
    }>;
    totalCount: number;
    idleCount: number;
    waitingCount: number;
  }>,
}));

vi.mock("pg", async (importOriginal) => {
  const actual = await importOriginal<typeof import("pg")>();
  class MockPool {
    totalCount = 0;
    idleCount = 1;
    waitingCount = 0;
    clients: Array<{
      query: ReturnType<typeof vi.fn>;
      release: ReturnType<typeof vi.fn>;
    }> = [];
    on = vi.fn();
    end = vi.fn(async () => undefined);
    constructor(readonly options: Record<string, unknown>) {
      harness.pools.push(this);
    }
    query = vi.fn(
      async (
        input: string | { text: string; values?: unknown[] },
        params: unknown[] = [],
      ) => {
        if (this.query.mock.calls.length > 100)
          throw new Error(
            `Mock query budget exceeded: ${JSON.stringify({ input, params })}`,
          );
        if (typeof input !== "string") params = input.values ?? params;
        const database = String(this.options.database) || "resolved_default";
        const sql = typeof input === "string" ? input : input.text;
        if (database === "denied") throw new Error("database access denied");
        if (sql.includes("current_database()"))
          return { rows: [{ name: database }] };
        if (sql.includes("FROM pg_attribute a")) {
          return {
            rows: [
              {
                column_name: "id",
                data_type: "integer",
                is_nullable: false,
                is_pk: true,
                pk_ordinal: 1,
              },
              {
                column_name: "amount",
                data_type: "numeric",
                is_nullable: false,
              },
              {
                column_name: `${database}_only`,
                data_type: "text",
                is_nullable: true,
              },
            ],
          };
        }
        if (sql.includes("SELECT c.relkind"))
          return { rows: [{ relkind: "r" }] };
        if (sql.includes(" AS ddl"))
          return { rows: [{ ddl: `-- ${database} DDL` }] };
        if (sql.includes("pg_get_constraintdef(con.oid, false)"))
          return {
            rows: [
              { constraint_name: "items_pkey", definition: "PRIMARY KEY (id)" },
            ],
          };
        if (sql.includes(" AS def"))
          return { rows: [{ def: `-- ${database} routine` }] };
        if (sql.includes("FROM pg_sequences"))
          return {
            rows: [
              {
                data_type: "bigint",
                start_value: 1,
                min_value: 1,
                max_value: 999,
                increment_by: 1,
                cache_size: 1,
                cycle: false,
              },
            ],
          };
        if (sql.includes("SELECT t.typtype"))
          return { rows: [{ typtype: "e" }] };
        if (sql.includes("SELECT e.enumlabel"))
          return { rows: [{ enumlabel: database }] };
        if (sql.includes("information_schema.schemata"))
          return { rows: [{ schema_name: database }] };
        if (sql.includes("information_schema.tables"))
          return { rows: [{ name: database, type: "BASE TABLE" }] };
        if (typeof input === "string" && /^INSERT .* RETURNING/.test(sql))
          return { rows: [{ __col_0: params[0] }], rowCount: 1 };
        if (typeof input === "string" && /^SELECT "id" AS "__col_0"/.test(sql))
          return {
            rows: [{ __col_0: params[0], __col_1: "22.5", __col_2: database }],
            rowCount: 1,
          };
        if (typeof input === "string" && /^SELECT .* AS "__col_0"/.test(sql))
          return { rows: [{ __col_0: "22.5" }], rowCount: 1 };
        if (typeof input === "string") return { rows: [], rowCount: 1 };
        if (/^SELECT COUNT/.test(sql))
          return { fields: [{ name: "cnt" }], rows: [[2]], rowCount: 1 };
        if (/^SELECT 1 FROM/.test(sql))
          return { fields: [], rows: [], rowCount: 0 };
        if (/^SELECT \*/.test(sql)) {
          const rows =
            params.length > 2
              ? []
              : [
                  [1, "22.5", database],
                  [2, "22.5", database],
                ];
          return {
            fields: [
              { name: "id" },
              { name: "amount" },
              { name: `${database}_only` },
            ],
            rows,
            rowCount: rows.length,
          };
        }
        if (/^SELECT/.test(sql))
          return {
            fields: [{ name: "__col_0" }],
            rows: [["22.5"]],
            rowCount: 1,
          };
        return { fields: [], rows: [], rowCount: 1 };
      },
    );
    connect = vi.fn(async () => {
      let rejectPending: ((error: Error) => void) | undefined;
      const client = {
        query: vi.fn((input: string | { text: string }, params?: unknown[]) => {
          const sql = typeof input === "string" ? input : input.text;
          if (sql.includes("slow"))
            return new Promise((_, reject) => {
              rejectPending = reject;
            });
          return this.query(input, params);
        }),
        release: vi.fn((destroy?: boolean) => {
          if (destroy) rejectPending?.(new Error("cancelled"));
        }),
      };
      this.clients.push(client);
      return client;
    });
  }
  return { ...actual, Pool: MockPool };
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}

const config = {
  id: "scope",
  name: "scope",
  type: "pg" as const,
  database: "a",
};
let driver: PostgresDriver;
const poolFor = (database: string) => {
  const pool = harness.pools.find((pool) => pool.options.database === database);
  if (!pool) throw new Error(`Missing pool ${database}`);
  return pool;
};
const managerFor = (driver: PostgresDriver) => ({
  getDriver: () => driver,
  getConnection: () => config,
  getSchemaSnapshotAsync: async () => ({
    databases: ["a", "b"].map((name) => ({
      name,
      schemas: [
        {
          name: "public",
          objects: [{ name: "items", type: "table", columns: [] }],
        },
      ],
    })),
  }),
  onDidDisconnect: () => ({ dispose() {} }),
  onDidRefreshSchemas: () => ({ dispose() {} }),
});

beforeEach(async () => {
  harness.pools.length = 0;
  driver = new PostgresDriver(config);
  await driver.connect();
});
afterEach(async () => {
  await driver.disconnect().catch(() => undefined);
  vi.useRealTimers();
});

describe("B01 PostgreSQL database scope (mock pools)", () => {
  it("releases once when cancellation rejects a pending read-only rollback", async () => {
    const pool = poolFor("a");
    const query = pool.query as (...args: unknown[]) => Promise<unknown>;
    const rollbackStarted = deferred<void>();
    let rejectRollback!: (error: Error) => void;
    const rollback = new Promise<never>((_, reject) => {
      rejectRollback = reject;
    });
    const rollbackError = new Error("rollback cancelled");
    let released = false;
    const client = {
      query: vi.fn((input: string | { text: string }, params?: unknown[]) => {
        if (input === "ROLLBACK") {
          rollbackStarted.resolve();
          return rollback;
        }
        return query(input, params);
      }),
      release: vi.fn((destroy?: boolean) => {
        if (released) throw new Error("Release called on client twice");
        released = true;
        if (destroy) rejectRollback(rollbackError);
      }),
    };
    pool.connect.mockResolvedValueOnce(client);
    const registry = driver as unknown as {
      activeQueryOperations: Set<unknown>;
      activeQueryClients: Set<unknown>;
    };
    const pending = driver.query("SELECT * FROM items", [], {
      readOnly: true,
      requestToken: 44,
    });
    const rejected = expect(pending).rejects.toMatchObject({
      name: "AggregateError",
      message: expect.stringContaining("rollback failed"),
      errors: [rollbackError],
    });
    await rollbackStarted.promise;
    expect(registry.activeQueryOperations.size).toBe(1);
    expect(registry.activeQueryClients.has(client)).toBe(true);

    await driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 44,
    });
    await rejected;

    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(registry.activeQueryOperations.size).toBe(0);
    expect(registry.activeQueryClients.size).toBe(0);
  });

  it.each([
    false,
    true,
  ])("cleans the query registry when release throws (rollback failure: %s)", async (rollbackFails) => {
    const pool = poolFor("a");
    const connect = pool.connect as () => Promise<
      (typeof pool.clients)[number]
    >;
    const query = pool.query as (...args: unknown[]) => Promise<unknown>;
    const client = await connect();
    pool.connect.mockResolvedValueOnce(client);
    if (rollbackFails) {
      client.query.mockImplementation(
        async (input: string | { text: string }) => {
          if (input === "ROLLBACK") throw new Error("rollback refused");
          return query(input);
        },
      );
    }
    const releaseError = new Error("release failed");
    client.release.mockImplementation(() => {
      throw releaseError;
    });

    await expect(
      driver.query("SELECT * FROM items", [], { readOnly: true }),
    ).rejects.toBe(releaseError);

    const registry = driver as unknown as {
      activeQueryOperations: Set<unknown>;
      activeQueryClients: Set<unknown>;
    };
    expect(registry.activeQueryOperations.size).toBe(0);
    expect(registry.activeQueryClients.size).toBe(0);
    if (rollbackFails)
      expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    else expect(client.release).toHaveBeenCalledExactlyOnceWith();
  });

  it("discards a read-only query client when rollback fails", async () => {
    const pool = poolFor("a");
    pool.query.mockImplementation(async (input) => {
      const sql = typeof input === "string" ? input : input.text;
      if (sql === "ROLLBACK") throw new Error("rollback refused");
      if (sql === "SELECT * FROM items") {
        return {
          fields: [{ name: "id" }],
          rows: [[1]],
          rowCount: 1,
        };
      }
      return { rows: [], rowCount: 1 };
    });

    await expect(
      driver.query("SELECT * FROM items", [], { readOnly: true }),
    ).rejects.toMatchObject({
      name: "AggregateError",
      message: expect.stringContaining("rollback failed"),
    });

    const client = pool.clients.at(-1);
    if (!client) throw new Error("Expected a checked-out PostgreSQL client");
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
  });

  it("discards a transaction client when rollback fails", async () => {
    const pool = poolFor("a");
    pool.query.mockImplementation(async (input) => {
      const sql = typeof input === "string" ? input : input.text;
      if (sql === "SELECT transaction_failure")
        throw new Error("transaction operation failed");
      if (sql === "ROLLBACK") throw new Error("rollback refused");
      return { rows: [], rowCount: 1 };
    });

    await expect(
      driver.runTransaction([{ sql: "SELECT transaction_failure" }]),
    ).rejects.toMatchObject({
      name: "AggregateError",
      message: expect.stringContaining("rollback failed"),
      errors: [
        expect.objectContaining({ message: "transaction operation failed" }),
        expect.objectContaining({ message: "rollback refused" }),
      ],
    });

    const client = pool.clients.at(-1);
    if (!client) throw new Error("Expected a checked-out PostgreSQL client");
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
  });

  it("routes every metadata/DDL path by database, including inherited columns/constraints and routine OIDs", async () => {
    for (const database of ["a", "b"]) {
      const before = poolFor("a").query.mock.calls.length;
      const columns = await driver.describeColumns(database, "public", "items");
      expect(columns.map((column) => column.name)).toContain(
        `${database}_only`,
      );
      expect(await driver.listSchemas(database)).toEqual([{ name: database }]);
      expect((await driver.listObjects(database, "public"))[0].name).toBe(
        database,
      );
      await Promise.all([
        driver.getIndexes(database, "public", "items"),
        driver.getForeignKeys(database, "public", "items"),
        driver.getConstraints(database, "public", "items"),
        driver.getTriggers(database, "public", "items"),
      ]);
      for (const ddl of [
        driver.getConstraintDDL(database, "public", "items", "pk"),
        driver.getIndexDDL(database, "public", "items", "idx"),
        driver.getTriggerDDL(database, "public", "items", "trigger"),
      ])
        expect(await ddl).toContain(database);
      expect(
        await driver.getCreateTableDDL(database, "public", "items"),
      ).toContain(`${database}_only`);
      expect(
        await driver.getRoutineDefinition(
          database,
          "public",
          "fn",
          "function",
          "oid:123",
        ),
      ).toContain(database);
      expect(
        await driver.getRoutineDefinition(database, "public", "fn", "function"),
      ).toContain(database);
      expect(
        await driver.getObjectDefinition(database, "public", "enum", "type"),
      ).toContain(`'${database}'`);
      expect(
        await driver.getObjectDefinition(database, "public", "seq", "sequence"),
      ).toContain("CREATE SEQUENCE");
      if (database === "b")
        expect(poolFor("a").query).toHaveBeenCalledTimes(before);
    }
    const catalogPools = harness.pools.filter(
      (pool) => pool.options.application_name === "RapiDB catalog DDL",
    );
    expect(catalogPools.map((pool) => pool.options.database)).toEqual([
      "a",
      "b",
    ]);
    for (const pool of catalogPools) {
      expect(pool.options.max).toBe(1);
      expect(pool.end).toHaveBeenCalledOnce();
    }
    expect(
      harness.pools.filter((pool) => !pool.options.application_name),
    ).toHaveLength(2);
  });

  it("scopes parallel table reads, metadata-dependent previews, all CRUD, verification, exports and ERD", async () => {
    const manager = managerFor(driver);
    const read = new TableReadService(manager as never);
    const mutation = new TableMutationService(manager as never, read);
    const pages = await Promise.all(
      ["a", "b"].map((database) =>
        read.getPage(config.id, database, "public", "items", 1, 10, []),
      ),
    );
    expect(pages.map((page) => page.rows[0])).toEqual([
      { id: 1, amount: "22.5", a_only: "a" },
      { id: 1, amount: "22.5", b_only: "b" },
    ]);
    for (const database of ["a", "b"]) {
      const before = poolFor(database === "a" ? "b" : "a").query.mock.calls
        .length;
      const insert = await mutation.prepareInsertRow(
        config.id,
        database,
        "public",
        "items",
        { id: 3, amount: "22.5", [`${database}_only`]: database },
      );
      expect(insert.previewStatements[0]).toContain(`${database}_only`);
      await mutation.executePreparedInsertPlan(insert);
      await mutation.updateRow(
        config.id,
        database,
        "public",
        "items",
        { id: 1 },
        { amount: "22.5" },
      );
      const columns = await read.getColumns(
        config.id,
        database,
        "public",
        "items",
      );
      const prepared = prepareApplyChangesPlan(
        manager as never,
        config.id,
        database,
        "public",
        "items",
        [{ primaryKeys: { id: 1 }, changes: { amount: "22.5" } }],
        columns,
      );
      expect(prepared.executable).toBe(true);
      if (!prepared.executable) throw new Error("Missing apply plan");
      expect(
        await executePreparedApplyPlan(manager as never, prepared.plan),
      ).toMatchObject({ success: true, rowOutcomes: [{ status: "applied" }] });
      expect(
        await executeAtomicSqlApplyPlan(manager as never, prepared.plan, [
          insert,
        ]),
      ).toMatchObject({ success: true, insertApplied: true });
      await mutation.deleteRows(config.id, database, "public", "items", [
        { id: 3 },
      ]);
      const chunks = [];
      for await (const chunk of read.exportAll(
        config.id,
        database,
        "public",
        "items",
        2,
      ))
        chunks.push(chunk);
      expect(chunks.flatMap((chunk) => chunk.rows)).toHaveLength(2);
      expect(poolFor(database === "a" ? "b" : "a").query).toHaveBeenCalledTimes(
        before,
      );
    }
    const erd = new ErdGraphService(manager as never);
    try {
      const graphs = await Promise.all(
        ["a", "b"].map((database) =>
          erd.getGraph({ connectionId: config.id, database, schema: "public" }),
        ),
      );
      expect(
        graphs.map(({ graph }) =>
          graph.nodes[0].columns.map((column) => column.name),
        ),
      ).toEqual([
        ["id", "amount", "a_only"],
        ["id", "amount", "b_only"],
      ]);
    } finally {
      erd.dispose();
    }
    expect(harness.pools).toHaveLength(2);
  });

  it("rejects an atomic plan spanning databases before executing any SQL", async () => {
    const manager = managerFor(driver);
    const read = new TableReadService(manager as never);
    const mutation = new TableMutationService(manager as never, read);
    const inserts = await Promise.all(
      ["a", "b"].map((database) =>
        mutation.prepareInsertRow(config.id, database, "public", "items", {
          id: 3,
        }),
      ),
    );
    const transaction = vi.spyOn(driver, "runTransaction");
    expect(
      await executeAtomicSqlApplyPlan(manager as never, null, inserts),
    ).toMatchObject({ success: false });
    expect(transaction).not.toHaveBeenCalled();
  });

  it("scopes the offset export fallback for tables without a primary key", async () => {
    const describeColumns = driver.describeColumns.bind(driver);
    vi.spyOn(driver, "describeColumns").mockImplementation(async (...args) =>
      (await describeColumns(...args)).map((column) => ({
        ...column,
        isPrimaryKey: false,
      })),
    );
    const read = new TableReadService(managerFor(driver) as never);
    const before = poolFor("a").query.mock.calls.length;
    const rows = [];
    for await (const chunk of read.exportAll(
      config.id,
      "b",
      "public",
      "items",
      10,
    ))
      rows.push(...chunk.rows);
    expect(rows.map((row) => row.b_only)).toEqual(["b", "b"]);
    expect(poolFor("a").query).toHaveBeenCalledTimes(before);
  });

  it("keeps readOnly and targeted cancellation isolated from a concurrent query-editor request", async () => {
    const editor = driver.query("SELECT slow", [], { requestToken: 22 });
    const editorRejected = expect(editor).rejects.toThrow("cancelled");
    const pending = driver.query("SELECT slow", [], {
      database: "b",
      requestToken: 11,
      readOnly: true,
    });
    const rejected = expect(pending).rejects.toThrow("cancelled");
    await vi.waitFor(() =>
      expect(poolFor("b").query).toHaveBeenCalledWith(
        "BEGIN READ ONLY",
        undefined,
      ),
    );
    await driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 11,
    });
    await rejected;
    expect(poolFor("b").clients[0].release).toHaveBeenCalledExactlyOnceWith(
      true,
    );
    expect(poolFor("a").clients[1].release).not.toHaveBeenCalled();
    await driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 22,
    });
    await editorRejected;
    const result = await driver.query("SELECT * FROM items");
    expect(result.rows[0].__col_2).toBe("a");
    await driver.query("SELECT * FROM items", [], {
      database: "b",
      readOnly: true,
    });
    expect(poolFor("b").query).toHaveBeenCalledWith("ROLLBACK", undefined);
  });

  it("shares each database pool across simultaneous callers without changing the default", async () => {
    const results = await Promise.all(
      ["b", "b", "a", "b"].map((database) =>
        driver.query("SELECT * FROM items", [], { database }),
      ),
    );
    expect(results.map((result) => result.rows[0].__col_2)).toEqual([
      "b",
      "b",
      "a",
      "b",
    ]);
    expect(harness.pools).toHaveLength(2);
    expect((await driver.query("SELECT * FROM items")).rows[0].__col_2).toBe(
      "a",
    );
  });

  it("reuses the resolved default database when the connection config omits its name", async () => {
    await driver.disconnect();
    driver = new PostgresDriver({ ...config, database: undefined });
    await driver.connect();
    const count = harness.pools.length;
    expect(
      (await driver.describeColumns("resolved_default", "public", "items")).map(
        (column) => column.name,
      ),
    ).toContain("resolved_default_only");
    expect(
      (
        await driver.query("SELECT * FROM items", [], {
          database: "resolved_default",
        })
      ).rows[0].__col_2,
    ).toBe("resolved_default");
    expect((await driver.query("SELECT * FROM items")).rows[0].__col_2).toBe(
      "resolved_default",
    );
    expect(harness.pools).toHaveLength(count);
  });

  it("waits on the selected pool and can cancel a queued query without blocking another scope", async () => {
    await driver.listSchemas("b");
    Object.assign(poolFor("b"), { totalCount: 5, idleCount: 0 });
    vi.useFakeTimers();
    const queued = driver.query("SELECT * FROM items", [], {
      database: "b",
      requestToken: 42,
    });
    const rejected = expect(queued).rejects.toThrow(
      "cancelled before execution",
    );
    expect((await driver.query("SELECT * FROM items")).rows[0].__col_2).toBe(
      "a",
    );
    await driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 42,
    });
    await rejected;
    expect(poolFor("b").connect).not.toHaveBeenCalled();
  });

  it("starts checkout in the same turn as the pool-capacity check", async () => {
    const pool = poolFor("a");
    const before = pool.connect.mock.calls.length;
    const pending = driver.query("SELECT * FROM items");

    // A peer pool consumer invoked before this microtask must not be able to
    // take the checked slot and strand this query in pg-pool's wait queue.
    expect(pool.connect).toHaveBeenCalledTimes(before + 1);
    expect((await pending).rows[0].__col_2).toBe("a");
  });

  it("cancels a saturated timed-out query wait and reacquires without stale SQL", async () => {
    const pool = poolFor("a");
    const connectCalls = pool.connect.mock.calls.length;
    Object.assign(pool, { totalCount: 5, idleCount: 0, waitingCount: 0 });
    vi.useFakeTimers();
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutMs: 10,
    }));

    const pending = wrapped.query("SELECT timed_out_before_checkout");
    const rejected = expect(pending).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(10);
    await rejected;

    expect(pool.connect).toHaveBeenCalledTimes(connectCalls);
    expect(pool.waitingCount).toBe(0);

    pool.idleCount = 1;
    const reacquired = await driver.query("SELECT * FROM items");
    expect(reacquired.rows[0].__col_2).toBe("a");
    expect(pool.connect).toHaveBeenCalledTimes(connectCalls + 1);
    expect(
      pool.clients.flatMap((client) =>
        client.query.mock.calls.map(([input]) =>
          typeof input === "string" ? input : input.text,
        ),
      ),
    ).not.toContain("SELECT timed_out_before_checkout");
  });

  it.each([
    "abort",
    "timeout",
  ] as const)("cancels a saturated transaction %s without leaving a checkout or late SQL", async (cancellation) => {
    const pool = poolFor("a");
    const originalConnect = pool.connect as unknown as () => Promise<
      (typeof pool.clients)[number]
    >;
    const pendingCheckout = deferred<(typeof pool.clients)[number]>();
    Object.assign(pool, { totalCount: 5, idleCount: 0, waitingCount: 0 });
    pool.connect = vi.fn(() => {
      if (pool.totalCount >= 5 && pool.idleCount === 0) {
        pool.waitingCount += 1;
        return pendingCheckout.promise;
      }
      return originalConnect();
    });
    const connectCalls = pool.connect.mock.calls.length;

    let rejected: Promise<void>;
    if (cancellation === "timeout") {
      vi.useFakeTimers();
      const wrapped = createTimeoutAwareDriver(driver, () => ({
        connectionTimeoutSeconds: 1,
        dbOperationTimeoutSeconds: 1,
        connectionTimeoutMs: 1000,
        dbOperationTimeoutMs: 10,
      }));
      const pending = wrapped.runTransaction([
        { sql: "SELECT timed_out_transaction" },
      ]);
      rejected = expect(pending).rejects.toThrow("timed out");
      await vi.advanceTimersByTimeAsync(10);
    } else {
      const controller = new AbortController();
      const pending = driver.runTransaction(
        [{ sql: "SELECT aborted_transaction" }],
        { signal: controller.signal, deadline: Infinity },
      );
      rejected = expect(pending).rejects.toThrow(/abort|cancel/i);
      controller.abort();
    }
    await rejected;

    expect(pool.connect).toHaveBeenCalledTimes(connectCalls);
    expect(pool.waitingCount).toBe(0);
    const priorSql = pool.clients.flatMap((client) =>
      client.query.mock.calls.map(([input]) =>
        typeof input === "string" ? input : input.text,
      ),
    );
    expect(priorSql).not.toContain("SELECT timed_out_transaction");
    expect(priorSql).not.toContain("SELECT aborted_transaction");
    expect(priorSql).not.toContain("ROLLBACK");

    pool.idleCount = 1;
    await driver.runTransaction([{ sql: "SELECT reacquired_transaction" }]);
    expect(pool.waitingCount).toBe(0);
    const acquiredSql = pool.clients.flatMap((client) =>
      client.query.mock.calls.map(([input]) =>
        typeof input === "string" ? input : input.text,
      ),
    );
    expect(acquiredSql).toContain("SELECT reacquired_transaction");
    expect(pool.connect).toHaveBeenCalledTimes(connectCalls + 1);
  });

  it("preserves query and transaction timeout fences in a scoped pool", async () => {
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutMs: 10,
    }));
    vi.useFakeTimers();
    const query = wrapped.query("SELECT slow", [], { database: "b" });
    const queryRejected = expect(query).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(15);
    await queryRejected;
    const transaction = wrapped.runTransaction(
      [{ sql: "slow" }, { sql: "never" }],
      undefined,
      { database: "b" },
    );
    const transactionRejected =
      expect(transaction).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(15);
    await transactionRejected;
    const client = poolFor("b").clients[1];
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(client.query).not.toHaveBeenCalledWith("never", []);
    expect(client.query).not.toHaveBeenCalledWith("COMMIT");
    expect((await wrapped.query("SELECT * FROM items")).rows[0].__col_2).toBe(
      "a",
    );
  });

  it("aborts only the transaction whose context was cancelled across parallel scopes", async () => {
    const firstAbort = new AbortController();
    const secondAbort = new AbortController();
    const first = driver.runTransaction(
      [{ sql: "slow" }, { sql: "never" }],
      { signal: firstAbort.signal, deadline: Infinity },
      { database: "a" },
    );
    const firstRejected = expect(first).rejects.toThrow("cancelled");
    const second = driver.runTransaction(
      [{ sql: "slow" }],
      { signal: secondAbort.signal, deadline: Infinity },
      { database: "b" },
    );
    const secondRejected = expect(second).rejects.toThrow("cancelled");
    await vi.waitFor(() => {
      expect(poolFor("a").clients[1].query).toHaveBeenCalledWith("slow", []);
      expect(poolFor("b").clients[0].query).toHaveBeenCalledWith("slow", []);
    });
    firstAbort.abort();
    await firstRejected;
    expect(poolFor("b").clients[0].release).not.toHaveBeenCalled();
    expect(poolFor("a").clients[1].query).not.toHaveBeenCalledWith("COMMIT");
    secondAbort.abort();
    await secondRejected;
  });

  it("uses native timeout settings in scoped pools and cleans every pool on reconnect/disconnect, including failed scopes", async () => {
    await expect(
      driver.describeColumns("denied", "public", "items"),
    ).rejects.toThrow("access denied");
    await driver.query("SELECT * FROM items", [], { database: "b" });
    expect(poolFor("b").options).toMatchObject({
      query_timeout: 180000,
      statement_timeout: 180000,
      connectionTimeoutMillis: 15000,
    });
    const oldPools = [...harness.pools];
    await driver.connect();
    for (const pool of oldPools) expect(pool.end).toHaveBeenCalledOnce();
    await driver.query("SELECT * FROM items", [], { database: "b" });
    harness.pools.at(-1)?.end.mockRejectedValueOnce(new Error("end failed"));
    await expect(driver.disconnect()).rejects.toThrow("end failed");
    for (const pool of harness.pools) expect(pool.end).toHaveBeenCalledOnce();
    await expect(
      driver.query("SELECT * FROM items", [], { database: "b" }),
    ).rejects.toThrow("not open");
    expect(driver.isConnected()).toBe(false);
  });
});
