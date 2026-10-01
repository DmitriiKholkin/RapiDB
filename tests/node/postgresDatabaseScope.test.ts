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
    expect(harness.pools).toHaveLength(2);
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
    await vi.advanceTimersByTimeAsync(25);
    await rejected;
    expect(poolFor("b").connect).not.toHaveBeenCalled();
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
