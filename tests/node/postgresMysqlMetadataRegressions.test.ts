import { afterEach, describe, expect, it, vi } from "vitest";
import { runWithMetadataCancellation } from "../../src/extension/dbDrivers/metadataCancellation";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

type MockClient = {
  processID?: number;
  secretKey?: number;
  query: ReturnType<typeof vi.fn>;
  release: ReturnType<typeof vi.fn>;
  getTransactionStatus: () => string;
};

const harness = vi.hoisted(() => ({
  events: [] as string[],
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
      queueMicrotask(() => this.emit("connect"));
    }
    cancel(processID: number, secretKey: number) {
      harness.events.push(`cancel ${processID}/${secretKey}`);
    }
  }
  function MockPool() {
    const pool = harness.pools.shift();
    if (!pool) throw new Error("Unexpected PostgreSQL pool creation");
    return pool;
  }
  return { ...actual, Connection: MockConnection, Pool: MockPool };
});

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
  const pool = {
    on: vi.fn(),
    connect: vi.fn(async () => client),
    query: vi.fn(
      async (_input: unknown): Promise<{ rows: unknown[] }> => ({ rows: [] }),
    ),
    end: vi.fn(async (): Promise<void> => undefined),
  };
  harness.pools.push(pool);
  const driver = new PostgresDriver({
    id: "g7",
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

function sqlOf(input: unknown): string {
  return typeof input === "string" ? input : (input as { text: string }).text;
}

afterEach(async () => {
  await Promise.all(
    drivers
      .splice(0)
      .map((driver) => driver.disconnect().catch(() => undefined)),
  );
  harness.pools.length = 0;
  harness.events.length = 0;
});

describe("PostgreSQL metadata fixes", () => {
  it("hides every temporary schema", async () => {
    const { driver, pool } = await connectedDriver(createClient());
    await driver.listSchemas("main");
    const sql = sqlOf(pool.query.mock.calls[0]?.[0]);
    expect(sql).toContain("!~ '^pg_(toast_)?temp_[0-9]+$'");
    expect(sql).not.toContain("pg_temp_1");
  });

  it("reads only key columns, flags partial indexes and skips them as unique constraints", async () => {
    const { driver, pool } = await connectedDriver(createClient());
    pool.query.mockImplementation(async (input: unknown) => {
      const sql = sqlOf(input);
      if (sql.includes("pg_get_indexdef(i.oid")) {
        return {
          rows: [
            {
              name: "u_inc",
              unique: true,
              primary: false,
              partial: false,
              column: "a",
            },
            {
              name: "u_part",
              unique: true,
              primary: false,
              partial: true,
              column: "b",
            },
          ],
        };
      }
      return { rows: [] };
    });

    const indexes = await driver.getIndexes("main", "public", "t");
    const indexSql = sqlOf(
      pool.query.mock.calls.find(([input]) =>
        sqlOf(input).includes("pg_get_indexdef(i.oid"),
      )?.[0],
    );
    expect(indexSql).toContain("indnkeyatts");
    expect(indexSql).toContain("indpred IS NOT NULL AS partial");
    expect(indexes).toEqual([
      { name: "u_inc", columns: ["a"], unique: true, primary: false },
      {
        name: "u_part",
        columns: ["b"],
        unique: true,
        primary: false,
        partial: true,
      },
    ]);

    const constraints = await driver.getConstraints("main", "public", "t");
    expect(constraints.map((constraint) => constraint.name)).toEqual(["u_inc"]);
  });

  it("does not reference pg_index.indnkeyatts directly so PostgreSQL < 11 still works", async () => {
    const { driver, pool } = await connectedDriver(createClient());
    pool.query.mockImplementation(async (input: unknown) => {
      const sql = sqlOf(input);
      if (/\bix\.indnkeyatts\b/.test(sql)) {
        throw new Error("column ix.indnkeyatts does not exist");
      }
      if (sql.includes("pg_get_indexdef(i.oid")) {
        expect(sql).toMatch(
          /key_ordinal <= COALESCE\(\(to_jsonb\(ix\)->>'indnkeyatts'\)::int, ix\.indnatts\)/,
        );
        return {
          rows: [{ name: "idx", unique: false, primary: false, column: "a" }],
        };
      }
      return { rows: [] };
    });

    await expect(driver.getIndexes("main", "public", "t")).resolves.toEqual([
      { name: "idx", columns: ["a"], unique: false, primary: false },
    ]);
  });

  it("returns index DDL for unique constraints derived from CREATE UNIQUE INDEX", async () => {
    const { driver, pool } = await connectedDriver(createClient());
    pool.query.mockImplementation(async (input: unknown) => {
      const sql = sqlOf(input);
      if (sql.includes("FROM pg_constraint")) return { rows: [] };
      if (sql.includes("pg_idx.indisunique")) {
        return {
          rows: [
            {
              ddl: "CREATE UNIQUE INDEX u_email ON public.t USING btree (email);",
            },
          ],
        };
      }
      return { rows: [] };
    });

    await expect(
      driver.getConstraintDDL("main", "public", "t", "u_email"),
    ).resolves.toBe(
      "CREATE UNIQUE INDEX u_email ON public.t USING btree (email);",
    );
    pool.query.mockImplementation(async () => ({ rows: [] }));
    await expect(
      driver.getConstraintDDL("main", "public", "t", "missing"),
    ).rejects.toThrow('Constraint "missing" not found');
  });

  it("sends a CancelRequest before discarding an aborted metadata query", async () => {
    const client = createClient(77, 5);
    const { driver } = await connectedDriver(client);
    let rejectRunning!: (error: Error) => void;
    client.query.mockReturnValueOnce(
      new Promise((_resolve, reject) => {
        rejectRunning = reject;
      }),
    );
    const controller = new AbortController();
    const running = runWithMetadataCancellation(
      controller.signal,
      Number.POSITIVE_INFINITY,
      () => driver.listSchemas("main"),
    );
    const rejected = expect(running).rejects.toBeDefined();
    await vi.waitFor(() => expect(client.query).toHaveBeenCalledOnce());

    controller.abort(new Error("metadata cancelled"));
    await vi.waitFor(() =>
      expect(harness.events).toEqual([
        "connect db.internal:6543",
        "cancel 77/5",
        "destroy",
      ]),
    );
    rejectRunning(new Error("canceling statement due to user request"));
    await rejected;
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
  });
});

describe("PostgreSQL catalog session cancellation", () => {
  it("sends a CancelRequest, then destroys the client, then closes the catalog pool", async () => {
    const { driver } = await connectedDriver(createClient());
    const rejectors: Array<(error: Error) => void> = [];
    const catalogClient = createClient(31, 41);
    catalogClient.query.mockImplementation(
      () =>
        new Promise((_resolve, reject) => {
          rejectors.push(reject);
        }),
    );
    catalogClient.release.mockImplementation((destroy?: boolean) => {
      harness.events.push(destroy ? "destroy" : "release");
      for (const reject of rejectors) reject(new Error("client destroyed"));
    });
    const catalogPool = {
      on: vi.fn(),
      connect: vi.fn(async () => catalogClient),
      query: vi.fn(async (_input: unknown) => ({ rows: [] })),
      end: vi.fn(async (): Promise<void> => {
        harness.events.push("end");
      }),
    };
    harness.pools.push(catalogPool);
    const controller = new AbortController();
    const running = runWithMetadataCancellation(
      controller.signal,
      Number.POSITIVE_INFINITY,
      () => driver.getCreateTableDDL("main", "public", "t"),
    );
    const rejected = expect(running).rejects.toBeDefined();
    await vi.waitFor(() => expect(catalogClient.query).toHaveBeenCalled());

    controller.abort(new Error("metadata cancelled"));
    await rejected;
    await vi.waitFor(() => expect(catalogPool.end).toHaveBeenCalledOnce());

    expect(harness.events).toEqual([
      "connect db.internal:6543",
      "cancel 31/41",
      "destroy",
      "end",
    ]);
    expect(catalogClient.release).toHaveBeenCalledExactlyOnceWith(true);
  });
});

describe("PostgreSQL query abort signal", () => {
  it("rejects an already aborted signal without checking out a client", async () => {
    const client = createClient(1, 2);
    const { driver, pool } = await connectedDriver(client);
    const controller = new AbortController();
    controller.abort(new Error("panel closed"));

    await expect(
      driver.query("SELECT 1", [], { signal: controller.signal }),
    ).rejects.toThrow("panel closed");
    expect(pool.connect).toHaveBeenCalledTimes(1);
  });

  it("cancels the backend and discards the client when aborted mid-query", async () => {
    const client = createClient(4242, 99);
    const { driver } = await connectedDriver(client);
    let rejectRunning!: (error: Error) => void;
    client.query.mockReturnValueOnce(
      new Promise((_resolve, reject) => {
        rejectRunning = reject;
      }),
    );
    const controller = new AbortController();
    const signal = controller.signal;
    const removeListener = vi.spyOn(signal, "removeEventListener");
    const running = driver.query("SELECT * FROM big", [], { signal });
    const rejected = expect(running).rejects.toThrow("canceling statement");
    await vi.waitFor(() => expect(client.query).toHaveBeenCalledOnce());

    controller.abort(new Error("export cancelled"));
    await vi.waitFor(() =>
      expect(harness.events).toEqual([
        "connect db.internal:6543",
        "cancel 4242/99",
        "destroy",
      ]),
    );
    rejectRunning(new Error("canceling statement due to user request"));
    await rejected;
    expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(removeListener).toHaveBeenCalledWith("abort", expect.any(Function));
  });

  it("releases a late client when aborted during pool checkout", async () => {
    const client = createClient(1, 2);
    const { driver, pool } = await connectedDriver(client);
    let resolveCheckout!: (value: MockClient) => void;
    pool.connect.mockImplementationOnce(
      () =>
        new Promise<MockClient>((resolve) => {
          resolveCheckout = resolve;
        }),
    );
    const controller = new AbortController();
    const running = driver.query("SELECT 1", [], {
      signal: controller.signal,
    });
    await vi.waitFor(() => expect(pool.connect).toHaveBeenCalledTimes(2));

    controller.abort(new Error("panel closed"));
    await expect(running).rejects.toThrow("cancelled during pool checkout");
    resolveCheckout(client);

    await vi.waitFor(() =>
      expect(client.release).toHaveBeenCalledExactlyOnceWith(true),
    );
    expect(client.query).not.toHaveBeenCalled();
    expect(harness.events).toEqual(["destroy"]);
  });

  it("removes the abort listener after a successful query", async () => {
    const client = createClient(1, 2);
    const { driver } = await connectedDriver(client);
    const controller = new AbortController();
    const removeListener = vi.spyOn(controller.signal, "removeEventListener");

    await driver.query("SELECT 1", [], { signal: controller.signal });
    controller.abort();

    expect(removeListener).toHaveBeenCalledWith("abort", expect.any(Function));
    expect(harness.events).toEqual(["release"]);
  });
});

function mysqlDriver(
  queryObjectRows: (sql: string, params?: unknown[]) => Promise<unknown[]>,
) {
  const driver = new MySQLDriver({
    id: "g7-mysql",
    name: "MySQL",
    type: "mysql",
    host: "localhost",
  });
  (
    driver as unknown as {
      queryObjectRows: typeof queryObjectRows;
    }
  ).queryObjectRows = queryObjectRows;
  return driver;
}

describe("MySQL index metadata", () => {
  const statistics = [
    {
      INDEX_NAME: "PRIMARY",
      COLUMN_NAME: "id",
      NON_UNIQUE: 0,
      SUB_PART: null,
      INDEX_TYPE: "BTREE",
      COLLATION: "A",
    },
    {
      INDEX_NAME: "ft_body",
      COLUMN_NAME: "body",
      NON_UNIQUE: 1,
      SUB_PART: null,
      INDEX_TYPE: "FULLTEXT",
      COLLATION: null,
      EXPRESSION: null,
    },
    {
      INDEX_NAME: "ix_mixed",
      COLUMN_NAME: "title",
      NON_UNIQUE: 1,
      SUB_PART: 10,
      INDEX_TYPE: "BTREE",
      COLLATION: "A",
      EXPRESSION: null,
    },
    {
      INDEX_NAME: "ix_mixed",
      COLUMN_NAME: null,
      NON_UNIQUE: 1,
      SUB_PART: null,
      INDEX_TYPE: "BTREE",
      COLLATION: "D",
      EXPRESSION: "lower(`email`)",
    },
    {
      INDEX_NAME: "sp_geo",
      COLUMN_NAME: "geo",
      NON_UNIQUE: 1,
      SUB_PART: 32,
      INDEX_TYPE: "SPATIAL",
      COLLATION: "A",
    },
    {
      INDEX_NAME: "ft_prefixed",
      COLUMN_NAME: "summary",
      NON_UNIQUE: 1,
      SUB_PART: 255,
      INDEX_TYPE: "FULLTEXT",
      COLLATION: null,
    },
    {
      INDEX_NAME: "uq_hash",
      COLUMN_NAME: "code",
      NON_UNIQUE: "0",
      SUB_PART: null,
      INDEX_TYPE: "HASH",
      COLLATION: null,
    },
    {
      INDEX_NAME: "uq_lower",
      COLUMN_NAME: null,
      NON_UNIQUE: 0,
      SUB_PART: null,
      INDEX_TYPE: "BTREE",
      COLLATION: "A",
      EXPRESSION: "lower(`email`)",
    },
  ];

  it("keeps functional key parts instead of null column names", async () => {
    const queryObjectRows = vi.fn(async (_sql: string) => statistics);
    const driver = mysqlDriver(queryObjectRows);

    const indexes = await driver.getIndexes("db", "", "posts");

    const sql = queryObjectRows.mock.calls[0]?.[0] ?? "";
    expect(sql).toContain("information_schema.STATISTICS");
    expect(sql).not.toMatch(/SELECT \*/);
    expect(sql).not.toContain("CARDINALITY");
    expect(sql).toContain("EXPRESSION");
    expect(indexes.find((index) => index.name === "ix_mixed")).toEqual({
      name: "ix_mixed",
      columns: ["title", "lower(`email`)"],
      unique: false,
      primary: false,
    });
    expect(
      indexes.every((index) =>
        index.columns.every((column) => typeof column === "string"),
      ),
    ).toBe(true);
  });

  it("renders index DDL with expressions, prefixes, order and index kinds", async () => {
    const driver = mysqlDriver(async () => statistics);

    await expect(
      driver.getIndexDDL("db", "", "posts", "ix_mixed"),
    ).resolves.toBe(
      "CREATE INDEX `ix_mixed` ON `db`.`posts` (`title`(10), (lower(`email`)) DESC);",
    );
    await expect(
      driver.getIndexDDL("db", "", "posts", "ft_body"),
    ).resolves.toBe(
      "CREATE FULLTEXT INDEX `ft_body` ON `db`.`posts` (`body`);",
    );
    await expect(driver.getIndexDDL("db", "", "posts", "sp_geo")).resolves.toBe(
      "CREATE SPATIAL INDEX `sp_geo` ON `db`.`posts` (`geo`);",
    );
    await expect(
      driver.getIndexDDL("db", "", "posts", "ft_prefixed"),
    ).resolves.toBe(
      "CREATE FULLTEXT INDEX `ft_prefixed` ON `db`.`posts` (`summary`);",
    );
    await expect(
      driver.getIndexDDL("db", "", "posts", "uq_hash"),
    ).resolves.toBe(
      "CREATE UNIQUE INDEX `uq_hash` USING HASH ON `db`.`posts` (`code`);",
    );
    await expect(
      driver.getIndexDDL("db", "", "posts", "PRIMARY"),
    ).resolves.toBe("ALTER TABLE `db`.`posts` ADD PRIMARY KEY (`id`);");
    await expect(
      driver.getIndexDDL("db", "", "posts", "missing"),
    ).rejects.toThrow('Index "missing" not found');
  });

  it("drops EXPRESSION once when the server lacks it and remembers that", async () => {
    const queryObjectRows = vi.fn(async (sql: string) => {
      if (sql.includes("EXPRESSION")) {
        throw Object.assign(new Error("Unknown column 'EXPRESSION'"), {
          code: "ER_BAD_FIELD_ERROR",
          errno: 1054,
        });
      }
      return statistics.filter((row) => row.COLUMN_NAME !== null);
    });
    const driver = mysqlDriver(queryObjectRows);

    await driver.getIndexes("db", "", "posts");
    const indexes = await driver.getIndexes("db", "", "posts");

    expect(
      queryObjectRows.mock.calls.map(([sql]) => sql.includes("EXPRESSION")),
    ).toEqual([true, false, false]);
    expect(indexes.find((index) => index.name === "ix_mixed")?.columns).toEqual(
      ["title"],
    );
  });

  it("does not swallow unrelated STATISTICS errors", async () => {
    const driver = mysqlDriver(async () => {
      throw Object.assign(new Error("denied"), {
        code: "ER_TABLEACCESS_DENIED_ERROR",
        errno: 1142,
      });
    });

    await expect(driver.getIndexes("db", "", "posts")).rejects.toThrow(
      "denied",
    );
  });

  it("renders unique and primary constraint DDL from index metadata", async () => {
    const driver = mysqlDriver(async (sql: string) =>
      sql.includes("information_schema.STATISTICS")
        ? statistics
        : [
            {
              CONSTRAINT_NAME: "PRIMARY",
              CONSTRAINT_TYPE: "PRIMARY KEY",
              COLUMN_NAME: "id",
            },
            {
              CONSTRAINT_NAME: "uq_lower",
              CONSTRAINT_TYPE: "UNIQUE",
              COLUMN_NAME: null,
            },
            {
              CONSTRAINT_NAME: "fk_author",
              CONSTRAINT_TYPE: "FOREIGN KEY",
              COLUMN_NAME: "author_id",
              REFERENCED_TABLE_SCHEMA: "db",
              REFERENCED_TABLE_NAME: "authors",
              REFERENCED_COLUMN_NAME: "id",
            },
          ],
    );

    await expect(
      driver.getConstraintDDL("db", "", "posts", "uq_lower"),
    ).resolves.toBe(
      "CREATE UNIQUE INDEX `uq_lower` ON `db`.`posts` ((lower(`email`)));",
    );
    await expect(
      driver.getConstraintDDL("db", "", "posts", "PRIMARY"),
    ).resolves.toBe("ALTER TABLE `db`.`posts` ADD PRIMARY KEY (`id`);");
    await expect(
      driver.getConstraintDDL("db", "", "posts", "fk_author"),
    ).resolves.toContain("FOREIGN KEY (`author_id`) REFERENCES");
  });
});

describe("MySQL/MariaDB check constraints", () => {
  const constraintRows = [
    {
      CONSTRAINT_NAME: "CONSTRAINT_1",
      CONSTRAINT_TYPE: "CHECK",
      COLUMN_NAME: null,
      REFERENCED_TABLE_SCHEMA: null,
      REFERENCED_TABLE_NAME: null,
      REFERENCED_COLUMN_NAME: null,
    },
  ];

  it("uses the check clause of the same MariaDB table", async () => {
    const queryObjectRows = vi.fn(async (sql: string) =>
      sql.includes("CHECK_CONSTRAINTS")
        ? [
            {
              CONSTRAINT_NAME: "CONSTRAINT_1",
              TABLE_NAME: "other",
              CHECK_CLAUSE: "`x` > 100",
            },
            {
              CONSTRAINT_NAME: "CONSTRAINT_1",
              TABLE_NAME: "items",
              CHECK_CLAUSE: "`qty` > 0",
            },
          ]
        : constraintRows,
    );
    const driver = mysqlDriver(queryObjectRows);

    const constraints = await driver.getConstraints("db", "", "items");

    expect(constraints).toEqual([
      expect.objectContaining({
        name: "CONSTRAINT_1",
        kind: "check",
        checkExpression: "`qty` > 0",
      }),
    ]);
    expect(queryObjectRows.mock.calls[0]?.[0]).not.toContain(
      "CHECK_CONSTRAINTS",
    );
  });

  it("drops ambiguous check clauses when the catalog has no table name", async () => {
    const driver = mysqlDriver(async (sql: string) =>
      sql.includes("CHECK_CONSTRAINTS")
        ? [
            { CONSTRAINT_NAME: "CONSTRAINT_1", CHECK_CLAUSE: "`x` > 100" },
            { CONSTRAINT_NAME: "CONSTRAINT_1", CHECK_CLAUSE: "`qty` > 0" },
          ]
        : constraintRows,
    );

    const [constraint] = await driver.getConstraints("db", "", "items");

    expect(constraint?.checkExpression).toBeUndefined();
  });

  it("keeps listing constraints when the server lacks CHECK_CONSTRAINTS", async () => {
    const driver = mysqlDriver(async (sql: string) => {
      if (sql.includes("CHECK_CONSTRAINTS")) {
        throw Object.assign(new Error("Unknown table"), {
          code: "ER_UNKNOWN_TABLE",
          errno: 1109,
        });
      }
      return constraintRows;
    });

    await expect(driver.getConstraints("db", "", "items")).resolves.toEqual([
      expect.objectContaining({ name: "CONSTRAINT_1", kind: "check" }),
    ]);
  });
});

describe("fixed-width CHAR persisted edit verification", () => {
  const column: ColumnTypeMeta = {
    name: "code",
    type: "char(5)",
    nativeType: "char(5)",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
  const driver = new MySQLDriver({
    id: "g7-char",
    name: "MySQL",
    type: "mysql",
    host: "localhost",
  } as ConnectionConfig);

  it("detects lost trailing control and non-breaking whitespace", () => {
    expect(
      driver.checkPersistedEdit(column, "a\t\n", { persistedValue: "a\t" })?.ok,
    ).toBe(false);
    expect(
      driver.checkPersistedEdit(column, "a\u00a0", { persistedValue: "a" })?.ok,
    ).toBe(false);
  });

  it("still ignores trailing ASCII space padding", () => {
    expect(
      driver.checkPersistedEdit(column, "a\t", { persistedValue: "a\t   " })
        ?.ok,
    ).toBe(true);
  });
});
