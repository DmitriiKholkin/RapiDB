import mssql from "mssql";
import oracledb from "oracledb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import { TransactionIdentityStore } from "../../src/extension/dbDrivers/transactionVerification";
import type {
  ColumnTypeMeta,
  TransactionOperation,
} from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import { prepareInsertVerification } from "../../src/extension/table/insertVerification";
import { executeAtomicSqlApplyPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    category: "integer",
    identityGeneration: "auto_increment",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
  {
    name: "amount",
    type: "DECIMAL",
    nativeType: "DECIMAL",
    category: "decimal",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];
type Engine = "pg" | "mysql" | "mssql" | "oracle";
function harness(
  engine: Engine,
  mismatch = false,
  readWait?: () => Promise<void>,
  triggerAssigned: boolean | "compatible" = false,
  autoIncrement = true,
) {
  const config = { id: "b02", name: "B02", type: engine, database: "target" };
  const driver =
    engine === "pg"
      ? new PostgresDriver(config)
      : engine === "mysql"
        ? new MySQLDriver(config)
        : engine === "mssql"
          ? new MSSQLDriver(config)
          : new OracleDriver(config);
  const events: string[] = [];
  const reads: unknown[][] = [];
  const statements: string[] = [];
  let lastId = 100;
  const commit = vi.fn(async () => {
    events.push("COMMIT");
  });
  const rollback = vi.fn(async () => {
    events.push("ROLLBACK");
  });
  const release = vi.fn();
  const cancel = vi.fn(async () => undefined);
  const stored = new Map<number, unknown>();
  const execute = vi.fn(
    async (
      input: string | { sql: string; values?: unknown[] },
      params: unknown[] = [],
      executeOptions?: oracledb.ExecuteOptions,
    ) => {
      const sql = typeof input === "string" ? input : input.sql;
      if (typeof input !== "string") params = input.values ?? [];
      statements.push(sql);
      if (engine === "mssql" && sql.includes("sys.trigger_events")) {
        events.push("TRIGGER_GUARD");
        return { recordset: [] };
      }
      if (
        engine === "mssql" &&
        sql.includes("OUTPUT INSERTED") &&
        !sql.includes(" INTO @__rapidb_identity")
      )
        throw new Error(
          "SQL334: OUTPUT without INTO is forbidden with an enabled trigger",
        );
      if (sql === "BEGIN") {
        events.push("BEGIN");
        return {};
      }
      if (sql === "COMMIT") {
        await commit();
        return {};
      }
      if (sql === "ROLLBACK") {
        await rollback();
        return {};
      }
      if (sql === "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE") {
        events.push("SERIALIZABLE");
        return [];
      }
      if (sql.startsWith("SELECT 1 ")) {
        events.push("KEY_GUARD");
        return [stored.has(Number(params[0])) ? [{ __col_0: 1 }] : []];
      }
      if (
        engine === "mysql" &&
        sql.startsWith("SELECT `id`") &&
        sql.endsWith("LIMIT 2")
      ) {
        events.push("IDENTITY");
        return [stored.has(Number(params[0])) ? [{ __col_0: params[0] }] : []];
      }
      if (sql.startsWith("SELECT") && sql.includes("LAST_INSERT_ID()")) {
        events.push("IDENTITY");
        return [[{ __col_0: lastId }]];
      }
      if (sql.startsWith("SELECT")) {
        events.push("VERIFY");
        reads.push(params);
        await readWait?.();
        const id = Number(params[0]);
        const value = mismatch ? "999" : stored.get(id);
        const values =
          sql.startsWith('SELECT "id"') ||
          sql.startsWith("SELECT [id]") ||
          sql.startsWith("SELECT `id`")
            ? [id, value]
            : [value];
        const row = Object.fromEntries(
          values.map((value, index) => [`__col_${index}`, value]),
        );
        if (engine === "mysql") return [[row]];
        if (engine === "mssql")
          return {
            recordset: [values],
            columns: [values.map((_, index) => ({ name: `__col_${index}` }))],
          };
        if (engine === "oracle") {
          expect(executeOptions).toMatchObject({
            autoCommit: false,
            resultSet: true,
            outFormat: oracledb.OUT_FORMAT_ARRAY,
          });
          const metadata = values.map((_, index) => ({
            name: `__col_${index}`,
            dbType: oracledb.DB_TYPE_NUMBER,
            precision: 20,
            scale: values.length > 1 && index === 0 ? 0 : 4,
          }));
          const decoded = values.map((value, index) => {
            const handler = executeOptions?.fetchTypeHandler?.(metadata[index]);
            expect(handler?.type).toBe(oracledb.STRING);
            return handler?.converter?.(String(value)) ?? value;
          });
          let fetched = false;
          return {
            resultSet: {
              metaData: metadata,
              getRows: vi.fn(async () => {
                const batch = fetched ? [] : [decoded];
                fetched = true;
                return batch;
              }),
              close: vi.fn(async () => {}),
            },
          };
        }
        return { rows: [row], rowCount: 1 };
      }
      events.push("INSERT");
      const outBinds = params.filter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          "dir" in value &&
          value.dir === oracledb.BIND_OUT,
      );
      const inputParams = params.filter((value) => !outBinds.includes(value));
      const id = triggerAssigned
        ? 1000
        : inputParams.length === 2
          ? Number(inputParams[0])
          : ++lastId;
      if (triggerAssigned) stored.set(lastId, inputParams.at(-1));
      stored.set(id, triggerAssigned === true ? "999" : inputParams.at(-1));
      const row = { __col_0: id };
      return engine === "mysql"
        ? [{ affectedRows: 1, insertId: id }]
        : {
            rowCount: 1,
            rowsAffected: 1,
            rows: [row],
            recordset: engine === "mssql" ? [{ __col_0: 999 }] : [row],
            recordsets:
              engine === "mssql" ? [[{ __col_0: 999 }], [row]] : [[row]],
            outBinds: outBinds.map(() => [String(id)]),
            output: { __rapidb_affected_rows: 1 },
          };
    },
  );
  const connection = {
    query: execute,
    execute,
    beginTransaction: async () => {
      events.push("BEGIN");
    },
    commit,
    rollback,
    release,
    close: release,
    destroy: cancel,
    break: cancel,
  };
  const lease = vi.fn(async () => connection);
  (driver as unknown as { pool: unknown }).pool = {
    connect: lease,
    getConnection: lease,
    config: { options: {} },
  };
  if (engine === "mssql") {
    vi.spyOn(mssql.Transaction.prototype, "begin").mockImplementation(
      (async () => {
        events.push("BEGIN");
      }) as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "commit").mockImplementation(
      commit as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "rollback").mockImplementation(
      rollback as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "request").mockImplementation(() => {
      const params: unknown[] = [];
      return {
        input: vi.fn((_name, _type, value) => {
          params.push(value);
        }),
        output: vi.fn(),
        cancel,
        query: (sql: string) => execute(sql, params),
      } as never;
    });
  }
  if (engine === "oracle") {
    const oracle = driver as unknown as {
      getConnection(): Promise<unknown>;
    };
    vi.spyOn(oracle, "getConnection").mockResolvedValue(connection);
  }
  const outsideQuery = vi
    .spyOn(driver, "query")
    .mockRejectedValue(new Error("No outside transaction reads"));
  vi.spyOn(driver, "getMutationAtomicityRisk").mockResolvedValue(null);
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  } as never;
  const service = new TableMutationService(manager, {
    getColumns: async () =>
      engine === "oracle"
        ? columns.map((column) => ({
            ...column,
            nativeType: column.isPrimaryKey ? "NUMBER(10,0)" : "NUMBER(20,4)",
          }))
        : columns.map((column) => ({
            ...column,
            identityGeneration: autoIncrement
              ? column.identityGeneration
              : undefined,
          })),
  });
  const prepare = (values: Record<string, unknown>) =>
    service.prepareInsertRow("b02", "target", "public", "edits", values);
  return {
    driver,
    manager,
    service,
    prepare,
    events,
    reads,
    statements,
    lease,
    outsideQuery,
    cancel,
    execute,
    stored,
  };
}
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("INSERT verification contracts (B02)", () => {
  it("MySQL never verifies the stale LAST_INSERT_ID row after a BEFORE trigger assigns NEW.id", async () => {
    const { service, prepare, events } = harness(
      "mysql",
      false,
      undefined,
      true,
    );
    await expect(
      service.executePreparedInsertPlan(await prepare({ amount: "1.25" })),
    ).rejects.toThrow(/INSERT verification failed/);
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });

  it("Oracle permits ordinary inserts with a generated identity", async () => {
    const { service, prepare } = harness("oracle");
    await expect(
      service.executePreparedInsertPlan(await prepare({ amount: "1.25" })),
    ).resolves.toBeUndefined();
  });

  it("MySQL verifies the current trigger-assigned PK and permits compatible inserts", async () => {
    const { service, prepare, reads, statements } = harness(
      "mysql",
      false,
      undefined,
      "compatible",
    );
    await service.executePreparedInsertPlan(await prepare({ amount: "1.25" }));
    expect(reads).toEqual([[1000]]);
    expect(statements.some((sql) => sql.includes("LAST_INSERT_ID()"))).toBe(
      false,
    );
  });

  it.each([
    undefined,
    0,
    "0",
    9007199254740992,
  ])("MySQL fails closed without a reliable current OK-packet ID (%s)", async (insertId) => {
    const { service, prepare, execute, reads, events } = harness("mysql");
    execute.mockResolvedValueOnce([{ affectedRows: 1, insertId }] as never);
    await expect(
      service.executePreparedInsertPlan(await prepare({ amount: "1.25" })),
    ).rejects.toThrow(/identity could not be captured reliably/);
    expect(reads).toEqual([]);
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });

  it("MSSQL preserves identifiers containing the old clause-replacement pattern", async () => {
    const { service, driver } = harness("mssql");
    const weird = { ...columns[1], name: "note) VALUES (suffix" };
    const manager = {
      getConnection: () => ({ type: "mssql" }),
      getDriver: () => driver,
    } as never;
    const mutation = new TableMutationService(manager, {
      getColumns: async () => [columns[0], weird],
    });
    const plan = await mutation.prepareInsertRow(
      "b02",
      "target",
      "public",
      "edits",
      { [weird.name]: "1.25" },
    );
    expect(plan.operation.sql).toContain(
      "([note) VALUES (suffix]) OUTPUT INSERTED",
    );
    expect(plan.operation.sql).toContain(" INTO @__rapidb_identity");
    expect(service).toBeDefined();
  });

  it.each([
    "note) VALUES (suffix",
    "bracket]name",
    "semi; -- ? quote'",
  ])("MSSQL OUTPUT INTO is structural for identifier %s", async (name) => {
    const { driver } = harness("mssql");
    const weird = { ...columns[1], name };
    const operation = buildInsertRowOperation(
      driver,
      "db) VALUES (suffix",
      "schema]name",
      name,
      { [name]: "1.25" },
      [columns[0], weird],
      { backend: "mssql", columns: [columns[0]] },
    );
    expect(operation.sql).toContain(
      `INSERT INTO ${driver.qualifiedTableName("db) VALUES (suffix", "schema]name", name)} (${driver.quoteIdentifier(name)}) OUTPUT INSERTED.[id] INTO @__rapidb_identity ([__col_0]) VALUES (`,
    );
    expect(operation.captureIdentity?.mssqlSelect).toBe(
      "SELECT [__col_0] FROM @__rapidb_identity;",
    );
  });

  it("MSSQL saves outer affectedRows before selecting the final identity after trigger resultsets", async () => {
    const { service, prepare, statements, reads } = harness("mssql");
    await service.executePreparedInsertPlan(await prepare({ amount: "1.25" }));
    const sql = statements.find((statement) =>
      statement.includes("INTO @__rapidb_identity"),
    );
    if (!sql) throw new Error("Expected MSSQL identity INSERT");
    expect(sql).toContain("INTO @__rapidb_identity");
    expect(
      sql.indexOf("SET @__rapidb_affected_rows = @@ROWCOUNT"),
    ).toBeLessThan(sql.indexOf("SELECT [__col_0] FROM @__rapidb_identity"));
    expect(reads).toEqual([[101]]);
  });

  it("MSSQL captures decimal composite keys without native JS-number precision loss", () => {
    const { driver } = harness("mssql");
    const key = {
      ...columns[1],
      name: "exact) VALUES (key",
      nativeType: "decimal(38,18)",
      isPrimaryKey: true,
    };
    const operation = buildInsertRowOperation(
      driver,
      "target",
      "public",
      "edits",
      { amount: "1.25" },
      columns,
      { backend: "mssql", columns: [columns[0], key] },
    );
    expect(operation.sql).toContain("[__col_1] decimal(38,18)");
    expect(operation.sql).toContain("INSERTED.[exact) VALUES (key]");
    expect(operation.captureIdentity?.mssqlSelect).toContain(
      "CONVERT(varchar(100), [__col_1], 2) AS [__col_1]",
    );
  });
  it.each([
    "pg",
    "mysql",
    "mssql",
  ] as const)("%s captures each generated key on the transaction and verifies before commit", async (engine) => {
    const { manager, prepare, events, reads, outsideQuery, statements } =
      harness(engine);
    const inserts = await Promise.all([
      prepare({ amount: "1.25" }),
      prepare({ amount: "2.5" }),
    ]);
    expect(
      await executeAtomicSqlApplyPlan(manager, null, inserts),
    ).toMatchObject({ success: true, insertApplied: true });
    expect(reads).toEqual([[101], [102]]);
    expect(events).toEqual([
      "BEGIN",
      ...(engine === "mssql" ? ["TRIGGER_GUARD"] : []),
      "INSERT",
      ...(engine === "mysql" ? ["IDENTITY"] : []),
      ...(engine === "mssql" ? ["TRIGGER_GUARD"] : []),
      "INSERT",
      ...(engine === "mysql" ? ["IDENTITY"] : []),
      "VERIFY",
      "VERIFY",
      "COMMIT",
    ]);
    expect(
      statements.some((sql) =>
        engine === "pg"
          ? sql.includes("RETURNING")
          : engine === "mssql"
            ? sql.includes("OUTPUT INSERTED")
            : !sql.includes("LAST_INSERT_ID()"),
      ),
    ).toBe(true);
    expect(outsideQuery).not.toHaveBeenCalled();
  });

  it.each([
    "pg",
    "mysql",
    "mssql",
    "oracle",
  ] as const)("%s verifies explicit keys and rolls back mismatches", async (engine) => {
    const { service, prepare, events, outsideQuery } = harness(engine, true);
    const plan = await prepare({ id: 3, amount: "1.25" });
    await expect(service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /INSERT verification failed/,
    );
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
    expect(outsideQuery).not.toHaveBeenCalled();
  });

  it.each([
    "pg",
    "mysql",
    "mssql",
  ] as const)("%s rolls back insert-only batches on generated-key value mismatch", async (engine) => {
    const { manager, prepare, events } = harness(engine, true);
    const inserts = await Promise.all([
      prepare({ amount: "1.25" }),
      prepare({ amount: "2.5" }),
    ]);
    const result = await executeAtomicSqlApplyPlan(manager, null, inserts);
    expect(result.success).toBe(false);
    expect(result.insertApplied).not.toBe(true);
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });

  it("Oracle binds generated identity OUT values and leaves default-only inserts intact", async () => {
    const { service, prepare, events } = harness("oracle");
    const generated = await prepare({ amount: "1.25" });
    expect(generated.operation.sql).toMatch(
      /RETURNING TO_CHAR\("id".*INTO :2$/,
    );
    expect(generated.operation.captureIdentity?.oracleOutTypes).toEqual([
      "string",
    ]);
    expect(events).toEqual([]);
    const plan = await prepare({});
    expect(plan.verification).toBeUndefined();
    expect(plan.operation.sql).toMatch(/DEFAULT/);
    expect(service).toBeDefined();
  });

  it("rejects MySQL generated identities without an AUTO_INCREMENT primary key before mutation", async () => {
    const { driver } = harness("mysql");
    const composite = [
      ...columns.map((column) => ({
        ...column,
        identityGeneration: undefined,
      })),
      { ...columns[0], name: "other", identityGeneration: undefined },
    ];
    const operation = buildInsertRowOperation(
      driver,
      "target",
      "public",
      "edits",
      { amount: "1.25" },
      composite,
    );
    expect(() =>
      prepareInsertVerification(
        driver,
        "mysql",
        "target",
        "public",
        "edits",
        { amount: "1.25" },
        composite,
        operation,
      ),
    ).toThrow(/cannot capture/);
  });

  it.each([
    "single",
    "batch",
  ])("blocks %s verified MySQL inserts into non-transactional tables before DML", async (mode) => {
    const { driver, manager, service, prepare, events } = harness("mysql");
    vi.spyOn(driver, "getMutationAtomicityRisk").mockResolvedValue(
      "Table uses non-transactional MyISAM engine",
    );
    const insert = await prepare({ amount: "1.25" });
    if (mode === "single")
      await expect(service.executePreparedInsertPlan(insert)).rejects.toThrow(
        /requires rollback support/,
      );
    else
      expect(
        await executeAtomicSqlApplyPlan(manager, null, [insert, insert]),
      ).toMatchObject({
        success: false,
        error: expect.stringContaining("requires rollback support"),
      });
    expect(events).toEqual([]);
  });

  it("MySQL verifies ordinary explicit non-auto keys under transaction-local serializable range locks", async () => {
    const { service, prepare, events, statements, reads } = harness(
      "mysql",
      false,
      undefined,
      false,
      false,
    );
    await service.executePreparedInsertPlan(
      await prepare({ id: 3, amount: "1.25" }),
    );
    expect(events).toEqual([
      "SERIALIZABLE",
      "BEGIN",
      "KEY_GUARD",
      "INSERT",
      "VERIFY",
      "COMMIT",
    ]);
    expect(statements[1]).toContain("LIMIT 1 FOR UPDATE");
    expect(reads).toEqual([[3]]);
  });

  it("MySQL cannot commit by verifying an old explicit non-auto key after a trigger redirects INSERT", async () => {
    const { service, prepare, events, stored } = harness(
      "mysql",
      false,
      undefined,
      true,
      false,
    );
    stored.set(3, "1.25");
    await expect(
      service.executePreparedInsertPlan(
        await prepare({ id: 3, amount: "1.25" }),
      ),
    ).rejects.toThrow(/already existing primary key/);
    expect(events).toEqual(["SERIALIZABLE", "BEGIN", "KEY_GUARD", "ROLLBACK"]);
  });

  it("MySQL rolls back when a BEFORE trigger redirects an explicit non-auto key that was absent", async () => {
    const { service, prepare, events } = harness(
      "mysql",
      false,
      undefined,
      true,
      false,
    );
    await expect(
      service.executePreparedInsertPlan(
        await prepare({ id: 3, amount: "1.25" }),
      ),
    ).rejects.toThrow(/INSERT verification failed/);
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });

  it("MySQL captures all actual composite PK components, including trigger/default-assigned keys", async () => {
    const { driver, execute } = harness("mysql");
    const composite = [
      ...columns,
      { ...columns[0], name: "tenant", identityGeneration: undefined },
    ];
    const operation = buildInsertRowOperation(
      driver,
      "target",
      "public",
      "edits",
      { amount: "1.25" },
      composite,
    );
    const verification = prepareInsertVerification(
      driver,
      "mysql",
      "target",
      "public",
      "edits",
      { amount: "1.25" },
      composite,
      operation,
    );
    execute.mockResolvedValueOnce([
      { affectedRows: 1, insertId: "9007199254740993" },
    ] as never);
    execute.mockResolvedValueOnce([
      [{ __col_0: "9007199254740993", __col_1: "actual-tenant" }],
    ] as never);
    execute.mockResolvedValueOnce([[{ __col_0: "1.25" }]] as never);
    if (!verification) throw new Error("Expected INSERT verification");
    await driver.runTransaction([operation], undefined, {
      verifications: [verification],
    });
    expect(execute.mock.calls[1][0]).toMatchObject({
      values: ["9007199254740993"],
    });
    expect(execute.mock.calls[2][0]).toMatchObject({
      values: ["9007199254740993", "actual-tenant"],
    });
  });

  it("MySQL fails closed when the current auto ID is ambiguous across a composite PK", async () => {
    const { driver, execute, events } = harness("mysql");
    const composite = [
      ...columns,
      { ...columns[0], name: "tenant", identityGeneration: undefined },
    ];
    const operation = buildInsertRowOperation(
      driver,
      "target",
      "public",
      "edits",
      { amount: "1.25" },
      composite,
    );
    const verification = prepareInsertVerification(
      driver,
      "mysql",
      "target",
      "public",
      "edits",
      { amount: "1.25" },
      composite,
      operation,
    );
    execute.mockResolvedValueOnce([
      { affectedRows: 1, insertId: 101 },
    ] as never);
    execute.mockResolvedValueOnce([
      [
        { __col_0: 101, __col_1: "old" },
        { __col_0: 101, __col_1: "new" },
      ],
    ] as never);
    if (!verification) throw new Error("Expected INSERT verification");
    await expect(
      driver.runTransaction([operation], undefined, {
        verifications: [verification],
      }),
    ).rejects.toThrow(/identity could not be captured reliably/);
    expect(events).toContain("ROLLBACK");
    expect(events).not.toContain("COMMIT");
  });

  it.each(
    [
      [],
      [{ __col_0: 1 }, { __col_0: 2 }],
      [{ __col_0: null }],
      [{ __col_0: 9007199254740992 }],
    ].map((rows) => ({ rows })),
  )("fails closed on unreliable returned identity $rows", async ({ rows }) => {
    const store = new TransactionIdentityStore();
    const operation: TransactionOperation = {
      sql: "INSERT",
      captureIdentity: {},
    };
    await store.capture(0, operation, rows, async () => []);
    expect(() =>
      store.resolve([
        {
          rowIndex: 0,
          mutation: "insert",
          sql: "SELECT",
          params: [null],
          values: [{ column: columns[1], expectedValue: "1.25" }],
          identity: { operationIndex: 0, parameterIndexes: [0] },
        },
      ]),
    ).toThrow(/identity could not be captured reliably/);
  });

  it.each([
    "pg",
    "mysql",
    "mssql",
    "oracle",
  ] as const)("%s never commits after verification settles beyond its timeout", async (engine) => {
    vi.useFakeTimers();
    let finish!: () => void;
    const wait = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const { driver: raw, prepare, events } = harness(engine, false, () => wait);
    const plan = await prepare({ id: 3, amount: "1.25" });
    if (!plan.verification) throw new Error("Expected INSERT verification");
    const driver = createTimeoutAwareDriver(raw, () => ({
      connectionTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutSeconds: 1,
      dbOperationTimeoutMs: 25,
    }));
    const pending = driver.runTransaction([plan.operation], undefined, {
      database: "target",
      verifications: [plan.verification],
    });
    const rejection = expect(pending).rejects.toThrow(/outcome may be unknown/);
    await vi.advanceTimersByTimeAsync(25);
    await rejection;
    finish();
    await vi.runAllTimersAsync();
    expect(events).not.toContain("COMMIT");
    expect(events).toContain("ROLLBACK");
  });
});
