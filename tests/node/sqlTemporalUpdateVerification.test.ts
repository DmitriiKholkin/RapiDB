import mssql from "mssql";
import oracledb from "oracledb";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { RowUpdate } from "../../src/extension/table/tableDataContracts";
import {
  applyChangesTransactional,
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: name === "id",
    primaryKeyOrdinal: name === "id" ? 1 : undefined,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  };
}

// Only the transport is mocked: the planner, binding, driver transaction,
// verification, commit/rollback and Apply outcomes are the production paths.
function harness(
  engine: "mssql" | "oracle",
  temporal: ColumnTypeMeta,
  persist = (value: unknown, _id: number) => value,
) {
  const config = { id: "temporal", name: "Temporal", type: engine };
  const driver =
    engine === "mssql" ? new MSSQLDriver(config) : new OracleDriver(config);
  const columns = [
    column("id", engine === "mssql" ? "int" : "NUMBER(10)", "integer"),
    temporal,
  ];
  const events: string[] = [];
  const stored = new Map<number, unknown>([
    [1, "original one"],
    [2, "original two"],
  ]);
  const before = new Map(stored);
  const connection = { ...config, readOnly: false };
  const manager = {
    getConnection: () => connection,
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  const run = async (sql: string, params: unknown[]) => {
    if (sql.startsWith("ALTER SESSION")) return {};
    if (sql.startsWith("SELECT")) {
      events.push("VERIFY");
      const rows = [[stored.get(Number(params[0]))]];
      if (engine === "mssql")
        return { recordset: rows, columns: [[{ name: "__col_0" }]] };
      let fetched = false;
      return {
        resultSet: {
          metaData: [{ name: "__col_0", dbType: oracledb.DB_TYPE_VARCHAR }],
          getRows: async () => {
            const result = fetched ? [] : rows;
            fetched = true;
            return result;
          },
          close: async () => undefined,
        },
      };
    }
    events.push("DML");
    stored.set(Number(params[1]), persist(params[0], Number(params[1])));
    return { rowsAffected: 1, output: { __rapidb_affected_rows: 1 } };
  };
  const commit = async () => {
    events.push("COMMIT");
  };
  const rollback = async () => {
    events.push("ROLLBACK");
    stored.clear();
    for (const [key, value] of before) stored.set(key, value);
  };
  if (engine === "mssql") {
    (driver as unknown as { pool: unknown }).pool = { config: { options: {} } };
    vi.spyOn(mssql.Transaction.prototype, "begin").mockImplementation(
      (async () => {
        events.push("BEGIN");
      }) as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "request").mockImplementation(() => {
      const params: unknown[] = [];
      return {
        input: (name: string, _type: unknown, value: unknown) => {
          params[Number(name.slice(1)) - 1] = value;
        },
        output: vi.fn(),
        cancel: vi.fn(),
        query: (sql: string) => run(sql, params),
      } as unknown as mssql.Request;
    });
    vi.spyOn(mssql.Transaction.prototype, "commit").mockImplementation(
      commit as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "rollback").mockImplementation(
      rollback as never,
    );
  } else {
    (driver as unknown as { pool: unknown }).pool = {
      getConnection: async () => ({
        execute: run,
        commit,
        rollback,
        close: async () => undefined,
      }),
    };
  }
  const transaction = vi.spyOn(driver, "runTransaction");
  const outsideRead = vi
    .spyOn(driver, "query")
    .mockRejectedValue(new Error("Read outside the transaction"));
  const prepare = (updates: RowUpdate[], metadata = columns) =>
    prepareApplyChangesPlan(
      manager,
      config.id,
      "db",
      "schema",
      "events",
      updates,
      metadata,
    );
  return {
    driver,
    columns,
    events,
    stored,
    before,
    connection,
    manager,
    prepare,
    transaction,
    outsideRead,
  };
}

const update = (id: number, value: unknown): RowUpdate => ({
  primaryKeys: { id },
  changes: { event_at: value },
});
const paths = ["prepared", "direct", "atomic"] as const;
async function execute(
  path: (typeof paths)[number],
  h: ReturnType<typeof harness>,
  updates: RowUpdate[],
) {
  if (path === "direct")
    return applyChangesTransactional(
      h.manager,
      "temporal",
      "db",
      "schema",
      "events",
      updates,
      h.columns,
    );
  const prepared = h.prepare(updates);
  if (!prepared.executable) return prepared.result;
  return path === "prepared"
    ? executePreparedApplyPlan(h.manager, prepared.plan)
    : executeAtomicSqlApplyPlan(h.manager, prepared.plan, []);
}

afterEach(() => vi.restoreAllMocks());

describe("MSSQL temporal UPDATE verification", () => {
  for (const path of paths) {
    it.each([
      [
        "datetime2(0)",
        "datetime",
        "2026-10-20 12:00:00",
        "2026-10-20 12:00:00.1234567",
      ],
      ["time(0)", "time", "12:00:00", "12:00:00.1234567"],
      [
        "datetimeoffset(0)",
        "datetime",
        "2026-10-20 12:00:00+02:00",
        "2026-10-20 12:00:00.1234567+02:00",
      ],
    ] as const)(`${path}: rejects %s precision loss before any write in the batch`, async (type, category, valid, lossy) => {
      const h = harness("mssql", column("event_at", type, category));
      const result = await execute(path, h, [
        update(1, valid),
        update(2, lossy),
      ]);
      expect(result).toMatchObject({
        success: false,
        failedRows: [1],
        rowOutcomes: [
          { rowIndex: 0, success: false, status: "skipped" },
          {
            rowIndex: 1,
            success: false,
            status: "prevalidation_failed",
            columns: ["event_at"],
          },
        ],
      });
      expect(h.stored).toEqual(h.before);
      expect(h.events).toEqual([]);
      expect(h.transaction).not.toHaveBeenCalled();
    });

    it(`${path}: verifies equivalent time zero-padding before committing`, async () => {
      const h = harness(
        "mssql",
        column("event_at", "time(7)", "time"),
        () => "12:00:00.1230000",
      );
      const result = await execute(path, h, [update(1, "12:00:00.123")]);
      expect(result).toMatchObject({
        success: true,
        rowOutcomes: [{ status: "applied" }],
      });
      expect(h.events).toEqual(["BEGIN", "DML", "VERIFY", "COMMIT"]);
      expect(h.outsideRead).not.toHaveBeenCalled();
    });

    it(`${path}: rolls back every row when readback detects a changed last fractional digit`, async () => {
      const wanted = "2026-10-20 12:00:00.1234567";
      const h = harness(
        "mssql",
        column("event_at", "datetime2(7)", "datetime"),
        (value, id) => (id === 2 ? wanted.replace(/7$/, "6") : value),
      );
      const result = await execute(path, h, [
        update(1, wanted),
        update(2, wanted),
      ]);
      expect(result).toMatchObject({
        success: false,
        failedRows: [1],
        rowOutcomes: [
          { rowIndex: 0, success: false, status: "skipped" },
          {
            rowIndex: 1,
            success: false,
            status: "verification_failed",
            columns: ["event_at"],
          },
        ],
      });
      expect(h.events).toEqual([
        "BEGIN",
        "DML",
        "DML",
        "VERIFY",
        "VERIFY",
        "ROLLBACK",
      ]);
      expect(h.stored).toEqual(h.before);
      expect(h.outsideRead).not.toHaveBeenCalled();
    });
  }

  it("reads back using the new exact temporal primary key", async () => {
    const temporal = {
      ...column("event_at", "datetime2(7)", "datetime"),
      isPrimaryKey: true,
      primaryKeyOrdinal: 2,
    };
    const h = harness("mssql", temporal);
    const wanted = "2026-10-20 12:00:00.1234567";
    const prepared = h.prepare([
      {
        primaryKeys: { id: 1, event_at: "2026-10-19 12:00:00.1234567" },
        changes: { event_at: wanted },
      },
    ]);
    if (!prepared.executable) throw new Error("Expected executable plan");
    const result = await executePreparedApplyPlan(h.manager, prepared.plan);
    expect(result.success).toBe(true);
    const verification = h.transaction.mock.calls[0][2]?.verifications?.[0];
    expect(verification?.params).toEqual([1, wanted]);
    expect(verification?.sql).toContain("WHERE [id] = ? AND [event_at] = ?");
    expect(verification?.values[0].expectedValue).toBe(wanted);
  });

  it("commits a datetimeoffset edit when the server normalizes negative zero offset", async () => {
    const h = harness(
      "mssql",
      column("event_at", "datetimeoffset(3)", "datetime"),
      () => "2026-10-20 12:00:00.1230000+00:00",
    );
    const result = await execute("prepared", h, [
      update(1, "2026-10-20T12:00:00.123-00:00"),
    ]);
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ status: "applied" }],
    });
    expect(h.events).toEqual(["BEGIN", "DML", "VERIFY", "COMMIT"]);
  });

  it.each([
    ["time(7)", "time", "12:00", "12:00:00.0000000"],
    [
      "datetime2(7)",
      "datetime",
      "2026-10-20T12:00",
      "2026-10-20 12:00:00.0000000",
    ],
  ] as const)("commits equivalent minute-only %s input", async (type, category, wanted, actual) => {
    const h = harness(
      "mssql",
      column("event_at", type, category),
      () => actual,
    );
    const result = await execute("prepared", h, [update(1, wanted)]);
    expect(result.success).toBe(true);
    expect(h.events).toEqual(["BEGIN", "DML", "VERIFY", "COMMIT"]);
  });

  it.each([
    "datetime",
    "smalldatetime",
  ])("rolls back %s rounding detected on readback", async (type) => {
    const h = harness(
      "mssql",
      column("event_at", type, "datetime"),
      () => "2026-10-20 12:00:00",
    );
    const result = await execute("prepared", h, [
      update(1, "2026-10-20 12:00:00.123"),
    ]);
    expect(result.rowOutcomes?.[0].status).toBe("verification_failed");
    expect(h.events).toEqual(["BEGIN", "DML", "VERIFY", "ROLLBACK"]);
    expect(h.stored).toEqual(h.before);
  });

  it("rolls back an INSERT together with UPDATEs on temporal verification failure", async () => {
    const h = harness(
      "mssql",
      column("event_at", "datetime2(7)", "datetime"),
      () => "2026-10-20 12:00:00",
    );
    const prepared = h.prepare([update(1, "2026-10-20 12:00:00.1234567")]);
    if (!prepared.executable) throw new Error("Expected executable plan");
    const result = await executeAtomicSqlApplyPlan(h.manager, prepared.plan, [
      {
        connectionId: "temporal",
        database: "db",
        schema: "schema",
        table: "events",
        operation: {
          sql: "INSERT INTO events (event_at, id) VALUES (?, ?)",
          params: ["2026-10-20 12:00:00", 3],
        },
        previewStatements: [],
        verificationCriteria: null,
      },
    ]);
    expect(result.success).toBe(false);
    expect(result.insertApplied).not.toBe(true);
    expect(h.events).toEqual(["BEGIN", "DML", "DML", "VERIFY", "ROLLBACK"]);
    expect(h.stored).toEqual(h.before);
  });

  it("keeps preview parameters exact and rechecks read-only status before execution", async () => {
    const h = harness(
      "mssql",
      column("event_at", "datetimeoffset(7)", "datetime"),
    );
    const wanted = "2026-10-20 12:00:00.1234567+02:00";
    const prepared = h.prepare([update(1, wanted)]);
    if (!prepared.executable) throw new Error("Expected executable plan");
    expect(prepared.plan.previewStatements[0]).toContain(wanted);
    expect(prepared.plan.operations[0].params?.[0]).toBe(wanted);
    expect(prepared.plan.verificationTargets[0].values[0].expectedValue).toBe(
      wanted,
    );
    expect(h.events).toEqual([]);
    h.connection.readOnly = true;
    expect(() => h.prepare([update(1, wanted)])).toThrow(/read.only/i);
    const result = await executePreparedApplyPlan(h.manager, prepared.plan);
    expect(result).toMatchObject({
      success: false,
      error: expect.stringMatching(/read.only/i),
    });
    expect(h.events).toEqual([]);
    expect(h.transaction).not.toHaveBeenCalled();
  });
});

describe("Oracle existing temporal verifiers in UPDATE plans", () => {
  it.each([
    ["DATE", "2026-10-20 12:00:00", "2026-10-20 12:00:00", "HH24:MI:SS"],
    [
      "TIMESTAMP(9)",
      "2026-10-20 12:00:00.123456789",
      "2026-10-20 12:00:00.123456789",
      "FF9",
    ],
    [
      "TIMESTAMP(9) WITH TIME ZONE",
      "2026-10-20 12:00:00.123456789 +02:00",
      "2026-10-20 10:00:00.123456789 +00:00",
      "TZH:TZM",
    ],
    [
      "TIMESTAMP(9) WITH LOCAL TIME ZONE",
      "2026-10-20 12:00:00.123456789 +02:00",
      "2026-10-20 10:00:00.123456789Z",
      "AT TIME ZONE 'UTC'",
    ],
  ])("verifies %s with its exact projection and timezone semantics", async (type, wanted, actual, projection) => {
    const h = harness(
      "oracle",
      column("event_at", type, "datetime"),
      () => actual,
    );
    const result = await execute("prepared", h, [update(1, wanted)]);
    expect(result).toMatchObject({ success: true });
    expect(h.transaction.mock.calls[0][2]?.verifications?.[0].sql).toContain(
      projection,
    );
    expect(h.events).toEqual(["DML", "VERIFY", "COMMIT"]);
    expect(h.outsideRead).not.toHaveBeenCalled();
  });

  it.each([
    "prepared",
    "direct",
    "atomic",
  ] as const)("%s: rolls back the whole batch on a nanosecond mismatch", async (path) => {
    const wanted = "2026-10-20 12:00:00.123456789 +02:00";
    const h = harness(
      "oracle",
      column("event_at", "TIMESTAMP(9) WITH TIME ZONE", "datetime"),
      (value, id) => (id === 2 ? wanted.replace("789", "788") : value),
    );
    const result = await execute(path, h, [
      update(1, wanted),
      update(2, wanted),
    ]);
    expect(result).toMatchObject({
      success: false,
      failedRows: [1],
      rowOutcomes: [
        { status: "skipped", success: false },
        { status: "verification_failed", columns: ["event_at"] },
      ],
    });
    expect(h.events).toEqual(["DML", "DML", "VERIFY", "VERIFY", "ROLLBACK"]);
    expect(h.stored).toEqual(h.before);
  });

  it("keeps Oracle fractional precision prevalidation before all writes", async () => {
    const h = harness("oracle", column("event_at", "TIMESTAMP(6)", "datetime"));
    const result = await execute("direct", h, [
      update(1, "2026-10-20 12:00:00.123456"),
      update(2, "2026-10-20 12:00:00.123456789"),
    ]);
    expect(result.rowOutcomes?.[1].status).toBe("prevalidation_failed");
    expect(h.events).toEqual([]);
  });
});

describe("untouched server-managed temporal columns", () => {
  it.each([
    "mssql",
    "oracle",
  ] as const)("%s verifies explicit temporal SQL NULL in the transaction", async (engine) => {
    const h = harness(
      engine,
      column(
        "event_at",
        engine === "mssql" ? "time(0)" : "TIMESTAMP(9)",
        engine === "mssql" ? "time" : "datetime",
      ),
    );
    const result = await execute("direct", h, [update(1, null)]);
    expect(result.success).toBe(true);
    expect(h.events).toContain("VERIFY");
    expect(h.events.at(-1)).toBe("COMMIT");
    expect(h.stored.get(1)).toBe(null);
  });

  it.each([
    "mssql",
    "oracle",
  ] as const)("%s only verifies explicitly changed columns, not temporal defaults/on-update fields", async (engine) => {
    const h = harness(
      engine,
      column(
        "event_at",
        engine === "mssql" ? "datetime2(7)" : "TIMESTAMP(9)",
        "datetime",
      ),
    );
    const serverManaged = {
      ...column("updated_at", h.columns[1].nativeType, "datetime"),
      defaultValue: "CURRENT_TIMESTAMP",
      onUpdateExpression: "CURRENT_TIMESTAMP",
    };
    const prepared = h.prepare(
      [update(1, "2026-10-20 12:00:00")],
      [...h.columns, serverManaged],
    );
    if (!prepared.executable) throw new Error("Expected executable plan");
    expect(prepared.plan.operations[0].sql).not.toContain("updated_at");
    const result = await executePreparedApplyPlan(h.manager, prepared.plan);
    expect(result.success).toBe(true);
    expect(
      h.transaction.mock.calls[0][2]?.verifications?.[0].values.map(
        ({ column }) => column.name,
      ),
    ).toEqual(["event_at"]);
  });
});
