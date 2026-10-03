import { afterEach, describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  executeAtomicSqlApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INT",
    nativeType: "INT",
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
    type: "INT",
    nativeType: "INT",
    category: "integer",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];
const noAuto = "STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO";
type Stage = "MODE" | "GUARD" | "INSERT" | "IDENTITY" | "VERIFY";

/** Mock only mysql2's leased connection, not planning, executor or value checks.
 * MySQL 8.4 sql-mode.html: NULL always generates; zero only without NOAUTO.
 * sql/sql_insert.cc: OK ID is this statement's generated ID or actual auto field,
 * including literal zero/BEFORE-trigger assignment, not session LAST_INSERT_ID.
 */
function harness(
  mode = "STRICT_TRANS_TABLES",
  options: {
    triggerId?: number;
    mismatch?: boolean;
    metadata?: ColumnTypeMeta[];
    packet?: unknown;
    modeResult?: unknown;
    onStage?: (stage: Stage) => void;
  } = {},
) {
  const config = { id: "f02", name: "F02", type: "mysql" as const };
  const driver = new MySQLDriver(config);
  const metadata = options.metadata ?? columns;
  const stored = new Map<number, unknown>();
  let snapshot = new Map(stored);
  let nextId = 100;
  let sessionMode = mode;
  const events: string[] = [];
  const identityReads: unknown[][] = [];
  const verifyReads: unknown[][] = [];
  const packets: unknown[] = [];
  const stage = (name: Stage) => {
    events.push(name);
    options.onStage?.(name);
  };
  const conn = {
    query: vi.fn(
      async (input: string | { sql: string; values?: unknown[] }) => {
        const sql = typeof input === "string" ? input : input.sql;
        const params = typeof input === "string" ? [] : (input.values ?? []);
        if (sql === "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE") {
          events.push("SERIALIZABLE");
          return [[]];
        }
        if (sql.startsWith("SET SESSION sql_mode")) {
          sessionMode = noAuto;
          return [{ affectedRows: 0 }];
        }
        if (sql.includes("@@SESSION.sql_mode")) {
          stage("MODE");
          return [
            [
              {
                __rapidb_sql_mode:
                  "modeResult" in options ? options.modeResult : sessionMode,
              },
            ],
          ];
        }
        if (sql.startsWith("SELECT 1 ")) {
          stage("GUARD");
          expect(sql).toContain("LIMIT 1 FOR UPDATE");
          return [stored.has(Number(params[0])) ? [{ found: 1 }] : []];
        }
        if (sql.startsWith("INSERT")) {
          stage("INSERT");
          const clause = /\(([^()]*)\) VALUES \(([^()]*)\)/.exec(sql);
          if (!clause) throw new Error("Expected positional INSERT");
          const names = [...clause[1].matchAll(/`(\w+)`/g)].map(
            (match) => match[1],
          );
          const expressions = clause[2].split(",").map((part) => part.trim());
          let bindIndex = 0;
          const inputs = Object.fromEntries(
            names.map((name, index) => [
              name,
              expressions[index] === "NULL" ? null : params[bindIndex++],
            ]),
          );
          const generates =
            inputs.id == null ||
            (Number(inputs.id) === 0 &&
              !sessionMode.split(",").includes("NO_AUTO_VALUE_ON_ZERO"));
          const id =
            options.triggerId ?? (generates ? ++nextId : Number(inputs.id));
          if (stored.has(id)) throw new Error("Duplicate entry");
          stored.set(id, options.mismatch ? 999 : inputs.amount);
          const packet = "packet" in options ? options.packet : id;
          packets.push(packet);
          return [{ affectedRows: 1, insertId: packet }];
        }
        if (sql.startsWith("UPDATE")) {
          events.push("UPDATE");
          stored.set(Number(params.at(-1)), params[0]);
          return [{ affectedRows: 1 }];
        }
        if (sql.endsWith("LIMIT 2")) {
          stage("IDENTITY");
          identityReads.push(params);
          return [
            stored.has(Number(params[0])) ? [{ __col_0: params[0] }] : [],
          ];
        }
        if (sql.startsWith("SELECT")) {
          stage("VERIFY");
          verifyReads.push(params);
          const id = Number(params[0]);
          const names = [...sql.split(" FROM ")[0].matchAll(/`(\w+)` AS/g)].map(
            (match) => match[1],
          );
          return [
            stored.has(id)
              ? [
                  Object.fromEntries(
                    names.map((name, index) => [
                      `__col_${index}`,
                      name === "id" ? id : stored.get(id),
                    ]),
                  ),
                ]
              : [],
          ];
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      },
    ),
    beginTransaction: vi.fn(async () => {
      events.push("BEGIN");
      snapshot = new Map(stored);
    }),
    commit: vi.fn(async () => {
      events.push("COMMIT");
    }),
    rollback: vi.fn(async () => {
      events.push("ROLLBACK");
      stored.clear();
      for (const [key, value] of snapshot) stored.set(key, value);
    }),
    destroy: vi.fn(),
    release: vi.fn(),
  };
  const lease = vi.fn(async () => conn);
  const poolQuery = vi.fn(async () => [
    [{ __rapidb_sql_mode: mode === noAuto ? "" : noAuto }],
  ]);
  (driver as unknown as { pool: unknown }).pool = {
    getConnection: lease,
    query: poolQuery,
  };
  const outsideQuery = vi
    .spyOn(driver, "query")
    .mockRejectedValue(new Error("No pooled verification reads"));
  vi.spyOn(driver, "getMutationAtomicityRisk").mockResolvedValue(null);
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  } as never;
  const service = new TableMutationService(manager, {
    getColumns: async () => metadata,
  });
  const prepare = (values: Record<string, unknown>) =>
    service.prepareInsertRow("f02", "db", "", "edits", values);
  const apply = () => {
    stored.set(50, 5);
    const prepared = prepareApplyChangesPlan(
      manager,
      "f02",
      "db",
      "",
      "edits",
      [{ primaryKeys: { id: 50 }, changes: { amount: 6 } }],
      metadata,
    );
    if (!prepared.executable) throw new Error("Expected executable update");
    return prepared.plan;
  };
  return {
    driver,
    manager,
    service,
    prepare,
    apply,
    stored,
    events,
    packets,
    identityReads,
    verifyReads,
    conn,
    lease,
    outsideQuery,
    poolQuery,
  };
}

afterEach(() => vi.restoreAllMocks());

describe("F02: MySQL AUTO_INCREMENT zero verification", () => {
  it.each([
    "",
    noAuto,
  ])("id-only zero INSERT remains verified (%s)", async (mode) => {
    const h = harness(mode);
    const plan = await h.prepare({ id: 0 });
    expect(plan.verification?.values).toHaveLength(1);
    await h.service.executePreparedInsertPlan(plan);
    expect(h.verifyReads).toEqual([[mode === noAuto ? 0 : 101]]);
    expect(h.events.at(-1)).toBe("COMMIT");
  });

  it.each([
    0,
    "0",
    0n,
    " +00.000 ",
  ])("single default-mode zero (%s) generates and keeps the requested baseline", async (id) => {
    const h = harness();
    const plan = await h.prepare({ id, amount: 7 });
    const baseline = structuredClone(plan);
    expect(h.conn.query).not.toHaveBeenCalled();
    await h.service.executePreparedInsertPlan(plan);
    expect(h.packets).toEqual([101]);
    expect(h.verifyReads).toEqual([[101]]);
    expect(h.stored.get(101)).toBe(7);
    expect(plan).toEqual(baseline);
    expect(h.events).not.toContain("GUARD");
    expect(h.events.at(-1)).toBe("COMMIT");
    expect(h.lease).toHaveBeenCalledOnce();
    expect(h.poolQuery).not.toHaveBeenCalled();
    expect(h.outsideQuery).not.toHaveBeenCalled();
  });

  it.each([
    0,
    "0",
    0n,
  ])("NOAUTO preserves literal zero (%s), including actual packet zero", async (id) => {
    const h = harness(noAuto);
    await h.service.executePreparedInsertPlan(
      await h.prepare({ id, amount: 7 }),
    );
    expect(h.packets).toEqual([0]);
    expect(h.identityReads).toEqual([[0]]);
    expect(h.verifyReads).toEqual([[0]]);
    expect(h.events).toEqual([
      "SERIALIZABLE",
      "BEGIN",
      "MODE",
      "GUARD",
      "INSERT",
      "IDENTITY",
      "VERIFY",
      "COMMIT",
    ]);
    expect(h.stored.get(0)).toBe(7);
    expect(h.poolQuery).not.toHaveBeenCalled();
  });

  it.each([
    "default",
    "NOAUTO",
  ])("insert-only and mixed batches resolve each operation independently (%s)", async (mode) => {
    for (const mixed of [false, true]) {
      const h = harness(mode === "NOAUTO" ? noAuto : "");
      const ids = mode === "NOAUTO" ? [0, null, 9] : [0, "0", 0n, null, 9];
      const inserts = await Promise.all(
        ids.map((id, index) => h.prepare({ id, amount: index + 10 })),
      );
      const baseline = structuredClone(inserts);
      const result = await executeAtomicSqlApplyPlan(
        h.manager,
        mixed ? h.apply() : null,
        inserts,
      );
      expect(result).toMatchObject({ success: true, insertApplied: true });
      expect(h.verifyReads).toEqual([
        ...(mode === "NOAUTO"
          ? [[0], [101], [9]]
          : [[101], [102], [103], [104], [9]]),
        ...(mixed ? [[50]] : []),
      ]);
      expect(inserts).toEqual(baseline);
      if (mixed) expect(h.stored.get(50)).toBe(6);
      expect(h.events.at(-1)).toBe("COMMIT");
    }
  });

  it.each([
    "",
    noAuto,
  ])("NULL/omitted keys always generate and explicit nonzero remains verified (%s)", async (mode) => {
    const h = harness(mode);
    const inserts = await Promise.all(
      [{ id: null, amount: 1 }, { amount: 2 }, { id: 9, amount: 3 }].map(
        h.prepare,
      ),
    );
    expect(
      await executeAtomicSqlApplyPlan(h.manager, null, inserts),
    ).toMatchObject({ success: true });
    expect(h.packets).toEqual([101, 102, 9]);
    expect(h.events).not.toContain("MODE");
    expect(h.events).not.toContain("SERIALIZABLE");
  });

  it.each([
    "single",
    "batch",
    "mixed",
  ])("never skips literal-zero or non-key value mismatch (%s)", async (kind) => {
    for (const mode of ["", noAuto]) {
      const h = harness(mode, { mismatch: true });
      const plan = await h.prepare({ id: 0, amount: 7 });
      if (kind === "single")
        await expect(h.service.executePreparedInsertPlan(plan)).rejects.toThrow(
          /INSERT verification failed/,
        );
      else {
        const result = await executeAtomicSqlApplyPlan(
          h.manager,
          kind === "mixed" ? h.apply() : null,
          [plan],
        );
        expect(result.success).toBe(false);
        expect(result.insertApplied).not.toBe(true);
        if (kind === "mixed") {
          expect(result.rowOutcomes).toMatchObject([
            { status: "skipped", success: false },
          ]);
          expect(h.stored.get(50)).toBe(5);
        }
      }
      expect(h.events.at(-1)).toBe("ROLLBACK");
      expect(h.stored.has(0)).toBe(false);
      expect(h.stored.has(101)).toBe(false);
    }
  });

  it("NOAUTO rejects a trigger-redirected zero even with compatible other values", async () => {
    const h = harness(noAuto, { triggerId: 900 });
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 0, amount: 7 }),
      ),
    ).rejects.toThrow(/INSERT verification failed/);
    expect(h.identityReads).toEqual([[900]]);
    expect(h.events.at(-1)).toBe("ROLLBACK");
  });

  it("an explicit nonzero PK still rejects a compatible trigger redirect", async () => {
    const h = harness("", { triggerId: 900 });
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 9, amount: 7 }),
      ),
    ).rejects.toThrow(/INSERT verification failed/);
    expect(h.identityReads).toEqual([[900]]);
    expect(h.events).not.toContain("MODE");
    expect(h.events.at(-1)).toBe("ROLLBACK");
  });

  it.each([
    false,
    true,
  ])("default zero verifies the actual trigger key, never a stale requested-key row (mismatch=%s)", async (mismatch) => {
    const h = harness("", { triggerId: 900, mismatch });
    h.stored.set(0, 7);
    const pending = h.service.executePreparedInsertPlan(
      await h.prepare({ id: 0, amount: 7 }),
    );
    if (mismatch)
      await expect(pending).rejects.toThrow(/INSERT verification failed/);
    else await pending;
    expect(h.verifyReads).toEqual([[900]]);
    expect(h.stored.get(0)).toBe(7);
    expect(h.events.at(-1)).toBe(mismatch ? "ROLLBACK" : "COMMIT");
  });

  it("NOAUTO's pre-DML guard blocks stale-old-zero success despite a redirecting trigger", async () => {
    const h = harness(noAuto, { triggerId: 900, packet: 0 });
    h.stored.set(0, 7);
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 0, amount: 7 }),
      ),
    ).rejects.toThrow(/already existing primary key/);
    expect(h.events).toEqual([
      "SERIALIZABLE",
      "BEGIN",
      "MODE",
      "GUARD",
      "ROLLBACK",
    ]);
    expect(h.stored.get(0)).toBe(7);
  });

  it("packet zero without a literal-zero request still fails closed", async () => {
    const h = harness("", { packet: 0 });
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 0, amount: 7 }),
      ),
    ).rejects.toThrow(/identity could not be captured reliably/);
    expect(h.identityReads).toEqual([]);
    expect(h.events.at(-1)).toBe("ROLLBACK");
  });

  it("reads session mode per operation, not once per batch or from planning/global defaults", async () => {
    const h = harness("");
    const first = await h.prepare({ id: 0, amount: 7 });
    const second = await h.prepare({ id: "0", amount: 8 });
    if (!first.verification || !second.verification?.identity)
      throw new Error("Expected verification");
    await h.driver.runTransaction(
      [
        first.operation,
        { sql: `SET SESSION sql_mode = '${noAuto}'` },
        second.operation,
      ],
      undefined,
      {
        verifications: [
          first.verification,
          {
            ...second.verification,
            identity: { ...second.verification.identity, operationIndex: 2 },
          },
        ],
      },
    );
    expect(h.packets).toEqual([101, 0]);
    expect(h.verifyReads).toEqual([[101], [0]]);
    expect(h.events.filter((event) => event === "MODE")).toHaveLength(2);
    expect(h.poolQuery).not.toHaveBeenCalled();
  });

  it.each([
    null,
    undefined,
    0,
  ])("unreadable session mode fails closed before DML (%s)", async (modeResult) => {
    const h = harness("", { modeResult });
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 0, amount: 7 }),
      ),
    ).rejects.toThrow(/cannot determine.*SQL mode/);
    expect(h.events).not.toContain("INSERT");
    expect(h.events.at(-1)).toBe("ROLLBACK");
  });

  it.each([
    "MODE",
    "GUARD",
    "INSERT",
    "IDENTITY",
    "VERIFY",
  ] as const)("cancellation after %s fences every further write/commit", async (cancelAt) => {
    const controller = new AbortController();
    const h = harness(noAuto, {
      onStage: (stage) => {
        if (stage === cancelAt) controller.abort(new Error("cancelled F02"));
      },
    });
    const plan = await h.prepare({ id: 0, amount: 7 });
    await expect(
      h.driver.runTransaction(
        [plan.operation],
        { signal: controller.signal, deadline: Date.now() + 10000 },
        { verifications: plan.verification ? [plan.verification] : [] },
      ),
    ).rejects.toThrow(/cancelled F02/);
    expect(h.events.at(-1)).toBe("ROLLBACK");
    expect(h.events.at(-2)).toBe(cancelAt);
    expect(h.conn.destroy).toHaveBeenCalledOnce();
    expect(h.conn.release).not.toHaveBeenCalled();
    expect(h.stored.size).toBe(0);
  });

  it("does not interpret an ordinary non-auto zero PK as generated", async () => {
    const metadata = columns.map((column) => ({
      ...column,
      identityGeneration: undefined,
    }));
    const h = harness(noAuto, { metadata });
    const plan = await h.prepare({ id: 0, amount: 7 });
    expect(plan.operation.captureIdentity?.mysqlInsertId).toBeUndefined();
    await h.service.executePreparedInsertPlan(plan);
    expect(h.verifyReads).toEqual([[0]]);
    expect(h.events).not.toContain("MODE");
    expect(h.events).toContain("GUARD");
  });

  it("filters unknown inputs before zero generation planning", async () => {
    const h = harness();
    const plan = await h.prepare({ unknown: 0, amount: 7 });
    expect(plan.operation.params).toEqual([7]);
    expect(plan.operation.captureIdentity?.mysqlInsertId?.zero).toBeUndefined();
    expect(plan.verification?.values.map((value) => value.column.name)).toEqual(
      ["amount"],
    );
  });

  it("only AUTO_INCREMENT primary keys receive zero generation indicators", async () => {
    const h = harness("", {
      metadata: [
        { ...columns[0], isPrimaryKey: false },
        { ...columns[1], isPrimaryKey: true },
      ],
    });
    const plan = await h.prepare({ id: 0, amount: 7 });
    expect(plan.operation.captureIdentity?.mysqlInsertId).toBeUndefined();
    expect(plan.operation.captureIdentity?.mysqlKeyGuard).toBeDefined();
    expect(plan.verification?.values[0].expectedValue).toBe(0);
  });

  it.each([
    0,
    "0",
  ])("literal zero supports official numeric/string OK packet ID %s", async (packet) => {
    const h = harness(noAuto, { packet });
    await h.service.executePreparedInsertPlan(
      await h.prepare({ id: "0", amount: 7 }),
    );
    expect(h.identityReads).toEqual([[packet]]);
    expect(h.events.at(-1)).toBe("COMMIT");
  });

  it("a reusable plan resolves zero afresh after its connection changes mode", async () => {
    const h = harness("");
    const plan = await h.prepare({ id: 0, amount: 7 });
    await h.service.executePreparedInsertPlan(plan);
    await h.conn.query(`SET SESSION sql_mode = '${noAuto}'`);
    await h.service.executePreparedInsertPlan(plan);
    expect(h.verifyReads).toEqual([[101], [0]]);
    expect(plan.verification?.values[0].expectedValue).toBe(0);
  });

  it.each([
    "",
    noAuto,
  ])("a composite PK resolves the zero key at its actual parameter position (%s)", async (mode) => {
    const tenant = {
      ...columns[0],
      name: "tenant",
      identityGeneration: undefined,
    };
    const h = harness(mode, { metadata: [tenant, ...columns] });
    const plan = await h.prepare({ id: 0, amount: 7 });
    expect(plan.operation.captureIdentity?.mysqlInsertId?.zero?.keyIndex).toBe(
      1,
    );
    h.conn.query.mockResolvedValueOnce([[]]); // SERIALIZABLE
    h.conn.query.mockResolvedValueOnce([[{ __rapidb_sql_mode: mode }]]);
    if (mode === noAuto) h.conn.query.mockResolvedValueOnce([[]]); // guard
    const actualId = mode === noAuto ? 0 : 101;
    h.conn.query.mockResolvedValueOnce([
      { affectedRows: 1, insertId: actualId },
    ]);
    h.conn.query.mockResolvedValueOnce([[{ __col_0: 42, __col_1: actualId }]]);
    h.conn.query.mockResolvedValueOnce([[{ __col_0: actualId, __col_1: 7 }]]);
    await h.service.executePreparedInsertPlan(plan);
    expect(h.conn.query.mock.calls.at(-1)?.[0]).toMatchObject({
      values: [42, actualId],
    });
    expect(h.events.at(-1)).toBe("COMMIT");
  });

  it.each([
    "",
    noAuto,
  ])("ambiguous composite identities fail closed, including literal packet zero (%s)", async (mode) => {
    const tenant = {
      ...columns[0],
      name: "tenant",
      identityGeneration: undefined,
    };
    const h = harness(mode, { metadata: [...columns, tenant] });
    const plan = await h.prepare({ id: 0, amount: 7 });
    h.conn.query.mockResolvedValueOnce([[]]);
    h.conn.query.mockResolvedValueOnce([[{ __rapidb_sql_mode: mode }]]);
    if (mode === noAuto) h.conn.query.mockResolvedValueOnce([[]]);
    const actualId = mode === noAuto ? 0 : 101;
    h.conn.query.mockResolvedValueOnce([
      { affectedRows: 1, insertId: actualId },
    ]);
    h.conn.query.mockResolvedValueOnce([
      [
        { __col_0: actualId, __col_1: 1 },
        { __col_0: actualId, __col_1: 2 },
      ],
    ]);
    await expect(h.service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /identity could not be captured reliably/,
    );
    expect(h.events.at(-1)).toBe("ROLLBACK");
  });

  it("a failed session-mode read rolls back without DML", async () => {
    const h = harness();
    const plan = await h.prepare({ id: 0, amount: 7 });
    h.conn.query.mockResolvedValueOnce([[]]);
    h.conn.query.mockRejectedValueOnce(new Error("mode read failed"));
    await expect(h.service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /mode read failed/,
    );
    expect(h.events).not.toContain("INSERT");
    expect(h.events.at(-1)).toBe("ROLLBACK");
  });

  it("unknown primary-key metadata still fails closed before leasing/writing", async () => {
    const h = harness("", { metadata: [columns[1]] });
    await expect(h.prepare({ id: 0, amount: 7 })).rejects.toThrow(
      /requires a reliable primary key/,
    );
    expect(h.lease).not.toHaveBeenCalled();
    expect(h.events).toEqual([]);
  });
});
