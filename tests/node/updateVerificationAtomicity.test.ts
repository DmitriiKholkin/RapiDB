import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type {
  PreparedApplyPlan,
  PreparedInsertPlan,
  RowUpdate,
} from "../../src/extension/table/tableDataContracts";
import {
  applyChangesTransactional,
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INT",
    nativeType: "INT",
    category: "integer",
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
    nativeType: "DECIMAL(10,2)",
    category: "decimal",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];
const riskMessage =
  "Table target.unsafe_rows uses non-transactional MySQL engine MYISAM.";

function harness(risk: string | null = riskMessage, mismatch = true) {
  const driver = new MySQLDriver({
    id: "f01",
    name: "F01",
    type: "mysql",
    database: "target",
  });
  const events: string[] = [];
  const stored = new Map<number, unknown>();
  const query = vi.fn(async (input: { sql: string; values?: unknown[] }) => {
    const params = input.values ?? [];
    if (input.sql.startsWith("UPDATE")) {
      events.push("UPDATE");
      stored.set(Number(params[1]), mismatch ? "999.00" : params[0]);
      return [{ affectedRows: 1 }];
    }
    if (input.sql.startsWith("INSERT")) {
      events.push("INSERT");
      stored.set(3, "inserted");
      return [{ affectedRows: 1 }];
    }
    if (input.sql.startsWith("SELECT")) {
      events.push("VERIFY");
      return [[{ __col_0: stored.get(Number(params[0])) }]];
    }
    throw new Error(`Unexpected SQL: ${input.sql}`);
  });
  const lease = vi.fn(async () => ({
    query,
    beginTransaction: async () => {
      events.push("BEGIN");
    },
    commit: async () => {
      events.push("COMMIT");
    },
    rollback: async () => {
      // Deliberately ineffective for known risk: model MyISAM's partial writes.
      events.push("ROLLBACK");
      if (risk === null) stored.clear();
    },
    release: vi.fn(),
  }));
  (driver as unknown as { pool: unknown }).pool = { getConnection: lease };
  const riskCheck = vi
    .spyOn(driver, "getMutationAtomicityRisk")
    .mockResolvedValue(risk);
  const transaction = vi.spyOn(driver, "runTransaction");
  const connection = { id: "f01", readOnly: false };
  const manager = {
    getConnection: () => connection,
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  function prepare(
    updates: RowUpdate[],
    metadata = columns,
  ): PreparedApplyPlan {
    const prepared = prepareApplyChangesPlan(
      manager,
      "f01",
      "target",
      "ignored_schema",
      "unsafe_rows",
      updates,
      metadata,
    );
    if (!prepared.executable)
      throw new Error("Expected an executable UPDATE plan");
    return prepared.plan;
  }
  return {
    driver,
    events,
    stored,
    lease,
    riskCheck,
    transaction,
    manager,
    connection,
    prepare,
  };
}

const update = (id: number): RowUpdate => ({
  primaryKeys: { id },
  changes: { amount: "12.34" },
});
const insert: PreparedInsertPlan = {
  connectionId: "f01",
  database: "target",
  schema: "ignored_schema",
  table: "other_rows",
  operation: { sql: "INSERT INTO other_rows VALUES (?)", params: ["inserted"] },
  previewStatements: [],
  verificationCriteria: null,
};
const paths = ["prepared", "atomic"] as const;
function execute(
  path: (typeof paths)[number],
  manager: ConnectionManager,
  plan: PreparedApplyPlan,
  inserts: PreparedInsertPlan[] = [],
) {
  return path === "prepared"
    ? executePreparedApplyPlan(manager, plan)
    : executeAtomicSqlApplyPlan(manager, plan, inserts);
}

afterEach(() => vi.restoreAllMocks());

describe("verified UPDATE rollback preflight", () => {
  for (const path of paths) {
    it.each([
      1, 2,
    ])(`${path}: rejects %i verified UPDATE(s) before DML on MyISAM`, async (count) => {
      const h = harness();
      const plan = h.prepare(
        Array.from({ length: count }, (_, index) => update(index + 1)),
      );
      expect(
        plan.verificationTargets.every((target) => target.values.length > 0),
      ).toBe(true);
      const result = await execute(path, h.manager, plan);
      // Include the result in BEFORE failure evidence: rollback cannot undo writes.
      expect({ stored: [...h.stored], events: h.events, result }).toMatchObject(
        {
          stored: [],
          events: [],
          result: {
            success: false,
            error: expect.stringContaining(
              "UPDATE verification requires rollback support",
            ),
          },
        },
      );
      expect(h.riskCheck).toHaveBeenCalledExactlyOnceWith(
        "target",
        "ignored_schema",
        "unsafe_rows",
      );
      expect(h.transaction).not.toHaveBeenCalled();
      expect(h.lease).not.toHaveBeenCalled();
      expect(result.rowOutcomes).toEqual(
        plan.updates.map((_, rowIndex) =>
          expect.objectContaining({
            rowIndex,
            success: false,
            status: "skipped",
          }),
        ),
      );
    });

    it(`${path}: fails closed if UPDATE risk metadata cannot be read`, async () => {
      const h = harness();
      h.riskCheck.mockRejectedValue(new Error("Engine metadata unavailable"));
      const result = await execute(
        path,
        h.manager,
        h.prepare([update(1)]),
        path === "atomic" ? [insert] : [],
      );
      expect(result).toMatchObject({
        success: false,
        error: expect.stringContaining("Engine metadata unavailable"),
      });
      expect(result.error).not.toContain("the transaction was not committed");
      expect(result.rowOutcomes).toEqual([
        expect.objectContaining({
          rowIndex: 0,
          success: false,
          status: "skipped",
        }),
      ]);
      expect(h.transaction).not.toHaveBeenCalled();
      expect(h.events).toEqual([]);
      expect([...h.stored]).toEqual([]);
      expect(result.insertApplied).not.toBe(true);
    });

    it(`${path}: read-only rejection precedes risk metadata and DML`, async () => {
      const h = harness();
      const plan = h.prepare([update(1)]);
      h.connection.readOnly = true;
      const result = await execute(path, h.manager, plan);
      expect(result).toMatchObject({
        success: false,
        error: expect.stringMatching(/read.only/i),
      });
      expect(h.riskCheck).not.toHaveBeenCalled();
      expect(h.transaction).not.toHaveBeenCalled();
    });

    it.each([
      "safe",
      "unreported",
    ] as const)(`${path}: preserves verified UPDATE execution when rollback risk is %s`, async (support) => {
      const h = harness(null, false);
      if (support === "unreported") {
        Object.defineProperty(h.driver, "getMutationAtomicityRisk", {
          value: undefined,
        });
      }
      const plan = h.prepare([update(1), update(2)]);
      const result = await execute(path, h.manager, plan);
      expect(result).toMatchObject({
        success: true,
        rowOutcomes: [
          { rowIndex: 0, success: true, status: "applied" },
          { rowIndex: 1, success: true, status: "applied" },
        ],
      });
      expect(h.transaction).toHaveBeenCalledExactlyOnceWith(
        plan.operations,
        undefined,
        expect.objectContaining({
          database: "target",
          verifications: expect.any(Array),
        }),
      );
      expect(h.events).toEqual([
        "BEGIN",
        "UPDATE",
        "UPDATE",
        "VERIFY",
        "VERIFY",
        "COMMIT",
      ]);
      if (support === "safe") expect(h.riskCheck).toHaveBeenCalledTimes(1);
      else expect(h.riskCheck).not.toHaveBeenCalled();
    });

    it(`${path}: preserves verification failure outcomes for rollback-capable tables`, async () => {
      const h = harness(null);
      const result = await execute(path, h.manager, h.prepare([update(1)]));
      expect(result).toMatchObject({
        success: false,
        failedRows: [0],
        rowOutcomes: [
          {
            rowIndex: 0,
            success: false,
            status: "verification_failed",
            columns: ["amount"],
          },
        ],
      });
      expect(h.riskCheck).toHaveBeenCalledTimes(1);
      expect(h.events).toEqual(["BEGIN", "UPDATE", "VERIFY", "ROLLBACK"]);
      expect([...h.stored]).toEqual([]);
    });

    it(`${path}: does not consult rollback risk when UPDATE has no verification`, async () => {
      const h = harness();
      const temporalColumns: ColumnTypeMeta[] = [
        columns[0],
        { ...columns[1], nativeType: "TIMESTAMP", category: "datetime" },
      ];
      const plan = h.prepare(
        [
          {
            primaryKeys: { id: 1 },
            changes: { amount: "2026-10-02 12:00:00" },
          },
        ],
        temporalColumns,
      );
      expect(plan.verificationTargets[0].values).toEqual([]);
      const result = await execute(path, h.manager, plan);
      expect(result.success).toBe(true);
      expect(h.riskCheck).not.toHaveBeenCalled();
      expect(h.transaction).toHaveBeenCalledExactlyOnceWith(
        plan.operations,
        undefined,
        { database: "target" },
      );
      expect(h.events).toEqual(["BEGIN", "UPDATE", "COMMIT"]);
    });

    it(`${path}: preserves skipped rows while blocking remaining verified UPDATEs`, async () => {
      const h = harness();
      const plan = h.prepare([
        { primaryKeys: { id: 1 }, changes: { amount: undefined } },
        update(2),
      ]);
      expect(plan.skippedRows).toEqual([0]);
      const result = await execute(path, h.manager, plan);
      expect(result.success).toBe(false);
      expect(result.rowOutcomes?.map((outcome) => outcome.status)).toEqual([
        "skipped",
        "skipped",
      ]);
      expect(result.rowOutcomes?.[1].success).toBe(false);
      expect(h.riskCheck).toHaveBeenCalledTimes(1);
      expect(h.transaction).not.toHaveBeenCalled();
    });
  }

  it("atomic: rejects an unsafe verified UPDATE before even an unverified INSERT on another table", async () => {
    const h = harness();
    const result = await executeAtomicSqlApplyPlan(
      h.manager,
      h.prepare([update(1)]),
      [insert],
    );
    expect({ stored: [...h.stored], events: h.events, result }).toMatchObject({
      stored: [],
      events: [],
      result: {
        success: false,
        error: expect.stringContaining(
          "UPDATE verification requires rollback support",
        ),
      },
    });
    expect(h.riskCheck).toHaveBeenCalledExactlyOnceWith(
      "target",
      "ignored_schema",
      "unsafe_rows",
    );
    expect(h.transaction).not.toHaveBeenCalled();
    expect(result.insertApplied).not.toBe(true);
  });

  it("does not inspect rollback metadata for an entirely skipped UPDATE plan", async () => {
    const h = harness();
    const result = await applyChangesTransactional(
      h.manager,
      "f01",
      "target",
      "ignored_schema",
      "unsafe_rows",
      [{ primaryKeys: { id: 1 }, changes: { amount: undefined } }],
      columns,
    );
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ rowIndex: 0, success: true, status: "skipped" }],
    });
    expect(h.riskCheck).not.toHaveBeenCalled();
    expect(h.transaction).not.toHaveBeenCalled();
  });

  it("atomic: preserves verified INSERT risk checks on the INSERT table after a safe UPDATE preflight", async () => {
    const h = harness();
    h.riskCheck.mockImplementation(async (_database, _schema, table) =>
      table === "other_rows" ? riskMessage : null,
    );
    const result = await executeAtomicSqlApplyPlan(
      h.manager,
      h.prepare([update(1)]),
      [
        {
          ...insert,
          verification: {
            mutation: "insert",
            rowIndex: 0,
            sql: "SELECT amount FROM other_rows WHERE id = ?",
            params: [3],
            values: [{ column: columns[1], expectedValue: "12.34" }],
          },
        },
      ],
    );
    expect(result).toMatchObject({
      success: false,
      error: expect.stringContaining(
        "INSERT verification requires rollback support",
      ),
    });
    expect(h.riskCheck.mock.calls).toEqual([
      ["target", "ignored_schema", "unsafe_rows"],
      ["target", "ignored_schema", "other_rows"],
    ]);
    expect(h.transaction).not.toHaveBeenCalled();
    expect(h.events).toEqual([]);
  });
});
