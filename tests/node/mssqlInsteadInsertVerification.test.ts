import mssql from "mssql";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  executeAtomicSqlApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "int",
    nativeType: "int",
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
    type: "decimal(20,4)",
    nativeType: "decimal(20,4)",
    category: "decimal",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];
type Stage = "GUARD" | "INSERT" | "VERIFY";
type Trigger = { instead: boolean; disabled: boolean; event: string };
type CatalogObject = {
  schema: string;
  table: string;
  objectId: number;
  trigger?: Trigger;
};

/** Only the node-mssql transaction/request boundary is mocked. SQL Server's
 * OUTPUT documentation, "Triggers": with INSTEAD OF, OUTPUT is generated as if
 * DML occurred, even when the trigger makes no modifications. The preexisting
 * row therefore passes ordinary read-back despite this INSERT doing no DML.
 * https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql#triggers
 */
function harness(
  options: {
    trigger?: Trigger;
    database?: string;
    schema?: string;
    table?: string;
    objectType?: "U" | "V";
    metadata?: ColumnTypeMeta[];
    permission?: number | null;
    guardError?: boolean;
    blockedTable?: string;
    mismatch?: boolean;
    onStage?: (stage: Stage) => void;
    catalog?: CatalogObject[];
  } = {},
) {
  const database = options.database ?? "chosen";
  const schema = options.schema ?? "dbo";
  const table = options.table ?? "edits";
  const config = {
    id: "f03",
    name: "F03",
    type: "mssql" as const,
    database: "other",
  };
  const driver = new MSSQLDriver(config);
  const metadata = options.metadata ?? columns;
  const stored = new Map<unknown, unknown>([
    [1, "1.25"],
    [50, "5"],
  ]);
  let snapshot = new Map(stored);
  const events: string[] = [];
  const statements: string[] = [];
  const reads: unknown[][] = [];
  const outputs: unknown[][] = [];
  const owners: unknown[] = [];
  const guardInputs: Array<Array<[string, unknown, unknown]>> = [];
  const lookupIds: Array<number | undefined> = [];
  const stage = (name: Stage) => {
    events.push(name);
    options.onStage?.(name);
  };
  const unsafeTrigger =
    options.trigger?.instead &&
    !options.trigger.disabled &&
    options.trigger.event === "INSERT";
  vi.spyOn(mssql.Transaction.prototype, "begin").mockImplementation(
    (async () => {
      events.push("BEGIN");
      snapshot = new Map(stored);
    }) as never,
  );
  vi.spyOn(mssql.Transaction.prototype, "commit").mockImplementation(
    (async () => {
      events.push("COMMIT");
    }) as never,
  );
  vi.spyOn(mssql.Transaction.prototype, "rollback").mockImplementation(
    (async () => {
      events.push("ROLLBACK");
      stored.clear();
      for (const [key, value] of snapshot) stored.set(key, value);
    }) as never,
  );
  vi.spyOn(mssql.Transaction.prototype, "request").mockImplementation(function (
    this: mssql.Transaction,
  ) {
    owners.push(this);
    const params: unknown[] = [];
    const inputs: Array<[string, unknown, unknown]> = [];
    return {
      input: vi.fn((name, type, value) => {
        inputs.push([name, type, value]);
        params.push(value);
      }),
      output: vi.fn(),
      cancel: vi.fn(),
      query: vi.fn(async (sql: string) => {
        statements.push(sql);
        if (sql.includes("sys.trigger_events")) {
          stage("GUARD");
          guardInputs.push(inputs);
          const actualTable = String(params[1]);
          // Validate executed SQL, not a mocked guard function or planning read.
          expect(sql).toContain(`USE ${driver.quoteIdentifier(database)};`);
          expect(sql).toContain("sys.triggers");
          expect(sql).toContain("is_instead_of_trigger = 1");
          expect(sql).toContain("is_disabled = 0");
          expect(sql).toContain("type_desc = 'INSERT'");
          expect(sql).toContain("o.type IN ('U', 'V')");
          expect(sql).toContain("HAS_PERMS_BY_NAME");
          expect(sql).toContain("VIEW DEFINITION");
          expect(sql).toContain(
            "ISNULL(HAS_PERMS_BY_NAME(@__rapidb_name, 'OBJECT', 'VIEW DEFINITION'), 0) <> 1",
          );
          expect(sql).toContain(
            "THROW 50001, 'INSERT verification does not support enabled INSTEAD OF INSERT triggers",
          );
          expect(sql).toContain("object_id = @__rapidb_target AND type = 'V'");
          expect(sql).toContain("TABLOCKX, HOLDLOCK");
          expect(sql).toContain("OPTION (EXPAND VIEWS)");
          expect(sql).toContain(
            driver.qualifiedTableName(
              database,
              schema,
              options.catalog ? table : actualTable,
            ),
          );
          if (!options.catalog) expect(params).toContain(schema);
          if (options.guardError) throw new Error("catalog query denied");
          if ("permission" in options && options.permission !== 1)
            throw new Error(
              "INSERT verification cannot inspect trigger metadata; grant VIEW DEFINITION",
            );
          expect(sql.indexOf("TABLOCKX, HOLDLOCK")).toBeLessThan(
            sql.indexOf("SET @__rapidb_target"),
          );
          expect(sql).toContain("t.parent_id = @__rapidb_target");
          expect(sql).toContain("t.parent_class = 1");
          expect(params[2]).toBe(
            `${driver.quoteIdentifier(schema)}.${driver.quoteIdentifier(options.catalog ? table : actualTable)}`,
          );
          // Evaluate object lookup using the actual bound schema/table, independently
          // of the bracket-quoted lock target and permission object's original name.
          const found = options.catalog?.find(
            (object) =>
              object.schema === params[0] && object.table === params[1],
          );
          if (options.catalog) {
            lookupIds.push(found?.objectId);
            if (!found)
              throw new Error(
                "INSERT verification cannot inspect trigger metadata",
              );
          }
          const lookupTrigger = options.catalog
            ? found?.trigger
            : options.trigger;
          const unsafeLookup =
            lookupTrigger?.instead &&
            !lookupTrigger.disabled &&
            lookupTrigger.event === "INSERT";
          if (
            unsafeLookup &&
            (!options.blockedTable || params[1] === options.blockedTable)
          )
            throw new Error(
              "INSERT verification does not support enabled INSTEAD OF INSERT triggers; use an explicit SQL transaction",
            );
          if (options.objectType === "V")
            throw new Error(
              "INSERT verification cannot reliably pin MSSQL view trigger metadata; insert into the base table",
            );
          return { recordset: [] };
        }
        if (sql.includes("OUTPUT INSERTED")) {
          stage("INSERT");
          expect(sql).toContain(" INTO @__rapidb_identity");
          expect(sql).toContain("SET @__rapidb_affected_rows = @@ROWCOUNT");
          const id = sql.includes("([id], [amount])") ? params[0] : 101;
          const amount = sql.includes("VALUES (@p1, NULL)")
            ? null
            : params.at(-1);
          if (
            !unsafeTrigger ||
            (options.blockedTable &&
              !sql.includes(
                driver.qualifiedTableName(
                  database,
                  schema,
                  options.blockedTable,
                ),
              ))
          ) {
            if (stored.has(id)) throw new Error("duplicate key");
            stored.set(id, options.mismatch ? "999" : amount);
          }
          outputs.push([id]);
          return {
            // Audit trigger resultset/count must never be treated as identity.
            recordset: [{ __col_0: 999 }],
            recordsets: [[{ __col_0: 999 }], [{ __col_0: id }]],
            rowsAffected: [1, 7],
            output: { __rapidb_affected_rows: 1 },
          };
        }
        if (sql.startsWith("UPDATE")) {
          events.push("UPDATE");
          stored.set(params.at(-1), params[0]);
          return { output: { __rapidb_affected_rows: 1 } };
        }
        if (sql.startsWith("SELECT")) {
          stage("VERIFY");
          reads.push(params);
          const id = params[0];
          const names = [
            ...sql.split(" FROM ")[0].matchAll(/\[(id|amount)\] AS/g),
          ].map((match) => match[1]);
          const values = names.map((name) =>
            name === "id" ? id : stored.get(id),
          );
          return {
            recordset: stored.has(id) ? [values] : [],
            columns: [names.map((_, index) => ({ name: `__col_${index}` }))],
          };
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      }),
    } as never;
  });
  (driver as unknown as { pool: unknown }).pool = { config: { options: {} } };
  const outsideQuery = vi
    .spyOn(driver, "query")
    .mockRejectedValue(new Error("No pooled reads"));
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  } as never;
  const service = new TableMutationService(manager, {
    getColumns: async () => metadata,
  });
  const prepare = (values: Record<string, unknown>, target = table) =>
    service.prepareInsertRow("f03", database, schema, target, values);
  const apply = () => {
    const prepared = prepareApplyChangesPlan(
      manager,
      "f03",
      database,
      schema,
      table,
      [{ primaryKeys: { id: 50 }, changes: { amount: "6" } }],
      metadata,
    );
    if (!prepared.executable) throw new Error("Expected update plan");
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
    statements,
    reads,
    outputs,
    owners,
    outsideQuery,
    guardInputs,
    lookupIds,
  };
}

afterEach(() => vi.restoreAllMocks());

describe("F03: MSSQL INSTEAD OF INSERT fabricated OUTPUT", () => {
  it.each([
    ["single", "table"],
    ["batch", "table"],
    ["mixed", "table"],
    ["single", "schema"],
    ["batch", "schema"],
  ])("rejects a normalized decoy lookup (%s, %s)", async (kind, part) => {
    const schema = part === "schema" ? " 2026-10-03" : "dbo";
    const table = part === "table" ? " 2026-10-03" : "edits";
    const trigger = { instead: true, disabled: false, event: "INSERT" };
    const h = harness({
      schema,
      table,
      trigger,
      catalog: [
        { schema, table, objectId: 1, trigger },
        { schema: schema.trim(), table: table.trim(), objectId: 2 },
      ],
    });
    const plan = await h.prepare({ id: 1, amount: "1.25" });
    if (kind === "single") {
      await expect(h.service.executePreparedInsertPlan(plan)).rejects.toThrow(
        /INSTEAD OF INSERT/,
      );
    } else {
      const result = await executeAtomicSqlApplyPlan(
        h.manager,
        kind === "mixed" ? h.apply() : null,
        [plan],
      );
      expect(result.success).toBe(false);
      expect(result.insertApplied).not.toBe(true);
    }
    expect(h.lookupIds).toEqual([1]);
    expect(h.events).toEqual(["BEGIN", "GUARD", "ROLLBACK"]);
    expect(h.stored.get(1)).toBe("1.25");
    expect(h.stored.get(50)).toBe("5");
  });

  it.each([
    " 2026-10-03",
    "2026-10-03 ",
    " 2026-10-03 ",
    "88B8A2E9-3D83-4C5E-A00F-B178B1DE4278",
    " 88B8A2E9-3D83-4C5E-A00F-B178B1DE4278 ",
    "name]; DROP TABLE decoy; -- ? ' 名",
    "表".repeat(128),
    "]".repeat(128),
  ])("binds raw schema/table names only as NVarChar (%s)", async (identifier) => {
    const database = " db]';--?2026-10-03 ";
    const h = harness({
      database,
      schema: identifier,
      table: identifier,
      catalog: [{ schema: identifier, table: identifier, objectId: 1 }],
    });
    await h.service.executePreparedInsertPlan(
      await h.prepare({ id: 2, amount: "1.25" }),
    );
    expect(h.guardInputs).toEqual([
      [
        ["p1", mssql.NVarChar(128), identifier],
        ["p2", mssql.NVarChar(128), identifier],
        [
          "p3",
          mssql.NVarChar(517),
          `${h.driver.quoteIdentifier(identifier)}.${h.driver.quoteIdentifier(identifier)}`,
        ],
      ],
    ]);
    expect(h.lookupIds).toEqual([1]);
    expect(h.statements[0]).toContain(
      `USE ${h.driver.quoteIdentifier(database)};`,
    );
    expect(h.events.at(-1)).toBe("COMMIT");
  });

  it.each([
    "single",
    "batch",
    "mixed",
  ])("rejects old-row/no-op-trigger false success (%s)", async (kind) => {
    const h = harness({
      trigger: { instead: true, disabled: false, event: "INSERT" },
    });
    const plan = await h.prepare({ id: 1, amount: "1.25" });
    if (kind === "single")
      await expect(h.service.executePreparedInsertPlan(plan)).rejects.toThrow(
        /INSTEAD OF INSERT/,
      );
    else {
      const result = await executeAtomicSqlApplyPlan(
        h.manager,
        kind === "mixed" ? h.apply() : null,
        [plan],
      );
      expect(result.success).toBe(false);
      expect(result.insertApplied).not.toBe(true);
      if (kind === "mixed")
        expect(result.rowOutcomes).toMatchObject([
          { success: false, status: "skipped" },
        ]);
    }
    expect(h.events).not.toContain("INSERT");
    expect(h.events.at(-1)).toBe("ROLLBACK");
    expect(h.stored.get(1)).toBe("1.25");
    expect(h.stored.get(50)).toBe("5");
  });

  it.each([
    undefined,
    { instead: false, disabled: false, event: "INSERT" },
    { instead: true, disabled: true, event: "INSERT" },
    { instead: true, disabled: false, event: "UPDATE" },
    { instead: true, disabled: false, event: "DELETE" },
  ])("supports reliable table OUTPUT with trigger %j", async (trigger) => {
    const h = harness({ trigger });
    const plan = await h.prepare({ id: 2, amount: "1.25" });
    const baseline = structuredClone(plan);
    await h.service.executePreparedInsertPlan(plan);
    expect(h.events).toEqual(["BEGIN", "GUARD", "INSERT", "VERIFY", "COMMIT"]);
    expect(h.outputs).toEqual([[2]]);
    expect(h.reads).toEqual([[2]]);
    expect(h.stored.get(2)).toBe("1.25");
    expect(plan).toEqual(baseline);
    expect(new Set(h.owners).size).toBe(1);
    expect(h.outsideQuery).not.toHaveBeenCalled();
  });

  it.each([
    false,
    true,
  ])("AFTER audit triggers preserve actual identities in batches (mixed=%s)", async (mixed) => {
    const h = harness({
      trigger: { instead: false, disabled: false, event: "INSERT" },
    });
    const inserts = [
      await h.prepare({ id: 2, amount: "2.5" }),
      await h.prepare({ amount: "3.75" }),
    ];
    expect(
      await executeAtomicSqlApplyPlan(
        h.manager,
        mixed ? h.apply() : null,
        inserts,
      ),
    ).toMatchObject({ success: true, insertApplied: true });
    expect(h.reads).toEqual([[2], [101], ...(mixed ? [[50]] : [])]);
    expect(new Set(h.owners).size).toBe(1);
  });

  it.each([
    null,
    0,
  ])("unknown/denied metadata permissions fail closed (%s)", async (permission) => {
    const h = harness({ permission });
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 2, amount: "1.25" }),
      ),
    ).rejects.toThrow(/VIEW DEFINITION/);
    expect(h.events).toEqual(["BEGIN", "GUARD", "ROLLBACK"]);
  });

  it("catalog errors abort without DML", async () => {
    const h = harness({ guardError: true });
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 2, amount: "1.25" }),
      ),
    ).rejects.toThrow(/catalog query denied/);
    expect(h.events).toEqual(["BEGIN", "GUARD", "ROLLBACK"]);
  });

  it.each([
    true,
    false,
  ])("views fail closed, including no-op INSTEAD OF triggers (trigger=%s)", async (trigger) => {
    const h = harness({
      objectType: "V",
      trigger: trigger
        ? { instead: true, disabled: false, event: "INSERT" }
        : undefined,
    });
    await expect(
      h.service.executePreparedInsertPlan(
        await h.prepare({ id: 1, amount: "1.25" }),
      ),
    ).rejects.toThrow(trigger ? /INSTEAD OF INSERT/ : /view trigger metadata/);
    expect(h.events).toEqual(["BEGIN", "GUARD", "ROLLBACK"]);
  });

  it.each([
    ["differentDB", "sales", "edits"],
    ["db]quote'--?", "schema]quote'--?", "table]quote'--?"],
  ])("checks the exact selected catalog and escaped target %s/%s/%s", async (database, schema, table) => {
    const h = harness({ database, schema, table });
    await h.service.executePreparedInsertPlan(
      await h.prepare({ id: 2, amount: "1.25" }),
    );
    expect(h.events.at(-1)).toBe("COMMIT");
    expect(h.statements[0]).not.toContain("USE [other]");
  });

  it.each([
    "GUARD",
    "INSERT",
    "VERIFY",
  ] as const)("cancellation after %s prevents further DML/commit", async (cancelAt) => {
    const controller = new AbortController();
    const h = harness({
      onStage: (stage) => {
        if (stage === cancelAt) controller.abort(new Error("cancelled F03"));
      },
    });
    const plan = await h.prepare({ id: 2, amount: "1.25" });
    await expect(
      h.driver.runTransaction(
        [plan.operation],
        { signal: controller.signal, deadline: Date.now() + 10000 },
        { verifications: plan.verification ? [plan.verification] : [] },
      ),
    ).rejects.toThrow(/cancelled F03/);
    expect(h.events.at(-2)).toBe(cancelAt);
    expect(h.events.at(-1)).toBe("ROLLBACK");
    expect(h.stored.has(2)).toBe(false);
  });

  it("backend NULL remains NULL with the new guard", async () => {
    const h = harness();
    await h.service.executePreparedInsertPlan(
      await h.prepare({ id: 2, amount: null }),
    );
    expect(h.stored.get(2)).toBeNull();
    expect(h.events.at(-1)).toBe("COMMIT");
  });

  it("AFTER-trigger value mismatches still roll back the complete batch", async () => {
    const h = harness({
      mismatch: true,
      trigger: { instead: false, disabled: false, event: "INSERT" },
    });
    const result = await executeAtomicSqlApplyPlan(h.manager, h.apply(), [
      await h.prepare({ id: 2, amount: "1.25" }),
    ]);
    expect(result.success).toBe(false);
    expect(result.insertApplied).not.toBe(true);
    expect(h.events).toContain("UPDATE");
    expect(h.events.at(-1)).toBe("ROLLBACK");
    expect(h.stored.get(50)).toBe("5");
    expect(h.stored.has(2)).toBe(false);
  });

  it("custom OUTPUT plans without an inspectable target fail closed before DML", async () => {
    const h = harness();
    const plan = await h.prepare({ id: 2, amount: "1.25" });
    delete plan.operation.captureIdentity?.mssqlInsertTarget;
    await expect(h.service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /Recreate the insert plan/,
    );
    expect(h.events).toEqual(["BEGIN", "ROLLBACK"]);
  });

  it("a later blocked target rolls back earlier INSERT and UPDATE operations", async () => {
    const h = harness({
      blockedTable: "edits",
      trigger: { instead: true, disabled: false, event: "INSERT" },
    });
    const safe = await h.prepare({ id: 2, amount: "2.5" }, "safe");
    const blocked = await h.prepare({ id: 1, amount: "1.25" });
    await expect(
      h.driver.runTransaction([
        ...h.apply().operations,
        safe.operation,
        blocked.operation,
      ]),
    ).rejects.toThrow(/INSTEAD OF INSERT/);
    expect(h.events).toEqual([
      "BEGIN",
      "UPDATE",
      "GUARD",
      "INSERT",
      "GUARD",
      "ROLLBACK",
    ]);
    expect(h.stored.has(2)).toBe(false);
    expect(h.stored.get(50)).toBe("5");
    expect(h.stored.get(1)).toBe("1.25");
  });

  it.each([
    {
      nativeType: "bigint",
      category: "integer" as const,
      id: 9007199254740993n,
    },
    {
      nativeType: "decimal(38,18)",
      category: "decimal" as const,
      id: "2.123456789012345678",
    },
    {
      nativeType: "uniqueidentifier",
      category: "uuid" as const,
      id: "88b8a2e9-3d83-4c5e-a00f-b178b1de4278",
    },
  ])("guard preserves OUTPUT key type $nativeType", async ({
    nativeType,
    category,
    id,
  }) => {
    const h = harness({
      metadata: [
        { ...columns[0], type: nativeType, nativeType, category },
        columns[1],
      ],
    });
    await h.service.executePreparedInsertPlan(
      await h.prepare({ id, amount: "1.25" }),
    );
    expect(h.stored.get(id)).toBe("1.25");
    expect(h.reads).toEqual([[id]]);
    expect(h.events.at(-1)).toBe("COMMIT");
  });
});
