import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { isMutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import {
  isUnverifiedTriggerVisibilityError,
  mariaDbGrantedRoles,
  mysqlGrantsShowTriggerAccess,
  parseMysqlCurrentRoles,
} from "../../src/extension/dbDrivers/mysqlTriggerVisibility";
import type {
  ColumnTypeMeta,
  TransactionOptions,
} from "../../src/extension/dbDrivers/types";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import {
  runLockedTableTransaction,
  setUnverifiedTriggerVisibilityConfirmation,
} from "../../src/extension/table/lockedTableMutation";
import {
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const goodEngine = { ENGINE: "InnoDB", TRANSACTIONS: "YES" };
const visible = [["GRANT TRIGGER ON *.* TO `editor`@`localhost`"]];
const config = { id: "locked", name: "Locked", type: "mysql" as const };
const columns: ColumnTypeMeta[] = ["id", "amount"].map((name) => ({
  name,
  type: "INT",
  nativeType: "INT",
  category: "integer",
  nullable: false,
  isPrimaryKey: name === "id",
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
}));

function harness(
  options: {
    engine?: readonly unknown[];
    ddl?: string;
    grants?: readonly unknown[];
    recheckedGrants?: readonly unknown[];
    triggerEvents?: string | null;
    metadataWarning?: boolean;
    onLock?: (sql: string, timeout?: number) => Promise<void>;
    mismatch?: boolean;
    rollbackWarnings?: number;
    currentRole?: string | null | Error;
    roleGrants?: readonly unknown[];
  } = {},
) {
  const events: string[] = [];
  const driver = new MySQLDriver(config);
  const connection = { ...config, readOnly: false };
  const manager = {
    getDriver: () => driver,
    getConnection: () => connection,
    getQueryEditorPresentation: () => undefined,
  } as unknown as ConnectionManager;
  let grantsRead = 0;
  const conn = {
    query: vi.fn(
      async (
        input:
          | string
          | {
              sql: string;
              values?: unknown[];
              timeout?: number;
              rowsAsArray?: boolean;
            },
      ) => {
        const sql = typeof input === "string" ? input : input.sql;
        const params = typeof input === "string" ? [] : (input.values ?? []);
        if (sql.startsWith("SET TRANSACTION")) return [[]];
        const lockPrefix = "SELECT CONNECTION_ID() FROM ";
        if (sql.startsWith(lockPrefix) && sql.endsWith(" LIMIT 0")) {
          events.push(`LOCK ${sql.slice(lockPrefix.length, -8)}`);
          await options.onLock?.(
            sql,
            typeof input === "string" ? undefined : input.timeout,
          );
          return [[]];
        }
        if (sql.startsWith("SHOW CREATE TABLE ")) {
          events.push(`BASE ${sql.slice(18)}`);
          return [
            [
              {
                "Create Table":
                  options.ddl ?? "CREATE TABLE `items` (`id` int)",
              },
            ],
          ];
        }
        if (sql.includes("information_schema.ENGINES")) {
          events.push(`ENGINE ${params.join("/")}`);
          return [options.engine ?? [goodEngine]];
        }
        if (sql === "SELECT CURRENT_ROLE()") {
          events.push("ROLES");
          if (options.currentRole instanceof Error) throw options.currentRole;
          return [
            [
              [
                options.currentRole === undefined
                  ? "NONE"
                  : options.currentRole,
              ],
            ],
          ];
        }
        if (sql.startsWith("SHOW GRANTS FOR CURRENT_USER() USING ")) {
          events.push(
            `GRANTS ${sql.slice("SHOW GRANTS FOR CURRENT_USER() ".length)}`,
          );
          return [options.roleGrants ?? []];
        }
        if (sql === "SHOW GRANTS FOR CURRENT_USER()") {
          expect(input).toMatchObject({ rowsAsArray: true });
          events.push("GRANTS");
          return [
            grantsRead++ > 0 && options.recheckedGrants
              ? options.recheckedGrants
              : (options.grants ?? visible),
          ];
        }
        if (sql.includes("information_schema.TRIGGERS")) {
          expect(params).toEqual([params[0], params[1], params[0], params[1]]);
          events.push(`TRIGGERS ${params.slice(0, 2).join("/")}`);
          return [
            options.triggerEvents === undefined
              ? []
              : [{ EVENT_MANIPULATION: options.triggerEvents }],
          ];
        }
        if (sql === "SHOW COUNT(*) WARNINGS") {
          return [[[options.metadataWarning ? 1 : 0]]];
        }
        if (sql === "ROLLBACK") {
          events.push("ROLLBACK");
          return [{ warningStatus: options.rollbackWarnings ?? 0 }];
        }
        if (
          sql.startsWith("INSERT") ||
          sql.startsWith("DELETE") ||
          sql.startsWith("UPDATE")
        ) {
          events.push(sql.split(" ")[0]);
          return [{ affectedRows: 1, insertId: 1 }];
        }
        if (sql.startsWith("SELECT 1 ") && sql.endsWith("FOR UPDATE"))
          return [[]];
        if (sql.startsWith("SELECT")) {
          events.push("VERIFY");
          const row = Object.fromEntries(
            [...sql.matchAll(/`(id|amount)` AS `?(__col_\d+)`?/g)].map(
              ([, name, alias]) => [
                alias,
                name === "id" ? 1 : options.mismatch ? 3 : 2,
              ],
            ),
          );
          return [[row]];
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      },
    ),
    beginTransaction: vi.fn(async () => void events.push("BEGIN")),
    commit: vi.fn(async () => void events.push("COMMIT")),
    release: vi.fn(() => void events.push("RELEASE")),
    destroy: vi.fn(() => void events.push("DESTROY")),
  };
  const pool = {
    getConnection: vi.fn(async () => {
      events.push("CHECKOUT");
      return conn;
    }),
    query: vi.fn(async () => [[goodEngine]]),
  };
  Object.assign(driver, { pool });
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  const controller = new TableMutationPreviewController({
    connectionId: config.id,
    tableName: "items",
    connectionManager: manager,
    tableDataService: service,
    notifyWarning: vi.fn(),
  });
  function apply(table = "items") {
    const result = prepareApplyChangesPlan(
      manager,
      config.id,
      "sales",
      "",
      table,
      [{ primaryKeys: { id: 1 }, changes: { amount: 2 } }],
      columns,
    );
    if (!result.executable)
      throw new Error("Expected an executable apply plan");
    return result.plan;
  }
  const insert = (table = "items") =>
    service.prepareInsertRow(config.id, "sales", "", table, {
      id: 1,
      amount: 2,
    });
  const deletePlan = () =>
    service.prepareDeleteRowsPlan(config.id, "sales", "", "items", [{ id: 1 }]);
  return {
    events,
    driver,
    conn,
    pool,
    manager,
    connection,
    service,
    controller,
    apply,
    insert,
    deletePlan,
  };
}

describe("locked MySQL Table Viewer mutations", () => {
  it("checks exact targets in stable order on the write session only after confirmation", async () => {
    const h = harness();
    const preview = h.controller.createApplyChangesPreview("confirm", {
      apply: h.apply("z_items"),
      inserts: [await h.insert("a`items")],
      applyResultWhenEmpty: null,
    });
    expect(h.events).toEqual([]);
    expect(preview.text).not.toMatch(/LOCK|SHOW GRANTS|information_schema/);
    expect(
      await h.controller.confirm(preview.previewToken, "confirm"),
    ).toMatchObject({
      type: "applyResult",
      payload: { success: true, insertApplied: true },
    });
    expect(h.events).toEqual([
      "CHECKOUT",
      "BEGIN",
      "LOCK `sales`.`a``items`",
      "LOCK `sales`.`z_items`",
      "BASE `sales`.`a``items`",
      "ENGINE sales/a`items/sales/a`items",
      "BASE `sales`.`z_items`",
      "ENGINE sales/z_items/sales/z_items",
      "ROLES",
      "GRANTS",
      "TRIGGERS sales/a`items",
      "TRIGGERS sales/z_items",
      "ROLES",
      "GRANTS",
      "INSERT",
      "UPDATE",
      "VERIFY",
      "VERIFY",
      "COMMIT",
      "RELEASE",
    ]);
  });

  it("deduplicates a shared target but checks both INSERT and UPDATE events", async () => {
    const h = harness();
    expect(
      await executeAtomicSqlApplyPlan(h.manager, h.apply(), [await h.insert()]),
    ).toMatchObject({ success: true });
    expect(h.events.filter((event) => /^(LOCK|TRIGGERS)/.test(event))).toEqual([
      "LOCK `sales`.`items`",
      "TRIGGERS sales/items",
    ]);
  });

  it("allows an UPDATE with only an INSERT trigger, but blocks a batch that would fire it", async () => {
    const update = harness({ triggerEvents: "INSERT" });
    expect(
      await executePreparedApplyPlan(update.manager, update.apply()),
    ).toMatchObject({ success: true });
    const mixed = harness({ triggerEvents: "INSERT" });
    expect(
      await executeAtomicSqlApplyPlan(mixed.manager, mixed.apply(), [
        await mixed.insert(),
      ]),
    ).toMatchObject({
      success: false,
      changesPossible: false,
      error: expect.stringContaining("INSERT trigger"),
    });
    expect(mixed.events).not.toContain("INSERT");
    expect(mixed.events).not.toContain("UPDATE");
  });

  it("does not use an existing pooled session to prove visibility using current grants", async () => {
    const h = harness();
    const native = {};
    const old = {
      connection: native,
      destroy: vi.fn(),
      beginTransaction: vi.fn(),
    };
    const fresh = {};
    Object.assign(h.conn, { connection: fresh });
    const tracked = new Set([native]);
    Reflect.get(h.driver, "poolConnections").set(h.pool, tracked);
    h.pool.getConnection.mockImplementationOnce(async () => {
      h.events.push("CHECKOUT OLD");
      return old as unknown as typeof h.conn;
    });
    h.pool.getConnection.mockImplementationOnce(async () => {
      h.events.push("CHECKOUT");
      tracked.add(fresh);
      return h.conn;
    });
    expect(await executePreparedApplyPlan(h.manager, h.apply())).toMatchObject({
      success: true,
    });
    expect(old.destroy).toHaveBeenCalledOnce();
    expect(old.beginTransaction).not.toHaveBeenCalled();
    expect(h.events.slice(0, 4)).toEqual([
      "CHECKOUT OLD",
      "CHECKOUT",
      "BEGIN",
      "LOCK `sales`.`items`",
    ]);
  });

  it.each([
    [
      { engine: [{ ENGINE: "Aria", TRANSACTIONS: "NO" }] },
      /non-transactional MySQL engine ARIA/,
    ],
    [{ engine: [] }, /engine transaction metadata is unavailable/],
    [
      { ddl: "CREATE TEMPORARY TABLE `items` (`id` int)" },
      /permanent MySQL base table/,
    ],
    [{ ddl: "CREATE VIEW `items` AS SELECT 1" }, /permanent MySQL base table/],
    [
      { triggerEvents: "INSERT, UPDATE" },
      /UPDATE trigger may have non-transactional side effects/,
    ],
    [{ triggerEvents: null }, /Cannot confirm trigger event metadata/],
    [
      {
        grants: [["GRANT SELECT, UPDATE ON `sales`.* TO `editor`@`localhost`"]],
      },
      /full trigger visibility/,
    ],
    [
      { recheckedGrants: [["GRANT SELECT ON *.* TO `editor`@`localhost`"]] },
      /full trigger visibility/,
    ],
    [{ metadataWarning: true }, /Cannot confirm trigger metadata/],
  ] as const)("refuses inconclusive/unsafe write-session metadata (%j), despite a good advisory engine check", async (options, diagnostic) => {
    const h = harness(options);
    expect(await executePreparedApplyPlan(h.manager, h.apply())).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
      error: expect.stringMatching(diagnostic),
      rowOutcomes: [{ status: "skipped" }],
    });
    expect(h.pool.query).toHaveBeenCalledOnce();
    expect(h.events.slice(0, 3)).toEqual([
      "CHECKOUT",
      "BEGIN",
      "LOCK `sales`.`items`",
    ]);
    expect(h.events.slice(-2)).toEqual(["ROLLBACK", "RELEASE"]);
    expect(h.events).not.toContain("UPDATE");
    expect(h.conn.commit).not.toHaveBeenCalled();
  });

  it("refuses a verified standalone INSERT and DELETE on relevant triggers without DML", async () => {
    const h = harness({ triggerEvents: "INSERT,DELETE" });
    const plan = await h.insert();
    expect(plan.verification).toBeDefined();
    const insert = h.controller.createInsertPreview("insert", plan);
    const deletePlan = await h.deletePlan();
    if (!deletePlan) throw new Error("Expected a DELETE plan");
    const del = h.controller.createDeleteRowsPreview("delete", deletePlan);
    expect(h.events).toEqual([]);
    expect(
      await h.controller.confirm(insert.previewToken, "insert"),
    ).toMatchObject({
      type: "insertResult",
      payload: {
        success: false,
        outcomeUnknown: false,
        error: expect.stringContaining("INSERT trigger"),
      },
    });
    expect(
      await h.controller.confirm(del.previewToken, "delete"),
    ).toMatchObject({
      type: "deleteResult",
      payload: {
        success: false,
        outcomeUnknown: false,
        error: expect.stringContaining("DELETE trigger"),
      },
    });
    expect(h.events.filter((event) => event.startsWith("TRIGGERS"))).toEqual([
      "TRIGGERS sales/items",
      "TRIGGERS sales/items",
    ]);
    expect(h.events).not.toContain("INSERT");
    expect(h.events).not.toContain("DELETE");
  });

  it.each([
    0, 1,
  ])("preserves the verification rollback contract with %i rollback warnings", async (rollbackWarnings) => {
    const h = harness({ mismatch: true, rollbackWarnings });
    const result = await executePreparedApplyPlan(h.manager, h.apply());
    expect(result).toMatchObject({
      success: false,
      changesPossible: rollbackWarnings !== 0,
      outcomeUnknown: rollbackWarnings !== 0,
    });
    expect(h.events.slice(-4)).toEqual([
      "UPDATE",
      "VERIFY",
      "ROLLBACK",
      rollbackWarnings ? "DESTROY" : "RELEASE",
    ]);
    expect(h.conn.commit).not.toHaveBeenCalled();
  });

  it("reports no DML even if rollback cleanup before DML could not be confirmed", async () => {
    const h = harness({ triggerEvents: "UPDATE", rollbackWarnings: 1 });
    expect(await executePreparedApplyPlan(h.manager, h.apply())).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
      error: expect.stringMatching(/the connection was discarded/i),
    });
    expect(h.events.slice(-2)).toEqual(["ROLLBACK", "DESTROY"]);
  });

  it("revalidates write permission after awaiting the lock", async () => {
    const h = harness({
      onLock: async () => {
        h.connection.readOnly = true;
      },
    });
    expect(await executePreparedApplyPlan(h.manager, h.apply())).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
      error: expect.stringMatching(/read.only/),
    });
    expect(h.events).not.toContain("UPDATE");
    expect(h.events.slice(-2)).toEqual(["ROLLBACK", "RELEASE"]);
  });

  it.each([
    "abort",
    "timeout",
  ] as const)("does not write after %s while obtaining the lock", async (kind) => {
    const abort = new AbortController();
    const assertWritable = vi.fn();
    const h = harness({
      onLock: async (_sql, timeout) => {
        expect(timeout).toBeGreaterThan(0);
        expect(timeout).toBeLessThanOrEqual(1_000);
        if (kind === "abort") abort.abort(new Error("cancelled while locking"));
        else
          throw Object.assign(new Error("lock timed out"), {
            code: "PROTOCOL_SEQUENCE_TIMEOUT",
          });
      },
    });
    const scope: TransactionOptions = {
      lockedTableMutation: {
        targets: [
          { database: "sales", schema: "", table: "items", event: "UPDATE" },
        ],
        assertWritable,
      },
    };
    const error = await h.driver
      .runTransaction(
        h.apply().operations,
        { signal: abort.signal, deadline: Date.now() + 1_000 },
        scope,
      )
      .catch((error: unknown) => error);
    expect(isMutationNotExecutedError(error)).toBe(true);
    expect(assertWritable).not.toHaveBeenCalled();
    expect(h.events).toEqual([
      "CHECKOUT",
      "BEGIN",
      "LOCK `sales`.`items`",
      "DESTROY",
    ]);
  });
});

describe("MySQL/MariaDB trigger grant proof", () => {
  it.each([
    "GRANT ALL PRIVILEGES ON *.* TO `editor`@`localhost`",
    "GRANT SELECT, TRIGGER ON `sales`.* TO `editor`@`localhost`",
    "GRANT TRIGGER ON sales.items TO `editor`@`localhost`",
    'GRANT TRIGGER ON "sales"."items" TO "editor"@"localhost"',
  ])("accepts a covering grant: %s", (grant) => {
    expect(mysqlGrantsShowTriggerAccess([[grant]], "sales", "items")).toBe(
      true,
    );
  });
  it.each([
    ["GRANT `trigger_admin` TO `editor`@`localhost`"],
    ["GRANT TRIGGER ON `sale%`.* TO `editor`@`localhost`"],
    ["GRANT TRIGGER ON `sales`.`other` TO `editor`@`localhost`"],
    ["GRANT SELECT ON `sales`.`items` TO `TRIGGER`@`localhost`"],
    [
      "GRANT TRIGGER ON *.* TO `editor`@`localhost`",
      "REVOKE TRIGGER ON `sales`.* FROM `editor`@`localhost`",
    ],
    [
      "GRANT USAGE ON *.* TO `editor`@`localhost`",
      "GRANT TRIGGER ON *.* TO `editor`@`localhost`",
    ],
  ])("fails closed on an insufficient or ambiguous grant: %s", (...grants) => {
    expect(
      mysqlGrantsShowTriggerAccess(
        grants.map((grant) => [grant]),
        "sales",
        "items",
      ),
    ).toBe(false);
  });

  it.each([
    // SHOW GRANTS prints an escaped literal for GRANT ... ON `my\_app`.*
    ["GRANT ALL PRIVILEGES ON `sales\\_eu`.* TO `editor`@`%`"],
    // An unescaped '_' is a wildcard that also names this database exactly,
    // and the only database-level grant that can apply.
    ["GRANT TRIGGER ON `sales_eu`.* TO `editor`@`localhost`"],
    [
      "GRANT USAGE ON *.* TO `editor`@`%`",
      "GRANT SELECT, TRIGGER ON `sales\\_eu`.* TO `editor`@`%`",
      "GRANT TRIGGER ON `sales%`.* TO `editor`@`%`",
    ],
  ])("accepts database names containing '_': %s", (...grants) => {
    expect(
      mysqlGrantsShowTriggerAccess(
        grants.map((grant) => [grant]),
        "sales_eu",
        "items",
      ),
    ).toBe(true);
  });

  it.each([
    // Another matching database-level row without TRIGGER may be the one the
    // server applies (first match), whichever is listed first.
    [
      "GRANT TRIGGER ON `sales_eu`.* TO `editor`@`%`",
      "GRANT SELECT ON `sales\\_eu`.* TO `editor`@`%`",
    ],
    [
      "GRANT ALL PRIVILEGES ON `sales\\_eu`.* TO `editor`@`%`",
      "GRANT SELECT ON `sales%`.* TO `editor`@`%`",
    ],
    // Case variants may apply with lower_case_table_names.
    [
      "GRANT TRIGGER ON `sales\\_eu`.* TO `editor`@`%`",
      "GRANT SELECT ON `SALES\\_EU`.* TO `editor`@`%`",
    ],
    // A pure wildcard is literal with partial_revokes; not proof by itself.
    ["GRANT TRIGGER ON `sales%`.* TO `editor`@`%`"],
    ["GRANT TRIGGER ON `sales\\_e%`.* TO `editor`@`%`"],
    // An escaped literal for a different name.
    ["GRANT TRIGGER ON `salesXeu`.* TO `editor`@`%`"],
  ])("rejects ambiguous database-level grants: %s", (...grants) => {
    expect(
      mysqlGrantsShowTriggerAccess(
        grants.map((grant) => [grant]),
        "sales_eu",
        "items",
      ),
    ).toBe(false);
  });

  it("parses active roles for SHOW GRANTS ... USING and MariaDB role chains", () => {
    expect(parseMysqlCurrentRoles("NONE", false)).toEqual([]);
    expect(parseMysqlCurrentRoles(null, true)).toEqual([]);
    expect(
      parseMysqlCurrentRoles("`app``rw`@`%`,`audit`@`localhost`", false),
    ).toEqual(["`app``rw`@`%`", "`audit`@`localhost`"]);
    expect(parseMysqlCurrentRoles("`a`@`%`; DROP", false)).toBeUndefined();
    expect(parseMysqlCurrentRoles("writer`x", true)).toEqual(["`writer``x`"]);
    expect(
      mariaDbGrantedRoles([
        ["GRANT `nested` TO `writer`"],
        ["GRANT SELECT ON `sales`.* TO `writer`"],
      ]),
    ).toEqual(["`nested`"]);
  });

  it("does not treat wildcard schema grants as exact table grants", () => {
    expect(
      mysqlGrantsShowTriggerAccess(
        [
          ["GRANT TRIGGER ON `sales_eu`.* TO `editor`@`localhost`"],
          ["GRANT SELECT ON `sales\\_eu`.* TO `editor`@`localhost`"],
        ],
        "sales_eu",
        "items",
      ),
    ).toBe(false);
    expect(
      mysqlGrantsShowTriggerAccess(
        [["GRANT TRIGGER ON `sales_eu`.`items` TO `editor`@`localhost`"]],
        "sales_eu",
        "items",
      ),
    ).toBe(true);
  });
});

describe("MySQL trigger visibility through roles and explicit confirmation", () => {
  const noTrigger = [["GRANT SELECT, UPDATE ON `sales`.* TO `editor`@`%`"]];
  afterEach(() => setUnverifiedTriggerVisibilityConfirmation(undefined));

  it("expands active MySQL 8 roles before deciding", async () => {
    const h = harness({
      grants: noTrigger,
      currentRole: "`trigger_role`@`%`",
      roleGrants: [
        ["GRANT USAGE ON *.* TO `editor`@`%`"],
        ["GRANT SELECT, UPDATE, TRIGGER ON `sales`.* TO `editor`@`%`"],
        ["GRANT `trigger_role`@`%` TO `editor`@`%`"],
      ],
    });
    expect(await executePreparedApplyPlan(h.manager, h.apply())).toMatchObject({
      success: true,
    });
    expect(h.events).toContain("GRANTS USING `trigger_role`@`%`");
    expect(h.events).toContain("UPDATE");
  });

  it("fails closed without a confirmation and writes nothing", async () => {
    const h = harness({ grants: noTrigger });
    const result = await executePreparedApplyPlan(h.manager, h.apply());
    expect(result).toMatchObject({
      success: false,
      changesPossible: false,
      error: expect.stringMatching(/full trigger visibility/),
    });
    expect(h.events).not.toContain("UPDATE");
  });

  it("retries once with exactly the reported tables after an explicit confirmation", async () => {
    const h = harness({ grants: noTrigger });
    const confirm = vi.fn(async () => true);
    setUnverifiedTriggerVisibilityConfirmation(confirm);
    expect(await executePreparedApplyPlan(h.manager, h.apply())).toMatchObject({
      success: true,
    });
    expect(confirm).toHaveBeenCalledExactlyOnceWith(["`sales`.`items`"]);
    // First attempt rolled back before any DML; the second one wrote.
    expect(h.events.filter((event) => event === "UPDATE")).toHaveLength(1);
    expect(h.events.indexOf("ROLLBACK")).toBeLessThan(
      h.events.indexOf("UPDATE"),
    );
  });

  it("still blocks a visible trigger after the confirmation", async () => {
    const h = harness({ grants: noTrigger, triggerEvents: "UPDATE" });
    setUnverifiedTriggerVisibilityConfirmation(async () => true);
    expect(await executePreparedApplyPlan(h.manager, h.apply())).toMatchObject({
      success: false,
      error: expect.stringMatching(/UPDATE trigger may have non-transactional/),
    });
    expect(h.events).not.toContain("UPDATE");
  });

  it("does not retry when the user declines", async () => {
    const run = vi.fn(async (options: TransactionOptions) => {
      if (!options.lockedTableMutation?.acceptUnverifiedTriggerVisibility) {
        const { UnverifiedTriggerVisibilityError } = await import(
          "../../src/extension/dbDrivers/mysqlTriggerVisibility"
        );
        throw new UnverifiedTriggerVisibilityError(["`sales`.`items`"]);
      }
    });
    setUnverifiedTriggerVisibilityConfirmation(async () => false);
    const options = {
      lockedTableMutation: { targets: [], assertWritable: () => {} },
    };
    const failure = await runLockedTableTransaction(options, run).catch(
      (error: unknown) => error,
    );
    expect(isUnverifiedTriggerVisibilityError(failure)).toBe(true);
    expect(isMutationNotExecutedError(failure)).toBe(true);
    expect(run).toHaveBeenCalledOnce();
  });
});
