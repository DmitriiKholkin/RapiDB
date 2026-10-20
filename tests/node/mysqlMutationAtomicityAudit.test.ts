import { randomUUID } from "node:crypto";
import mysql from "mysql2/promise";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { RowUpdate } from "../../src/extension/table/tableDataContracts";
import {
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

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
const database = "rapidb_atomicity_audit";
const config = { id: "atomicity-audit", name: "audit", type: "mysql" as const };
const update = (id: number, original = id * 10): RowUpdate => ({
  primaryKeys: { id },
  changes: { amount: id * 10 + 1 },
  originalValues: { amount: original },
});

function managerFor(driver: MySQLDriver): ConnectionManager {
  return {
    getDriver: () => driver,
    getConnection: () => config,
  } as unknown as ConnectionManager;
}

function prepare(
  manager: ConnectionManager,
  table: string,
  updates: RowUpdate[],
  metadata = columns,
) {
  const result = prepareApplyChangesPlan(
    manager,
    config.id,
    database,
    "",
    table,
    updates,
    metadata,
  );
  if (!result.executable) throw new Error("Expected executable plan");
  return result.plan;
}

function harness(rows: Record<string, unknown>[]) {
  const driver = new MySQLDriver(config);
  const query = vi.fn(async (_input: mysql.QueryOptions) => [rows]);
  const lease = vi.fn();
  (driver as unknown as { pool: unknown }).pool = {
    query,
    getConnection: lease,
  };
  const transaction = vi.spyOn(driver, "runTransaction").mockResolvedValue();
  const manager = managerFor(driver);
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  return { driver, query, lease, transaction, manager, service };
}

afterEach(() => vi.restoreAllMocks());

describe("MySQL mutation atomicity metadata audit", () => {
  it.each([
    "Aria",
    "MyISAM",
    "MEMORY",
    "CSV",
    "ARCHIVE",
    "MRG_MYISAM",
    "ISAM",
    "CONNECT",
    "FEDERATED",
    "unknown_plugin",
  ])("blocks the server-declared non-transactional engine %s", async (engine) => {
    const h = harness([{ ENGINE: engine, TRANSACTIONS: "NO" }]);
    expect(
      await h.driver.getMutationAtomicityRisk(database, "", "edits"),
    ).toMatch(/non-transactional.*engine/i);
    expect(h.query).toHaveBeenCalledOnce();
    expect(h.query.mock.calls[0]).toEqual([
      expect.objectContaining({
        sql: expect.stringContaining("information_schema.ENGINES"),
        values: [database, "edits"],
      }),
    ]);
  });

  it.for([
    [],
    [{}],
    [{ ENGINE: null, TRANSACTIONS: "YES" }],
    [{ ENGINE: "", TRANSACTIONS: "YES" }],
    [{ ENGINE: "   ", TRANSACTIONS: "YES" }],
    [{ ENGINE: 123, TRANSACTIONS: "YES" }],
    [{ ENGINE: "unknown_plugin" }],
    [{ ENGINE: "unknown_plugin", TRANSACTIONS: null }],
    [{ ENGINE: "InnoDB", TRANSACTIONS: "" }],
    [{ ENGINE: "InnoDB", TRANSACTIONS: "UNKNOWN" }],
    [{ ENGINE: "InnoDB", TRANSACTIONS: true }],
  ])("fails closed for missing or inconclusive metadata: %j", async (rows) => {
    const h = harness(rows);
    expect(
      await h.driver.getMutationAtomicityRisk(database, "", "edits"),
    ).toMatch(/cannot confirm.*rollback support/i);
  });

  it.each([
    "InnoDB",
    "XtraDB",
    "NDBCLUSTER",
    "transactional_plugin",
  ])("allows %s only with affirmative server transaction capability", async (engine) => {
    const h = harness([{ ENGINE: engine, TRANSACTIONS: " yes " }]);
    expect(
      await h.driver.getMutationAtomicityRisk(database, "", "edits"),
    ).toBeNull();
  });

  it("uses bound target names and reads fresh capabilities on every check", async () => {
    const rows = [{ ENGINE: "InnoDB", TRANSACTIONS: "YES" }];
    const h = harness(rows);
    expect(
      await h.driver.getMutationAtomicityRisk("db'", "", "table`"),
    ).toBeNull();
    rows[0] = { ENGINE: "Aria", TRANSACTIONS: "NO" };
    expect(
      await h.driver.getMutationAtomicityRisk("db'", "", "table`"),
    ).toContain("ARIA");
    expect(h.query).toHaveBeenCalledTimes(2);
    expect(h.query.mock.calls[0]).toEqual([
      expect.objectContaining({ values: ["db'", "table`"] }),
    ]);
  });

  for (const path of ["prepared", "atomic"] as const) {
    it.for([
      [{ ENGINE: "Aria", TRANSACTIONS: "NO" }],
      [{ ENGINE: "unlisted_engine", TRANSACTIONS: null }],
      [],
    ])(`${path}: rejects verified UPDATE before any DML (%j)`, async (rows) => {
      const h = harness(rows);
      const plan = prepare(h.manager, "edits", [update(1)]);
      const result = await (path === "prepared"
        ? executePreparedApplyPlan(h.manager, plan)
        : executeAtomicSqlApplyPlan(h.manager, plan, []));
      expect(result).toMatchObject({
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
        error: expect.stringContaining(
          "UPDATE verification requires rollback support",
        ),
        rowOutcomes: [{ success: false, status: "skipped" }],
      });
      expect(h.transaction).not.toHaveBeenCalled();
      expect(h.lease).not.toHaveBeenCalled();
    });
  }

  it("also rejects multiple operations without value verifications", async () => {
    const h = harness([{ ENGINE: "Aria", TRANSACTIONS: "NO" }]);
    const metadata = columns.map((column) =>
      column.name === "amount"
        ? { ...column, nativeType: "DATE", category: "date" as const }
        : column,
    );
    const plan = prepare(
      h.manager,
      "edits",
      [1, 2].map((id) => ({
        primaryKeys: { id },
        changes: { amount: "2026-10-19" },
      })),
      metadata,
    );
    expect(
      plan.verificationTargets.every((target) => !target.values.length),
    ).toBe(true);
    expect(await executePreparedApplyPlan(h.manager, plan)).toMatchObject({
      success: false,
      changesPossible: false,
    });
    expect(h.transaction).not.toHaveBeenCalled();
  });

  it("rejects verified INSERT through the same real metadata guard", async () => {
    const h = harness([{ ENGINE: "Aria", TRANSACTIONS: "NO" }]);
    const plan = await h.service.prepareInsertRow(
      config.id,
      database,
      "",
      "edits",
      { id: 3, amount: 30 },
    );
    expect(plan.verification).toBeDefined();
    await expect(h.service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /INSERT verification requires rollback support/,
    );
    expect(h.transaction).not.toHaveBeenCalled();
  });

  it("preserves metadata permission failures and never starts DML", async () => {
    const h = harness([]);
    h.query.mockRejectedValue(new Error("SELECT command denied for ENGINES"));
    const result = await executePreparedApplyPlan(
      h.manager,
      prepare(h.manager, "edits", [update(1), update(2)]),
    );
    expect(result).toMatchObject({
      success: false,
      changesPossible: false,
      error: expect.stringContaining("SELECT command denied for ENGINES"),
    });
    expect(h.transaction).not.toHaveBeenCalled();
    expect(h.lease).not.toHaveBeenCalled();
  });
});

const livePort = Number(process.env.RAPIDB_MYSQL_ATOMICITY_AUDIT_PORT);
describe.runIf(Boolean(livePort))("MySQL atomicity (live MariaDB)", () => {
  async function live(
    engine: "Aria" | "InnoDB",
    run: (context: {
      admin: mysql.Connection;
      driver: MySQLDriver;
      manager: ConnectionManager;
      table: string;
    }) => Promise<void>,
  ) {
    const connection = {
      host: "127.0.0.1",
      port: livePort,
      user: "root",
      password: "rapidb-audit-only",
      database,
    };
    const admin = await mysql.createConnection(connection);
    const driver = new MySQLDriver({
      ...config,
      ...connection,
      username: connection.user,
    });
    const table = `audit_${randomUUID().replaceAll("-", "")}`;
    try {
      await driver.connect();
      await admin.query(
        `CREATE TABLE ${table} (id INT PRIMARY KEY, amount INT NOT NULL) ENGINE=${engine}`,
      );
      await admin.query(`INSERT INTO ${table} VALUES (1,10),(2,20)`);
      await run({ admin, driver, manager: managerFor(driver), table });
    } finally {
      await driver.disconnect();
      await admin.query(`DROP TABLE IF EXISTS ${table}`);
      await admin.end();
    }
  }

  it("independently demonstrates that Aria ROLLBACK does not undo writes", async () => {
    await live("Aria", async ({ admin, table }) => {
      const [capabilities] = await admin.query(
        "SELECT ENGINE, TRANSACTIONS FROM information_schema.ENGINES WHERE ENGINE='Aria'",
      );
      expect(capabilities).toEqual([{ ENGINE: "Aria", TRANSACTIONS: "NO" }]);
      await admin.beginTransaction();
      await admin.query(`UPDATE ${table} SET amount=11 WHERE id=1`);
      await admin.rollback();
      const [rows] = await admin.query(
        `SELECT amount FROM ${table} WHERE id=1`,
      );
      expect(rows).toEqual([{ amount: 11 }]);
    });
  });

  for (const engine of ["Aria", "InnoDB"] as const) {
    it(`${engine}: no partial write when the second UPDATE is stale`, async () => {
      await live(engine, async ({ admin, manager, table }) => {
        const result = await executePreparedApplyPlan(
          manager,
          prepare(manager, table, [update(1), update(2, -1)]),
        );
        const [rows] = await admin.query(`SELECT * FROM ${table} ORDER BY id`);
        expect({ result, rows }).toMatchObject({
          result: { success: false },
          rows: [
            { id: 1, amount: 10 },
            { id: 2, amount: 20 },
          ],
        });
        if (engine === "Aria") {
          expect(result.changesPossible).toBe(false);
          expect(result.error).toMatch(/rollback support.*ARIA/);
        }
      });
    });

    it(`${engine}: no persisted trigger-adjusted value after rejected verification`, async () => {
      await live(engine, async ({ admin, manager, table }) => {
        await admin.query(
          `CREATE TRIGGER ${table}_bu BEFORE UPDATE ON ${table} FOR EACH ROW SET NEW.amount=NEW.amount+100`,
        );
        const result = await executePreparedApplyPlan(
          manager,
          prepare(manager, table, [update(1)]),
        );
        const [rows] = await admin.query(
          `SELECT amount FROM ${table} WHERE id=1`,
        );
        expect({ result, rows }).toMatchObject({
          result: { success: false },
          rows: [{ amount: 10 }],
        });
        expect(result.error).toMatch(
          engine === "Aria" ? /rollback support.*ARIA/ : /verification failed/,
        );
        if (engine === "Aria") expect(result.changesPossible).toBe(false);
      });
    });
  }

  it("allows successful verified InnoDB writes", async () => {
    await live("InnoDB", async ({ admin, manager, table }) => {
      expect(
        await executePreparedApplyPlan(
          manager,
          prepare(manager, table, [update(1), update(2)]),
        ),
      ).toMatchObject({ success: true });
      const [rows] = await admin.query(`SELECT * FROM ${table} ORDER BY id`);
      expect(rows).toEqual([
        { id: 1, amount: 11 },
        { id: 2, amount: 21 },
      ]);
    });
  });

  it("reads capabilities with table DML privileges only, without admin grants", async () => {
    await live("Aria", async ({ admin, table }) => {
      const username = `audit_${randomUUID().replaceAll("-", "").slice(0, 16)}`;
      const driver = new MySQLDriver({
        ...config,
        host: "127.0.0.1",
        port: livePort,
        database,
        username,
        password: "limited-audit-only",
      });
      try {
        await admin.query(
          `CREATE USER '${username}'@'%' IDENTIFIED BY 'limited-audit-only'`,
        );
        await admin.query(
          `GRANT SELECT, INSERT, UPDATE, DELETE ON ${database}.${table} TO '${username}'@'%'`,
        );
        await driver.connect();
        expect(
          await driver.getMutationAtomicityRisk(database, "", table),
        ).toContain("ARIA");
        expect(
          await driver.getMutationAtomicityRisk(
            database,
            "",
            `${table}_invisible`,
          ),
        ).toMatch(/cannot confirm.*rollback support/i);
        await admin.query(`ALTER TABLE ${table} ENGINE=InnoDB`);
        expect(
          await driver.getMutationAtomicityRisk(database, "", table),
        ).toBeNull();
        const manager = managerFor(driver);
        expect(
          await executePreparedApplyPlan(
            manager,
            prepare(manager, table, [update(1)]),
          ),
        ).toMatchObject({ success: true });
      } finally {
        await driver.disconnect();
        await admin.query(`DROP USER IF EXISTS '${username}'@'%'`);
      }
    });
  });

  it("fails closed for a view whose ENGINE metadata is NULL", async () => {
    await live("InnoDB", async ({ admin, driver, table }) => {
      const view = `${table}_view`;
      try {
        await admin.query(`CREATE VIEW ${view} AS SELECT * FROM ${table}`);
        expect(
          await driver.getMutationAtomicityRisk(database, "", view),
        ).toMatch(/cannot confirm.*rollback support/i);
      } finally {
        await admin.query(`DROP VIEW IF EXISTS ${view}`);
      }
    });
  });
});
