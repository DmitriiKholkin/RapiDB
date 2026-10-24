import { randomUUID } from "node:crypto";
import mysql from "mysql2/promise";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

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
];

function harness(transactional = false) {
  const config = { id: "delete-safety", name: "audit", type: "mysql" as const };
  const driver = new MySQLDriver(config);
  // The second selected row has already disappeared in another session.
  const rows = new Set([1]);
  let snapshot: number[] = [];
  const query = vi.fn(async (input: { sql: string; values?: unknown[] }) => {
    if (input.sql.startsWith("DELETE")) {
      let affectedRows = 0;
      for (const id of input.values ?? []) {
        if (rows.delete(Number(id))) affectedRows++;
      }
      return [{ affectedRows }];
    }
    if (input.sql.startsWith("SELECT 1")) return [[]];
    throw new Error(`Unexpected SQL: ${input.sql}`);
  });
  const lease = vi.fn(async () => ({
    query,
    beginTransaction: async () => {
      snapshot = [...rows];
    },
    commit: vi.fn(),
    rollback: async () => {
      if (transactional) {
        rows.clear();
        for (const id of snapshot) rows.add(id);
      }
    },
    release: vi.fn(),
  }));
  Object.assign(driver, { pool: { getConnection: lease } });
  const risk = vi
    .spyOn(driver, "getMutationAtomicityRisk")
    .mockResolvedValue(
      transactional ? null : "Table uses non-transactional engine MYISAM.",
    );
  const connection = { ...config, readOnly: false };
  let currentDriver: unknown = driver;
  const manager = {
    getConnection: () => connection,
    getDriver: () => currentDriver,
  } as unknown as ConnectionManager;
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  return {
    service,
    driver,
    rows,
    lease,
    risk,
    connection,
    replaceDriver: () => {
      currentDriver = { runTransaction: vi.fn() };
    },
    prepare: () =>
      service.prepareDeleteRowsPlan(config.id, "db", "", "items", [
        { id: 1 },
        { id: 2 },
      ]),
  };
}

afterEach(() => vi.restoreAllMocks());

describe("DELETE affected-row verification requires rollback support", () => {
  it("blocks a single DELETE statement on MyISAM before any row is lost", async () => {
    const h = harness();
    const pending = h.prepare().then(async (plan) => {
      if (!plan) throw new Error("Expected a delete plan");
      expect(plan.operations).toHaveLength(1);
      await h.service.executePreparedDeletePlan(plan);
    });
    await expect(pending).rejects.toThrow(/rollback support.*MYISAM/i);
    expect([...h.rows]).toEqual([1]);
    expect(h.lease).not.toHaveBeenCalled();
  });

  it("rechecks engine capabilities after a preview, reporting no attempted writes", async () => {
    const h = harness(true);
    const plan = await h.prepare();
    if (!plan) throw new Error("Expected a delete plan");
    h.risk.mockResolvedValue("Table now uses MYISAM.");
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
        rowOutcomes: [{ status: "skipped" }, { status: "skipped" }],
      },
    });
    expect([...h.rows]).toEqual([1]);
    expect(h.lease).not.toHaveBeenCalled();
  });

  it.each([
    "read-only",
    "replacement",
  ] as const)("rejects a %s transition during deferred rollback preflight", async (transition) => {
    const h = harness(true);
    const plan = await h.prepare();
    if (!plan) throw new Error("Expected a delete plan");
    let finish!: (risk: null) => void;
    h.risk.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const pending = h.service
      .executePreparedDeletePlan(plan)
      .catch((error: unknown) => error);
    expect(h.risk).toHaveBeenCalled();
    if (transition === "read-only") h.connection.readOnly = true;
    else h.replaceDriver();
    finish(null);
    expect(await pending).toMatchObject({
      deleteResult: { changesPossible: false, outcomeUnknown: false },
    });
    expect(h.lease).not.toHaveBeenCalled();
  });

  it("retains transactional rollback when a selected row is stale", async () => {
    const h = harness(true);
    const plan = await h.prepare();
    if (!plan) throw new Error("Expected a delete plan");
    await expect(h.service.executePreparedDeletePlan(plan)).rejects.toThrow(
      /expected 2/,
    );
    expect([...h.rows]).toEqual([1]);
    expect(h.lease).toHaveBeenCalledOnce();
  });
});

describe.skipIf(!process.env.RAPIDB_MYSQL_DELETE_AUDIT_PORT)(
  "DELETE rollback safeguards (live MySQL)",
  () => {
    it.each([
      "MyISAM",
      "InnoDB",
    ] as const)("preserves rows with a stale selection on %s", async (engine) => {
      const database = `rapidb_delete_audit_${randomUUID().replaceAll("-", "")}`;
      const config = {
        id: database,
        name: database,
        type: "mysql" as const,
        host: "127.0.0.1",
        port: Number(process.env.RAPIDB_MYSQL_DELETE_AUDIT_PORT),
        username: "root",
        password: "root_pass123",
        database,
      };
      const admin = await mysql.createConnection({
        host: config.host,
        port: config.port,
        user: config.username,
        password: config.password,
      });
      const driver = new MySQLDriver(config);
      try {
        await admin.query(`CREATE DATABASE \`${database}\``);
        await admin.query(
          `CREATE TABLE \`${database}\`.items (id INT PRIMARY KEY) ENGINE=${engine}`,
        );
        await admin.query(`INSERT INTO \`${database}\`.items VALUES (1)`);
        await driver.connect();
        const manager = {
          getDriver: () => driver,
          getConnection: () => config,
        } as unknown as ConnectionManager;
        const service = new TableMutationService(manager, {
          getColumns: () => driver.describeColumns(database, "", "items"),
        });
        const plan = await service.prepareDeleteRowsPlan(
          config.id,
          database,
          "",
          "items",
          [{ id: 1 }, { id: 2 }],
        );
        if (!plan) throw new Error("Expected a delete plan");
        await expect(service.executePreparedDeletePlan(plan)).rejects.toThrow(
          engine === "MyISAM" ? /rollback support/ : /expected 2/,
        );
        const [rows] = await admin.query(`SELECT * FROM \`${database}\`.items`);
        expect(rows).toEqual([{ id: 1 }]);
      } finally {
        await driver.disconnect();
        await admin.query(`DROP DATABASE IF EXISTS \`${database}\``);
        await admin.end();
      }
    });
  },
);
