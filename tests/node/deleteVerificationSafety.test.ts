import { randomUUID } from "node:crypto";
import { join } from "node:path";
import mysql from "mysql2/promise";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { createProjectTempDir } from "../runtime/tempDirectories";

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
  const rows = new Set([1]);
  let snapshot: number[] = [];
  const rollback = vi.fn(async () => {
    if (transactional) {
      rows.clear();
      for (const id of snapshot) rows.add(id);
    }
  });
  const query = vi.fn(async (input: { sql: string; values?: unknown[] }) => {
    if (input.sql === "ROLLBACK") {
      await rollback();
      return [{ warningStatus: 0 }];
    }
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
    rollback,
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
    rollback,
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
    const pending = h.service
      .prepareDeleteRowsPlan("delete-safety", "db", "", "items", [{ id: 1 }])
      .then(async (plan) => {
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
      /expected 1/,
    );
    expect([...h.rows]).toEqual([1]);
    expect(h.lease).toHaveBeenCalledOnce();
    expect(h.rollback).toHaveBeenCalledOnce();
  });
});

describe("batched single-key DELETE verifies each requested key", () => {
  it("rolls back when a duplicated key hides a missing key and deletes exact batches", async () => {
    const directory = await createProjectTempDir("sqlite-delete-keys");
    const config = {
      id: "sqlite-delete-keys",
      name: "sqlite-delete-keys",
      type: "sqlite" as const,
      filePath: join(directory, "db.sqlite"),
    };
    const driver = new SQLiteCoreDriver(config);
    await driver.connect();
    const runQuery = driver.query.bind(driver);
    let queue: Promise<unknown> = Promise.resolve();
    Object.assign(driver, {
      query: (...args: Parameters<SQLiteCoreDriver["query"]>) => {
        const run = queue.then(() => runQuery(...args));
        queue = run.catch(() => undefined);
        return run;
      },
    });
    try {
      await driver.query("CREATE TABLE items (id INTEGER, name TEXT)");
      for (const id of [1, 1, 3, 4]) {
        await driver.query("INSERT INTO items (id, name) VALUES (?, 'x')", [
          id,
        ]);
      }
      const service = new TableMutationService(
        {
          getConnection: () => ({ ...config, readOnly: false }),
          getDriver: () => driver,
        } as unknown as ConnectionManager,
        { getColumns: async () => columns },
      );
      const remaining = async () =>
        (await driver.query("SELECT id FROM items ORDER BY id")).rows.map(
          (row) => row.__col_0,
        );

      const unsafe = await service.prepareDeleteRowsPlan(
        config.id,
        "main",
        "main",
        "items",
        [{ id: 1 }, { id: 2 }],
      );
      if (!unsafe) throw new Error("Expected a delete plan");
      expect(unsafe.operations).toHaveLength(1);
      await expect(service.executePreparedDeletePlan(unsafe)).rejects.toThrow(
        /expected 2/,
      );
      expect(await remaining()).toEqual([1, 1, 3, 4]);

      const safe = await service.prepareDeleteRowsPlan(
        config.id,
        "main",
        "main",
        "items",
        [{ id: 3 }, { id: 4 }],
      );
      if (!safe) throw new Error("Expected a delete plan");
      await service.executePreparedDeletePlan(safe);
      expect(await remaining()).toEqual([1, 1]);
    } finally {
      await driver.disconnect();
    }
  });
});

describe("composite-key DELETE identities", () => {
  it("does not collapse distinct keys whose values contain identity separators", async () => {
    const directory = await createProjectTempDir("sqlite-delete-composite");
    const config = {
      id: "sqlite-delete-composite",
      name: "sqlite-delete-composite",
      type: "sqlite" as const,
      filePath: join(directory, "db.sqlite"),
    };
    const driver = new SQLiteDriver(config);
    await driver.connect();
    try {
      await driver.query(
        "CREATE TABLE pairs (a TEXT, b TEXT, PRIMARY KEY (a, b))",
      );
      const keys = [
        { a: 'x|"b"=string:y', b: "z" },
        { a: "x", b: 'y|"b"=string:z' },
      ];
      for (const key of keys) {
        await driver.query("INSERT INTO pairs (a, b) VALUES (?, ?)", [
          key.a,
          key.b,
        ]);
      }
      const service = new TableMutationService(
        {
          getConnection: () => ({ ...config, readOnly: false }),
          getDriver: () => driver,
        } as unknown as ConnectionManager,
        {
          getColumns: () => driver.describeColumns("main", "main", "pairs"),
        },
      );
      const plan = await service.prepareDeleteRowsPlan(
        config.id,
        "main",
        "main",
        "pairs",
        [...keys, keys[0]],
      );
      if (!plan) throw new Error("Expected a delete plan");
      expect(plan.verificationCriteriaList).toEqual(keys);
      await service.executePreparedDeletePlan(plan);
      expect(
        (await driver.query("SELECT COUNT(*) FROM pairs")).rows[0]?.__col_0,
      ).toBe(0);
    } finally {
      await driver.disconnect();
    }
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
