import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../../src/extension/dbDrivers/types";
import { applyHardCapToSqlQuery } from "../../../src/extension/utils/sqlHardCap";
import type { DbEngineId } from "../../contracts/testingContracts";
import {
  createLiveDriverHarness,
  disposeLiveDriverHarness,
  fixtureRoutineName,
  fixtureSupportSummary,
  fixtureTableName,
  type LiveDriverHarnessOptions,
  rowsFromQuery,
  truthyBoolean,
} from "../../support/liveDbHarness";

class MockEventEmitter<T> {
  private listeners: Array<(value: T) => unknown> = [];

  readonly event = (listener: (value: T) => unknown) => {
    this.listeners.push(listener);
    return {
      dispose: () => {
        this.listeners = this.listeners.filter(
          (candidate) => candidate !== listener,
        );
      },
    };
  };

  fire(value: T): void {
    for (const listener of this.listeners) {
      listener(value);
    }
  }

  dispose(): void {
    this.listeners = [];
  }
}

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
}));

function findColumn(columns: ColumnTypeMeta[], name: string): ColumnTypeMeta {
  const column = columns.find(
    (candidate) => candidate.name.toLowerCase() === name.toLowerCase(),
  );

  if (!column) {
    throw new Error(`Column ${name} not found.`);
  }

  return column;
}

function getCaseInsensitive(
  row: Record<string, unknown>,
  key: string,
): unknown {
  const match = Object.keys(row).find(
    (candidate) => candidate.toLowerCase() === key.toLowerCase(),
  );
  return match ? row[match] : undefined;
}

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function sqlTimestamp(engineId: DbEngineId, iso: string): string {
  const timestamp = iso.replace("T", " ").replace(/Z$/, "");
  switch (engineId) {
    case "postgres":
      return `TIMESTAMPTZ ${sqlString(iso)}`;
    case "mysql":
    case "sqlite":
      return sqlString(timestamp);
    case "mssql":
      return `CAST(${sqlString(timestamp)} AS DATETIME2(3))`;
    case "oracle":
      return `TO_TIMESTAMP(${sqlString(timestamp)}, 'YYYY-MM-DD HH24:MI:SS.FF3')`;
  }
}

function createProbeTableSql(
  engineId: DbEngineId,
  qualifiedName: string,
): string {
  switch (engineId) {
    case "postgres":
    case "sqlite":
    case "mysql":
      return `CREATE TABLE ${qualifiedName} (id INTEGER NOT NULL PRIMARY KEY, note VARCHAR(64) NOT NULL)`;
    case "mssql":
      return `CREATE TABLE ${qualifiedName} (id INT NOT NULL PRIMARY KEY, note NVARCHAR(64) NOT NULL)`;
    case "oracle":
      return `CREATE TABLE ${qualifiedName} (ID NUMBER(10) NOT NULL PRIMARY KEY, NOTE VARCHAR2(64 CHAR) NOT NULL)`;
  }
}

function probeColumnName(
  engineId: DbEngineId,
  logicalName: "id" | "note" | "amount",
): string {
  return engineId === "oracle" ? logicalName.toUpperCase() : logicalName;
}

function createMonetaryProbeTableSql(
  engineId: DbEngineId,
  qualifiedName: string,
): string {
  switch (engineId) {
    case "postgres":
      return `CREATE TABLE ${qualifiedName} (id INTEGER NOT NULL PRIMARY KEY, amount MONEY NOT NULL)`;
    case "mssql":
      return `CREATE TABLE ${qualifiedName} (id INT NOT NULL PRIMARY KEY, amount MONEY NOT NULL)`;
    case "oracle":
      return `CREATE TABLE ${qualifiedName} (ID NUMBER(10) NOT NULL PRIMARY KEY, AMOUNT NUMBER(19,4) NOT NULL)`;
    case "mysql":
      return `CREATE TABLE ${qualifiedName} (id INT NOT NULL PRIMARY KEY, amount DECIMAL(19,4) NOT NULL)`;
    case "sqlite":
      return `CREATE TABLE ${qualifiedName} (id INTEGER NOT NULL PRIMARY KEY, amount DECIMAL(19,4) NOT NULL)`;
  }
}

function sqlNumericLiteral(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "string") {
    return sqlString(value);
  }
  throw new Error(`Unsupported numeric literal value: ${String(value)}`);
}

function parseMonetaryLike(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  const normalizedSign =
    trimmed.startsWith("(") && trimmed.endsWith(")")
      ? `-${trimmed.slice(1, -1)}`
      : trimmed;
  const normalizedDigits = normalizedSign
    .replace(/[^0-9,.-]/g, "")
    .replace(/,/g, "");
  if (
    normalizedDigits === "" ||
    normalizedDigits === "-" ||
    normalizedDigits === "."
  ) {
    return null;
  }

  const parsed = Number(normalizedDigits);
  return Number.isFinite(parsed) ? parsed : null;
}

export function registerLiveDriverConformanceTests(
  engineId: DbEngineId,
  options: LiveDriverHarnessOptions = {},
): void {
  const transport = options.transport ?? "direct";

  describe(`${engineId} live driver conformance${transport === "ssh" ? " over SSH" : ""}`, () => {
    let harness: Awaited<ReturnType<typeof createLiveDriverHarness>>;

    beforeAll(async () => {
      harness = await createLiveDriverHarness(engineId, options);
    });

    afterAll(async () => {
      await disposeLiveDriverHarness(harness);
    });

    if (["postgres", "mssql", "sqlite"].includes(engineId)) {
      it("collects original SQL with a driver-owned row budget and exact result counts", async () => {
        expect(harness.driver.getCapabilities?.().boundedQueryResults).toBe(
          true,
        );
        const source =
          "WITH src(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) SELECT n AS same, n + 10 AS same FROM src ORDER BY n";
        const result = await harness.driver.query(source, undefined, {
          hardCap: 2,
        });
        expect(result).toMatchObject({
          columns: ["same", "same"],
          rowCount: 4,
          truncated: true,
        });
        expect(
          result.rows.map((row) => [Number(row.__col_0), Number(row.__col_1)]),
        ).toEqual([
          [1, 11],
          [2, 12],
        ]);
        const empty = await harness.driver.query(
          "SELECT 1 AS empty_result WHERE 1=0",
          undefined,
          { hardCap: 2 },
        );
        expect(empty).toMatchObject({
          columns: ["empty_result"],
          rows: [],
          rowCount: 0,
          truncated: false,
        });
        const parameterized = await harness.driver.query(
          engineId === "postgres"
            ? "SELECT $1::int AS bound_value"
            : "SELECT ? AS bound_value",
          [7],
          { hardCap: 2 },
        );
        expect(parameterized).toMatchObject({
          columns: ["bound_value"],
          rowCount: 1,
          truncated: false,
        });
        expect(Number(parameterized.rows[0].__col_0)).toBe(7);
      });

      it("finishes native mutations and RETURNING/OUTPUT after the retained row budget fills", async () => {
        const table = harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          `rapidb_stream_probe_${Date.now()}`,
        );
        await harness.driver.query(`CREATE TABLE ${table} (id INTEGER)`);
        try {
          const statements =
            engineId === "mssql"
              ? [
                  `INSERT INTO ${table} (id) OUTPUT inserted.id VALUES (1), (2), (3), (4), (5)`,
                  `UPDATE ${table} SET id = id + 100 OUTPUT inserted.id`,
                  `DELETE FROM ${table} OUTPUT deleted.id`,
                ]
              : [
                  `INSERT INTO ${table} (id) VALUES (1), (2), (3), (4), (5) RETURNING id`,
                  `UPDATE ${table} SET id = id + 100 RETURNING id`,
                  `DELETE FROM ${table} RETURNING id`,
                ];
          for (let index = 0; index < statements.length; index++) {
            const result = await harness.driver.query(
              statements[index],
              undefined,
              { hardCap: 2 },
            );
            expect(result.rows).toHaveLength(2);
            expect(result).toMatchObject({
              rowCount: 5,
              affectedRows: 5,
              truncated: true,
            });
            const state = await harness.driver.query(
              `SELECT COUNT(*) AS count_value, SUM(id) AS sum_value FROM ${table}`,
            );
            expect(Number(state.rows[0].__col_0)).toBe(index === 2 ? 0 : 5);
            if (index === 1) expect(Number(state.rows[0].__col_1)).toBe(515);
          }
        } finally {
          await harness.driver.query(`DROP TABLE ${table}`);
        }
      });
    }

    if (engineId === "postgres") {
      it("drains many native PostgreSQL result sets and returns only the bounded last command", async () => {
        const source = Array.from(
          { length: 25 },
          (_, i) => `SELECT ${i} AS batch_number, generate_series(1, 500) AS n`,
        ).join("; ");
        const result = await harness.driver.query(source, undefined, {
          hardCap: 3,
        });
        expect(result).toMatchObject({ rowCount: 500, truncated: true });
        expect(result.rows).toEqual([
          { __col_0: 24, __col_1: 1 },
          { __col_0: 24, __col_1: 2 },
          { __col_0: 24, __col_1: 3 },
        ]);
        for (const sql of [
          "SHOW ALL",
          "EXPLAIN SELECT * FROM generate_series(1, 20) AS n WHERE n > 0 ORDER BY n",
        ]) {
          const full = await harness.driver.query(sql);
          const bounded = await harness.driver.query(sql, undefined, {
            hardCap: 2,
          });
          expect(full.rows.length).toBeGreaterThan(2);
          expect(bounded).toMatchObject({
            rowCount: full.rows.length,
            truncated: true,
          });
          expect(bounded.rows).toEqual(full.rows.slice(0, 2));
        }
      });
    }

    if (engineId === "postgres" || engineId === "mssql") {
      it("propagates late native errors after a full budget and recovers the pool", async () => {
        const source =
          engineId === "postgres"
            ? "SELECT generate_series(1, 20); DO $$ BEGIN RAISE EXCEPTION 'budget late failure'; END $$;"
            : "WITH c(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) SELECT n FROM c; THROW 50000, 'budget late failure', 1;";
        await expect(
          harness.driver.query(source, undefined, { hardCap: 2 }),
        ).rejects.toThrow("budget late failure");
        const next = await harness.driver.query(
          "SELECT 7 AS recovered",
          undefined,
          { hardCap: 2 },
        );
        expect(Number(next.rows[0].__col_0)).toBe(7);
        expect(next.truncated).toBe(false);
      });

      if (transport === "direct") {
        it("keeps native timeouts effective during bounded draining and recovers afterward", async () => {
          const timeouts = () => ({
            connectionTimeoutSeconds: 5,
            connectionTimeoutMs: 5000,
            dbOperationTimeoutSeconds: 1,
            dbOperationTimeoutMs: 1000,
          });
          const timed =
            engineId === "postgres"
              ? new PostgresDriver(harness.connection, timeouts)
              : new MSSQLDriver(harness.connection, timeouts);
          await timed.connect();
          try {
            const source =
              engineId === "postgres"
                ? "SELECT generate_series(1, 20); SELECT pg_sleep(3)"
                : "WITH c(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) SELECT n FROM c; WAITFOR DELAY '00:00:03';";
            await expect(
              timed.query(source, undefined, { hardCap: 2, requestToken: 81 }),
            ).rejects.toThrow(/timeout|timed out|cancel/i);
            expect(
              Number(
                (
                  await timed.query("SELECT 7 AS recovered", undefined, {
                    hardCap: 2,
                  })
                ).rows[0].__col_0,
              ),
            ).toBe(7);
          } finally {
            await timed.disconnect();
          }
        });
      }
    }

    if (engineId === "mssql") {
      it("bounds native trigger results and drains STATISTICS PROFILE result sets", async () => {
        const tableName = `rapidb_trigger_budget_${Date.now()}`;
        const table = harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          tableName,
        );
        const trigger = `${harness.driver.quoteIdentifier(harness.schemaName)}.${harness.driver.quoteIdentifier(`${tableName}_trg`)}`;
        await harness.driver.query(`CREATE TABLE ${table} (id INTEGER)`);
        try {
          await harness.driver.query(
            `CREATE TRIGGER ${trigger} ON ${table} AFTER INSERT AS BEGIN SET NOCOUNT ON; SELECT inserted.id FROM inserted CROSS JOIN (VALUES (1), (2), (3), (4)) AS n(value); END`,
          );
          const result = await harness.driver.query(
            `INSERT INTO ${table} VALUES (1), (2), (3), (4), (5)`,
            undefined,
            { hardCap: 2 },
          );
          expect(result).toMatchObject({ rowCount: 20, truncated: true });
          expect(result.rows).toHaveLength(2);
          expect(
            Number(
              (await harness.driver.query(`SELECT COUNT(*) FROM ${table}`))
                .rows[0].__col_0,
            ),
          ).toBe(5);
          const profile = await harness.driver.query(
            `SET STATISTICS PROFILE ON; SELECT id FROM ${table} ORDER BY id; SET STATISTICS PROFILE OFF;`,
            undefined,
            { hardCap: 2 },
          );
          expect(profile).toMatchObject({
            columns: ["id"],
            rowCount: 5,
            truncated: true,
          });
          expect(profile.rows).toHaveLength(2);
          const json = await harness.driver.query(
            `SELECT id FROM ${table} ORDER BY id FOR JSON PATH`,
            undefined,
            { hardCap: 1 },
          );
          expect(json).toMatchObject({ rowCount: 1, truncated: false });
          expect(JSON.parse(String(json.rows[0].__col_0))).toHaveLength(5);
          const xml = await harness.driver.query(
            `SELECT id AS [@id] FROM ${table} ORDER BY id FOR XML PATH('item')`,
            undefined,
            { hardCap: 1 },
          );
          expect(xml).toMatchObject({ rowCount: 1, truncated: false });
          expect(String(xml.rows[0].__col_0).match(/<item /g)).toHaveLength(5);
        } finally {
          await harness.driver.query(`DROP TABLE ${table}`);
        }
      });
    }

    it("executes query editor hard-cap SQL for standard and recursive CTEs", async () => {
      const fromDual = engineId === "oracle" ? " FROM dual" : "";
      const recursive = ["postgres", "mysql", "sqlite"].includes(engineId)
        ? "RECURSIVE "
        : "";
      for (const source of [
        `WITH src(n) AS (SELECT 1${fromDual} UNION ALL SELECT 2${fromDual} UNION ALL SELECT 3${fromDual}), final_src AS (SELECT n FROM src) SELECT n FROM final_src ORDER BY n`,
        `WITH ${recursive}seq(n) AS (SELECT 1${fromDual} UNION ALL SELECT n + 1 FROM seq WHERE n < 50) SELECT n FROM seq ORDER BY n`,
      ]) {
        const rewrite = applyHardCapToSqlQuery(
          source,
          harness.connection.type,
          2,
        );
        expect(rewrite.error).toBeUndefined();
        expect(rewrite.decision.applied).toBe(true);
        const result = await harness.driver.query(rewrite.queryText);
        expect(result.rows).toHaveLength(2);
        expect(result.rows.map((row) => Number(row.__col_0))).toEqual([1, 2]);
      }
    });

    it("executes query editor hard-cap SQL for parentheses and quoted comment markers", async () => {
      const fromDual = engineId === "oracle" ? " FROM dual" : "";
      const source = `((SELECT ';--/* literal */' AS label${fromDual})); -- real comment`;
      const rewrite = applyHardCapToSqlQuery(
        source,
        harness.connection.type,
        2,
      );
      expect(rewrite.error).toBeUndefined();
      const result = await harness.driver.query(rewrite.queryText);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].__col_0).toBe(";--/* literal */");
    });

    if (engineId !== "mssql") {
      it("executes preflighted ordinary scripts with bounded result statements", async () => {
        const fromDual = engineId === "oracle" ? " FROM dual" : "";
        const source = `SELECT 'first; result' AS label${fromDual}; WITH c(n) AS (SELECT 1${fromDual} UNION ALL SELECT 2${fromDual} UNION ALL SELECT 3${fromDual}) SELECT n FROM c ORDER BY n;`;
        const rewrite = applyHardCapToSqlQuery(
          source,
          harness.connection.type,
          2,
        );
        expect(rewrite.error).toBeUndefined();
        const result = await harness.driver.query(rewrite.queryText);
        expect(result.rows.map((row) => Number(row.__col_0))).toEqual([1, 2]);
      });
    }

    if (engineId === "sqlite" || engineId === "mysql") {
      it("preserves duplicate result names, set ordering and LIMIT offsets", async () => {
        const prefix =
          "WITH src(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) ";
        for (const suffix of ["", " LIMIT 100 OFFSET 1", " LIMIT 1, 100"]) {
          const source = `${prefix}SELECT n AS same, n + 10 AS same FROM src ORDER BY n${suffix}`;
          const rewrite = applyHardCapToSqlQuery(
            source,
            harness.connection.type,
            2,
          );
          expect(rewrite.error).toBeUndefined();
          const result = await harness.driver.query(rewrite.queryText);
          expect(result.columns).toEqual(["same", "same"]);
          expect(result.rows.map((row) => Number(row.__col_0))).toEqual(
            suffix ? [2, 3] : [1, 2],
          );
          expect(Number(result.rows[0].__col_1)).toBe(suffix ? 12 : 11);
        }
        const commented = applyHardCapToSqlQuery(
          `${prefix}SELECT n FROM src -- comment\r ORDER BY n`,
          harness.connection.type,
          2,
        );
        expect(
          (await harness.driver.query(commented.queryText)).rows,
        ).toHaveLength(2);
      });
    }

    if (engineId === "sqlite") {
      it("executes bounded metadata PRAGMAs and preserves empty-result columns", async () => {
        for (const source of [
          "PRAGMA main.table_info('sqlite_schema')",
          "PRAGMA table_info = sqlite_schema",
          "PRAGMA compile_options",
        ]) {
          const original = await harness.driver.query(source);
          const rewrite = applyHardCapToSqlQuery(source, "sqlite", 2);
          expect(rewrite.error).toBeUndefined();
          const bounded = await harness.driver.query(rewrite.queryText);
          expect(bounded.columns).toEqual(original.columns);
          expect(bounded.rows).toEqual(original.rows.slice(0, 2));
        }
        const empty = applyHardCapToSqlQuery(
          "SELECT 1 AS same, 2 AS same WHERE 0",
          "sqlite",
          2,
        );
        const result = await harness.driver.query(empty.queryText);
        expect(result.columns).toEqual(["same", "same"]);
        expect(result.rows).toEqual([]);
        // A bare CR does not end a native SQLite -- comment. Check the native
        // prepared path as well as the rewritten script path.
        const native = await harness.driver.query(
          "SELECT ? AS n -- comment\r UNION ALL SELECT 2",
          [1],
        );
        const capped = applyHardCapToSqlQuery(
          "SELECT 1 AS n -- comment\r UNION ALL SELECT 2",
          "sqlite",
          2,
        );
        expect(native.rows).toHaveLength(1);
        expect((await harness.driver.query(capped.queryText)).rows).toEqual(
          native.rows,
        );
        // Raw result metadata must not break the driver's existing fallback
        // from non-reader PRAGMAs to run().
        await expect(
          harness.driver.query("PRAGMA user_version = 41"),
        ).resolves.toMatchObject({ columns: [], rows: [] });
        await harness.driver.query("PRAGMA user_version = 0");
      });

      it("executes VALUES CTEs, compound VALUES, unlimited LIMIT and bounded scripts", async () => {
        for (const source of [
          "WITH c(n) AS (VALUES (1), (2), (3)) SELECT n FROM c ORDER BY n",
          "SELECT 1 AS n UNION ALL VALUES (2), (3)",
          "WITH c(n) AS (VALUES (1), (2), (3)) SELECT n FROM c LIMIT -1",
          "SELECT 99; WITH c(n) AS (VALUES (1), (2), (3)) SELECT n FROM c ORDER BY n;",
        ]) {
          const rewrite = applyHardCapToSqlQuery(source, "sqlite", 2);
          expect(rewrite.error).toBeUndefined();
          const result = await harness.driver.query(rewrite.queryText);
          expect(result.rows.map((row) => Number(row.__col_0))).toEqual([1, 2]);
        }
      });
    }

    if (engineId === "mssql") {
      it("executes TOP, scalar subqueries and OFFSET/FETCH caps", async () => {
        const prefix =
          "WITH src(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) ";
        for (const [query, expected] of [
          ["SELECT TOP/* count */(100)n FROM src ORDER BY n", [1, 2]],
          [
            "SELECT n, (SELECT MAX(n) FROM src) AS maximum FROM src ORDER BY n",
            [1, 2],
          ],
          ["SELECT n FROM src ORDER BY n OFFSET 1 ROWS", [2, 3]],
          [
            "SELECT n FROM src ORDER BY n OFFSET 1 ROWS FETCH NEXT 100 ROWS ONLY",
            [2, 3],
          ],
        ] as const) {
          const rewrite = applyHardCapToSqlQuery(prefix + query, "mssql", 2);
          expect(rewrite.error).toBeUndefined();
          const result = await harness.driver.query(rewrite.queryText);
          expect(result.rows.map((row) => Number(row.__col_0))).toEqual(
            expected,
          );
        }
      });
    }

    if (engineId === "postgres") {
      it("caps RETURNING while completing every mutation", async () => {
        const table = harness.driver.qualifiedTableName(
          harness.databaseName,
          harness.schemaName,
          `rapidb_cap_probe_${Date.now()}`,
        );
        await harness.driver.query(`CREATE TABLE ${table} (id integer)`);
        try {
          for (const [source, expectedCount] of [
            [`INSERT INTO ${table} VALUES (1), (2), (3), (4) RETURNING id`, 4],
            [
              `WITH changed AS (UPDATE ${table} SET id = id + 10 RETURNING id) SELECT * FROM changed`,
              4,
            ],
            [`DELETE FROM ${table} RETURNING id`, 0],
          ] as const) {
            const rewrite = applyHardCapToSqlQuery(source, "pg", 2);
            expect(rewrite.error).toBeUndefined();
            expect(
              (await harness.driver.query(rewrite.queryText)).rows,
            ).toHaveLength(2);
            const count = await harness.driver.query(
              `SELECT count(*) FROM ${table}`,
            );
            expect(Number(count.rows[0].__col_0)).toBe(expectedCount);
            if (source.startsWith("WITH")) {
              const updated = await harness.driver.query(
                `SELECT count(*) FROM ${table} WHERE id >= 11`,
              );
              expect(Number(updated.rows[0].__col_0)).toBe(4);
            }
          }
        } finally {
          await harness.driver.query(`DROP TABLE ${table}`);
        }
      });
    }

    it("connects, disconnects, and lists the seeded fixture namespace", async () => {
      expect(harness.driver.isConnected()).toBe(true);

      await harness.driver.disconnect();
      expect(harness.driver.isConnected()).toBe(false);

      await harness.driver.connect();
      expect(harness.driver.isConnected()).toBe(true);

      const databases = await harness.driver.listDatabases();
      expect(databases.length).toBeGreaterThan(0);
      expect(
        databases.some(
          (database) =>
            database.name.toLowerCase() === harness.databaseName.toLowerCase(),
        ),
      ).toBe(true);

      const schemas = await harness.driver.listSchemas(harness.databaseName);
      expect(schemas.length).toBeGreaterThan(0);
      expect(
        schemas.some(
          (schema) =>
            schema.name.toLowerCase() === harness.schemaName.toLowerCase(),
        ),
      ).toBe(true);

      const objects = await harness.driver.listObjects(
        harness.databaseName,
        harness.schemaName,
      );
      expect(
        objects.some(
          (object) =>
            object.name.toLowerCase() ===
              fixtureTableName(engineId, "fixtureRows").toLowerCase() &&
            object.type === "table",
        ),
      ).toBe(true);
    });

    it("describes columns, indexes, foreign keys, ddl, and routine definitions", async () => {
      const fixtureRows = fixtureTableName(engineId, "fixtureRows");
      const parentRecords = fixtureTableName(engineId, "parentRecords");
      const childRecords = fixtureTableName(engineId, "childRecords");

      const describedTable = await harness.driver.describeTable(
        harness.databaseName,
        harness.schemaName,
        fixtureRows,
      );
      const describedColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureRows,
      );
      const indexes = await harness.driver.getIndexes(
        harness.databaseName,
        harness.schemaName,
        parentRecords,
      );
      const foreignKeys = await harness.driver.getForeignKeys(
        harness.databaseName,
        harness.schemaName,
        childRecords,
      );
      const ddl = await harness.driver.getCreateTableDDL(
        harness.databaseName,
        harness.schemaName,
        fixtureRows,
      );

      expect(describedTable.map((column) => column.name.toLowerCase())).toEqual(
        expect.arrayContaining(["id", "display_name", "amount"]),
      );
      expect(findColumn(describedColumns, "id").isPrimaryKey).toBe(true);
      expect(findColumn(describedColumns, "notes").nullable).toBe(true);
      expect(
        indexes.some(
          (index) =>
            index.unique &&
            index.columns.some(
              (columnName) => columnName.toLowerCase() === "code",
            ),
        ),
      ).toBe(true);
      expect(
        foreignKeys.some(
          (foreignKey) =>
            foreignKey.column.toLowerCase() === "parent_id" &&
            foreignKey.referencedTable.toLowerCase() ===
              fixtureTableName(engineId, "parentRecords").toLowerCase(),
        ),
      ).toBe(true);
      expect(ddl.toLowerCase()).toContain(fixtureRows.toLowerCase());

      const support = fixtureSupportSummary(engineId);
      if (support.routines) {
        const definition = await harness.driver.getRoutineDefinition(
          harness.databaseName,
          harness.schemaName,
          fixtureRoutineName(engineId, "totalAmount"),
          "function",
        );

        expect(definition.length).toBeGreaterThan(0);
        expect(definition.toLowerCase()).toMatch(/fixture|sum|create/);
      }
    });

    it("executes select, ddl, dml, multi-statement scripts, and transactions", async () => {
      const fixtureRowsColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "fixtureRows"),
      );
      const fixtureRowsIdColumn = findColumn(fixtureRowsColumns, "id");
      const transactionProbeColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "transactionProbe"),
      );
      const transactionProbeIdColumn = findColumn(
        transactionProbeColumns,
        "id",
      );
      const transactionProbeAccountNameColumn = findColumn(
        transactionProbeColumns,
        "account_name",
      );
      const transactionProbeBalanceColumn = findColumn(
        transactionProbeColumns,
        "balance",
      );
      const transactionProbeUpdatedAtColumn = findColumn(
        transactionProbeColumns,
        "updated_at",
      );
      const fixtureRowsTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "fixtureRows"),
      );
      const transactionProbeTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "transactionProbe"),
      );
      const selected = await harness.driver.query(
        `SELECT * FROM ${fixtureRowsTable} ORDER BY ${harness.driver.quoteIdentifier(fixtureRowsIdColumn.name)}`,
      );

      expect(selected.rowCount).toBe(2);
      expect(selected.columns.length).toBeGreaterThanOrEqual(6);

      const probeTableName = `rapidb_driver_probe_${engineId}_${Date.now()}`;
      const qualifiedProbeTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        probeTableName,
      );
      const probeIdColumn = probeColumnName(engineId, "id");
      const probeNoteColumn = probeColumnName(engineId, "note");
      await harness.driver.query(
        createProbeTableSql(engineId, qualifiedProbeTable),
      );
      await harness.driver.query(
        `INSERT INTO ${qualifiedProbeTable} (${harness.driver.quoteIdentifier(probeIdColumn)}, ${harness.driver.quoteIdentifier(probeNoteColumn)}) VALUES (1, ${sqlString("probe row")})`,
      );
      const probeSelect = await harness.driver.query(
        `SELECT ${harness.driver.quoteIdentifier(probeNoteColumn)} AS probe_note FROM ${qualifiedProbeTable}`,
      );
      expect(
        getCaseInsensitive(rowsFromQuery(probeSelect)[0] ?? {}, "probe_note"),
      ).toBe("probe row");
      await harness.driver.query(`DROP TABLE ${qualifiedProbeTable}`);

      const scriptId = 950_000 + Math.floor(Math.random() * 10_000);
      const scriptResult = await harness.driver.query(
        `INSERT INTO ${transactionProbeTable} (${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeAccountNameColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeUpdatedAtColumn.name)}) VALUES (${scriptId}, ${sqlString("Script Probe")}, 10.00, ${sqlTimestamp(engineId, "2026-04-21T10:00:00.000Z")});\nUPDATE ${transactionProbeTable} SET ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} = ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} + 5.25 WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${scriptId};\nSELECT ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} AS probe_balance FROM ${transactionProbeTable} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${scriptId}`,
      );
      expect(
        Number(
          getCaseInsensitive(
            rowsFromQuery(scriptResult)[0] ?? {},
            "probe_balance",
          ),
        ),
      ).toBeCloseTo(15.25, 2);

      const transactionId = scriptId + 1;
      await harness.driver.query(
        `INSERT INTO ${transactionProbeTable} (${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeAccountNameColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeUpdatedAtColumn.name)}) VALUES (${transactionId}, ${sqlString("Tx Probe")}, 20.00, ${sqlTimestamp(engineId, "2026-04-21T10:00:01.000Z")})`,
      );
      await harness.driver.runTransaction([
        {
          sql: `UPDATE ${transactionProbeTable} SET ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} = ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} + 1 WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${transactionId}`,
          checkAffectedRows: true,
        },
        {
          sql: `UPDATE ${transactionProbeTable} SET ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} = ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} + 2 WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${transactionId}`,
          checkAffectedRows: true,
        },
      ]);
      const transactionCheck = await harness.driver.query(
        `SELECT ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} AS probe_balance FROM ${transactionProbeTable} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${transactionId}`,
      );
      expect(
        Number(
          getCaseInsensitive(
            rowsFromQuery(transactionCheck)[0] ?? {},
            "probe_balance",
          ),
        ),
      ).toBeCloseTo(23, 2);

      await harness.driver.query(
        `DELETE FROM ${transactionProbeTable} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} IN (${scriptId}, ${transactionId})`,
      );
    });

    it("supports qualified names, pagination SQL, input coercion, and persisted edit checks", async () => {
      const paginationTable = fixtureTableName(engineId, "paginationRows");
      const paginationQualifiedName = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        paginationTable,
      );
      expect(paginationQualifiedName.length).toBeGreaterThan(0);

      const pagination = harness.driver.buildPagination(12, 5, 1);
      const paginationColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        paginationTable,
      );
      const paginationIdColumn = findColumn(paginationColumns, "id");
      const paginationResult = await harness.driver.query(
        `SELECT ${harness.driver.quoteIdentifier(paginationIdColumn.name)} AS row_id FROM ${paginationQualifiedName} ORDER BY ${harness.driver.quoteIdentifier(paginationIdColumn.name)} ${pagination.sql}`,
        pagination.params,
      );
      expect(
        Number(
          getCaseInsensitive(
            rowsFromQuery(paginationResult)[0] ?? {},
            "row_id",
          ),
        ),
      ).toBe(13);

      const fixtureColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "fixtureRows"),
      );
      const boolColumn = findColumn(fixtureColumns, "is_active");
      expect(
        truthyBoolean(harness.driver.coerceInputValue("true", boolColumn)),
      ).toBe(true);

      const numericColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "exactNumericSamples"),
      );
      const exactAmountColumn = findColumn(numericColumns, "exact_amount");
      const ratioColumn = findColumn(numericColumns, "ratio");
      const persistedCheck = harness.driver.checkPersistedEdit(
        exactAmountColumn,
        "123.450000",
        { persistedValue: "123.450000" },
      );
      expect(persistedCheck?.ok ?? true).toBe(true);

      if (engineId !== "sqlite") {
        const exactNumericResult = await harness.driver.query(
          `SELECT ${harness.driver.quoteIdentifier(exactAmountColumn.name)}, ${harness.driver.quoteIdentifier(ratioColumn.name)} FROM ${harness.driver.qualifiedTableName(harness.databaseName, harness.schemaName, fixtureTableName(engineId, "exactNumericSamples"))} ORDER BY ${harness.driver.quoteIdentifier(findColumn(numericColumns, "id").name)}`,
        );
        const exactNumericRows = rowsFromQuery(exactNumericResult);
        expect(
          String(getCaseInsensitive(exactNumericRows[0] ?? {}, "exact_amount")),
        ).toBe("123456789012.123456");
        expect(
          String(getCaseInsensitive(exactNumericRows[0] ?? {}, "ratio")),
        ).toBe("1.2500");
        expect(
          String(getCaseInsensitive(exactNumericRows[1] ?? {}, "exact_amount")),
        ).toBe("-45.600100");
        expect(
          String(getCaseInsensitive(exactNumericRows[2] ?? {}, "ratio")),
        ).toBe("0.3333");
      }

      const moneyProbeTable = `rapidb_money_probe_${engineId}_${Date.now()}`;
      const qualifiedMoneyProbeTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        moneyProbeTable,
      );
      const moneyIdColumn = probeColumnName(engineId, "id");
      const moneyAmountColumn = probeColumnName(engineId, "amount");
      const moneyProbeDescribeName =
        engineId === "oracle" ? moneyProbeTable.toUpperCase() : moneyProbeTable;
      await harness.driver.query(
        createMonetaryProbeTableSql(engineId, qualifiedMoneyProbeTable),
      );

      try {
        await harness.driver.query(
          `INSERT INTO ${qualifiedMoneyProbeTable} (${harness.driver.quoteIdentifier(moneyIdColumn)}, ${harness.driver.quoteIdentifier(moneyAmountColumn)}) VALUES (1, 10.25)`,
        );

        const moneyColumns = await harness.driver.describeColumns(
          harness.databaseName,
          harness.schemaName,
          moneyProbeDescribeName,
        );
        const amountColumn =
          moneyColumns.find(
            (candidate) => candidate.name.toLowerCase() === "amount",
          ) ??
          (engineId === "oracle"
            ? {
                ...exactAmountColumn,
                name: moneyAmountColumn,
                nativeType: "NUMBER(19,4)",
                category: "decimal" as const,
              }
            : findColumn(moneyColumns, "amount"));

        expect(amountColumn.category).toBe("decimal");

        const coercedMoneyInput = harness.driver.coerceInputValue(
          "1234.56",
          amountColumn,
        );

        await harness.driver.query(
          `UPDATE ${qualifiedMoneyProbeTable} SET ${harness.driver.quoteIdentifier(moneyAmountColumn)} = ${sqlNumericLiteral(coercedMoneyInput)} WHERE ${harness.driver.quoteIdentifier(moneyIdColumn)} = 1`,
        );

        const moneySelect = await harness.driver.query(
          `SELECT ${harness.driver.quoteIdentifier(moneyAmountColumn)} AS probe_amount FROM ${qualifiedMoneyProbeTable} WHERE ${harness.driver.quoteIdentifier(moneyIdColumn)} = 1`,
        );
        const rawMoneyValue = getCaseInsensitive(
          rowsFromQuery(moneySelect)[0] ?? {},
          "probe_amount",
        );
        const formattedMoneyValue = harness.driver.formatOutputValue(
          rawMoneyValue,
          amountColumn,
        );

        const numericMoneyValue = parseMonetaryLike(formattedMoneyValue);
        expect(numericMoneyValue).not.toBeNull();
        expect(numericMoneyValue).toBeCloseTo(1234.56, 2);
      } finally {
        await harness.driver.query(`DROP TABLE ${qualifiedMoneyProbeTable}`);
      }
    });
  });
}
