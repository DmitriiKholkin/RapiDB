import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";
import { TableDataService } from "../../src/extension/tableDataService";

const sqliteConfig = {
  id: "sqlite-hardening",
  name: "sqlite-hardening",
  type: "sqlite" as const,
  filePath: "/tmp/rapidb-sqlite-hardening.db",
};

function setSqliteDb(driver: SQLiteDriver, db: unknown): void {
  (driver as unknown as { db: unknown }).db = db;
}

function makeConnectionManager(
  connectionId: string,
  driver: IDBDriver,
): ConnectionManager {
  return {
    getConnection: () => ({
      ...sqliteConfig,
      id: connectionId,
      filePath: sqliteConfig.filePath,
    }),
    getDriver: () => driver,
  } as unknown as ConnectionManager;
}

describe("SQLite hardening", () => {
  it("treats WITH-prefixed INSERT statements as DML", async () => {
    const driver = new SQLiteDriver(sqliteConfig);
    const calls: string[] = [];

    setSqliteDb(driver, {
      isOpen: true,
      all: (sql: string) => {
        calls.push(`all:${sql}`);
        throw new Error("DML should fall back to run()");
      },
      run: (sql: string) => {
        calls.push(`run:${sql}`);
        return { changes: 2 };
      },
    });

    await expect(
      driver.query(
        "WITH seed(value) AS (SELECT 1) INSERT INTO logs(value) SELECT value FROM seed",
      ),
    ).resolves.toEqual({
      columns: [],
      rows: [],
      rowCount: 2,
      executionTimeMs: expect.any(Number),
      affectedRows: 2,
    });

    expect(calls).toEqual([
      "run:WITH seed(value) AS (SELECT 1) INSERT INTO logs(value) SELECT value FROM seed",
    ]);
  });

  it("uses raw exec for trigger scripts instead of unsafe statement splitting", async () => {
    const driver = new SQLiteDriver(sqliteConfig);
    const executed: string[] = [];

    setSqliteDb(driver, {
      isOpen: true,
      exec: (sql: string) => {
        executed.push(sql);
      },
      all: () => {
        throw new Error(
          "split execution should not be used for trigger scripts",
        );
      },
      run: () => {
        throw new Error(
          "split execution should not be used for trigger scripts",
        );
      },
    });

    const triggerScript = `
      CREATE TRIGGER audit_events_after_insert
      AFTER INSERT ON events
      BEGIN
        INSERT INTO audit_log(event_id, action) VALUES (NEW.id, 'insert');
        UPDATE counters SET total = total + 1;
      END;
    `;

    await expect(driver.query(triggerScript)).resolves.toEqual({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: expect.any(Number),
    });
    expect(executed).toEqual([triggerScript]);
  });

  it("does not mistake RETURNING inside a string literal for a RETURNING clause", async () => {
    const driver = new SQLiteDriver(sqliteConfig);
    const calls: string[] = [];

    setSqliteDb(driver, {
      isOpen: true,
      all: (sql: string) => {
        calls.push(`all:${sql}`);
        throw new Error("string literals must not force db.all() for DML");
      },
      run: (sql: string) => {
        calls.push(`run:${sql}`);
        return { changes: 1 };
      },
    });

    await expect(
      driver.query("INSERT INTO logs(message) VALUES ('RETURNING soon')"),
    ).resolves.toEqual({
      columns: [],
      rows: [],
      rowCount: 1,
      executionTimeMs: expect.any(Number),
      affectedRows: 1,
    });

    expect(calls).toEqual([
      "run:INSERT INTO logs(message) VALUES ('RETURNING soon')",
    ]);
  });

  it("ignores block comments when a script contains no executable SQL", async () => {
    const driver = new SQLiteDriver(sqliteConfig);
    const calls: string[] = [];

    setSqliteDb(driver, {
      isOpen: true,
      all: (sql: string) => {
        calls.push(`all:${sql}`);
        return [];
      },
      run: (sql: string) => {
        calls.push(`run:${sql}`);
        return { changes: 0 };
      },
    });

    await expect(driver.query("/* block comment only */")).resolves.toEqual({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    });

    expect(calls).toEqual([]);
  });

  it("uses table_xinfo metadata, preserves PK ordinals, and keeps generated columns read-only", async () => {
    const driver = new SQLiteDriver(sqliteConfig);

    setSqliteDb(driver, {
      isOpen: true,
      all: (sql: string) => {
        if (sql.startsWith("PRAGMA table_xinfo")) {
          return [
            {
              name: "id",
              type: "INTEGER",
              notnull: 1,
              dflt_value: null,
              pk: 1,
              hidden: 0,
            },
            {
              name: "event_time",
              type: "TIME(3)",
              notnull: 0,
              dflt_value: null,
              pk: 0,
              hidden: 0,
            },
            {
              name: "event_at",
              type: "TIMESTAMP(6)",
              notnull: 0,
              dflt_value: null,
              pk: 0,
              hidden: 0,
            },
            {
              name: "note_lower",
              type: "TEXT",
              notnull: 0,
              dflt_value: null,
              pk: 0,
              hidden: 3,
            },
            {
              name: "fts_rank",
              type: "TEXT",
              notnull: 0,
              dflt_value: null,
              pk: 0,
              hidden: 1,
            },
          ];
        }

        if (sql.startsWith("PRAGMA foreign_key_list")) {
          return [{ from: "event_time" }];
        }

        if (sql.startsWith("SELECT sql FROM sqlite_master")) {
          return [
            {
              sql: "CREATE TABLE events (id INTEGER PRIMARY KEY, event_time TIME, event_at TIMESTAMP, note_lower TEXT GENERATED ALWAYS AS (lower(event_at)) STORED)",
            },
          ];
        }

        return [];
      },
    });

    await expect(
      driver.describeTable("main", "main", "events"),
    ).resolves.toEqual([
      {
        name: "id",
        type: "INTEGER",
        nullable: false,
        defaultValue: undefined,
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
        isForeignKey: false,
        isAutoIncrement: false,
      },
      {
        name: "event_time",
        type: "TIME(3)",
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: true,
        isAutoIncrement: false,
      },
      {
        name: "event_at",
        type: "TIMESTAMP(6)",
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        isAutoIncrement: false,
      },
      {
        name: "note_lower",
        type: "TEXT",
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        isAutoIncrement: false,
      },
    ]);

    const columns = await driver.describeColumns("main", "main", "events");
    expect(columns.map((column) => column.name)).toEqual([
      "id",
      "event_time",
      "event_at",
      "note_lower",
    ]);
    expect(columns.find((column) => column.name === "note_lower")).toEqual(
      expect.objectContaining({ editable: false }),
    );
    expect(columns.find((column) => column.name === "event_time")).toEqual(
      expect.objectContaining({
        filterOperators: [
          "eq",
          "neq",
          "between",
          "like",
          "is_null",
          "is_not_null",
        ],
      }),
    );
    expect(columns.find((column) => column.name === "event_at")).toEqual(
      expect.objectContaining({
        filterOperators: [
          "eq",
          "neq",
          "between",
          "like",
          "is_null",
          "is_not_null",
        ],
      }),
    );
  });

  it("falls back to LIKE for non-canonical temporal equality filters", () => {
    const driver = new SQLiteDriver(sqliteConfig);

    expect(
      driver.buildFilterCondition(
        {
          name: "event_at",
          type: "TIMESTAMP",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "datetime",
          nativeType: "TIMESTAMP",
          filterable: true,
          editable: true,
          filterOperators: ["eq", "like"],
          valueSemantics: "plain",
        },
        "eq",
        "April 15 2026 10:30",
        1,
      ),
    ).toEqual({
      sql: '"event_at" LIKE ?',
      params: ["%April 15 2026 10:30%"],
    });
  });

  it("rejects invalid datetime range filters instead of using lexical BETWEEN", () => {
    const driver = new SQLiteDriver(sqliteConfig);

    expect(() =>
      buildWhere(
        driver,
        [
          {
            column: "event_at",
            operator: "between",
            value: ["2026-04-15 10:30:00", "not-a-datetime"],
          },
        ],
        [
          {
            name: "event_at",
            type: "TIMESTAMP",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            category: "datetime",
            nativeType: "TIMESTAMP",
            filterable: true,
            editable: true,
            filterOperators: ["between"],
            valueSemantics: "plain",
          },
        ],
      ),
    ).toThrow(
      "[RapiDB Filter] Column event_at expects a valid datetime value.",
    );
  });
});

describe("SQLite round-trip hardening", () => {
  let tempDir = "";
  let dbPath = "";
  let driver: SQLiteDriver;
  let service: TableDataService;
  let connectionId = "sqlite-roundtrip";

  beforeEach(async () => {
    tempDir = mkdtempSync(join(tmpdir(), "rapidb-sqlite-"));
    dbPath = join(tempDir, "roundtrip.db");
    connectionId = `sqlite-roundtrip-${Date.now()}`;
    driver = new SQLiteDriver({
      ...sqliteConfig,
      id: connectionId,
      filePath: dbPath,
    });
    await driver.connect();

    service = new TableDataService(makeConnectionManager(connectionId, driver));

    await driver.query(`
      PRAGMA foreign_keys = ON;
      CREATE TABLE events (
        id INTEGER PRIMARY KEY,
        created_on DATE NOT NULL,
        created_at DATETIME NOT NULL,
        starts_at TIME NOT NULL,
        note TEXT NOT NULL,
        note_lower TEXT GENERATED ALWAYS AS (lower(note)) STORED
      );
      INSERT INTO events (created_on, created_at, starts_at, note)
      VALUES
        ('2026-04-15', '2026-04-15 10:30:00', '10:30:00', 'Alpha'),
        ('2026-04-16', '2026-04-16 14:45:00', '14:45:00', 'Beta');
    `);
  });

  afterEach(async () => {
    try {
      await driver.disconnect();
    } catch {}
    rmSync(tempDir, { recursive: true, force: true });
    (
      TableDataService as unknown as { _colCache: Map<string, unknown> }
    )._colCache.clear();
  });

  it("supports schema display, insert, update, and canonical temporal filtering", async () => {
    const columns = await service.getColumns(
      connectionId,
      "main",
      "main",
      "events",
    );

    expect(columns.find((column) => column.name === "id")).toEqual(
      expect.objectContaining({
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
        isAutoIncrement: false,
      }),
    );
    expect(columns.find((column) => column.name === "note_lower")).toEqual(
      expect.objectContaining({ editable: false }),
    );

    await service.insertRow(connectionId, "main", "main", "events", {
      created_on: "2026-04-17",
      created_at: "2026-04-17 09:15:00",
      starts_at: "09:15:00",
      note: "Gamma",
      note_lower: "SHOULD_NOT_BE_WRITTEN",
    });

    let page = await service.getPage(
      connectionId,
      "main",
      "main",
      "events",
      1,
      50,
      [
        {
          column: "created_at",
          operator: "eq",
          value: "2026-04-17 09:15:00",
        },
      ],
    );

    expect(page.rows).toEqual([
      {
        id: 3,
        created_on: "2026-04-17",
        created_at: "2026-04-17 09:15:00",
        starts_at: "09:15:00",
        note: "Gamma",
        note_lower: "gamma",
      },
    ]);

    await service.updateRow(
      connectionId,
      "main",
      "main",
      "events",
      { id: 3 },
      {
        note: "Gamma Updated",
        note_lower: "IGNORED_ON_UPDATE",
      },
    );

    page = await service.getPage(
      connectionId,
      "main",
      "main",
      "events",
      1,
      50,
      [
        {
          column: "starts_at",
          operator: "between",
          value: ["09:00:00", "11:00:00"],
        },
      ],
    );

    expect(page.rows).toEqual([
      {
        id: 1,
        created_on: "2026-04-15",
        created_at: "2026-04-15 10:30:00",
        starts_at: "10:30:00",
        note: "Alpha",
        note_lower: "alpha",
      },
      {
        id: 3,
        created_on: "2026-04-17",
        created_at: "2026-04-17 09:15:00",
        starts_at: "09:15:00",
        note: "Gamma Updated",
        note_lower: "gamma updated",
      },
    ]);
  });

  it("maps JSON and UUID columns correctly and uses PK ordinals for default ordering", async () => {
    await driver.query(`
      CREATE TABLE docs (
        user_id INTEGER NOT NULL,
        tenant_id INTEGER NOT NULL,
        external_id UUID NOT NULL,
        payload JSON NOT NULL,
        PRIMARY KEY (tenant_id, user_id)
      );
      INSERT INTO docs (user_id, tenant_id, external_id, payload)
      VALUES
        (2, 1, 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', '{"name":"second"}'),
        (1, 2, 'cccccccc-cccc-4ccc-8ccc-cccccccccccc', '{"name":"third"}'),
        (1, 1, 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', '{"name":"first"}');
    `);

    const columns = await service.getColumns(
      connectionId,
      "main",
      "main",
      "docs",
    );

    expect(columns.find((column) => column.name === "payload")).toEqual(
      expect.objectContaining({
        category: "json",
        filterOperators: ["like", "in", "is_null", "is_not_null"],
      }),
    );
    expect(columns.find((column) => column.name === "external_id")).toEqual(
      expect.objectContaining({
        category: "uuid",
        filterOperators: ["like", "in", "is_null", "is_not_null"],
      }),
    );
    expect(columns.find((column) => column.name === "tenant_id")).toEqual(
      expect.objectContaining({ isPrimaryKey: true, primaryKeyOrdinal: 1 }),
    );
    expect(columns.find((column) => column.name === "user_id")).toEqual(
      expect.objectContaining({ isPrimaryKey: true, primaryKeyOrdinal: 2 }),
    );

    await service.insertRow(connectionId, "main", "main", "docs", {
      user_id: 2,
      tenant_id: 2,
      external_id: "550e8400-e29b-41d4-a716-446655440000",
      payload: '{"name":"inserted"}',
    });

    await service.updateRow(
      connectionId,
      "main",
      "main",
      "docs",
      { tenant_id: 2, user_id: 2 },
      { payload: '{"name":"updated"}' },
    );

    const orderedPage = await service.getPage(
      connectionId,
      "main",
      "main",
      "docs",
      1,
      50,
      [],
    );

    expect(
      orderedPage.rows.map(
        (row) => `${String(row.tenant_id)}:${String(row.user_id)}`,
      ),
    ).toEqual(["1:1", "1:2", "2:1", "2:2"]);

    const filteredPage = await service.getPage(
      connectionId,
      "main",
      "main",
      "docs",
      1,
      50,
      [{ column: "external_id", operator: "like", value: "550e8400" }],
    );

    expect(filteredPage.rows).toEqual([
      {
        user_id: 2,
        tenant_id: 2,
        external_id: "550e8400-e29b-41d4-a716-446655440000",
        payload: '{"name":"updated"}',
      },
    ]);
  });
});
