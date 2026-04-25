import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

interface DriverBehavior {
  connectError?: Error;
}

const driverBehaviors = new Map<string, DriverBehavior>();
const driverInstances: FakeDriver[] = [];

class MockEventEmitter<T> {
  private readonly listeners = new Set<(value: T) => void>();

  readonly event = (listener: (value: T) => void) => {
    this.listeners.add(listener);
    return {
      dispose: () => {
        this.listeners.delete(listener);
      },
    };
  };

  fire(value: T): void {
    for (const listener of this.listeners) {
      listener(value);
    }
  }
}

class FakeDriver implements IDBDriver {
  connectCalls = 0;
  disconnectCalls = 0;
  private connected = false;

  constructor(readonly config: { id: string }) {
    driverInstances.push(this);
  }

  async connect(): Promise<void> {
    this.connectCalls += 1;
    const behavior = driverBehaviors.get(this.config.id);
    if (behavior?.connectError) {
      throw behavior.connectError;
    }
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.disconnectCalls += 1;
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  async listDatabases() {
    return [{ name: "main", schemas: [] }];
  }

  async listSchemas() {
    return [{ name: "public" }];
  }

  async listObjects() {
    return [];
  }

  async describeTable(): Promise<ColumnMeta[]> {
    return [];
  }

  async describeColumns(): Promise<ColumnTypeMeta[]> {
    return [];
  }

  async getIndexes() {
    return [];
  }

  async getForeignKeys() {
    return [];
  }

  async getCreateTableDDL(): Promise<string> {
    return "";
  }

  async getRoutineDefinition(): Promise<string> {
    return "";
  }

  async query() {
    return {
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    };
  }

  async runTransaction(): Promise<void> {}

  quoteIdentifier(name: string): string {
    return `"${name}"`;
  }

  qualifiedTableName(_database: string, schema: string, table: string): string {
    return `${schema}.${table}`;
  }

  buildPagination(offset: number, limit: number) {
    return {
      sql: `LIMIT ${limit} OFFSET ${offset}`,
      params: [],
    };
  }

  buildOrderByDefault(): string {
    return "ORDER BY 1";
  }

  coerceInputValue(value: unknown): unknown {
    return value;
  }

  formatOutputValue(value: unknown): unknown {
    return value;
  }

  checkPersistedEdit() {
    return null;
  }

  normalizeFilterValue(
    _column: ColumnTypeMeta,
    _operator: never,
    value: string | [string, string] | undefined,
  ) {
    return value;
  }

  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: never,
    value: string | [string, string] | undefined,
  ) {
    return {
      sql: `${column.name}:${String(operator)}`,
      params: value === undefined ? [] : Array.isArray(value) ? value : [value],
    };
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, paramIndex: number): string {
    return `$${paramIndex}`;
  }

  buildInsertDefaultValuesSql(qualifiedTableName: string): string {
    return `INSERT INTO ${qualifiedTableName} DEFAULT VALUES`;
  }

  buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = $${paramIndex}`;
  }

  materializePreviewSql(sql: string): string {
    return sql;
  }
}

function driverFactory() {
  return {
    MSSQLDriver: FakeDriver,
    MySQLDriver: FakeDriver,
    OracleDriver: FakeDriver,
    PostgresDriver: FakeDriver,
    SQLiteDriver: FakeDriver,
  };
}

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

vi.mock("../../src/extension/dbDrivers/mssql", driverFactory);
vi.mock("../../src/extension/dbDrivers/mysql", driverFactory);
vi.mock("../../src/extension/dbDrivers/oracle", driverFactory);
vi.mock("../../src/extension/dbDrivers/postgres", driverFactory);
vi.mock("../../src/extension/dbDrivers/sqlite", driverFactory);

beforeEach(() => {
  driverBehaviors.clear();
  driverInstances.splice(0, driverInstances.length);
  vi.resetModules();
});

describe("ConnectionManager", () => {
  it("deduplicates concurrent connect attempts for the same connection", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    const first = manager.beginConnect("conn-1");
    const second = manager.beginConnect("conn-1");

    expect(first.isNew).toBe(true);
    expect(second.isNew).toBe(false);

    await Promise.all([first.promise, second.promise]);

    expect(driverInstances).toHaveLength(1);
    expect(driverInstances[0]?.connectCalls).toBe(1);
    expect(manager.isConnected("conn-1")).toBe(true);
  });

  it("disconnects an edited connection after saving updated settings", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
      },
    ]);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    await manager.connectTo("conn-1");

    const connectedDriver = driverInstances[0];
    expect(connectedDriver?.isConnected()).toBe(true);

    await manager.saveConnection({
      id: "conn-1",
      name: "Primary Updated",
      type: "pg",
      database: "next_db",
    });

    expect(connectedDriver?.disconnectCalls).toBe(1);
    expect(manager.isConnected("conn-1")).toBe(false);
    expect(store.getConnections()[0]?.name).toBe("Primary Updated");
  });

  it("removes a connection and purges associated history, bookmarks, and secrets", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setConnections([
      {
        id: "conn-1",
        name: "Primary",
        type: "sqlite",
        useSecretStorage: true,
      },
      {
        id: "conn-2",
        name: "Secondary",
        type: "sqlite",
      },
    ]);
    store.setHistory([
      {
        id: "h1",
        connectionId: "conn-1",
        sql: "select 1",
        executedAt: "2026-04-21T00:00:00.000Z",
      },
      {
        id: "h2",
        connectionId: "conn-2",
        sql: "select 2",
        executedAt: "2026-04-21T00:00:01.000Z",
      },
    ]);
    store.setBookmarks([
      {
        id: "b1",
        connectionId: "conn-1",
        sql: "select 1",
        savedAt: "2026-04-21T00:00:00.000Z",
      },
      {
        id: "b2",
        connectionId: "conn-2",
        sql: "select 2",
        savedAt: "2026-04-21T00:00:01.000Z",
      },
    ]);
    store.setSecret("conn-1", "shh");

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await expect(manager.removeConnection("conn-1")).resolves.toBe(true);

    expect(store.getConnections().map((connection) => connection.id)).toEqual([
      "conn-2",
    ]);
    expect(store.readHistory().map((entry) => entry.connectionId)).toEqual([
      "conn-2",
    ]);
    expect(store.readBookmarks().map((entry) => entry.connectionId)).toEqual([
      "conn-2",
    ]);
    await expect(store.getSecret("conn-1")).resolves.toBeUndefined();
  });

  it("disconnects a failed test connection and returns a normalized error", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    driverBehaviors.set("__test__", {
      connectError: new Error("Cannot connect"),
    });

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
    );

    const result = await manager.testConnection({
      name: "Broken",
      type: "mysql",
    });

    expect(result.success).toBe(false);
    expect(result.error).toContain("Cannot connect");
    expect(driverInstances).toHaveLength(1);
    expect(driverInstances[0]?.disconnectCalls).toBe(1);
  });

  it("deduplicates identical trailing history entries and trims to the configured limit", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );

    const store = new FakeConnectionManagerStore();
    store.setHistoryLimit(2);

    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );

    await manager.addToHistory("conn-1", "  select 1  ");
    await manager.addToHistory("conn-1", "select 1");
    await manager.addToHistory("conn-1", "select 2");
    await manager.addToHistory("conn-1", "select 3");

    expect(store.readHistory().map((entry) => entry.sql)).toEqual([
      "select 3",
      "select 2",
    ]);
  });
});
