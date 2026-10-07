import { afterEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import type { DriverConnectionConfig } from "../../src/extension/driverRuntimeConfig";
import { logger } from "../../src/extension/utils/logger";

const harness = vi.hoisted(() => ({ pools: [] as FakePool[] }));
vi.mock("pg", async (importOriginal) => {
  const actual = await importOriginal<typeof import("pg")>();
  class MockPool {
    constructor(options: Record<string, unknown>) {
      const pool = harness.pools.shift();
      if (!pool) throw new Error("Unexpected pool creation");
      pool.options = options;
      // biome-ignore lint/correctness/noConstructorReturn: pg Pool test double
      return pool as never;
    }
  }
  return { ...actual, Pool: MockPool };
});

function deferred<T>() {
  let resolve: (value: T) => void = () => undefined;
  let reject: (error: Error) => void = () => undefined;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

class FakeClient {
  destroyed = false;
  readGate?: ReturnType<typeof deferred<void>>;
  errorSql?: RegExp;
  serverVersionNum = 120000;
  columnRows?: Array<{
    column_name: string;
    data_type: string;
    is_nullable: boolean;
    column_default?: string | null;
    generated_kind?: string | null;
    identity_kind?: string | null;
  }>;
  query = vi.fn(async (sql: string) => {
    if (this.destroyed) throw new Error("client destroyed");
    if (this.errorSql?.test(sql)) throw new Error("catalog SQL failure");
    if (this.serverVersionNum < 120000 && /\ba\.attgenerated\b/.test(sql)) {
      throw new Error("column a.attgenerated does not exist");
    }
    if (sql.includes("current_database()"))
      return { rows: [{ name: "resolved" }] };
    if (sql.includes("SELECT c.relkind")) return { rows: [{ relkind: "r" }] };
    if (sql.includes("FROM pg_attribute a")) {
      await this.readGate?.promise;
      if (this.destroyed) throw new Error("client destroyed");
      return {
        rows: this.columnRows ?? [
          { column_name: "id", data_type: "integer", is_nullable: false },
        ],
      };
    }
    if (sql.includes("pg_get_constraintdef"))
      return {
        rows: [{ constraint_name: "pk", definition: "PRIMARY KEY (id)" }],
      };
    return { rows: [] };
  });
  release = vi.fn((destroy?: boolean) => {
    if (destroy) {
      this.destroyed = true;
      this.readGate?.reject(new Error("client destroyed"));
    }
  });
}
class FakePool {
  options: Record<string, unknown> = {};
  readonly client = new FakeClient();
  serverVersionNum = 120000;
  columnRows?: Array<{
    column_name: string;
    data_type: string;
    is_nullable: boolean;
    column_default?: string | null;
    generated_kind?: string | null;
    identity_kind?: string | null;
    is_pk?: boolean;
    is_fk?: boolean;
    pk_ordinal?: number | null;
  }>;
  query = vi.fn(async (sql: string) => {
    if (this.serverVersionNum < 120000 && /\ba\.attgenerated\b/.test(sql)) {
      throw new Error("column a.attgenerated does not exist");
    }
    if (sql.includes("FROM pg_attribute a")) {
      return { rows: this.columnRows ?? [] };
    }
    return { rows: [] };
  });
  on = vi.fn();
  connect = vi.fn(async () => this.client);
  end = vi.fn(async (): Promise<void> => undefined);
}
function pool() {
  const pool = new FakePool();
  harness.pools.push(pool);
  return pool;
}
const timeouts = {
  connectionTimeoutSeconds: 1,
  connectionTimeoutMs: 25,
  dbOperationTimeoutSeconds: 1,
  dbOperationTimeoutMs: 50,
};
const drivers: PostgresDriver[] = [];
async function connect(overrides: Partial<DriverConnectionConfig> = {}) {
  const editor = pool();
  const driver = new PostgresDriver(
    {
      id: "catalog-test",
      name: "PG",
      type: "pg",
      database: "",
      host: "pg.internal",
      port: 5432,
      username: "user",
      password: "secret",
      ...overrides,
    },
    () => timeouts,
  );
  drivers.push(driver);
  await driver.connect();
  return { driver, editor };
}
async function flush() {
  for (let i = 0; i < 25; i++) await Promise.resolve();
}
afterEach(async () => {
  await Promise.all(
    drivers
      .splice(0)
      .map((driver) => driver.disconnect().catch(() => undefined)),
  );
  harness.pools.length = 0;
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("B08 owned PostgreSQL catalog session", () => {
  it.each([
    {
      serverVersionNum: 110000,
      rows: [
        {
          column_name: "plain",
          data_type: "text",
          is_nullable: true,
          column_default: "'default'",
        },
      ],
      expectedDdl: ["\"plain\" text DEFAULT 'default'"],
    },
    {
      serverVersionNum: 120000,
      rows: [
        {
          column_name: "name_lower",
          data_type: "text",
          is_nullable: true,
          column_default: "lower(name)",
          generated_kind: "v",
        },
        {
          column_name: "name_trimmed",
          data_type: "text",
          is_nullable: true,
          column_default: "trim(name)",
          generated_kind: "s",
        },
      ],
      expectedDdl: [
        '"name_lower" text GENERATED ALWAYS AS (lower(name)) VIRTUAL',
        '"name_trimmed" text GENERATED ALWAYS AS (trim(name)) STORED',
      ],
    },
  ])("reconstructs table DDL on server_version_num $serverVersionNum without a pre-12 catalog reference", async ({
    serverVersionNum,
    rows,
    expectedDdl,
  }) => {
    const { driver } = await connect();
    const catalog = pool();
    catalog.client.serverVersionNum = serverVersionNum;
    catalog.client.columnRows = rows;

    const ddl = await driver.getCreateTableDDL("", "public", "items");
    const columnQuery = catalog.client.query.mock.calls.find(([sql]) =>
      sql.includes("FROM pg_attribute a"),
    )?.[0];

    for (const expected of expectedDdl) expect(ddl).toContain(expected);
    expect(columnQuery).toContain("to_jsonb(a)->>'attgenerated'");
    expect(columnQuery).not.toMatch(/\ba\.attgenerated\b/);
  });

  it.each([
    {
      serverVersionNum: 110000,
      rows: [
        {
          column_name: "plain",
          data_type: "text",
          is_nullable: true,
          generated_kind: null,
        },
      ],
      expected: [
        { name: "plain", isComputed: false, generatedKind: undefined },
      ],
    },
    {
      serverVersionNum: 120000,
      rows: [
        {
          column_name: "name_lower",
          data_type: "text",
          is_nullable: true,
          column_default: "lower(name)",
          generated_kind: "v",
        },
        {
          column_name: "name_trimmed",
          data_type: "text",
          is_nullable: true,
          column_default: "trim(name)",
          generated_kind: "s",
        },
      ],
      expected: [
        { name: "name_lower", isComputed: true, generatedKind: "virtual" },
        { name: "name_trimmed", isComputed: true, generatedKind: "stored" },
      ],
    },
  ])("describes columns at server_version_num $serverVersionNum using a single compatible catalog query", async ({
    serverVersionNum,
    rows,
    expected,
  }) => {
    const { driver, editor } = await connect();
    editor.serverVersionNum = serverVersionNum;
    editor.columnRows = rows;

    const columns = await driver.describeColumns("", "public", "items");
    const sql = editor.query.mock.calls[0]?.[0];

    expect(
      columns.map(({ name, isComputed, generatedKind }) => ({
        name,
        isComputed,
        generatedKind,
      })),
    ).toEqual(expected);
    expect(editor.query).toHaveBeenCalledOnce();
    expect(sql).toContain("to_jsonb(a)->>'attgenerated'");
    expect(sql).not.toMatch(/\ba\.attgenerated\b/);
  });

  it.each([
    "",
    "other",
  ])("isolates DDL in database %j with the same SSH/TLS/timeout settings", async (database) => {
    const { driver, editor } = await connect({
      tls: { mode: "requireTrustServerCertificate" },
      runtimeOverrides: {
        tlsServername: "pg.internal",
        transport: {
          kind: "tcpForward",
          localHost: "127.0.0.1",
          localPort: 15432,
          remoteHost: "pg.internal",
          remotePort: 5432,
        },
      },
    });
    const catalog = pool();
    const ddl = await driver.getCreateTableDDL(database, "public", "items");
    expect(ddl).toContain('CONSTRAINT "pk" PRIMARY KEY (id)');
    expect(catalog.options).toMatchObject({
      database: database || "resolved",
      host: "pg.internal",
      port: 15432,
      stream: expect.any(Function),
      user: "user",
      password: "secret",
      max: 1,
      application_name: "RapiDB catalog DDL",
      connectionTimeoutMillis: 25,
      query_timeout: 50,
      statement_timeout: 50,
      ssl: { servername: "pg.internal", rejectUnauthorized: false },
    });
    expect(editor.options.max).toBe(5);
    expect(editor.client.query).toHaveBeenCalledExactlyOnceWith(
      "SELECT current_database() AS name",
    );
    expect(editor.connect).toHaveBeenCalledOnce();
    expect(editor.end).not.toHaveBeenCalled();
    expect(catalog.client.query.mock.calls.map(([sql]) => sql)).toEqual([
      expect.stringContaining("SELECT c.relkind"),
      "BEGIN READ ONLY",
      "SET LOCAL search_path TO pg_catalog",
      expect.stringContaining("FROM pg_attribute a"),
      expect.stringContaining("pg_get_constraintdef"),
      "ROLLBACK",
    ]);
    expect(catalog.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(catalog.end).toHaveBeenCalledOnce();
  });

  it("ends an isolated pool on connect failure and preserves the error even if end fails", async () => {
    vi.spyOn(logger, "error").mockReturnValue(new Error("logged"));
    const { driver, editor } = await connect();
    const catalog = pool();
    catalog.connect.mockRejectedValue(new Error("connect refused"));
    catalog.end.mockRejectedValue(new Error("cleanup failed"));
    await expect(
      driver.getCreateTableDDL("other", "public", "items"),
    ).rejects.toThrow("connect refused");
    expect(catalog.end).toHaveBeenCalledOnce();
    expect(catalog.client.release).not.toHaveBeenCalled();
    expect(editor.end).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(true);
  });

  it.each([
    "SELECT c.relkind",
    "FROM pg_attribute a",
  ])("closes only the catalog session after failure in %s", async (sql) => {
    const { driver, editor } = await connect();
    const catalog = pool();
    catalog.client.errorSql = new RegExp(sql);
    await expect(
      driver.getCreateTableDDL("", "public", "items"),
    ).rejects.toThrow("catalog SQL failure");
    expect(catalog.end).toHaveBeenCalledOnce();
    expect(catalog.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(editor.client.release).toHaveBeenCalledExactlyOnceWith();
    expect(editor.end).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(true);
  });

  it("returns reconstructed DDL when rollback fails after successful catalog reads", async () => {
    const { driver, editor } = await connect();
    const catalog = pool();
    catalog.client.errorSql = /^ROLLBACK$/;

    const ddl = await driver.getCreateTableDDL("", "public", "items");

    expect(ddl).toContain('CREATE TABLE "public"."items"');
    expect(catalog.client.query).toHaveBeenCalledWith("BEGIN READ ONLY");
    expect(catalog.client.query).toHaveBeenCalledWith("ROLLBACK");
    expect(catalog.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(catalog.end).toHaveBeenCalledOnce();
    expect(editor.end).not.toHaveBeenCalled();
  });

  it("preserves the catalog error when rollback also fails", async () => {
    const { driver } = await connect();
    const catalog = pool();
    catalog.client.errorSql = /FROM pg_attribute a|ROLLBACK/;

    await expect(
      driver.getCreateTableDDL("", "public", "items"),
    ).rejects.toThrow("catalog SQL failure");

    expect(catalog.client.query).toHaveBeenCalledWith("ROLLBACK");
    expect(catalog.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(catalog.end).toHaveBeenCalledOnce();
  });

  it("fences a late acquired backend after disconnect without touching a reconnected editor", async () => {
    const { driver } = await connect();
    const catalog = pool();
    const acquisition = deferred<FakeClient>();
    const ended = deferred<void>();
    catalog.connect.mockReturnValue(acquisition.promise);
    catalog.end.mockReturnValue(ended.promise);
    catalog.client.release.mockImplementation(() => ended.resolve(undefined));
    const ddl = driver.getCreateTableDDL("", "public", "items");
    const rejected = expect(ddl).rejects.toThrow("catalog session cancelled");
    const disconnect = driver.disconnect();
    await flush();
    expect(catalog.end).toHaveBeenCalledOnce();
    const fresh = pool();
    await driver.connect();
    acquisition.resolve(catalog.client);
    await Promise.all([rejected, disconnect]);
    expect(catalog.client.query).not.toHaveBeenCalled();
    expect(catalog.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(catalog.end).toHaveBeenCalledOnce();
    expect(fresh.end).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(true);
  });

  it("disconnect destroys an in-flight catalog query, releases once and ends the pool", async () => {
    const { driver } = await connect();
    const catalog = pool();
    catalog.client.readGate = deferred<void>();
    const ddl = driver.getCreateTableDDL("", "public", "items");
    const rejected = expect(ddl).rejects.toThrow("client destroyed");
    await flush();
    expect(catalog.client.query).toHaveBeenCalledWith(
      expect.stringContaining("FROM pg_attribute a"),
      ["public", "items"],
    );
    await Promise.all([driver.disconnect(), rejected]);
    expect(catalog.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(catalog.end).toHaveBeenCalledOnce();
  });

  it("closes a pending catalog connect after native connection timeout settles behind the wrapper", async () => {
    vi.useFakeTimers();
    const { driver, editor } = await connect();
    const catalog = pool();
    const acquisition = deferred<FakeClient>();
    catalog.connect.mockImplementation(() => {
      // Model pg's configured connectionTimeoutMillis, not a caller abort.
      setTimeout(
        () =>
          acquisition.reject(
            new Error("Connection terminated due to connection timeout"),
          ),
        Number(catalog.options.connectionTimeoutMillis),
      );
      return acquisition.promise;
    });
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      ...timeouts,
      dbOperationTimeoutMs: 10,
    }));
    const rejected = expect(
      wrapped.getCreateTableDDL("", "public", "items"),
    ).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(11);
    await rejected;
    await vi.advanceTimersByTimeAsync(15);
    expect(catalog.client.query).not.toHaveBeenCalled();
    expect(catalog.client.release).not.toHaveBeenCalled();
    expect(catalog.end).toHaveBeenCalledOnce();
    expect(editor.end).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(true);
  });

  it("cleans late settlement after an external DDL timeout without recycling the editor", async () => {
    vi.useFakeTimers();
    const { driver, editor } = await connect();
    const catalog = pool();
    catalog.client.readGate = deferred<void>();
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      ...timeouts,
      dbOperationTimeoutMs: 10,
    }));
    const ddl = wrapped.getCreateTableDDL("", "public", "items");
    const rejected = expect(ddl).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(11);
    await rejected;
    expect(editor.end).not.toHaveBeenCalled();
    // Model native query_timeout settling after the external wrapper's limit.
    catalog.client.readGate.reject(new Error("Query read timeout"));
    await flush();
    expect(catalog.end).toHaveBeenCalledOnce();
    expect(catalog.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(editor.end).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(true);
  });
});
