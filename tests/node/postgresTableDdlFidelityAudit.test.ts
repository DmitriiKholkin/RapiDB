import { afterEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";

const harness = vi.hoisted(() => ({ pools: [] as FakePool[] }));
vi.mock("pg", async (importOriginal) => {
  const actual = await importOriginal<typeof import("pg")>();
  const MockPool = vi.fn(function MockPool() {
    const pool = harness.pools.shift();
    if (!pool) throw new Error("Unexpected pool creation");
    return pool;
  });
  return { ...actual, Pool: MockPool };
});

type ColumnRow = {
  column_name: string;
  data_type: string;
  is_nullable: boolean;
  column_default?: string | null;
  generated_kind?: string | null;
  identity_kind?: string | null;
  collation_schema?: string | null;
  collation_name?: string | null;
};

class FakeClient {
  relkind = "r";
  columnRows: ColumnRow[] = [];
  query = vi.fn(async (sql: string) => {
    if (sql.includes("current_database()"))
      return { rows: [{ name: "resolved" }] };
    if (sql.includes("SELECT c.relkind"))
      return { rows: [{ relkind: this.relkind }] };
    if (sql.includes("FROM pg_attribute a")) return { rows: this.columnRows };
    return { rows: [] };
  });
  release = vi.fn();
}
class FakePool {
  readonly client = new FakeClient();
  query = vi.fn(async () => ({ rows: [] }));
  on = vi.fn();
  connect = vi.fn(async () => this.client);
  end = vi.fn(async (): Promise<void> => undefined);
}
function pool() {
  const created = new FakePool();
  harness.pools.push(created);
  return created;
}

const drivers: PostgresDriver[] = [];
async function connect() {
  pool();
  const driver = new PostgresDriver(
    {
      id: "pg-table-ddl-audit",
      name: "PG",
      type: "pg",
      database: "",
      host: "pg.internal",
      port: 5432,
      username: "user",
      password: "secret",
    },
    () => ({
      connectionTimeoutSeconds: 1,
      connectionTimeoutMs: 25,
      dbOperationTimeoutSeconds: 1,
      dbOperationTimeoutMs: 50,
    }),
  );
  drivers.push(driver);
  await driver.connect();
  return driver;
}

afterEach(async () => {
  await Promise.all(
    drivers
      .splice(0)
      .map((driver) => driver.disconnect().catch(() => undefined)),
  );
  harness.pools.length = 0;
  vi.restoreAllMocks();
});

describe("PostgreSQL table DDL fidelity", () => {
  it("does not emit an executable CREATE TABLE for a foreign table", async () => {
    const driver = await connect();
    const catalog = pool();
    catalog.client.relkind = "f";
    catalog.client.columnRows = [
      { column_name: "id", data_type: "integer", is_nullable: false },
    ];

    const ddl = await driver.getCreateTableDDL(
      "",
      "public",
      'remote\n"; DROP TABLE x; --',
    );

    expect(ddl).toMatch(/foreign table/i);
    expect(ddl).not.toMatch(/CREATE TABLE/i);
    for (const line of ddl.split(/\r?\n|\r/)) {
      expect(line.trim() === "" || line.trimStart().startsWith("--")).toBe(
        true,
      );
    }
  });

  it("keeps non-default column collations, quoted from the catalog", async () => {
    const driver = await connect();
    const catalog = pool();
    catalog.client.columnRows = [
      {
        column_name: "code",
        data_type: "text",
        is_nullable: false,
        collation_schema: "pg_catalog",
        collation_name: "C",
      },
      {
        column_name: "label",
        data_type: "character varying(20)",
        is_nullable: true,
        column_default: "'x'::character varying",
        collation_schema: 'my"schema',
        collation_name: "de-x-icu",
      },
      {
        column_name: "folded",
        data_type: "text",
        is_nullable: true,
        column_default: "lower(code)",
        generated_kind: "s",
        collation_schema: "public",
        collation_name: "nocase",
      },
      { column_name: "plain", data_type: "text", is_nullable: true },
    ];

    const ddl = await driver.getCreateTableDDL("", "public", "items");
    const columnQuery = catalog.client.query.mock.calls.find(([sql]) =>
      sql.includes("FROM pg_attribute a"),
    )?.[0];

    expect(columnQuery).toMatch(/attcollation/);
    expect(columnQuery).toMatch(/typcollation/);
    expect(ddl).toContain('"code" text COLLATE "pg_catalog"."C" NOT NULL');
    expect(ddl).toContain(
      '"label" character varying(20) COLLATE "my""schema"."de-x-icu" DEFAULT \'x\'::character varying',
    );
    expect(ddl).toContain(
      '"folded" text COLLATE "public"."nocase" GENERATED ALWAYS AS (lower(code)) STORED',
    );
    expect(ddl).toMatch(/"plain" text\n/);
  });
});
