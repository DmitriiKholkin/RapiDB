import { describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const postgresConfig = {
  id: "pg-view-options-audit-test",
  name: "Postgres View Options Audit Test",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "test_db",
  username: "user",
  password: "pass",
} as const satisfies Partial<ConnectionConfig>;

type ViewRow = {
  qualified_name: string;
  reloptions: string[] | null;
  viewdef: string;
};

function createDriver(viewRow: ViewRow | undefined) {
  const driver = new PostgresDriver(postgresConfig as ConnectionConfig);
  const query = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("pg_get_viewdef")) {
      expect(params).toEqual(["public", "v_secure"]);
      return { rows: viewRow ? [viewRow] : [] };
    }
    if (sql.includes("pg_class c")) {
      expect(params).toEqual(["public", "v_secure"]);
      return { rows: [{ relkind: "v" }] };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  (driver as unknown as { pool: { query: typeof query } }).pool = {
    query,
  } as never;
  vi.spyOn(
    driver as unknown as { createPool(): unknown },
    "createPool",
  ).mockReturnValue({
    on: vi.fn(),
    connect: async () => ({ query, release: vi.fn() }),
    end: async () => undefined,
  } as never);
  return driver;
}

describe("Postgres view DDL keeps view reloptions", () => {
  it("emits security_invoker, security_barrier and check option", async () => {
    const driver = createDriver({
      qualified_name: 'public."v_secure"',
      reloptions: [
        "security_barrier=true",
        "security_invoker=on",
        "check_option=cascaded",
      ],
      viewdef: " SELECT id\n   FROM t\n  WHERE owner = CURRENT_USER;",
    });

    await expect(
      driver.getCreateTableDDL("test_db", "public", "v_secure"),
    ).resolves.toBe(
      'CREATE OR REPLACE VIEW public."v_secure"\n' +
        "  WITH (security_barrier='true', security_invoker='on') AS\n" +
        " SELECT id\n   FROM t\n  WHERE owner = CURRENT_USER\n" +
        "  WITH CASCADED CHECK OPTION;",
    );
  });

  it("emits only the check option when it is the sole option", async () => {
    const driver = createDriver({
      qualified_name: "public.v_secure",
      reloptions: ["check_option=local"],
      viewdef: " SELECT 1;",
    });

    await expect(
      driver.getCreateTableDDL("test_db", "public", "v_secure"),
    ).resolves.toBe(
      "CREATE OR REPLACE VIEW public.v_secure AS\n SELECT 1\n  WITH LOCAL CHECK OPTION;",
    );
  });

  it("leaves views without reloptions unchanged (older servers)", async () => {
    const driver = createDriver({
      qualified_name: "public.v_secure",
      reloptions: null,
      viewdef: " SELECT 1;",
    });

    await expect(
      driver.getCreateTableDDL("test_db", "public", "v_secure"),
    ).resolves.toBe("CREATE OR REPLACE VIEW public.v_secure AS\n SELECT 1;");
  });

  it("quotes option values and rejects unsafe option names", async () => {
    const driver = createDriver({
      qualified_name: "public.v_secure",
      reloptions: ["security_barrier=it's", "bad name) AS SELECT 1; --=true"],
      viewdef: " SELECT 1;",
    });

    const ddl = await driver.getCreateTableDDL("test_db", "public", "v_secure");
    expect(ddl).toBe(
      "CREATE OR REPLACE VIEW public.v_secure\n  WITH (security_barrier='it''s') AS\n SELECT 1;",
    );
  });
});
