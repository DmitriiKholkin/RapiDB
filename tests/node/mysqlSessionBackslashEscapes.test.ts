import { describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";

const SCRIPT = String.raw`SELECT 'C:\'; SELECT 2;`;
const MODE_QUERY = "SELECT @@SESSION.sql_mode AS __rapidb_sql_mode";

function harness(sqlMode: string | Error) {
  const sql: string[] = [];
  const connection = {
    connection: { config: {} },
    query: vi.fn(
      async (
        options: { sql: string } | string,
      ): Promise<[unknown, unknown[]]> => {
        const text = typeof options === "string" ? options : options.sql;
        sql.push(text);
        if (text === MODE_QUERY) {
          if (sqlMode instanceof Error) throw sqlMode;
          return [[{ __rapidb_sql_mode: sqlMode }], []];
        }
        if (text === "DO 0") return [{ serverStatus: 2 }, []];
        return [[[1]], [{ name: "value", columnType: 3 }]];
      },
    ),
    destroy: vi.fn(),
    release: vi.fn(),
  };
  const driver = new MySQLDriver(
    { id: "backslash", name: "MySQL", type: "mysql", host: "localhost" },
    () => ({
      connectionTimeoutMs: 1000,
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutMs: 1000,
      dbOperationTimeoutSeconds: 1,
    }),
  );
  const tracked = driver as unknown as {
    pool: unknown;
    activeQueryConnections: Set<unknown>;
    activeQueryConnectionSlots: number;
  };
  tracked.pool = { getConnection: vi.fn(async () => connection) };
  const statements = () =>
    sql.filter((text) => text !== MODE_QUERY && text !== "DO 0");
  return { driver, connection, sql, statements, tracked };
}

describe("MySQL session NO_BACKSLASH_ESCAPES script splitting", () => {
  it("splits on the session mode when it disables backslash escapes", async () => {
    const h = harness("STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES");
    const result = await h.driver.query(SCRIPT);
    expect(result.error).toBeUndefined();
    expect(h.sql[0]).toBe(MODE_QUERY);
    expect(h.statements()).toEqual([String.raw`SELECT 'C:\'`, "SELECT 2"]);
  });

  it("keeps backslash escapes under the default session mode", async () => {
    const h = harness("STRICT_TRANS_TABLES");
    await h.driver.query(SCRIPT);
    expect(h.sql[0]).toBe(MODE_QUERY);
    expect(h.statements()).toEqual([String.raw`SELECT 'C:\'; SELECT 2;`]);
  });

  it("does not read sql_mode when the script has no backslash", async () => {
    const h = harness("NO_BACKSLASH_ESCAPES");
    await h.driver.query("SELECT 'C:'; SELECT 2;");
    expect(h.sql).not.toContain(MODE_QUERY);
    expect(h.statements()).toEqual(["SELECT 'C:'", "SELECT 2"]);
  });

  it("does not read sql_mode when backslashes cannot change boundaries", async () => {
    const h = harness("NO_BACKSLASH_ESCAPES");
    await h.driver.query(String.raw`SELECT 'a\nb'; SELECT 2;`);
    expect(h.sql).not.toContain(MODE_QUERY);
    expect(h.statements()).toEqual([String.raw`SELECT 'a\nb'`, "SELECT 2"]);
  });

  it("uses the session mode for bounded queries", async () => {
    const h = harness("NO_BACKSLASH_ESCAPES");
    const native = h.connection.connection as Record<string, unknown>;
    native.query = vi.fn((command: import("mysql2").Query) => {
      h.sql.push(Reflect.get(command, "sql"));
      queueMicrotask(() => command.emit("end"));
      return command;
    });
    await h.driver.query(SCRIPT, undefined, { hardCap: 10 });
    expect(h.sql[0]).toBe(MODE_QUERY);
    expect(h.statements()).toEqual([String.raw`SELECT 'C:\'`, "SELECT 2"]);
  });

  it("rejects ambiguous scripts that change sql_mode before running anything", async () => {
    const h = harness("NO_BACKSLASH_ESCAPES");
    await expect(
      h.driver.query(`SET SESSION sql_mode = ''; ${SCRIPT}`),
    ).rejects.toThrow(/sql_mode.*not submitted/);
    expect(h.sql).toEqual([]);
  });

  it.each([
    "SET sql_mode/**/= '';",
    "SET `sql_mode` = '';",
    "set Sql_Mode='';",
    "SET @@sql_mode = '';",
    "SET @@session.sql_mode = '';",
    "SET @@LOCAL.`sql_mode` = '';",
    "SET LOCAL sql_mode = '';",
    "SET sql_mode := '';",
    "SET STATEMENT sql_mode = '' FOR SELECT 1;",
    "SET @a = 1, sql_mode = '';",
    "SET GLOBAL sql_mode = '';",
    "/*!40101 SET sql_mode = '' */;",
    "/*!99999 SET sql_mode = '' */;",
    "/*!99999 SELECT */ SET sql_mode = '';",
    "/*!99999 CREATE */ SET sql_mode = '';",
    "/*M!100000 CREATE */ SET sql_mode = '';",
    "BEGIN NOT ATOMIC SET sql_mode = ''; END;",
    "SET @a = IF(1, 2, 3), sql_mode = '';",
    "SET @a = (SELECT 1), SESSION sql_mode = '';",
    "SET NAMES utf8mb4, sql_mode = '';",
    "SET CHARACTER SET utf8mb4, @@persist.sql_mode = '';",
  ])("rejects %s before connecting", async (prefix) => {
    const h = harness("NO_BACKSLASH_ESCAPES");
    await expect(h.driver.query(`${prefix}\n${SCRIPT}`)).rejects.toThrow(
      /sql_mode.*not submitted/,
    );
    expect(h.sql).toEqual([]);
  });

  it("rejects an assignment that only one escape mode exposes", async () => {
    const h = harness("NO_BACKSLASH_ESCAPES");
    await expect(
      h.driver.query(String.raw`SELECT 'a\'; SET sql_mode = ''; SELECT 'b\';`),
    ).rejects.toThrow(/sql_mode.*not submitted/);
    expect(h.sql).toEqual([]);
  });

  it.each([
    "-- SET sql_mode = ''\n",
    "# SET sql_mode = ''\n",
    "/* SET sql_mode = '' */ ",
    "UPDATE t SET sql_mode = 'x';\n",
    "SET @sql_mode = 'x';\n",
    "SET @old = @@sql_mode;\n",
    "SELECT 'SET sql_mode = 1';\n",
    "CREATE PROCEDURE p() SET sql_mode = '';\n",
    "SET @is_default = @@SESSION.sql_mode = '';\n",
    "SET @a = 1, @b = @@sql_mode;\n",
    "SET STATEMENT max_statement_time = 1 FOR UPDATE t SET sql_mode = 'x';\n",
  ])("does not treat %j as a sql_mode change", async (prefix) => {
    const h = harness("NO_BACKSLASH_ESCAPES");
    const result = await h.driver.query(`${prefix}${SCRIPT}`);
    expect(result.error).toBeUndefined();
    expect(h.sql[0]).toBe(MODE_QUERY);
    expect(h.statements().slice(-2)).toEqual([
      String.raw`SELECT 'C:\'`,
      "SELECT 2",
    ]);
  });

  it("does not treat sql_mode text inside an escaped literal as a change", async () => {
    const h = harness("STRICT_TRANS_TABLES");
    const result = await h.driver.query(
      String.raw`SELECT 'it\'s sql_mode ='; SELECT 2;`,
    );
    expect(result.error).toBeUndefined();
    expect(h.sql[0]).toBe(MODE_QUERY);
    expect(h.statements()).toEqual([
      String.raw`SELECT 'it\'s sql_mode ='`,
      "SELECT 2",
    ]);
  });

  it("destroys the connection when the sql_mode query fails", async () => {
    const h = harness(new Error("mode lookup failed"));
    await expect(h.driver.query(SCRIPT)).rejects.toThrow("mode lookup failed");
    expect(h.statements()).toEqual([]);
    expect(h.connection.destroy).toHaveBeenCalledTimes(1);
    expect(h.tracked.activeQueryConnections.size).toBe(0);
    expect(h.tracked.activeQueryConnectionSlots).toBe(0);
  });

  it("does not submit the script when sql_mode is unreadable", async () => {
    const h = harness(undefined as unknown as string);
    await expect(h.driver.query(SCRIPT)).rejects.toThrow(/not submitted/);
    expect(h.statements()).toEqual([]);
    expect(h.connection.destroy).toHaveBeenCalledTimes(1);
  });
});
