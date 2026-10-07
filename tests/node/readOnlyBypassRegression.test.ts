import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import {
  MySQLDriver,
  splitMySQLScript,
} from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ReadOnlyQueryGuard } from "../../src/extension/dbDrivers/types";
import {
  mayChangeDatabaseSchema,
  sqlReadOnlyQueryGuard,
} from "../../src/extension/utils/readOnlyGuards";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function pgGuard(): ReadOnlyQueryGuard {
  const driver = new PostgresDriver({
    id: "pg",
    name: "pg",
    type: "pg",
    host: "localhost",
    port: 5432,
    database: "postgres",
    username: "u",
    password: "p",
  } as ConnectionConfig);
  const guard = driver.getCapabilities().readOnlyQueryGuard;
  if (!guard) throw new Error("missing pg guard");
  return guard;
}

function mysqlGuard(): ReadOnlyQueryGuard {
  const driver = new MySQLDriver({
    id: "mysql",
    name: "mysql",
    type: "mysql",
    host: "localhost",
    port: 3306,
    database: "db",
    username: "u",
    password: "p",
  } as ConnectionConfig);
  const guard = driver.getCapabilities().readOnlyQueryGuard;
  if (!guard) throw new Error("missing mysql guard");
  return guard;
}

function mssqlGuard(): ReadOnlyQueryGuard {
  const driver = new MSSQLDriver({
    id: "mssql",
    name: "mssql",
    type: "mssql",
    host: "localhost",
    port: 1433,
    database: "master",
    username: "u",
    password: "p",
  } as ConnectionConfig);
  const guard = driver.getCapabilities().readOnlyQueryGuard;
  if (!guard) throw new Error("missing mssql guard");
  return guard;
}

describe("readOnly bypass regressions (stage 1)", () => {
  it("denies DML hidden in subqueries", () => {
    const guard = pgGuard();
    expect(
      guard("SELECT * FROM (DELETE FROM users RETURNING *) AS x").allowed,
    ).toBe(false);
    expect(
      guard("SELECT * FROM (INSERT INTO t (a) VALUES (1) RETURNING *) AS x")
        .allowed,
    ).toBe(false);
    expect(
      guard("SELECT * FROM (UPDATE t SET a = 1 RETURNING *) AS x").allowed,
    ).toBe(false);
    expect(
      guard("WITH moved AS (DELETE FROM users RETURNING *) SELECT * FROM moved")
        .allowed,
    ).toBe(false);
  });

  it("still allows plain subselects", () => {
    const guard = pgGuard();
    expect(guard("SELECT * FROM (SELECT 1 AS a) AS x")).toEqual({
      allowed: true,
    });
    expect(guard("WITH src AS (SELECT 1 AS a) SELECT * FROM src")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT a, b FROM t WHERE x IN (SELECT y FROM u)")).toEqual({
      allowed: true,
    });
  });

  it("denies mutating functions", () => {
    const guard = pgGuard();
    expect(guard("SELECT nextval('my_seq')").allowed).toBe(false);
    expect(guard("SELECT setval('my_seq', 1)").allowed).toBe(false);
    expect(guard("SELECT pg_terminate_backend(1234)").allowed).toBe(false);
    expect(guard("SELECT * FROM t WHERE id = nextval('s')").allowed).toBe(
      false,
    );
    expect(guard("SELECT dblink('c', 'SELECT 1')").allowed).toBe(false);
    expect(guard("SELECT set_config('a', 'b', false)").allowed).toBe(false);
    expect(guard("SELECT lo_create(1)").allowed).toBe(false);
    expect(guard("VALUES (nextval('s'))").allowed).toBe(false);
    expect(guard("VALUES (pg_terminate_backend(1))").allowed).toBe(false);
    expect(guard("SELECT pg_catalog.nextval('s')").allowed).toBe(false);
    expect(guard("SELECT NEXTVAL('s')").allowed).toBe(false);
    // Bare name without call is a column, not a mutation.
    expect(guard("SELECT nextval FROM t")).toEqual({ allowed: true });
  });

  it("denies quoted mutating-function calls", () => {
    const guard = pgGuard();
    expect(guard("SELECT \"nextval\"('s')").allowed).toBe(false);
    expect(guard('SELECT "pg_terminate_backend"(1)').allowed).toBe(false);
    expect(guard("SELECT `nextval`('s')").allowed).toBe(false);
    expect(guard("SELECT [nextval](1)").allowed).toBe(false);
    expect(guard("SELECT pg_catalog.\"nextval\"('s')").allowed).toBe(false);
    // Quoted non-call stays allowed.
    expect(guard('SELECT "nextval" FROM t')).toEqual({ allowed: true });
  });

  it("denies quoted calls with comments between name and paren", () => {
    const guard = pgGuard();
    expect(guard("SELECT \"nextval\"/*x*/('s')").allowed).toBe(false);
    expect(guard("SELECT \"nextval\"--x\n('s')").allowed).toBe(false);
    expect(guard("SELECT `nextval`/*x*/('s')").allowed).toBe(false);
    expect(guard("SELECT [nextval]/*x*/(1)").allowed).toBe(false);
    expect(guard("SELECT pg_catalog.\"nextval\"/*x*/('s')").allowed).toBe(
      false,
    );
    expect(guard('SELECT "dblink_foo"/*x*/(1)').allowed).toBe(false);
    expect(guard("VALUES (\"nextval\"/*x*/('s'))").allowed).toBe(false);
  });

  it("denies extended mutating-function families", () => {
    const guard = pgGuard();
    expect(guard("SELECT pg_notify('c', 'm')").allowed).toBe(false);
    expect(guard("SELECT pg_advisory_lock(1)").allowed).toBe(false);
    expect(guard("SELECT pg_try_advisory_lock(1, 2)").allowed).toBe(false);
    expect(guard("SELECT pg_advisory_unlock(1)").allowed).toBe(false);
    expect(guard("SELECT lo_open(1, 131072)").allowed).toBe(false);
    expect(guard("SELECT lowrite(1, 'x')").allowed).toBe(false);
    expect(guard("SELECT lo_truncate(1, 0)").allowed).toBe(false);
    expect(guard("SELECT dblink_connect('c', 'host=localhost')").allowed).toBe(
      false,
    );
    expect(guard("SELECT dblink_send_query('c', 'SELECT 1')").allowed).toBe(
      false,
    );
  });

  it("denies PostgreSQL large-object creation and modification in read-only queries", () => {
    const guard = pgGuard();
    const mutatingQueries = [
      "SELECT lo_from_bytea(0, decode('4142', 'hex'))",
      "SELECT lo_put(123, 0, decode('4142', 'hex'))",
      "SELECT pg_catalog.lo_from_bytea(0, decode('41', 'hex'))",
      "SELECT pg_catalog.\"lo_put\"(123, 0, decode('41', 'hex'))",
      "SELECT \"lo_from_bytea\"/* comment */(0, decode('41', 'hex'))",
      "WITH obj AS (SELECT lo_from_bytea(0, decode('41', 'hex'))) SELECT * FROM obj",
      "VALUES (lo_put(123, 0, decode('41', 'hex')))",
      "SELECT ARRAY[lo_put(123, 0, decode('41', 'hex'))]",
    ];
    for (const sql of mutatingQueries) {
      expect(guard(sql), sql).toHaveProperty("allowed", false);
      expect(sqlReadOnlyQueryGuard(sql, "postgresql"), sql).toHaveProperty(
        "allowed",
        false,
      );
    }

    expect(guard("SELECT lo_get(123)")).toEqual({ allowed: true });
    expect(guard("SELECT 'lo_put(123, 0, data)' AS example")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT lo_put FROM function_names")).toEqual({
      allowed: true,
    });
  });

  it("does not flag function names inside literals", () => {
    const guard = pgGuard();
    expect(guard("SELECT 'nextval(''x'')'")).toEqual({ allowed: true });
  });

  it("fails closed on backslash-escaped quotes with uncertain SQL string modes", () => {
    const pgEscape = String.raw`SELECT E'\'' , nextval('my_seq')`;
    const mysqlEscape = String.raw`SELECT '\'' /*!50000 INTO @a */`;
    expect(pgGuard()(pgEscape).allowed).toBe(false);
    expect(mysqlGuard()(mysqlEscape).allowed).toBe(false);
    expect(mayChangeDatabaseSchema(mysqlEscape, "mysql")).toBe(true);
    expect(pgGuard()(String.raw`SELECT E'it\'s nextval(1)'`).allowed).toBe(
      false,
    );
    expect(pgGuard()("SELECT 'plain string' AS value").allowed).toBe(true);
    expect(
      sqlReadOnlyQueryGuard(String.raw`SELECT 'C:\' AS path`, "transactsql"),
    ).toEqual({ allowed: true });
  });

  it("fails closed on unterminated comments, strings, and quoted identifiers", () => {
    const cases: Array<
      readonly [
        string,
        "postgresql" | "mysql" | "sqlite" | "transactsql" | "plsql",
      ]
    > = [
      ["SELECT 1 /* unterminated ; DELETE FROM t", "postgresql"],
      ["SELECT 1 /* unterminated ; DELETE FROM t", "mysql"],
      ["SELECT 'unterminated; DELETE FROM t", "sqlite"],
      ['SELECT "unterminated; DELETE FROM t', "postgresql"],
      ["SELECT `unterminated; DELETE FROM t", "mysql"],
      ["SELECT [unterminated; DELETE FROM t", "transactsql"],
      ["SELECT 'unterminated; DELETE FROM t", "plsql"],
    ];

    for (const [sql, dialect] of cases) {
      expect(
        sqlReadOnlyQueryGuard(sql, dialect),
        `${dialect}: ${sql}`,
      ).toHaveProperty("allowed", false);
    }
  });

  it("recognizes dollar-quoted strings only in PostgreSQL and requires a close", () => {
    const guard = pgGuard();
    expect(
      guard("SELECT $$DELETE; INSERT INTO t VALUES (1)$$ AS example"),
    ).toEqual({
      allowed: true,
    });
    expect(guard("SELECT $body$DELETE FROM t$body$ AS example")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT 1; $body$; DELETE FROM t").allowed).toBe(false);

    const mysqlScript =
      "SELECT 1 AS $tag$; CREATE TEMPORARY TABLE rapidb_temp (id INT);";
    expect(sqlReadOnlyQueryGuard(mysqlScript, "mysql").allowed).toBe(false);
    expect(splitMySQLScript(mysqlScript)).toEqual([
      "SELECT 1 AS $tag$",
      "CREATE TEMPORARY TABLE rapidb_temp (id INT)",
    ]);
  });

  it("recognizes PostgreSQL dollar tags with Unicode identifier characters", () => {
    const guard = pgGuard();
    const combiningTag = "$e\u0301٢_$";

    expect(guard("SELECT $é$DELETE FROM users$é$ AS payload")).toEqual({
      allowed: true,
    });
    expect(
      guard(
        `SELECT ${combiningTag}DELETE FROM users${combiningTag} AS payload`,
      ),
    ).toEqual({ allowed: true });
    expect(guard("SELECT $é$DELETE FROM users$É$ AS payload").allowed).toBe(
      false,
    );
    expect(guard("SELECT $é$DELETE FROM users").allowed).toBe(false);
  });

  it("does not strip MySQL subtraction as a -- comment", () => {
    expect(mysqlGuard()("SELECT 1--1 INTO @a").allowed).toBe(false);
    expect(sqlReadOnlyQueryGuard("SELECT 1--1 INTO @a").allowed).toBe(false);
    expect(mysqlGuard()("SELECT 1-- actual comment\n").allowed).toBe(true);
  });

  it("blocks MySQL session mutations that survive READ ONLY rollback and pooling", () => {
    const guard = mysqlGuard();
    expect(guard("SELECT @rapidb_value := 1").allowed).toBe(false);
    expect(guard("SELECT GET_LOCK('rapidb-test-lock', 0)").allowed).toBe(false);
    expect(guard("SELECT RELEASE_LOCK('rapidb-test-lock')").allowed).toBe(
      false,
    );
    expect(guard("SELECT `GET_LOCK`('rapidb-test-lock', 0)").allowed).toBe(
      false,
    );
    expect(guard("SELECT LAST_INSERT_ID(1)").allowed).toBe(false);
    expect(guard("SELECT LAST_INSERT_ID()")).toEqual({ allowed: true });
    expect(guard("SELECT @rapidb_value")).toEqual({ allowed: true });
  });

  it.each([
    "SELECT LAST_INSERT_ID('123')",
    "SELECT LAST_INSERT_ID('')",
    "SELECT LAST_INSERT_ID(' ') ",
    'SELECT LAST_INSERT_ID("123")',
    "SELECT LAST_INSERT_ID(`id`) FROM users",
    "SELECT LAST_INSERT_ID(/* before */ '123' /* after */)",
    "SELECT LAST_INSERT_ID -- before\n('123' -- after\n)",
    "SELECT LAST_INSERT_ID # before\n('123' # after\n)",
    "SELECT LAST_INSERT_ID('123''456')",
    "SELECT LAST_INSERT_ID('/* not a comment */')",
    "SELECT LAST_INSERT_ID('); SELECT 1; --')",
    "SELECT `LAST_INSERT_ID`('123')",
  ])("rejects quoted MySQL LAST_INSERT_ID arguments: %s", (sql) => {
    expect(mysqlGuard()(sql).allowed).toBe(false);
  });

  it.each([
    "SELECT LAST_INSERT_ID()",
    "SELECT LAST_INSERT_ID( /* no argument */ )",
    "SELECT LAST_INSERT_ID # before\n( # inside\n)",
    "SELECT LAST_INSERT_ID -- before\n( -- inside\n)",
    "SELECT 'LAST_INSERT_ID(123)' AS example",
    'SELECT "LAST_INSERT_ID(123)" AS example',
    "SELECT 1 /* LAST_INSERT_ID('123') */",
    "SELECT 1 # LAST_INSERT_ID('123')\n",
    "SELECT 'DELETE FROM users; DROP TABLE users' AS example",
  ])("preserves harmless MySQL literals and empty calls: %s", (sql) => {
    expect(mysqlGuard()(sql)).toEqual({ allowed: true });
  });

  it("blocks MySQL named locks concealed by # comments but respects quoted hashes", () => {
    const guard = mysqlGuard();
    const sql = "SELECT GET_LOCK #x\n('rapidb',0)";

    expect(guard(sql).allowed).toBe(false);
    const splitStatement = splitMySQLScript(sql)[0];
    expect(splitStatement).toMatch(/^SELECT GET_LOCK\s*\('rapidb',0\)$/i);
    expect(guard("SELECT 1 # GET_LOCK('rapidb',0)\n").allowed).toBe(true);
    expect(guard("SELECT '# GET_LOCK(''rapidb'',0)' AS example").allowed).toBe(
      true,
    );
    expect(guard("SELECT 'first line\n# still a string' AS example")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT `column#name` FROM items")).toEqual({
      allowed: true,
    });
  });

  it.each([
    "\n",
    "\r\n",
    "\r",
  ])("does not consume the line ending after an empty MySQL hash comment %j", (lineEnding) => {
    const guard = mysqlGuard();
    expect(
      guard(`SELECT 1 #${lineEnding}; COMMIT; DELETE FROM items;`).allowed,
    ).toBe(false);
    expect(
      mayChangeDatabaseSchema(
        `SELECT 1 #${lineEnding}; CREATE TABLE items (id INT);`,
        "mysql",
      ),
    ).toBe(true);
    expect(
      guard(`SELECT \`GET_LOCK\` #${lineEnding}('rapidb', 0)`).allowed,
    ).toBe(false);
    expect(guard(`SELECT 1 #${lineEnding}; SELECT 2;`)).toEqual({
      allowed: true,
    });
  });

  it.each([
    "\n",
    "\r\n",
  ])("agrees with the MySQL splitter after an empty hash comment %j", (lineEnding) => {
    const script = `SELECT 1 #${lineEnding}; COMMIT; DELETE FROM items;`;
    expect(splitMySQLScript(script)).toEqual([
      "SELECT 1",
      "COMMIT",
      "DELETE FROM items",
    ]);
    expect(mysqlGuard()(script).allowed).toBe(false);
  });

  it("blocks T-SQL NEXT VALUE FOR sequence advancement without matching literals", () => {
    const guard = mssqlGuard();
    expect(guard("SELECT NEXT VALUE FOR dbo.orders_seq").allowed).toBe(false);
    expect(
      guard("SELECT next /* comment */ value for [dbo].[orders_seq]").allowed,
    ).toBe(false);
    expect(
      guard(
        "WITH next_seq AS (SELECT NEXT VALUE FOR dbo.orders_seq) SELECT * FROM next_seq",
      ).allowed,
    ).toBe(false);
    expect(guard("SELECT 'NEXT VALUE FOR dbo.orders_seq' AS example")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT next_value FROM orders")).toEqual({ allowed: true });
  });

  it.each([
    "SELECT 1, 序列.NEXTVAL FROM dual",
    "SELECT 1, ακολουθία.NEXTVAL FROM dual",
    "SELECT 1, последовательность.NEXTVAL FROM dual",
    "SELECT 1, 序列 /* sequence */ . /* advance */ NEXTVAL FROM dual",
    "SELECT 1, 模式.序列.NEXTVAL FROM dual",
    "SELECT (序列.NEXTVAL) FROM dual",
    'SELECT 1, "序列".NEXTVAL FROM dual',
  ])("blocks Oracle Unicode sequence advancement: %s", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "plsql").allowed).toBe(false);
  });

  it.each([
    "SELECT 1, 序列.CURRVAL FROM dual",
    "SELECT 1, 序列.值 FROM 表",
    "SELECT '序列.NEXTVAL' AS example FROM dual",
    "SELECT 1 /* 序列.NEXTVAL */ FROM dual",
    'SELECT "序列.NEXTVAL" FROM items',
  ])("preserves harmless Oracle Unicode identifiers and literals: %s", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "plsql")).toEqual({ allowed: true });
  });

  it("denies encoded PostgreSQL mutating function names", () => {
    const encoded = String.raw`SELECT U&"pg_advisory_lo\0063k"(1)`;
    expect(pgGuard()(encoded).allowed).toBe(false);
    expect(mayChangeDatabaseSchema(encoded, "postgresql")).toBe(true);
  });

  it("does not hide mutating functions in PostgreSQL array brackets", () => {
    expect(pgGuard()("SELECT ARRAY[pg_try_advisory_lock(42)]").allowed).toBe(
      false,
    );
    expect(
      sqlReadOnlyQueryGuard("SELECT ARRAY[pg_try_advisory_lock(42)]").allowed,
    ).toBe(false);
    expect(pgGuard()("SELECT ARRAY[1, 2, 3]").allowed).toBe(true);
  });

  it("denies statements concealed after nested PostgreSQL block comments", () => {
    expect(
      pgGuard()('SELECT "pg_advisory_lock"/* a /* b */ c */(42)').allowed,
    ).toBe(false);
    expect(pgGuard()("SELECT 1 /* outer /* inner */ */").allowed).toBe(false);
  });

  it("stops SQL line comments on carriage returns", () => {
    expect(
      pgGuard()("SELECT 1 -- ignored\r; SELECT pg_advisory_lock(42)").allowed,
    ).toBe(false);
    expect(pgGuard()("SELECT 1 -- ignored\r\nSELECT 2").allowed).toBe(true);
  });

  it("denies MySQL executable comments on mysql dialect", () => {
    const guard = mysqlGuard();
    expect(guard("SELECT 1 /*!50000 DELETE FROM t */").allowed).toBe(false);
    expect(guard("SELECT 1 /*M!50000 DELETE FROM t */").allowed).toBe(false);
    expect(guard("SELECT 1").allowed).toBe(true);
    // Executable-looking text inside a string literal must not trigger.
    expect(guard("SELECT '/*!50000 DELETE */'").allowed).toBe(true);
  });

  it("treats executable comments as plain comments on other dialects", () => {
    // PostgreSQL does not execute /*! ... */, so classification stays read-only.
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 /*!50000 DELETE FROM t */", "postgresql"),
    ).toEqual({ allowed: true });
  });

  it("ignores spaced comments and handles unterminated executable comments", () => {
    const guard = mysqlGuard();
    // `/* !` with a space is a plain comment, not executable.
    expect(guard("SELECT 1 /* !50000 DELETE FROM t */").allowed).toBe(true);
    // Lowercase m is a plain comment; only uppercase M! executes on MariaDB.
    expect(guard("SELECT 1 /*m!50000 DELETE FROM t */").allowed).toBe(true);
    // Unterminated executable comment fails closed.
    expect(guard("SELECT 1 /*!50000 DELETE FROM t").allowed).toBe(false);
  });

  it("does not collide on sentinel-like identifiers", () => {
    const guard = mysqlGuard();
    expect(guard("SELECT __rapidb_executable_comment__ FROM t").allowed).toBe(
      true,
    );
  });

  it("allows quoted reserved-looking identifiers", () => {
    const guard = pgGuard();
    expect(guard('SELECT "comment" FROM t')).toEqual({ allowed: true });
  });

  it("allows a COMMENT column in SELECT expressions but still denies mutations", () => {
    const guard = pgGuard();
    expect(guard("SELECT count(comment) FROM posts")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT comment FROM posts")).toEqual({ allowed: true });
    expect(guard("SELECT id, comment FROM posts")).toEqual({ allowed: true });
    expect(guard("SELECT comment, id FROM posts")).toEqual({ allowed: true });
    expect(guard("SELECT comment AS body FROM posts")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT posts.comment FROM posts")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT count(posts.comment) FROM posts")).toEqual({
      allowed: true,
    });
    expect(guard("SELECT count((DELETE FROM posts RETURNING *))").allowed).toBe(
      false,
    );
    expect(guard("SELECT COMMENT ON TABLE posts IS 'changed'").allowed).toBe(
      false,
    );
    expect(
      guard(
        "SELECT pg_catalog.pg_create_logical_replication_slot('rapidb_slot', 'test_decoding')",
      ).allowed,
    ).toBe(false);
    expect(
      guard("SELECT pg_catalog.pg_create_restore_point('rapidb')").allowed,
    ).toBe(false);
  });

  it("flags executable comments as schema-changing", () => {
    expect(mayChangeDatabaseSchema("SELECT 1 /*!50000 DELETE FROM t */")).toBe(
      true,
    );
    // Explicit non-MySQL dialect avoids the conservative refresh.
    expect(
      mayChangeDatabaseSchema("SELECT 1 /*!50000 SELECT 1 */", "postgresql"),
    ).toBe(false);
  });
});
