import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
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
  });

  it("does not strip MySQL subtraction as a -- comment", () => {
    expect(mysqlGuard()("SELECT 1--1 INTO @a").allowed).toBe(false);
    expect(sqlReadOnlyQueryGuard("SELECT 1--1 INTO @a").allowed).toBe(false);
    expect(mysqlGuard()("SELECT 1-- actual comment\n").allowed).toBe(true);
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
