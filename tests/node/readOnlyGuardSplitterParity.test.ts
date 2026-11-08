import { describe, expect, it } from "vitest";
import { splitMySQLScript } from "../../src/extension/dbDrivers/mysql";
import { sqlReadOnlyQueryGuard } from "../../src/extension/utils/readOnlyGuards";

type Dialect = "postgresql" | "mysql";

describe("MySQL read-only guard and script splitter agree on comments", () => {
  it.each([
    "\x01",
    "\x0b",
    "\x1f",
    "\x7f",
  ])("treats -- followed by control character %j as a comment in both", (control) => {
    const sql = `SELECT 1 --${control};\nCOMMIT --${control};\nDELETE FROM t`;
    expect(splitMySQLScript(sql)).toHaveLength(1);
    expect(sqlReadOnlyQueryGuard(sql, "mysql").allowed).toBe(false);
  });

  it.each([
    "\u00a0",
    "\u2028",
    "\ufeff",
    "\u3000",
  ])("does not split on a quote hidden behind -- followed by %j", (space) => {
    const sql = `SELECT 1 --${space}'\n; COMMIT; DELETE FROM t; --${space}'`;
    expect(splitMySQLScript(sql)).toHaveLength(1);
    expect(sqlReadOnlyQueryGuard(sql, "mysql").allowed).toBe(false);
  });

  it("denies a commit hidden behind an ordinary dash comment", () => {
    const sql = "SELECT 1 -- '\n; COMMIT; DELETE FROM t; -- '";
    expect(splitMySQLScript(sql)).toEqual([
      "SELECT 1",
      "COMMIT",
      "DELETE FROM t",
    ]);
    expect(sqlReadOnlyQueryGuard(sql, "mysql").allowed).toBe(false);
  });

  it("denies when the driver would split into a different statement list", () => {
    expect(
      sqlReadOnlyQueryGuard("SELECT CASE; SELECT 1", "mysql").allowed,
    ).toBe(false);
  });

  it.each([
    "SELECT 1 -- note\nFROM dual",
    "SELECT 1 --\tnote\nFROM dual",
    "SELECT 1 --\x01note\nFROM dual",
    "SELECT 1 # note\nFROM dual",
    "SELECT 5--1",
    "SELECT 1; SELECT 2;",
    "SELECT 'a;b' AS x; SELECT `c;d` FROM t",
  ])("still allows %j", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "mysql")).toEqual({ allowed: true });
  });
});

describe("read-only guard keeps transaction control strict for PostgreSQL and MySQL", () => {
  it.each<[string, Dialect]>([
    ["SELECT 1 COMMIT DELETE FROM t", "postgresql"],
    ["SELECT 1 commit and no chain delete from t", "postgresql"],
    ["SELECT 1 END DELETE FROM t", "postgresql"],
    ["SELECT 1 ABORT", "postgresql"],
    ["SELECT 1 ROLLBACK", "postgresql"],
    ["SELECT 1 BEGIN", "postgresql"],
    ["SELECT 1 START TRANSACTION READ WRITE", "postgresql"],
    ["SELECT 1 SET default_transaction_read_only = off", "postgresql"],
    [
      "SELECT 1 SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE",
      "postgresql",
    ],
    ["SELECT 1 PREPARE TRANSACTION 'x'", "postgresql"],
    ["SELECT 1 SAVEPOINT a", "postgresql"],
    ["SELECT 1 RELEASE SAVEPOINT a", "postgresql"],
    ["SELECT 1 EXECUTE p", "postgresql"],
    ["SHOW search_path COMMIT DELETE FROM t", "postgresql"],
    ["EXPLAIN SELECT 1 COMMIT", "postgresql"],
    ["VALUES (1) COMMIT", "postgresql"],
    ["SELECT 1 COMMIT DELETE FROM t", "mysql"],
    ["SELECT 1 BEGIN", "mysql"],
    ["SELECT 1 START TRANSACTION", "mysql"],
    ["SELECT 1 SET autocommit = 1", "mysql"],
    ["SELECT 1 SET @@session.autocommit = 1", "mysql"],
    ["SHOW TABLES COMMIT", "mysql"],
    ["DESCRIBE t ROLLBACK", "mysql"],
  ])("denies %s (%s)", (sql, dialect) => {
    expect(sqlReadOnlyQueryGuard(sql, dialect).allowed).toBe(false);
  });

  it.each<[string, Dialect]>([
    ["SELECT * FROM events ORDER BY id, start DESC", "postgresql"],
    ["SELECT start + 1 FROM events", "postgresql"],
    ["SELECT begin, finish FROM ranges ORDER BY begin DESC", "postgresql"],
    ["SELECT * FROM ranges ORDER BY id, begin DESC", "postgresql"],
    ["SELECT begin + 1 FROM ranges", "postgresql"],
    ["SELECT commit FROM commits", "postgresql"],
    ["SELECT c.commit, c.author FROM commits c", "postgresql"],
    ["SELECT count(*) AS commit FROM t", "postgresql"],
    ["SELECT * FROM t ORDER BY id, release DESC", "postgresql"],
    ["SELECT * FROM release r JOIN t ON t.id = r.id", "postgresql"],
    ["TABLE release", "postgresql"],
    ["SELECT jsonb_set(a, '{x}', '1') FROM t", "postgresql"],
    ["SELECT * FROM t WHERE set = 1 AND rollback IS NULL", "postgresql"],
    ["SHOW TRANSACTION ISOLATION LEVEL", "postgresql"],
    ["SELECT * FROM t WHERE release > 3 ORDER BY id, start", "mysql"],
    ["SELECT start + 1, comment FROM t JOIN `lock` l ON l.id = t.id", "mysql"],
    ["SELECT CAST(x AS CHAR CHARACTER SET utf8mb4) FROM t", "mysql"],
    ["SHOW CHARACTER SET LIKE 'utf8%'", "mysql"],
    ["SELECT begin, `end` FROM ranges ORDER BY id, begin DESC", "mysql"],
  ])("allows %s (%s)", (sql, dialect) => {
    expect(sqlReadOnlyQueryGuard(sql, dialect)).toEqual({ allowed: true });
  });
});
