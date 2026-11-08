import { describe, expect, it } from "vitest";
import { sqlReadOnlyQueryGuard } from "../../src/extension/utils/readOnlyGuards";

type Dialect = "postgresql" | "mysql" | "sqlite" | "transactsql" | "sql";

describe("read-only guard counts mutation keywords only at statement starts", () => {
  it.each([
    ["SELECT * FROM events ORDER BY id, start DESC", "postgresql"],
    ["SELECT start + 1 FROM events", "postgresql"],
    ["SELECT start::date FROM events", "postgresql"],
    ["SELECT comment || 'x' FROM t", "postgresql"],
    ["SELECT e.id FROM events e JOIN lock l ON l.id = e.id", "postgresql"],
    ["SELECT id, comment FROM t WHERE comment IS NOT NULL", "postgresql"],
    ["SELECT (start + 1) * 2 FROM events", "postgresql"],
    ["SELECT * FROM t WHERE (start IS NULL OR start > now())", "postgresql"],
    [
      "WITH e AS (SELECT start FROM events) SELECT start, copy FROM e",
      "postgresql",
    ],
    ["TABLE events ORDER BY id, start", "postgresql"],
    ["VALUES (1, 2) ORDER BY 1, 2", "postgresql"],
    ["EXPLAIN (VERBOSE) SELECT 1", "postgresql"],
    ["EXPLAIN (COSTS OFF) SELECT 1", "postgresql"],
    [
      "EXPLAIN (FORMAT JSON, COSTS false, BUFFERS) SELECT * FROM t",
      "postgresql",
    ],
    ["EXPLAIN (GENERIC_PLAN) SELECT * FROM t WHERE id = $1", "postgresql"],
    ["EXPLAIN VERBOSE SELECT 1", "postgresql"],
    ["EXPLAIN (VERBOSE) WITH x AS (SELECT 1) SELECT * FROM x", "postgresql"],
    ["SELECT * FROM t WHERE release > 3 ORDER BY id, start", "mysql"],
    ["SELECT start + 1, comment FROM t JOIN `lock` l ON l.id = t.id", "mysql"],
    ["SELECT copy, start FROM t ORDER BY start DESC", "sqlite"],
    ["SELECT start + 1 FROM t JOIN lock l ON l.id = t.id", "sqlite"],
  ] as const)("allows %s (%s)", (sql, dialect) => {
    expect(sqlReadOnlyQueryGuard(sql, dialect)).toEqual({ allowed: true });
  });

  it.each([
    ["DELETE FROM t", "postgresql"],
    ["SELECT 1; DELETE FROM t", "postgresql"],
    ["SELECT 1; start transaction", "mysql"],
    ["SELECT * FROM (DELETE FROM users RETURNING *) AS x", "postgresql"],
    [
      "SELECT * FROM t WHERE id IN (UPDATE u SET a = 1 RETURNING id)",
      "postgresql",
    ],
    [
      "SELECT * FROM t WHERE id IN (INSERT INTO u DEFAULT VALUES RETURNING id)",
      "postgresql",
    ],
    ["WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d", "postgresql"],
    ["WITH d AS (SELECT 1) DELETE FROM t", "postgresql"],
    ["WITH d AS (UPDATE t SET a = 1 RETURNING *) SELECT * FROM d", "sqlite"],
    ["WITH d AS (SELECT 1) INSERT INTO t SELECT * FROM d", "mysql"],
    ["SELECT a INTO b FROM t", "postgresql"],
    ["SELECT start INTO new_table FROM events", "postgresql"],
    ["SELECT a INTO @v FROM t", "mysql"],
    ["SELECT * FROM t INTO OUTFILE '/tmp/x'", "mysql"],
    ["SELECT * FROM t FOR UPDATE", "postgresql"],
    ["SELECT * FROM t FOR NO KEY UPDATE", "postgresql"],
    ["SELECT * FROM t LOCK IN SHARE MODE", "mysql"],
    ["SELECT nextval('s'), start FROM t", "postgresql"],
    ["EXPLAIN ANALYZE DELETE FROM t", "postgresql"],
    ["EXPLAIN ANALYZE SELECT 1", "postgresql"],
    ["EXPLAIN (ANALYZE) DELETE FROM t", "postgresql"],
    ["EXPLAIN (ANALYZE true) SELECT 1", "postgresql"],
    ["EXPLAIN (ANALYZE false) DELETE FROM t", "postgresql"],
    ["EXPLAIN (VERBOSE, ANALYZE) SELECT 1", "postgresql"],
    ["EXPLAIN (ANALYZE, VERBOSE) SELECT 1", "postgresql"],
    ["EXPLAIN VERBOSE ANALYZE SELECT 1", "postgresql"],
    ["EXPLAIN (VERBOSE) DELETE FROM t", "postgresql"],
    ["EXPLAIN (VERBOSE) INSERT INTO t VALUES (1)", "postgresql"],
    ["EXPLAIN (VERBOSE) SELECT * INTO t2 FROM t", "postgresql"],
    [
      "EXPLAIN (VERBOSE) WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
      "postgresql",
    ],
    ["EXPLAIN (COSTS OFF", "postgresql"],
    ["EXPLAIN (COSTS OFF)", "postgresql"],
    ["EXPLAIN () SELECT 1", "postgresql"],
    ["EXPLAIN (UNKNOWN) SELECT 1", "postgresql"],
    ["EXPLAIN (COSTS (OFF)) SELECT 1", "postgresql"],
    ["EXPLAIN (COSTS OFF) (DELETE FROM t)", "postgresql"],
    ["EXPLAIN (COSTS OFF) SELECT 1", "mysql"],
    ["EXPLAIN (COSTS OFF) SELECT 1", "sqlite"],
    ["EXPLAIN (COSTS OFF) SELECT 1", "sql"],
  ] as const)("keeps denying %s (%s)", (sql, dialect) => {
    expect(sqlReadOnlyQueryGuard(sql, dialect).allowed).toBe(false);
  });

  it.each<[string, Dialect]>([
    ["SELECT 1 DELETE FROM t", "transactsql"],
    ["SELECT 1 DELETE FROM t", "sql"],
  ])("stays strict for dialects without mandatory statement separators: %s (%s)", (sql, dialect) => {
    expect(sqlReadOnlyQueryGuard(sql, dialect).allowed).toBe(false);
  });
});
