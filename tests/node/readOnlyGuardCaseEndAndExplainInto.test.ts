import { describe, expect, it } from "vitest";
import { sqlReadOnlyQueryGuard } from "../../src/extension/utils/readOnlyGuards";

type Dialect = "postgresql" | "mysql";

describe("read-only guard does not treat CASE ... END as an END statement", () => {
  it.each<[string, Dialect]>([
    ["SELECT CASE WHEN a THEN 1 ELSE 2 END", "postgresql"],
    ["SELECT CASE WHEN a THEN 'x' ELSE 'y' END", "postgresql"],
    ["SELECT id FROM t ORDER BY CASE WHEN a THEN 1 ELSE 2 END", "postgresql"],
    [
      "SELECT * FROM t ORDER BY CASE WHEN a THEN 1 ELSE t.end END",
      "postgresql",
    ],
    [
      "SELECT * FROM t ORDER BY CASE WHEN a THEN (SELECT 1 AS end) END",
      "postgresql",
    ],
    [
      "SELECT id FROM t ORDER BY CASE WHEN a THEN 1 ELSE 2 END;\nSELECT 2",
      "postgresql",
    ],
    [
      "SELECT count(*) FROM t GROUP BY CASE WHEN a THEN 1 ELSE 2 END",
      "postgresql",
    ],
    ["SELECT CASE WHEN a THEN 1 ELSE 2 END x", "postgresql"],
    ["SELECT CASE WHEN a THEN 1 ELSE 2 END x FROM t", "postgresql"],
    ["SELECT CASE a WHEN 1 THEN 'x' END;", "postgresql"],
    [
      "SELECT CASE WHEN a THEN CASE WHEN b THEN 1 ELSE 2 END ELSE 3 END",
      "postgresql",
    ],
    [
      "SELECT id FROM t ORDER BY CASE WHEN a THEN CASE WHEN b THEN 1 END END",
      "postgresql",
    ],
    ["SELECT $$BEGIN; END$$ AS body", "postgresql"],
    ["SELECT CASE WHEN a THEN 1 ELSE 2 END", "mysql"],
    ["SELECT CASE WHEN a THEN 1 ELSE 2 END x FROM t", "mysql"],
  ])("allows %j (%s)", (sql, dialect) => {
    expect(sqlReadOnlyQueryGuard(sql, dialect)).toEqual({ allowed: true });
  });

  it.each<[string, Dialect]>([
    ["SELECT CASE WHEN a THEN 1 END; END", "postgresql"],
    ["SELECT CASE WHEN a THEN 1 END; END WORK", "postgresql"],
    ["SELECT 1; END", "postgresql"],
    ["SELECT 1; END TRANSACTION", "postgresql"],
    ["SELECT 1; END WORK", "postgresql"],
    ["SELECT 1 END", "postgresql"],
    ["SELECT CASE WHEN a THEN 1 END END", "postgresql"],
    ["SELECT CASE WHEN a THEN 1 END END DELETE FROM t", "postgresql"],
    ["SELECT CASE WHEN a THEN CASE WHEN b THEN 1 END END END", "postgresql"],
    ["SELECT t.case FROM t END", "postgresql"],
    ["SELECT 1 AS case END", "postgresql"],
  ])("denies %j (%s)", (sql, dialect) => {
    expect(sqlReadOnlyQueryGuard(sql, dialect).allowed).toBe(false);
  });
});

describe("read-only guard denies MySQL EXPLAIN ... INTO", () => {
  it.each([
    "EXPLAIN FORMAT=JSON INTO @v SELECT 1",
    "EXPLAIN FORMAT = JSON INTO @v SELECT 1",
    "DESC FORMAT=JSON INTO @v SELECT 1",
    "DESCRIBE FORMAT=JSON INTO @v SELECT 1",
  ])("denies %j", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "mysql").allowed).toBe(false);
  });

  it.each([
    "EXPLAIN FORMAT=JSON SELECT 1",
    "EXPLAIN FORMAT=TREE SELECT * FROM t",
    "DESC FORMAT=JSON SELECT 1",
  ])("allows %j", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "mysql")).toEqual({ allowed: true });
  });
});
