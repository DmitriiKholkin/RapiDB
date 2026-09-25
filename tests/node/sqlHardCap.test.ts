import { describe, expect, it } from "vitest";
import { applyHardCapToSqlQuery } from "../../src/extension/utils/sqlHardCap";
import type { ConnectionType } from "../../src/shared/connectionTypes";

const dialects: ConnectionType[] = ["pg", "mysql", "sqlite", "mssql", "oracle"];
const rewrite = (sql: string, dialect: ConnectionType = "pg") =>
  applyHardCapToSqlQuery(sql, dialect, 11);

describe("SQL editor hard cap boundaries (#23)", () => {
  it.each(dialects)("caps parenthesized SELECT on %s", (dialect) => {
    const result = rewrite("((SELECT * FROM items)); -- done", dialect);
    expect(result.error).toBeUndefined();
    expect(result.decision.applied).toBe(true);
    expect(result.queryText).toBe(
      dialect === "mssql"
        ? "SELECT TOP (11) * FROM items"
        : dialect === "oracle"
          ? "SELECT * FROM (SELECT * FROM items) rapidb_query_cap FETCH FIRST 11 ROWS ONLY"
          : dialect === "pg"
            ? "SELECT * FROM (SELECT * FROM items) AS rapidb_query_cap LIMIT 11"
            : "SELECT * FROM items LIMIT 11",
    );
  });

  it("caps PostgreSQL TABLE and whole set operations", () => {
    expect(rewrite("TABLE public.items").queryText).toBe(
      "SELECT * FROM (TABLE public.items) AS rapidb_query_cap LIMIT 11",
    );
    expect(
      rewrite("(SELECT * FROM a) UNION ALL (SELECT * FROM b)").queryText,
    ).toBe(
      "SELECT * FROM ((SELECT * FROM a) UNION ALL (SELECT * FROM b)) AS rapidb_query_cap LIMIT 11",
    );
  });

  it.each([
    "mssql",
  ] as const)("rejects an entire multi-statement batch before execution on %s", (dialect) => {
    for (const sql of [
      "SELECT * FROM a; SELECT * FROM b",
      "INSERT INTO a VALUES (1); SELECT * FROM b",
      "UPDATE a SET x = 1 RETURNING *; SELECT * FROM b",
      "DELETE FROM a; INSERT INTO b VALUES (1) RETURNING *",
      "CREATE TABLE a (x int); SELECT * FROM b",
    ]) {
      expect(rewrite(sql, dialect).error).toContain("one statement at a time");
    }
  });

  it.each(dialects)("preserves ordinary DML and DDL on %s", (dialect) => {
    for (const sql of [
      "INSERT INTO items (id) VALUES (1)",
      "UPDATE items SET id = 2 WHERE id = 1",
      "DELETE FROM items WHERE id = 2",
      "CREATE TABLE items (id int)",
      "ALTER TABLE items ADD label varchar(30)",
      "DROP TABLE items",
      "UPDATE items SET id = CASE WHEN id = 1 THEN 2 ELSE 3 END",
    ]) {
      expect(rewrite(sql, dialect)).toEqual({
        queryText: sql,
        decision: { applied: false, reason: "non_limitable_statement" },
      });
    }
    if (dialect !== "pg")
      expect(rewrite("DELETE FROM items RETURNING *", dialect).error).toContain(
        "RETURNING/OUTPUT",
      );
  });

  it("rejects unbounded/ambiguous MSSQL forms rather than capping only one branch", () => {
    for (const sql of [
      "SELECT * FROM a UNION ALL SELECT * FROM b",
      "SELECT TOP (100) PERCENT * FROM a",
      "SELECT * FROM a SELECT * FROM b",
      "INSERT INTO a VALUES (1) SELECT * FROM b",
      "UPDATE a SET x = 1 OUTPUT inserted.*",
      "SELECT * FROM a ORDER BY x OFFSET 0 ROWS FETCH NEXT @n ROWS ONLY",
      "EXEC p",
      "DECLARE @x int SELECT * FROM a",
      "SELECT 1 WAITFOR (RECEIVE * FROM queue)",
      "INSERT INTO a VALUES (1) READTEXT a.payload 0x00 0 99999",
      "INSERT INTO a VALUES (1) (SELECT * FROM huge)",
      "SELECT * FROM a (SELECT * FROM huge)",
      "INSERT INTO a (id) SELECT id FROM huge",
    ])
      expect(rewrite(sql, "mssql").error, sql).toBeTruthy();
  });

  it("inserts MSSQL TOP by source position, never into a comment containing SELECT", () => {
    expect(
      rewrite("/* SELECT */ SELECT /* ALL */ DISTINCT id FROM a", "mssql")
        .queryText,
    ).toBe("/* SELECT */ SELECT /* ALL */ DISTINCT TOP (11) id FROM a");
    expect(rewrite("(/* SELECT */ SELECT id FROM a)", "mssql").queryText).toBe(
      "SELECT TOP (11) id FROM a",
    );
  });

  it("supports ordinary CASE expressions in MSSQL", () => {
    const sql = "SELECT CASE WHEN id > 1 THEN 2 ELSE 1 END FROM a";
    expect(rewrite(sql, "mssql").queryText).toBe(
      "SELECT TOP (11) CASE WHEN id > 1 THEN 2 ELSE 1 END FROM a",
    );
  });

  it("supports MSSQL subqueries in unambiguous operand positions", () => {
    const body =
      "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id)";
    expect(rewrite(body, "mssql").error).toBeUndefined();
    expect(
      rewrite("UPDATE a SET id = (SELECT MAX(id) FROM huge)", "mssql").error,
    ).toBeUndefined();
    const prefix = `WITH c AS (${body}) `;
    expect(rewrite(`${prefix}SELECT id FROM c`, "mssql").queryText).toBe(
      `${prefix}SELECT TOP (11) id FROM c`,
    );
  });

  it.each([
    "pg",
    "mysql",
    "sqlite",
    "oracle",
  ] as const)("preserves INSERT SELECT on %s", (dialect) => {
    const sql = "INSERT INTO archive (id) SELECT id FROM items";
    expect(rewrite(sql, dialect)).toEqual({
      queryText: sql,
      decision: { applied: false, reason: "non_limitable_statement" },
    });
  });

  it.each([
    ["pg", "EXPLAIN SELECT * FROM items"],
    ["mysql", "SHOW TABLES"],
    ["mysql", "ANALYZE TABLE items"],
    ["sqlite", "PRAGMA journal_mode = WAL"],
    ["mssql", "DBCC CHECKDB"],
    ["oracle", "CALL report()"],
  ] as const)("explicitly rejects unsupported result commands on %s: %s", (dialect, sql) => {
    expect(rewrite(sql, dialect).error).toContain("unsupported");
  });

  it.each([
    ["pg", "SELECT ';--/*' AS \";select\"; -- done"],
    ["pg", "SELECT $body$'; SELECT * FROM huge; --$body$;"],
    ["pg", "/* outer /* nested */ still comment */ SELECT 1;"],
    ["pg", "SELECT E'escaped\\'; SELECT text';"],
    ["mysql", "# SELECT ignored;\nSELECT ';' AS `semi;colon`; # done"],
    ["sqlite", "SELECT ';' AS [semi;colon];"],
    ["mssql", "SELECT ';' AS [semi;co]]lon];"],
    ["oracle", "SELECT q'[quote'; SELECT text]' FROM dual;"],
  ] as const)("respects %s quotes/comments: %s", (dialect, sql) => {
    expect(rewrite(sql, dialect).error).toBeUndefined();
    expect(rewrite(sql, dialect).decision.applied).toBe(true);
  });

  it.each([
    ["mysql", "INSERT INTO a VALUES (1) /*!; SELECT * FROM huge */"],
    ["mysql", "SELECT 1 /*M! UNION SELECT * FROM huge */"],
    ["mysql", "SELECT 'a\\' ; SELECT * FROM huge"],
    ["pg", "SELECT 'a\\' ; SELECT * FROM huge"],
    ["pg", "SELECT 'unfinished"],
    ["pg", "SELECT 1 /* unfinished"],
    ["oracle", "BEGIN NULL; END;"],
    ["mssql", "INSERT INTO [a;]]b] VALUES (1); SELECT * FROM huge"],
  ] as const)("fails closed for %s ambiguous SQL: %s", (dialect, sql) => {
    expect(rewrite(sql, dialect).error).toBeTruthy();
  });
});

describe("read-only CTE hard caps (#1)", () => {
  it.each(
    dialects,
  )("keeps WITH at statement scope and caps the final SELECT for %s", (dialect) => {
    const source =
      "WITH src AS (SELECT id FROM items), other AS (SELECT id FROM src) ";
    const result = rewrite(
      `${source}SELECT id FROM other ORDER BY id;`,
      dialect,
    );
    expect(result.error).toBeUndefined();
    expect(result.queryText).toBe(
      source +
        (dialect === "mssql"
          ? "SELECT TOP (11) id FROM other ORDER BY id"
          : dialect === "oracle"
            ? "SELECT * FROM (SELECT id FROM other ORDER BY id) rapidb_query_cap FETCH FIRST 11 ROWS ONLY"
            : dialect === "pg"
              ? "SELECT * FROM (SELECT id FROM other ORDER BY id) AS rapidb_query_cap LIMIT 11"
              : "SELECT id FROM other ORDER BY id LIMIT 11"),
    );
    expect(result.decision.applied).toBe(true);
  });

  it.each([
    "pg",
    "mysql",
    "sqlite",
  ] as const)("supports native recursive CTEs for %s", (dialect) => {
    const prefix =
      "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 30) ";
    const result = rewrite(`${prefix}SELECT n FROM seq`, dialect);
    expect(result.error).toBeUndefined();
    expect(result.queryText).toBe(
      dialect === "pg"
        ? `${prefix}SELECT * FROM (SELECT n FROM seq) AS rapidb_query_cap LIMIT 11`
        : `${prefix}SELECT n FROM seq LIMIT 11`,
    );
  });

  it.each([
    "mssql",
    "oracle",
  ] as const)("supports recursive CTE syntax without RECURSIVE for %s", (dialect) => {
    const prefix = `WITH seq(n) AS (SELECT 1${dialect === "oracle" ? " FROM dual" : ""} UNION ALL SELECT n + 1 FROM seq WHERE n < 30) `;
    const result = rewrite(`${prefix}SELECT n FROM seq`, dialect);
    expect(result.error).toBeUndefined();
    expect(result.queryText.startsWith(prefix)).toBe(true);
    expect(result.queryText).toContain(
      dialect === "oracle"
        ? "FETCH FIRST 11 ROWS ONLY"
        : "SELECT TOP (11) n FROM seq",
    );
    expect(
      rewrite("WITH RECURSIVE c AS (SELECT 1) SELECT * FROM c", dialect).error,
    ).toContain("native recursive CTE syntax");
  });

  it("supports leading ;WITH, quoted names, comments and DISTINCT in T-SQL", () => {
    expect(
      rewrite(
        "; /* SELECT */ WITH [odd;cte] ([id]) AS (SELECT 1)\nSELECT /* SELECT */ DISTINCT [id] FROM [odd;cte]; -- done",
        "mssql",
      ).queryText,
    ).toBe(
      "WITH [odd;cte] ([id]) AS (SELECT 1)\nSELECT /* SELECT */ DISTINCT TOP (11) [id] FROM [odd;cte]",
    );
  });

  it("does not confuse SQL Server table hints with nested WITH", () => {
    const prefix = "WITH c AS (SELECT id FROM items WITH (NOLOCK)) ";
    expect(rewrite(`${prefix}SELECT id FROM c`, "mssql").queryText).toBe(
      `${prefix}SELECT TOP (11) id FROM c`,
    );
  });

  it("preserves PostgreSQL materialization and caps all main set branches", () => {
    const prefix = "WITH \"src\" AS NOT MATERIALIZED (SELECT ';--' AS label) ";
    expect(
      rewrite(
        `${prefix}SELECT label FROM "src" UNION ALL SELECT label FROM "src"`,
      ).queryText,
    ).toBe(
      `${prefix}SELECT * FROM (SELECT label FROM "src" UNION ALL SELECT label FROM "src") AS rapidb_query_cap LIMIT 11`,
    );
  });

  it.each(
    dialects.filter((dialect) => dialect !== "pg"),
  )("rejects write CTEs for %s", (dialect) => {
    for (const sql of [
      "WITH c AS (DELETE FROM items RETURNING *) SELECT * FROM c",
      "WITH c AS (SELECT 1) DELETE FROM items RETURNING *",
      "WITH c AS (SELECT * FROM (WITH d AS (DELETE FROM items RETURNING *) SELECT * FROM d) q) SELECT * FROM c",
    ]) {
      const result = rewrite(sql, dialect);
      expect(result.error, sql).toBeTruthy();
      expect(result.decision.applied).toBe(false);
    }
  });

  it("rejects CTE shapes requiring additional dialect parsing", () => {
    expect(
      rewrite("WITH c AS (VALUES (1)) SELECT * FROM c").error,
    ).toBeUndefined();
    expect(
      rewrite(
        "WITH c AS (SELECT 1 AS n) SELECT n FROM c UNION ALL SELECT n FROM c",
        "mssql",
      ).error,
    ).toContain("set operations");
    expect(
      rewrite(
        "WITH c AS (WITH d AS (SELECT 1) SELECT * FROM d) SELECT * FROM c",
        "oracle",
      ).error,
    ).toContain("nested WITH");
  });
});

describe("independent cap review regressions", () => {
  it.each([
    "pg",
    "mysql",
    "sqlite",
    "oracle",
  ] as const)("does not apply read-only keyword policy to ordinary %s SELECT projections", (dialect) => {
    expect(rewrite("SELECT comment FROM items", dialect).error).toBeUndefined();
    expect(
      rewrite("SELECT * INTO archive FROM items", dialect).error,
    ).toBeTruthy();
    expect(
      rewrite("SELECT * FROM items FOR UPDATE", dialect).error,
    ).toBeTruthy();
  });
  it("caps the inner MariaDB SET STATEMENT command instead of passing SET through", () => {
    expect(
      rewrite(
        "SET STATEMENT max_statement_time = 1 FOR SELECT * FROM huge",
        "mysql",
      ).queryText,
    ).toBe(
      "SET STATEMENT max_statement_time = 1 FOR SELECT * FROM huge LIMIT 11",
    );
    expect(
      rewrite("SET STATEMENT max_statement_time = 1 FOR SHOW TABLES", "mysql")
        .error,
    ).toBeTruthy();
    const mutation =
      "SET STATEMENT max_statement_time = 1 FOR UPDATE items SET id = 2";
    expect(rewrite(mutation, "mysql")).toEqual({
      queryText: mutation,
      decision: { applied: false, reason: "non_limitable_statement" },
    });
  });
  it.each(
    dialects,
  )("does not classify the REPLACE function as DML on %s", (dialect) => {
    expect(
      rewrite(
        "WITH c AS (SELECT REPLACE('a', 'a', 'b') AS v) SELECT v FROM c",
        dialect,
      ).error,
    ).toBeUndefined();
  });
  it.each([
    "mysql",
    "sqlite",
  ] as const)("preserves duplicate projections and literal LIMIT semantics on %s", (dialect) => {
    for (const [source, expected] of [
      [
        "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id",
        "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id LIMIT 11",
      ],
      [
        "SELECT id FROM a LIMIT 100 OFFSET 7",
        "SELECT id FROM a LIMIT 11 OFFSET 7",
      ],
      ["SELECT id FROM a LIMIT 7, 100", "SELECT id FROM a LIMIT 7, 11"],
      ["SELECT id FROM a LIMIT 2", "SELECT id FROM a LIMIT 2"],
      [
        "SELECT id FROM a LIMIT /* count */ 100 OFFSET /* offset */ 7",
        "SELECT id FROM a LIMIT /* count */ 11 OFFSET /* offset */ 7",
      ],
      ["SELECT id FROM a LIMIT -1", "SELECT id FROM a LIMIT 11"],
      [
        "SELECT id FROM a LIMIT 18446744073709551615",
        "SELECT id FROM a LIMIT 11",
      ],
    ])
      expect(rewrite(source, dialect).queryText).toBe(expected);
  });

  it("clamps MSSQL literal TOP without changing smaller limits or ordering", () => {
    expect(
      rewrite("SELECT TOP (100) id FROM a ORDER BY id", "mssql").queryText,
    ).toBe("SELECT TOP (11) id FROM a ORDER BY id");
    expect(rewrite("SELECT DISTINCT TOP 2 id FROM a", "mssql").queryText).toBe(
      "SELECT DISTINCT TOP 2 id FROM a",
    );
    for (const source of [
      "SELECT TOP (@n) * FROM a",
      "SELECT TOP 10 WITH TIES * FROM a ORDER BY id",
      "SELECT TOP 10 PERCENT * FROM a",
    ])
      expect(rewrite(source, "mssql").error).toBeTruthy();
    expect(
      rewrite("SELECT TOP/* hint */(100)id FROM a", "mssql").queryText,
    ).toBe("SELECT TOP/* hint */(11)id FROM a");
    expect(
      rewrite("SELECT id FROM a OPTION (RECOMPILE)", "mssql").queryText,
    ).toBe("SELECT TOP (11) id FROM a OPTION (RECOMPILE)");
    expect(
      rewrite("SELECT (SELECT MAX(id) FROM b) AS id FROM a", "mssql").queryText,
    ).toBe("SELECT TOP (11) (SELECT MAX(id) FROM b) AS id FROM a");
  });

  it("bounds SQL Server OFFSET/FETCH without adding an incompatible TOP", () => {
    for (const [source, expected] of [
      [
        "SELECT id FROM a ORDER BY id OFFSET 3 ROWS",
        "SELECT id FROM a ORDER BY id OFFSET 3 ROWS FETCH NEXT 11 ROWS ONLY",
      ],
      [
        "SELECT id FROM a ORDER BY id OFFSET 3 ROWS FETCH NEXT 100 ROWS ONLY",
        "SELECT id FROM a ORDER BY id OFFSET 3 ROWS FETCH NEXT 11 ROWS ONLY",
      ],
      [
        "SELECT id FROM a ORDER BY id OFFSET 3 ROWS FETCH FIRST 2 ROWS ONLY",
        "SELECT id FROM a ORDER BY id OFFSET 3 ROWS FETCH FIRST 2 ROWS ONLY",
      ],
    ])
      expect(rewrite(source, "mssql").queryText).toBe(expected);
    expect(
      rewrite(
        "SELECT id FROM a ORDER BY id OFFSET 3 ROWS (SELECT * FROM huge)",
        "mssql",
      ).error,
    ).toBeTruthy();
  });

  it("uses bounded native relations for supported metadata commands", () => {
    expect(rewrite("SHOW ALL").queryText).toBe(
      "SELECT name, setting, description FROM pg_catalog.pg_settings ORDER BY name LIMIT 11",
    );
    expect(rewrite("PRAGMA main.table_info('items')", "sqlite").queryText).toBe(
      "SELECT * FROM pragma_table_info('items', 'main') LIMIT 11",
    );
    expect(rewrite('PRAGMA table_info("odd""name")', "sqlite").queryText).toBe(
      "SELECT * FROM pragma_table_info('odd\"name') LIMIT 11",
    );
    expect(rewrite("PRAGMA table_info = items", "sqlite").queryText).toBe(
      "SELECT * FROM pragma_table_info('items') LIMIT 11",
    );
    expect(rewrite("PRAGMA database_list", "sqlite").queryText).toBe(
      "SELECT * FROM pragma_database_list() LIMIT 11",
    );
    expect(
      rewrite("PRAGMA database_list = something", "sqlite").error,
    ).toBeTruthy();
  });

  it("bounds PostgreSQL RETURNING, not the mutation", () => {
    expect(rewrite("DELETE FROM items RETURNING *").queryText).toBe(
      "WITH rapidb_returning_cap AS (DELETE FROM items RETURNING *) SELECT * FROM rapidb_returning_cap LIMIT 11",
    );
    expect(
      rewrite("UPDATE rapidb_returning_cap SET id = 1 RETURNING *").queryText,
    ).toContain("WITH rapidb_returning_cap_ AS (");
    expect(
      rewrite("WITH c AS (DELETE FROM items RETURNING *) SELECT * FROM c")
        .queryText,
    ).toBe(
      "WITH c AS (DELETE FROM items RETURNING *) SELECT * FROM (SELECT * FROM c) AS rapidb_query_cap LIMIT 11",
    );
    expect(
      rewrite("WITH c AS (SELECT 1) INSERT INTO items SELECT * FROM c").error,
    ).toBeTruthy();
  });

  it.each([
    "pg",
    "sqlite",
    "mysql",
    "oracle",
  ] as const)("preflights and caps every statement on %s", (dialect) => {
    const result = rewrite(
      "INSERT INTO a VALUES (1); SELECT * FROM a; SELECT * FROM b;",
      dialect,
    );
    expect(result.error).toBeUndefined();
    expect(
      result.queryText.match(/LIMIT 11|FETCH FIRST 11 ROWS ONLY/g),
    ).toHaveLength(2);
    expect(
      rewrite("INSERT INTO a VALUES (1); EXPLAIN SELECT * FROM a", dialect)
        .error,
    ).toBeTruthy();
    expect(
      rewrite("WITH c AS (SELECT 1) SELECT * FROM c; SELECT * FROM a", dialect)
        .error,
    ).toBeUndefined();
  });
});
