import { describe, expect, it, vi } from "vitest";
import {
  OracleDriver,
  splitOracleStatements,
} from "../../src/extension/dbDrivers/oracle";

function oracleHarness(execute: (sql: string, params?: unknown[]) => unknown) {
  const connection = {
    execute: vi.fn(async (sql: string, params?: unknown[]) =>
      sql.startsWith("ALTER SESSION SET NLS_") ? {} : execute(sql, params),
    ),
    commit: vi.fn(async () => {}),
    rollback: vi.fn(async () => {}),
    close: vi.fn(async (_options?: unknown) => {}),
    break: vi.fn(async () => {}),
  };
  const driver = new OracleDriver({
    id: "oracle-script-safety",
    name: "oracle-script-safety",
    type: "oracle",
  });
  (driver as unknown as { pool: unknown }).pool = {
    getConnection: vi.fn(async () => connection),
  };
  return { driver, connection };
}

function userStatements(connection: {
  execute: { mock: { calls: unknown[][] } };
}): string[] {
  return connection.execute.mock.calls
    .map(([sql]) => String(sql))
    .filter((sql) => !sql.startsWith("ALTER SESSION SET NLS_"));
}

const CLEANUP_PROCEDURE = [
  "CREATE OR REPLACE PROCEDURE cleanup_orders AS",
  "  c_tag CONSTANT VARCHAR2(10) := 'nightly';",
  "BEGIN",
  "  DELETE FROM orders WHERE created_at < SYSDATE - 30;",
  "END;",
].join("\n");

const THIN_COMPILATION_WARNING = {
  code: "NJS-700",
  message: "NJS-700: creation succeeded with compilation errors",
};

const THICK_COMPILATION_WARNING = {
  code: "NJS-700",
  errorNum: 24344,
  message:
    "NJS-700: creation succeeded with compilation errors\nORA-24344: success with compilation error",
};

const COMPILATION_WARNING = THIN_COMPILATION_WARNING;

describe("Oracle PL/SQL block detection in scripts", () => {
  it.each([
    ["a -- comment", "-- Bob's nightly cleanup\n"],
    ["a /* */ comment", "/* it's the nightly cleanup */\n"],
    ["several comments", "-- don't\n/* won't */ -- can't\n"],
    ["a 2000-character comment", `-- ${"x".repeat(2000)}\n`],
  ])("keeps a procedure whole after an apostrophe in %s", (_label, prefix) => {
    expect(
      splitOracleStatements(
        `${prefix}${CLEANUP_PROCEDURE}\n/\nSELECT 1 FROM dual;\n`,
      ),
    ).toEqual([`${prefix}${CLEANUP_PROCEDURE}`.trim(), "SELECT 1 FROM dual"]);
  });

  it("keeps a procedure whole with a long comment inside its header", () => {
    const procedure = CLEANUP_PROCEDURE.replace(
      "CREATE OR REPLACE PROCEDURE",
      `CREATE OR REPLACE /* ${"x".repeat(1500)} */ PROCEDURE`,
    );
    expect(
      splitOracleStatements(`${procedure}\n/\nSELECT 1 FROM dual;\n`),
    ).toEqual([procedure, "SELECT 1 FROM dual"]);
  });

  it.each([
    "q'[it's; done]'",
    "q'{it's; done}'",
    "q'(it's; done)'",
    "q'<it's; done>'",
    "q'!it's; done!'",
    "nq'[it's; done]'",
    "Nq'{it's; done}'",
  ])("keeps blocks with %s whole after a commented apostrophe", (literal) => {
    const block = `-- don't split\nBEGIN\n  INSERT INTO log VALUES (${literal});\n  COMMIT;\nEND;`;
    expect(splitOracleStatements(`${block}\n/\nSELECT 2 FROM dual`)).toEqual([
      block,
      "SELECT 2 FROM dual",
    ]);
  });

  it("keeps a WITH FUNCTION query whole after a commented apostrophe", () => {
    const query =
      "-- it's inline\nWITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nSELECT f FROM dual";
    expect(splitOracleStatements(`${query};\nSELECT 2 FROM dual;`)).toEqual([
      query,
      "SELECT 2 FROM dual",
    ]);
  });

  it("keeps a procedure with a quoted apostrophe name whole", () => {
    const procedure = CLEANUP_PROCEDURE.replace("cleanup_orders", `"it's"`);
    expect(
      splitOracleStatements(`${procedure}\n/\nSELECT 1 FROM dual;\n`),
    ).toEqual([procedure, "SELECT 1 FROM dual"]);
  });

  it.each([
    "<<outer>>\nBEGIN\n  NULL;\n  NULL;\nEND outer;",
    '<<"Main Block">> DECLARE\n  v NUMBER;\nBEGIN\n  v := 1;\nEND;',
    "<<a>> <<b>>\nBEGIN\n  NULL;\nEND;",
    '<<"it\'s">>\nBEGIN\n  NULL;\n  NULL;\nEND;',
  ])("keeps labelled block %j whole", (block) => {
    expect(splitOracleStatements(`${block}\n/\nSELECT 3 FROM dual`)).toEqual([
      block,
      "SELECT 3 FROM dual",
    ]);
  });

  it("still splits plain SQL that follows a commented apostrophe", () => {
    expect(
      splitOracleStatements(
        "-- it's fine\nSELECT 'a;b' FROM dual; SELECT \"it's\" FROM t;",
      ),
    ).toEqual([
      "-- it's fine\nSELECT 'a;b' FROM dual",
      'SELECT "it\'s" FROM t',
    ]);
  });

  it("sends a commented procedure to Oracle as one statement", async () => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(`-- Bob's nightly cleanup\n${CLEANUP_PROCEDURE}\n/\n`);
    expect(userStatements(connection)).toEqual([
      `-- Bob's nightly cleanup\n${CLEANUP_PROCEDURE}`,
    ]);
  });
});

describe("Oracle PL/SQL compilation errors", () => {
  it.each([
    ["Thin", THIN_COMPILATION_WARNING],
    ["Thick", THICK_COMPILATION_WARNING],
  ])("reports a %s-mode compilation warning with ALL_ERRORS details instead of success", async (_mode, warning) => {
    const lookups: unknown[][] = [];
    const { driver, connection } = oracleHarness((sql, params) => {
      if (sql.includes("all_errors")) {
        lookups.push(params ?? []);
        return {
          rows: [
            {
              TYPE: "PROCEDURE",
              LINE: 1,
              POSITION: 26,
              TEXT: "PLS-00201: identifier 'X' must be declared\n",
              ATTRIBUTE: "ERROR",
            },
          ],
        };
      }
      return { warning };
    });

    const rejected = await driver
      .query("CREATE OR REPLACE PROCEDURE p AS BEGIN x; END;")
      .catch((error: unknown) => error);

    expect(rejected).toBeInstanceOf(Error);
    expect((rejected as Error).message).toContain(
      "Oracle compilation failed for P; the object was saved in an invalid state. NJS-700: creation succeeded with compilation errors",
    );
    expect((rejected as Error).message).toContain(
      "PROCEDURE line 1, column 26: PLS-00201: identifier 'X' must be declared",
    );
    expect(lookups).toEqual([[null, "P", "PROCEDURE"]]);
    expect(connection.commit).not.toHaveBeenCalled();
  });

  function allErrorsByType(
    rowsByType: Record<string, { ATTRIBUTE: string; TEXT: string }[]>,
  ) {
    const lookups: unknown[][] = [];
    const harness = oracleHarness((sql, params = []) => {
      if (!sql.includes("all_errors")) return { warning: COMPILATION_WARNING };
      lookups.push(params);
      return {
        rows: params.slice(2).flatMap((type) =>
          (rowsByType[String(type)] ?? []).map((row) => ({
            TYPE: type,
            LINE: 1,
            POSITION: 1,
            ...row,
          })),
        ),
      };
    });
    return { ...harness, lookups };
  }

  const STALE_BODY_ERRORS = {
    PACKAGE: [
      {
        ATTRIBUTE: "WARNING",
        TEXT: "PLW-05018: unit P omitted optional AUTHID clause",
      },
    ],
    "PACKAGE BODY": [
      {
        ATTRIBUTE: "ERROR",
        TEXT: "PLS-00201: identifier 'X' must be declared",
      },
    ],
  };

  it("ignores stale errors of a same-named package body when a spec compiles with warnings", async () => {
    const { driver, connection, lookups } = allErrorsByType(STALE_BODY_ERRORS);
    await expect(
      driver.query("CREATE OR REPLACE PACKAGE p AS PROCEDURE q; END;"),
    ).resolves.toMatchObject({ rowCount: 0 });
    expect(lookups).toEqual([[null, "P", "PACKAGE"]]);
    expect(connection.commit).toHaveBeenCalledOnce();
  });

  it("checks both spec and body errors for ALTER PACKAGE ... COMPILE", async () => {
    const { driver, lookups } = allErrorsByType(STALE_BODY_ERRORS);
    await expect(driver.query("ALTER PACKAGE app.p COMPILE")).rejects.toThrow(
      /Oracle compilation failed for P[\s\S]*PACKAGE BODY line 1/,
    );
    expect(lookups).toEqual([["APP", "P", "PACKAGE", "PACKAGE BODY"]]);
  });

  it.each([
    ["TYPE BODY", 'CREATE OR REPLACE TYPE BODY "T" AS END;', "T"],
    [
      "TRIGGER",
      "CREATE OR REPLACE TRIGGER trg BEFORE INSERT ON t BEGIN NULL; END;",
      "TRG",
    ],
    [
      "FUNCTION",
      "CREATE OR REPLACE FUNCTION f(a NUMBER) RETURN NUMBER AS BEGIN RETURN a; END;",
      "F",
    ],
    ["VIEW", "CREATE OR REPLACE FORCE VIEW v AS SELECT x FROM missing", "V"],
  ])("filters ALL_ERRORS by %s", async (type, sql, name) => {
    const { driver, lookups } = allErrorsByType({
      [type]: [{ ATTRIBUTE: "WARNING", TEXT: "PLW-06009: warning" }],
    });
    await expect(driver.query(sql)).resolves.toMatchObject({ rowCount: 0 });
    expect(lookups).toEqual([[null, name, type]]);
  });

  it("falls back to the generic error when the object name is not recognised", async () => {
    const { driver, lookups } = allErrorsByType({
      PROCEDURE: [{ ATTRIBUTE: "WARNING", TEXT: "PLW-05018: warning" }],
    });
    await expect(
      driver.query("CREATE OR REPLACE PROCEDURE café AS BEGIN x; END;"),
    ).rejects.toThrow(
      "Oracle compilation failed; the object was saved in an invalid state. NJS-700",
    );
    expect(lookups).toEqual([]);
  });

  it("stops a script at the failed compile", async () => {
    const { driver, connection } = oracleHarness((sql) => {
      if (sql.includes("all_errors")) {
        return {
          rows: [
            {
              TYPE: "PACKAGE BODY",
              LINE: 3,
              POSITION: 5,
              TEXT: "PL/SQL: Statement ignored",
              ATTRIBUTE: "ERROR",
            },
          ],
        };
      }
      return sql.startsWith("CREATE")
        ? { warning: COMPILATION_WARNING }
        : { rowsAffected: 1 };
    });

    await expect(
      driver.query(
        'CREATE OR REPLACE PACKAGE BODY "App"."Pkg" AS\nPROCEDURE p IS BEGIN x; END;\nEND;\n/\nINSERT INTO t VALUES (1);',
      ),
    ).rejects.toThrow(/Oracle compilation failed for Pkg[\s\S]*PACKAGE BODY/);

    const executed = userStatements(connection);
    expect(executed.some((sql) => sql.startsWith("INSERT"))).toBe(false);
    expect(connection.execute).toHaveBeenCalledWith(
      expect.stringContaining("all_errors"),
      ["App", "Pkg", "PACKAGE BODY"],
      expect.anything(),
    );
    expect(connection.commit).not.toHaveBeenCalled();
  });

  it("fails even when the error details cannot be read", async () => {
    const { driver } = oracleHarness((sql) => {
      if (sql.includes("all_errors")) throw new Error("ORA-00942");
      return { warning: COMPILATION_WARNING };
    });
    await expect(
      driver.query(
        "CREATE OR REPLACE TRIGGER trg BEFORE INSERT ON t BEGIN x; END;",
      ),
    ).rejects.toThrow(/NJS-700/);
  });

  it("accepts objects that compiled with PL/SQL warnings only", async () => {
    const { driver, connection } = oracleHarness((sql) =>
      sql.includes("all_errors")
        ? {
            rows: [
              {
                TYPE: "PROCEDURE",
                LINE: 1,
                POSITION: 1,
                TEXT: "PLW-05018: unit P omitted optional AUTHID clause",
                ATTRIBUTE: "WARNING",
              },
            ],
          }
        : { warning: COMPILATION_WARNING },
    );
    await expect(
      driver.query("CREATE OR REPLACE PROCEDURE p AS BEGIN NULL; END;"),
    ).resolves.toMatchObject({ rowCount: 0 });
    expect(connection.commit).toHaveBeenCalledOnce();
  });
});

describe("Oracle dynamic SQL session-state detection", () => {
  it.each([
    "BEGIN EXECUTE IMMEDIATE 'ALTER ' || 'SESSION SET NLS_SORT=BINARY_CI'; END;",
    "DECLARE s VARCHAR2(10) := 'ALTER'; BEGIN EXECUTE IMMEDIATE s || ' SESSION SET TIME_ZONE = ''UTC'''; END;",
    "BEGIN EXECUTE IMMEDIATE (v_sql); END;",
    "DECLARE c INTEGER := DBMS_SQL.OPEN_CURSOR; n INTEGER; BEGIN DBMS_SQL.PARSE(c, v, DBMS_SQL.NATIVE); n := DBMS_SQL.EXECUTE(c); END;",
    "BEGIN SYS.DBMS_UTILITY.EXEC_DDL_STATEMENT(v_sql); END;",
    "BEGIN EXECUTE IMMEDIATE 'BEGIN EXECUTE IMMEDIATE v; END;'; END;",
    "BEGIN EXECUTE IMMEDIATE 'DECLARE s VARCHAR2(9) := ''--''; BEGIN EXECUTE IMMEDIATE ''ALTER '' || ''SESSION SET NLS_SORT=BINARY_CI''; END;'; END;",
    "BEGIN EXECUTE IMMEDIATE 'DECLARE s VARCHAR2(9) := ''--''; BEGIN EXECUTE/**/IMMEDIATE ''ALTER '' || ''SESSION SET NLS_SORT=BINARY_CI''; END;'; END;",
    "BEGIN EXECUTE IMMEDIATE 'DECLARE s VARCHAR2(9) := ''--''; BEGIN execute -- x\n immediate v; END;'; END;",
    "BEGIN EXECUTE IMMEDIATE 'DECLARE s VARCHAR2(9) := ''--''; BEGIN DBMS_SESSION.SET_ROLE(''ALL''); END;'; END;",
    "BEGIN execute/**/immediate 'ALTER ' || 'SESSION SET NLS_SORT=BINARY_CI'; END;",
    "begin execute immediate 'alter ' || 'session set nls_sort=binary_ci'; end;",
    "BEGIN EXECUTE IMMEDIATE q'[ALTER ]' || 'SESSION SET NLS_SORT=BINARY_CI'; END;",
  ])("drops the connection after %s", async (sql) => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(sql);
    expect(connection.close).toHaveBeenCalledWith({ drop: true });
  });

  it.each([
    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE t'; EXCEPTION WHEN OTHERS THEN NULL; END;",
    "BEGIN EXECUTE IMMEDIATE 'INSERT INTO t VALUES (:1)' USING 1; END;",
    "BEGIN EXECUTE IMMEDIATE q'[DELETE FROM t WHERE c = 'x']'; END;",
    "INSERT INTO notes VALUES ('-- O''Brien /* :) */', 'execute later')",
  ])("returns the connection to the pool after %s", async (sql) => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(sql);
    expect(connection.close).toHaveBeenCalledWith();
  });
});
