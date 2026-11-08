import { describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";

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
    id: "oracle-compile-targets",
    name: "oracle-compile-targets",
    type: "oracle",
  });
  (driver as unknown as { pool: unknown }).pool = {
    getConnection: vi.fn(async () => connection),
  };
  return { driver, connection };
}

function allErrorsByType(
  rowsByType: Record<string, { ATTRIBUTE: string; TEXT: string }[]>,
) {
  const lookups: unknown[][] = [];
  const harness = oracleHarness((sql, params = []) => {
    if (!sql.includes("all_errors")) {
      return {
        warning: {
          code: "NJS-700",
          message: "NJS-700: creation succeeded with compilation errors",
        },
      };
    }
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

const WARNING_ONLY = [{ ATTRIBUTE: "WARNING", TEXT: "PLW-05018: warning" }];
const STALE_ERROR = [
  { ATTRIBUTE: "ERROR", TEXT: "PLS-00201: identifier 'X' must be declared" },
];

describe("Oracle compiled object detection", () => {
  it.each([
    [
      "CREATE PROCEDURE IF NOT EXISTS app.p AS BEGIN NULL; END;",
      "APP",
      "P",
      "PROCEDURE",
    ],
    [
      'CREATE OR REPLACE EDITIONABLE PACKAGE BODY if not exists "Pkg" AS END;',
      null,
      "Pkg",
      "PACKAGE BODY",
    ],
    ["CREATE VIEW IF NOT EXISTS v AS SELECT x FROM t", null, "V", "VIEW"],
    ["ALTER PROCEDURE IF EXISTS s.p COMPILE", "S", "P", "PROCEDURE"],
  ])("reads the object name after IF [NOT] EXISTS in %s", async (sql, owner, name, type) => {
    const { driver, lookups } = allErrorsByType({ [type]: WARNING_ONLY });
    await expect(driver.query(sql)).resolves.toMatchObject({ rowCount: 0 });
    expect(lookups).toEqual([[owner, name, type]]);
  });

  it.each([
    ["ALTER PACKAGE app.p COMPILE SPECIFICATION", ["PACKAGE"]],
    [
      "ALTER PACKAGE app.p EDITIONABLE COMPILE DEBUG SPECIFICATION REUSE SETTINGS",
      ["PACKAGE"],
    ],
    ["alter type app.p compile specification;", ["TYPE"]],
  ])("checks only the spec for %s", async (sql, types) => {
    const { driver, connection, lookups } = allErrorsByType({
      PACKAGE: WARNING_ONLY,
      TYPE: WARNING_ONLY,
      "PACKAGE BODY": STALE_ERROR,
      "TYPE BODY": STALE_ERROR,
    });
    await expect(driver.query(sql)).resolves.toMatchObject({ rowCount: 0 });
    expect(lookups).toEqual([["APP", "P", ...types]]);
    expect(connection.commit).toHaveBeenCalledOnce();
  });

  it.each([
    ["ALTER PACKAGE app.p COMPILE BODY", ["PACKAGE BODY"]],
    ["ALTER TYPE app.p COMPILE DEBUG BODY", ["TYPE BODY"]],
    ["ALTER PACKAGE app.p COMPILE PACKAGE", ["PACKAGE", "PACKAGE BODY"]],
    ["ALTER PACKAGE app.p COMPILE", ["PACKAGE", "PACKAGE BODY"]],
    ["ALTER TYPE app.p COMPILE BODYX", ["TYPE", "TYPE BODY"]],
  ])("checks the units compiled by %s", async (sql, types) => {
    const { driver, lookups } = allErrorsByType({});
    await expect(driver.query(sql)).rejects.toThrow(
      "Oracle compilation failed for P",
    );
    expect(lookups).toEqual([["APP", "P", ...types]]);
  });
});

describe("Oracle literal session-state scan", () => {
  it.each([
    ["EXECUTE /*", "EXECUTE /*"],
    ["alter /* ", "alter /* "],
    ["set --", "set --"],
    ["--ALTER\\n", "--ALTER\n"],
  ])("scans a 400 KB literal of %s in linear time", async (_label, unit) => {
    const literal = unit.repeat(Math.ceil(400_000 / unit.length));
    const { driver, connection } = oracleHarness(() => ({}));
    const started = performance.now();
    await driver.query(`INSERT INTO notes VALUES ('${literal}')`);
    expect(performance.now() - started).toBeLessThan(200);
    expect(connection.close).toHaveBeenCalledWith();
  });

  it("still drops the connection for session changes hidden in a large literal", async () => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(
      `BEGIN EXECUTE IMMEDIATE '${"EXECUTE /*".repeat(40_000)} */ ALTER/**/ -- x\n SESSION SET NLS_SORT=BINARY_CI'; END;`,
    );
    expect(connection.close).toHaveBeenCalledWith({ drop: true });
  });

  it.each([
    ["ALTER SESSION after comments", "ALTER /* a */ -- b\n\tSESSION SET X=1"],
    ["SET ROLE inside a comment", "/* SET ROLE ALL */"],
    ["a line comment ending the text", "EXECUTE --\nIMMEDIATE"],
    ["DBMS_SQL", "x$dbms_sql.parse"],
    ["DBMS_SESSION", "DBMS_SESSION.SET_ROLE"],
  ])("drops the connection for %s in a literal", async (_label, text) => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(`INSERT INTO notes VALUES ('${text}')`);
    expect(connection.close).toHaveBeenCalledWith({ drop: true });
  });

  it.each([
    ["an assignment to a column named DBMS_SESSION", "dbms_session = 1"],
    ["ALTER SESSION = x", "alter session = x"],
    ["an unclosed comment", "ALTER /* SESSION"],
    [
      "words that only contain the keywords",
      "ALTERED SESSIONS, EXECUTE_IMMEDIATE",
    ],
  ])("keeps the connection for %s in a literal", async (_label, text) => {
    const { driver, connection } = oracleHarness(() => ({}));
    await driver.query(`INSERT INTO notes VALUES ('${text}')`);
    expect(connection.close).toHaveBeenCalledWith();
  });
});
