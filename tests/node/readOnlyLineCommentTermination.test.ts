import { describe, expect, it } from "vitest";
import {
  mayChangeDatabaseSchema,
  sqlReadOnlyQueryGuard,
} from "../../src/extension/utils/readOnlyGuards";

describe("read-only line comment termination", () => {
  it.each([
    ["mysql", 'GET_LOCK("x", 0)'],
    ["sqlite", "1; DELETE FROM t"],
  ] as const)("does not end %s '--' comments at a carriage return", (dialect, call) => {
    expect(
      sqlReadOnlyQueryGuard(`SELECT 1 -- \r'\n, ${call} -- '`, dialect).allowed,
    ).toBe(false);
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 -- \r'\n; DELETE FROM t -- '", dialect)
        .allowed,
    ).toBe(false);
    expect(
      mayChangeDatabaseSchema(
        "SELECT 1 -- \r'\n; CREATE TABLE t (id INT) -- '",
        dialect,
      ),
    ).toBe(true);
  });

  it("does not end MySQL '#' comments at a carriage return", () => {
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 # \r'\n, GET_LOCK(\"x\", 0) # '", "mysql")
        .allowed,
    ).toBe(false);
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 #\r; DELETE FROM t", "mysql"),
    ).toEqual({ allowed: true });
  });

  it.each([
    "mysql",
    "sqlite",
  ] as const)("keeps %s text after a carriage return inside the comment", (dialect) => {
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 -- \r; DELETE FROM t", dialect),
    ).toEqual({ allowed: true });
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 -- note\r\n; DELETE FROM t", dialect)
        .allowed,
    ).toBe(false);
  });

  it("ends PostgreSQL '--' comments at a carriage return like its lexer", () => {
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 -- \r; DELETE FROM t", "postgresql")
        .allowed,
    ).toBe(false);
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 -- '\r, nextval('s') -- '", "postgresql")
        .allowed,
    ).toBe(false);
  });

  it.each([
    "transactsql",
    "plsql",
    "sql",
  ] as const)("rejects a lone carriage return inside a %s line comment", (dialect) => {
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 -- \r'\nSELECT 2 -- '", dialect).allowed,
    ).toBe(false);
    expect(
      mayChangeDatabaseSchema("SELECT 1 -- \r'\nSELECT 2 -- '", dialect),
    ).toBe(true);
  });

  it("keeps CRLF-terminated comments readable for every dialect", () => {
    for (const dialect of [
      "mysql",
      "sqlite",
      "postgresql",
      "transactsql",
      "sql",
    ] as const) {
      expect(
        sqlReadOnlyQueryGuard("SELECT 1 -- note\r\nSELECT 2", dialect),
      ).toEqual({ allowed: true });
    }
  });

  it("rejects generic SQL comment forms whose meaning depends on the backend", () => {
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 --x'\n, GET_LOCK('x', 0) --'", "sql")
        .allowed,
    ).toBe(false);
    expect(
      sqlReadOnlyQueryGuard("SELECT 1 #'\n, GET_LOCK('x', 0) #'", "sql")
        .allowed,
    ).toBe(false);
  });
});
