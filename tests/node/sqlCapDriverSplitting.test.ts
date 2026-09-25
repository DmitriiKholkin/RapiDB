import { describe, expect, it } from "vitest";
import { splitMssqlBatches } from "../../src/extension/dbDrivers/mssql";
import { splitOracleStatements } from "../../src/extension/dbDrivers/oracle";
import { applyHardCapToSqlQuery } from "../../src/extension/utils/sqlHardCap";

describe("cap and driver lexical agreement", () => {
  it("keeps Oracle q/nq literals and quoted identifiers inside one capped statement", () => {
    for (const sql of [
      "SELECT q'[quote'; SELECT text]' AS label FROM dual",
      "SELECT nq'{quote'; SELECT text}' AS label FROM dual",
      'SELECT 1 AS "semi;""colon" FROM dual',
    ]) {
      const result = applyHardCapToSqlQuery(sql, "oracle", 11);
      expect(result.error).toBeUndefined();
      expect(splitOracleStatements(result.queryText)).toEqual([
        result.queryText,
      ]);
    }
    expect(
      splitOracleStatements("SELECT 1 FROM dual; SELECT 2 FROM dual;"),
    ).toEqual(["SELECT 1 FROM dual", "SELECT 2 FROM dual"]);
  });

  it("only splits real GO lines, not literal/comment/identifier contents", () => {
    for (const sql of [
      "SELECT 'before\nGO\nafter' AS value",
      "SELECT /* before\nGO\nafter */ 1 AS value",
      "SELECT 1 AS [before\nGO\nafter]",
    ]) {
      const result = applyHardCapToSqlQuery(sql, "mssql", 11);
      expect(result.error).toBeUndefined();
      expect(splitMssqlBatches(result.queryText)).toEqual([result.queryText]);
    }
    expect(splitMssqlBatches("SELECT 1\r\nGO\r\nSELECT 2\nGO 2\n")).toEqual([
      "SELECT 1",
      "SELECT 2",
    ]);
  });
});
