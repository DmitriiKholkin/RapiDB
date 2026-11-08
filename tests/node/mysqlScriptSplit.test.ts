import { describe, expect, it } from "vitest";
import { splitMySQLScript } from "../../src/extension/dbDrivers/mysql";
import { applyHardCapToSqlQuery } from "../../src/extension/utils/sqlHardCap";

describe("splitMySQLScript", () => {
  it.each([
    "DELETE FROM accounts /*!50000 WHERE id = 1 */",
    "DELETE FROM accounts /*! WHERE id = 1 */",
    "DELETE FROM accounts /*M!100000 WHERE id = 1 */",
    "SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM accounts",
    "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */",
    "SELECT '/*! not a comment */'",
    "SELECT /*!80000 'a;b' */ 1",
  ])("preserves server-interpreted comments: %s", (sql) => {
    expect(splitMySQLScript(`${sql}; SELECT 2;`)).toEqual([sql, "SELECT 2"]);
  });

  it.each([
    "SELECT 1 /* unfinished",
    "DELETE FROM accounts /*! WHERE id = 1",
  ])("rejects unterminated comments before submitting any script: %s", (sql) =>
    expect(() => splitMySQLScript(sql)).toThrow(/Unterminated/));
  it("retains a cap after CR comments and separates tokens around block comments", () => {
    const capped = applyHardCapToSqlQuery(
      "SELECT * FROM items -- comment\r ORDER BY id",
      "mysql",
      11,
    );
    expect(splitMySQLScript(capped.queryText)).toEqual([
      "SELECT * FROM items LIMIT 11",
    ]);
    expect(splitMySQLScript("SELECT/* comment */1")).toEqual(["SELECT 1"]);
  });
  it("splits CREATE DATABASE IF NOT EXISTS + USE + CREATE TABLE script", () => {
    const sql = `CREATE DATABASE IF NOT EXISTS rapidb_test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE rapidb_test;

CREATE TABLE all_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    col_notnull VARCHAR(100) NOT NULL DEFAULT 'default_val'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`;

    const statements = splitMySQLScript(sql);

    expect(statements).toHaveLength(3);
    expect(statements[0]).toContain(
      "CREATE DATABASE IF NOT EXISTS rapidb_test",
    );
    expect(statements[1]).toBe("USE rapidb_test");
    expect(statements[2]).toContain("CREATE TABLE all_types");
  });

  it("splits inserts when the first statement uses REPEAT()", () => {
    const sql = `INSERT INTO all_types (col_longtext, col_notnull)
VALUES (REPEAT('MySQL long text test 0123456789 ', 32000), 'longtext_1mb');

INSERT INTO all_types (col_json, col_notnull)
VALUES (
    JSON_OBJECT(
        'string', 'hello',
        'integer', 42,
        'float', 3.14,
        'bool_t', JSON_ARRAY(true),
        'null_v', NULL,
        'array', JSON_ARRAY(1, 'two', true),
        'nested', JSON_OBJECT('deep', JSON_OBJECT('deeper', JSON_ARRAY(1,2,3)))
    ),
    'json_all_types'
);`;

    const statements = splitMySQLScript(sql);

    expect(statements).toHaveLength(2);
    expect(statements[0]).toContain("VALUES (REPEAT(");
    expect(statements[1]).toContain("JSON_OBJECT(");
  });

  it.each([
    [
      "DELIMITER $$\nCREATE PROCEDURE p() BEGIN IF 1 THEN SELECT 1; END IF; END$$\nDELIMITER ;\nSELECT 2;",
      "CREATE PROCEDURE p() BEGIN IF 1 THEN SELECT 1; END IF; END",
    ],
    [
      "DELIMITER $$\nCREATE PROCEDURE p() BEGIN DROP TABLE IF EXISTS t; SELECT IF(1,2,3), REPEAT('a',2); END$$\nDELIMITER ;\nSELECT 2;",
      "CREATE PROCEDURE p() BEGIN DROP TABLE IF EXISTS t; SELECT IF(1,2,3), REPEAT('a',2); END",
    ],
    [
      "DELIMITER //\nCREATE OR REPLACE PROCEDURE p(x INT) BEGIN lbl: LOOP IF x THEN LEAVE lbl; ELSEIF x < 0 THEN SET x = 1; END IF; CASE x WHEN 1 THEN SET x = CASE WHEN x THEN 1 END; END CASE; WHILE x DO REPEAT SET x = 0; UNTIL x END REPEAT; END WHILE; END LOOP lbl; END//\nDELIMITER ;\nSELECT 2;",
      "CREATE OR REPLACE PROCEDURE p(x INT) BEGIN lbl: LOOP IF x THEN LEAVE lbl; ELSEIF x < 0 THEN SET x = 1; END IF; CASE x WHEN 1 THEN SET x = CASE WHEN x THEN 1 END; END CASE; WHILE x DO REPEAT SET x = 0; UNTIL x END REPEAT; END WHILE; END LOOP lbl; END",
    ],
    [
      "BEGIN NOT ATOMIC IF 1 THEN SELECT 1; END IF; SELECT 3; END; SELECT 2;",
      "BEGIN NOT ATOMIC IF 1 THEN SELECT 1; END IF; SELECT 3; END",
    ],
    [
      "DELIMITER $$\nCREATE PROCEDURE p() BEGIN DECLARE v INT; SELECT CASE WHEN a THEN IF(b,1,2) ELSE 3 END INTO v; SELECT v; END$$\nDELIMITER ;\nSELECT 2;",
      "CREATE PROCEDURE p() BEGIN DECLARE v INT; SELECT CASE WHEN a THEN IF(b,1,2) ELSE 3 END INTO v; SELECT v; END",
    ],
    [
      "DELIMITER $$\nCREATE PROCEDURE p() BEGIN SET @v = CASE WHEN a THEN 1 ELSE IF(b,1,2) END; END$$\nDELIMITER ;\nSELECT 2;",
      "CREATE PROCEDURE p() BEGIN SET @v = CASE WHEN a THEN 1 ELSE IF(b,1,2) END; END",
    ],
    [
      "DELIMITER $$\nCREATE FUNCTION f() RETURNS TEXT DETERMINISTIC BEGIN RETURN CASE WHEN 1 THEN REPEAT('-',3) END; END$$\nDELIMITER ;\nSELECT 2;",
      "CREATE FUNCTION f() RETURNS TEXT DETERMINISTIC BEGIN RETURN CASE WHEN 1 THEN REPEAT('-',3) END; END",
    ],
    [
      "DELIMITER $$\nCREATE PROCEDURE p() BEGIN FOR i IN 1..3 DO SELECT i; END FOR; END$$\nDELIMITER ;\nSELECT 2;",
      "CREATE PROCEDURE p() BEGIN FOR i IN 1..3 DO SELECT i; END FOR; END",
    ],
  ])("closes compound statement blocks: %s", (sql, body) => {
    expect(splitMySQLScript(sql)).toEqual([body, "SELECT 2"]);
  });

  it("does not treat IF() or REPEAT() inside top-level CASE expressions as blocks", () => {
    expect(
      splitMySQLScript(
        "SELECT CASE WHEN a THEN IF(b,1,2) END FROM t; SELECT 2;",
      ),
    ).toEqual(["SELECT CASE WHEN a THEN IF(b,1,2) END FROM t", "SELECT 2"]);
    expect(
      splitMySQLScript(
        "UPDATE t SET a = CASE WHEN x THEN IF(b,1,2) ELSE REPEAT('a',2) END; DELETE FROM t WHERE a IS NULL;",
      ),
    ).toEqual([
      "UPDATE t SET a = CASE WHEN x THEN IF(b,1,2) ELSE REPEAT('a',2) END",
      "DELETE FROM t WHERE a IS NULL",
    ]);
  });

  it.each([
    "CREATE PROCEDURE p() BEGIN UPDATE t SET t.end=1; DELETE FROM t; END",
    "CREATE PROCEDURE p() BEGIN UPDATE t SET end = 1, begin = 2 WHERE end > begin; DELETE FROM t; END",
    "CREATE PROCEDURE p() BEGIN SELECT r.begin, r.end, @end FROM ranges r ORDER BY end; DELETE FROM t; END",
    "CREATE PROCEDURE p() BEGIN SET @v = CASE WHEN end THEN begin ELSE end END; DELETE FROM t; END",
    "CREATE PROCEDURE p() BEGIN INSERT INTO t (begin, end) VALUES (1, 2); REPEAT SET x = 0; UNTIL end END REPEAT; END",
    "CREATE PROCEDURE p() BEGIN IF end_at THEN BEGIN SELECT case_id FROM t; END; END IF; END",
    "CREATE PROCEDURE p() BEGIN SELECT t.case FROM t; END",
    "CREATE PROCEDURE p() BEGIN DECLARE end INT; ALTER TABLE t ADD COLUMN end INT; SELECT t. end FROM t; DELETE FROM t; END",
    "CREATE PROCEDURE p() BEGIN SELECT CASE WHEN x THEN 1 ELSE t.interval END AS iv FROM t; DELETE FROM t; END",
    "SELECT CASE WHEN x THEN 1 ELSE t.interval END AS iv FROM t",
    "SELECT CASE WHEN x THEN 1 ELSE 0 /*!80000 + 0 */ END FROM t",
  ])("treats BEGIN/END/CASE used as identifiers as columns: %s", (body) => {
    expect(splitMySQLScript(`${body}; SELECT 2;`)).toEqual([body, "SELECT 2"]);
  });

  it.each([
    "CREATE PROCEDURE p() BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; END; DELETE FROM t; END",
    "CREATE PROCEDURE p() BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '23000', NOT FOUND, 1062 BEGIN SET @dup = 1; END; DELETE FROM t; END",
    "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW BEGIN DECLARE EXIT HANDLER FOR SQLWARNING BEGIN END; SET NEW.x = 1; END",
    "CREATE DEFINER=`app user`@`%` PROCEDURE p() BEGIN DELETE FROM log; UPDATE s SET n = n + 1; END",
    "CREATE /*!50017 DEFINER=`app`@`%` */ PROCEDURE p() BEGIN DELETE FROM log; UPDATE s SET n = n + 1; END",
    "ALTER EVENT e DO BEGIN DELETE FROM log; UPDATE s SET n = n + 1; END",
  ])("keeps routine bodies with nested or unusual headers together: %s", (body) => {
    expect(splitMySQLScript(`${body}; SELECT 2;`)).toEqual([body, "SELECT 2"]);
    expect(
      splitMySQLScript(`DELIMITER $$\n${body}$$\nDELIMITER ;\nSELECT 2;`),
    ).toEqual([body, "SELECT 2"]);
  });

  it("recognizes BEGIN NOT ATOMIC across comments", () => {
    const statements = splitMySQLScript(
      "BEGIN /* c */ NOT ATOMIC DELETE FROM log; UPDATE s SET n = 1; END; SELECT 2;",
    );
    expect(statements).toHaveLength(2);
    expect(statements[1]).toBe("SELECT 2");
  });

  it.each([
    [
      "CREATE PROCEDURE p() UPDATE t SET begin = 1; DELETE FROM t;",
      ["CREATE PROCEDURE p() UPDATE t SET begin = 1", "DELETE FROM t"],
    ],
    [
      "CREATE PROCEDURE p(OUT begin INT) SELECT 1 INTO begin; DELETE FROM t;",
      [
        "CREATE PROCEDURE p(OUT begin INT) SELECT 1 INTO begin",
        "DELETE FROM t",
      ],
    ],
  ])("does not open a block for BEGIN used as a column in a single-statement routine: %s", (sql, statements) => {
    expect(splitMySQLScript(sql)).toEqual(statements);
  });

  it("splits transaction BEGIN and COMMIT as ordinary statements", () => {
    expect(splitMySQLScript("BEGIN; SELECT 1; COMMIT;")).toEqual([
      "BEGIN",
      "SELECT 1",
      "COMMIT",
    ]);
  });
});
