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

  it.each([
    "DELIMITER $$ \nCREATE PROCEDURE p() BEGIN SELECT 1; END$$\nDELIMITER ; \nSELECT 2;\nSELECT 3;",
    "DELIMITER $$\t-- switch\r\nCREATE PROCEDURE p() BEGIN SELECT 1; END$$\r\nDELIMITER ;\t\r\nSELECT 2;\r\nSELECT 3;",
    "  DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END$$\n\tdelimiter ;\nSELECT 2;\nSELECT 3;",
    "-- setup\nDELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END $$\nDELIMITER ;\nSELECT 2;\nSELECT 3;",
  ])("reads the DELIMITER argument as one token: %j", (sql) => {
    expect(splitMySQLScript(sql)).toEqual([
      "CREATE PROCEDURE p() BEGIN SELECT 1; END",
      "SELECT 2",
      "SELECT 3",
    ]);
  });

  it("does not treat a DELIMITER line inside a statement as a directive", () => {
    expect(
      splitMySQLScript("SELECT id,\ndelimiter\nFROM t;\nSELECT 2;\nSELECT 3;"),
    ).toEqual(["SELECT id,\ndelimiter\nFROM t", "SELECT 2", "SELECT 3"]);
    expect(
      splitMySQLScript("SELECT id,\n  delimiter AS d\nFROM t;\nSELECT 2;"),
    ).toEqual(["SELECT id,\n  delimiter AS d\nFROM t", "SELECT 2"]);
    expect(
      splitMySQLScript(
        "CREATE PROCEDURE p() BEGIN\nDELIMITER $$\nSELECT 1; END; SELECT 2;",
      ),
    ).toEqual([
      "CREATE PROCEDURE p() BEGIN\nDELIMITER $$\nSELECT 1; END",
      "SELECT 2",
    ]);
  });

  it("requires the DELIMITER argument on the same line", () => {
    expect(splitMySQLScript("DELIMITER\n$$;\nSELECT 2;")).toEqual([
      "DELIMITER\n$$",
      "SELECT 2",
    ]);
  });

  it.each([
    "ALTER TABLE event ADD begin INT",
    "ALTER TABLE `procedure` ADD begin INT",
    "ALTER TABLE t ADD begin INT COMMENT 'function'",
  ])("does not open a block for BEGIN in non-routine DDL: %s", (sql) => {
    expect(splitMySQLScript(`${sql}; SELECT 2; SELECT 3`)).toEqual([
      sql,
      "SELECT 2",
      "SELECT 3",
    ]);
  });

  it.each([
    "CREATE OR REPLACE DEFINER = CURRENT_USER() PROCEDURE p() BEGIN DELETE FROM log; SELECT 1; END",
    "CREATE DEFINER='app'@'localhost' FUNCTION f() RETURNS INT BEGIN DELETE FROM log; RETURN 1; END",
    "CREATE DEFINER=app@localhost EVENT IF NOT EXISTS e ON SCHEDULE EVERY 1 DAY DO BEGIN DELETE FROM log; SELECT 1; END",
    "CREATE AGGREGATE FUNCTION agg(x INT) RETURNS INT BEGIN DECLARE s INT DEFAULT 0; RETURN s; END",
    "ALTER DEFINER = `a``b`@`%` EVENT e DO BEGIN DELETE FROM log; SELECT 1; END",
    "/* header */ CREATE /*!50017 DEFINER=`app`@`%` */ TRIGGER trg BEFORE INSERT ON t FOR EACH ROW BEGIN SET NEW.a = 1; SET NEW.b = 2; END",
  ])("still recognizes routine headers with modifiers: %s", (body) => {
    const statements = splitMySQLScript(`${body}; SELECT 2;`);
    expect(statements).toHaveLength(2);
    expect(statements[0]).toMatch(/END$/);
    expect(statements[1]).toBe("SELECT 2");
  });

  it("recognizes a DEFINER account with backslash-escaped quotes", () => {
    const body =
      "CREATE DEFINER='o\\'neil'@'%' PROCEDURE p() BEGIN DELETE FROM log; SELECT 1; END";
    expect(splitMySQLScript(`${body}; SELECT 2;`)).toEqual([body, "SELECT 2"]);
    const doubled =
      "CREATE DEFINER='o''neil'@'%' PROCEDURE p() BEGIN DELETE FROM log; SELECT 1; END";
    expect(
      splitMySQLScript(`${doubled}; SELECT 2;`, { backslashEscapes: false }),
    ).toEqual([doubled, "SELECT 2"]);
    expect(
      splitMySQLScript(
        'CREATE DEFINER="a\\"b"@"%" FUNCTION f() RETURNS INT BEGIN DELETE FROM log; RETURN 1; END; SELECT 2;',
      ),
    ).toHaveLength(2);
  });

  it.each([
    "CREATE PROCEDURE IF NOT EXISTS p() BEGIN DELETE FROM log; SELECT 1; END",
    "CREATE OR REPLACE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW BEGIN SET NEW.a = 1; SET NEW.b = 2; END",
    "CREATE PROCEDURE p() lbl: BEGIN DELETE FROM log; LEAVE lbl; END lbl",
  ])("opens a routine block without DELIMITER: %s", (body) => {
    expect(splitMySQLScript(`${body}; SELECT 2;`)).toEqual([body, "SELECT 2"]);
  });

  it("accepts DELIMITER ; at end of input without a trailing newline", () => {
    expect(
      splitMySQLScript(
        "DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END$$\nDELIMITER ;",
      ),
    ).toEqual(["CREATE PROCEDURE p() BEGIN SELECT 1; END"]);
  });

  it("splits a mysqldump trigger section using DELIMITER ;;", () => {
    const trigger =
      "/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `t_bi` BEFORE INSERT ON `t` FOR EACH ROW BEGIN\n  SET NEW.a = 1;\n  SET NEW.b = 2;\nEND */";
    for (const eol of ["\n", "\r\n"]) {
      const eolTrigger = trigger.replace(/\n/g, eol);
      const dump = [
        "/*!50003 SET @saved_cs_client = @@character_set_client */ ;",
        "/*!50003 SET sql_mode = 'STRICT_TRANS_TABLES' */ ;",
        "DELIMITER ;;",
        `${eolTrigger};;`,
        "DELIMITER ;",
        "/*!50003 SET sql_mode = @saved_sql_mode */ ;",
        "INSERT INTO t VALUES (1);",
        "",
      ].join(eol);
      expect(splitMySQLScript(dump)).toEqual([
        "/*!50003 SET @saved_cs_client = @@character_set_client */",
        "/*!50003 SET sql_mode = 'STRICT_TRANS_TABLES' */",
        eolTrigger,
        "/*!50003 SET sql_mode = @saved_sql_mode */",
        "INSERT INTO t VALUES (1)",
      ]);
    }
  });

  it("handles DELIMITER ;; with CRLF line endings", () => {
    expect(
      splitMySQLScript(
        "DELIMITER ;;\r\nCREATE PROCEDURE p()\r\nBEGIN\r\n  SELECT 1;\r\n  SELECT 2;\r\nEND;;\r\nDELIMITER ;\r\nSELECT 3;\r\n",
      ),
    ).toEqual([
      "CREATE PROCEDURE p()\r\nBEGIN\r\n  SELECT 1;\r\n  SELECT 2;\r\nEND",
      "SELECT 3",
    ]);
  });

  it("keeps comments and quoted delimiters out of statement boundaries", () => {
    expect(
      splitMySQLScript(
        "SELECT 'a;b', \"c;d\", `e;f` -- x;y\nFROM t; # z;w\nSELECT 'it''s; fine';",
      ),
    ).toEqual([
      "SELECT 'a;b', \"c;d\", `e;f`  \nFROM t",
      "SELECT 'it''s; fine'",
    ]);
  });

  it("honors backslash escapes in strings unless disabled", () => {
    const sql = "SELECT 'a\\'; SELECT 2;";
    expect(splitMySQLScript(sql)).toEqual(["SELECT 'a\\'; SELECT 2;"]);
    expect(splitMySQLScript(sql, { backslashEscapes: false })).toEqual([
      "SELECT 'a\\'",
      "SELECT 2",
    ]);
    expect(
      splitMySQLScript('SELECT "x\\"; SELECT 2;', { backslashEscapes: false }),
    ).toEqual(['SELECT "x\\"', "SELECT 2"]);
    expect(splitMySQLScript("SELECT 'a\\'b'; SELECT 2;")).toEqual([
      "SELECT 'a\\'b'",
      "SELECT 2",
    ]);
  });

  it("ignores a leading UTF-8 byte order mark", () => {
    expect(
      splitMySQLScript(
        "\uFEFFDELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END$$\nDELIMITER ;\nSELECT 2;",
      ),
    ).toEqual(["CREATE PROCEDURE p() BEGIN SELECT 1; END", "SELECT 2"]);
    expect(splitMySQLScript("\uFEFFSELECT '\uFEFF'; SELECT 2;")).toEqual([
      "SELECT '\uFEFF'",
      "SELECT 2",
    ]);
  });
});
