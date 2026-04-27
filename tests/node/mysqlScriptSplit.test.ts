import { describe, expect, it } from "vitest";
import { splitMySQLScript } from "../../src/extension/dbDrivers/mysql";

describe("splitMySQLScript", () => {
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
});
