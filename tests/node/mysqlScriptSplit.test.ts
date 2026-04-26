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
});
