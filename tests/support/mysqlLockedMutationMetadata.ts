/** Benign metadata for tests covering DML/verification after MySQL preflight. */
export function mysqlLockedMutationMetadata(sql: string) {
  if (
    (sql.startsWith("SELECT CONNECTION_ID() FROM ") &&
      sql.endsWith(" LIMIT 0")) ||
    sql.includes("FROM information_schema.TRIGGERS")
  ) {
    return [[]];
  }
  if (sql === "SHOW COUNT(*) WARNINGS") return [[[0]]];
  if (sql.startsWith("SHOW CREATE TABLE ")) {
    return [[{ "Create Table": "CREATE TABLE `items` (`id` int)" }]];
  }
  if (sql.includes("information_schema.ENGINES")) {
    return [[{ ENGINE: "InnoDB", TRANSACTIONS: "YES" }]];
  }
  if (sql === "SELECT CURRENT_ROLE()") return [[["NONE"]]];
  if (sql === "SHOW GRANTS FOR CURRENT_USER()") {
    return [[["GRANT TRIGGER ON *.* TO `editor`@`localhost`"]]];
  }
  return undefined;
}
