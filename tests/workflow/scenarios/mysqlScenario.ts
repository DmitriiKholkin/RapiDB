import type { EngineScenario } from "./types";

const mysql = (): EngineScenario => ({
  engineId: "mysql",
  connectionType: "mysql",
  displayName: "MySQL",
  buildConnection: (password: string = "mysql_pass123") => ({
    id: "workflow-mysql",
    name: "Workflow MySQL",
    type: "mysql",
    host: "127.0.0.1",
    port: 3306,
    database: "rapidb_mysql_db",
    username: "mysql_user",
    password,
    folder: "Workflow Tests",
  }),
  capabilities: {
    primaryKey: true,
    rowUpdateByPk: true,
    structuredCell: false,
    multiRowPaste: true,
    csvExport: true,
    jsonExport: true,
    inlineInsert: true,
    deleteBySelection: true,
    supportsTransactions: false,
    driverEditorLanguage: "sql",
    driverSqlDialect: "mysql",
    defaultDatabase: "rapidb_mysql_db",
  },
  preflight: ["SELECT 1"],
  createEntities: {
    description: "Create a small MySQL table and seed it.",
    runAs: "query",
    statements: [
      "DROP TABLE IF EXISTS workflow_todos",
      `CREATE TABLE workflow_todos (
         id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
         title VARCHAR(255) NOT NULL,
         done TINYINT(1) NOT NULL DEFAULT 0,
         created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
       )`,
      `INSERT INTO workflow_todos (title, done) VALUES
         ('Write workflow test', 1),
         ('Review PR', 0),
         ('Ship release', 0)`,
    ],
    expectedNodeLabels: ["workflow_todos"],
  },
  tableFixture: {
    database: "rapidb_mysql_db",
    table: "workflow_todos",
    rowCount: 3,
    primaryKey: ["id"],
    columns: [
      { name: "id", type: "integer" },
      { name: "title", type: "string" },
      { name: "done", type: "integer" },
    ],
    expectedAfterInsert: 4,
    expectedAfterUpdate: 4,
    expectedAfterDelete: 3,
  },
  export: {
    format: "csv",
    expectedRowCount: 3,
    expectedHeaderSubstrings: ["id", "title", "done", "created_at"],
    rowAssertions: [
      {
        description: "row 1 contains seeded title",
        contains: ["Write workflow test"],
      },
    ],
  },
  cleanup: {
    statements: ["DROP TABLE IF EXISTS workflow_todos"],
    verifyGone: [
      "SELECT 1 FROM information_schema.tables WHERE table_name = 'workflow_todos'",
    ],
  },
});

export default mysql;
