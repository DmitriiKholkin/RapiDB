import type { EngineScenario } from "./types";

const mssql = (): EngineScenario => ({
  engineId: "mssql",
  connectionType: "mssql",
  displayName: "MSSQL",
  buildConnection: (password: string = "mssql_pass123") => ({
    id: "workflow-mssql",
    name: "Workflow MSSQL",
    type: "mssql",
    host: "localhost",
    port: 1433,
    database: "rapidb_mssql_db",
    username: "rapidb_test_user",
    password,
    tls: { mode: "requireTrustServerCertificate" },
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
    supportsTransactions: true,
    driverEditorLanguage: "sql",
    driverSqlDialect: "transactsql",
    defaultDatabase: "rapidb_mssql_db",
    defaultSchema: "dbo",
  },
  preflight: ["SELECT 1 AS ready_value"],
  createEntities: {
    description: "Create a small MSSQL table and seed it.",
    runAs: "query",
    statements: [
      "DROP TABLE IF EXISTS workflow_todos",
      `CREATE TABLE workflow_todos (
         id BIGINT IDENTITY(1,1) PRIMARY KEY,
         title NVARCHAR(255) NOT NULL,
         done BIT NOT NULL DEFAULT 0,
         created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
       )`,
      `INSERT INTO workflow_todos (title, done) VALUES
         (N'Write workflow test', 1),
         (N'Review PR', 0),
         (N'Ship release', 0)`,
    ],
    expectedNodeLabels: ["workflow_todos"],
  },
  tableFixture: {
    database: "rapidb_mssql_db",
    schema: "dbo",
    table: "workflow_todos",
    rowCount: 3,
    primaryKey: ["id"],
    columns: [
      { name: "id", type: "integer" },
      { name: "title", type: "string" },
      { name: "done", type: "boolean" },
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
      "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'workflow_todos'",
    ],
  },
});

export default mssql;
