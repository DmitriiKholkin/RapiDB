import type { EngineScenario } from "./types";

const sqlite = (): EngineScenario => ({
  engineId: "sqlite",
  connectionType: "sqlite",
  displayName: "SQLite",
  buildConnection: (filePath: string = "/tmp/workflow-rapidb.sqlite") => ({
    id: "workflow-sqlite",
    name: "Workflow SQLite",
    type: "sqlite",
    filePath,
    folder: "Workflow Tests",
  }),
  capabilities: {
    primaryKey: true,
    rowUpdateByPk: true,
    structuredCell: true,
    multiRowPaste: true,
    csvExport: true,
    jsonExport: true,
    inlineInsert: true,
    deleteBySelection: true,
    supportsTransactions: true,
    driverEditorLanguage: "sql",
    driverSqlDialect: "sqlite",
    defaultDatabase: "main",
  },
  preflight: ["SELECT 1"],
  createEntities: {
    description: "Create a small table with seeded data for the journey.",
    runAs: "query",
    statements: [
      "DROP TABLE IF EXISTS workflow_todos",
      `CREATE TABLE workflow_todos (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         title TEXT NOT NULL,
         done INTEGER NOT NULL DEFAULT 0,
         created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
       )`,
      `INSERT INTO workflow_todos (title, done) VALUES
         ('Write workflow test', 1),
         ('Review PR', 0),
         ('Ship release', 0)`,
    ],
    expectedNodeLabels: ["workflow_todos"],
  },
  tableFixture: {
    database: "main",
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
      { description: "row 2 contains seeded title", contains: ["Review PR"] },
    ],
  },
  cleanup: {
    statements: ["DROP TABLE IF EXISTS workflow_todos"],
    verifyGone: [
      "SELECT name FROM sqlite_master WHERE name = 'workflow_todos'",
    ],
  },
});

export default sqlite;
