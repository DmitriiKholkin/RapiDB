import type { EngineScenario } from "./types";

const postgres = (): EngineScenario => ({
  engineId: "postgres",
  connectionType: "pg",
  displayName: "PostgreSQL",
  buildConnection: (password: string = "pg_pass123") => ({
    id: "workflow-postgres",
    name: "Workflow Postgres",
    type: "pg",
    host: "127.0.0.1",
    port: 5432,
    database: "rapidb_pg_db",
    username: "db_admin",
    password,
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
    driverSqlDialect: "postgresql",
    defaultDatabase: "rapidb_pg_db",
    defaultSchema: "public",
  },
  preflight: ["SELECT 1"],
  createEntities: {
    description: "Create a small workflow table and seed it.",
    runAs: "query",
    statements: [
      "DROP TABLE IF EXISTS workflow_todos",
      `CREATE TABLE workflow_todos (
         id BIGSERIAL PRIMARY KEY,
         title TEXT NOT NULL,
         done BOOLEAN NOT NULL DEFAULT FALSE,
         created_at TIMESTAMPTZ NOT NULL DEFAULT now()
       )`,
      `INSERT INTO workflow_todos (title, done) VALUES
         ('Write workflow test', TRUE),
         ('Review PR', FALSE),
         ('Ship release', FALSE)`,
    ],
    expectedNodeLabels: ["workflow_todos"],
  },
  tableFixture: {
    database: "rapidb_pg_db",
    schema: "public",
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
      "SELECT 1 FROM information_schema.tables WHERE table_name = 'workflow_todos'",
    ],
  },
});

export default postgres;
