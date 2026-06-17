import type { EngineScenario } from "./types";

const mongo = (): EngineScenario => {
  const database = "rapidb_mongo_db";
  const collection = "workflow_todos";
  return {
    engineId: "mongodb",
    connectionType: "mongodb",
    displayName: "MongoDB",
    buildConnection: (password: string = "mongo_pass123") => ({
      id: "workflow-mongodb",
      name: "Workflow MongoDB",
      type: "mongodb",
      host: "127.0.0.1",
      port: 27017,
      database,
      username: "mongo_admin",
      password,
      authSource: "admin",
      directConnection: true,
      folder: "Workflow Tests",
    }),
    capabilities: {
      primaryKey: false,
      rowUpdateByPk: false,
      structuredCell: false,
      multiRowPaste: false,
      csvExport: true,
      jsonExport: true,
      inlineInsert: false,
      deleteBySelection: false,
      supportsTransactions: false,
      driverEditorLanguage: "javascript",
      defaultDatabase: database,
    },
    preflight: ["db.runCommand({ ping: 1 })"],
    createEntities: {
      description: "Create a collection and insert workflow documents.",
      runAs: "query",
      statements: [
        `db.${collection}.deleteMany({})`,
        `db.${collection}.insertMany([
           { title: "Write workflow test", done: true,  created_at: new Date() },
           { title: "Review PR",          done: false, created_at: new Date() },
           { title: "Ship release",       done: false, created_at: new Date() }
         ])`,
        `db.${collection}.countDocuments({})`,
      ],
      expectedNodeLabels: [collection],
    },
    tableFixture: {
      database,
      table: collection,
      rowCount: 3,
      columns: [
        { name: "_id", type: "string" },
        { name: "title", type: "string" },
        { name: "done", type: "boolean" },
      ],
    },
    export: {
      format: "json",
      expectedRowCount: 3,
      expectedHeaderSubstrings: ["title", "done", "created_at"],
      rowAssertions: [
        {
          description: "rows include seeded title",
          contains: ["Write workflow test"],
        },
      ],
    },
    cleanup: {
      statements: [`db.${collection}.deleteMany({})`],
      verifyGone: [`db.${collection}.countDocuments({})`],
    },
    notes: [
      "MongoDB has no primary key constraint. We still display _id as a column but skip row-update, paste, and delete-by-selection steps.",
    ],
  };
};

export default mongo;
