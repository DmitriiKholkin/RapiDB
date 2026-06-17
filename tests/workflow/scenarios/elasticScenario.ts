import type { EngineScenario } from "./types";

const elastic = (): EngineScenario => {
  // Unique index name per factory call so parallel/consecutive test runs
  // never collide. Both createEntities and cleanup share the same closure,
  // so the name is consistent within a single test.
  const indexName = `workflow_todos_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
  return {
    engineId: "elasticsearch",
    connectionType: "elasticsearch",
    displayName: "Elasticsearch",
    buildConnection: () => ({
      id: "workflow-elastic",
      name: "Workflow Elasticsearch",
      type: "elasticsearch",
      endpoint: "http://127.0.0.1:9200",
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
      defaultDatabase: "default",
    },
    preflight: [],
    createEntities: {
      description: "Create an index and seed documents via REST commands.",
      runAs: "query",
      statements: [
        `PUT /${indexName} { "settings": { "number_of_replicas": 0 } }`,
        `POST /${indexName}/_doc/1 { "title": "Write workflow test", "done": true }`,
        `POST /${indexName}/_doc/2 { "title": "Review PR", "done": false }`,
        `POST /${indexName}/_doc/3 { "title": "Ship release", "done": false }`,
      ],
      expectedNodeLabels: [indexName],
    },
    tableFixture: {
      database: "default",
      table: indexName,
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
      expectedHeaderSubstrings: ["title", "done"],
      rowAssertions: [
        {
          description: "rows include seeded title",
          contains: ["Write workflow test"],
        },
      ],
    },
    cleanup: {
      statements: [`DELETE /${indexName}?ignore_unavailable=true`],
      verifyGone: [`HEAD /${indexName}`],
    },
    notes: [
      "Elasticsearch data viewer is read-only. We skip row-update, paste, insert, and delete steps.",
    ],
  };
};

export default elastic;
