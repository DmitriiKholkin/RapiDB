import type { EngineScenario } from "./types";

const redis = (): EngineScenario => {
  const keyspace = "db0";
  return {
    engineId: "redis",
    connectionType: "redis",
    displayName: "Redis",
    buildConnection: (password: string = "redis_pass123") => ({
      id: "workflow-redis",
      name: "Workflow Redis",
      type: "redis",
      host: "127.0.0.1",
      port: 6379,
      password,
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
      defaultDatabase: keyspace,
    },
    preflight: ["PING"],
    createEntities: {
      description: "Seed a few hash keys for the journey.",
      runAs: "query",
      statements: [
        "FLUSHDB",
        'HSET workflow_todos:1 title "Write workflow test" done "1"',
        'HSET workflow_todos:2 title "Review PR" done "0"',
        'HSET workflow_todos:3 title "Ship release" done "0"',
      ],
      expectedNodeLabels: [
        "workflow_todos:1",
        "workflow_todos:2",
        "workflow_todos:3",
      ],
    },
    tableFixture: {
      database: keyspace,
      table: "workflow_todos:1",
      rowCount: 1,
      columns: [
        { name: "field", type: "string" },
        { name: "value", type: "string" },
      ],
    },
    export: {
      format: "json",
      expectedRowCount: 1,
      expectedHeaderSubstrings: ["field", "value"],
      rowAssertions: [
        {
          description: "row 1 contains title field",
          contains: ["title", "Write workflow test"],
        },
      ],
    },
    cleanup: {
      statements: [
        "DEL workflow_todos:1",
        "DEL workflow_todos:2",
        "DEL workflow_todos:3",
      ],
      verifyGone: ["EXISTS workflow_todos:1"],
    },
    notes: [
      "Redis data viewer treats each key as a row. We open a single hash key for the table-fixture step.",
    ],
  };
};

export default redis;
