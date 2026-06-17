import type { EngineScenario } from "./types";

const dynamo = (): EngineScenario => {
  const tableName = "WorkflowTodos";
  return {
    engineId: "dynamodb",
    connectionType: "dynamodb",
    displayName: "DynamoDB",
    buildConnection: () => ({
      id: "workflow-dynamodb",
      name: "Workflow DynamoDB",
      type: "dynamodb",
      awsRegion: "us-east-1",
      endpoint: "http://127.0.0.1:8000",
      awsAccessKeyId: "rapidb",
      awsSecretAccessKey: "rapidb-secret",
      folder: "Workflow Tests",
    }),
    capabilities: {
      primaryKey: true,
      rowUpdateByPk: true,
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
      description:
        "Seed items via the native driver; the table itself is created/cleaned by the test harness via AWS SDK.",
      runAs: "query",
      statements: [
        `{"TableName":"${tableName}","Item":{"id":{"S":"1"},"title":{"S":"Write workflow test"},"done":{"BOOL":true}}}`,
        `{"TableName":"${tableName}","Item":{"id":{"S":"2"},"title":{"S":"Review PR"},"done":{"BOOL":false}}}`,
        `{"TableName":"${tableName}","Item":{"id":{"S":"3"},"title":{"S":"Ship release"},"done":{"BOOL":false}}}`,
      ],
      expectedNodeLabels: [tableName],
    },
    tableFixture: {
      database: "default",
      table: tableName,
      rowCount: 3,
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "string" },
        { name: "title", type: "string" },
        { name: "done", type: "boolean" },
      ],
    },
    export: {
      format: "json",
      expectedRowCount: 3,
      expectedHeaderSubstrings: ["id", "title", "done"],
      rowAssertions: [
        {
          description: "rows include seeded title",
          contains: ["Write workflow test"],
        },
      ],
    },
    cleanup: {
      statements: [
        `{"TableName":"${tableName}","Key":{"id":{"S":"1"}}}`,
        `{"TableName":"${tableName}","Key":{"id":{"S":"2"}}}`,
        `{"TableName":"${tableName}","Key":{"id":{"S":"3"}}}`,
      ],
      verifyGone: [
        `{"TableName":"${tableName}","FilterExpression":"begins_with(id, :p)","ExpressionAttributeValues":{":p":{"S":"none"}}}`,
      ],
    },
    notes: [
      "DynamoDB has no inline insert UI; updates use the partition key only. We skip the insert and delete-by-selection steps.",
    ],
  };
};

export default dynamo;
