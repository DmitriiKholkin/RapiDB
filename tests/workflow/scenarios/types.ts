import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import type { ConnectionType } from "../../../src/shared/connectionTypes";

export type WorkflowEngineId =
  | "sqlite"
  | "postgres"
  | "mysql"
  | "mssql"
  | "oracle"
  | "mongodb"
  | "redis"
  | "elasticsearch"
  | "dynamodb";

export interface EngineCapabilities {
  primaryKey: boolean;
  rowUpdateByPk: boolean;
  structuredCell: boolean;
  multiRowPaste: boolean;
  csvExport: boolean;
  jsonExport: boolean;
  inlineInsert: boolean;
  deleteBySelection: boolean;
  supportsTransactions: boolean;
  driverEditorLanguage: "sql" | "javascript";
  driverSqlDialect?:
    | "postgresql"
    | "mysql"
    | "transactsql"
    | "sqlite"
    | "plsql"
    | "sql";
  defaultDatabase: string;
  defaultSchema?: string;
}

export interface EngineEntitySeed {
  description: string;
  runAs: "query" | "driver";
  statements: string[];
  cleanupStatements?: string[];
  expectedNodeLabels: string[];
}

export interface EngineTableFixture {
  database: string;
  schema?: string;
  table: string;
  rowCount: number;
  columns: { name: string; type: "string" | "integer" | "boolean" }[];
  primaryKey?: string[];
  expectedAfterInsert?: number;
  expectedAfterUpdate?: number;
  expectedAfterDelete?: number;
}

export interface EngineExportExpectation {
  format: "csv" | "json";
  expectedRowCount: number;
  expectedHeaderSubstrings: string[];
  rowAssertions: Array<{
    description: string;
    contains: string[];
  }>;
}

export interface EngineScenario {
  engineId: WorkflowEngineId;
  connectionType: ConnectionType;
  displayName: string;
  buildConnection: (credential?: string) => ConnectionConfig;
  capabilities: EngineCapabilities;
  preflight: string[];
  createEntities: EngineEntitySeed;
  tableFixture: EngineTableFixture;
  export: EngineExportExpectation;
  cleanup: {
    statements: string[];
    verifyGone: string[];
  };
  notes?: string[];
}
