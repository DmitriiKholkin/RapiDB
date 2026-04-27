import type { ConnectionConfig } from "../../src/shared/connectionConfig.ts";

export const TEST_PROJECT_IDS = [
  "unit-node",
  "webview-jsdom",
  "db-sqlite",
  "db-postgres",
  "db-mysql",
  "db-mssql",
  "db-oracle",
  "extension-host",
] as const;

export type TestProjectId = (typeof TEST_PROJECT_IDS)[number];

export const DB_ENGINE_IDS = [
  "sqlite",
  "postgres",
  "mysql",
  "mssql",
  "oracle",
] as const;

export type DbEngineId = (typeof DB_ENGINE_IDS)[number];

export const COMPOSE_BACKED_DB_ENGINE_IDS = [
  "postgres",
  "mysql",
  "mssql",
  "oracle",
] as const;

export type ComposeBackedDbEngineId =
  (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number];

export const TEST_DATABASE_NAMES = {
  postgres: "rapidb_pg_db",
  mysql: "rapidb_mysql_db",
  mssql: "rapidb_mssql_db",
  oracle: "FREEPDB1",
} as const;

export const TEST_MSSQL_APP_LOGIN = {
  username: "rapidb_test_user",
  password: "mssql_pass123",
} as const;

export const TEST_ORACLE_APP_USERNAME = "oracle_test_user";

export interface EngineCapabilityProfile {
  engineId: DbEngineId;
  projectId: TestProjectId;
  driverType: ConnectionConfig["type"];
  usesDockerCompose: boolean;
  dockerServiceName: string | null;
  supportsSchemas: boolean;
  supportsMultipleDatabases: boolean;
  supportsNativeBoolean: boolean;
  supportsJsonColumns: boolean;
  supportsReturning: boolean;
  supportsTransactionalDdl: boolean;
  requiresOracleServiceName: boolean;
}

export const DB_ENGINE_TO_CONNECTION_TYPE: Record<
  DbEngineId,
  ConnectionConfig["type"]
> = {
  sqlite: "sqlite",
  postgres: "pg",
  mysql: "mysql",
  mssql: "mssql",
  oracle: "oracle",
};

export function projectIdForEngine(engineId: DbEngineId): TestProjectId {
  switch (engineId) {
    case "sqlite":
      return "db-sqlite";
    case "postgres":
      return "db-postgres";
    case "mysql":
      return "db-mysql";
    case "mssql":
      return "db-mssql";
    case "oracle":
      return "db-oracle";
  }
}

export const ENGINE_CAPABILITY_PROFILES: Record<
  DbEngineId,
  EngineCapabilityProfile
> = {
  sqlite: {
    engineId: "sqlite",
    projectId: "db-sqlite",
    driverType: "sqlite",
    usesDockerCompose: false,
    dockerServiceName: null,
    supportsSchemas: false,
    supportsMultipleDatabases: false,
    supportsNativeBoolean: false,
    supportsJsonColumns: false,
    supportsReturning: true,
    supportsTransactionalDdl: true,
    requiresOracleServiceName: false,
  },
  postgres: {
    engineId: "postgres",
    projectId: "db-postgres",
    driverType: "pg",
    usesDockerCompose: true,
    dockerServiceName: "postgres",
    supportsSchemas: true,
    supportsMultipleDatabases: true,
    supportsNativeBoolean: true,
    supportsJsonColumns: true,
    supportsReturning: true,
    supportsTransactionalDdl: true,
    requiresOracleServiceName: false,
  },
  mysql: {
    engineId: "mysql",
    projectId: "db-mysql",
    driverType: "mysql",
    usesDockerCompose: true,
    dockerServiceName: "mysql",
    supportsSchemas: false,
    supportsMultipleDatabases: true,
    supportsNativeBoolean: false,
    supportsJsonColumns: true,
    supportsReturning: false,
    supportsTransactionalDdl: false,
    requiresOracleServiceName: false,
  },
  mssql: {
    engineId: "mssql",
    projectId: "db-mssql",
    driverType: "mssql",
    usesDockerCompose: true,
    dockerServiceName: "mssql",
    supportsSchemas: true,
    supportsMultipleDatabases: true,
    supportsNativeBoolean: false,
    supportsJsonColumns: false,
    supportsReturning: false,
    supportsTransactionalDdl: true,
    requiresOracleServiceName: false,
  },
  oracle: {
    engineId: "oracle",
    projectId: "db-oracle",
    driverType: "oracle",
    usesDockerCompose: true,
    dockerServiceName: "oracle",
    supportsSchemas: true,
    supportsMultipleDatabases: false,
    supportsNativeBoolean: false,
    supportsJsonColumns: true,
    supportsReturning: true,
    supportsTransactionalDdl: true,
    requiresOracleServiceName: true,
  },
};

export interface CanonicalFixtureColumnExpectation {
  name: string;
  logicalType:
    | "integer"
    | "string"
    | "decimal"
    | "boolean"
    | "datetime"
    | "text";
  nullable: boolean;
  primaryKey?: boolean;
  comparisonMode: "strict" | "numeric" | "truthy" | "iso-datetime";
}

export interface CanonicalFixtureRowExpectation {
  id: number;
  display_name: string;
  amount: string;
  is_active: boolean;
  created_at: string;
  notes: string | null;
}

export interface CanonicalFixtureSchema {
  datasetId: "baseline-v1";
  schemaName: string;
  tableName: string;
  orderBy: string;
  columns: readonly CanonicalFixtureColumnExpectation[];
  seedRows: readonly CanonicalFixtureRowExpectation[];
}

export const CANONICAL_FIXTURE_SCHEMA: CanonicalFixtureSchema = {
  datasetId: "baseline-v1",
  schemaName: "rapidb_test",
  tableName: "fixture_rows",
  orderBy: "id ASC",
  columns: [
    {
      name: "id",
      logicalType: "integer",
      nullable: false,
      primaryKey: true,
      comparisonMode: "strict",
    },
    {
      name: "display_name",
      logicalType: "string",
      nullable: false,
      comparisonMode: "strict",
    },
    {
      name: "amount",
      logicalType: "decimal",
      nullable: false,
      comparisonMode: "numeric",
    },
    {
      name: "is_active",
      logicalType: "boolean",
      nullable: false,
      comparisonMode: "truthy",
    },
    {
      name: "created_at",
      logicalType: "datetime",
      nullable: false,
      comparisonMode: "iso-datetime",
    },
    {
      name: "notes",
      logicalType: "text",
      nullable: true,
      comparisonMode: "strict",
    },
  ],
  seedRows: [
    {
      id: 1,
      display_name: "Alpha Row",
      amount: "19.95",
      is_active: true,
      created_at: "2026-01-02T03:04:05.000Z",
      notes: "seed row alpha",
    },
    {
      id: 2,
      display_name: "Beta Row",
      amount: "0.00",
      is_active: false,
      created_at: "2026-02-03T04:05:06.000Z",
      notes: null,
    },
  ],
};

export interface TestConnectionSeed {
  engineId: DbEngineId;
  projectId: TestProjectId;
  usesDockerCompose: boolean;
  composeServiceNames: readonly string[];
  connection: ConnectionConfig;
}

export const TEST_CONNECTION_SEEDS: Record<DbEngineId, TestConnectionSeed> = {
  sqlite: {
    engineId: "sqlite",
    projectId: "db-sqlite",
    usesDockerCompose: false,
    composeServiceNames: [],
    connection: {
      id: "test-sqlite",
      name: "Test SQLite",
      type: "sqlite",
      filePath: "__RAPIDB_TEST_SQLITE_FILE__",
      folder: "Automated Tests",
    },
  },
  postgres: {
    engineId: "postgres",
    projectId: "db-postgres",
    usesDockerCompose: true,
    composeServiceNames: ["postgres"],
    connection: {
      id: "test-postgres",
      name: "Test PostgreSQL",
      type: "pg",
      host: "127.0.0.1",
      port: 5432,
      database: TEST_DATABASE_NAMES.postgres,
      username: "db_admin",
      password: "pg_pass123",
      folder: "Automated Tests",
    },
  },
  mysql: {
    engineId: "mysql",
    projectId: "db-mysql",
    usesDockerCompose: true,
    composeServiceNames: ["mysql"],
    connection: {
      id: "test-mysql",
      name: "Test MySQL",
      type: "mysql",
      host: "127.0.0.1",
      port: 3306,
      database: TEST_DATABASE_NAMES.mysql,
      username: "mysql_user",
      password: "mysql_pass123",
      folder: "Automated Tests",
    },
  },
  mssql: {
    engineId: "mssql",
    projectId: "db-mssql",
    usesDockerCompose: true,
    composeServiceNames: ["mssql"],
    connection: {
      id: "test-mssql",
      name: "Test MSSQL",
      type: "mssql",
      host: "localhost",
      port: 1433,
      database: TEST_DATABASE_NAMES.mssql,
      username: TEST_MSSQL_APP_LOGIN.username,
      password: TEST_MSSQL_APP_LOGIN.password,
      ssl: true,
      rejectUnauthorized: false,
      folder: "Automated Tests",
    },
  },
  oracle: {
    engineId: "oracle",
    projectId: "db-oracle",
    usesDockerCompose: true,
    composeServiceNames: ["oracle"],
    connection: {
      id: "test-oracle",
      name: "Test Oracle",
      type: "oracle",
      host: "127.0.0.1",
      port: 1521,
      username: TEST_ORACLE_APP_USERNAME,
      password: "oracle_pass123",
      serviceName: TEST_DATABASE_NAMES.oracle,
      database: TEST_DATABASE_NAMES.oracle,
      folder: "Automated Tests",
    },
  },
};

export const TEST_ADMIN_CONNECTION_SEEDS: Partial<
  Record<ComposeBackedDbEngineId, TestConnectionSeed>
> = {
  mysql: {
    engineId: "mysql",
    projectId: "db-mysql",
    usesDockerCompose: true,
    composeServiceNames: ["mysql"],
    connection: {
      id: "test-mysql-admin",
      name: "Test MySQL Admin",
      type: "mysql",
      host: "127.0.0.1",
      port: 3306,
      database: TEST_DATABASE_NAMES.mysql,
      username: "root",
      password: "root_pass123",
      folder: "Automated Tests",
    },
  },
  mssql: {
    engineId: "mssql",
    projectId: "db-mssql",
    usesDockerCompose: true,
    composeServiceNames: ["mssql"],
    connection: {
      id: "test-mssql-admin",
      name: "Test MSSQL Admin",
      type: "mssql",
      host: "localhost",
      port: 1433,
      database: "master",
      username: "sa",
      password: "Rapidb_Pass123!",
      ssl: true,
      rejectUnauthorized: false,
      folder: "Automated Tests",
    },
  },
};

export interface TestFailureSummary {
  projectId: TestProjectId;
  engineId?: DbEngineId;
  moduleId: string;
  testName: string;
  message: string;
  stack?: string;
  durationMs?: number;
}

export interface TestRunSummary {
  command: string;
  primaryCommand: string;
  selectedProjects: TestProjectId[];
  reason: "passed" | "failed" | "interrupted" | "running";
  startedAt: string;
  finishedAt?: string;
  passed: number;
  failed: number;
  skipped: number;
  failures: TestFailureSummary[];
}

export const ONE_COMMAND_TEST_MANIFEST = {
  primaryCommand: "npm run test:all",
  keepServicesEnvVar: "RAPIDB_KEEP_TEST_SERVICES",
  scripts: {
    compile: "npm run compile",
    typecheck: "npm run typecheck",
    test: "npm run test:unit",
    testAll: "npm run test:all",
    testUnit: "npm run test:unit",
    testWebview: "npm run test:webview",
    testDb: "npm run test:db",
    testDbSqlite: "npm run test:db:sqlite",
    testDbPostgres: "npm run test:db:postgres",
    testDbMysql: "npm run test:db:mysql",
    testDbMssql: "npm run test:db:mssql",
    testDbOracle: "npm run test:db:oracle",
    testExtension: "npm run test:extension",
    dbUp: "npm run db:up",
    dbWait: "npm run db:wait",
    dbReset: "npm run db:reset",
    dbSeed: "npm run db:seed",
    dbPrepare: "npm run db:prepare",
    dbDown: "npm run db:down",
  },
  projects: TEST_PROJECT_IDS,
} as const;

export function isTestProjectId(value: string): value is TestProjectId {
  return TEST_PROJECT_IDS.includes(value as TestProjectId);
}

export function engineIdForProject(
  projectId: TestProjectId,
): DbEngineId | undefined {
  switch (projectId) {
    case "db-sqlite":
      return "sqlite";
    case "db-postgres":
      return "postgres";
    case "db-mysql":
      return "mysql";
    case "db-mssql":
      return "mssql";
    case "db-oracle":
      return "oracle";
    default:
      return undefined;
  }
}
