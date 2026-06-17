import type { WorkflowEngineId } from "./scenarios/types";

const driverClassByEngine: Record<
  WorkflowEngineId,
  { module: string; className: string }
> = {
  sqlite: {
    module: "../../src/extension/dbDrivers/sqlite",
    className: "SQLiteDriver",
  },
  postgres: {
    module: "../../src/extension/dbDrivers/postgres",
    className: "PostgresDriver",
  },
  mysql: {
    module: "../../src/extension/dbDrivers/mysql",
    className: "MySQLDriver",
  },
  mssql: {
    module: "../../src/extension/dbDrivers/mssql",
    className: "MSSQLDriver",
  },
  oracle: {
    module: "../../src/extension/dbDrivers/oracle",
    className: "OracleDriver",
  },
  mongodb: {
    module: "../../src/extension/dbDrivers/mongodb",
    className: "MongoDBDriver",
  },
  redis: {
    module: "../../src/extension/dbDrivers/redis",
    className: "RedisDriver",
  },
  elasticsearch: {
    module: "../../src/extension/dbDrivers/elasticsearch",
    className: "ElasticsearchDriver",
  },
  dynamodb: {
    module: "../../src/extension/dbDrivers/dynamodb",
    className: "DynamoDBDriver",
  },
};

export interface RawDriver {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  query(
    sql: string,
    params?: unknown[],
  ): Promise<{
    rows: unknown[];
    columns: string[];
    rowCount?: number;
    affectedRows?: number;
  }>;
}

export async function withDriver<T>(
  engineId: WorkflowEngineId,
  connection: unknown,
  fn: (driver: RawDriver) => Promise<T>,
): Promise<T> {
  const { module: mod, className } = driverClassByEngine[engineId];
  const driverModule = await import(mod);
  const DriverClass = (
    driverModule as Record<string, new (cfg: unknown) => RawDriver>
  )[className];
  if (!DriverClass) {
    throw new Error(`No driver class found for engine ${engineId}`);
  }
  const driver = new DriverClass(connection);
  await driver.connect();
  try {
    return await fn(driver);
  } finally {
    await driver.disconnect();
  }
}
