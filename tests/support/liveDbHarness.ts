import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  IDBDriver,
  QueryResult,
} from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import type { DbEngineId } from "../contracts/testingContracts";
import {
  CANONICAL_FIXTURE_DATASET,
  FIXTURE_ROUTINE_NAMES,
  FIXTURE_TABLE_NAMES,
  physicalizeFixtureIdentifier,
  resolveFixtureNamespace,
} from "../fixtures/canonicalDataset";
import {
  materializeSqliteFixture,
  resetEngineFixtures,
  seedEngineFixtures,
} from "../runtime/liveDbOrchestration";
import { resolveConnectionSeed } from "../runtime/testRuntimeConfig";

export interface LiveDriverHarness {
  engineId: DbEngineId;
  connection: ConnectionConfig;
  driver: IDBDriver;
  databaseName: string;
  schemaName: string;
}

export async function createLiveDriverHarness(
  engineId: DbEngineId,
): Promise<LiveDriverHarness> {
  let connection: ConnectionConfig;

  if (engineId === "sqlite") {
    const result = await materializeSqliteFixture();
    connection = result.connection;
  } else {
    await resetEngineFixtures(engineId);
    await seedEngineFixtures(engineId);
    connection = await resolveConnectionSeed(engineId);
  }

  const driver = createDriver(engineId, connection);
  await driver.connect();

  const namespace = resolveFixtureNamespace(engineId);
  return {
    engineId,
    connection,
    driver,
    databaseName:
      namespace.physicalDatabaseName ?? connection.database ?? "main",
    schemaName: namespace.physicalSchemaName,
  };
}

export async function disposeLiveDriverHarness(
  harness: LiveDriverHarness | undefined,
): Promise<void> {
  await harness?.driver.disconnect();
}

export function createDriver(
  engineId: DbEngineId,
  connection: ConnectionConfig,
): IDBDriver {
  switch (engineId) {
    case "sqlite":
      return new SQLiteDriver(connection);
    case "postgres":
      return new PostgresDriver(connection);
    case "mysql":
      return new MySQLDriver(connection);
    case "mssql":
      return new MSSQLDriver(connection);
    case "oracle":
      return new OracleDriver(connection);
  }
}

export function fixtureTableName(
  engineId: DbEngineId,
  key: keyof typeof FIXTURE_TABLE_NAMES,
): string {
  return physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES[key]);
}

export function fixtureRoutineName(
  engineId: DbEngineId,
  key: keyof typeof FIXTURE_ROUTINE_NAMES,
): string {
  return physicalizeFixtureIdentifier(engineId, FIXTURE_ROUTINE_NAMES[key]);
}

export function rowsFromQuery(
  result: QueryResult,
): Array<Record<string, unknown>> {
  return result.rows.map((row) =>
    Object.fromEntries(
      result.columns.map((columnName, index) => [
        columnName,
        row[`__col_${index}`],
      ]),
    ),
  );
}

export function truthyBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value === 1;
  }

  return ["1", "true", "t", "y", "yes"].includes(String(value).toLowerCase());
}

export function fixtureSupportSummary(engineId: DbEngineId): {
  routines: boolean;
} {
  return {
    routines: CANONICAL_FIXTURE_DATASET.routines.some((routine) =>
      routine.supportedEngines.some(
        (supportedEngine) => supportedEngine === engineId,
      ),
    ),
  };
}
