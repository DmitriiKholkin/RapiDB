import type { ConnectionConfig } from "../../src/shared/connectionConfig.ts";
import {
  COMPOSE_BACKED_DB_ENGINE_IDS,
  DB_ENGINE_IDS,
  type DbEngineId,
  TEST_ADMIN_CONNECTION_SEEDS,
  TEST_CONNECTION_SEEDS,
} from "../contracts/testingContracts.ts";
import { buildFixtureMaterializationPlan } from "../fixtures/materializers.ts";
import { withSqlExecutor } from "./sqlExecutors.ts";
import {
  resolveAdminConnectionSeed,
  resolveComposeBackedEngineIds,
  resolveConnectionSeed,
} from "./testRuntimeConfig.ts";

interface ReadinessBudget {
  timeoutMs: number;
  retryDelayMs: number;
  readinessSql: string;
  slowPath: boolean;
}

interface EngineOperationResult {
  engineId: DbEngineId;
  elapsedMs: number;
}

const READINESS_BUDGETS: Record<
  (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number],
  ReadinessBudget
> = {
  postgres: {
    timeoutMs: 90_000,
    retryDelayMs: 2_000,
    readinessSql: "SELECT 1",
    slowPath: false,
  },
  mysql: {
    timeoutMs: 120_000,
    retryDelayMs: 2_500,
    readinessSql: "SELECT 1",
    slowPath: false,
  },
  mssql: {
    timeoutMs: 180_000,
    retryDelayMs: 4_000,
    readinessSql: "SELECT 1 AS ready_value",
    slowPath: false,
  },
  oracle: {
    timeoutMs: 480_000,
    retryDelayMs: 5_000,
    readinessSql: "SELECT 1 AS ready_value FROM dual",
    slowPath: true,
  },
};

function delay(durationMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
}

function collectKnownSecrets(): string[] {
  const seeds = [
    ...Object.values(TEST_CONNECTION_SEEDS),
    ...Object.values(TEST_ADMIN_CONNECTION_SEEDS).filter(
      (seed): seed is NonNullable<typeof seed> => Boolean(seed),
    ),
  ];

  return seeds
    .map((seed) => seed.connection.password)
    .filter(
      (password): password is string =>
        typeof password === "string" && password.length > 0,
    );
}

const KNOWN_SECRETS = collectKnownSecrets();

function redactSecrets(text: string): string {
  return KNOWN_SECRETS.reduce(
    (current, secret) => current.split(secret).join("[REDACTED]"),
    text,
  );
}

function logMessage(message: string): void {
  console.log(`[RapiDB:testdb] ${message}`);
}

function normalizeErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return redactSecrets(error.message);
  }
  return redactSecrets(String(error));
}

function assertComposeBackedEngine(
  engineId: DbEngineId,
): asserts engineId is (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number] {
  if (
    !COMPOSE_BACKED_DB_ENGINE_IDS.includes(
      engineId as (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number],
    )
  ) {
    throw new Error(
      `[RapiDB:testdb] Engine ${engineId} is not compose-backed.`,
    );
  }
}

async function executeStatements(
  engineId: DbEngineId,
  connection: ConnectionConfig,
  statements: readonly string[],
): Promise<void> {
  await withSqlExecutor(engineId, connection, async (executor) => {
    for (const statement of statements) {
      await executor.execute(statement);
    }
  });
}

async function measureOperation(
  engineId: DbEngineId,
  label: string,
  operation: () => Promise<void>,
): Promise<EngineOperationResult> {
  const startedAt = Date.now();
  logMessage(`${label} ${engineId}...`);
  await operation();
  const elapsedMs = Date.now() - startedAt;
  logMessage(`${label} ${engineId} finished in ${elapsedMs}ms.`);
  return { engineId, elapsedMs };
}

export function resolveRequestedComposeBackedEngines(
  rawArgs: readonly string[],
): readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][] {
  if (rawArgs.length === 0) {
    return resolveComposeBackedEngineIds() as readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][];
  }

  const requested = rawArgs.filter(
    (value): value is (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number] =>
      COMPOSE_BACKED_DB_ENGINE_IDS.includes(
        value as (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number],
      ),
  );

  if (requested.length === 0) {
    throw new Error(
      `[RapiDB:testdb] No supported compose-backed engines were requested. Supported values: ${COMPOSE_BACKED_DB_ENGINE_IDS.join(", ")}.`,
    );
  }

  return requested;
}

export async function waitForEngineReady(
  engineId: (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number],
): Promise<EngineOperationResult> {
  const budget = READINESS_BUDGETS[engineId];
  const connection =
    (await resolveAdminConnectionSeed(engineId)) ??
    (await resolveConnectionSeed(engineId));
  const startedAt = Date.now();
  let lastError = "No connection attempts executed.";
  if (budget.slowPath) {
    logMessage(
      `${engineId} is treated as a slow-path startup and may take up to ${budget.timeoutMs}ms.`,
    );
  }

  while (Date.now() - startedAt < budget.timeoutMs) {
    try {
      await withSqlExecutor(engineId, connection, async (executor) => {
        await executor.queryScalar(budget.readinessSql);
      });
      const elapsedMs = Date.now() - startedAt;
      logMessage(`${engineId} readiness probe passed in ${elapsedMs}ms.`);
      return { engineId, elapsedMs };
    } catch (error) {
      lastError = normalizeErrorMessage(error);
      await delay(budget.retryDelayMs);
    }
  }

  throw new Error(
    `[RapiDB:testdb] ${engineId} did not become ready within ${budget.timeoutMs}ms. Last error: ${lastError}`,
  );
}

export async function waitForComposeBackedDatabases(
  engineIds: readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][] = resolveComposeBackedEngineIds() as readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][],
): Promise<readonly EngineOperationResult[]> {
  const results: EngineOperationResult[] = [];
  for (const engineId of engineIds) {
    results.push(await waitForEngineReady(engineId));
  }
  return results;
}

export async function resetEngineFixtures(
  engineId: DbEngineId,
): Promise<EngineOperationResult> {
  const plan = buildFixtureMaterializationPlan(engineId);

  return measureOperation(engineId, "Resetting fixtures for", async () => {
    const adminConnection = await resolveAdminConnectionSeed(engineId);
    if (adminConnection && plan.bootstrapStatements.length > 0) {
      await executeStatements(
        engineId,
        adminConnection,
        plan.bootstrapStatements,
      );
    }

    const connection =
      engineId === "mysql"
        ? (adminConnection ?? (await resolveConnectionSeed(engineId)))
        : await resolveConnectionSeed(engineId);
    await executeStatements(engineId, connection, plan.resetStatements);
  });
}

export async function seedEngineFixtures(
  engineId: DbEngineId,
): Promise<EngineOperationResult> {
  const plan = buildFixtureMaterializationPlan(engineId);

  return measureOperation(engineId, "Seeding fixtures for", async () => {
    const connection =
      engineId === "mysql"
        ? ((await resolveAdminConnectionSeed(engineId)) ??
          (await resolveConnectionSeed(engineId)))
        : await resolveConnectionSeed(engineId);
    await executeStatements(engineId, connection, plan.seedStatements);
  });
}

export async function resetComposeBackedFixtures(
  engineIds: readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][] = resolveComposeBackedEngineIds() as readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][],
): Promise<readonly EngineOperationResult[]> {
  const results: EngineOperationResult[] = [];
  for (const engineId of engineIds) {
    assertComposeBackedEngine(engineId);
    results.push(await resetEngineFixtures(engineId));
  }
  return results;
}

export async function seedComposeBackedFixtures(
  engineIds: readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][] = resolveComposeBackedEngineIds() as readonly (typeof COMPOSE_BACKED_DB_ENGINE_IDS)[number][],
): Promise<readonly EngineOperationResult[]> {
  const results: EngineOperationResult[] = [];
  for (const engineId of engineIds) {
    assertComposeBackedEngine(engineId);
    results.push(await seedEngineFixtures(engineId));
  }
  return results;
}

export async function materializeSqliteFixture(
  filePath?: string,
): Promise<{ connection: ConnectionConfig; filePath: string }> {
  const connection = await resolveConnectionSeed("sqlite");
  const finalConnection = filePath
    ? {
        ...connection,
        filePath,
      }
    : connection;

  await executeStatements(
    "sqlite",
    finalConnection,
    buildFixtureMaterializationPlan("sqlite").resetStatements,
  );
  await executeStatements(
    "sqlite",
    finalConnection,
    buildFixtureMaterializationPlan("sqlite").seedStatements,
  );

  return {
    connection: finalConnection,
    filePath: finalConnection.filePath ?? "",
  };
}

export function isDbEngineId(value: string): value is DbEngineId {
  return DB_ENGINE_IDS.includes(value as DbEngineId);
}
