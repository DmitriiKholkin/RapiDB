import { spawnSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import type { ClientConfig as PgClientConfig } from "pg";
import type { DbKind } from "./db-fixtures";

type NetworkDbKind = Exclude<DbKind, "sqlite">;

const NETWORKED_DB_KINDS: readonly NetworkDbKind[] = [
  "pg",
  "mysql",
  "mssql",
  "oracle",
];

interface ServicePreflight {
  kind: NetworkDbKind;
  composeService: string;
  wait: () => Promise<void>;
}

async function loadCommonJsModule<T>(specifier: string): Promise<T> {
  const imported = await import(specifier);
  return (imported.default ?? imported) as T;
}

function isDbKind(value: string): value is DbKind {
  return (
    value === "pg" ||
    value === "mysql" ||
    value === "mssql" ||
    value === "oracle" ||
    value === "sqlite"
  );
}

export function parseRequestedKinds(
  argv: string[] = process.argv.slice(2),
  envValue: string = process.env.RAPIDB_DBS ?? "",
): Set<DbKind> {
  const tokens = [...argv, ...envValue.split(",")];
  const requested = new Set<DbKind>();

  for (const token of tokens) {
    const normalized = token.trim().toLowerCase();
    if (!normalized) continue;
    if (isDbKind(normalized)) {
      requested.add(normalized);
      continue;
    }

    if (normalized.startsWith("--db=")) {
      const value = normalized.slice(5);
      if (isDbKind(value)) requested.add(value);
      continue;
    }

    if (normalized.startsWith("db=")) {
      const value = normalized.slice(3);
      if (isDbKind(value)) requested.add(value);
    }
  }

  return requested;
}

export function getRequestedNetworkKinds(
  requested: Set<DbKind>,
): NetworkDbKind[] {
  const activeKinds =
    requested.size === 0 ? new Set(NETWORKED_DB_KINDS) : requested;
  return NETWORKED_DB_KINDS.filter((kind) => activeKinds.has(kind));
}

function runCommand(command: string, args: string[]): void {
  const result = spawnSync(command, args, {
    cwd: process.cwd(),
    stdio: "inherit",
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    throw new Error(`Command failed: ${command} ${args.join(" ")}`);
  }
}

function detectComposeRunner(): { command: string; baseArgs: string[] } {
  const candidates = [
    {
      command: "docker",
      baseArgs: ["compose"],
      versionArgs: ["compose", "version"],
    },
    { command: "docker-compose", baseArgs: [], versionArgs: ["version"] },
  ] as const;

  for (const candidate of candidates) {
    const result = spawnSync(candidate.command, candidate.versionArgs, {
      cwd: process.cwd(),
      stdio: "ignore",
    });

    if (!result.error && result.status === 0) {
      return { command: candidate.command, baseArgs: [...candidate.baseArgs] };
    }
  }

  throw new Error(
    "Docker Compose is required for test:db. Install Docker Desktop or docker compose, then rerun the command.",
  );
}

async function waitForCondition(
  label: string,
  check: () => Promise<void>,
  timeoutMs = 600_000,
  intervalMs = 2_000,
): Promise<void> {
  const startedAt = Date.now();
  let lastError: unknown = null;
  let loggedFirstError = false;

  console.log(`[test:db] waiting for ${label}`);

  while (Date.now() - startedAt < timeoutMs) {
    try {
      await check();
      console.log(`[test:db] ${label} ready`);
      return;
    } catch (error) {
      lastError = error;
      if (!loggedFirstError) {
        loggedFirstError = true;
        const message = error instanceof Error ? error.message : String(error);
        console.log(`[test:db] ${label} not ready yet: ${message}`);
      }
      await delay(intervalMs);
    }
  }

  const message =
    lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(`Timed out waiting for ${label}: ${message}`);
}

async function waitForPostgres(): Promise<void> {
  const { Client } = await import("pg");
  const config: PgClientConfig = {
    host: "localhost",
    port: 5432,
    user: "db_admin",
    password: "pg_pass123",
    database: "happy_pg_db",
  };

  const client = new Client(config);
  await client.connect();
  try {
    await client.query("SELECT 1");
  } finally {
    await client.end();
  }
}

async function waitForMySql(): Promise<void> {
  const { createConnection } = await import("mysql2/promise");
  const connection = await createConnection({
    host: "localhost",
    port: 3306,
    user: "mysql_user",
    password: "mysql_pass123",
    database: "happy_mysql_db",
  });

  try {
    await connection.query("SELECT 1");
  } finally {
    await connection.end();
  }
}

async function waitForMssql(): Promise<void> {
  const mssql = await loadCommonJsModule<typeof import("mssql")>("mssql");
  const pool = new mssql.ConnectionPool({
    server: "localhost",
    port: 1433,
    database: "master",
    user: "sa",
    password: "Happy_Pass123!",
    connectionTimeout: 10_000,
    requestTimeout: 10_000,
    options: {
      encrypt: false,
      trustServerCertificate: true,
      enableArithAbort: true,
      abortTransactionOnError: true,
      useUTC: true,
    },
  });

  await pool.connect();
  try {
    await pool.request().query("SELECT 1 AS ok");
  } finally {
    await pool.close();
  }
}

async function waitForOracle(): Promise<void> {
  const oracledb =
    await loadCommonJsModule<typeof import("oracledb")>("oracledb");
  const connection = await oracledb.getConnection({
    user: "oracle_test_user",
    password: "oracle_pass123",
    connectString: "localhost:1521/FREEPDB1",
  });

  try {
    await connection.execute("SELECT 1 FROM dual");
  } finally {
    await connection.close();
  }
}

function servicePreflights(kinds: NetworkDbKind[]): ServicePreflight[] {
  const services: ServicePreflight[] = [];

  if (kinds.includes("pg")) {
    services.push({
      kind: "pg",
      composeService: "postgres",
      wait: waitForPostgres,
    });
  }
  if (kinds.includes("mysql")) {
    services.push({
      kind: "mysql",
      composeService: "mysql",
      wait: waitForMySql,
    });
  }
  if (kinds.includes("mssql")) {
    services.push({
      kind: "mssql",
      composeService: "mssql",
      wait: waitForMssql,
    });
  }
  if (kinds.includes("oracle")) {
    services.push({
      kind: "oracle",
      composeService: "oracle",
      wait: waitForOracle,
    });
  }

  return services;
}

export async function main(): Promise<void> {
  const requestedKinds = parseRequestedKinds();
  const networkKinds = getRequestedNetworkKinds(requestedKinds);

  if (networkKinds.length === 0) {
    console.log(
      "[test:db] SQLite-only run detected; no Docker services required.",
    );
    return;
  }

  const composeRunner = detectComposeRunner();
  const services = servicePreflights(networkKinds);

  console.log(
    `[test:db] starting Docker services: ${services.map((service) => service.composeService).join(", ")}`,
  );
  runCommand(composeRunner.command, [
    ...composeRunner.baseArgs,
    "-f",
    "compose.yaml",
    "up",
    "-d",
    ...services.map((service) => service.composeService),
  ]);

  await Promise.all(
    services.map((service) =>
      waitForCondition(`${service.kind} readiness`, service.wait),
    ),
  );
}

if (process.argv[1]?.endsWith("db-preflight.cjs")) {
  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[test:db] ${message}`);
    process.exit(1);
  });
}
