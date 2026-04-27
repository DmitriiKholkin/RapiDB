import { mkdir } from "node:fs/promises";
import { relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { ConnectionConfig } from "../../src/shared/connectionConfig.ts";
import {
  COMPOSE_BACKED_DB_ENGINE_IDS,
  type DbEngineId,
  ONE_COMMAND_TEST_MANIFEST,
  projectIdForEngine,
  TEST_ADMIN_CONNECTION_SEEDS,
  TEST_CONNECTION_SEEDS,
  type TestProjectId,
} from "../contracts/testingContracts.ts";
import { createProjectTempDir, ensureRunTempRoot } from "./tempDirectories.ts";

export const WORKSPACE_ROOT = fileURLToPath(new URL("../../", import.meta.url));
export const COMPOSE_FILE_PATH = resolve(WORKSPACE_ROOT, "compose.yaml");

export interface TestRuntimePaths {
  workspaceRoot: string;
  composeFilePath: string;
  runTempRoot: string;
}

export async function resolveRuntimePaths(): Promise<TestRuntimePaths> {
  const runTempRoot = await ensureRunTempRoot();
  await mkdir(runTempRoot, { recursive: true });

  return {
    workspaceRoot: WORKSPACE_ROOT,
    composeFilePath: COMPOSE_FILE_PATH,
    runTempRoot,
  };
}

export async function resolveConnectionSeed(
  engineId: DbEngineId,
): Promise<ConnectionConfig> {
  const baseSeed = TEST_CONNECTION_SEEDS[engineId].connection;

  if (engineId !== "sqlite") {
    return { ...baseSeed };
  }

  const sqliteDirectory = await createProjectTempDir(
    projectIdForEngine(engineId),
    "sqlite-",
  );
  const filePath = resolve(sqliteDirectory, "rapidb.test.sqlite");
  return {
    ...baseSeed,
    filePath,
  };
}

export async function resolveAdminConnectionSeed(
  engineId: DbEngineId,
): Promise<ConnectionConfig | null> {
  if (engineId === "sqlite") {
    return null;
  }

  const adminSeed = TEST_ADMIN_CONNECTION_SEEDS[engineId]?.connection;
  return adminSeed ? { ...adminSeed } : null;
}

export function resolveComposeBackedEngineIds(): readonly DbEngineId[] {
  return COMPOSE_BACKED_DB_ENGINE_IDS;
}

export function relativeToWorkspace(filePath: string): string {
  if (!filePath.startsWith(WORKSPACE_ROOT)) {
    return filePath;
  }
  return relative(WORKSPACE_ROOT, filePath);
}

export async function resolveSummaryFilePath(
  projectId: TestProjectId | "workspace",
): Promise<string> {
  const { runTempRoot } = await resolveRuntimePaths();
  return resolve(runTempRoot, `${projectId}.summary.json`);
}

export const TEST_RUNTIME_MANIFEST = {
  workspaceRoot: WORKSPACE_ROOT,
  composeFilePath: COMPOSE_FILE_PATH,
  primaryCommand: ONE_COMMAND_TEST_MANIFEST.primaryCommand,
};
