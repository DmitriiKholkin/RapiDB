import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

const createdTempDirectories = new Set<string>();
let managedRunRoot: string | null = null;

export async function ensureRunTempRoot(): Promise<string> {
  const configuredRoot = process.env.RAPIDB_TEST_TEMP_ROOT;
  if (configuredRoot) {
    await mkdir(configuredRoot, { recursive: true });
    return configuredRoot;
  }

  if (managedRunRoot) {
    return managedRunRoot;
  }

  managedRunRoot = await mkdtemp(join(tmpdir(), "rapidb-tests-"));
  return managedRunRoot;
}

export async function createProjectTempDir(
  projectId: string,
  prefix = "case-",
): Promise<string> {
  const runRoot = await ensureRunTempRoot();
  const directory = await mkdtemp(join(runRoot, `${projectId}-${prefix}`));
  createdTempDirectories.add(directory);
  return directory;
}

export async function ensureParentDirectory(filePath: string): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true });
}

export async function cleanupTempDirectories(): Promise<void> {
  if (managedRunRoot) {
    await rm(managedRunRoot, { recursive: true, force: true });
    createdTempDirectories.clear();
    managedRunRoot = null;
    return;
  }

  await Promise.all(
    Array.from(createdTempDirectories, (directory) =>
      rm(directory, { recursive: true, force: true }),
    ),
  );
  createdTempDirectories.clear();
}
