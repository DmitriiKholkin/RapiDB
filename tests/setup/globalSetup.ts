import { join } from "node:path";
import { build } from "esbuild";
import { sqliteWorkerConfig } from "../../esbuild.config.mjs";
import {
  cleanupTempDirectories,
  createProjectTempDir,
  ensureRunTempRoot,
} from "../runtime/tempDirectories";

export default async function globalSetup(): Promise<() => Promise<void>> {
  await ensureRunTempRoot();
  const directory = await createProjectTempDir("sqlite-worker");
  process.env.RAPIDB_SQLITE_WORKER_PATH = join(directory, "sqliteWorker.js");
  await build({
    ...sqliteWorkerConfig,
    outfile: process.env.RAPIDB_SQLITE_WORKER_PATH,
    sourcemap: false,
  });
  return async () => {
    await cleanupTempDirectories();
  };
}
