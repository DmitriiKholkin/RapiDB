import {
  cleanupTempDirectories,
  ensureRunTempRoot,
} from "../runtime/tempDirectories";

export default async function globalSetup(): Promise<() => Promise<void>> {
  await ensureRunTempRoot();
  return async () => {
    await cleanupTempDirectories();
  };
}
