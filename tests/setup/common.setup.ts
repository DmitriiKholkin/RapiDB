import { afterEach, beforeAll, vi } from "vitest";
import { ensureRunTempRoot } from "../runtime/tempDirectories";

// Explicit test option: tests may load the lockfile-verified better-sqlite3
// development dependency (see allowSQLiteDevelopmentRuntime). Production
// extensions never set this.
(globalThis as Record<symbol, unknown>)[
  Symbol.for("rapidb.sqlite.allowDevelopmentRuntime")
] = true;

beforeAll(async () => {
  process.env.TZ = "UTC";
  await ensureRunTempRoot();
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});
