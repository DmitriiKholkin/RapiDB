import { afterEach, beforeAll, vi } from "vitest";
import { ensureRunTempRoot } from "../runtime/tempDirectories";

beforeAll(async () => {
  process.env.TZ = "UTC";
  await ensureRunTempRoot();
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});
