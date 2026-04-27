import { beforeAll } from "vitest";
import { ensureRunTempRoot } from "../runtime/tempDirectories";

beforeAll(async () => {
  process.env.RAPIDB_EXTENSION_TEST_MODE = "true";
  await ensureRunTempRoot();
});
