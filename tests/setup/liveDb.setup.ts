import { beforeAll } from "vitest";
import { ensureRunTempRoot } from "../runtime/tempDirectories";

beforeAll(async () => {
  process.env.TZ = "UTC";
  await ensureRunTempRoot();
});
