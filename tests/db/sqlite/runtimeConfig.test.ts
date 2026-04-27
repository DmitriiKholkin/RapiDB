import { stat } from "node:fs/promises";
import { dirname } from "node:path";
import { describe, expect, it } from "vitest";
import {
  resolveConnectionSeed,
  resolveRuntimePaths,
} from "../../runtime/testRuntimeConfig";

describe("sqlite runtime config", () => {
  it("creates a sqlite temp path inside the managed run root", async () => {
    const runtimePaths = await resolveRuntimePaths();
    const seed = await resolveConnectionSeed("sqlite");

    expect(seed.type).toBe("sqlite");
    expect(seed.filePath).toContain(runtimePaths.runTempRoot);

    const directory = await stat(dirname(seed.filePath ?? ""));
    expect(directory.isDirectory()).toBe(true);
  });
});
