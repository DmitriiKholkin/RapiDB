import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import { RunSummaryReporter } from "./tests/reporters/runSummaryReporter";
import projects from "./vitest.workspace";

export default defineConfig({
  resolve: {
    alias: {
      "monaco-editor": resolve(__dirname, "tests/mocks/monaco-editor.ts"),
    },
  },
  test: {
    projects,
    globals: true,
    clearMocks: true,
    restoreMocks: true,
    mockReset: true,
    unstubGlobals: true,
    unstubEnvs: true,
    passWithNoTests: true,
    globalSetup: ["./tests/setup/globalSetup.ts"],
    reporters: ["default", new RunSummaryReporter()],
  },
});
