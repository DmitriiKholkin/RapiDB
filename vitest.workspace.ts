import { resolve } from "node:path";
import { createVitestProject } from "./tests/config/createVitestProject";
import {
  DB_ENGINE_IDS,
  EXTENSION_HOST_PROJECT_INDEX,
  projectIdForEngine,
  TEST_PROJECT_IDS,
} from "./tests/contracts/testingContracts";

const dbProjects = DB_ENGINE_IDS.map((engineId) =>
  createVitestProject({
    projectId: projectIdForEngine(engineId),
    include: [`tests/db/${engineId}/**/*.test.ts`],
    environment: "node",
    setupFiles: [
      "./tests/setup/common.setup.ts",
      "./tests/setup/liveDb.setup.ts",
    ],
    testTimeout: 120000,
    hookTimeout: 300000,
    fileParallelism: false,
  }),
);
export default [
  createVitestProject({
    projectId: TEST_PROJECT_IDS[0],
    include: ["tests/node/**/*.test.ts"],
    environment: "node",
    setupFiles: ["./tests/setup/common.setup.ts"],
  }),
  createVitestProject({
    projectId: TEST_PROJECT_IDS[1],
    include: ["tests/webview/**/*.test.ts", "tests/webview/**/*.test.tsx"],
    environment: "jsdom",
    setupFiles: [
      "./tests/setup/common.setup.ts",
      "./tests/setup/webview.setup.ts",
    ],
    resolveAlias: {
      "monaco-editor": resolve(__dirname, "tests/mocks/monaco-editor.ts"),
    },
  }),
  ...dbProjects,
  createVitestProject({
    projectId: TEST_PROJECT_IDS[EXTENSION_HOST_PROJECT_INDEX],
    include: ["tests/extension/**/*.test.ts"],
    environment: "node",
    setupFiles: [
      "./tests/setup/common.setup.ts",
      "./tests/setup/extension.setup.ts",
    ],
    testTimeout: 60000,
    hookTimeout: 60000,
    fileParallelism: false,
  }),
  createVitestProject({
    projectId: "db-workflow",
    include: ["tests/workflow/**/*.test.ts", "tests/workflow/**/*.test.tsx"],
    environment: "jsdom",
    setupFiles: [
      "./tests/setup/common.setup.ts",
      "./tests/setup/liveDb.setup.ts",
      "./tests/workflow/setup/workflow.setup.ts",
    ],
    testTimeout: 300000,
    hookTimeout: 600000,
    fileParallelism: false,
    resolveAlias: {
      "monaco-editor": resolve(__dirname, "tests/mocks/monaco-editor.ts"),
      vscode: resolve(__dirname, "tests/mocks/vscode.ts"),
    },
  }),
];
