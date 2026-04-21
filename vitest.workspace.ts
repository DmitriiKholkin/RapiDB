import { createVitestProject } from "./tests/config/createVitestProject";
import {
  DB_ENGINE_IDS,
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
    testTimeout: 120_000,
    hookTimeout: 300_000,
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
  }),
  ...dbProjects,
  createVitestProject({
    projectId: TEST_PROJECT_IDS[7],
    include: ["tests/extension/**/*.test.ts"],
    environment: "node",
    setupFiles: [
      "./tests/setup/common.setup.ts",
      "./tests/setup/extension.setup.ts",
    ],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    fileParallelism: false,
  }),
];
