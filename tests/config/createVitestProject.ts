import { defineProject } from "vitest/config";
import type { TestProjectId } from "../contracts/testingContracts";

interface CreateVitestProjectOptions {
  projectId: TestProjectId;
  include: string[];
  environment: "node" | "jsdom";
  setupFiles?: string[];
  testTimeout?: number;
  hookTimeout?: number;
  fileParallelism?: boolean;
}

export function createVitestProject({
  projectId,
  include,
  environment,
  setupFiles = [],
  testTimeout,
  hookTimeout,
  fileParallelism,
}: CreateVitestProjectOptions) {
  return defineProject({
    test: {
      name: projectId,
      include,
      environment,
      setupFiles,
      testTimeout,
      hookTimeout,
      fileParallelism,
    },
  });
}
