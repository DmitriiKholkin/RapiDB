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
  resolveAlias?: Record<string, string>;
}

export function createVitestProject({
  projectId,
  include,
  environment,
  setupFiles = [],
  testTimeout,
  hookTimeout,
  fileParallelism,
  resolveAlias,
}: CreateVitestProjectOptions) {
  return defineProject({
    resolve: resolveAlias
      ? {
          alias: resolveAlias,
        }
      : undefined,
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
